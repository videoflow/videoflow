'''
Plumbing for the RUN cases that exercise the runtime ledger through the real
task loops: RUN-003/004/013/017/022 (``test_run_commit.py``), RUN-014/015
(``test_run_sources.py``) and RUN-020/021/023/034 (``test_run_partitions.py``).

What lives here and why:

- ``ledger``/``runtime_for``: a ``FileRuntimeStore`` under the test's tmp dir —
  the one store the plan lets a process-level case call durable (the memory
  store never qualifies) — and the ``FlowRuntime`` a worker would build on it.
- ``messenger_for``: a ``NATSMessenger`` for a *real* node object (the rigs in
  ``_payloads`` only build stub nodes), over the rig's memory backend or the
  compose broker, registered with the rig so its ``close`` tears it down.
- ``TaskThread``: one of the three task loops on a thread, with whatever ended
  it — a ``SimulatedCrash`` from a barrier, a typed error — kept for the test.
- ``Collector``: a sink-side messenger draining a channel and recording the
  identity of every input (the "sink identity ledger" the catalog asks for).
- ``WorkerProcess``: the real ``videoflow.runtime.worker`` as a subprocess,
  driven by the same environment contract a deployment writes, with the fault
  schedule delivered through ``VF_FAULT_SCHEDULE_JSON`` (ENV-16/17).

The oracles stay in the case modules; nothing here decides a case. The helpers
the broker-level cases needed from ``tests/integration`` are copied, not imported.
'''
from __future__ import absolute_import, division, print_function

import contextlib
import json
import os
import pathlib
import subprocess
import sys
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence

from _payloads import JetStreamRig, MemoryRig, spec, wait_until, write_evidence  # noqa: F401 — re-exported plumbing
from _runnodes import SimulatedCrash

from videoflow.backends import faults
from videoflow.backends.memory.runtime_store import FileRuntimeStore
from videoflow.backends.runtime import FlowRuntime, RuntimeStore
from videoflow.core.constants import BATCH
from videoflow.core.context import RuntimeContext
from videoflow.core.node import ConsumerNode, Node, ProcessorNode, ProducerNode
from videoflow.core.task import ConsumerTask, NodeTask, ProcessorTask, ProducerTask
from videoflow.messaging.nats_messenger import NATSMessenger
from videoflow.wire.serialization import MSG_TYPE_DATA, derive_message_id

HERE = pathlib.Path(__file__).parent
#: The lease every rig here provisions: short, so a crashed process's unsettled
#: deliveries reach its replacement in seconds (model seconds on the fake clock).
ACK_WAIT = 5


# -- the ledger ---------------------------------------------------------------------------

def ledger(root : pathlib.Path, name : str = 'ledger') -> FileRuntimeStore:
    '''A durable, host-shared runtime store under the test's directory (``file://``).'''
    return FileRuntimeStore(str(root / name))


def runtime_for(store : RuntimeStore, rig : Any, node : str, replica_id : int = 0, nb_tasks : int = 1,
                partition_by : Optional[str] = None, parent_replicas : Optional[Dict[str, int]] = None,
                clock : Optional[Callable[[], float]] = None, run_id : Optional[str] = None) -> FlowRuntime:
    '''The ledger a worker builds for ``node`` (``videoflow.runtime.worker``), on ``store``.'''
    return FlowRuntime(store, rig.flow_id, run_id or rig.run_id, node, replica_id, nb_tasks, partition_by,
                       parent_replicas, clock = clock or time.time)


def outbox_snapshot(runtime : FlowRuntime) -> List[Dict[str, Any]]:
    '''Every outbox entry of the node, as evidence.'''
    out = []
    for key, raw, version in runtime.store.scan(runtime.key('outbox', '')):
        doc = json.loads(raw.decode())
        out.append({'publication_id': key.rsplit('/', 1)[-1], 'outcome': doc.get('outcome'), 'kind': doc.get('kind'),
                    'digest': doc.get('digest'), 'version': version})
    return out


def checkpoint_snapshot(runtime : FlowRuntime) -> Dict[str, Any]:
    state, position = runtime.restore_checkpoint()
    output = dict(position.get('output') or {})
    if 'body' in output:
        output['body_bytes'] = len(bytes.fromhex(output.pop('body')))
    return {'state': json.loads(state.decode()) if state else None,
            'position': dict(position, output = output) if output else position}


# -- messengers over a rig ------------------------------------------------------------------

def declare_dead(messengers : Sequence[NATSMessenger], node : str, replica_id : int = 0) -> None:
    '''
    A replacement for ``node``'s replica is being built: every earlier messenger
    of that identity still held by the harness is declared dead — its partition
    lease forfeited the way a crashed process's lapses (the wait skipped), its
    renewals stopped. Without this an in-process "crash" would keep renewing and
    the replacement would be refused as a scaled-out singleton (RUN-018/019).
    '''
    for previous in messengers:
        if previous._node.name == node and previous._replica_id == replica_id:
            previous._stop_lease_renewal(release = True)


_BUILT : Dict[int, List[NATSMessenger]] = {}


def messenger_for(rig : Any, node : Node, parents : Sequence[str], runtime : Optional[FlowRuntime] = None,
                  run_id : Optional[str] = None, nats_url : Optional[str] = None, **kwargs : Any) -> NATSMessenger:
    '''
    A ``NATSMessenger`` bound to a real node object on the rig's backend (memory)
    or on the compose broker (its own connection), with the node's class-level
    declarations (``replayable``, ``replay_policy``, ``partition_key_policy``)
    read exactly as the worker reads them. A second messenger for the same node
    and replica on one rig is a replacement: see ``declare_dead``.
    '''
    declare_dead(_BUILT.get(id(rig), []), node.name, int(kwargs.get('replica_id', 0)))
    if isinstance(rig, MemoryRig):
        kwargs.setdefault('backend', rig.backend)
        kwargs.setdefault('payload_store', rig.store)
        kwargs.setdefault('ack_wait', ACK_WAIT)
    else:
        kwargs.setdefault('ack_wait', rig.ack_wait)
        kwargs.setdefault('max_retries', rig.max_retries)
    if isinstance(node, ProducerNode):
        kwargs.setdefault('replayable', node.replayable)
    if isinstance(node, (ProcessorNode, ConsumerNode)) and node.partition_by:
        kwargs.setdefault('partition_by', node.partition_by)
    if isinstance(node, ProcessorNode):
        kwargs.setdefault('nb_tasks', node.nb_tasks)
    m = NATSMessenger(node, list(parents), nats_url or rig.nats_url, rig.flow_id, rig.flow_type,
                      run_id or rig.run_id, runtime = runtime, **kwargs)
    _BUILT.setdefault(id(rig), []).append(m)
    rig._messengers.append(m)
    return m


def context_for(rig : Any, messenger : NATSMessenger, node : str, replica_id : int = 0,
                run_id : Optional[str] = None) -> RuntimeContext:
    import logging
    return RuntimeContext(rig.flow_id, run_id or rig.run_id, node, replica_id,
                          logging.getLogger(f'videoflow.node.{node}'), messenger = messenger)


def publication_id(rig : Any, node : str, trace_id : str, seq : int, run_id : Optional[str] = None) -> str:
    '''The content-derived id of ``node``'s data output for the group ``(trace_id, seq)`` — also its group key.'''
    return derive_message_id(rig.flow_id, run_id or rig.run_id, node, trace_id, seq, MSG_TYPE_DATA)


# -- task loops on threads ----------------------------------------------------------------------

def allow_task_threads(monkeypatch : Any) -> None:
    '''
    Let a task loop run off the main thread. The loop wraps each iteration in
    ``DelayedKeyboardInterrupt``, a ``signal`` handler only the main thread may
    install; the cases here run several loops at once (a paused owner and its
    replacement, a sink beside a processor), so for their duration the wrapper
    is a no-op — Ctrl-C handling is not what they measure.
    '''
    from videoflow.core import task as task_module
    monkeypatch.setattr(task_module, 'DelayedKeyboardInterrupt', contextlib.nullcontext)


class TaskThread(threading.Thread):
    '''
    Runs one task loop until it ends — end of stream, a ``SimulatedCrash`` from
    a barrier, or an error — and keeps what ended it. ``crashed`` is the
    in-process death; anything else in ``error`` is the test's to judge.
    '''
    def __init__(self, task : NodeTask, name : str = 'task') -> None:
        super().__init__(name = name, daemon = True)
        self.task = task
        self.error : Optional[BaseException] = None
        self.finished = threading.Event()

    def run(self) -> None:
        try:
            self.task.run()
        except BaseException as e:  # noqa: BLE001 — surfaced to the test thread
            self.error = e
        finally:
            self.finished.set()

    @property
    def crashed(self) -> bool:
        return isinstance(self.error, SimulatedCrash)

    def wait(self, timeout : float) -> None:
        self.join(timeout)
        assert not self.is_alive(), f'{self.name} still running after {timeout:.0f}s'


def processor_task(rig : Any, node : ProcessorNode, messenger : NATSMessenger, parents : Sequence[str],
                   has_children : bool = True, replica_id : int = 0, run_id : Optional[str] = None,
                   **kwargs : Any) -> ProcessorTask:
    ctx = context_for(rig, messenger, node.name, replica_id, run_id)
    return ProcessorTask(node, messenger, has_children, list(parents), ctx = ctx, **kwargs)


def consumer_task(rig : Any, node : ConsumerNode, messenger : NATSMessenger, parents : Sequence[str],
                  replica_id : int = 0, run_id : Optional[str] = None, **kwargs : Any) -> ConsumerTask:
    ctx = context_for(rig, messenger, node.name, replica_id, run_id)
    return ConsumerTask(node, messenger, False, list(parents), ctx = ctx, **kwargs)


def producer_task(rig : Any, node : ProducerNode, messenger : NATSMessenger, run_id : Optional[str] = None,
                  **kwargs : Any) -> ProducerTask:
    ctx = context_for(rig, messenger, node.name, 0, run_id)
    kwargs.setdefault('resume_offset', messenger.resume_offset())
    return ProducerTask(node, messenger, True, ctx = ctx, **kwargs)


class Collector(threading.Thread):
    '''
    A sink-side messenger draining one parent and recording every input's
    identity — ``trace_id``, ``seq``, the publication id it was derived from,
    the message — until the parent's end of stream (``stop()`` ends it early).
    The sink identity ledger of RUN-003/004/013/014/015.
    '''
    def __init__(self, rig : Any, parent : str, name : str = 'sink', run_id : Optional[str] = None,
                 **kwargs : Any) -> None:
        super().__init__(name = f'collector-{name}', daemon = True)
        self.rig = rig
        self.parent = parent
        self.node_name = name
        self.messenger = rig.messenger(name, [parent], run_id = run_id, **kwargs)
        self.received : List[Dict[str, Any]] = []
        self.error : Optional[BaseException] = None
        self.stopped_by_eos = False
        self.aborted = False
        self._stop_requested = threading.Event()
        self._lock = threading.Lock()

    def run(self) -> None:
        try:
            while not self._stop_requested.is_set():
                inputs = self.messenger.receive_message()
                entry = inputs[self.parent]
                if entry.get('is_stop_signal'):
                    self.stopped_by_eos = True
                    self.aborted = bool(entry.get('is_abort'))
                    return
                info = (self.messenger.last_input_info() or {}).get(self.parent) or {}
                with self._lock:
                    self.received.append({
                        'trace_id': info.get('trace_id'), 'seq': info.get('seq'), 'message': entry['message'],
                        'metadata': entry.get('metadata'), 'event_ts': entry.get('event_ts'),
                        'input_key': self.messenger.last_input_key(), 'at': time.monotonic(),
                    })
                self.messenger.ack_inputs()
        except BaseException as e:  # noqa: BLE001 — surfaced to the test thread
            self.error = e

    def snapshot(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.received)

    def identities(self) -> List[tuple]:
        return [(r['trace_id'], r['seq']) for r in self.snapshot()]

    def count(self) -> int:
        with self._lock:
            return len(self.received)

    def stop(self) -> None:
        self._stop_requested.set()
        self.messenger.quiesce()

    def finish(self, timeout : float) -> None:
        '''Wait for the end of stream; fail loudly rather than hang.'''
        self.join(timeout)
        assert self.error is None, f'{self.name}: {self.error!r}'
        assert not self.is_alive(), f'{self.name} never saw the end of stream ({self.count()} received)'


# -- worker subprocesses ------------------------------------------------------------------------

def worker_env(rig : Any, node : Node, kind : str, parents : Sequence[str], has_children : bool,
               store_url : str, replica_id : int = 0, nb_tasks : int = 1, run_id : Optional[str] = None,
               schedule : Optional[faults.FaultSchedule] = None, parent_replicas : Optional[Sequence[int]] = None,
               ack_wait : int = 5, extra : Optional[Dict[str, str]] = None) -> Dict[str, str]:
    '''
    The environment ``videoflow.runtime.worker`` reads, for ``node`` on the rig's
    broker: the compiled-spec contract (class, params, kind, names), the ledger
    (``VF_RUNTIME_STORE_URL``), the fault schedule (``VF_FAULT_SCHEDULE_JSON``)
    and the switch. ``PYTHONPATH`` gains this directory so ``VF_NODE_CLASS`` can
    name a fixture in ``_runnodes``.
    '''
    env = dict(os.environ)
    env['PYTHONPATH'] = str(HERE) + (os.pathsep + env['PYTHONPATH'] if env.get('PYTHONPATH') else '')
    env.update({
        'VF_NODE_CLASS': f'{type(node).__module__}.{type(node).__name__}',
        'VF_NODE_PARAMS_JSON': json.dumps(node.get_params()),
        'VF_NODE_KIND': kind, 'VF_NODE_NAME': node.name,
        'VF_PARENT_NAMES': ','.join(parents), 'VF_HAS_CHILDREN': '1' if has_children else '0',
        'VF_NATS_URL': rig.nats_url, 'VF_FLOW_ID': rig.flow_id, 'VF_FLOW_TYPE': rig.flow_type,
        'VF_RUN_ID': run_id or rig.run_id, 'VF_REPLICA_ID': str(replica_id), 'VF_NB_TASKS': str(nb_tasks),
        'VF_ACK_WAIT_SECONDS': str(ack_wait), 'VF_MAX_RETRIES': str(getattr(rig, 'max_retries', 3)),
        'VF_RUNTIME_STORE_URL': store_url, 'VF_HEALTH_PORT': '0', 'VF_WATCHDOG_INTERVAL_SECONDS': '0',
        'VF_PROGRESS_TIMEOUT_SECONDS': '0', 'VF_EOS_QUIESCENCE_MS': '200',
        'VF_PARTITION_LEASE_SECONDS': '2',   # a crashed worker's lease lapses fast; its replacement binds in seconds
    })
    if isinstance(node, (ProcessorNode, ConsumerNode)) and node.partition_by:
        env['VF_PARTITION_BY'] = node.partition_by
    if parent_replicas is not None:
        env['VF_PARENT_REPLICAS'] = ','.join(str(n) for n in parent_replicas)
    if schedule is not None:
        env.update(schedule.to_env())
    else:
        env.pop(faults.ENV_SCHEDULE, None)
        env.pop(faults.ENV_MARKER_DIR, None)
    env.update(extra or {})
    return env


class WorkerProcess:
    '''
    The real worker entrypoint as a child process. ``run`` blocks until it exits
    and returns ``(exit_code, stderr_tail)``; ``start``/``wait`` run it in the
    background so a test can interleave a paused worker with its replacement.
    '''
    SCRIPT = 'import sys\nfrom videoflow.runtime.worker import main\nsys.exit(main())\n'

    def __init__(self, env : Dict[str, str], name : str = 'worker') -> None:
        self.env = env
        self.name = name
        self.proc : Optional[subprocess.Popen] = None
        self.started_at = 0.0
        self.exit_code : Optional[int] = None
        self.stderr = ''

    def start(self) -> 'WorkerProcess':
        self.started_at = time.monotonic()
        self.proc = subprocess.Popen([sys.executable, '-c', self.SCRIPT], env = self.env, stdout = subprocess.PIPE,
                                     stderr = subprocess.PIPE, text = True, cwd = os.getcwd())
        return self

    def wait(self, timeout : float) -> int:
        assert self.proc is not None
        try:
            _out, err = self.proc.communicate(timeout = timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            _out, err = self.proc.communicate()
            raise AssertionError(f'{self.name} did not exit within {timeout:.0f}s; stderr tail:\n{err[-3000:]}') from None
        self.stderr = err
        self.exit_code = self.proc.returncode
        return self.exit_code

    def run(self, timeout : float) -> int:
        return self.start().wait(timeout)

    @property
    def took(self) -> float:
        return time.monotonic() - self.started_at

    def record(self) -> Dict[str, Any]:
        return {'name': self.name, 'exit_code': self.exit_code, 'took_s': round(self.took, 3),
                'stderr_tail': self.stderr[-1500:]}

    def kill(self) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.kill()
            with contextlib.suppress(Exception):
                self.proc.communicate(timeout = 10)


# -- small shared bits ----------------------------------------------------------------------

def crash_at(name : str = 'simulated crash') -> faults.RaiseError:
    '''The in-process crash action: the barrier raises ``SimulatedCrash``.'''
    return faults.RaiseError(lambda: SimulatedCrash(name))


class ContextSchedule(faults.FaultSchedule):
    '''
    A ``FaultSchedule`` whose actions apply only to hits whose context satisfies
    a predicate (``when[name]``); other hits of the same barrier neither fire the
    action nor count towards an ``Nth``. Several sites share a barrier name —
    ``group.commit.*`` fires when a join group is persisted *and* when a node's
    output is committed, ``settle.*`` for every consumer in the process — and a
    case that means one of them must not depend on the others' arithmetic.
    '''
    def __init__(self, actions : Any, when : Optional[Dict[str, Callable[[Any], bool]]] = None,
                 marker_dir : Optional[str] = None) -> None:
        super().__init__(actions, marker_dir)
        self._when = dict(when or {})

    def hit(self, name : str, context : Any) -> faults.Hit:
        predicate = self._when.get(name)
        if predicate is not None and not predicate(context):
            return faults.NO_HIT
        return super().hit(name, context)


def output_commit(context : Any) -> bool:
    '''The ``group.commit.*`` hits of an output commit (``op_id``), not of a persisted join group (``group``).'''
    return 'op_id' in context


def settling(consumer : str, message_ids : Optional[Sequence[str]] = None) -> Callable[[Any], bool]:
    '''The ``settle.*`` hits of one consumer node — of the given message ids only, when named (a terminator's ack is a settle too).'''
    wanted = None if message_ids is None else set(message_ids)
    return lambda context: context.get('consumer') == consumer and (wanted is None or context.get('op_id') in wanted)


def data_ids(rig : Any, node : str, count : int, run_id : Optional[str] = None) -> List[str]:
    '''The message ids of a replayable source's first ``count`` data publications (``{node}:{offset}``).'''
    return [derive_message_id(rig.flow_id, run_id or rig.run_id, node, f'{node}:{i}', i, MSG_TYPE_DATA)
            for i in range(1, count + 1)]


def reference_totals(records : Sequence[Dict[str, Any]]) -> Dict[str, int]:
    '''The uninterrupted accumulator: what the recovered state must equal.'''
    totals : Dict[str, int] = {}
    for r in records:
        totals[str(r['key'])] = totals.get(str(r['key']), 0) + int(r['value'])
    return totals


def batch_specs(processor : str = 'acc', processor_kind : str = 'processor', nb_tasks : int = 1,
                partition_by : Optional[str] = None, sink : Optional[str] = 'sink') -> List[Any]:
    '''src → processor → sink, the three-node shape most cases here use.'''
    out = [spec('src', [], 'producer', True),
           spec(processor, ['src'], processor_kind, sink is not None, nb_tasks = nb_tasks, partition_by = partition_by)]
    if sink is not None:
        out.append(spec(sink, [processor], 'consumer', False))
    return out


def memory_rig(specs : Sequence[Any], ack_wait : int = ACK_WAIT, **kwargs : Any) -> MemoryRig:
    return MemoryRig(BATCH, specs = list(specs), ack_wait = ack_wait, **kwargs)


def provision_run(rig : Any, run_id : str, specs : Sequence[Any]) -> None:
    '''Provision ``specs`` for another run of the same flow, on either rig.'''
    if isinstance(rig, MemoryRig):
        rig.provision(list(specs), run_id = run_id, ack_wait = ACK_WAIT)
    else:
        rig.provision_run(run_id, list(specs))
