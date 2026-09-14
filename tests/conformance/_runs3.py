'''
Helpers for the RUN-family cases of plan Phase 3 — credits (RUN-024/025), the
rollout drain (RUN-030) and the ledger variants of MSG-008/009/012.

Everything here is plumbing two modules would otherwise duplicate: worker
threads that take one input each and wait at a barrier (the MSG-017 shape,
reused by RUN-024), a JetStream driver whose receivers carry a byte budget
(RUN-025), a real ``videoflow.runtime.worker`` subprocess built from an
environment (RUN-030), and messengers over a durable ``FileRuntimeStore`` ledger
with the durables provisioned at ``max_deliver = -1`` (STREAM-15, D11). The
oracles stay in the case modules, next to the assertions they decide. Nothing
is imported from ``tests/integration``.
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
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence

from _brokers import run_async
from _msgdrivers import Bound, JetStreamDriver, StubNode

from videoflow.backends import faults
from videoflow.backends.memory.runtime_store import FileRuntimeStore
from videoflow.backends.messaging import Completed
from videoflow.backends.outcomes import SettleConfirmed
from videoflow.backends.runtime import FlowRuntime
from videoflow.core.compiler import NodeSpec
from videoflow.core.constants import BATCH
from videoflow.messaging import topology
from videoflow.messaging.jetstream_backend import JetStreamMessagingBackend
from videoflow.messaging.nats_messenger import NATSMessenger

HERE = pathlib.Path(__file__).parent


def write_evidence(evidence_dir : Any, name : str, record : Dict[str, Any]) -> None:
    (evidence_dir / name).write_text(json.dumps(record, indent = 2, default = str))


# -- worker threads over a driver ---------------------------------------------------------------

def hold_at_barrier(driver : Any, bounds : Dict[str, Bound], barrier : str = 'worker.ready.after') -> tuple:
    '''
    One thread per binding: each takes one input, reaches ``barrier`` (paused by
    the caller's schedule) while holding it, and completes it once released.
    Returns ``(held, settled, release)``: what each worker holds (bytes), the ids
    completed so far, and ``release(schedule)`` which lets the pause go, stops
    the threads and re-raises anything a thread hit.
    '''
    stop = threading.Event()
    held : Dict[str, int] = {}
    settled : List[str] = []
    errors : List[BaseException] = []
    lock = threading.Lock()

    def worker(name : str, bound : Bound) -> None:
        try:
            while not stop.is_set():
                got = driver.receive(bound, timeout = 0.3)
                if not got:
                    time.sleep(0.01)
                    continue
                delivery = got[0]
                with lock:
                    held[name] = delivery.size
                faults.barrier(barrier, worker = name, message_id = delivery.token.message_id)
                driver.settle(bound, delivery.token, Completed())
                with lock:
                    settled.append(delivery.token.message_id)
                return
        except BaseException as e:  # noqa: BLE001 — surfaced to the test thread
            errors.append(e)

    threads = [threading.Thread(target = worker, args = (name, bound), daemon = True, name = f'vf-conf-{name}')
               for name, bound in bounds.items()]
    for t in threads:
        t.start()

    def release(schedule : faults.FaultSchedule, pause : str) -> None:
        schedule.release(pause)
        stop.set()
        for t in threads:
            t.join(15)
        assert not errors, errors
    return held, settled, release


def drain_concurrently(driver : Any, bounds : Dict[str, Bound], done : Callable[[Dict[str, List[tuple]]], bool],
                       timeout : float) -> Dict[str, List[tuple]]:
    '''
    One receiving thread per binding, each completing whatever it gets, until
    ``done`` says the ledger is complete (or the timeout). Returns, per receiver,
    the ``(message_id, attempt)`` pairs it completed.
    '''
    settled : Dict[str, List[tuple]] = {name: [] for name in bounds}
    stop = threading.Event()
    errors : List[BaseException] = []

    def worker(name : str, bound : Bound) -> None:
        try:
            while not stop.is_set():
                got = driver.receive(bound, timeout = 0.3)
                for delivery in got:
                    outcome = driver.settle(bound, delivery.token, Completed())
                    if isinstance(outcome, SettleConfirmed):
                        settled[name].append((delivery.token.message_id, delivery.token.attempt))
                if not got:
                    time.sleep(0.01)
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target = worker, args = (name, bound), daemon = True, name = f'vf-conf-{name}')
               for name, bound in bounds.items()]
    for t in threads:
        t.start()
    try:
        driver.until(lambda: done(settled) or bool(errors), timeout, step = 0.25)
    finally:
        stop.set()
        for t in threads:
            t.join(10)
    assert not errors, errors
    return settled


# -- a byte-budgeted JetStream driver (RUN-025) --------------------------------------------------

class BudgetedJetStreamDriver(JetStreamDriver):
    '''
    ``JetStreamDriver`` whose receivers are built with the adapter's byte budget
    (``JetStreamMessagingBackend(byte_budget = ...)``, what ``VF_PREFETCH_BYTES``
    sets on a worker), so a receiver stops fetching once it holds ``byte_budget``
    unsettled bytes. ``retire``/``kill``/``close`` work exactly as on the base.
    '''
    def __init__(self, nats_url : str, flow_id : str, run_id : str, byte_budget : int | None,
                 flow_type : str = BATCH) -> None:
        self.byte_budget = byte_budget
        super().__init__(nats_url, flow_id, run_id, flow_type)

    def _start(self, name : str, prefetch : int, keepalive : bool) -> JetStreamMessagingBackend:
        budget = None if name == '_provision' else self.byte_budget
        backend = JetStreamMessagingBackend(self.client_url, self.flow_id, self.run_id, self.flow_type,
                                            prefetch = prefetch, keepalive = keepalive, byte_budget = budget)
        backend.start()
        self._backends[name] = backend
        return backend


# -- a real worker subprocess (RUN-030) ------------------------------------------------------------

def worker_env(nats_url : str, flow_id : str, run_id : str, node_name : str, node_class : str,
               params : Dict[str, Any], parents : Sequence[str], kind : str = 'consumer',
               ack_wait : int = 10, max_retries : int = 3, extra : Optional[Dict[str, str]] = None) -> Dict[str, str]:
    '''The environment ``videoflow.runtime.worker`` reads (its module docstring is the contract), RFC 0006 on.'''
    env = dict(os.environ)
    env.update({
        'VF_NODE_CLASS': node_class, 'VF_NODE_PARAMS_JSON': json.dumps(params), 'VF_NODE_KIND': kind,
        'VF_NODE_NAME': node_name, 'VF_PARENT_NAMES': ','.join(parents), 'VF_HAS_CHILDREN': '0',
        'VF_NATS_URL': nats_url, 'VF_FLOW_ID': flow_id, 'VF_FLOW_TYPE': BATCH, 'VF_RUN_ID': run_id,
        'VF_REPLICA_ID': '0', 'VF_ACK_WAIT_SECONDS': str(ack_wait), 'VF_MAX_RETRIES': str(max_retries),
        'VF_HEALTH_PORT': '0',
        # A crashed worker's partition lease lapses this fast, so the replacement a
        # case starts right after a real crash binds within seconds, not a default lease.
        'VF_PARTITION_LEASE_SECONDS': '2',
        'PYTHONPATH': os.pathsep.join([str(HERE)] + [p for p in [os.environ.get('PYTHONPATH')] if p]),
    })
    env.update(extra or {})
    return env


def start_worker(env : Dict[str, str], log_path : pathlib.Path) -> subprocess.Popen:
    '''``python -m videoflow.worker`` (the published entrypoint), stderr captured to ``log_path``.'''
    log = open(log_path, 'ab')
    return subprocess.Popen([sys.executable, '-m', 'videoflow.worker'], env = env, stdout = log, stderr = log,
                            cwd = os.getcwd())


def read_sink_log(path : pathlib.Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def stop_flow(nats_url : str, flow_id : str, run_id : str) -> None:
    '''Publish the run's flow-wide stop on its control subject (what the CLI does on abort).'''
    import nats  # optional dep (distributed extras)

    async def _go() -> None:
        nc = await nats.connect(nats_url)
        try:
            await nc.publish(topology.control_subject_for(flow_id, run_id), b'stop')
            await nc.flush()
        finally:
            await nc.close()
    run_async(_go)


# -- messengers over a durable ledger (MSG-008/009/012 ledger variants) -------------------------------

def provision_with_ledger(driver : JetStreamDriver, specs : List[NodeSpec], flow_type : str,
                          max_retries : int, ack_wait : int = 60) -> None:
    '''
    What the provision Job does when ``VF_RUNTIME_STORE_URL`` reads back durable
    and shared: at-least-once durables at ``max_deliver = -1`` (``ledger_budget``),
    the retry budget left to the workers' ledger (STREAM-15, D11).
    '''
    topology.provision_flow_sync(driver.nats_url, specs, driver.flow_id, driver.run_id, flow_type,
                                 max_retries = max_retries, ack_wait = ack_wait, ledger_budget = True, timeout = 60)


def ledger_runtime(root : str, flow_id : str, run_id : str, node : str) -> FlowRuntime:
    '''A fresh ``FlowRuntime`` over the file ledger at ``root`` — what a *new process* of ``node`` would build.'''
    return FlowRuntime(FileRuntimeStore(root), flow_id, run_id, node)


class Messengers:
    '''Messengers built for one test, quiesced and closed in reverse order on exit whatever happened.'''
    def __init__(self) -> None:
        self._open : List[NATSMessenger] = []

    def ledger_messenger(self, driver : JetStreamDriver, root : str, node : str, parents : Sequence[str],
                         max_retries : int, ack_wait : int = 2, delivery : Optional[dict] = None,
                         flow_type : str = BATCH) -> NATSMessenger:
        '''
        A worker's messenger over the file ledger: with RFC 0006 on, it binds at
        the ledger budget (``max_deliver = -1``). A second messenger for the same
        node is a *replacement*: the predecessor this pool still holds is
        declared dead first (its partition lease forfeited, as a crashed process's
        would lapse), so the replacement binds at once instead of being refused
        as a scaled-out singleton.
        '''
        for previous in list(self._open):
            if previous._node.name == node:
                previous._stop_lease_renewal(release = True)
        m = NATSMessenger(StubNode(node), list(parents), driver.client_url, driver.flow_id, flow_type, driver.run_id,
                          max_retries = max_retries, ack_wait = ack_wait, delivery_policy = delivery,
                          runtime = ledger_runtime(root, driver.flow_id, driver.run_id, node))
        self._open.append(m)
        return m

    def messenger(self, driver : JetStreamDriver, node : str, parents : Sequence[str], max_retries : int = 3,
                  ack_wait : int = 10, flow_type : str = BATCH) -> NATSMessenger:
        m = NATSMessenger(StubNode(node), list(parents), driver.client_url, driver.flow_id, flow_type, driver.run_id,
                          max_retries = max_retries, ack_wait = ack_wait)
        self._open.append(m)
        return m

    def close(self, m : NATSMessenger) -> None:
        if m in self._open:
            self._open.remove(m)
        with contextlib.suppress(Exception):
            m.quiesce()
        with contextlib.suppress(Exception):
            m.close()

    def __enter__(self) -> 'Messengers':
        return self

    def __exit__(self, *exc : object) -> None:
        for m in reversed(list(self._open)):
            self.close(m)


@contextlib.contextmanager
def messengers() -> Iterator[Messengers]:
    with Messengers() as pool:
        yield pool


def status_unresolved(m : NATSMessenger, parent : str) -> Optional[int]:
    '''``unresolved`` of the messenger's data subscription on ``parent``, or None when the observation is Unknown.'''
    from videoflow.backends.outcomes import Known
    observed = m.subscription_status()[parent]
    return observed.value.unresolved if isinstance(observed, Known) else None
