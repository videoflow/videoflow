'''
Shared plumbing for the RUN join and completion cases (RUN-001, RUN-002, RUN-005,
RUN-006, RUN-007, RUN-008, RUN-009, RUN-010, RUN-016): two *rigs* that stand a
parents → child flow up on either the in-memory backend under a ``FakeClock`` or
the compose broker, both driving the real ``NATSMessenger`` with a real
``FlowRuntime`` ledger; the node classes a real ``videoflow.runtime.worker``
subprocess runs for the process-level primaries; and the small helpers every
oracle in ``test_run_joins.py`` / ``test_run_completion.py`` would otherwise
copy — publishing what a parent replica would publish (data, EOS, ABORT), reading
the ledger a worker left behind, draining a messenger on a thread.

Only plumbing lives here; every oracle stays in its case module next to the
assertion it decides. The one discipline this module enforces is cleanup on a
shared dev broker: a broker rig deletes the run it provisioned and the flow's
DLQ stream in ``close()``, by exact ownership, whatever happened before.

Why the ledger is a *file* store even under the memory backend: the EOS-7
completion barrier and the CTRL-4 group ledger engage only when
``FlowRuntime.durable_shared()`` reads back true, which a ``MemoryRuntimeStore``
never does (and must not — RFC 0006 §9). A ``FileRuntimeStore`` under the test's
``tmp_path`` is durable and shared across the processes of one host, so a
"replacement process" — a second messenger in the memory variants, a real
subprocess in the primaries — starts from the same facts.
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
import types
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

from _brokers import delete_dlq, delete_run, publish_raw, run_async, unique_ids

from videoflow.backends import faults
from videoflow.backends.capabilities import RELIABLE_WORK
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.messaging import MemoryMessagingBackend
from videoflow.backends.memory.runtime_store import FileRuntimeStore
from videoflow.backends.messaging import (
    KIND_DATA,
    KIND_EOS,
    SUBSCRIPTION_DATA,
    ChannelId,
    Delivery,
    Envelope,
    SubscriptionId,
)
from videoflow.backends.observation import ObservationLog
from videoflow.backends.outcomes import Accepted, Known, Observation, known, unknown
from videoflow.backends.runtime import FlowRuntime
from videoflow.core.compiler import NodeSpec
from videoflow.core.constants import BATCH
from videoflow.core.errors import SchemaError
from videoflow.core.node import ConsumerNode, Node, ProcessorNode, ProducerNode
from videoflow.messaging import topology
from videoflow.messaging.jetstream_backend import JetStreamMessagingBackend, channel_spec_for, subscription_spec_for
from videoflow.messaging.nats_messenger import NATSMessenger
from videoflow.wire.serialization import (
    MSG_TYPE_ABORT,
    MSG_TYPE_DATA,
    MSG_TYPE_EOS,
    decode_envelope,
    derive_message_id,
    encode_envelope,
)

HERE = pathlib.Path(__file__).parent
#: The messenger's receive poll while a rig is in use — applied to every case by
#: the conftest's ``_fast_messenger_poll`` (the module default is 1 s).
MESSENGER_POLL_SECONDS = 0.05
#: The historical drain rule's quiescence window (EOS-3), which EOS-7 replaces.
#: The rule's real value was 500 ms; the completion cases hand this one to their
#: messengers and then wait three of it in real time to prove the rule is gone,
#: so it is what the wait costs, not what the rule was.
HISTORICAL_QUIESCENCE_MS = 100


class SimulatedCrash(Exception):
    '''The in-process stand-in for a worker dying: raised from a barrier, never caught below the test.'''


# -- node classes the worker subprocesses run ---------------------------------------------

class StubNode(Node):
    '''The minimum a messenger needs from a node: a real ``Node`` with a name.'''
    def __init__(self, name : str) -> None:
        super().__init__(name = name)

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None


class CountingProducer(ProducerNode):
    '''Yields ``{'r': replica, 'n': 0}`` … ``{'r': replica, 'n': count - 1}`` and ends: a finite replica of a parent.'''
    def __init__(self, count : int, name : Optional[str] = None) -> None:
        self._count = count
        self._i = 0
        super().__init__(name = name)

    def next(self, ctx : Any = None) -> Any:
        if self._i >= self._count:
            raise StopIteration
        self._i += 1
        return {'r': ctx.replica_id if ctx is not None else 0, 'n': self._i - 1}


class FailingProducer(ProducerNode):
    '''Yields ``count`` items, then dies of a poison error — terminal by nature, so the worker publishes ABORT.'''
    def __init__(self, count : int, name : Optional[str] = None) -> None:
        self._count = count
        self._i = 0
        super().__init__(name = name)

    def next(self, ctx : Any = None) -> Any:
        if self._i >= self._count:
            raise SchemaError('injected terminal failure in replica', remedy = 'fix the source')
        self._i += 1
        return {'r': ctx.replica_id if ctx is not None else 0, 'n': self._i - 1}


class PairJoin(ProcessorNode):
    '''A two-parent join whose result carries both halves.'''
    def process(self, a : Any, b : Any) -> Any:  # type: ignore[override] — one positional arg per parent
        return {'a': a, 'b': b}


class FileSink(ConsumerNode):
    '''
    Records every input by logical identity (``ctx.input_info``: producer, trace,
    seq per parent) as one JSON line, and its own close as another — the sink
    "instrumented by logical identity" the completion cases read back.
    '''
    def __init__(self, path : str, name : Optional[str] = None) -> None:
        self._path = path
        super().__init__(name = name)

    def consume(self, item : Any, ctx : Any = None) -> None:
        _append(self._path, {'event': 'input', 'item': item, 'inputs': ctx.input_info if ctx is not None else None,
                             'at': time.time()})

    def close(self) -> None:
        _append(self._path, {'event': 'closed', 'at': time.time()})


def _append(path : str, record : Dict[str, Any]) -> None:
    with open(path, 'a') as f:
        f.write(json.dumps(record, sort_keys = True, default = str) + '\n')
        f.flush()
        os.fsync(f.fileno())


def read_jsonl(path : str) -> List[Dict[str, Any]]:
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]


# -- specs and identities ---------------------------------------------------------------------

def spec(name : str, parents : Sequence[str], kind : str, has_children : bool, nb_tasks : int = 1,
         join_policy : Optional[dict] = None, node_class : str = 'videoflow.processors.basic.IdentityProcessor',
         params : Optional[dict] = None) -> NodeSpec:
    '''A real NodeSpec — provisioning reads name/parents/nb_tasks/partition_by/delivery.'''
    return NodeSpec(name = name, node_class = node_class, params = dict(params or {}), parents = list(parents),
                    kind = kind, has_children = has_children, nb_tasks = nb_tasks, device_type = 'cpu',
                    is_finite = True, join_policy = join_policy)


def data_id(flow_id : str, run_id : str, parent : str, trace : str, seq : int) -> str:
    return derive_message_id(flow_id, run_id, parent, trace, seq, MSG_TYPE_DATA)


def terminator_id(flow_id : str, run_id : str, parent : str, kind : str, replica_id : int, seq : int) -> str:
    '''The publication id ``publish_stop_signal`` / ``publish_abort`` derive for a replica's marker.'''
    trace = f'eos-r{replica_id}' if kind == KIND_EOS else f'abort-r{replica_id}'
    return derive_message_id(flow_id, run_id, parent, trace, seq, MSG_TYPE_EOS if kind == KIND_EOS else MSG_TYPE_ABORT)


def terminator_bytes(flow_id : str, run_id : str, parent : str, kind : str, replica_id : int, seq : int,
                     error : Optional[dict] = None) -> bytes:
    trace = f'eos-r{replica_id}' if kind == KIND_EOS else f'abort-r{replica_id}'
    return encode_envelope(parent, flow_id, run_id, trace, seq, MSG_TYPE_EOS if kind == KIND_EOS else MSG_TYPE_ABORT,
                           None, None, replica_id = replica_id, error = error)


def _declare_dead(messengers : Sequence[NATSMessenger], node : str, replica_id : int = 0) -> None:
    '''A replacement is being built: earlier messengers of the same identity forfeit their partition lease (see ``_runs2.declare_dead``).'''
    for previous in messengers:
        if previous._node.name == node and previous._replica_id == replica_id:
            previous._stop_lease_renewal(release = True)


def ledger(store_dir : str, flow_id : str, run_id : str, node : str, replica_id : int = 0, nb_tasks : int = 1,
           parent_replicas : Optional[Dict[str, int]] = None) -> FlowRuntime:
    '''A reader over the file ledger a worker (or a rig messenger) wrote.'''
    return FlowRuntime(FileRuntimeStore(store_dir), flow_id, run_id, node, replica_id, nb_tasks,
                       parent_replicas = parent_replicas)


def ledger_snapshot(store_dir : str) -> Dict[str, Any]:
    '''Every key of a file ledger with its document — the durable-state snapshot an evidence file carries.'''
    store = FileRuntimeStore(store_dir)
    out : Dict[str, Any] = {}
    for key, raw, version in store.scan(''):
        try:
            out[key] = {'version': version, 'doc': json.loads(raw.decode())}
        except ValueError:
            out[key] = {'version': version, 'raw': raw.hex()}
    for entry in sorted(os.listdir(store_dir)):
        if entry.endswith('.log'):
            log = entry[:-4].replace('%2F', '/')
            out[log] = {'entries': [e.decode(errors = 'replace') for e in store.log_entries(log)]}
    return out


# -- the in-process receive loop --------------------------------------------------------------

class Receiver(threading.Thread):
    '''
    Drains one messenger on its own thread — receive, record, settle — until the
    messenger reports every parent stopped (``terminal``), ``stop()`` is called,
    or an exception escapes (kept in ``error``, never swallowed: a crash raised
    from a barrier ends the loop exactly as a dying worker would).

    ``on_input(messenger, inputs)`` decides the settlement: ``True`` (or None)
    acks, ``False`` leaves the group held un-acked for the test to settle from its
    own thread later (what a node still inside ``process()`` looks like to the
    broker) — the loop keeps polling, so the EOS drain keeps being evaluated.
    '''
    def __init__(self, messenger : NATSMessenger,
                 on_input : Optional[Callable[[NATSMessenger, dict], Optional[bool]]] = None,
                 name : str = 'vf-conf-receiver') -> None:
        super().__init__(name = name, daemon = True)
        self.messenger = messenger
        self.inputs : List[dict] = []
        self.error : Optional[BaseException] = None
        self.terminal : Optional[dict] = None
        self.completed_at : Optional[float] = None
        self._on_input = on_input
        self._stop_requested = threading.Event()

    def run(self) -> None:
        try:
            while not self._stop_requested.is_set():
                inputs = self.messenger.receive_message()
                if all(v.get('is_stop_signal') for v in inputs.values()):
                    self.terminal = inputs
                    self.completed_at = time.monotonic()
                    return
                self.inputs.append(inputs)
                verdict = self._on_input(self.messenger, inputs) if self._on_input is not None else True
                if verdict is None or verdict:
                    self.messenger.ack_inputs()
        except BaseException as e:  # noqa: BLE001 — surfaced to the test thread
            self.error = e

    def stop(self) -> None:
        self._stop_requested.set()
        self.messenger.quiesce()

    @property
    def stopped_cleanly(self) -> bool:
        return self.terminal is not None and not any(v.get('is_abort') for v in self.terminal.values())

    @property
    def aborted(self) -> bool:
        return self.terminal is not None and any(v.get('is_abort') for v in self.terminal.values())


def wait_until(predicate : Callable[[], bool], timeout : float, interval : float = 0.02) -> bool:
    '''Poll ``predicate`` until it holds or ``timeout`` (real seconds) elapses; returns its final value.'''
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return bool(predicate())


def write_evidence(directory : pathlib.Path, name : str, payload : Any) -> pathlib.Path:
    path = directory / name
    path.write_text(json.dumps(payload, indent = 2, default = str, sort_keys = True))
    return path


# -- rigs ---------------------------------------------------------------------------------------

@dataclass
class Retained:
    sequence : int
    publication_id : str
    decoded : Dict[str, Any] = field(default_factory = dict)


class ModelRig:
    '''
    A flow on the in-memory backend under a ``FakeClock``. Model time advances
    ``auto_advance`` seconds on every empty receive poll of a messenger (so leases
    lapse and NAK delays pass without a hand on the clock); set it to 0 to freeze
    time while a test holds a delivery, or construct the rig frozen and advance
    explicitly (the controllable clock of RUN-006). Every backend event (deliver,
    settle, publish, reclaim) lands in ``log``, the independent attempt trace.
    '''
    name = 'memory'

    def __init__(self, auto_advance : float = 0.02, prefix : str = 'run') -> None:
        self.clock = FakeClock()
        self.log = ObservationLog(clock = self.clock.monotonic)
        self.backend = MemoryMessagingBackend(self.clock, log = self.log)
        self.flow_id, self.run_id = unique_ids(prefix)
        self.nats_url = 'memory://model'
        self.auto_advance = auto_advance
        self._messengers : List[NATSMessenger] = []
        rig = self

        def receive_any(self_ : MemoryMessagingBackend, subscriptions : Sequence[SubscriptionId], item_credit : int,
                        byte_credit : int, deadline : float) -> List[tuple[SubscriptionId, Delivery]]:
            while True:
                out : List[tuple[SubscriptionId, Delivery]] = []
                for subscription in subscriptions:
                    for delivery in self_.receive(subscription, item_credit, byte_credit, 0.0):
                        out.append((subscription, delivery))
                if out or time.monotonic() >= deadline:
                    return out
                if rig.auto_advance > 0:
                    rig.clock.advance(rig.auto_advance)
                time.sleep(0.001)
        self.backend.receive_any = types.MethodType(receive_any, self.backend)  # type: ignore[method-assign]

    def channel(self, node : str) -> ChannelId:
        return ChannelId(self.flow_id, self.run_id, node)

    def provision(self, specs : Sequence[NodeSpec], ack_wait : int = 2, max_retries : int = 3,
                  ledger_budget : bool = True, credit : Optional[int] = None) -> None:
        '''What ``provision_flow`` does before any worker starts: every channel, then one data subscription per edge.'''
        for s in specs:
            self.backend.ensure_channel(channel_spec_for(self.flow_id, self.run_id, s.name, BATCH, RELIABLE_WORK),
                                        'provision')
        for s in specs:
            max_deliver = topology.max_deliver_for(BATCH, max_retries, s.delivery, ledger_budget = ledger_budget)
            for parent in s.parents:
                sub = SubscriptionId(self.channel(parent), s.name, None, SUBSCRIPTION_DATA)
                self.backend.ensure_subscription(
                    subscription_spec_for(sub, ack_wait, max_deliver,
                                          credit if credit is not None else topology.consumer_credit(s.nb_tasks, False)),
                    'provision')

    def messenger(self, name : str, parents : Sequence[str], runtime : Optional[FlowRuntime] = None,
                  **kwargs : Any) -> NATSMessenger:
        kwargs.setdefault('ack_wait', 2)
        _declare_dead(self._messengers, name, int(kwargs.get('replica_id', 0)))
        m = NATSMessenger(StubNode(name), list(parents), self.nats_url, self.flow_id, BATCH, self.run_id,
                          backend = self.backend, runtime = runtime, **kwargs)
        self._messengers.append(m)
        return m

    def kill(self, messenger : NATSMessenger) -> None:
        '''The process dies: nothing it holds is settled or handed back; its leases lapse on the model clock.'''
        messenger._closing.set()
        messenger._termination_event.set()
        messenger._stop_lease_renewal(release = True)       # the partition lease lapses with the process

    def publish_data(self, parent : str, trace : str, seq : int, payload : Any, replica_id : int = 0,
                     msg_id : Optional[str] = None, event_ts : Optional[float] = None) -> str:
        buf = encode_envelope(parent, self.flow_id, self.run_id, trace, seq, MSG_TYPE_DATA, None, payload,
                              replica_id = replica_id, event_ts = event_ts)
        mid = msg_id or data_id(self.flow_id, self.run_id, parent, trace, seq)
        return self._publish(parent, buf, mid, KIND_DATA)

    def publish_terminator(self, parent : str, kind : str, replica_id : int, seq : int,
                           error : Optional[dict] = None, msg_id : Optional[str] = None) -> str:
        buf = terminator_bytes(self.flow_id, self.run_id, parent, kind, replica_id, seq, error)
        mid = msg_id or terminator_id(self.flow_id, self.run_id, parent, kind, replica_id, seq)
        return self._publish(parent, buf, mid, kind)

    def _publish(self, node : str, buf : bytes, mid : str, kind : str) -> str:
        envelope = Envelope(self.channel(node), mid, {'Nats-Msg-Id': mid}, buf, len(buf), mid, None, None,
                            None, None, 4, (), kind)
        outcome = self.backend.publish(envelope, 0.0)
        assert isinstance(outcome, Accepted), outcome
        return mid

    def consumer_state(self, child : str, parent : str) -> Observation[tuple[int, int]]:
        '''``(pending, leased)`` of the child's data subscription on the parent, or Unknown.'''
        sub = SubscriptionId(self.channel(parent), child, None, SUBSCRIPTION_DATA)
        observed = self.backend.observe_subscription(sub)
        if not isinstance(observed, Known):
            return observed
        return known((observed.value.available, observed.value.leased), observed.generation)

    def retained(self, node : str) -> List[Retained]:
        '''The data envelopes the model still holds on ``node``'s channel, in sequence order.'''
        channel = self.backend._channels.get(self.channel(node))
        if channel is None:
            return []
        with self.backend._lock:
            stored = [(m.sequence, m.envelope) for m in channel.messages if m.envelope.kind == KIND_DATA]
        return [Retained(seq, e.publication_id, decode_envelope(e.body, resolve_blobs = False)) for seq, e in stored]

    def deliveries(self, consumer : str) -> List[Dict[str, Any]]:
        '''The attempt trace: every delivery to ``consumer`` as ``{op_id, attempt, generation}``, in order.'''
        return [dict(e.fields) for e in self.log.events('deliver', consumer = consumer)]

    def settlements(self, op_id : Optional[str] = None) -> List[Dict[str, Any]]:
        return [dict(e.fields) for e in self.log.events('settle') if op_id is None or e.fields.get('op_id') == op_id]

    def wait(self, seconds : float) -> None:
        self.clock.advance(seconds)

    def until(self, condition : Callable[[], bool], timeout : float, step : float = 0.02) -> bool:
        return wait_until(condition, timeout, step)

    def close(self) -> None:
        for m in self._messengers:
            with contextlib.suppress(Exception):
                m.quiesce()
                m.close()
        self._messengers = []
        self.backend.close(self.backend.channel_ids(), '')


class BrokerRig:
    '''
    A flow on the compose broker: provisioned with ``provision_flow_sync`` (as the
    init Job does), every messenger on its own connection (as every worker is),
    retained messages read back by sequence with direct gets — no consumer is ever
    created for inspection, so nothing here perturbs interest retention.
    ``close`` deletes the run it provisioned and the flow's DLQ, by exact ownership.
    '''
    name = 'jetstream'

    def __init__(self, nats_url : str, prefix : str = 'run') -> None:
        self.nats_url = nats_url
        self.flow_id, self.run_id = unique_ids(prefix)
        self.clock = None
        self._messengers : List[NATSMessenger] = []
        self._backends : List[JetStreamMessagingBackend] = []

    def channel(self, node : str) -> ChannelId:
        return ChannelId(self.flow_id, self.run_id, node)

    def provision(self, specs : Sequence[NodeSpec], ack_wait : int = 2, max_retries : int = 3,
                  ledger_budget : bool = True, credit : Optional[int] = None) -> None:
        topology.provision_flow_sync(self.nats_url, list(specs), self.flow_id, self.run_id, BATCH,
                                     max_retries = max_retries, ack_wait = ack_wait, max_ack_pending = credit,
                                     ledger_budget = ledger_budget, timeout = 30.0)

    def messenger(self, name : str, parents : Sequence[str], runtime : Optional[FlowRuntime] = None,
                  keepalive : bool = True, **kwargs : Any) -> NATSMessenger:
        kwargs.setdefault('ack_wait', 2)
        _declare_dead(self._messengers, name, int(kwargs.get('replica_id', 0)))
        backend = JetStreamMessagingBackend(self.nats_url, self.flow_id, self.run_id, BATCH, keepalive = keepalive)
        self._backends.append(backend)
        m = NATSMessenger(StubNode(name), list(parents), self.nats_url, self.flow_id, BATCH, self.run_id,
                          backend = backend, runtime = runtime, **kwargs)
        self._messengers.append(m)
        return m

    def kill(self, messenger : NATSMessenger) -> None:
        '''The process dies: the connection drops without a NAK; its leases lapse on the broker's clock.'''
        messenger._closing.set()
        messenger._stop_lease_renewal(release = True)       # the partition lease lapses with the process
        messenger._termination_event.set()
        backend = messenger._backend
        assert isinstance(backend, JetStreamMessagingBackend)
        backend.shutdown(hand_back = False)

    def publish_data(self, parent : str, trace : str, seq : int, payload : Any, replica_id : int = 0,
                     msg_id : Optional[str] = None, event_ts : Optional[float] = None) -> str:
        buf = encode_envelope(parent, self.flow_id, self.run_id, trace, seq, MSG_TYPE_DATA, None, payload,
                              replica_id = replica_id, event_ts = event_ts)
        mid = msg_id or data_id(self.flow_id, self.run_id, parent, trace, seq)
        publish_raw(self.nats_url, topology.subject_for(self.flow_id, self.run_id, parent), buf,
                    headers = {'Nats-Msg-Id': mid})
        return mid

    def publish_terminator(self, parent : str, kind : str, replica_id : int, seq : int,
                           error : Optional[dict] = None, msg_id : Optional[str] = None) -> str:
        buf = terminator_bytes(self.flow_id, self.run_id, parent, kind, replica_id, seq, error)
        mid = msg_id or terminator_id(self.flow_id, self.run_id, parent, kind, replica_id, seq)
        publish_raw(self.nats_url, topology.eos_subject_for(self.flow_id, self.run_id, parent), buf,
                    headers = {'Nats-Msg-Id': mid})
        return mid

    def consumer_state(self, child : str, parent : str) -> Observation[tuple[int, int]]:
        import nats  # optional dep (distributed extras)
        durable = topology.durable_name_for(child, parent)
        stream = topology.stream_name_for(self.flow_id, self.run_id, parent)

        async def _go() -> Observation[tuple[int, int]]:
            nc = await nats.connect(self.nats_url)
            try:
                info = await nc.jetstream().consumer_info(stream, durable)
                return known((int(info.num_pending), int(info.num_ack_pending)))
            finally:
                await nc.drain()
        try:
            return run_async(_go)
        except Exception as e:  # noqa: BLE001 — reported, never zero
            return unknown('api', f'{type(e).__name__}: {e}')

    def consumer_info(self, child : str, parent : str) -> Dict[str, Any]:
        '''``num_pending``, ``num_ack_pending``, ``num_redelivered``, ``ack_floor`` of the child's durable on the parent.'''
        import nats  # optional dep (distributed extras)
        durable = topology.durable_name_for(child, parent)
        stream = topology.stream_name_for(self.flow_id, self.run_id, parent)

        async def _go() -> Dict[str, Any]:
            nc = await nats.connect(self.nats_url)
            try:
                info = await nc.jetstream().consumer_info(stream, durable)
                return {'num_pending': int(info.num_pending), 'num_ack_pending': int(info.num_ack_pending),
                        'num_redelivered': int(info.num_redelivered or 0),
                        'ack_floor': int(info.ack_floor.stream_seq or 0) if info.ack_floor is not None else 0,
                        'max_ack_pending': info.config.max_ack_pending, 'max_deliver': info.config.max_deliver}
            finally:
                await nc.drain()
        return run_async(_go)

    def retained(self, node : str) -> List[Retained]:
        '''The data envelopes the stream still holds for ``node``, read by sequence with direct gets.'''
        import nats  # optional dep (distributed extras)
        stream = topology.stream_name_for(self.flow_id, self.run_id, node)
        subject = topology.subject_for(self.flow_id, self.run_id, node)

        async def _go() -> List[Retained]:
            nc = await nats.connect(self.nats_url)
            out : List[Retained] = []
            try:
                js = nc.jetstream()
                try:
                    state = (await js.stream_info(stream)).state
                except Exception:  # noqa: BLE001 — no stream: nothing retained
                    return out
                for seq in range(state.first_seq, state.last_seq + 1):
                    try:
                        raw = await js.get_msg(stream, seq)
                    except Exception:  # noqa: BLE001 — a deleted (acked, reclaimed) sequence
                        continue
                    if (raw.subject or '') != subject:
                        continue
                    out.append(Retained(seq, (raw.headers or {}).get('Nats-Msg-Id', ''),
                                        decode_envelope(raw.data or b'', resolve_blobs = False)))
            finally:
                await nc.close()
            return out
        return run_async(_go)

    def stop_flow(self) -> None:
        '''Publish the run's flow-wide stop on its control subject (what ``videoflow`` does on abort).'''
        import nats  # optional dep (distributed extras)

        async def _go() -> None:
            nc = await nats.connect(self.nats_url)
            try:
                await nc.publish(topology.control_subject_for(self.flow_id, self.run_id), b'stop')
                await nc.flush()
            finally:
                await nc.close()
        run_async(_go)

    def wait(self, seconds : float) -> None:
        time.sleep(seconds)

    def until(self, condition : Callable[[], bool], timeout : float, step : float = 0.05) -> bool:
        return wait_until(condition, timeout, step)

    def close(self) -> None:
        for m in self._messengers:
            with contextlib.suppress(Exception):
                m.quiesce()
                m.close()
        self._messengers = []
        for backend in self._backends:
            with contextlib.suppress(Exception):
                backend.shutdown()
        self._backends = []
        with contextlib.suppress(Exception):
            delete_run(self.nats_url, self.flow_id, self.run_id)
        with contextlib.suppress(Exception):
            delete_dlq(self.nats_url, self.flow_id)


# -- real worker subprocesses -----------------------------------------------------------------

@dataclass
class WorkerOutcome:
    exit_code : int
    took_s : float
    stderr_tail : str
    termination : Optional[Dict[str, Any]] = None


def worker_env(name : str, kind : str, node_class : str, nats_url : str, flow_id : str, run_id : str,
               parents : Sequence[str] = (), has_children : bool = False, params : Optional[dict] = None,
               replica_id : int = 0, nb_tasks : int = 1, parent_replicas : Optional[Sequence[int]] = None,
               store_url : Optional[str] = None, ack_wait : int = 2, max_retries : int = 3,
               eos_quiescence_ms : int = HISTORICAL_QUIESCENCE_MS, join_policy : Optional[dict] = None,
               schedule : Optional[faults.FaultSchedule] = None, termination_log : Optional[str] = None,
               progress_timeout : float = 120.0) -> Dict[str, str]:
    '''
    The environment ``videoflow.runtime.worker`` reads (its module docstring is the
    contract): exactly what ``engines.local._worker_env`` renders for one replica,
    plus the RFC 0006 rows (``VF_RUNTIME_STORE_URL``, ``VF_PARENT_REPLICAS``,
    and, for a fault case, the schedule (``ENV-16``/``ENV-17``).
    '''
    env = dict(os.environ)
    path = [str(HERE), str(HERE.parent)]
    if env.get('PYTHONPATH'):
        path.append(env['PYTHONPATH'])
    env['PYTHONPATH'] = os.pathsep.join(path)
    env.update({
        'VF_NODE_CLASS': node_class,
        'VF_NODE_PARAMS_JSON': json.dumps(params or {}),
        'VF_NODE_KIND': kind,
        'VF_NODE_NAME': name,
        'VF_PARENT_NAMES': ','.join(parents),
        'VF_HAS_CHILDREN': '1' if has_children else '0',
        'VF_NATS_URL': nats_url,
        'VF_FLOW_ID': flow_id,
        'VF_FLOW_TYPE': BATCH,
        'VF_RUN_ID': run_id,
        'VF_REPLICA_ID': str(replica_id),
        'VF_NB_TASKS': str(nb_tasks),
        'VF_ACK_WAIT_SECONDS': str(ack_wait),
        'VF_MAX_RETRIES': str(max_retries),
        'VF_EOS_QUIESCENCE_MS': str(eos_quiescence_ms),
        'VF_HEALTH_PORT': '0',
        'VF_PROGRESS_TIMEOUT_SECONDS': str(progress_timeout),
        'VF_PARTITION_LEASE_SECONDS': '2',   # a crashed worker's lease lapses fast; its replacement binds in seconds
    })
    for stale in ('VF_FAULT_SCHEDULE_JSON', 'VF_FAULT_MARKER_DIR', 'VF_JOIN_POLICY_JSON', 'VF_PARENT_REPLICAS',
                  'VF_RUNTIME_STORE_URL', 'VF_TERMINATION_LOG', 'VF_BLOB_REDIS_URL', 'VF_PARTITION_BY'):
        env.pop(stale, None)
    if parent_replicas:
        env['VF_PARENT_REPLICAS'] = ','.join(str(n) for n in parent_replicas)
    if store_url:
        env['VF_RUNTIME_STORE_URL'] = store_url
    if join_policy:
        env['VF_JOIN_POLICY_JSON'] = json.dumps(join_policy)
    if termination_log:
        env['VF_TERMINATION_LOG'] = termination_log
    if schedule is not None:
        env.update(schedule.to_env())
    return env


def spawn_worker(env : Dict[str, str], log_path : str) -> subprocess.Popen:
    '''Start ``videoflow.runtime.worker`` with ``env``; its stderr goes to ``log_path`` (the evidence log).'''
    log = open(log_path, 'ab')
    return subprocess.Popen([sys.executable, '-m', 'videoflow.runtime.worker'], env = env, stdout = log,
                            stderr = subprocess.STDOUT, cwd = str(HERE.parent.parent))


def wait_worker(proc : subprocess.Popen, timeout : float, log_path : str,
                termination_log : Optional[str] = None) -> WorkerOutcome:
    '''Wait for a worker; one that outlives ``timeout`` is killed and reported with exit code -9.'''
    started = time.monotonic()
    try:
        code = proc.wait(timeout = timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout = 10)
        code = -9
    tail = ''
    if os.path.exists(log_path):
        with open(log_path, 'rb') as f:
            tail = f.read()[-3000:].decode(errors = 'replace')
    termination = None
    if termination_log and os.path.exists(termination_log):
        with open(termination_log) as f:
            text = f.read().strip()
        if text:
            termination = json.loads(text)
    return WorkerOutcome(code, time.monotonic() - started, tail, termination)


def run_worker(env : Dict[str, str], log_path : str, timeout : float,
               termination_log : Optional[str] = None) -> WorkerOutcome:
    return wait_worker(spawn_worker(env, log_path), timeout, log_path, termination_log)
