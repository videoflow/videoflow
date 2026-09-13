'''
Shared plumbing for the PAY conformance cases: deterministic frames, a
read-attributing ``PayloadStore`` proxy, and two *rigs* that stand a
parent → children flow up on either the in-memory backends (``FakeClock``, no
infrastructure) or the compose broker (JetStream + Redis). Both rigs drive the
real ``NATSMessenger`` — the composed messenger is the component under test in
most PAY cases, and the memory rig lets its obligation, dead-letter and replay
decisions be exercised deterministically wherever the catalog's level allows.

Only plumbing lives here; every oracle stays in the case module next to the
assertion it decides. The one discipline this module enforces is cleanup on a
shared dev broker: a JetStream rig tears down every run it provisioned and the
flow's DLQ stream in ``close()``, by exact ownership, whatever happened.
'''
from __future__ import absolute_import, division, print_function

import contextlib
import dataclasses
import hashlib
import json
import pathlib
import threading
import time
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence

import numpy as np
from _brokers import delete_dlq, delete_run, run_async, sweep_refs, unique_ids

from videoflow.backends.capabilities import LIVE_LATEST, RELIABLE_WORK, PayloadCapabilities
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.messaging import MemoryMessagingBackend, make_envelope
from videoflow.backends.memory.payload import MemoryPayloadStore, StaticLedger
from videoflow.backends.messaging import (
    KIND_ABORT,
    KIND_DATA,
    KIND_DLQ,
    KIND_EOS,
    SUBSCRIPTION_DATA,
    ChannelId,
    Delivery,
    SubscriptionId,
)
from videoflow.backends.outcomes import Known, Observation
from videoflow.backends.payload import (
    Corrupt,
    DurableReceipt,
    ImmutablePayloadRef,
    Missing,
    ObligationLedger,
    PayloadBytes,
    PayloadStore,
    ReadOutcome,
    ReclamationObservation,
    ReleaseReceipt,
    RetentionContract,
    TransientFailure,
)
from videoflow.core.compiler import NodeSpec
from videoflow.core.constants import BATCH, REALTIME
from videoflow.messaging import topology
from videoflow.messaging.jetstream_backend import channel_spec_for, subscription_spec_for
from videoflow.messaging.nats_messenger import NATSMessenger
from videoflow.messaging.topology import consumer_credit, max_deliver_for, provision_flow_sync

# -- fixtures: frames and specs --------------------------------------------------------

#: The catalog's specimen sizes: an exact million-byte frame, and the offload threshold.
MEGAFRAME_SHAPE = (1000, 1000)
INLINE_THRESHOLD = 512 * 1024


def frame_bytes(size : int, seed : int = 0) -> bytes:
    '''``size`` deterministic, incompressible bytes.'''
    return np.random.default_rng(seed).integers(0, 256, size, dtype = np.uint8).tobytes()


def frame_array(shape : Sequence[int], seed : int = 0) -> np.ndarray:
    '''A deterministic uint8 array of ``shape`` — the decoded-frame specimen.'''
    return np.random.default_rng(seed).integers(0, 256, tuple(shape), dtype = np.uint8)


def sha256(data : bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class StubNode:
    '''The minimum a messenger needs from a node: a name.'''
    def __init__(self, name : str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None


def spec(name : str, parents : Sequence[str], kind : str, has_children : bool, nb_tasks : int = 1,
         partition_by : Optional[str] = None, delivery : Optional[dict] = None) -> NodeSpec:
    '''A real NodeSpec — provisioning reads name/parents/nb_tasks/partition_by/delivery.'''
    return NodeSpec(name = name, node_class = 'videoflow.processors.basic.IdentityProcessor',
                    params = {}, parents = list(parents), kind = kind, has_children = has_children,
                    nb_tasks = nb_tasks, device_type = 'cpu', is_finite = True,
                    partition_by = partition_by, delivery = delivery)


def wait_until(predicate : Callable[[], bool], timeout : float = 10.0, interval : float = 0.05) -> bool:
    '''Poll ``predicate`` until it holds or ``timeout`` elapses; returns its final value.'''
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


# -- a read-attributing store proxy ----------------------------------------------------

@dataclasses.dataclass(frozen = True)
class ReadRecord:
    reader : str
    key : str
    size : int
    outcome : str
    at : float
    thread : str


class ReadLog:
    '''Every read through a ``CountingStore``, attributed to the reader that made it, plus the peak in-flight count.'''
    def __init__(self) -> None:
        self._records : List[ReadRecord] = []
        self._lock = threading.Lock()
        self.inflight = 0
        self.max_inflight = 0

    def _start(self) -> None:
        with self._lock:
            self.inflight += 1
            self.max_inflight = max(self.max_inflight, self.inflight)

    def _finish(self, record : ReadRecord) -> None:
        with self._lock:
            self.inflight -= 1
            self._records.append(record)

    def records(self, reader : Optional[str] = None) -> List[ReadRecord]:
        with self._lock:
            return [r for r in self._records if reader is None or r.reader == reader]

    def count(self, reader : Optional[str] = None, outcome : Optional[str] = None) -> int:
        return sum(1 for r in self.records(reader) if outcome is None or r.outcome == outcome)

    def bytes(self, reader : Optional[str] = None, outcome : str = 'bytes') -> int:
        return sum(r.size for r in self.records(reader) if r.outcome == outcome)

    def by_reader(self) -> Dict[str, int]:
        out : Dict[str, int] = {}
        for r in self.records():
            out[r.reader] = out.get(r.reader, 0) + 1
        return out


class CountingStore(PayloadStore):
    '''
    A ``PayloadStore`` that delegates everything and attributes every read to one
    reader label — one instance per replica, so GET traffic is measured by who
    made it (PAY-018) — and can refuse reads outright (``blocked``), the model of
    a replica whose store connection is down.
    '''
    def __init__(self, inner : PayloadStore, reader : str, log : ReadLog,
                 blocked : Optional[Callable[[ImmutablePayloadRef], Optional[BaseException]]] = None) -> None:
        self._inner = inner
        self._reader = reader
        self._log = log
        self._blocked = blocked

    @property
    def inner(self) -> PayloadStore:
        return self._inner

    def capabilities(self) -> PayloadCapabilities:
        return self._inner.capabilities()

    def put(self, data : bytes, content_id : str, contract : RetentionContract) -> ImmutablePayloadRef:
        return self._inner.put(data, content_id, contract)

    def acquire_obligation(self, ref : ImmutablePayloadRef, obligation_id : str, deadline : float) -> DurableReceipt:
        return self._inner.acquire_obligation(ref, obligation_id, deadline)

    def renew_obligation(self, ref : ImmutablePayloadRef, obligation_id : str, deadline : float) -> DurableReceipt:
        return self._inner.renew_obligation(ref, obligation_id, deadline)

    def read(self, ref : ImmutablePayloadRef, reader : Optional[str] = None) -> ReadOutcome:
        self._log._start()
        outcome_name = 'raised'
        size = 0
        try:
            refused = self._blocked(ref) if self._blocked is not None else None
            if refused is not None:
                outcome_name = 'blocked'
                raise refused
            outcome : Any = self._inner.read(ref, reader = self._reader)   # type: ignore[call-arg]
            if isinstance(outcome, PayloadBytes):
                outcome_name, size = 'bytes', len(outcome.data)
            elif isinstance(outcome, TransientFailure):
                outcome_name = 'transient'
            elif isinstance(outcome, Missing):
                outcome_name = 'missing'
            elif isinstance(outcome, Corrupt):
                outcome_name = 'corrupt'
            return outcome
        finally:
            self._log._finish(ReadRecord(self._reader, ref.key, size, outcome_name, time.monotonic(),
                                         threading.current_thread().name))

    def release_obligation(self, ref : ImmutablePayloadRef, obligation_id : str,
                           completion_receipt : str) -> ReleaseReceipt:
        return self._inner.release_obligation(ref, obligation_id, completion_receipt)

    def reconcile(self, ledger : ObligationLedger, operation_id : str) -> ReclamationObservation:
        return self._inner.reconcile(ledger, operation_id)

    def inventory(self) -> Observation[tuple[ImmutablePayloadRef, ...]]:
        return self._inner.inventory()

    def ref_for_key(self, key : str) -> ImmutablePayloadRef:
        return self._inner.ref_for_key(key)


class KeyRecordingStore(CountingStore):
    '''A ``CountingStore`` that also remembers every key its puts created, in order — the object inventory a publisher made.'''
    def __init__(self, inner : PayloadStore, reader : str = 'publisher', log : Optional[ReadLog] = None) -> None:
        super().__init__(inner, reader, log or ReadLog())
        self.keys : List[str] = []

    def put(self, data : bytes, content_id : str, contract : RetentionContract) -> ImmutablePayloadRef:
        ref = super().put(data, content_id, contract)
        if ref.key not in self.keys:
            self.keys.append(ref.key)
        return ref


def isolated_redis_db(url : str, db : int) -> str:
    '''
    The same server, another logical database: ``SCAN``-driven operations
    (reconcile, inventory) see only this test's keys, never another suite's on the
    shared dev server. The db index replaces whatever the fixture URL selected.
    '''
    base, _, _rest = url.partition('?')
    scheme_and_host, _, _ = base.rpartition('/') if base.count('/') >= 3 else (base, '', '')
    return f'{scheme_and_host}/{db}'


def raw_store(store : PayloadStore) -> PayloadStore:
    '''The store beneath any ``CountingStore`` layers.'''
    while isinstance(store, CountingStore):
        store = store.inner
    return store


def obligations_of(store : PayloadStore, key : str) -> set:
    '''The obligation ids a store currently holds for ``key`` — memory or Redis, through their own records.'''
    inner = raw_store(store)
    if isinstance(inner, MemoryPayloadStore):
        return set(inner.obligations(key))
    from videoflow.wire.redis_payload_store import obligation_key  # optional dep: only reached with a Redis store
    return {m.decode() if isinstance(m, bytes) else str(m) for m in inner.client.smembers(obligation_key(key))}


def object_exists(store : PayloadStore, key : str) -> bool:
    inner = raw_store(store)
    if isinstance(inner, MemoryPayloadStore):
        return isinstance(inner.read(inner.ref_for_key(key)), PayloadBytes) if inner.ref_for_key(key).generation else False
    return bool(inner.client.exists(key))


def ledger(required : Dict[str, Sequence[str]]) -> StaticLedger:
    return StaticLedger({k: tuple(v) for k, v in required.items()})


# -- rigs --------------------------------------------------------------------------------

class SubjectFilteringMemoryBackend(MemoryMessagingBackend):
    '''
    The memory model delivers every envelope of a channel to every subscription;
    JetStream binds a durable to one subject, so a node's data durable never sees
    a terminator and its EOS durable never sees data. Without that filter the
    messenger's EOS subscription would take the first data message for an
    end-of-stream and declare the parent finished as soon as its data durable
    went quiet. This subclass mirrors the filter: a subscription receives only
    envelopes of its kind; the rest are settled on its behalf, off the barriers.
    '''
    def receive(self, subscription : SubscriptionId, item_credit : int, byte_credit : int,
                deadline : float) -> list:
        wanted = {KIND_DATA} if subscription.kind == SUBSCRIPTION_DATA else {KIND_EOS, KIND_ABORT}
        kept : List[Delivery] = []
        for delivery in super().receive(subscription, item_credit, byte_credit, deadline):
            with self._lock:
                stored, view = self._find(delivery.token)
                if stored is not None and stored.envelope.kind not in wanted:
                    if view is not None:
                        view.settled = 'completed'
                        view.leased_until = None
                    self._reclaim_if_settled(self._channels[subscription.channel], stored)
                    continue
            kept.append(delivery)
        return kept


@dataclasses.dataclass(frozen = True)
class DeadLetter:
    subject : str
    headers : Dict[str, str]
    body : bytes


@dataclasses.dataclass(frozen = True)
class ConsumerState:
    pending : int
    unacked : int
    redelivered : int


class MemoryRig:
    '''
    A flow on the in-memory backends: one ``MemoryMessagingBackend`` and one
    payload store shared by every messenger, under one ``FakeClock``. Dead letters
    land on the failing node's own channel with kind ``dlq`` (the messenger
    addresses them by origin node), which is where ``dead_letters`` reads them.
    '''
    def __init__(self, flow_type : str = BATCH, clock : Optional[FakeClock] = None,
                 store : Optional[PayloadStore] = None, max_payload_bytes : int = 1 << 20,
                 specs : Optional[Sequence[NodeSpec]] = None, ack_wait : int = 30, max_retries : int = 3,
                 **backend_kwargs : Any) -> None:
        self.flow_type = flow_type
        self.clock = clock or FakeClock()
        self.backend = SubjectFilteringMemoryBackend(self.clock, max_payload_bytes = max_payload_bytes,
                                                     **backend_kwargs)
        self.store : PayloadStore = store if store is not None else MemoryPayloadStore(self.clock)
        self.flow_id, self.run_id = unique_ids('pay')
        self.nats_url = 'memory://'
        self._messengers : List[NATSMessenger] = []
        if specs is not None:
            self.provision(specs, ack_wait = ack_wait, max_retries = max_retries)

    def provision(self, specs : Sequence[NodeSpec], run_id : Optional[str] = None, ack_wait : int = 30,
                  max_retries : int = 3) -> None:
        '''
        What ``provision_flow_sync`` does for JetStream, on the model: every channel
        and every child's data subscription exists before anything is published,
        so interest retention keeps a message for a reader whose worker starts
        late — the one thing a rig built messenger-by-messenger would get wrong.
        '''
        run = run_id or self.run_id
        profile = LIVE_LATEST if self.flow_type == REALTIME else RELIABLE_WORK
        for s in specs:
            self.backend.ensure_channel(channel_spec_for(self.flow_id, run, s.name, self.flow_type, profile),
                                        'provision')
        for s in specs:
            partitioned = bool(s.partition_by) and s.nb_tasks > 1
            for parent in s.parents:
                channel = ChannelId(self.flow_id, run, parent)
                partitions = range(s.nb_tasks) if partitioned else [None]
                for partition in partitions:
                    sub = SubscriptionId(channel, s.name, partition, SUBSCRIPTION_DATA)
                    self.backend.ensure_subscription(
                        subscription_spec_for(sub, ack_wait, max_deliver_for(self.flow_type, max_retries),
                                              consumer_credit(s.nb_tasks, partitioned)), 'provision')

    def messenger(self, name : str, parents : Sequence[str], run_id : Optional[str] = None,
                  store : Optional[PayloadStore] = None, **kwargs : Any) -> NATSMessenger:
        m = NATSMessenger(StubNode(name), list(parents), self.nats_url, self.flow_id, self.flow_type,  # type: ignore[arg-type]
                          run_id or self.run_id, backend = self.backend,
                          payload_store = self.store if store is None else store, **kwargs)
        self._messengers.append(m)
        return m

    def channel(self, node : str, run_id : Optional[str] = None) -> ChannelId:
        return ChannelId(self.flow_id, run_id or self.run_id, node)

    def dead_letters(self, node : Optional[str] = None, run_id : Optional[str] = None) -> List[DeadLetter]:
        out : List[DeadLetter] = []
        for cid in self.backend.channel_ids():
            if cid.flow_id != self.flow_id or (run_id is not None and cid.run_id != run_id):
                continue
            if node is not None and cid.node != node:
                continue
            for e in self.backend.stored(cid):
                if e.kind == KIND_DLQ:
                    out.append(DeadLetter(topology.dlq_subject_for(cid.flow_id, cid.run_id, cid.node),
                                          dict(e.headers), e.body))
        return out

    def retained(self, node : str, run_id : Optional[str] = None) -> List[bytes]:
        '''The data envelopes the broker currently retains on ``node``'s channel, in sequence order.'''
        return [body for _seq, body in self.retained_with_seq(node, run_id)]

    def retained_with_seq(self, node : str, run_id : Optional[str] = None) -> List[tuple]:
        '''``(sequence, body)`` for every retained data envelope — the broker's own ordering.'''
        return [(seq, e.body) for seq, e in self.retained_entries(node, run_id)]

    def retained_entries(self, node : str, run_id : Optional[str] = None) -> List[tuple]:
        '''``(sequence, DeadLetter)`` — subject, headers and body — for every retained data envelope.'''
        channel = self.backend._channels.get(self.channel(node, run_id))
        if channel is None:
            return []
        subject = topology.subject_for(self.flow_id, run_id or self.run_id, node)
        with self.backend._lock:
            return [(m.sequence, DeadLetter(subject, dict(m.envelope.headers), m.envelope.body))
                    for m in channel.messages if m.envelope.kind == KIND_DATA]

    @contextlib.contextmanager
    def ticking(self, step : float = 0.2, every : float = 0.05) -> Iterator[None]:
        '''
        Advance the fake clock ``step`` seconds every ``every`` real seconds while
        the block runs, so model timers a threaded receiver waits on (a NAK's
        redelivery delay) fire without a hand on the clock.
        '''
        stop = threading.Event()

        def tick() -> None:
            while not stop.wait(every):
                self.clock.advance(step)
        thread = threading.Thread(target = tick, name = 'fake-clock-ticker', daemon = True)
        thread.start()
        try:
            yield
        finally:
            stop.set()
            thread.join(timeout = 2)

    def ack_floor(self, child : str, parent : str, replica : Optional[int] = None,
                  run_id : Optional[str] = None) -> int:
        '''The highest sequence up to which ``child`` has settled every data message of ``parent`` (0: none).'''
        sub = SubscriptionId(self.channel(parent, run_id), child, replica, 'data')
        channel = self.backend._channels.get(self.channel(parent, run_id))
        floor = 0
        if channel is None:
            return floor
        with self.backend._lock:
            for stored in channel.messages:
                if stored.envelope.kind != KIND_DATA:
                    continue
                view = stored.views.get(sub)
                if view is None or view.settled is None:
                    break
                floor = stored.sequence
            # Messages already reclaimed were settled by everyone: the floor is at least the next sequence - 1.
            first = min((m.sequence for m in channel.messages), default = channel.next_sequence)
            floor = max(floor, first - 1)
        return floor

    def consumer_state(self, child : str, parent : str, replica : Optional[int] = None,
                       run_id : Optional[str] = None) -> ConsumerState:
        sub = SubscriptionId(self.channel(parent, run_id), child, replica, 'data')
        observed = self.backend.observe_subscription(sub)
        assert isinstance(observed, Known), observed
        return ConsumerState(observed.value.available + observed.value.unresolved, observed.value.leased, 0)

    def publish_raw(self, node : str, body : bytes, headers : Optional[Dict[str, str]] = None,
                    run_id : Optional[str] = None, publication_id : Optional[str] = None,
                    kind : str = KIND_DATA) -> Any:
        pid = publication_id or f'raw-{sha256(body)[:16]}-{time.monotonic_ns()}'
        return self.backend.publish(make_envelope(self.channel(node, run_id), pid, body, headers = headers,
                                                  kind = kind), time.monotonic() + 5.0)

    def stop_flow(self, run_id : Optional[str] = None) -> None:
        '''The flow-wide stop: every messenger of the run sees its termination event.'''
        for m in self._messengers:
            if m._run_id == (run_id or self.run_id):
                m.quiesce()

    def teardown_run(self, run_id : Optional[str] = None) -> Any:
        '''
        Delete the run's channels. The model keeps dead letters on the failing
        node's own channel (JetStream keeps them on a flow-level stream a run's
        teardown never owns), so a channel carrying dead letters is left standing
        — the model's stand-in for the DLQ stream surviving the run.
        '''
        run = run_id or self.run_id
        owned = [cid for cid in self.backend.channel_ids() if cid.flow_id == self.flow_id and cid.run_id == run
                 and not any(e.kind == KIND_DLQ for e in self.backend.stored(cid))]
        return self.backend.close(owned, 'teardown')

    def close(self) -> None:
        for m in self._messengers:
            with contextlib.suppress(Exception):
                m.close()
        self._messengers = []


class JetStreamRig:
    '''
    A flow on the compose broker: provisioned with ``provision_flow_sync`` (as the
    init Job does), every messenger on its own connection (as every worker is),
    dead letters and retained messages read back by sequence with direct gets —
    no consumer is ever created for inspection, so nothing here perturbs interest
    retention. ``close`` deletes every run it provisioned and the flow's DLQ.
    '''
    def __init__(self, nats_url : str, flow_type : str, specs : Sequence[NodeSpec],
                 redis_url : Optional[str] = None, store : Optional[PayloadStore] = None,
                 max_retries : int = 3, ack_wait : int = 30, run_id : Optional[str] = None) -> None:
        self.nats_url = nats_url
        self.flow_type = flow_type
        self.specs = list(specs)
        self.flow_id, self.run_id = unique_ids('pay')
        if run_id is not None:
            self.run_id = run_id
        self.runs : List[str] = []
        self.max_retries = max_retries
        self.ack_wait = ack_wait
        self.provision_run(self.run_id)
        if store is not None:
            self.store : Optional[PayloadStore] = store
        elif redis_url is not None:
            from videoflow.wire.redis_payload_store import RedisPayloadStore  # optional dep (redis extra)
            self.store = RedisPayloadStore(redis_url)
        else:
            self.store = None
        # Every put through the rig's default store is recorded so ``close`` can
        # remove exactly the keys this run created on the shared dev Redis —
        # never a pattern sweep, which would take other suites' keys with it.
        self._recording : Optional[KeyRecordingStore] = None
        if self.store is not None:
            self._recording = self.store if isinstance(self.store, KeyRecordingStore) else KeyRecordingStore(self.store)
        self._messengers : List[NATSMessenger] = []

    def provision_run(self, run_id : str, specs : Optional[Sequence[NodeSpec]] = None) -> None:
        provision_flow_sync(self.nats_url, list(specs or self.specs), self.flow_id, run_id, self.flow_type,
                            max_retries = self.max_retries, ack_wait = self.ack_wait, timeout = 30.0)
        if run_id not in self.runs:
            self.runs.append(run_id)

    def messenger(self, name : str, parents : Sequence[str], run_id : Optional[str] = None,
                  store : Optional[PayloadStore] = None, nats_url : Optional[str] = None,
                  **kwargs : Any) -> NATSMessenger:
        kwargs.setdefault('ack_wait', self.ack_wait)
        kwargs.setdefault('max_retries', self.max_retries)
        m = NATSMessenger(StubNode(name), list(parents), nats_url or self.nats_url, self.flow_id,  # type: ignore[arg-type]
                          self.flow_type, run_id or self.run_id,
                          payload_store = self._recording if store is None else store, **kwargs)
        self._messengers.append(m)
        return m

    def created_keys(self) -> List[str]:
        '''Every key a publisher on the rig's default store created, in order.'''
        return list(self._recording.keys) if self._recording is not None else []

    def channel(self, node : str, run_id : Optional[str] = None) -> ChannelId:
        return ChannelId(self.flow_id, run_id or self.run_id, node)

    def teardown_run(self, run_id : Optional[str] = None) -> Any:
        '''Delete the run's streams by exact ownership — the flow's DLQ stream is never one of them.'''
        return delete_run(self.nats_url, self.flow_id, run_id or self.run_id)

    def _messages(self, stream : str, subject_filter : Optional[Callable[[str], bool]] = None) -> List[DeadLetter]:
        return [m for _seq, m in self._messages_with_seq(stream, subject_filter)]

    def _messages_with_seq(self, stream : str,
                           subject_filter : Optional[Callable[[str], bool]] = None) -> List[tuple]:
        import nats  # optional dep (distributed extras)

        async def _go() -> List[tuple]:
            nc = await nats.connect(self.nats_url)
            out : List[tuple] = []
            try:
                js = nc.jetstream()
                try:
                    state = (await js.stream_info(stream)).state
                except Exception:  # noqa: BLE001 — no stream yet means nothing retained
                    return out
                for seq in range(state.first_seq, state.last_seq + 1):
                    try:
                        raw = await js.get_msg(stream, seq)
                    except Exception:  # noqa: BLE001 — a deleted sequence
                        continue
                    if subject_filter is None or subject_filter(raw.subject or ''):
                        out.append((seq, DeadLetter(raw.subject or '', dict(raw.headers or {}), raw.data or b'')))
            finally:
                await nc.close()
            return out
        return run_async(_go)

    def dead_letters(self, node : Optional[str] = None, run_id : Optional[str] = None) -> List[DeadLetter]:
        '''Every dead letter of the flow (optionally one run / one node), left in place.'''
        def matches(subject : str) -> bool:
            if run_id is not None and subject.split('.')[3:4] != [topology.sanitize(run_id)]:
                return False
            return node is None or subject.split('.')[4:5] == [topology.sanitize(node)]
        return self._messages(topology.dlq_stream_name(self.flow_id), matches)

    def retained(self, node : str, run_id : Optional[str] = None) -> List[bytes]:
        '''The data envelopes the stream currently retains for ``node``, in sequence order.'''
        return [body for _seq, body in self.retained_with_seq(node, run_id)]

    def retained_with_seq(self, node : str, run_id : Optional[str] = None) -> List[tuple]:
        '''``(stream sequence, body)`` for every retained data envelope of ``node``.'''
        return [(seq, e.body) for seq, e in self.retained_entries(node, run_id)]

    def retained_entries(self, node : str, run_id : Optional[str] = None) -> List[tuple]:
        '''``(stream sequence, DeadLetter)`` — subject, headers and body — for every retained data envelope.'''
        subject = topology.subject_for(self.flow_id, run_id or self.run_id, node)
        return self._messages_with_seq(topology.stream_name_for(self.flow_id, run_id or self.run_id, node),
                                       lambda s: s == subject)

    def ack_floor(self, child : str, parent : str, replica : Optional[int] = None,
                  run_id : Optional[str] = None) -> int:
        '''The consumer's ``ack_floor.stream_seq``: every message up to it is settled by ``child`` (0: none).'''
        import nats  # optional dep (distributed extras)
        durable = (topology.partitioned_durable_name_for(child, parent, replica) if replica is not None
                   else topology.durable_name_for(child, parent))
        stream = topology.stream_name_for(self.flow_id, run_id or self.run_id, parent)

        async def _go() -> int:
            nc = await nats.connect(self.nats_url)
            try:
                info = await nc.jetstream().consumer_info(stream, durable)
                return int(info.ack_floor.stream_seq or 0) if info.ack_floor is not None else 0
            finally:
                await nc.close()
        return run_async(_go)

    def stream_state(self, node : str, run_id : Optional[str] = None) -> Any:
        from _brokers import stream_state
        return stream_state(self.nats_url, topology.stream_name_for(self.flow_id, run_id or self.run_id, node))

    def consumer_state(self, child : str, parent : str, replica : Optional[int] = None,
                       run_id : Optional[str] = None) -> ConsumerState:
        import nats  # optional dep (distributed extras)
        durable = (topology.partitioned_durable_name_for(child, parent, replica) if replica is not None
                   else topology.durable_name_for(child, parent))
        stream = topology.stream_name_for(self.flow_id, run_id or self.run_id, parent)

        async def _go() -> ConsumerState:
            nc = await nats.connect(self.nats_url)
            try:
                info = await nc.jetstream().consumer_info(stream, durable)
                return ConsumerState(int(info.num_pending or 0), int(info.num_ack_pending or 0),
                                     int(info.num_redelivered or 0))
            finally:
                await nc.close()
        return run_async(_go)

    def publish_raw(self, node : str, body : bytes, headers : Optional[Dict[str, str]] = None,
                    run_id : Optional[str] = None, subject : Optional[str] = None) -> Any:
        '''Bytes on ``node``'s data subject (or ``subject``), as a foreign publisher would put them.'''
        from _brokers import publish_raw
        return publish_raw(self.nats_url, subject or topology.subject_for(self.flow_id, run_id or self.run_id, node),
                           body, headers = headers)

    def stop_flow(self, run_id : Optional[str] = None) -> None:
        '''Publish the run's flow-wide stop on its control subject (what ``videoflow`` does on abort).'''
        import nats  # optional dep (distributed extras)

        async def _go() -> None:
            nc = await nats.connect(self.nats_url)
            try:
                await nc.publish(topology.control_subject_for(self.flow_id, run_id or self.run_id), b'stop')
                await nc.flush()
            finally:
                await nc.close()
        run_async(_go)

    def close(self) -> None:
        for m in self._messengers:
            with contextlib.suppress(Exception):
                m.close()
        self._messengers = []
        for run in self.runs:
            with contextlib.suppress(Exception):
                delete_run(self.nats_url, self.flow_id, run)
        with contextlib.suppress(Exception):
            delete_dlq(self.nats_url, self.flow_id)
        if self._recording is not None and self._recording.keys:
            client = getattr(raw_store(self._recording), 'client', None)
            if client is not None:
                sweep_refs(client, self._recording.keys)


class Receiver(threading.Thread):
    '''
    Drains one messenger on its own thread — receive, record, settle — until it
    has ``expected`` inputs, the flow stops, or ``stop()`` is called. Exceptions
    are kept, never swallowed: the test asserts on ``error``.
    '''
    def __init__(self, messenger : NATSMessenger, expected : int,
                 settle : Optional[Callable[[NATSMessenger, dict], None]] = None,
                 on_input : Optional[Callable[[dict], None]] = None, name : str = 'receiver') -> None:
        super().__init__(name = name, daemon = True)
        self.messenger = messenger
        self.expected = expected
        self.inputs : List[dict] = []
        self.error : Optional[BaseException] = None
        self._settle = settle
        self._on_input = on_input
        self._stop_requested = threading.Event()

    def run(self) -> None:
        try:
            while len(self.inputs) < self.expected and not self._stop_requested.is_set():
                inputs = self.messenger.receive_message()
                if all(v.get('is_stop_signal') for v in inputs.values()):
                    return
                self.inputs.append(inputs)
                if self._on_input is not None:
                    self._on_input(inputs)
                if self._settle is not None:
                    self._settle(self.messenger, inputs)
                else:
                    self.messenger.ack_inputs()
        except BaseException as e:  # noqa: BLE001 — surfaced to the test thread
            self.error = e

    def stop(self) -> None:
        self._stop_requested.set()
        self.messenger.quiesce()
