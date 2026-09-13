'''
Conformance cases: PAY-002, PAY-003, PAY-019.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions taking the component under
test — a rig standing a parent → child flow up on the memory backends or on the
compose broker (``_payloads.py``) — so the paired negative control can run the same
oracle against the reviewed defect (``defects_pay.py``) and prove it fails.
'''
from __future__ import absolute_import, division, print_function

import itertools
import logging
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import defects
import defects_pay
import numpy as np
import pytest
from _payloads import (
    CountingStore,
    JetStreamRig,
    MemoryRig,
    ReadLog,
    Receiver,
    frame_array,
    object_exists,
    obligations_of,
    spec,
    wait_until,
    write_evidence,
)

from videoflow.backends import faults
from videoflow.backends.capabilities import LIVE_LATEST, RETENTION_LIMITS
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.messaging import make_channel
from videoflow.backends.memory.payload import MemoryPayloadStore
from videoflow.backends.payload import (
    DurableReceipt,
    ImmutablePayloadRef,
    ObligationLedger,
    PayloadStore,
    ReclamationObservation,
    ReleaseReceipt,
    RetentionContract,
)
from videoflow.backends.payload_bridge import PayloadStoreBlobBridge
from videoflow.core import constants
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.errors import PoisonMessage, TransientFailure
from videoflow.messaging import nats_messenger
from videoflow.wire.serialization import peek_envelope

pytestmark = pytest.mark.timeout(180)

#: The frame every access case ships: a deterministic million-byte specimen.
FRAME = frame_array((1000, 1000), seed = 2)
#: R — how long A may take to complete after the store recovers (the catalog's bound).
R_SECONDS = 30.0


def _specs() -> list:
    return [spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False)]


def _last_key(rig : Any, node : str) -> str:
    '''The payload key the newest retained envelope on ``node``'s channel references.'''
    bodies = rig.retained(node)
    assert bodies, f'no envelope retained on {node}'
    key = peek_envelope(bodies[-1]).get('blob_ref')
    assert key, 'the newest envelope carries no payload reference'
    return key


# -- PAY-002 ----------------------------------------------------------------------

def _recovering_faults(holder : list, kinds : List[Callable[[str], BaseException]],
                       recovered : Dict[str, float]) -> Callable[[], BaseException]:
    '''
    A ``RaiseError`` factory raising ``kinds[i]`` on the i-th GET (ConnectionError,
    timeout, temporarily unavailable — the catalog's separate cases) and
    uninstalling the schedule after the last one: the payload store recovers.
    '''
    calls = itertools.count(1)

    def factory() -> BaseException:
        n = next(calls)
        if n >= len(kinds):
            holder[0].uninstall()
            recovered['at'] = time.monotonic()
        return kinds[min(n, len(kinds)) - 1](f'injected GET failure #{n}')
    return factory


_GET_FAULTS : List[Callable[[str], BaseException]] = [
    ConnectionError, TimeoutError,
    lambda message: TransientFailure(message, remedy = 'retry: the store is temporarily unavailable'),
]


def _oracle_pay_002(rig : Any, evidence : Dict[str, Any], caplog : Any) -> faults.FaultSchedule:
    '''A: offloaded, its first three GETs fail transiently; B: an inline sentinel behind it.'''
    child = rig.messenger('child', ['parent'])
    parent = rig.messenger('parent', [], blob_reader_ids = ['child'])
    holder : list = [None]
    recovered : Dict[str, float] = {}
    schedule = faults.FaultSchedule({'payload.read.before': faults.RaiseError(
        _recovering_faults(holder, _GET_FAULTS, recovered))})
    holder[0] = schedule
    seen : Dict[str, Dict[str, Any]] = {}
    schedule.install()
    try:
        parent.publish_message(FRAME)                                   # A
        key = _last_key(rig, 'parent')
        parent.publish_message({'sentinel': True})                      # B
        started = time.monotonic()
        while len(seen) < 2 and time.monotonic() - started < 120:
            inputs = child.receive_message()
            message = inputs['parent']['message']
            attempt = child._inflight_handles[0].num_delivered if child._inflight_handles else None
            name = 'A' if isinstance(message, np.ndarray) else 'B'
            seen[name] = {'at': time.monotonic(), 'attempt': attempt,
                          'identical': bool(isinstance(message, np.ndarray) and np.array_equal(message, FRAME))}
            child.ack_inputs()
    finally:
        schedule.uninstall()
    evidence['seen'] = seen
    evidence['recovered_at'] = recovered.get('at')
    evidence['faults'] = schedule.fired()
    evidence['dead_letters'] = [dl.headers for dl in rig.dead_letters()]
    evidence['drops'] = child.take_drops()
    state = rig.consumer_state('child', 'parent')
    evidence['consumer'] = state.__dict__
    warnings = [r.getMessage() for r in caplog.records if 'payload fetch failed transiently' in r.getMessage()]
    evidence['diagnostics'] = warnings
    assert set(seen) == {'A', 'B'}, f'A and the sentinel must both complete; got {sorted(seen)}'
    assert seen['A']['identical'], 'A did not hydrate to byte-identical data after recovery'
    assert seen['A']['attempt'] == len(_GET_FAULTS) + 1, seen['A']
    assert evidence['dead_letters'] == [], f'a transient GET fault was dead-lettered: {evidence["dead_letters"]}'
    assert not ({'undecodable', 'exhausted'} & set(evidence['drops'])), evidence['drops']
    assert state.pending == 0 and state.unacked == 0, state
    assert schedule.fired().get('payload.read.before') == len(_GET_FAULTS), schedule.fired()
    assert 'at' in recovered and seen['A']['at'] - recovered['at'] < R_SECONDS, (seen, recovered)
    assert not object_exists(rig.store, key), 'A was acked by its only reader but the object was not reclaimed'
    assert len(warnings) >= len(_GET_FAULTS), 'the transient failures left no diagnostic'
    return schedule


@pytest.mark.case('PAY-002')
@pytest.mark.level('broker')
def test_pay_002_transient_payload_access_failures_retry_without_terminal(nats_url, redis_url, evidence_dir,
                                                                          record_faults, monkeypatch, caplog) -> None:
    '''
    PAY-002 (P0, payload, broker): Transient payload access failures retry without terminal
    loss.

    Acceptance: A completes within R after payload-store recovery with byte-identical data; no
    terminal settlement occurs solely because of transient GET failure.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    caplog.set_level(logging.WARNING, logger = 'videoflow.messaging')
    evidence : Dict[str, Any] = {}
    rig = JetStreamRig(nats_url, BATCH, _specs(), redis_url = redis_url, max_retries = 5, ack_wait = 30)
    try:
        schedule = _oracle_pay_002(rig, evidence, caplog)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'retry_history.json', evidence)
    record_faults(schedule)


def _oracle_pay_002_model(store : PayloadStore, classifier : Callable[[BaseException], bool],
                          evidence : Dict[str, Any]) -> faults.FaultSchedule:
    '''
    The store and the bridge alone: a GET that fails with the store unreachable is
    a typed *transient* outcome the messenger's classifier retries; the object
    and its obligation are untouched until the reader finishes.
    '''
    bridge = PayloadStoreBlobBridge(store, ['child'], horizon_seconds = 3600, durable_required = True,
                                    content_id = lambda: 'parent:t1:1', intent = lambda: 'intent/p1')
    key = bridge.put_with_readers(FRAME.tobytes(), 1, 3600)
    ref = store.ref_for_key(key)
    store.release_obligation(ref, 'intent/p1', 'puback')
    holder : list = [None]
    recovered : Dict[str, float] = {}
    schedule = faults.FaultSchedule({'payload.read.before': faults.RaiseError(
        _recovering_faults(holder, _GET_FAULTS, recovered))})
    holder[0] = schedule
    attempts : List[str] = []
    schedule.install()
    try:
        for attempt in range(1, len(_GET_FAULTS) + 1):
            try:
                bridge.get(key)
            except Exception as e:  # noqa: BLE001 — the outcome under test
                error : BaseException = e
            else:
                raise AssertionError(f'attempt {attempt}: the injected GET failure did not surface')
            attempts.append(f'{type(error).__name__}: {error}')
            assert classifier(error), (f'attempt {attempt}: {type(error).__name__} was classified as '
                                       f'non-transient, which terminates the delivery')
            assert isinstance(error, TransientFailure) and not isinstance(error, PoisonMessage), error
            assert obligations_of(store, key) == {'child'}, 'a failed GET changed the ownership record'
    finally:
        schedule.uninstall()
    data = bridge.get(key)
    assert data == FRAME.tobytes(), 'the bytes after recovery are not the original'
    assert obligations_of(store, key) == {'child'}
    receipt = store.release_obligation(ref, 'child', 'ack')
    assert receipt.reclaimed and not object_exists(store, key)
    evidence['attempts'] = attempts
    evidence['faults'] = schedule.fired()
    return schedule


@pytest.mark.case('PAY-002')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_pay_002_memory_store_reads_are_transient_never_poison(evidence_dir, record_faults) -> None:
    '''The reference store and bridge: three transient GET faults, then byte-identical recovery.'''
    evidence : Dict[str, Any] = {}
    schedule = _oracle_pay_002_model(MemoryPayloadStore(FakeClock()),
                                     lambda e: nats_messenger._is_transient_store_failure(e), evidence)
    write_evidence(evidence_dir, 'retry_history.json', evidence)
    record_faults(schedule)


@pytest.mark.negative_control(of = 'PAY-002')
def test_pay_002_detects_a_messenger_that_terminates_on_any_store_failure(monkeypatch) -> None:
    defects_pay.poison_on_any_store_failure(monkeypatch)
    assert defects.detects(_oracle_pay_002_model, MemoryPayloadStore(FakeClock()),
                           lambda e: nats_messenger._is_transient_store_failure(e), {})


# -- PAY-003 ----------------------------------------------------------------------

BROKEN_KINDS = ('missing', 'truncated', 'mismatch')


class _SabotagedPuts(PayloadStore):
    '''
    The publisher's store, breaking each object right after it is written — a
    deletion, a truncation, a flipped byte — before any consumer can load it.
    ``plan`` lists what to do to each successive put (None leaves it intact).
    '''
    def __init__(self, inner : PayloadStore, plan : List[Optional[str]]) -> None:
        self._inner = inner
        self._plan = list(plan)
        self.keys : List[str] = []

    def capabilities(self) -> Any:
        return self._inner.capabilities()

    def put(self, data : bytes, content_id : str, contract : RetentionContract) -> ImmutablePayloadRef:
        ref = self._inner.put(data, content_id, contract)
        kind = self._plan.pop(0) if self._plan else None
        if kind is not None:
            _break_object(self._inner, ref.key, kind)
        self.keys.append(ref.key)
        return ref

    def acquire_obligation(self, ref : ImmutablePayloadRef, obligation_id : str, deadline : float) -> DurableReceipt:
        return self._inner.acquire_obligation(ref, obligation_id, deadline)

    def renew_obligation(self, ref : ImmutablePayloadRef, obligation_id : str, deadline : float) -> DurableReceipt:
        return self._inner.renew_obligation(ref, obligation_id, deadline)

    def read(self, ref : ImmutablePayloadRef) -> Any:
        return self._inner.read(ref)

    def release_obligation(self, ref : ImmutablePayloadRef, obligation_id : str,
                           completion_receipt : str) -> ReleaseReceipt:
        return self._inner.release_obligation(ref, obligation_id, completion_receipt)

    def reconcile(self, ledger : ObligationLedger, operation_id : str) -> ReclamationObservation:
        return self._inner.reconcile(ledger, operation_id)

    def inventory(self) -> Any:
        return self._inner.inventory()

    def ref_for_key(self, key : str) -> ImmutablePayloadRef:
        return self._inner.ref_for_key(key)


def _break_object(store : PayloadStore, key : str, kind : str) -> None:
    '''Delete, truncate or corrupt the backing object through the store's own fabric.'''
    if isinstance(store, MemoryPayloadStore):
        if kind == 'missing':
            store.delete(key)
        elif kind == 'truncated':
            store.truncate(key, store.ref_for_key(key).size // 2)
        else:
            store.corrupt(key)
        return
    client = store.client   # type: ignore[attr-defined]  # the Redis store
    if kind == 'missing':
        client.unlink(key)
        return
    data = bytes(client.get(key))
    broken = data[:len(data) // 2] if kind == 'truncated' else bytes([data[0] ^ 0xFF]) + data[1:]
    client.set(key, broken, keepttl = True)


def _oracle_pay_003(rig : Any, flow_type : str, evidence : Dict[str, Any],
                    handling_deadline : float = 30.0) -> faults.FaultSchedule:
    '''
    Three offloaded inputs whose objects are missing, truncated and checksum-
    mismatched, then a healthy inline sentinel. Every broken one must end in an
    explicit terminal record naming the ref and the reason — never in the node's
    hands, never in an endless retry — and the sentinel must still be processed.
    '''
    publisher_store = _SabotagedPuts(rig.store, list(BROKEN_KINDS) + [None])
    child = rig.messenger('child', ['parent'])
    parent = rig.messenger('parent', [], store = publisher_store, blob_reader_ids = ['child'])
    receiver = Receiver(child, expected = 1, name = 'pay-003-child')
    schedule = faults.FaultSchedule({'payload.read.before': faults.Delay(0.0)})
    timeline : List[Dict[str, Any]] = []
    with schedule:
        receiver.start()
        for i, kind in enumerate(BROKEN_KINDS):
            published = time.monotonic()
            parent.publish_message(frame_array((900, 900), seed = 10 + i))
            expected = i + 1
            assert wait_until(lambda n = expected: len(rig.dead_letters('child')) >= n, handling_deadline), \
                f'{kind}: no terminal record within {handling_deadline}s ({len(rig.dead_letters("child"))} so far)'
            timeline.append({'kind': kind, 'key': publisher_store.keys[-1],
                             'terminal_after_s': round(time.monotonic() - published, 3)})
        parent.publish_message({'sentinel': True})
        receiver.join(timeout = handling_deadline)
    assert receiver.error is None, receiver.error
    assert not receiver.is_alive(), 'the sentinel never reached the node'
    letters = rig.dead_letters('child')
    drops = child.take_drops()
    state = rig.consumer_state('child', 'parent')
    evidence.update({'timeline': timeline, 'dead_letters': [dict(dl.headers) for dl in letters],
                     'drops': drops, 'consumer': state.__dict__, 'returned_to_node': len(receiver.inputs),
                     'faults': schedule.fired(), 'created_keys': list(publisher_store.keys)})
    # None of the broken inputs was handed to the node as valid frame data.
    assert len(receiver.inputs) == 1 and receiver.inputs[0]['parent']['message'] == {'sentinel': True}
    assert drops.get('undecodable') == len(BROKEN_KINDS), drops
    assert state.pending == 0 and state.unacked == 0, f'a broken input is stranded or still retrying: {state}'
    for entry in timeline:
        assert entry['terminal_after_s'] < handling_deadline
    # Each dead letter names the ref and the reason; the raw envelope travels verbatim.
    by_key = {}
    for dl in letters:
        assert dl.headers.get('VF-Code') == 'VF_POISON_DECODE', dl.headers
        assert dl.headers.get('VF-Disposition') == 'poison', dl.headers
        assert dl.headers.get('VF-Origin-Node') == 'child'
        ref = peek_envelope(dl.body).get('blob_ref')
        assert ref and ref in dl.headers.get('VF-Error', ''), (ref, dl.headers)
        by_key[ref] = dl.headers['VF-Error']
    if flow_type == BATCH:
        assert len(letters) == len(BROKEN_KINDS), [dl.headers for dl in letters]
        for entry in timeline:
            reason = 'missing' if entry['kind'] == 'missing' else 'corrupt'
            assert reason in by_key[entry['key']], (entry, by_key)
    else:
        # Live: an allowed loss, dropped with a bounded specimen carrying identity and reason.
        assert 1 <= len(letters) <= len(BROKEN_KINDS), [dl.headers for dl in letters]
        assert all(('missing' in v) or ('corrupt' in v) for v in by_key.values()), by_key
    return schedule


@pytest.mark.case('PAY-003')
@pytest.mark.level('broker')
def test_pay_003_permanent_missing_or_corrupt_payloads_are_explicitly(nats_url, redis_url, evidence_dir,
                                                                      record_faults, monkeypatch) -> None:
    '''
    PAY-003 (P0, payload, broker): Permanent missing or corrupt payloads are explicitly
    accounted.

    Acceptance: Each broken reference has the correct observable terminal/drop outcome within
    its configured handling deadline; none is recorded as successful processing.
    '''
    from _brokers import sweep_refs
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    for flow_type in (BATCH, REALTIME):
        rig = JetStreamRig(nats_url, flow_type, _specs(), redis_url = redis_url, max_retries = 2, ack_wait = 30)
        try:
            evidence[flow_type] = {}
            schedule = _oracle_pay_003(rig, flow_type, evidence[flow_type])
        finally:
            rig.close()
            # The dead letters pinned their payloads for the DLQ retention (by design, PAY-014);
            # on the shared dev Redis this test lets go of exactly the objects it made.
            sweep_refs(rig.store.client, evidence[flow_type].get('created_keys', []))   # type: ignore[union-attr]
            write_evidence(evidence_dir, 'terminal_ledger.json', evidence)
        record_faults(schedule)


def _memory_rig_pay_003(flow_type : str) -> MemoryRig:
    rig = MemoryRig(flow_type)
    if flow_type == REALTIME:
        # The model shares one REALTIME slot between data and dead letters on a
        # channel; give the child's channel room for its specimens so the oracle
        # can read them all (the real DLQ is a separate, week-long stream).
        rig.backend.ensure_channel(make_channel(rig.flow_id, rig.run_id, 'child', LIVE_LATEST, RETENTION_LIMITS,
                                                max_msgs = 100), 'pay-003')
    return rig


@pytest.mark.case('PAY-003')
@pytest.mark.level('broker')
@pytest.mark.variant('memory')
def test_pay_003_memory_backends_account_for_every_broken_reference(evidence_dir, record_faults,
                                                                    monkeypatch) -> None:
    '''The same flow on the in-memory backends: reliable and live, deterministic.'''
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    for flow_type in (BATCH, REALTIME):
        rig = _memory_rig_pay_003(flow_type)
        try:
            evidence[flow_type] = {}
            schedule = _oracle_pay_003(rig, flow_type, evidence[flow_type])
        finally:
            rig.close()
        record_faults(schedule)
    write_evidence(evidence_dir, 'terminal_ledger.json', evidence)


@pytest.mark.negative_control(of = 'PAY-003')
def test_pay_003_detects_a_silent_terminal_discard(monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    defects_pay.silent_terminal_discard(monkeypatch)
    rig = _memory_rig_pay_003(BATCH)
    try:
        assert defects.detects(_oracle_pay_003, rig, BATCH, {}, 10.0)
    finally:
        rig.close()


# -- PAY-019 ----------------------------------------------------------------------

#: L — the control/status latency bound while a payload read is blocked.
CONTROL_LATENCY_BOUND = 1.0
#: How many inputs the case pushes through while the first GET is blocked.
INPUTS = 6


def _oracle_pay_019(rig : Any, delay_s : float, ack_wait : int, evidence : Dict[str, Any]) -> faults.FaultSchedule:
    '''
    The first GET blocks for longer than the ack wait. Meanwhile status requests
    on the same client must answer within L, every parked input must keep its
    lease (no redelivery), and at most one GET is ever in flight per receiver.
    '''
    log = ReadLog()
    child = rig.messenger('child', ['parent'], store = CountingStore(rig.store, 'child', log), ack_wait = ack_wait)
    parent = rig.messenger('parent', [], blob_reader_ids = ['child'])
    frames = [frame_array((256, 256), seed = 100 + i) for i in range(INPUTS)]
    attempts : List[int] = []

    def settle(messenger : Any, inputs : dict) -> None:
        attempts.append(messenger._inflight_handles[0].num_delivered)
        messenger.ack_inputs()

    receiver = Receiver(child, expected = INPUTS, settle = settle, name = 'pay-019-child')
    schedule = faults.FaultSchedule({'payload.read.before': faults.Nth(1, faults.Delay(delay_s))})
    latencies : List[float] = []
    stop = threading.Event()

    def monitor() -> None:
        while not stop.is_set():
            started = time.monotonic()
            child.subscription_status()
            latencies.append(time.monotonic() - started)
            time.sleep(0.2)

    with schedule:
        for frame in frames:
            parent.publish_message(frame)
        receiver.start()
        watcher = threading.Thread(target = monitor, name = 'pay-019-status', daemon = True)
        watcher.start()
        receiver.join(timeout = delay_s + 60)
        stop.set()
        watcher.join(timeout = 5)
    assert receiver.error is None, receiver.error
    assert len(receiver.inputs) == INPUTS, f'{len(receiver.inputs)} of {INPUTS} inputs completed'
    started = time.monotonic()
    rig.stop_flow()
    stopped = wait_until(child.check_for_termination, CONTROL_LATENCY_BOUND * 5)
    control_latency = time.monotonic() - started
    state = rig.consumer_state('child', 'parent')
    evidence.update({'attempts': attempts, 'status_latency_max_s': max(latencies), 'status_samples': len(latencies),
                     'control_latency_s': control_latency, 'max_inflight_gets': log.max_inflight,
                     'reads_by_thread': sorted({r.thread for r in log.records()}),
                     'consumer': state.__dict__, 'faults': schedule.fired()})
    assert attempts and all(a == 1 for a in attempts), f'a lease expired during the blocked read: {attempts}'
    assert state.redelivered == 0 and state.unacked == 0 and state.pending == 0, state
    assert latencies and max(latencies) < CONTROL_LATENCY_BOUND, f'status latency {max(latencies):.2f}s'
    assert stopped and control_latency < CONTROL_LATENCY_BOUND, f'control stop took {control_latency:.2f}s'
    assert log.max_inflight <= 1, f'{log.max_inflight} GETs in flight at once'
    assert evidence['reads_by_thread'] == ['pay-019-child'], 'a payload was read off the receiving thread'
    return schedule


@pytest.mark.case('PAY-019')
@pytest.mark.level('process')
def test_pay_019_slow_payload_reads_cannot_block_broker_heartbeats_or(nats_url, redis_url, evidence_dir,
                                                                      record_faults, monkeypatch) -> None:
    '''
    PAY-019 (P0, integration, process): Slow payload reads cannot block broker heartbeats or
    control delivery.

    Acceptance: Control latency remains below L and unrelated inputs retain their valid leases;
    active GET count and queued bytes never exceed configured limits.

    Process level on the real client runtime (a JetStream connection with its
    keepalive loop, gated on the compose broker and Redis): a blocked read on the
    receiving thread must leave the loop thread free to renew leases and to
    answer status and control traffic.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, 'MAX_INLINE_PAYLOAD_BYTES', 1024)     # every specimen offloads
    evidence : Dict[str, Any] = {}
    rig = JetStreamRig(nats_url, BATCH, _specs(), redis_url = redis_url, max_retries = 3, ack_wait = 2)
    try:
        schedule = _oracle_pay_019(rig, delay_s = 5.0, ack_wait = 2, evidence = evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'heartbeat_history.json', evidence)
    record_faults(schedule)


@pytest.mark.case('PAY-019')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_pay_019_memory_backends_keep_reads_off_the_transport_path(evidence_dir, record_faults, monkeypatch) -> None:
    '''
    The model: no loop thread exists, so what it can show is that hydration runs
    on the receiving thread only, that status calls answer while a read is
    blocked, and that every input keeps its (fake-clock) lease.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, 'MAX_INLINE_PAYLOAD_BYTES', 1024)
    evidence : Dict[str, Any] = {}
    rig = MemoryRig(BATCH)
    try:
        schedule = _oracle_pay_019(rig, delay_s = 1.5, ack_wait = 2, evidence = evidence)
    finally:
        rig.close()
    write_evidence(evidence_dir, 'heartbeat_history.json', evidence)
    record_faults(schedule)


@pytest.mark.negative_control(of = 'PAY-019')
def test_pay_019_detects_a_payload_fetched_on_the_broker_loop(nats_url, redis_url, monkeypatch) -> None:
    '''A GET inside the admission filter blocks the loop: leases lapse and status calls stall.'''
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, 'MAX_INLINE_PAYLOAD_BYTES', 1024)
    defects_pay.hydrate_on_the_loop(monkeypatch)
    rig = JetStreamRig(nats_url, BATCH, _specs(), redis_url = redis_url, max_retries = 3, ack_wait = 2)
    try:
        assert defects.detects(_oracle_pay_019, rig, 5.0, 2, {})
    finally:
        rig.close()
