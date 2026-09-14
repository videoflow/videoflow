'''
Conformance cases: MSG-008, MSG-009, MSG-010, MSG-011, MSG-012.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions over a driver
(``_msgdrivers``): the broker-level primary runs them on the JetStream adapter, the
``memory`` variant on the reference model under a fake clock, and the paired negative
control on the model with the reviewed defect monkeypatched in (``defects.py``).

MSG-008, MSG-009 and MSG-010 drive the real ``NATSMessenger`` — its delivery ladder,
dead-letter publish and poison classification — over either backend; MSG-011 and
MSG-012 drive the ``MessagingBackend`` contract directly. The parts of MSG-008,
MSG-009 and MSG-012 that need the runtime ledger (an attempt budget kept outside
the broker with ``max_deliver = -1``, a ``pending_handoff`` record that survives a
restart, a stranded-work record that survives a restart) are the ``ledger``
variants: broker-level, each worker "process" a messenger over the same
``FileRuntimeStore`` (``file://``, durable and shared on one host), the durables
provisioned at the ledger budget (STREAM-15, decision D11).
'''
from __future__ import absolute_import, division, print_function

import json
import threading
import time
from typing import Any, Callable, Dict, List

import defects
import pytest
from _brokers import unique_ids
from _msgdrivers import JetStreamDriver, MemoryDriver, digest, spec
from _runs3 import ledger_runtime, messengers, provision_with_ledger, status_unresolved

from videoflow.backends import faults
from videoflow.backends.capabilities import RELIABLE_WORK, RETENTION_INTEREST
from videoflow.backends.messaging import ChannelId, Completed, Retry, Terminal
from videoflow.backends.outcomes import Known, SettleConfirmed, SettleStale, Unknown
from videoflow.core.constants import BATCH
from videoflow.core.errors import DeviceError, SchemaError, TransientFailure
from videoflow.v1 import envelope_pb2
from videoflow.wire.serialization import MSG_TYPE_DATA, BlobStore, encode_envelope

NATS_TIMEOUT = 180


def _write(evidence_dir : Any, name : str, record : Dict[str, Any]) -> None:
    (evidence_dir / name).write_text(json.dumps(record, indent = 2, default = str))


def _settled(driver : Any, child : str, parent : str, timeout : float) -> bool:
    '''True once the child's subscription on the parent holds nothing pending and nothing leased.'''
    def clear() -> bool:
        observed = driver.consumer_state(child, parent)
        return isinstance(observed, Known) and observed.value == (0, 0)
    return driver.until(clear, timeout)


# -- MSG-008 -------------------------------------------------------------------------

def _oracle_msg_008(driver : Any, record : Dict[str, Any]) -> None:
    '''
    Worker 1 fails A with a worker-fatal error: the ladder hands A back (NAK), never
    dead-letters it, and the replacement worker binding the same logical
    subscription completes it. Then, at the STREAM-5 cap (one delivery), the same
    failure strands an input — and the subscription's observation must say so
    (``unresolved >= 1``) rather than read as an empty queue.
    '''
    driver.provision([spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False)],
                     BATCH, max_retries = 3)
    w1 = driver.messenger('child', ['parent'], BATCH, max_retries = 3, ack_wait = 2)
    a = driver.publish_parent('parent', 'A', 1, {'input': 'A'})
    group = driver.receive_group(w1, timeout = 30)
    assert group['parent']['message'] == {'input': 'A'}
    first_attempt = w1._inflight_handles[0].num_delivered
    started = time.monotonic()
    w1.fail_inputs(DeviceError('injected worker-fatal: device lost', remedy = 'replace the worker'))
    record['worker_fatal'] = {'message_id': a, 'attempt': first_attempt}
    assert driver.dlq('child') == [], 'a worker-fatal failure blamed the message'
    assert 'exhausted' not in w1.take_drops(), 'a worker-fatal failure consumed the retry budget'
    w1.quiesce()
    w1.close()                                                  # the breaker takes the worker out
    w2 = driver.messenger('child', ['parent'], BATCH, max_retries = 3, ack_wait = 2)
    group = driver.receive_group(w2, timeout = 60)
    assert group['parent']['message'] == {'input': 'A'}
    replacement_attempt = w2._inflight_handles[0].num_delivered
    assert replacement_attempt == first_attempt + 1, replacement_attempt
    w2.ack_inputs()
    record['replacement'] = {'attempt': replacement_attempt, 'recovery_seconds': time.monotonic() - started}
    assert _settled(driver, 'child', 'parent', 20), driver.consumer_state('child', 'parent')
    status = w2.subscription_status()['parent']
    assert isinstance(status, Known) and status.value.unresolved == 0, status
    assert driver.dlq('child') == []
    record['completed_status'] = str(status.value)
    w2.quiesce()
    w2.close()

    # The final-delivery case at today's cap: a naked input the broker will not
    # redeliver is retained and *reported*, never counted as done.
    driver.provision([spec('parent_b', [], 'producer', True), spec('sink', ['parent_b'], 'consumer', False)],
                     BATCH, max_retries = 0)
    w3 = driver.messenger('sink', ['parent_b'], BATCH, max_retries = 0, ack_wait = 2)
    b = driver.publish_parent('parent_b', 'B', 1, {'input': 'B'})
    group = driver.receive_group(w3, timeout = 30)
    assert group['parent_b']['message'] == {'input': 'B'}
    w3.fail_inputs(DeviceError('injected worker-fatal at the final delivery'))
    assert driver.dlq('sink') == [], 'a worker-fatal failure at the cap was dead-lettered'

    def stranded_is_visible() -> bool:
        observed = w3.subscription_status()['parent_b']
        return isinstance(observed, Known) and observed.value.unresolved >= 1
    assert driver.until(stranded_is_visible, 20), w3.subscription_status()['parent_b']
    observed = w3.subscription_status()['parent_b']
    assert isinstance(observed, Known)
    assert observed.value.available == 0 and observed.value.leased == 0, observed.value
    record['stranded_at_cap'] = {'message_id': b, 'observation': str(observed.value)}
    w3.quiesce()
    w3.close()


@pytest.mark.case('MSG-008')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_008_worker_fatal_failure_remains_recoverable_at_the_broker(nats_url, evidence_dir, monkeypatch) -> None:
    '''
    MSG-008 (P0, messaging, broker): Worker-fatal failure remains recoverable at the broker
    delivery limit.

    Acceptance: Within R after worker replacement, A successfully completes; a zero-pending
    counter without a completed or explicit terminal outcome fails.

    Today the transport cap is ``STREAM-5``'s ``retries + 1`` for provisioning and
    worker alike (decision D11); the separated transport budget with a ledger-kept
    attempt count is the pending ``ledger`` variant.
    '''
    flow, run = unique_ids('msg008')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_008(driver, record)
    finally:
        driver.close()
        _write(evidence_dir, 'delivery_ledger.json', record)


@pytest.mark.case('MSG-008')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_008_memory_worker_fatal_is_naked_and_completed_by_the_replacement(evidence_dir, monkeypatch) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_008(driver, record)
    finally:
        driver.close()
        _write(evidence_dir, 'delivery_ledger.json', record)


@pytest.mark.case('MSG-008')
@pytest.mark.level('broker')
@pytest.mark.variant('ledger')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_008_ledger_budget_survives_the_final_broker_delivery(nats_url, evidence_dir, monkeypatch, tmp_path) -> None:
    '''
    STREAM-15's separated budgets: the durable provisioned with ``max_deliver = -1``
    and the retry budget (``max_retries + 1`` counted attempts) kept by the ledger's
    attempt counts, which a worker-fatal failure never increments — so A, past what
    would have been its final broker delivery, is still redelivered to the
    replacement and completes, while a transient failure that does exhaust the
    ledger's budget is dead-lettered by the ledger, not stranded by the broker.
    '''
    flow, run = unique_ids('msg008l')
    driver = JetStreamDriver(nats_url, flow, run)
    root = str(tmp_path / 'ledger')
    record : Dict[str, Any] = {'flow': flow, 'run': run, 'store': f'file://{root}'}
    try:
        provision_with_ledger(driver, [spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False)],
                              BATCH, max_retries = 1)
        effective = driver.subscription_effective(ChannelId(flow, run, 'parent'), 'child')
        assert effective['max_deliver'] == -1, effective                # the broker never strands
        record['durable'] = effective
        with messengers() as pool:
            w1 = pool.ledger_messenger(driver, root, 'child', ['parent'], max_retries = 1)
            assert w1._max_deliver == -1
            a = driver.publish_parent('parent', 'A', 1, {'input': 'A'})
            group = driver.receive_group(w1, timeout = 30)
            assert group['parent']['message'] == {'input': 'A'}
            started = time.monotonic()
            w1.fail_inputs(DeviceError('injected worker-fatal: device lost', remedy = 'replace the worker'))
            assert ledger_runtime(root, flow, run, 'child').attempts_for(a) == 0, 'a worker-fatal failure counted against the budget'
            assert driver.dlq('child') == [] and 'exhausted' not in w1.take_drops()
            pool.close(w1)                                              # the breaker takes the worker out
            # The replacement (a new process over the same ledger) sees A again — at
            # its second broker delivery, with a budget of two counted attempts — fails
            # it transiently once (counted), then worker-fatally (not counted), and
            # is still delivered A a fourth time: past the STREAM-5 cap of two.
            w2 = pool.ledger_messenger(driver, root, 'child', ['parent'], max_retries = 1)
            group = driver.receive_group(w2, timeout = 60)
            assert group['parent']['message'] == {'input': 'A'}
            assert w2._inflight_handles[0].num_delivered == 2
            w2.fail_inputs(TransientFailure('injected transient', remedy = 'retry'))
            assert ledger_runtime(root, flow, run, 'child').attempts_for(a) == 1
            group = driver.receive_group(w2, timeout = 60)
            assert group['parent']['message'] == {'input': 'A'} and w2._inflight_handles[0].num_delivered == 3
            w2.fail_inputs(DeviceError('injected worker-fatal at what the broker would have called the final delivery'))
            assert ledger_runtime(root, flow, run, 'child').attempts_for(a) == 1
            assert driver.dlq('child') == [], 'a worker-fatal failure at the budget was dead-lettered'
            pool.close(w2)
            w3 = pool.ledger_messenger(driver, root, 'child', ['parent'], max_retries = 1)
            group = driver.receive_group(w3, timeout = 60)
            assert group['parent']['message'] == {'input': 'A'}
            delivered = w3._inflight_handles[0].num_delivered
            assert delivered >= 4, delivered
            w3.ack_inputs()
            record['A'] = {'message_id': a, 'deliveries': delivered, 'counted_attempts': 1,
                           'recovery_seconds': time.monotonic() - started}
            assert _settled(driver, 'child', 'parent', 20), driver.consumer_state('child', 'parent')
            assert status_unresolved(w3, 'parent') == 0 and driver.dlq('child') == []
            # The ledger enforces the budget it took over: B's second counted failure
            # is dead-lettered at the broker's second delivery, and reported as exhausted.
            b = driver.publish_parent('parent', 'B', 2, {'input': 'B'})
            for attempt in (1, 2):
                group = driver.receive_group(w3, timeout = 60)
                assert group['parent']['message'] == {'input': 'B'} and w3._inflight_handles[0].num_delivered == attempt
                w3.fail_inputs(TransientFailure(f'injected transient #{attempt}', remedy = 'retry'))
            assert ledger_runtime(root, flow, run, 'child').attempts_for(b) == 2
            assert driver.until(lambda: len(driver.dlq('child')) == 1, 20), driver.dlq('child')
            headers = driver.dlq('child')[0]['headers']
            assert headers['VF-Num-Delivered'] == '2' and headers['VF-Disposition'] == 'transient', headers
            assert w3.take_drops().get('exhausted') == 1
            assert _settled(driver, 'child', 'parent', 20), driver.consumer_state('child', 'parent')
            assert status_unresolved(w3, 'parent') == 0
            record['B'] = {'message_id': b, 'dead_letter': headers}
    finally:
        driver.close()
        _write(evidence_dir, 'ledger_budget.json', record)


@pytest.mark.negative_control(of = 'MSG-008')
def test_msg_008_detects_a_ladder_that_blames_the_message(monkeypatch) -> None:
    '''A worker-fatal failure treated as transient dead-letters A at the cap: the oracle must catch it.'''
    defects.blaming_ladder(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_008, driver, {})
    finally:
        driver.close()


# -- MSG-009 -------------------------------------------------------------------------

def _oracle_msg_009(driver : Any, record : Dict[str, Any], record_faults : Callable[..., None] | None = None) -> None:
    '''
    Poison A fails while the dead-letter path is down: the ladder keeps A (a delayed
    NAK, never TERM) and no record is claimed; the worker is replaced while the
    outage lasts; once the DLQ is back A is redelivered, dead-lettered under its
    stable identity — a lost receipt on that publish is reconciled to the same
    record — and only then terminated. Exactly one dead letter identifies A.
    '''
    driver.provision([spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False)],
                     BATCH, max_retries = 1)
    w1 = driver.messenger('child', ['parent'], BATCH, max_retries = 1, ack_wait = 2)
    a = driver.publish_parent('parent', 'A', 1, {'input': 'A'})
    group = driver.receive_group(w1, timeout = 30)
    assert group['parent']['message'] == {'input': 'A'}
    driver.dlq_outage('child')
    outage_started = time.monotonic()
    w1.fail_inputs(SchemaError('injected poison A', remedy = 'fix the producer'))
    assert driver.dlq('child') == [], 'a dead letter was claimed while the DLQ was unavailable'
    kept = driver.consumer_state('child', 'parent')
    assert isinstance(kept, Known) and sum(kept.value) >= 1, f'A was not kept for a later attempt: {kept}'
    record['during_outage'] = {'message_id': a, 'consumer_state': kept.value}
    w1.quiesce()
    w1.close()                                                  # restart while the DLQ is still down
    w2 = driver.messenger('child', ['parent'], BATCH, max_retries = 1, ack_wait = 2)
    driver.dlq_restore('child')
    group = driver.receive_group(w2, timeout = 60)
    assert group['parent']['message'] == {'input': 'A'}
    attempt = w2._inflight_handles[0].num_delivered
    assert attempt == 2, attempt
    # The dead-letter publish loses its receipt once: the retry under the same
    # record id is coalesced by the broker, so the record stays singular.
    schedule = faults.FaultSchedule({'publish.receipt.before': faults.Nth(1, faults.DropResponse())})
    with schedule:
        w2.fail_inputs(SchemaError('injected poison A, second delivery'))
    if record_faults is not None:
        record_faults(schedule)
    record['faults'] = schedule.fired()
    dlq = driver.dlq('child')
    assert len(dlq) == 1, [d['headers'] for d in dlq]
    headers = dlq[0]['headers']
    assert headers['VF-Code'] == 'VF_POISON_SCHEMA' and headers['VF-Disposition'] == 'poison', headers
    # One record under the stable id: the copy that won is either the outage-time
    # publication accepted late (a stalled broker) or the replacement's (a refused
    # one) — the rest were coalesced onto it.
    assert headers['VF-Num-Delivered'] in ('1', '2') and headers['VF-Origin-Node'] == 'child', headers
    assert headers['Nats-Msg-Id'].startswith(f'dlq:{driver.flow_id}:{driver.run_id}:child:'), headers
    record['terminal_record'] = {k: v for k, v in headers.items()}
    record['recovery_seconds'] = time.monotonic() - outage_started
    assert _settled(driver, 'child', 'parent', 20), driver.consumer_state('child', 'parent')
    status = w2.subscription_status()['parent']
    assert isinstance(status, Known) and status.value.unresolved == 0, status
    w2.quiesce()
    w2.close()


@pytest.mark.case('MSG-009')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_009_dlq_outage_at_final_delivery_cannot_strand_an_input(nats_url, evidence_dir, monkeypatch,
                                                                    record_faults) -> None:
    '''
    MSG-009 (P0, messaging, broker): DLQ outage at final delivery cannot strand an input.

    Acceptance: After DLQ restoration and within R, exactly one logical terminal outcome
    identifies A; duplicate physical DLQ publications are either deduplicated or coalesced by
    stable identity.

    The outage is real: the flow's DLQ stream is deleted, so the dead-letter publish
    gets a 503 no-responders. A's first failure is at its first broker delivery of
    two; the handoff at the *last* allowed attempt needs the ``pending_handoff``
    ledger record and is the pending ``ledger`` variant.
    '''
    flow, run = unique_ids('msg009')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_009(driver, record, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'handoff_ledger.json', record)


@pytest.mark.case('MSG-009')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_009_memory_dlq_outage_keeps_the_input_until_the_record_exists(evidence_dir, monkeypatch,
                                                                            record_faults) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_009(driver, record, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'handoff_ledger.json', record)


@pytest.mark.case('MSG-009')
@pytest.mark.level('broker')
@pytest.mark.variant('ledger')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_009_pending_handoff_record_survives_the_worker_restart(nats_url, evidence_dir, monkeypatch, tmp_path) -> None:
    '''
    DELIV-15's ``pending_handoff`` record: a dead-letter publish that is not accepted
    is written to the runtime store, so a replacement process retries it at start
    under the same ``Nats-Msg-Id`` — while the DLQ is still down the record stays;
    once it is back the very next replacement records the dead letter before it
    receives anything — and recovery never depends on a NAK past a broker cap
    (``max_deliver = -1``). Exactly one dead letter identifies A.
    '''
    flow, run = unique_ids('msg009l')
    driver = JetStreamDriver(nats_url, flow, run)
    root = str(tmp_path / 'ledger')
    record : Dict[str, Any] = {'flow': flow, 'run': run, 'store': f'file://{root}'}
    try:
        provision_with_ledger(driver, [spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False)],
                              BATCH, max_retries = 1)
        with messengers() as pool:
            w1 = pool.ledger_messenger(driver, root, 'child', ['parent'], max_retries = 1)
            a = driver.publish_parent('parent', 'A', 1, {'input': 'A'})
            group = driver.receive_group(w1, timeout = 30)
            assert group['parent']['message'] == {'input': 'A'}
            driver.dlq_outage('child')
            outage_started = time.monotonic()
            w1.fail_inputs(SchemaError('injected poison A', remedy = 'fix the producer'))
            assert driver.dlq('child') == [], 'a dead letter was claimed while the DLQ was unavailable'
            # The durable handoff intent, visible to a process that never saw the delivery.
            handoffs = ledger_runtime(root, flow, run, 'child').pending_handoffs()
            assert len(handoffs) == 1, handoffs
            handoff = handoffs[0]
            assert handoff.record_id.startswith(f'dlq:{flow}:{run}:child:') and handoff.headers['VF-Code'] == 'VF_POISON_SCHEMA'
            assert handoff.headers['Nats-Msg-Id'] == handoff.record_id
            kept = driver.consumer_state('child', 'parent')
            assert isinstance(kept, Known) and sum(kept.value) >= 1, kept
            assert status_unresolved(w1, 'parent') == 1, w1.subscription_status()
            record['during_outage'] = {'message_id': a, 'handoff': handoff.record_id, 'attempts': handoff.attempts,
                                       'consumer_state': kept.value}
            pool.close(w1)                                              # restart while the DLQ is still down
            w2 = pool.ledger_messenger(driver, root, 'child', ['parent'], max_retries = 1)
            still = ledger_runtime(root, flow, run, 'child').pending_handoffs()
            assert [h.record_id for h in still] == [handoff.record_id], 'the record did not survive the restart'
            assert driver.dlq('child') == []
            assert status_unresolved(w2, 'parent') == 1, 'the replacement does not see the stranded input'
            pool.close(w2)
            driver.dlq_restore('child')
            # The next replacement records the dead letter at start, from the ledger,
            # before it has received anything.
            w3 = pool.ledger_messenger(driver, root, 'child', ['parent'], max_retries = 1)
            assert ledger_runtime(root, flow, run, 'child').pending_handoffs() == []
            dlq = driver.dlq('child')
            assert len(dlq) == 1 and dlq[0]['headers']['Nats-Msg-Id'] == handoff.record_id, [d['headers'] for d in dlq]
            assert dlq[0]['headers']['VF-Num-Delivered'] == '1'
            record['recorded_at_start'] = {'seconds_after_outage': time.monotonic() - outage_started,
                                           'headers': dict(dlq[0]['headers'])}
            # A is redelivered (the broker cap is the ledger's), fails again, and its
            # terminal settlement coalesces onto the record already there.
            group = driver.receive_group(w3, timeout = 60)
            assert group['parent']['message'] == {'input': 'A'} and w3._inflight_handles[0].num_delivered == 2
            w3.fail_inputs(SchemaError('injected poison A, second delivery'))
            dlq = driver.dlq('child')
            assert len(dlq) == 1 and dlq[0]['headers']['Nats-Msg-Id'] == handoff.record_id, [d['headers'] for d in dlq]
            assert _settled(driver, 'child', 'parent', 20), driver.consumer_state('child', 'parent')
            assert status_unresolved(w3, 'parent') == 0
            assert ledger_runtime(root, flow, run, 'child').pending_handoffs() == []
            record['terminal_record'] = dict(dlq[0]['headers'])
            record['recovery_seconds'] = time.monotonic() - outage_started
    finally:
        driver.close()
        _write(evidence_dir, 'handoff_ledger.json', record)


@pytest.mark.negative_control(of = 'MSG-009')
def test_msg_009_detects_a_terminated_handoff(monkeypatch) -> None:
    '''A ladder that terminates the delivery when the dead-letter publish failed strands A: the oracle must catch it.'''
    defects.terminating_dlq_failure(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_009, driver, {})
    finally:
        driver.close()


# -- MSG-010 -------------------------------------------------------------------------

class _DictBlobStore(BlobStore):
    '''An offload store the test controls; the transient failure is injected at the ``payload.read.before`` barrier.'''
    def __init__(self) -> None:
        self._data : Dict[str, bytes] = {}
        self.gets = 0

    def put(self, data : bytes, ttl_seconds : int = 3600) -> str:
        ref = f'blob:{len(self._data) + 1}'
        self._data[ref] = data
        return ref

    def get(self, ref : str) -> bytes:
        self.gets += 1
        return self._data[ref]


def _unsupported_version(flow_id : str, run_id : str) -> bytes:
    '''A well-formed protobuf envelope that declares wire version 5 — decodable by nobody today.'''
    env = envelope_pb2.Envelope()
    env.ParseFromString(encode_envelope('parent', flow_id, run_id, 'v5', 9, MSG_TYPE_DATA, None, {'x': 1}))
    env.v = 5
    return env.SerializeToString()


def _oracle_msg_010(driver : Any, record : Dict[str, Any], flow_type : str,
                    record_faults : Callable[..., None] | None = None) -> None:
    '''
    Three specimens, each followed by a valid sentinel: malformed bytes, an
    unsupported wire version, and a valid envelope whose payload read fails once
    transiently. The first two get a dead letter carrying the raw bytes and a
    poison code; the third is retried and completes; every sentinel completes.
    '''
    driver.provision([spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False)],
                     flow_type, max_retries = 3)
    store = _DictBlobStore()
    w = driver.messenger('child', ['parent'], flow_type, max_retries = 3, ack_wait = 2, blob_store = store)
    malformed = b'\x80\x81 not an envelope'
    version5 = _unsupported_version(driver.flow_id, driver.run_id)
    specimens = [('malformed', malformed), ('unsupported-version', version5)]
    record['specimens'] = {name: {'sha256': digest(data), 'bytes': len(data)} for name, data in specimens}
    groups : List[Dict[str, Any]] = []
    errors : List[BaseException] = []
    stop = threading.Event()

    def consume() -> None:
        try:
            while not stop.is_set():
                group = w.receive_message()
                if all(v.get('is_stop_signal') for v in group.values()):
                    return
                groups.append({'message': group['parent']['message'],
                               'attempt': w._inflight_handles[0].num_delivered})
                w.ack_inputs()
        except BaseException as e:  # noqa: BLE001 — surfaced to the test thread
            errors.append(e)

    schedule = faults.FaultSchedule({'payload.read.before': faults.Nth(1, faults.RaiseError(
        lambda: TransientFailure('injected: payload store unreachable', remedy = 'retry')))})
    consumer = threading.Thread(target = consume, daemon = True, name = 'vf-conf-msg010')
    with schedule:
        consumer.start()
        for index, (name, data) in enumerate(specimens, start = 1):
            driver.publish_bytes('parent', data, f'poison-{index}')
            assert driver.until(lambda n = index: len(driver.dlq('child')) >= n, 30), \
                f'{name}: no terminal record within the poison handling deadline'
            driver.publish_parent('parent', f's{index}', index, {'sentinel': index})
            assert driver.until(lambda n = index: len(groups) >= n, 30), f'sentinel {index} did not complete'
        # A valid envelope whose payload lives in the store: the first read fails transiently.
        driver.publish_parent('parent', 'transient', 7, {'input': 'transient'}, blob_store = store, inline_threshold = 0)
        driver.publish_parent('parent', 's3', 3, {'sentinel': 3})
        assert driver.until(lambda: len(groups) >= 4, 60), groups
        stop.set()
        w.quiesce()
        consumer.join(15)
    if record_faults is not None:
        record_faults(schedule)
    assert not errors, errors
    record['groups'] = groups
    dlq = driver.dlq('child')
    record['dead_letters'] = [{'headers': d['headers'], 'sha256': digest(d['data'])} for d in dlq]
    assert len(dlq) == 2, [d['headers'] for d in dlq]
    by_digest = {digest(d['data']): d['headers'] for d in dlq}
    for name, data in specimens:
        headers = by_digest.get(digest(data))
        assert headers is not None, f'{name}: no dead letter carries the specimen bytes verbatim'
        assert headers['VF-Code'] == 'VF_POISON_DECODE' and headers['VF-Disposition'] == 'poison', headers
        assert headers['Nats-Msg-Id'].startswith(f'dlq:{driver.flow_id}:{driver.run_id}:child:'), headers
    transient = [g for g in groups if g['message'] == {'input': 'transient'}]
    assert len(transient) == 1 and transient[0]['attempt'] == 2, transient
    assert store.gets == 1, store.gets                          # the read that succeeded, after the injected failure
    sentinels = sorted(g['message']['sentinel'] for g in groups if 'sentinel' in (g['message'] or {}))
    assert sentinels == [1, 2, 3], groups
    drops = w.take_drops()
    assert drops.get('undecodable') == 2 and 'exhausted' not in drops, drops
    assert _settled(driver, 'child', 'parent', 20), driver.consumer_state('child', 'parent')
    w.close()


@pytest.mark.case('MSG-010')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_010_poison_wire_data_has_an_explicit_terminal_disposition(nats_url, evidence_dir, monkeypatch,
                                                                       record_faults) -> None:
    '''
    MSG-010 (P0, messaging, broker): Poison wire data has an explicit terminal disposition.

    Acceptance: Every malformed accepted reliable ID has a durable terminal record within the
    configured poison handling deadline, and sentinel completes; no transient infrastructure
    error is classified as malformed bytes.
    '''
    flow, run = unique_ids('msg010')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_010(driver, record, BATCH, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'poison_ledger.json', record)


@pytest.mark.case('MSG-010')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_010_memory_poison_bytes_are_dead_lettered_and_transient_reads_retried(evidence_dir, monkeypatch,
                                                                                    record_faults) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_010(driver, record, BATCH, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'poison_ledger.json', record)


@pytest.mark.case('MSG-010')
@pytest.mark.level('model')
@pytest.mark.variant('memory-terminal-log')
def test_msg_010_undecodable_bytes_under_a_sampled_out_dlq_hit_the_terminal_log(evidence_dir, monkeypatch) -> None:
    '''
    DELIV-15's other branch: under best-effort delivery the ERR-6 sampler decides
    whether a specimen is dead-lettered; when it admits none, undecodable bytes are
    terminated against the node's terminal log instead — still a durable record,
    never a bare TERM.
    '''
    driver = MemoryDriver('f', 'r')
    try:
        # A retained (BATCH) channel so the poison is not evicted before delivery,
        # with the child declared best-effort so the ERR-6 sampler decides.
        driver.provision([spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False)], BATCH)
        w = driver.messenger('child', ['parent'], BATCH, max_retries = 3, ack_wait = 2,
                             delivery = {'delivery': 'best-effort'})
        monkeypatch.setattr(w, '_dlq_sampler', _NeverSample())
        driver.publish_bytes('parent', b'\x80\x81 not an envelope', 'poison-1')
        driver.publish_parent('parent', 's1', 1, {'sentinel': 1})
        group = driver.receive_group(w, timeout = 30)
        assert group['parent']['message'] == {'sentinel': 1}
        w.ack_inputs()
        assert driver.dlq('child') == []
        entries = w.terminal_entries()
        assert len(entries) == 1 and entries[0]['reason'] == 'sampled-out', entries
        assert entries[0]['code'] == 'VF_POISON_DECODE'
        _write(evidence_dir, 'terminal_log.json', {'entries': entries})
        w.close()
    finally:
        driver.close()


class _NeverSample:
    '''A dead-letter sampler that admits no specimen (``dlq: off``, or every sample slot used).'''
    def admit(self, code : str, node : str) -> bool:
        return False


@pytest.mark.negative_control(of = 'MSG-010')
def test_msg_010_detects_a_catch_all_decode_terminator(monkeypatch) -> None:
    '''A handler that TERMs every decode-path exception without a record — and calls a store outage poison — must fail the oracle.'''
    defects.catch_all_decode_terminator(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_010, driver, {}, BATCH)
    finally:
        driver.close()


# -- MSG-011 -------------------------------------------------------------------------

def _oracle_msg_011(driver : Any, record : Dict[str, Any]) -> None:
    channel = driver.channel('p', RELIABLE_WORK, RETENTION_INTEREST)
    body = b'A:' + b'x' * 512
    bound = driver.bind(channel, 'c', receiver = 'w1', ack_wait = 1, max_deliver = 8, credit = 4)
    driver.publish(channel, 'A', body)
    first : List[Any] = []
    assert driver.until(lambda: first.extend(driver.receive(bound, timeout = 0.5)) or bool(first), 10)
    h1 = first[0]
    assert h1.token.attempt == 1
    # The lease expires without settlement; the broker redelivers as h2.
    second : List[Any] = []
    driver.wait(1.5)
    assert driver.until(lambda: second.extend(driver.receive(bound, timeout = 0.5)) or bool(second), 10)
    h2 = second[0]
    assert h2.token.message_id == h1.token.message_id and h2.token.attempt == 2, h2.token
    record['h1'] = str(h1.token)
    record['h2'] = str(h2.token)
    # Local replacement of h1, then a stale destructive settlement through it.
    record['supersede_h1'] = bound.backend.supersede(h1.token)
    stale = driver.settle(bound, h1.token, Terminal('ledger:stale-h1'))
    assert isinstance(stale, SettleStale), stale
    record['stale_settlement'] = str(stale)
    assert driver.retained(channel) == 1, 'a stale handle terminated the current delivery'
    # The h2 holder dies before completing; the replacement recovers input and payload.
    driver.kill('w1')
    replacement = driver.bind(channel, 'c', receiver = 'w2', ack_wait = 1, max_deliver = 8, credit = 4)
    third : List[Any] = []
    started = time.monotonic()
    assert driver.until(lambda: third.extend(driver.receive(replacement, timeout = 0.5)) or bool(third), 15, step = 0.5)
    h3 = third[0]
    assert h3.token.message_id == h1.token.message_id and h3.token.attempt >= 3, h3.token
    assert digest(h3.envelope_bytes) == digest(body)
    done = driver.settle(replacement, h3.token, Completed())
    assert isinstance(done, SettleConfirmed), done
    record['replacement'] = {'token': str(h3.token), 'payload_sha256': digest(h3.envelope_bytes),
                             'recovery_seconds': time.monotonic() - started}
    assert driver.until(lambda: driver.retained(channel) == 0, 10)


@pytest.mark.case('MSG-011')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_011_superseded_delivery_handles_cannot_terminally_discard_the(nats_url, evidence_dir) -> None:
    '''
    MSG-011 (P0, messaging, broker): Superseded delivery handles cannot terminally discard the
    current delivery.

    Acceptance: Input completes at the replacement within R; settlement history contains no TERM
    emitted as a side effect of local handle replacement.
    '''
    flow, run = unique_ids('msg011')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_011(driver, record)
    finally:
        driver.close()
        _write(evidence_dir, 'settlement_trace.json', record)


@pytest.mark.case('MSG-011')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_011_memory_stale_terminal_is_fenced_by_attempt(evidence_dir) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_011(driver, record)
    finally:
        driver.close()
        _write(evidence_dir, 'settlement_trace.json', record)


@pytest.mark.negative_control(of = 'MSG-011')
def test_msg_011_detects_an_unfenced_settlement(monkeypatch) -> None:
    defects.unfenced_settle(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_011, driver, {})
    finally:
        driver.close()


# -- MSG-012 -------------------------------------------------------------------------

def _oracle_msg_012(driver : Any, record : Dict[str, Any], record_faults : Callable[..., None] | None = None) -> None:
    channel = driver.channel('p', RELIABLE_WORK, RETENTION_INTEREST)
    bound = driver.bind(channel, 'c', receiver = 'w1', ack_wait = 1, max_deliver = 1, credit = 4)
    driver.publish(channel, 'A', b'A')
    got : List[Any] = []
    assert driver.until(lambda: got.extend(driver.receive(bound, timeout = 0.5)) or bool(got), 10)
    delivery = got[0]
    driver.settle(bound, delivery.token, Retry(0.0))            # the final delivery, handed back: stranded
    driver.wait(1.5)
    driver.receive(bound, timeout = 0.2)                         # nothing comes: the cap is reached

    def stranded_visible() -> bool:
        observed = driver.observe(bound)
        return isinstance(observed, Known) and observed.value.unresolved >= 1
    assert driver.until(stranded_visible, 15), driver.observe(bound)
    before = driver.observe(bound)
    assert isinstance(before, Known)
    assert before.value.available == 0 and before.value.leased == 0 and before.value.unresolved >= 1, before.value
    assert driver.retained(channel) == 1
    record['before_fault'] = str(before.value)
    # Deny the management API: an injected exception, then a real client timeout.
    schedule = faults.FaultSchedule({'observe.subscription.before': faults.RaiseError(
        lambda: PermissionError('injected: consumer info denied'))})
    with schedule:
        denied = driver.observe(bound)
    if record_faults is not None:
        record_faults(schedule)
    assert isinstance(denied, Unknown) and denied.reason == 'api', denied
    driver.deny_observation(bound, 'timeout')
    try:
        timed_out = driver.observe(bound)
    finally:
        driver.restore_observation(bound)
    assert isinstance(timed_out, Unknown), timed_out
    record['during_fault'] = [str(denied), str(timed_out)]
    # Restore, then resolve the stranded input: a durable record and its removal.
    restored = driver.observe(bound)
    assert isinstance(restored, Known) and restored.value.unresolved >= 1, restored
    record['resolution_record'] = driver.resolve_stranded(bound, delivery)
    after = driver.observe(bound)
    assert isinstance(after, Known), after
    assert after.value == type(after.value)(0, 0, 0, after.value.dropped, after.value.rejected_publications,
                                            after.value.observed_at, after.value.generation), after.value
    assert driver.retained(channel) == 0
    record['after_resolution'] = str(after.value)


@pytest.mark.case('MSG-012')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_012_status_distinguishes_empty_unavailable_and_stranded_work(nats_url, evidence_dir, monkeypatch,
                                                                          record_faults) -> None:
    '''
    MSG-012 (P0, messaging, broker): Status distinguishes empty, unavailable, and stranded work.

    Acceptance: Status returns unresolved>=1 or an equivalent explicit exhausted-work state
    before resolution; API failure yields unknown, never healthy empty.

    The stranded count is fed by the broker's ``MAX_DELIVERIES`` advisory, which
    only the receiver that saw it holds; a stranded record that survives a worker
    restart is the pending ``ledger`` variant.
    '''
    flow, run = unique_ids('msg012')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_012(driver, record, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'status_trace.json', record)


@pytest.mark.case('MSG-012')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_012_memory_status_is_unresolved_then_unknown_then_safe_zero(evidence_dir, record_faults) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_012(driver, record, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'status_trace.json', record)


@pytest.mark.case('MSG-012')
@pytest.mark.level('broker')
@pytest.mark.variant('ledger')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_012_stranded_work_is_durable_across_worker_restarts(nats_url, evidence_dir, monkeypatch, tmp_path) -> None:
    '''
    Under the ledger budget nothing is stranded by a broker cap, so exhausted work
    is what the *ledger* says is exhausted and unrecorded: an input whose counted
    attempts are spent and whose dead letter could not be written. That record
    (``pending_handoff``) survives the process that failed it — a replacement
    reports ``unresolved >= 1`` on the same subscription and the broker's counters
    never read as all-done — an API failure is Unknown, never empty, and the
    node's terminal log (a delivery ended against the ledger, ``dlq: off``) is
    read back by a new process too.
    '''
    flow, run = unique_ids('msg012l')
    driver = JetStreamDriver(nats_url, flow, run)
    root = str(tmp_path / 'ledger')
    record : Dict[str, Any] = {'flow': flow, 'run': run, 'store': f'file://{root}'}
    try:
        provision_with_ledger(driver, [spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False),
                                       spec('parent_t', [], 'producer', True), spec('sink', ['parent_t'], 'consumer', False)],
                              BATCH, max_retries = 0)
        with messengers() as pool:
            w1 = pool.ledger_messenger(driver, root, 'child', ['parent'], max_retries = 0)
            a = driver.publish_parent('parent', 'A', 1, {'input': 'A'})
            group = driver.receive_group(w1, timeout = 30)
            assert group['parent']['message'] == {'input': 'A'}
            driver.dlq_outage('child')
            w1.fail_inputs(TransientFailure('injected transient at the budget', remedy = 'retry'))   # budget of one: exhausted
            assert w1.take_drops().get('exhausted') == 1
            before = w1.subscription_status()['parent']
            assert isinstance(before, Known) and before.value.unresolved >= 1, before
            assert (before.value.available, before.value.leased) != (0, 0) or before.value.unresolved >= 1
            fresh = ledger_runtime(root, flow, run, 'child')            # a process that never saw the delivery
            assert len(fresh.pending_handoffs()) == 1, fresh.pending_handoffs()
            assert fresh.pending_handoffs()[0].record_id.startswith(f'dlq:{flow}:{run}:child:')
            record['before_restart'] = {'message_id': a, 'observation': str(before.value),
                                        'handoffs': [h.record_id for h in fresh.pending_handoffs()]}
            pool.close(w1)
            # A replacement, during the outage: the stranded input is its unresolved
            # work too, and an API failure is unknown, never empty.
            w2 = pool.ledger_messenger(driver, root, 'child', ['parent'], max_retries = 0)
            assert status_unresolved(w2, 'parent') == 1, w2.subscription_status()
            replacement_view = w2.subscription_status()['parent']
            assert isinstance(replacement_view, Known) and not (replacement_view.value.available == 0
                                                                and replacement_view.value.leased == 0
                                                                and replacement_view.value.unresolved == 0)
            schedule = faults.FaultSchedule({'observe.subscription.before': faults.RaiseError(
                lambda: PermissionError('injected: consumer info denied'))})
            with schedule:
                denied = w2.subscription_status()['parent']
            assert isinstance(denied, Unknown) and denied.reason == 'api', denied
            record['during_fault'] = str(denied)
            record['replacement_view'] = str(replacement_view.value)
            pool.close(w2)
            # Restore and resolve: the next process records the dead letter at start,
            # and A's next delivery is terminated against it.
            driver.dlq_restore('child')
            w3 = pool.ledger_messenger(driver, root, 'child', ['parent'], max_retries = 0)
            assert ledger_runtime(root, flow, run, 'child').pending_handoffs() == []
            assert len(driver.dlq('child')) == 1
            group = driver.receive_group(w3, timeout = 60)
            assert group['parent']['message'] == {'input': 'A'}
            w3.fail_inputs(TransientFailure('injected transient after restoration'))
            assert _settled(driver, 'child', 'parent', 20), driver.consumer_state('child', 'parent')
            after = w3.subscription_status()['parent']
            assert isinstance(after, Known) and (after.value.available, after.value.leased, after.value.unresolved) == (0, 0, 0)
            assert len(driver.dlq('child')) == 1
            record['after_resolution'] = {'observation': str(after.value), 'dead_letter': dict(driver.dlq('child')[0]['headers'])}
            # The terminal log: a delivery ended without a dead letter (dlq off) leaves a
            # ledger record that a new process reads back.
            sink = pool.ledger_messenger(driver, root, 'sink', ['parent_t'], max_retries = 0, delivery = {'dlq': 'off'})
            driver.publish_parent('parent_t', 'T', 1, {'input': 'T'})
            group = driver.receive_group(sink, timeout = 30)
            assert group['parent_t']['message'] == {'input': 'T'}
            sink.fail_inputs(SchemaError('injected poison T'))
            pool.close(sink)
            entries = ledger_runtime(root, flow, run, 'sink').terminal_entries()
            assert len(entries) == 1 and entries[0]['code'] == 'VF_POISON_SCHEMA' and entries[0]['reason'] == 'terminated', entries
            assert _settled(driver, 'sink', 'parent_t', 20)
            record['terminal_log'] = entries
    finally:
        driver.close()
        _write(evidence_dir, 'stranded_ledger.json', record)


@pytest.mark.negative_control(of = 'MSG-012')
def test_msg_012_detects_a_zero_on_failure_observation(monkeypatch) -> None:
    defects.zero_on_failure_observation(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_012, driver, {})
    finally:
        driver.close()
