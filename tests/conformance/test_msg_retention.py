'''
Conformance cases: MSG-002, MSG-003, MSG-016, MSG-018, MSG-024, MSG-025.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions over a driver (``_msgdrivers``),
so the broker-level primary, the ``memory`` variant and the negative control decide
with the same assertions.

Retention is where a backend's declaration matters most, so three of these cases
end in a truthful rejection on JetStream: it declares no per-key latest slot
(MSG-003), no durable control ledger (MSG-018) and no archive independent of the
working consumers (MSG-024), and the planner refuses those profiles before a stream
exists — the reference model, which implements them, carries the full oracle. A
Core-NATS adapter (MSG-025) does not exist yet, so that primary is NOT_RUN.
'''
from __future__ import absolute_import, division, print_function

import json
import time
from typing import Any, Dict, List

import defects
import pytest
from _brokers import redis_client, unique_ids
from _msgdrivers import JetStreamDriver, MemoryDriver, digest, spec
from _status import not_run, unsupported

from videoflow.backends.capabilities import (
    DURABLE_CONTROL,
    LIVE_LATEST,
    RELIABLE_WORK,
    REPLAY_ARCHIVE,
    RETENTION_INTEREST,
    RETENTION_LIMITS,
    FlowRequirements,
    PayloadCapabilities,
    ProfileRequest,
    RuntimeCapabilities,
    default_requirements,
    plan_composition,
)
from videoflow.backends.messaging import KIND_EOS, SUBSCRIPTION_EOS, Completed, Retry
from videoflow.backends.outcomes import (
    Accepted,
    Known,
    PublicationUnresolvable,
    Rejected,
    SettleConfirmed,
    SettleUnknown,
    known,
)
from videoflow.core.constants import REALTIME
from videoflow.core.errors import IncompatibleProfile
from videoflow.wire.serialization import MSG_TYPE_DATA, RedisBlobStore, encode_envelope, peek_envelope

NATS_TIMEOUT = 180
_DURABLE_RUNTIME = RuntimeCapabilities('redis', durable = known(True), shared_across_processes = True,
                                       restart_safe_joins = True, elastic_state = False)


def _write(evidence_dir : Any, name : str, record : Dict[str, Any]) -> None:
    (evidence_dir / name).write_text(json.dumps(record, indent = 2, default = str))


def _collect(driver : Any, bound : Any, wanted : int, timeout : float, settle : bool = True) -> List[Any]:
    got : List[Any] = []

    def step() -> bool:
        for delivery in driver.receive(bound, timeout = 0.5):
            got.append(delivery)
            if settle:
                driver.settle(bound, delivery.token, Completed())
        return len(got) >= wanted
    driver.until(step, timeout)
    return got


# -- MSG-002 -------------------------------------------------------------------------

SPECS_002 = [spec('P', [], 'producer', True),
             spec('alerts', ['P'], 'consumer', False, delivery = {'delivery': 'at-least-once'}),
             spec('display', ['P'], 'consumer', False)]


def _oracle_msg_002(driver : Any, record : Dict[str, Any]) -> None:
    '''
    A live graph whose alert consumer opts into at-least-once: the compiler must
    either reject the mixed retention on one channel or serve both classes; and on
    the composition that runs, alert A must survive its retry while B and the
    display's single-slot traffic overwrite everything around it.
    '''
    requirements = default_requirements(REALTIME, SPECS_002)
    record['requirements'] = [r.to_dict() for r in requirements.profiles]
    assert {r.profile for r in requirements.profiles if r.channel == 'P'} == {LIVE_LATEST, RELIABLE_WORK}, requirements
    capabilities = driver.capabilities()
    try:
        plan = plan_composition(requirements, capabilities)
    except IncompatibleProfile as e:
        assert e.code == 'VF_INCOMPATIBLE_PROFILE' and 'P' in e.context.get('channels', ()), e
        assert driver.channel_ids() == [], 'a rejected graph left channels behind'
        record['admission'] = {'outcome': 'rejected', 'message': str(e)[:300]}
        alerts_channel = driver.channel('P-alerts', RELIABLE_WORK, RETENTION_INTEREST)
        display_channel = driver.channel('P-display', LIVE_LATEST, RETENTION_LIMITS, max_msgs = 1)
    else:
        retention = plan.channel_retention['P']
        record['admission'] = {'outcome': 'admitted', 'retention': retention, 'notes': list(plan.notes)}
        alerts_channel = display_channel = driver.channel('P', plan.channel_profiles['P'], retention,
                                                          max_msgs = 1 if retention == RETENTION_LIMITS else 100)
    alerts = driver.bind(alerts_channel, 'alerts', receiver = 'alerts', ack_wait = 5, max_deliver = 8, credit = 4, prefetch = 1)
    display = driver.bind(display_channel, 'display', receiver = 'display', ack_wait = 5, max_deliver = 1, credit = 4, prefetch = 1)

    def produce(pid : str) -> None:
        for channel in {alerts_channel, display_channel}:
            out = driver.publish(channel, pid, pid.encode())
            assert isinstance(out, Accepted), out
    ledger : List[Dict[str, Any]] = []
    produce('A')
    ledger.append({'event': 'published', 'id': 'A', 'at': time.monotonic()})
    fetched = _collect(driver, alerts, 1, 15, settle = False)
    assert fetched and fetched[0].token.message_id == 'A', fetched
    retry = driver.settle(alerts, fetched[0].token, Retry(3.0))
    assert isinstance(retry, SettleConfirmed), retry
    ledger.append({'event': 'retry requested', 'id': 'A', 'delay': 3.0, 'at': time.monotonic()})
    produce('B')                                                # before the retry becomes eligible
    ledger.append({'event': 'published', 'id': 'B', 'at': time.monotonic()})
    driver.kill('alerts')                                       # the reliable subscriber reconnects
    reconnected = driver.bind(alerts_channel, 'alerts', receiver = 'alerts-2', ack_wait = 5, max_deliver = 8, credit = 4, prefetch = 1)
    completed = {d.token.message_id: d.token.attempt for d in _collect(driver, reconnected, 2, 30)}
    ledger.append({'event': 'completed', 'ids': completed, 'at': time.monotonic()})
    record['ledger'] = ledger
    assert set(completed) == {'A', 'B'}, f'reliable completion set {sorted(completed)}'
    assert completed['A'] >= 2, completed
    seen_by_display : set = set()

    def display_settled() -> bool:
        for delivery in driver.receive(display, timeout = 0.3):
            seen_by_display.add(delivery.token.message_id)
            driver.settle(display, delivery.token, Completed())
        observed = driver.observe(display)
        return 'B' in seen_by_display or (isinstance(observed, Known) and observed.value.dropped >= 1)
    driver.until(display_settled, 15)
    observed = driver.observe(display)
    assert isinstance(observed, Known), observed
    record['display'] = {'received': sorted(seen_by_display), 'observation': str(observed.value)}
    if display_channel is not alerts_channel:
        # The single-slot display either got the newest frame or reports the overwrite of the older one.
        assert 'B' in seen_by_display or observed.value.dropped >= 1, record['display']


@pytest.mark.case('MSG-002')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_002_reliable_alerts_survive_concurrent_newer_publications_in_a(nats_url, evidence_dir) -> None:
    '''
    MSG-002 (P0, messaging, broker): Reliable alerts survive concurrent newer publications in a
    live graph.

    Acceptance: Observed successful ID set equals {A,B}; no accepted reliable ID disappears
    during latest-only traffic. Backend configurations that cannot satisfy the graph reject it
    before start.
    '''
    flow, run = unique_ids('msg002')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_002(driver, record)
        assert record['admission']['outcome'] == 'rejected', record['admission']    # JetStream keeps one retention class per channel
    finally:
        driver.close()
        _write(evidence_dir, 'retention_ledger.json', record)


@pytest.mark.case('MSG-002')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_002_memory_mixed_retention_is_rejected_and_the_separated_graph_keeps_a(evidence_dir) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_002(driver, record)
        assert record['admission']['outcome'] == 'rejected', record['admission']
    finally:
        driver.close()
        _write(evidence_dir, 'retention_ledger.json', record)


@pytest.mark.case('MSG-002')
@pytest.mark.level('model')
@pytest.mark.variant('memory-mixed')
def test_msg_002_memory_a_backend_with_mixed_retention_serves_both_classes(evidence_dir) -> None:
    '''A backend that declares mixed retention per channel is admitted, and must still keep A.'''
    driver = MemoryDriver('f', 'r', mixed_retention = True)
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_002(driver, record)
        assert record['admission']['outcome'] == 'admitted', record['admission']
    finally:
        driver.close()
        _write(evidence_dir, 'retention_ledger.json', record)


@pytest.mark.negative_control(of = 'MSG-002')
def test_msg_002_detects_requirements_blind_to_the_delivery_override(monkeypatch) -> None:
    '''Requirements that ignore the per-node override admit the graph as all-live: A is overwritten by B.'''
    defects.override_blind_requirements(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_002, driver, {})
    finally:
        driver.close()


# -- MSG-003 -------------------------------------------------------------------------

MAX_AGE = 0.2


def _oracle_msg_003(driver : Any, record : Dict[str, Any]) -> None:
    request = FlowRequirements(profiles = (ProfileRequest('cam', LIVE_LATEST, {'latest_per_key': True}),))
    plan = plan_composition(request, driver.capabilities())
    assert plan.channel_profiles == {'cam': LIVE_LATEST}
    channel = driver.channel('cam', LIVE_LATEST, RETENTION_LIMITS, max_msgs = 1, max_age = MAX_AGE, per_subject = True)
    proc = driver.bind(channel, 'proc', receiver = 'proc', ack_wait = 1, max_deliver = 1, credit = 8, prefetch = 1)
    published : List[str] = []

    def frame(pid : str, key : str) -> None:
        out = driver.publish(channel, pid, pid.encode(), partition_key = key, event_ts = driver.clock.time())
        assert isinstance(out, Accepted), out
        published.append(pid)
    # Processing paused: A1, B1, then A2..A100 with B untouched.
    frame('A1', 'A')
    frame('B1', 'B')
    for i in range(2, 101):
        frame(f'A{i}', 'A')
    waiting = driver.retained_ids(channel)
    observed = driver.observe(proc)
    assert isinstance(observed, Known)
    record['at_resume'] = {'waiting': waiting, 'observation': str(observed.value), 'published': len(published)}
    assert set(waiting) == {'A100', 'B1'}, waiting
    assert observed.value.dropped == 99, observed.value
    # Resume while both are within the requested age.
    driver.wait(MAX_AGE / 2)
    resumed = driver.receive(proc, timeout = 0.2, item_credit = 8)
    processed = [d.token.message_id for d in resumed]
    for delivery in resumed:
        assert isinstance(driver.settle(proc, delivery.token, Completed()), SettleConfirmed)
    assert sorted(processed) == ['A100', 'B1'], processed
    # Advance past the age limit: an expired waiting frame never reaches processing.
    frame('A101', 'A')
    frame('B2', 'B')
    driver.wait(MAX_AGE + 0.05)
    expired = [d.token.message_id for d in driver.receive(proc, timeout = 0.2)]
    assert expired == [], expired
    observed = driver.observe(proc)
    assert isinstance(observed, Known) and observed.value.dropped == 101, observed.value
    # The documented in-flight exception, measured apart: a frame handed out before
    # its age passed may still be settled, and a settlement after eviction says so.
    frame('A102', 'A')
    in_flight = driver.receive(proc, timeout = 0.2, item_credit = 8)
    assert [d.token.message_id for d in in_flight] == ['A102'], in_flight
    driver.wait(MAX_AGE + 0.05)
    late_settlement = driver.settle(proc, in_flight[0].token, Completed())
    in_flight_exceptions = 1 if isinstance(late_settlement, SettleUnknown) else 0
    # A slow subscriber reconnecting with more frames queued: only the latest per key waits.
    driver.kill('proc')
    slow = driver.bind(channel, 'proc', receiver = 'proc-slow', ack_wait = 1, max_deliver = 1, credit = 8, prefetch = 1)
    for pid in ('A103', 'A104', 'A105'):
        frame(pid, 'A')
    assert driver.retained_ids(channel) == ['A105'], driver.retained_ids(channel)
    slow_deliveries = driver.receive(slow, timeout = 0.2, item_credit = 8)
    reconnected = [d.token.message_id for d in slow_deliveries]
    for delivery in slow_deliveries:
        driver.settle(slow, delivery.token, Completed())
    assert reconnected == ['A105'], reconnected
    final = driver.observe(slow)
    assert isinstance(final, Known)
    processed_total = len(processed) + len(in_flight) + len(reconnected)
    record['reconciliation'] = {'published': len(published), 'processed': processed_total, 'dropped': final.value.dropped,
                                'in_flight_exceptions': in_flight_exceptions, 'late_settlement': str(late_settlement)}
    assert len(published) == processed_total + final.value.dropped - in_flight_exceptions, record['reconciliation']
    assert driver.retained(channel) <= 2                         # one waiting slot per key, bytes bounded with it


@pytest.mark.case('MSG-003')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_003_latest_value_behavior_is_bounded_and_scoped_to_the(nats_url, evidence_dir) -> None:
    '''
    MSG-003 (P1, messaging, broker): Latest-value behavior is bounded and scoped to the
    requested camera key.

    Acceptance: At resume the eligible waiting set includes A100 and B1; after expiry no expired
    waiting payload is processed. Queue and drop counters reconcile with generated IDs.

    JetStream declares ``latest_per_key = False``: the per-key request is an expected
    rejection before provisioning, and the case is UNSUPPORTED there — the model
    variant carries the full oracle.
    '''
    flow, run = unique_ids('msg003')
    driver = JetStreamDriver(nats_url, flow, run, REALTIME)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        capabilities = driver.capabilities()
        record['declared'] = {'latest_per_key': capabilities.latest_per_key, 'adapter': capabilities.adapter,
                              'version': capabilities.version}
        request = FlowRequirements(profiles = (ProfileRequest('cam', LIVE_LATEST, {'latest_per_key': True}),))
        with pytest.raises(IncompatibleProfile) as info:
            plan_composition(request, capabilities)
        assert 'per-key' in str(info.value) and info.value.remedy
        assert driver.channel_ids() == [], 'the rejection left channels behind'
        record['rejection'] = str(info.value)[:300]
    finally:
        driver.close()
        _write(evidence_dir, 'admission.json', record)
    unsupported('jetstream declares no per-key latest-value queue (latest_per_key=False); the request was rejected '
                'before provisioning with zero side effects')


@pytest.mark.case('MSG-003')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_003_memory_per_key_slots_are_scoped_and_age_bounded(evidence_dir) -> None:
    driver = MemoryDriver('f', 'r', latest_per_key = True)
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_003(driver, record)
    finally:
        driver.close()
        _write(evidence_dir, 'per_key_ledger.json', record)


@pytest.mark.negative_control(of = 'MSG-003')
def test_msg_003_detects_one_global_slot(monkeypatch) -> None:
    defects.global_slot_latest(monkeypatch)
    driver = MemoryDriver('f', 'r', latest_per_key = True)
    try:
        assert defects.detects(_oracle_msg_003, driver, {})
    finally:
        driver.close()


# -- MSG-016 -------------------------------------------------------------------------

KB, MB = 1_000, 1_000_000
BYTE_BUDGET = 2_500_000


def _body(i : int) -> bytes:
    return f'{i}:'.encode() + (b'x' * (MB if i % 2 else KB))


def _oracle_msg_016(driver : Any, record : Dict[str, Any]) -> None:
    trace : Dict[str, Any] = {}
    # Reliable, count budget: the fifth is refused with a typed outcome; nothing accepted is evicted.
    counted = driver.channel('rel-count', RELIABLE_WORK, RETENTION_INTEREST, max_msgs = 4, max_bytes = BYTE_BUDGET)
    reader = driver.bind(counted, 'r', receiver = 'paused-r', ack_wait = 60, max_deliver = 4, credit = 8, prefetch = 1)
    outcomes = [driver.publish(counted, f'm{i}', _body(i)) for i in range(6)]
    accepted = [i for i, o in enumerate(outcomes) if isinstance(o, Accepted)]
    refused = [(i, o) for i, o in enumerate(outcomes) if isinstance(o, Rejected)]
    assert accepted == [0, 1, 2, 3], [str(o) for o in outcomes]
    assert [i for i, _o in refused] == [4, 5], refused
    assert all(o.retryable and ('messages' in o.reason or 'bytes' in o.reason) for _i, o in refused), refused
    assert driver.retained(counted) == 4
    observed = driver.observe(reader)
    assert isinstance(observed, Known)
    trace['reliable_count'] = {'accepted': accepted, 'refused': [str(o) for _i, o in refused], 'observation': str(observed.value)}
    got = _collect(driver, reader, 4, 30)
    assert {digest(d.envelope_bytes) for d in got} == {digest(_body(i)) for i in accepted}, 'accepted inputs are not all readable'
    # Reliable, byte budget.
    sized = driver.channel('rel-bytes', RELIABLE_WORK, RETENTION_INTEREST, max_msgs = 100, max_bytes = BYTE_BUDGET)
    driver.bind(sized, 'r', receiver = 'paused-r2', ack_wait = 60, max_deliver = 4, credit = 8, prefetch = 1)
    outcomes = [driver.publish(sized, f'b{i}', _body(i)) for i in range(6)]
    accepted = [i for i, o in enumerate(outcomes) if isinstance(o, Accepted)]
    refused = [(i, o) for i, o in enumerate(outcomes) if isinstance(o, Rejected)]
    assert accepted == [0, 1, 2, 3, 4], [str(o) for o in outcomes]
    assert [i for i, _o in refused] == [5] and 'bytes' in refused[0][1].reason, refused
    assert driver.retained(sized) == 5
    trace['reliable_bytes'] = {'accepted': accepted, 'refused': [str(o) for _i, o in refused]}
    # Live, count budget: the documented policy evicts, and the drops are reported.
    # More frames than the buffer and the paused reader's own hand together can
    # hold, so some are lost before any subscription received them.
    live = driver.channel('live-count', LIVE_LATEST, RETENTION_LIMITS, max_msgs = 4)
    viewer = driver.bind(live, 'v', receiver = 'paused-v', ack_wait = 60, max_deliver = 1, credit = 8, prefetch = 1)
    outcomes = [driver.publish(live, f'l{i}', _body(i)) for i in range(10)]
    assert all(isinstance(o, Accepted) for o in outcomes), [str(o) for o in outcomes]
    assert driver.until(lambda: driver.retained(live) == 4, 5), driver.retained(live)
    observed = driver.observe(viewer)
    assert isinstance(observed, Known) and observed.value.dropped >= 2, observed
    trace['live_count'] = {'retained': driver.retained(live), 'observation': str(observed.value)}
    # Live, byte budget.
    live_bytes = driver.channel('live-bytes', LIVE_LATEST, RETENTION_LIMITS, max_msgs = 100, max_bytes = BYTE_BUDGET)
    viewer2 = driver.bind(live_bytes, 'v', receiver = 'paused-v2', ack_wait = 60, max_deliver = 1, credit = 8, prefetch = 1)
    outcomes = [driver.publish(live_bytes, f'lb{i}', _body(i)) for i in range(12)]
    assert all(isinstance(o, Accepted) for o in outcomes), [str(o) for o in outcomes]
    assert driver.until(lambda: driver.retained(live_bytes) <= 5, 5), driver.retained(live_bytes)
    observed = driver.observe(viewer2)
    assert isinstance(observed, Known) and observed.value.dropped >= 1, observed
    trace['live_bytes'] = {'retained': driver.retained(live_bytes), 'observation': str(observed.value)}
    # Offloaded payloads: an evictable store cannot back reliable references, whatever the envelope limit says.
    evictable = PayloadCapabilities('redis', durable = known(False), evictable = known(True), atomic_multikey = known(True),
                                    max_object_bytes = None, reader_identities = True)
    with pytest.raises(IncompatibleProfile) as info:
        plan_composition(FlowRequirements(profiles = (ProfileRequest('rel-count', RELIABLE_WORK),)), driver.capabilities(),
                         payload = evictable, payload_refs_in_use = True)
    trace['evictable_store_admission'] = str(info.value)[:300]
    record['trace'] = trace


@pytest.mark.case('MSG-016')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_016_backpressure_enforces_both_envelope_count_and_payload_byte(nats_url, redis_url, evidence_dir) -> None:
    '''
    MSG-016 (P0, integration, broker): Backpressure enforces both envelope count and payload-
    byte budgets.

    Acceptance: Observed retained payload bytes plus reserved admissions never exceed the
    declared budget by more than explicitly bounded in-flight reservations; accepted reliable
    inputs remain readable.
    '''
    flow, run = unique_ids('msg016')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_016(driver, record)
        # Payload bytes held outside the broker, measured on the store itself.
        with redis_client(redis_url) as client:
            policy = client.config_get('maxmemory-policy').get('maxmemory-policy')
            observed_store = PayloadCapabilities('redis', durable = known(policy == 'noeviction'),
                                                 evictable = known(policy != 'noeviction'), atomic_multikey = known(True),
                                                 max_object_bytes = None, reader_identities = True)
            store = RedisBlobStore(redis_url)
            offload = driver.channel('rel-offload', RELIABLE_WORK, RETENTION_INTEREST, max_msgs = 4)
            driver.bind(offload, 'r', receiver = 'paused-r3', ack_wait = 60, max_deliver = 4, credit = 8, prefetch = 1)
            outcomes, keys = [], []
            for i in range(6):
                buf = encode_envelope('rel-offload', flow, run, f't{i}', i, MSG_TYPE_DATA, None, b'x' * MB,
                                      blob_store = store, inline_threshold = 0)
                keys.append(peek_envelope(buf)['blob_ref'])
                outcomes.append(driver.publish(offload, f'o{i}', buf))
            accepted = [i for i, o in enumerate(outcomes) if isinstance(o, Accepted)]
            reserved = [i for i, o in enumerate(outcomes) if isinstance(o, Rejected)]
            # MEMORY USAGE per key: this run's bytes, on a store other runs share.
            usage = {keys[i]: int(client.memory_usage(keys[i]) or 0) for i in range(6)}
            retained_bytes = sum(usage[keys[i]] for i in accepted)
            reserved_bytes = sum(usage[keys[i]] for i in reserved)
            budget = 4 * MB
            overhead = 64 * 1024                                          # allocator and key overhead per object
            record['payload_store'] = {'policy': policy, 'accepted': accepted, 'in_flight_reservations': reserved,
                                       'retained_payload_bytes': retained_bytes, 'reserved_payload_bytes': reserved_bytes,
                                       'declared_budget': budget}
            assert accepted == [0, 1, 2, 3] and reserved == [4, 5], [str(o) for o in outcomes]
            assert retained_bytes <= budget + len(accepted) * overhead, record['payload_store']
            assert retained_bytes + reserved_bytes <= budget + len(reserved) * (MB + overhead) + len(accepted) * overhead, \
                record['payload_store']
            assert all(len(store.get(keys[i])) >= MB for i in accepted)   # accepted inputs stay readable (encoded, so framing adds bytes)
            # The store this broker is composed with is evictable: reliable references are refused before start.
            admission = FlowRequirements(profiles = (ProfileRequest('rel-offload', RELIABLE_WORK),))
            if observed_store.evictable == known(True).__class__(True, 0.0) or policy != 'noeviction':
                with pytest.raises(IncompatibleProfile):
                    plan_composition(admission, driver.capabilities(), payload = observed_store, payload_refs_in_use = True)
                record['payload_store']['admission'] = 'rejected (evictable store)'
            else:
                plan_composition(admission, driver.capabilities(), payload = observed_store, payload_refs_in_use = True)
                record['payload_store']['admission'] = 'admitted (durable store)'
    finally:
        driver.close()
        _write(evidence_dir, 'capacity_snapshots.json', record)


@pytest.mark.case('MSG-016')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_016_memory_count_and_byte_budgets_reject_or_evict_as_declared(evidence_dir) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_016(driver, record)
    finally:
        driver.close()
        _write(evidence_dir, 'capacity_snapshots.json', record)


@pytest.mark.negative_control(of = 'MSG-016')
def test_msg_016_detects_a_count_only_backpressure(monkeypatch) -> None:
    defects.count_only_backpressure(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_016, driver, {})
    finally:
        driver.close()


# -- MSG-018 -------------------------------------------------------------------------

def _oracle_msg_018(driver : Any, record : Dict[str, Any]) -> None:
    request = FlowRequirements(profiles = (ProfileRequest('frames', DURABLE_CONTROL),))
    plan = plan_composition(request, driver.capabilities(), runtime = _DURABLE_RUNTIME)
    assert plan.channel_profiles == {'frames': DURABLE_CONTROL}
    backend = driver.backend
    frames = driver.channel('frames', LIVE_LATEST, RETENTION_LIMITS, max_msgs = 1, per_subject = True)
    driver.bind(frames, 'viewer', receiver = 'viewer', ack_wait = 5, max_deliver = 1, credit = 4, prefetch = 1)
    completion = json.dumps({'state': 'completed', 'epoch': 1, 'final_sequence': 1}).encode()
    abort = json.dumps({'state': 'aborted', 'epoch': 1, 'reason': 'injected'}).encode()
    trace : List[Dict[str, Any]] = []
    assert isinstance(driver.publish(frames, 'A', b'A'), Accepted)
    assert backend.commit_control(frames, 'epoch/1', None, completion) is True
    assert isinstance(driver.publish(frames, 'C', b'EOS', kind = KIND_EOS), Accepted)
    assert isinstance(driver.publish(frames, 'B', b'B'), Accepted)             # delayed data after completion
    trace.append({'step': 'A, C(completed), EOS, B', 'retained_data': driver.retained_ids(frames),
                  'retained_eos': driver.retained_ids(frames, KIND_EOS), 'control': backend.control_state(frames, 'epoch/1')})
    assert driver.retained_ids(frames) == ['B'] and driver.retained_ids(frames, KIND_EOS) == ['C'], trace[-1]
    assert backend.control_state(frames, 'epoch/1') == (completion, '1')
    # Data pressure on a full reliable channel, then an authoritative abort for the same epoch.
    work = driver.channel('work', RELIABLE_WORK, RETENTION_INTEREST, max_msgs = 2)
    driver.bind(work, 'w', receiver = 'w', ack_wait = 60, max_deliver = 4, credit = 4, prefetch = 1)
    for pid in ('w1', 'w2'):
        assert isinstance(driver.publish(work, pid, pid.encode()), Accepted)
    assert isinstance(driver.publish(work, 'w3', b'w3'), Rejected)
    assert backend.commit_control(frames, 'epoch/1', '1', abort) is True
    assert backend.commit_control(frames, 'epoch/1', '1', completion) is False    # stale same-epoch completion
    trace.append({'step': 'abort under data pressure, stale completion refused', 'control': backend.control_state(frames, 'epoch/1')})
    # A late reader reconstructs the authoritative state and the terminal evidence while traffic continues.
    assert isinstance(driver.publish(frames, 'D', b'D'), Accepted)
    late_state = backend.control_state(frames, 'epoch/1')
    assert late_state == (abort, '2'), late_state
    late = driver.bind(frames, 'late', receiver = 'late', ack_wait = 5, max_deliver = 1, credit = 4, prefetch = 1,
                       kind = SUBSCRIPTION_EOS, instance = 'late')
    evidence = [d.token.message_id for d in _collect(driver, late, 1, 5)]
    assert evidence == ['C'], evidence
    assert driver.retained_ids(frames) == ['D']
    trace.append({'step': 'late reader', 'control': late_state, 'terminal_evidence': evidence,
                  'retained_data': driver.retained_ids(frames)})
    record['trace'] = trace


@pytest.mark.case('MSG-018')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_018_control_retention_is_independent_of_frame_eviction_and(nats_url, evidence_dir) -> None:
    '''
    MSG-018 (P0, messaging, broker): Control retention is independent of frame eviction and full
    data queues.

    Acceptance: Throughout the declared control horizon, late readers recover the authoritative
    epoch state and required terminal/final-sequence/abort evidence independently of data
    pressure. Same-epoch stale completion cannot erase an accepted abort. Compaction passes if
    these semantics survive; full command history is tested only when explicitly requested.

    JetStream declares ``durable_control = False`` (the run-state ledger is Phase 3), so the
    profile is an expected rejection there. The data-slot half is still decided here: a
    per-subject limit keeps the final frame and the terminator apart, while the default
    shared slot — declared as ``control_shares_data_slot = True`` — evicts one for the other.
    '''
    flow, run = unique_ids('msg018')
    driver = JetStreamDriver(nats_url, flow, run, REALTIME)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        capabilities = driver.capabilities()
        record['declared'] = {'durable_control': capabilities.durable_control,
                              'control_shares_data_slot': capabilities.control_shares_data_slot}
        assert capabilities.durable_control is False and capabilities.control_shares_data_slot is True
        with pytest.raises(IncompatibleProfile) as info:
            plan_composition(FlowRequirements(profiles = (ProfileRequest('frames', DURABLE_CONTROL),)), capabilities,
                             runtime = _DURABLE_RUNTIME)
        assert driver.channel_ids() == []
        record['rejection'] = str(info.value)[:300]
        # Per-subject slots: the terminator and the final frame survive each other.
        frames = driver.channel('frames', LIVE_LATEST, RETENTION_LIMITS, max_msgs = 1, per_subject = True)
        for pid, body, kind in (('A', b'A', 'data'), ('C', b'EOS', KIND_EOS), ('B', b'B', 'data')):
            assert isinstance(driver.publish(frames, pid, body, kind = kind), Accepted)
        assert driver.until(lambda: driver.retained(frames) == 2, 5), driver.retained(frames)
        assert driver.last_retained(frames) == 'B' and driver.last_retained(frames, KIND_EOS) == 'C'
        # The declared shared slot: exactly the eviction the declaration warns about.
        shared = driver.channel('shared', LIVE_LATEST, RETENTION_LIMITS, max_msgs = 1)
        for pid, body, kind in (('A', b'A', 'data'), ('C', b'EOS', KIND_EOS), ('B', b'B', 'data')):
            assert isinstance(driver.publish(shared, pid, body, kind = kind), Accepted)
        assert driver.until(lambda: driver.retained(shared) == 1, 5)
        record['data_slot'] = {'per_subject_retained': driver.retained(frames), 'shared_retained': driver.retained(shared),
                               'shared_survivor': driver.last_retained(shared) or driver.last_retained(shared, KIND_EOS)}
    finally:
        driver.close()
        _write(evidence_dir, 'control_retention.json', record)
    unsupported('jetstream declares durable_control=False: no run-state ledger is composed (RFC 0006 section 9, '
                'Phase 3); the profile was rejected before provisioning with zero side effects')


@pytest.mark.case('MSG-018')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_018_memory_control_ledger_survives_data_pressure_and_stale_completion(evidence_dir) -> None:
    driver = MemoryDriver('f', 'r', durable_control = True)
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_018(driver, record)
    finally:
        driver.close()
        _write(evidence_dir, 'control_retention.json', record)


@pytest.mark.case('MSG-018')
@pytest.mark.level('broker')
@pytest.mark.variant('ledger')
@pytest.mark.pending('phase 3')
def test_msg_018_ledger_durable_control_on_jetstream_with_a_durable_runtime_store() -> None:
    '''
    The run-state bucket of RFC 0006 section 9 on a durable RuntimeStore
    (``redis_durable_url``): versioned epoch state and terminal evidence that a late
    controller reconciles independently of the node streams. Needs the Phase-3 ledger.
    '''


@pytest.mark.negative_control(of = 'MSG-018')
def test_msg_018_detects_a_control_slot_shared_with_data(monkeypatch) -> None:
    defects.shared_control_slot(monkeypatch)
    driver = MemoryDriver('f', 'r', durable_control = True)
    try:
        assert defects.detects(_oracle_msg_018, driver, {})
    finally:
        driver.close()


# -- MSG-024 -------------------------------------------------------------------------

HORIZON = 60.0
PROVENANCE = {'VF-Model': 'detector@sha256:3f9c1a2b', 'VF-Config': 'flow.yaml@v3', 'VF-Source': 'cam-1/epoch-7',
              'VF-Archive-Pin': 'true'}


def _oracle_msg_024(driver : Any, record : Dict[str, Any]) -> None:
    request = FlowRequirements(profiles = (ProfileRequest('src', REPLAY_ARCHIVE, {'horizon_seconds': HORIZON}),))
    plan = plan_composition(request, driver.capabilities())
    assert plan.channel_profiles == {'src': REPLAY_ARCHIVE}
    backend = driver.backend
    work = driver.channel('src', REPLAY_ARCHIVE, RETENTION_INTEREST)
    proc = driver.bind(work, 'proc', receiver = 'proc', ack_wait = 5, max_deliver = 4, credit = 4, prefetch = 1)
    body = b'frame-bytes:' + b'\x00\x01\x02' * 64
    assert isinstance(driver.publish(work, 'pub-1', body, headers = PROVENANCE, event_id = 'evt-1'), Accepted)
    archived_at = driver.clock.now()
    got = _collect(driver, proc, 1, 5)                            # the working subscription completes and settles
    assert [d.token.message_id for d in got] == ['evt-1']
    assert driver.retained(work) == 0                             # reclaimed from the working channel
    teardown = backend.close([work], '')
    assert teardown.complete and work not in backend.channel_ids()
    record['before_replay'] = {'archived_at': archived_at, 'teardown': str(teardown)}
    # H minus one tick: the original identity, provenance and bytes.
    driver.wait(HORIZON - 1.0)
    replay = backend.replay(work, 'evt-1')
    assert replay is not None, 'archive history vanished with the working ACKs and the run teardown'
    assert replay.event_id == 'evt-1' and replay.publication_id == 'pub-1'
    assert dict(replay.headers) == {'Nats-Msg-Id': 'pub-1', **PROVENANCE}, dict(replay.headers)
    assert digest(replay.body) == digest(body)
    replay_attempt = f'replay:{digest(body)[:8]}'
    assert replay_attempt != replay.publication_id                 # a replay is a new execution, not the original ingestion
    record['pre_expiry'] = {'event_id': replay.event_id, 'publication_id': replay.publication_id,
                            'provenance': dict(replay.headers), 'payload_sha256': digest(replay.body),
                            'replay_attempt_id': replay_attempt}
    # After the horizon: an explicit expiry, not a replay with missing inputs.
    driver.wait(2.0)
    expired = backend.replay(work, 'evt-1')
    assert expired is None, expired
    record['post_expiry'] = {'result': None, 'at': driver.clock.now()}


@pytest.mark.case('MSG-024')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_024_replay_history_remains_available_after_all_working(nats_url, evidence_dir) -> None:
    '''
    MSG-024 (P1, messaging, broker): Replay history remains available after all working
    subscriptions complete.

    Acceptance: Pre-expiry replay resolves the original envelope and payload after working ACKs
    and teardown; post-expiry request returns an explicit expiry outcome.

    JetStream declares ``archive = False``: an INTEREST stream is a processing buffer
    (the working ACK reclaims the message, the teardown removes the stream), so the
    profile is an expected rejection there and the case is UNSUPPORTED.
    '''
    flow, run = unique_ids('msg024')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        capabilities = driver.capabilities()
        record['declared'] = {'archive': capabilities.archive}
        with pytest.raises(IncompatibleProfile) as info:
            plan_composition(FlowRequirements(profiles = (ProfileRequest('src', REPLAY_ARCHIVE),)), capabilities)
        assert driver.channel_ids() == []
        record['rejection'] = str(info.value)[:300]
        # Why the declaration is truthful: the working channel keeps nothing past its ACKs.
        work = driver.channel('src', RELIABLE_WORK, RETENTION_INTEREST)
        proc = driver.bind(work, 'proc', receiver = 'proc', ack_wait = 5, max_deliver = 4, credit = 4, prefetch = 1)
        assert isinstance(driver.publish(work, 'pub-1', b'frame', headers = PROVENANCE, event_id = 'evt-1'), Accepted)
        assert [d.token.message_id for d in _collect(driver, proc, 1, 10)] == ['pub-1']
        assert driver.until(lambda: driver.retained(work) == 0, 10)
        record['working_channel_after_ack'] = driver.retained(work)
    finally:
        driver.close()
        _write(evidence_dir, 'archive_admission.json', record)
    unsupported('jetstream declares archive=False: an ACK-drained work queue is not an archive; the profile was '
                'rejected before provisioning with zero side effects')


@pytest.mark.case('MSG-024')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_024_memory_archive_outlives_working_acks_and_teardown_until_the_horizon(evidence_dir) -> None:
    driver = MemoryDriver('f', 'r', archive = True, archive_horizon_seconds = HORIZON)
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_024(driver, record)
    finally:
        driver.close()
        _write(evidence_dir, 'archive_ledger.json', record)


@pytest.mark.negative_control(of = 'MSG-024')
def test_msg_024_detects_a_working_queue_posing_as_an_archive(monkeypatch) -> None:
    defects.working_queue_as_archive(monkeypatch)
    driver = MemoryDriver('f', 'r', archive = True, archive_horizon_seconds = HORIZON)
    try:
        assert defects.detects(_oracle_msg_024, driver, {})
    finally:
        driver.close()


# -- MSG-025 -------------------------------------------------------------------------

QUEUE_LIMIT = 8


def _oracle_msg_025(driver : Any, record : Dict[str, Any]) -> None:
    capabilities = driver.capabilities()
    record['declared'] = {'retained_backlog': capabilities.retained_backlog,
                          'recoverable_delivery': capabilities.recoverable_delivery,
                          'publication_ledger': capabilities.publication_ledger}
    assert not capabilities.retained_backlog and not capabilities.recoverable_delivery
    for profile in (RELIABLE_WORK, DURABLE_CONTROL, REPLAY_ARCHIVE):
        with pytest.raises(IncompatibleProfile):
            plan_composition(FlowRequirements(profiles = (ProfileRequest('cam', profile),)), capabilities,
                             runtime = _DURABLE_RUNTIME)
    assert plan_composition(FlowRequirements(profiles = (ProfileRequest('cam', LIVE_LATEST),)), capabilities) \
        .channel_profiles == {'cam': LIVE_LATEST}
    channel = driver.channel('cam', LIVE_LATEST, RETENTION_LIMITS)
    # Before the subscription is ready nothing is promised — and nothing arrives.
    early = driver.publish(channel, 'pre-ready', b'p')
    assert isinstance(early, Accepted) and early.durability_boundary == 'memory', early
    assert isinstance(driver.observe_publication(channel, 'pre-ready', b'p'), PublicationUnresolvable)
    viewer = driver.bind(channel, 'viewer', receiver = 'viewer', ack_wait = 1, max_deliver = 1, credit = 8, prefetch = 1)
    assert isinstance(driver.publish(channel, 'post-ready', b'q'), Accepted)
    first = [d.token.message_id for d in driver.receive(viewer, timeout = 0.2)]
    assert first == ['post-ready'], first
    # Reads paused past the slow-consumer bound: the queue stays bounded and the losses are reported.
    for i in range(20):
        assert isinstance(driver.publish(channel, f'burst-{i:02d}', b'b'), Accepted)
    observed = driver.observe(viewer)
    assert isinstance(observed, Known)
    assert observed.value.available == QUEUE_LIMIT and observed.value.dropped == 20 - QUEUE_LIMIT, observed.value
    burst = [d.token.message_id for d in driver.receive(viewer, timeout = 0.2, item_credit = 64)]
    assert len(burst) == QUEUE_LIMIT and burst[0] == 'burst-00', burst
    record['slow_consumer'] = {'queue_limit': QUEUE_LIMIT, 'observation': str(observed.value), 'delivered': burst}
    # A fresh sentinel after the burst reaches the subscriber at once.
    assert isinstance(driver.publish(channel, 'sentinel', b's'), Accepted)
    assert [d.token.message_id for d in driver.receive(viewer, timeout = 0.2)] == ['sentinel']
    record['sentinel'] = 'delivered'


@pytest.mark.case('MSG-025')
@pytest.mark.level('broker')
def test_msg_025_core_nats_connection_readiness_and_slow_consumer_loss_are() -> None:
    '''
    MSG-025 (P1, messaging, broker): Core NATS connection readiness and slow-consumer loss are
    explicit.

    Acceptance: No false durability claims; all observed losses belong to permitted live-loss
    conditions, bounded queues hold, and fresh delivery resumes after reconnect.

    No Core-NATS messaging adapter exists (``messaging/corenats_backend.py`` is a
    Phase-5 stretch); the JetStream adapter is not a core-only transport, so the
    broker-level primary has no fixture to run against.
    '''
    not_run('no Core-NATS messaging adapter exists (a Phase-5 stretch); the JetStream adapter is not a '
            'core-only transport, so there is nothing to run this case against')


@pytest.mark.case('MSG-025')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_025_memory_core_only_transport_promises_nothing_it_lacks(evidence_dir) -> None:
    driver = MemoryDriver('f', 'r', core_only = True, client_queue_limit = QUEUE_LIMIT)
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_025(driver, record)
    finally:
        driver.close()
        _write(evidence_dir, 'core_ledger.json', record)


@pytest.mark.negative_control(of = 'MSG-025')
def test_msg_025_detects_a_core_transport_claiming_a_backlog(monkeypatch) -> None:
    defects.core_claims_backlog(monkeypatch)
    driver = MemoryDriver('f', 'r', core_only = True, client_queue_limit = QUEUE_LIMIT)
    try:
        assert defects.detects(_oracle_msg_025, driver, {})
    finally:
        driver.close()
