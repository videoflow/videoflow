'''
Conformance cases: PAY-009, PAY-010, PAY-014 (PAY-011 lives in test_pay_durability.py).

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions taking the component under
test — payload stores, or a rig standing a flow up on the memory backends or on the
compose broker (``_payloads.py``) — so the paired negative control can run the same
oracle against the reviewed defect (``defects_pay.py``) and prove it fails.
'''
from __future__ import absolute_import, division, print_function

import time
from typing import Any, Callable, Dict

import defects
import defects_pay
import numpy as np
import pytest
from _payloads import (
    JetStreamRig,
    MemoryRig,
    frame_array,
    frame_bytes,
    object_exists,
    obligations_of,
    raw_store,
    spec,
    wait_until,
    write_evidence,
)

from videoflow.backends import faults
from videoflow.backends.capabilities import (
    LIVE_LATEST,
    RELIABLE_WORK,
    FlowRequirements,
    ProfileRequest,
    plan_composition,
)
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.messaging import MemoryMessagingBackend
from videoflow.backends.memory.payload import TIER_EVICTABLE, MemoryPayloadStore
from videoflow.backends.observation import ObservationLog
from videoflow.backends.outcomes import Known
from videoflow.backends.payload import (
    DurableReceipt,
    ImmutablePayloadRef,
    Missing,
    PayloadBytes,
    PayloadStore,
    RetentionContract,
    admit_retention,
)
from videoflow.backends.payload import TransientFailure as TransientRead
from videoflow.backends.payload_bridge import PayloadStoreBlobBridge
from videoflow.core import constants
from videoflow.core.constants import BATCH
from videoflow.core.errors import DecodeError, IncompatibleProfile, ResourceUnavailable, SchemaError
from videoflow.core.errors import TransientFailure as TransientError
from videoflow.messaging import nats_messenger
from videoflow.wire.serialization import decode_envelope, hydrate_message, peek_envelope

pytestmark = pytest.mark.timeout(180)

FRAME = frame_array((1000, 1000), seed = 9)
FRAME_BYTES = frame_bytes(256 * 1024, seed = 9)


def _count(store : PayloadStore) -> int:
    observed = raw_store(store).inventory()
    assert isinstance(observed, Known), observed
    return len(observed.value)


def _fake_redis_store(now : Callable[[], float]) -> Any:
    from support_redis import FakeRedis

    from videoflow.wire.redis_payload_store import RedisPayloadStore
    fake = FakeRedis()
    return RedisPayloadStore(client = fake, clock = now), fake


# -- PAY-009 ----------------------------------------------------------------------

def _oracle_pay_009(reliable : PayloadStore, live : PayloadStore, advance : Callable[[float], None],
                    now : Callable[[], float], ttl : int, horizon : int, live_ttl : int,
                    evidence : Dict[str, Any]) -> faults.FaultSchedule:
    '''
    A reliable input outstanding past the store TTL but inside the promised
    horizon H must still load its bytes after the reader's pause — through a
    store blip at restart and a late lease renewal; a horizon the TTL cannot
    cover is refused before any byte is written; a live input's expiry is an
    observable, attributed loss.
    '''
    ref = reliable.put(FRAME_BYTES, 'cam:t1:1', RetentionContract(ttl, horizon, True, ('reader',)))
    advance(ttl + 1)                                         # the reader was stopped across the TTL
    schedule = faults.FaultSchedule({'payload.read.before': faults.Nth(1, faults.RaiseError(
        lambda: ConnectionError('payload service unavailable as the reader restarts')))})
    with schedule:
        first = reliable.read(reliable.ref_for_key(ref.key))
        assert isinstance(first, TransientRead), first
        second = reliable.read(reliable.ref_for_key(ref.key))
    assert isinstance(second, PayloadBytes) and second.data == FRAME_BYTES, \
        'an accepted reliable input still within H lost its bytes at the store TTL'
    advance(horizon - ttl - 3)                               # H - 2: the renewal comes late, but in time
    reliable.renew_obligation(ref, 'reader', now() + 2 * horizon)
    advance(5)                                               # past the original horizon
    renewed = reliable.read(reliable.ref_for_key(ref.key))
    assert isinstance(renewed, PayloadBytes) and renewed.data == FRAME_BYTES, 'a renewed lease did not extend retention'
    receipt = reliable.release_obligation(ref, 'reader', 'ack')
    assert receipt.reclaimed
    # A TTL shorter than the promised horizon with nothing pinning the object: refused before a write.
    before = _count(reliable)
    with pytest.raises(IncompatibleProfile) as rejected:
        admit_retention(RetentionContract(ttl, horizon, True, ()))
    bridge = PayloadStoreBlobBridge(reliable, [], horizon_seconds = horizon, durable_required = True,
                                    content_id = lambda: 'cam:t2:2')
    with pytest.raises(IncompatibleProfile):
        bridge.put(FRAME_BYTES, ttl)
    assert _count(reliable) == before, 'the rejected contract wrote an object'
    # Live: expiry is allowed, attributed, and never mistaken for reliable recovery.
    live_ref = live.put(FRAME_BYTES, 'cam:t3:3', RetentionContract(live_ttl, live_ttl, False, ()))
    advance(live_ttl + 1)
    expired = live.read(live.ref_for_key(live_ref.key))
    assert isinstance(expired, Missing), expired
    live_bridge = PayloadStoreBlobBridge(live, [], horizon_seconds = live_ttl, durable_required = False,
                                         content_id = lambda: '')
    with pytest.raises(DecodeError) as loss:
        live_bridge.get(live_ref.key)
    assert loss.value.context.get('reason') == 'missing' and live_ref.key in str(loss.value), loss.value
    evidence.update({'ttl': ttl, 'horizon': horizon, 'live_ttl': live_ttl, 'reliable_key': ref.key,
                     'live_key': live_ref.key, 'rejection': str(rejected.value)[:200], 'faults': schedule.fired(),
                     'live_loss': {'code': loss.value.code, 'reason': loss.value.context.get('reason')}})
    return schedule


@pytest.mark.case('PAY-009')
@pytest.mark.level('process')
def test_pay_009_payload_retention_covers_the_entire_accepted_retry_and(evidence_dir, record_faults) -> None:
    '''
    PAY-009 (P0, payload, process): Payload retention covers the entire accepted retry and
    outage horizon.

    Acceptance: Every accepted reliable input still within H loads original bytes after the
    pause; incompatible retention configuration is rejected before publication.
    '''
    clock = FakeClock()
    log = ObservationLog()
    reliable = MemoryPayloadStore(clock)
    live = MemoryPayloadStore(clock, tier = TIER_EVICTABLE, log = log)
    evidence : Dict[str, Any] = {}
    schedule = _oracle_pay_009(reliable, live, clock.advance, clock.now, 10, 100, 5, evidence)
    expiries = [e.as_dict() for e in log.events('expire')]
    evidence['live_expiry_events'] = expiries
    assert any(e.get('key') == evidence['live_key'] for e in expiries), 'the live expiry left no attributed record'
    write_evidence(evidence_dir, 'retention_horizon.json', evidence)
    record_faults(schedule)


@pytest.mark.case('PAY-009')
@pytest.mark.level('process')
@pytest.mark.variant('redis')
def test_pay_009_redis_store_pins_past_a_short_real_ttl(redis_url, evidence_dir, record_faults) -> None:
    '''The Redis store on the real server, real time: a 2 s TTL under a 6 s horizon, a 1 s live TTL.'''
    from _brokers import sweep_refs

    from videoflow.wire.redis_payload_store import RedisPayloadStore
    store = RedisPayloadStore(redis_url)
    evidence : Dict[str, Any] = {}
    try:
        schedule = _oracle_pay_009(store, store, time.sleep, time.time, 2, 6, 1, evidence)
    finally:
        sweep_refs(store.client, [k for k in (evidence.get('reliable_key'), evidence.get('live_key')) if k])
    write_evidence(evidence_dir, 'retention_horizon.json', evidence)
    record_faults(schedule)


@pytest.mark.negative_control(of = 'PAY-009')
def test_pay_009_detects_a_fixed_ttl_independent_of_outstanding_work(monkeypatch) -> None:
    defects_pay.fixed_ttl_store(monkeypatch)
    clock = FakeClock()
    assert defects.detects(_oracle_pay_009, MemoryPayloadStore(clock), MemoryPayloadStore(clock, tier = TIER_EVICTABLE),
                           clock.advance, clock.now, 10, 100, 5, {})


# -- PAY-010 ----------------------------------------------------------------------

PRESSURE_OBJECT = frame_bytes(1 << 20, seed = 10)


def _memory_pressure(store : PayloadStore, ref : ImmutablePayloadRef, limit : int = 12) -> Dict[str, Any]:
    '''Unrelated TTL-bearing writes until ``ref`` is gone (evictable) or a write is refused (durable).'''
    writes = 0
    for i in range(limit):
        try:
            store.put(PRESSURE_OBJECT, f'unrelated:{i}', RetentionContract(120, 120, False, ()))
        except ResourceUnavailable as e:
            return {'writes': writes, 'refused': str(e)[:120], 'evicted': False}
        writes += 1
        if not object_exists(store, ref.key):
            return {'writes': writes, 'refused': None, 'evicted': True}
    return {'writes': writes, 'refused': None, 'evicted': False}


def _oracle_pay_010(evictable : PayloadStore, durable : PayloadStore,
                    pressure : Callable[[PayloadStore, ImmutablePayloadRef], Dict[str, Any]],
                    evidence : Dict[str, Any]) -> None:
    messaging = MemoryMessagingBackend(FakeClock()).capabilities()
    reliable = FlowRequirements(profiles = (ProfileRequest('cam', RELIABLE_WORK),))
    live = FlowRequirements(profiles = (ProfileRequest('cam', LIVE_LATEST),))
    caps = evictable.capabilities()
    assert isinstance(caps.evictable, Known) and caps.evictable.value, caps
    assert isinstance(caps.durable, Known) and not caps.durable.value, caps
    with pytest.raises(IncompatibleProfile) as rejected:
        plan_composition(reliable, messaging, payload = caps, payload_refs_in_use = True)
    assert 'evictable' in str(rejected.value)
    plan_composition(live, messaging, payload = caps, payload_refs_in_use = True)
    evidence['cache'] = {'capabilities': {'durable': caps.durable.value, 'evictable': caps.evictable.value},
                         'reliable_admission': f'rejected: {str(rejected.value)[:160]}', 'live_admission': 'admitted'}
    # The physics the rejection is about: a reliable frame outstanding while the cache fills.
    ref = evictable.put(FRAME_BYTES, 'cam:t1:1', RetentionContract(120, 120, True, ('det',)))
    outcome = pressure(evictable, ref)
    evidence['cache']['pressure'] = outcome
    assert outcome['evicted'], f'the cache never evicted the outstanding frame: {outcome}'
    assert isinstance(evictable.read(evictable.ref_for_key(ref.key)), Missing)
    bridge = PayloadStoreBlobBridge(evictable, [], horizon_seconds = 120, durable_required = False,
                                    content_id = lambda: '')
    with pytest.raises(DecodeError) as loss:                 # a live loss: attributed, explicit
        bridge.get(ref.key)
    assert loss.value.context.get('reason') == 'missing' and ref.key in str(loss.value)
    evidence['cache']['live_loss'] = {'code': loss.value.code, 'reason': 'missing', 'key': ref.key}
    # The durable candidate: admitted, and it refuses rather than evicts.
    dcaps = durable.capabilities()
    assert isinstance(dcaps.durable, Known) and dcaps.durable.value, dcaps
    plan_composition(reliable, messaging, payload = dcaps, payload_refs_in_use = True)
    protected = durable.put(FRAME_BYTES, 'cam:t1:1', RetentionContract(120, 120, True, ('det',)))
    outcome = pressure(durable, protected)
    evidence['durable'] = {'capabilities': {'durable': dcaps.durable.value, 'evictable': dcaps.evictable.value},
                           'reliable_admission': 'admitted', 'pressure': outcome}
    assert not outcome['evicted'], 'the durable candidate evicted a reliable frame'
    recovered = durable.read(durable.ref_for_key(protected.key))
    assert isinstance(recovered, PayloadBytes) and recovered.data == FRAME_BYTES


@pytest.mark.case('PAY-010')
@pytest.mark.level('process')
def test_pay_010_evictable_cache_cannot_certify_durable_payload_storage(evidence_dir) -> None:
    '''
    PAY-010 (P0, payload, process): Evictable cache cannot certify durable payload storage.

    Acceptance: Reliable configuration either rejects the cache or demonstrates byte-identical
    recovery from a certified protected copy; live missing frames produce observable allowed-
    loss outcomes.
    '''
    clock = FakeClock()
    log = ObservationLog()
    evictable = MemoryPayloadStore(clock, tier = TIER_EVICTABLE, max_bytes = 4 << 20, log = log)
    durable = MemoryPayloadStore(clock, max_bytes = 4 << 20)
    evidence : Dict[str, Any] = {}
    _oracle_pay_010(evictable, durable, _memory_pressure, evidence)
    evictions = [e.as_dict() for e in log.events('evict')]
    evidence['eviction_events'] = evictions
    assert any(e.get('obligations') for e in evictions), 'the eviction of an owned object left no attributed record'
    write_evidence(evidence_dir, 'store_validation.json', evidence)


@pytest.mark.case('PAY-010')
@pytest.mark.level('broker')
@pytest.mark.variant('redis_small')
def test_pay_010_a_64mb_volatile_lru_redis_evicts_an_outstanding_reliable_frame(redis_small_url, evidence_dir) -> None:
    '''The compose ``redis-small`` cache (64 MB, ``volatile-lru``): eviction under real pressure, read back as ``evicted_keys``.'''
    from _brokers import redis_client

    from videoflow.wire.redis_payload_store import RedisPayloadStore
    evidence : Dict[str, Any] = {}
    with redis_client(redis_small_url, sweep = 'vf-blob*') as client:   # an isolated instance: sweep it
        store = RedisPayloadStore(redis_small_url)
        before = int(client.info('stats').get('evicted_keys', 0))

        def pressure(target : PayloadStore, ref : ImmutablePayloadRef) -> Dict[str, Any]:
            # Redis' approximated LRU has a one-second clock: written in the same second
            # as the pressure objects the frame merely ties with them, and the
            # eviction sampler then picks among equals. Age it past the tick first.
            time.sleep(1.2)
            outcome = _memory_pressure(target, ref, limit = 200)
            outcome['evicted_keys_delta'] = int(client.info('stats').get('evicted_keys', 0)) - before
            return outcome

        _oracle_pay_010(store, MemoryPayloadStore(FakeClock(), max_bytes = 4 << 20), pressure, evidence)
    assert evidence['cache']['pressure']['evicted_keys_delta'] > 0, evidence['cache']['pressure']
    write_evidence(evidence_dir, 'store_validation.json', evidence)


@pytest.mark.case('PAY-010')
@pytest.mark.level('broker')
@pytest.mark.variant('redis_durable')
def test_pay_010_an_append_only_noeviction_redis_is_a_certified_durable_candidate(redis_durable_url, evidence_dir) -> None:
    '''The compose ``redis-durable`` store: durability read back live, reliable work admitted, bytes recovered.'''
    from _brokers import redis_client

    from videoflow.wire.redis_payload_store import RedisPayloadStore
    evidence : Dict[str, Any] = {}
    clock = FakeClock()
    with redis_client(redis_durable_url, sweep = 'vf-blob*'):           # an isolated instance: sweep it
        durable = RedisPayloadStore(redis_durable_url)

        def pressure(target : PayloadStore, ref : ImmutablePayloadRef) -> Dict[str, Any]:
            if target is durable:
                return {'writes': 0, 'refused': None, 'evicted': False, 'note': 'a 4 GB noeviction store is not filled here'}
            return _memory_pressure(target, ref)

        _oracle_pay_010(MemoryPayloadStore(clock, tier = TIER_EVICTABLE, max_bytes = 4 << 20), durable, pressure, evidence)
    write_evidence(evidence_dir, 'store_validation.json', evidence)


@pytest.mark.negative_control(of = 'PAY-010')
def test_pay_010_detects_a_cache_that_advertises_durability(monkeypatch) -> None:
    defects_pay.lying_capabilities(monkeypatch)
    clock = FakeClock()
    assert defects.detects(_oracle_pay_010, MemoryPayloadStore(clock, tier = TIER_EVICTABLE, max_bytes = 4 << 20),
                           MemoryPayloadStore(clock, max_bytes = 4 << 20), _memory_pressure, {})


# -- PAY-014 ----------------------------------------------------------------------

def _specs_pay_014() -> list:
    return [spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False),
            spec('other', ['parent'], 'consumer', False)]


def _oracle_pay_014(rig : Any, horizon : int, ordinary_ttl : int, advance : Callable[[float], None],
                    pin_ttl_of : Callable[[str], float], evidence : Dict[str, Any]) -> faults.FaultSchedule:
    '''
    ``child`` dead-letters an offloaded frame while ``other`` still reads it. The
    dead letter pins the payload for H (longer than the ordinary TTL); the pin
    survives the other reader's release and the run's teardown; at H - 1 tick
    the entry still hydrates and carries its provenance; after H it is gone.
    '''
    parent = rig.messenger('parent', [], blob_reader_ids = ['child', 'other'], blob_ttl_seconds = ordinary_ttl)
    child = rig.messenger('child', ['parent'], max_retries = 0)
    other = rig.messenger('other', ['parent'])
    schedule = faults.FaultSchedule({'obligation.acquire.after': faults.Delay(0.0)})   # the pin, observed
    with schedule:
        parent.publish_message(FRAME)
        original = rig.retained_entries('parent')[-1][1]
        key = peek_envelope(original.body)['blob_ref']
        assert np.array_equal(child.receive_message()['parent']['message'], FRAME)
        child.fail_inputs(SchemaError('this frame will never parse', remedy = 'fix the producer'))
        assert wait_until(lambda: len(rig.dead_letters('child')) == 1, 20)
    pin = f'dlq/{rig.flow_id}'
    assert obligations_of(rig.store, key) == {'other', pin}, obligations_of(rig.store, key)
    pinned_for = pin_ttl_of(key)
    evidence['pin'] = {'key': key, 'seconds': pinned_for, 'horizon': horizon, 'ordinary_ttl': ordinary_ttl}
    assert horizon - 2 <= pinned_for <= horizon + 1, f'the dead letter pinned the payload for {pinned_for}s, not H = {horizon}s'
    advance(ordinary_ttl + 1)                                # the ordinary payload TTL has passed
    assert isinstance(rig.store.read(rig.store.ref_for_key(key)), PayloadBytes), 'the payload expired at its ordinary TTL'
    assert np.array_equal(other.receive_message()['parent']['message'], FRAME)
    other.ack_inputs()                                       # every normal reader is done
    assert obligations_of(rig.store, key) == {pin}
    teardown = rig.teardown_run()
    evidence['teardown'] = getattr(teardown, '__dict__', str(teardown))
    assert len(rig.dead_letters('child')) == 1, 'the run\'s teardown took the dead letter with it'
    assert isinstance(rig.store.read(rig.store.ref_for_key(key)), PayloadBytes), 'normal cleanup released the forensic bytes'
    advance(max(0, horizon - ordinary_ttl - 3))              # H - 2: the DLQ reader comes online
    letter = rig.dead_letters('child')[0]
    decoded = decode_envelope(letter.body, resolve_blobs = False)
    assert decoded['blob_ref'] == key and decoded['producer_name'] == 'parent'
    bridge = PayloadStoreBlobBridge(rig.store, [], horizon_seconds = horizon, durable_required = True, content_id = lambda: '')
    replayed = hydrate_message(decoded, bridge)
    assert np.array_equal(replayed, FRAME), 'the dead letter does not hydrate to the original frame inside H'
    evidence['provenance'] = {'headers': dict(letter.headers), 'trace_id': decoded['trace_id'], 'seq': decoded['seq'],
                              'run_id': decoded['run_id'], 'original_pid': original.headers.get('Nats-Msg-Id')}
    for header in ('VF-Origin-Node', 'VF-Run-Id', 'VF-Code', 'VF-Disposition', 'Nats-Msg-Id'):
        assert letter.headers.get(header), (header, letter.headers)
    assert letter.headers['VF-Origin-Node'] == 'child' and letter.headers['VF-Run-Id'] == rig.run_id
    advance(4)                                               # past H: expiry is deliberate
    gone = rig.store.read(rig.store.ref_for_key(key))
    evidence['after_horizon'] = {'read': type(gone).__name__, 'obligations': sorted(obligations_of(rig.store, key))}
    assert isinstance(gone, Missing), f'the payload outlived the declared horizon: {gone}'
    evidence['faults'] = schedule.fired()
    return schedule


@pytest.mark.case('PAY-014')
@pytest.mark.level('broker')
def test_pay_014_dead_letters_retain_images_for_the_complete_forensic_and(nats_url, redis_url, evidence_dir,
                                                                          record_faults, monkeypatch) -> None:
    '''
    PAY-014 (P0, payload, broker): Dead letters retain images for the complete forensic and
    replay horizon.

    Acceptance: Original frame bytes and provenance are readable throughout H after normal input
    cleanup; expiry is deliberate and observable after H.

    H is 9 real seconds here (the DLQ retention the worker pins for), the
    ordinary payload TTL 2 s; the store is the compose Redis, time is real.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, 'DLQ_RETENTION_SECONDS', 9)
    evidence : Dict[str, Any] = {}
    rig = JetStreamRig(nats_url, BATCH, _specs_pay_014(), redis_url = redis_url)
    try:
        schedule = _oracle_pay_014(rig, 9, 2, time.sleep, lambda key: float(rig.store.client.ttl(key)), evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'forensic_horizon.json', evidence)
    record_faults(schedule)


@pytest.mark.case('PAY-014')
@pytest.mark.level('broker')
@pytest.mark.variant('memory')
def test_pay_014_memory_backends_pin_the_dead_letters_payload_for_h(evidence_dir, record_faults, monkeypatch) -> None:
    '''The model under a wall-aligned fake clock (the worker stamps pin deadlines in wall time): H = 100 s.'''
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, 'DLQ_RETENTION_SECONDS', 100)
    evidence : Dict[str, Any] = {}
    clock = FakeClock(start = time.time())
    rig = MemoryRig(BATCH, clock = clock, specs = _specs_pay_014())
    store = raw_store(rig.store)
    assert isinstance(store, MemoryPayloadStore)
    try:
        schedule = _oracle_pay_014(rig, 100, 2, clock.advance,
                                   lambda key: store.obligations(key)[f'dlq/{rig.flow_id}'] - clock.now(), evidence)
    finally:
        rig.close()
    write_evidence(evidence_dir, 'forensic_horizon.json', evidence)
    record_faults(schedule)


class _PinRefusingStore(PayloadStore):
    '''The child's store: every acquire fails as an unreachable store would; everything else is real.'''
    def __init__(self, inner : PayloadStore) -> None:
        self._inner = inner
        self.refused = 0

    def capabilities(self) -> Any:
        return self._inner.capabilities()

    def put(self, data : bytes, content_id : str, contract : RetentionContract) -> ImmutablePayloadRef:
        return self._inner.put(data, content_id, contract)

    def acquire_obligation(self, ref : ImmutablePayloadRef, obligation_id : str, deadline : float) -> DurableReceipt:
        self.refused += 1
        raise TransientError(f'the payload store was unreachable while pinning {ref.key} for {obligation_id}',
                             remedy = 'retry the pin')

    def renew_obligation(self, ref : ImmutablePayloadRef, obligation_id : str, deadline : float) -> DurableReceipt:
        return self.acquire_obligation(ref, obligation_id, deadline)

    def read(self, ref : ImmutablePayloadRef) -> Any:
        return self._inner.read(ref)

    def release_obligation(self, ref : ImmutablePayloadRef, obligation_id : str, completion_receipt : str) -> Any:
        return self._inner.release_obligation(ref, obligation_id, completion_receipt)

    def reconcile(self, ledger : Any, operation_id : str) -> Any:
        return self._inner.reconcile(ledger, operation_id)

    def inventory(self) -> Any:
        return self._inner.inventory()

    def ref_for_key(self, key : str) -> ImmutablePayloadRef:
        return self._inner.ref_for_key(key)


@pytest.mark.case('PAY-014')
@pytest.mark.level('broker')
@pytest.mark.variant('pin_failure')
def test_pay_014_a_failed_dead_letter_pin_keeps_the_source_input_redeliverable(evidence_dir, monkeypatch) -> None:
    '''
    The catalog's third assertion: an incomplete DLQ pin transaction cannot
    terminally release the source input (``BLOB-14`` step 3: the pin precedes
    termination). The child's store refuses the pin; the delivery must then be
    kept for a later attempt — not terminated against a dead letter whose bytes
    are only TTL-protected.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, 'DLQ_RETENTION_SECONDS', 100)
    clock = FakeClock(start = time.time())
    rig = MemoryRig(BATCH, clock = clock, specs = _specs_pay_014())
    refusing = _PinRefusingStore(rig.store)
    evidence : Dict[str, Any] = {}
    try:
        parent = rig.messenger('parent', [], blob_reader_ids = ['child', 'other'], blob_ttl_seconds = 2)
        child = rig.messenger('child', ['parent'], store = refusing, max_retries = 0)
        parent.publish_message(FRAME)
        key = peek_envelope(rig.retained('parent')[-1])['blob_ref']
        child.receive_message()
        child.fail_inputs(SchemaError('this frame will never parse', remedy = 'fix the producer'))
        state = rig.consumer_state('child', 'parent')
        held = obligations_of(rig.store, key)
        evidence.update({'pin_attempts': refusing.refused, 'consumer_after': state.__dict__, 'obligations': sorted(held),
                         'dead_letters': len(rig.dead_letters('child'))})
        write_evidence(evidence_dir, 'pin_failure.json', evidence)
        assert refusing.refused >= 1
        assert f'dlq/{rig.flow_id}' in held or state.pending + state.unacked >= 1, (
            'the dead-letter pin failed but the delivery was terminated anyway: the source input is gone and its '
            f'payload is left to its ordinary TTL (obligations {sorted(held)}, consumer {state}). BLOB-14 step 3 '
            'requires the pin before termination; a failed pin must keep the delivery (NAK) for a later attempt.')
    finally:
        rig.close()


@pytest.mark.negative_control(of = 'PAY-014')
def test_pay_014_detects_a_dead_letter_that_never_pins_its_payload(monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, 'DLQ_RETENTION_SECONDS', 100)
    defects_pay.unpinned_dead_letter(monkeypatch)
    clock = FakeClock(start = time.time())
    rig = MemoryRig(BATCH, clock = clock, specs = _specs_pay_014())
    store = raw_store(rig.store)
    try:
        assert defects.detects(_oracle_pay_014, rig, 100, 2, clock.advance,
                               lambda key: store.obligations(key).get(f'dlq/{rig.flow_id}', clock.now()) - clock.now(), {})
    finally:
        rig.close()
