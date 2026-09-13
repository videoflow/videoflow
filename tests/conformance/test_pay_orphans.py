'''
Conformance cases: PAY-007, PAY-012, PAY-013.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions taking the component under
test — a rig standing a flow up on the memory backends or on the compose broker
(``_payloads.py``) — so the paired negative control can run the same oracle against
the reviewed defect (``defects_pay.py``) and prove it fails.

The reconciler is always handed a ledger the test builds from the broker's own
state (retained envelopes, ack floors, publication outcomes) plus the intents the
store holds — what the Phase-3 runtime ledger will derive by itself; the
``ledger`` variant of PAY-012 records that debt.
'''
from __future__ import absolute_import, division, print_function

import time
from typing import Any, Callable, Dict, List, Optional

import defects
import defects_pay
import pytest
from _payloads import (
    JetStreamRig,
    KeyRecordingStore,
    MemoryRig,
    frame_array,
    isolated_redis_db,
    ledger,
    object_exists,
    obligations_of,
    spec,
    write_evidence,
)

from videoflow.backends import faults
from videoflow.backends.capabilities import RELIABLE_WORK, RETENTION_INTEREST
from videoflow.backends.memory.messaging import make_channel
from videoflow.backends.memory.payload import MemoryPayloadStore
from videoflow.backends.messaging import OVERFLOW_REJECT
from videoflow.backends.outcomes import Known
from videoflow.backends.payload import PayloadBytes, PayloadStore
from videoflow.core import constants
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.errors import BrokerUnavailable
from videoflow.messaging import nats_messenger, topology
from videoflow.wire.serialization import peek_envelope

pytestmark = pytest.mark.timeout(180)

FRAME = frame_array((1000, 1000), seed = 7)
G_SECONDS = 10.0
ORPHAN_GRACE = 60.0


class SimulatedCrash(Exception):
    '''The in-process stand-in for a publisher dying at a barrier.'''


def _fake_redis_store(clock : Callable[[], float]) -> Any:
    from support_redis import FakeRedis

    from videoflow.wire.redis_payload_store import RedisPayloadStore
    fake = FakeRedis()
    return RedisPayloadStore(client = fake, clock = clock, orphan_grace_seconds = ORPHAN_GRACE), fake


def _inventory_keys(store : PayloadStore) -> List[str]:
    inner = store.inner if isinstance(store, KeyRecordingStore) else store
    observed = inner.inventory()
    assert isinstance(observed, Known), observed
    return [ref.key for ref in observed.value]


# -- PAY-007 ----------------------------------------------------------------------

def _ledger_from_evidence(rig : Any, store : PayloadStore, readers : List[str]) -> Any:
    '''
    What a restarted publisher's runtime can know: an object a retained,
    unsettled envelope references is owed to the readers behind it; an object
    whose only clue is a publisher intent is owed nothing (its publication was
    never accepted, or was accepted naming another object); an object holding
    the same content as a retained publication without being the key that
    publication names is a deduplicated retry's leftover, owed nothing; an
    object with none of these clues is not the ledger's to decide (only the
    orphan policy applies).
    '''
    floors = {reader: rig.ack_floor(reader, 'parent') for reader in readers}
    referenced : Dict[str, List[str]] = {}
    for seq, body in rig.retained_with_seq('parent'):
        key = peek_envelope(body).get('blob_ref')
        if key is not None:
            referenced[key] = [reader for reader in readers if seq > floors[reader]]
    inner = store.inner if isinstance(store, KeyRecordingStore) else store
    observed = inner.inventory()
    assert isinstance(observed, Known), observed
    content_of = {ref.key: ref.content_id for ref in observed.value}
    published_content = {content_of[k] for k in referenced if k in content_of}
    required : Dict[str, tuple] = {}
    for key, content in content_of.items():
        if key in referenced:
            required[key] = tuple(referenced[key])
        elif any(o.startswith('intent/') for o in obligations_of(store, key)):
            required[key] = ()
        elif content in published_content:
            required[key] = ()
    return ledger(required), {'floors': floors, 'referenced': referenced, 'required': required}


def _oracle_pay_007(rig : Any, store : KeyRecordingStore, advance : Callable[[float], None],
                    evidence : Dict[str, Any]) -> List[faults.FaultSchedule]:
    child = rig.messenger('child', ['parent'])
    schedules : List[faults.FaultSchedule] = []
    runs : List[Dict[str, Any]] = []
    counter = {'trace': 0}

    def restarted_publisher() -> Any:
        '''A fresh publisher process: it re-emits the logical output it was working on (same ids).'''
        publisher = rig.messenger('parent', [], store = store, blob_reader_ids = ['child'])
        publisher._trace_counter = counter['trace']
        return publisher

    def attempt(publisher : Any, barrier : str, action : Any, label : str) -> None:
        counter['trace'] = publisher._trace_counter
        schedule = faults.FaultSchedule({barrier: faults.Nth(1, action)})
        before = list(store.keys)
        with schedule:
            try:
                publisher.publish_message(frame_array((1000, 1000), seed = 70 + len(runs)))
            except (SimulatedCrash, ConnectionError) as e:
                outcome = f'{type(e).__name__}: {e}'
            else:
                raise AssertionError(f'{label}: the injected fault did not stop the publication')
        schedules.append(schedule)
        created = [k for k in store.keys if k not in before]
        runs.append({'label': label, 'barrier': barrier, 'created': created, 'outcome': outcome,
                     'obligations': {k: sorted(obligations_of(store, k)) for k in created},
                     'retained_after': len(rig.retained('parent'))})

    # Three crashes, at each barrier of the publication protocol, then a lost write response.
    crash = faults.RaiseError(lambda: SimulatedCrash('the publisher died here'))
    publisher = restarted_publisher()
    attempt(publisher, 'payload.write.after', crash, 'after object write, before ownership')
    publisher = restarted_publisher()                        # restart: re-emits the same logical output
    publisher.publish_message(frame_array((1000, 1000), seed = 70))
    attempt(publisher, 'obligation.acquire.after', crash, 'after ownership, before publish')
    publisher = restarted_publisher()
    publisher.publish_message(frame_array((1000, 1000), seed = 71))
    attempt(publisher, 'publish.receipt.before', crash, 'after acceptance, before the receipt')
    publisher = restarted_publisher()
    publisher.publish_message(frame_array((1000, 1000), seed = 72))        # deduplicated: same operation identity
    attempt(publisher, 'payload.write.after', faults.RaiseError(lambda: ConnectionError('write response lost')),
            'lost object-store write response')
    publisher.publish_message(frame_array((1000, 1000), seed = 73))        # the publisher retries its put
    # A live publisher dies after ownership and its input is dropped: the publication is never retried.
    attempt(publisher, 'obligation.acquire.after', crash, 'after ownership, publication abandoned')
    evidence['runs'] = runs
    evidence['publication_stats'] = dict(publisher.publication_stats)
    # Every accepted envelope names an object that exists — nothing partially created was handed out.
    retained = rig.retained_with_seq('parent')
    referenced = [peek_envelope(body)['blob_ref'] for _seq, body in retained]
    assert len(retained) == 4 and all(referenced), referenced
    abandoned = [k for k in _inventory_keys(store) if k not in referenced and obligations_of(store, k) >= {'child'}]
    assert abandoned, 'the abandoned publication left no owned object to reconcile'
    assert set(referenced) <= set(store.keys) and all(object_exists(store, k) for k in referenced)
    # Recovery, at the publisher's restart: the ledger from broker evidence; a
    # fresh put is spared until the grace elapses.
    before_grace, facts = _ledger_from_evidence(rig, store, ['child'])
    created = _inventory_keys(store)                         # the crashed puts never returned their keys
    orphans = [k for k in created if k not in referenced and not obligations_of(store, k)]
    first = store.reconcile(before_grace, 'pay-007:restart')
    unreferenced = [k for k in created if k not in referenced]
    evidence['recovery'] = {'ledger': facts, 'first': {'reclaimed': list(first.reclaimed), 'retained': list(first.retained)},
                            'unreferenced': unreferenced, 'counterless': orphans}
    assert orphans and all(k in first.retained for k in orphans), \
        'an object whose put may still be in progress was reclaimed inside the orphan grace'
    assert all(k in first.reclaimed for k in unreferenced if k not in orphans), first
    assert all(k in first.reclaimed for k in abandoned), \
        'an object owned only by readers of a publication that never happened was not reclaimed'
    assert all(object_exists(store, k) for k in referenced), 'reconciliation reclaimed a referenced object'
    # The reader resolves every accepted envelope.
    delivered = []
    for _ in range(4):
        inputs = child.receive_message()
        delivered.append(inputs['parent']['message'].shape)
        child.ack_inputs()
    assert delivered == [(1000, 1000)] * 4, delivered
    advance(ORPHAN_GRACE + 1)
    started = time.monotonic()
    after_grace, _facts = _ledger_from_evidence(rig, store, ['child'])
    second = store.reconcile(after_grace, 'pay-007:after-grace')
    evidence['recovery']['second'] = {'reclaimed': list(second.reclaimed), 'retained': list(second.retained),
                                      'took_s': time.monotonic() - started}
    assert all(k in second.reclaimed for k in orphans), 'a counterless object outlived the orphan policy'
    assert not any(object_exists(store, k) for k in created), \
        f'objects without a live reference survive: {[k for k in created if object_exists(store, k)]}'
    return schedules


@pytest.mark.case('PAY-007')
@pytest.mark.level('process')
def test_pay_007_creation_crashes_and_partial_publication_produce_bounded(evidence_dir, record_faults,
                                                                          monkeypatch) -> None:
    '''
    PAY-007 (P1, payload, process): Creation crashes and partial publication produce bounded
    orphan recovery.

    Acceptance: After recovery every accepted envelope resolves; every object lacking a
    live/replay/terminal reference is reclaimed within G. No counterless object persists beyond
    its bounded orphan policy.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    rig = MemoryRig(BATCH)
    # No reference forwarding: a retry must not quietly adopt the crashed put's object, as on Redis.
    store = KeyRecordingStore(MemoryPayloadStore(rig.clock, orphan_grace_seconds = ORPHAN_GRACE, forward_unchanged = False))
    rig.store = store
    try:
        schedules = _oracle_pay_007(rig, store, rig.clock.advance, evidence)
    finally:
        rig.close()
    write_evidence(evidence_dir, 'orphan_recovery.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.case('PAY-007')
@pytest.mark.level('process')
@pytest.mark.variant('redis')
def test_pay_007_redis_store_reconciles_orphans_past_the_grace(evidence_dir, record_faults, monkeypatch) -> None:
    '''The Redis store on the in-process fake: metadata dates the interrupted put, the grace spares it, then not.'''
    pytest.importorskip('redis')
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    rig = MemoryRig(BATCH)
    redis_store, fake = _fake_redis_store(clock = lambda: rig.clock.now())
    store = KeyRecordingStore(redis_store)
    rig.store = store

    def advance(seconds : float) -> None:
        rig.clock.advance(seconds)
        fake.advance(seconds)

    try:
        schedules = _oracle_pay_007(rig, store, advance, evidence)
    finally:
        rig.close()
    write_evidence(evidence_dir, 'orphan_recovery.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.negative_control(of = 'PAY-007')
def test_pay_007_detects_a_publisher_without_an_intent(monkeypatch) -> None:
    '''No intent ties an object to its publication: an unpublished object is nobody's to reclaim.'''
    monkeypatch.setattr(constants, 'RFC0006', True)
    defects_pay.intent_less_bridge(monkeypatch)
    rig = MemoryRig(BATCH)
    store = KeyRecordingStore(MemoryPayloadStore(rig.clock, orphan_grace_seconds = ORPHAN_GRACE, forward_unchanged = False))
    rig.store = store
    try:
        assert defects.detects(_oracle_pay_007, rig, store, rig.clock.advance, {})
    finally:
        rig.close()


# -- PAY-012 ----------------------------------------------------------------------

FRAMES = 1000
RECONCILE_EVERY = 20
#: The declared live budget: one reclamation horizon's worth of frames, plus two in flight.
BUDGET_FRAMES = RECONCILE_EVERY + 2


def _live_ledger(rig : Any, store : KeyRecordingStore, readers : List[str], retired : List[str]) -> Any:
    '''A retained live frame is owed to every non-retired reader whose ack floor is below it; nothing else is owed anything.'''
    floors = {reader: rig.ack_floor(reader, 'parent') for reader in readers}
    required : Dict[str, tuple] = {key: () for key in store.keys}
    for seq, body in rig.retained_with_seq('parent'):
        key = peek_envelope(body).get('blob_ref')
        if key is not None:
            required[key] = tuple(r for r in readers if r not in retired and seq > floors[r])
    return ledger(required)


def _oracle_pay_012(rig : Any, store : KeyRecordingStore, size_of : Callable[[], int],
                    evidence : Dict[str, Any], frames : int = FRAMES) -> faults.FaultSchedule:
    '''
    Latest-buffer-one live channel, a fast reader keeping up, a slow reader that
    never settles anything. The reconciler runs every ``RECONCILE_EVERY`` frames
    against the broker's retained set; stored bytes must plateau.
    '''
    publisher = rig.messenger('parent', [], store = store, blob_reader_ids = ['fast', 'slow'])
    fast = rig.messenger('fast', ['parent'])
    rig.messenger('slow', ['parent'])                        # subscribed, never receives
    schedule = faults.FaultSchedule({'obligation.release.after': faults.Delay(0.0)})
    frame = FRAME.copy()
    samples : List[Dict[str, Any]] = []
    frame_size = None
    with schedule:
        for i in range(frames):
            frame.flat[0] = i & 0xFF
            publisher.publish_message(frame)
            inputs = fast.receive_message()
            assert inputs['parent']['message'].shape == FRAME.shape
            if frame_size is None:
                frame_size = store.inner.ref_for_key(store.keys[-1]).size
            if i < frames - 1:
                fast.ack_inputs()                            # the last frame stays in fast's hands
            if (i + 1) % RECONCILE_EVERY == 0 or i == frames - 1:
                result = store.reconcile(_live_ledger(rig, store, ['fast', 'slow'], []), f'pay-012:{i + 1}')
                samples.append({'frames': i + 1, 'stored_bytes': size_of(), 'reclaimed': len(result.reclaimed),
                                'retained': len(result.retained)})
    assert frame_size is not None
    budget = BUDGET_FRAMES * frame_size
    evidence.update({'frames': frames, 'frame_bytes': frame_size, 'budget_bytes': budget, 'samples': samples,
                     'peak_bytes': max(s['stored_bytes'] for s in samples), 'faults': schedule.fired()})
    assert all(s['stored_bytes'] <= budget for s in samples), \
        f'stored bytes grew past the live budget: peak {evidence["peak_bytes"]} > {budget}'
    # Stop: the frame fast still holds is protected; the slow reader is retired by policy; GC.
    held = store.keys[-1]
    assert obligations_of(store, held) >= {'fast'}
    started = time.monotonic()
    result = store.reconcile(_live_ledger(rig, store, ['fast', 'slow'], retired = ['slow']), 'pay-012:retire-slow')
    assert held in result.retained and isinstance(store.inner.read(store.inner.ref_for_key(held)), PayloadBytes), \
        'a frame still legitimately in use was deleted by GC'
    assert obligations_of(store, held) == {'fast'}, obligations_of(store, held)
    fast.ack_inputs()
    survivors = [k for k in store.keys if object_exists(store, k)]
    evidence['after_stop'] = {'survivors': survivors, 'stored_bytes': size_of(), 'gc_s': time.monotonic() - started}
    assert not survivors, f'unpinned objects survive after stop and retirement: {len(survivors)}'
    assert time.monotonic() - started < G_SECONDS
    return schedule


def _memory_rig_pay_012() -> tuple:
    rig = MemoryRig(REALTIME)
    store = KeyRecordingStore(MemoryPayloadStore(rig.clock, forward_unchanged = False))
    rig.store = store
    return rig, store


@pytest.mark.case('PAY-012')
@pytest.mark.level('process')
def test_pay_012_dropped_and_disconnected_reader_frames_have_a_bounded(evidence_dir, record_faults,
                                                                       monkeypatch) -> None:
    '''
    PAY-012 (P0, payload, process): Dropped and disconnected-reader frames have a bounded
    reclamation path.

    Acceptance: After steady state stored bytes remain within budget plus bounded in-flight
    allowance; after stop, all unpinned objects are reclaimed within G without deleting valid
    in-use frames.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    rig, store = _memory_rig_pay_012()
    try:
        schedule = _oracle_pay_012(rig, store, store.inner.stored_bytes, evidence)
    finally:
        rig.close()
    write_evidence(evidence_dir, 'stored_bytes.json', evidence)
    record_faults(schedule)


@pytest.mark.case('PAY-012')
@pytest.mark.level('broker')
@pytest.mark.variant('redis')
def test_pay_012_jetstream_eviction_is_reconciled_against_a_redis_store(nats_url, redis_url, evidence_dir,
                                                                        record_faults, monkeypatch) -> None:
    '''A real REALTIME stream (``max_msgs = 1``) evicting under a slow reader; the store is a Redis database of its own.'''
    from _brokers import redis_client

    from videoflow.wire.redis_payload_store import RedisPayloadStore
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    url = isolated_redis_db(redis_url, 7)
    specs = [spec('parent', [], 'producer', True), spec('fast', ['parent'], 'consumer', False),
             spec('slow', ['parent'], 'consumer', False)]
    with redis_client(url) as client:
        store = KeyRecordingStore(RedisPayloadStore(url))
        rig = JetStreamRig(nats_url, REALTIME, specs, store = store, ack_wait = 30)

        def stored_bytes() -> int:
            return sum(int(client.strlen(k)) for k in store.keys if client.exists(k))

        try:
            schedule = _oracle_pay_012(rig, store, stored_bytes, evidence, frames = 400)
        finally:
            rig.close()                                     # sweeps exactly the keys the rig's publishers created
    write_evidence(evidence_dir, 'stored_bytes.json', evidence)
    record_faults(schedule)


@pytest.mark.case('PAY-012')
@pytest.mark.level('process')
@pytest.mark.variant('ledger')
@pytest.mark.pending('phase 3')
def test_pay_012_the_runtime_ledger_cancels_evicted_messages_itself() -> None:
    '''
    The Phase-3 sub-assertion: the runtime's ``ObligationLedger`` lists the
    broker-evicted messages (stream sequences below ``stream_info().state.first_seq``)
    on its own and the worker reconciles periodically without a test-built ledger
    (RFC 0006 ``BLOB-14`` step 4). Pending the runtime ledger.
    '''


@pytest.mark.negative_control(of = 'PAY-012')
def test_pay_012_detects_a_store_with_no_eviction_reconciliation(monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    defects_pay.leaking_reconciler(monkeypatch)
    rig, store = _memory_rig_pay_012()
    try:
        assert defects.detects(_oracle_pay_012, rig, store, store.inner.stored_bytes, {}, 200)
    finally:
        rig.close()


# -- PAY-013 ----------------------------------------------------------------------

def _same_output(messenger : Any, trace : str, seq : int, payload : Any) -> None:
    '''A processor re-emitting the same logical output: the carried-forward identity, so the id is the same.'''
    messenger._last_trace_id = trace
    messenger._last_seq = seq
    messenger.publish_message(payload)


def _oracle_pay_013(rig : Any, store : KeyRecordingStore, limit_channel : Callable[[Optional[int]], None],
                    sever_channel : Callable[[str], None], evidence : Dict[str, Any]) -> faults.FaultSchedule:
    child = rig.messenger('child', ['parent'])
    parent = rig.messenger('parent', [], store = store, blob_reader_ids = ['child'])
    stages : Dict[str, Any] = {}

    def created_by(action : Callable[[], None]) -> List[str]:
        before = list(store.keys)
        action()
        return [k for k in store.keys if k not in before]

    # (1) a full stream: a definite (retryable, then final) rejection.
    limit_channel(1)
    _same_output(parent, 't-full', 1, FRAME)
    with pytest.raises(BrokerUnavailable):
        rejected = created_by(lambda: _same_output(parent, 't-full', 2, frame_array((1000, 1000), seed = 13)))
    rejected = [k for k in store.keys if k != store.keys[0]]
    stages['full_stream'] = {'created': rejected, 'obligations': {k: sorted(obligations_of(store, k)) for k in rejected},
                             'stats': dict(parent.publication_stats)}
    limit_channel(None)
    # (2) a definite connection rejection: nothing captures the subject.
    stray = rig.messenger('parent', [], store = store, blob_reader_ids = ['child'], run_id = 'severed')
    sever_channel('severed')
    with pytest.raises(BrokerUnavailable):
        created_by(lambda: _same_output(stray, 't-severed', 1, FRAME))
    severed = store.keys[len(rejected) + 1:]
    stages['severed'] = {'created': severed, 'obligations': {k: sorted(obligations_of(store, k)) for k in severed}}
    # (3) ten retries of one logical output: one acceptance, nine duplicate acknowledgments, ten objects.
    before = len(store.keys)
    for _ in range(10):
        _same_output(parent, 't-dup', 1, FRAME)
    duplicates = store.keys[before:]
    stages['duplicates'] = {'created': len(duplicates), 'stats': dict(parent.publication_stats)}
    assert parent.publication_stats.get('duplicate', 0) >= 9, parent.publication_stats
    # (4) a lost acceptance response, reconciled by an idempotent re-publish inside the dedup window.
    schedule = faults.FaultSchedule({'publish.receipt.before': faults.Nth(1, faults.DropResponse())})
    with schedule:
        before = len(store.keys)
        _same_output(parent, 't-lost', 1, frame_array((1000, 1000), seed = 14))
    lost = store.keys[before:]
    stages['lost_receipt'] = {'created': lost, 'stats': dict(parent.publication_stats),
                              'obligations': {k: sorted(obligations_of(store, k)) for k in lost}}
    assert len(lost) == 1 and parent.publication_stats.get('unknown') == 1, stages['lost_receipt']
    # Reachability: the retained envelopes name exactly the objects readers will need.
    retained = rig.retained_with_seq('parent')
    referenced = {peek_envelope(body)['blob_ref'] for _seq, body in retained}
    assert len(retained) == 3 and len(referenced) == 3, [peek_envelope(b)['blob_ref'] for _s, b in retained]
    started = time.monotonic()
    result = store.reconcile(_ledger_from_evidence(rig, store, ['child'])[0], 'pay-013:reconcile')
    unreachable = [k for k in store.keys if k not in referenced and object_exists(store, k)]
    evidence.update({'stages': stages, 'created': len(store.keys), 'referenced': sorted(referenced),
                     'reconcile': {'reclaimed': len(result.reclaimed), 'retained': len(result.retained),
                                   'unknown': list(result.unknown), 'took_s': time.monotonic() - started},
                     'faults': schedule.fired()})
    assert not unreachable, f'{len(unreachable)} unreachable object(s) survive reconciliation'
    assert all(object_exists(store, k) for k in referenced), 'a referenced object was reclaimed'
    for _ in range(3):
        inputs = child.receive_message()
        assert inputs['parent']['message'].shape == FRAME.shape, 'a retained envelope no longer resolves its object'
        child.ack_inputs()
    assert not any(object_exists(store, k) for k in store.keys)
    return schedule


def _memory_rig_pay_013() -> tuple:
    rig = MemoryRig(BATCH)
    store = KeyRecordingStore(MemoryPayloadStore(rig.clock, forward_unchanged = False))
    rig.store = store

    def limit_channel(max_msgs : Optional[int]) -> None:
        rig.backend.ensure_channel(make_channel(rig.flow_id, rig.run_id, 'parent', RELIABLE_WORK, RETENTION_INTEREST,
                                                max_msgs = max_msgs or topology.DEFAULT_BATCH_MAX_MSGS,
                                                overflow = OVERFLOW_REJECT), 'pay-013')

    def sever_channel(run_id : str) -> None:
        observation = rig.backend.close([rig.channel('parent', run_id)], 'pay-013')
        assert observation.complete, observation
    return rig, store, limit_channel, sever_channel


@pytest.mark.case('PAY-013')
@pytest.mark.level('broker')
def test_pay_013_rejected_and_deduplicated_publications_do_not_accumulate(nats_url, redis_url, evidence_dir,
                                                                          record_faults, monkeypatch) -> None:
    '''
    PAY-013 (P1, payload, broker): Rejected and deduplicated publications do not accumulate
    unreachable blobs.

    Acceptance: After G there is no unreachable object from definite rejection/deduplication;
    each actually retained envelope still resolves its exact referenced object.
    '''
    import nats  # optional dep (distributed extras)
    from _brokers import delete_run, redis_client, run_async

    from videoflow.wire.redis_payload_store import RedisPayloadStore
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, '_PUBLISH_TIMEOUT', 3)        # a full stream is final after 3 s here
    evidence : Dict[str, Any] = {}
    url = isolated_redis_db(redis_url, 8)
    specs = [spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False)]
    with redis_client(url):
        store = KeyRecordingStore(RedisPayloadStore(url))
        rig = JetStreamRig(nats_url, BATCH, specs, store = store, ack_wait = 30)

        def limit_channel(max_msgs : Optional[int]) -> None:
            config = topology.stream_config_for(rig.flow_id, rig.run_id, 'parent', BATCH,
                                                batch_max_msgs = max_msgs or topology.DEFAULT_BATCH_MAX_MSGS)

            async def _go() -> None:
                nc = await nats.connect(nats_url)
                try:
                    await nc.jetstream().update_stream(config)
                finally:
                    await nc.close()
            run_async(_go)

        def sever_channel(run_id : str) -> None:
            delete_run(nats_url, rig.flow_id, run_id)

        try:
            rig.provision_run('severed')
            schedule = _oracle_pay_013(rig, store, limit_channel, sever_channel, evidence)
        finally:
            rig.close()                                     # sweeps exactly the keys the rig's publishers created
    write_evidence(evidence_dir, 'object_reachability.json', evidence)
    record_faults(schedule)


@pytest.mark.case('PAY-013')
@pytest.mark.level('broker')
@pytest.mark.variant('memory')
def test_pay_013_memory_backends_reclaim_every_unreachable_object(evidence_dir, record_faults, monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, '_PUBLISH_TIMEOUT', 2)
    evidence : Dict[str, Any] = {}
    rig, store, limit_channel, sever_channel = _memory_rig_pay_013()
    try:
        schedule = _oracle_pay_013(rig, store, limit_channel, sever_channel, evidence)
    finally:
        rig.close()
    write_evidence(evidence_dir, 'object_reachability.json', evidence)
    record_faults(schedule)


@pytest.mark.negative_control(of = 'PAY-013')
def test_pay_013_detects_a_store_that_never_reclaims_a_rejected_publications_object(monkeypatch) -> None:
    '''
    The reviewed pair: a publisher that settles only its intent on a refused or
    deduplicated send, and a reconciler that never reclaims what that leaves —
    either half alone is covered by the fixed other, so the control removes both.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, '_PUBLISH_TIMEOUT', 2)
    defects_pay.leaking_reconciler(monkeypatch)
    defects_pay.intent_only_resolution(monkeypatch)
    rig, store, limit_channel, sever_channel = _memory_rig_pay_013()
    try:
        assert defects.detects(_oracle_pay_013, rig, store, limit_channel, sever_channel, {})
    finally:
        rig.close()
