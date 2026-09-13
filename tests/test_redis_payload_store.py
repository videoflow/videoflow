'''
RedisPayloadStore (RFC 0006 BLOB-13/14/15) against the fake Redis in
tests/support_redis.py: no server, but real WATCH/MULTI/EXEC semantics, expiry,
transport faults and hand-interleaved "processes" (two store handles on one fake).

Each test names the catalog case it proves — PAY-002 (transient reads), PAY-003
(missing/corrupt), PAY-005 (release idempotent by reader), PAY-007/013 (orphans,
intents), PAY-008/021 (atomic release, generation fences), PAY-014 (dead-letter
pins) — plus the RFC 0002 counter fallback (BLOB-5/6) for blobs an older
publisher wrote.
'''
from __future__ import absolute_import, division, print_function

import hashlib
import itertools
import subprocess
import sys

import pytest

pytest.importorskip('redis')
from redis import exceptions as rexc
from support_redis import FakeRedis, keyslot

from videoflow.backends import faults
from videoflow.backends.memory.payload import StaticLedger
from videoflow.backends.outcomes import Known, Unknown
from videoflow.backends.payload import (
    Corrupt,
    ImmutablePayloadRef,
    Missing,
    PayloadBytes,
    ReclamationObservation,
    RetentionContract,
    TransientFailure,
)
from videoflow.core.errors import ResourceUnavailable
from videoflow.core.errors import TransientFailure as TransientError
from videoflow.wire import redis_payload_store as rps
from videoflow.wire import serialization as s
from videoflow.wire.redis_payload_store import (
    RedisPayloadStore,
    counter_key,
    metadata_key,
    obligation_key,
    redis_capabilities_observed,
)

READERS = ('det', 'track/p0', 'track/p1')
INTENT = 'intent/745708c2eeaf98a60fdbebafe1d04eff'
FRAME = b'\x00\x01\x02' * 1000


def _contract(ttl = 3600, horizon = 3600, obligations = READERS, durable = True):
    return RetentionContract(ttl, horizon, durable, tuple(obligations))


@pytest.fixture
def fake():
    return FakeRedis()


def _store(fake, **kwargs):
    return RedisPayloadStore(client = fake, clock = fake.time, **kwargs)


def _keys(key):
    return key, metadata_key(key), obligation_key(key)


def _members(fake, key):
    value = fake.value(obligation_key(key))
    return set() if value is None else {m.decode() for m in value}


def _legacy_store(fake):
    '''RedisBlobStore (RFC 0002) on the same fake: an older publisher sharing the server.'''
    store = s.RedisBlobStore.__new__(s.RedisBlobStore)  # skip __init__: no server
    store._client = fake
    return store


# -- put ---------------------------------------------------------------------------

def test_put_writes_blob_then_metadata_then_obligations_with_one_ttl(fake):
    store = _store(fake)
    ref = store.put(FRAME, 'cam:t1:5', _contract(ttl = 120, horizon = 120, obligations = READERS + (INTENT,)))
    assert ref.store == 'redis' and ref.key.startswith('vf-blob-') and len(ref.key) == len('vf-blob-') + 32
    assert ref.size == len(FRAME) and ref.digest == hashlib.sha256(FRAME).hexdigest()
    assert len(ref.generation) == 8 and ref.content_id == 'cam:t1:5'
    blob, meta, obl = _keys(ref.key)
    assert fake.value(blob) == FRAME
    record = {k.decode(): v.decode() for k, v in fake.value(meta).items()}
    assert record == {'size': str(len(FRAME)), 'digest': ref.digest, 'generation': ref.generation,
                      'content_id': 'cam:t1:5', 'created_at': repr(fake.now)}
    assert _members(fake, ref.key) == set(READERS) | {INTENT}
    assert fake.ttl(blob) == fake.ttl(meta) == fake.ttl(obl) == 120
    # BLOB-13: the companions are hash-tagged onto the blob key — one Cluster slot.
    assert len({keyslot(k) for k in (blob, meta, obl)}) == 1
    # BLOB-14 step 1: bytes first, then metadata, then the set — an interrupted put is TTL-only.
    assert [e[0] for e in fake.log if e[0] in ('SET', 'HSET', 'SADD')] == ['SET', 'HSET', 'SADD']
    assert fake.commands('SET')[0][1] == blob and fake.commands('HSET')[0][1] == meta
    assert fake.commands('SADD')[0][1] == obl


def test_put_without_obligations_is_ttl_only_and_obligations_pin_to_the_horizon(fake):
    store = _store(fake)
    plain = store.put(FRAME, 'c', _contract(ttl = 10, horizon = 100, obligations = ()))
    assert fake.live_keys('vf-blobobl-*') == [] and fake.ttl(plain.key) == 10
    pinned = store.put(FRAME, 'c', _contract(ttl = 10, horizon = 100, obligations = ('x',)))
    assert all(fake.ttl(k) == 100 for k in _keys(pinned.key))
    forever = store.put(FRAME, 'c', _contract(ttl = 0, obligations = ('x',)))
    assert all(fake.ttl(k) == -1 for k in _keys(forever.key))


def test_put_failures_are_typed_and_leave_at_most_ttl_only_bytes(fake):
    store = _store(fake)
    fake.fail('SET', rexc.OutOfMemoryError('OOM command not allowed when used memory > maxmemory.'))
    with pytest.raises(ResourceUnavailable):
        store.put(FRAME, 'c', _contract())
    fake.fail('HSET', rexc.ConnectionError('Connection closed by server.'))
    with pytest.raises(TransientError):
        store.put(FRAME, 'c', _contract())
    # The bytes made it, nothing else did: TTL-only, never reclaimable early (BLOB-5).
    assert len(fake.live_keys('vf-blob-*')) == 1
    assert fake.live_keys('vf-blobmeta-*') == [] and fake.live_keys('vf-blobobl-*') == []


# -- read (PAY-002, PAY-003) ---------------------------------------------------------

def test_read_verifies_the_digest_and_types_missing_and_corrupt(fake):
    store = _store(fake)
    ref = store.put(FRAME, 'c', _contract())
    out = store.read(ref, reader = 'det')
    assert isinstance(out, PayloadBytes) and out.data == FRAME and out.digest_verified
    damaged = FRAME[:-1] + b'!'
    fake.set(ref.key, damaged, keepttl = True)
    out = store.read(ref)
    assert isinstance(out, Corrupt) and out.expected_digest == ref.digest
    assert out.actual_digest == hashlib.sha256(damaged).hexdigest()
    # A reader that only has the key verifies against the metadata digest.
    bare = ImmutablePayloadRef('redis', ref.key, 0, '', '', '')
    assert isinstance(store.read(bare), Corrupt)
    fake.set(ref.key, FRAME, keepttl = True)
    restored = store.read(bare)
    assert isinstance(restored, PayloadBytes) and restored.digest_verified
    fake.unlink(ref.key)
    assert isinstance(store.read(ref), Missing)


def test_read_without_metadata_is_unverified_not_refused(fake):
    key = _legacy_store(fake).put(FRAME, ttl_seconds = 60)   # an RFC 0002 publisher: no metadata
    store = _store(fake)
    ref = store.ref_for_key(key)
    assert ref.store == 'unknown' and ref.digest == '' and ref.generation == ''
    out = store.read(ref)
    assert isinstance(out, PayloadBytes) and out.data == FRAME and not out.digest_verified


def test_transport_failures_on_read_are_transient_never_malformed(fake):
    store = _store(fake)
    ref = store.put(FRAME, 'c', _contract())
    for exc in (rexc.ConnectionError('gone'), rexc.TimeoutError('slow'), rexc.BusyLoadingError('LOADING')):
        fake.fail('GET', exc)
        out = store.read(ref)
        assert isinstance(out, TransientFailure) and type(exc).__name__ in out.reason
        assert isinstance(store.read(ref), PayloadBytes)   # the bytes were there all along
    with faults.FaultSchedule({'payload.read.before': faults.RaiseError(lambda: TransientError('blip'))}):
        assert isinstance(store.read(ref), TransientFailure)


# -- release (PAY-005, PAY-008, PAY-021) -----------------------------------------------

def test_release_lifecycle_follows_the_rfc_worked_example(fake):
    store = _store(fake)
    ref = store.put(FRAME, 'cam:cam:3f9c1a2b7d4e:5:5', _contract(obligations = (INTENT,) + READERS))
    blob, meta, obl = _keys(ref.key)
    receipt = store.release_obligation(ref, INTENT, 'puback')
    assert (receipt.remaining, receipt.reclaimed, receipt.stale, receipt.unknown) == (3, False, False, False)
    assert store.release_obligation(ref, 'det', 'ack').remaining == 2
    assert store.release_obligation(ref, 'track/p1', 'ack').remaining == 1
    assert isinstance(store.read(ref, reader = 'track/p0'), PayloadBytes)
    last = store.release_obligation(ref, 'track/p0', 'ack-and-skip')
    assert last.reclaimed and last.remaining == 0
    assert fake.live_keys('vf-blob*') == []
    # One EXEC did SREM + UNLINK of all three keys (BLOB-14 step 2).
    assert ('UNLINK', blob, meta, obl) in fake.log
    assert isinstance(store.read(ref), Missing)


def test_release_is_idempotent_by_reader_across_handles_and_processes(fake):
    a, b = _store(fake), _store(fake)   # two worker processes sharing one server
    ref = a.put(FRAME, 'c', _contract(obligations = ('x', 'y')))
    assert a.release_obligation(ref, 'x', 'ack:newer-handle').remaining == 1
    assert a.release_obligation(ref, 'x', 'ack:older-handle').remaining == 1
    restarted = b.ref_for_key(ref.key)   # X after a restart: only the wire key survived
    assert restarted == ref
    assert b.release_obligation(restarted, 'x', 'ack:after-restart').remaining == 1
    assert _members(fake, ref.key) == {'y'}
    out = b.read(restarted, reader = 'y')
    assert isinstance(out, PayloadBytes) and out.data == FRAME
    assert b.release_obligation(ref, 'y', 'ack').reclaimed
    # After the final cleanup: harmless, and nothing is recreated for a key nobody owns.
    again = a.release_obligation(ref, 'y', 'ack')
    assert (again.remaining, again.reclaimed, again.stale, again.unknown) == (None, False, False, False)
    assert fake.live_keys('vf-blob*') == []


def test_lost_release_response_is_unknown_and_the_retry_is_harmless(fake):
    store = _store(fake)
    ref = store.put(FRAME, 'c', _contract(obligations = ('x', 'y')))
    fake.lose_response('EXEC')
    first = store.release_obligation(ref, 'x', 'ack')
    assert first.unknown and first.remaining is None and not first.reclaimed
    assert _members(fake, ref.key) == {'y'}   # it had been applied
    again = store.release_obligation(ref, 'x', 'ack')
    assert not again.unknown and again.remaining == 1
    # A transport error on the EXEC itself: the caller cannot tell it apart, so it is
    # unknown too; nothing was applied, and the retry applies it.
    fake.fail('EXEC', rexc.ConnectionError('Connection closed by server.'))
    assert store.release_obligation(ref, 'y', 'ack').unknown
    assert fake.value(ref.key) == FRAME
    assert store.release_obligation(ref, 'y', 'ack').reclaimed
    assert fake.live_keys('vf-blob*') == []
    # A transport error before anything was sent is simply retried.
    other = store.put(FRAME, 'c', _contract(obligations = ('x',)))
    fake.fail('WATCH', rexc.ConnectionError('blip'))
    receipt = store.release_obligation(other, 'x', 'ack')
    assert receipt.reclaimed and not receipt.unknown


def test_dropped_response_at_the_barrier_reports_unknown(fake):
    store = _store(fake)
    ref = store.put(FRAME, 'c', _contract(obligations = ('x', 'y')))
    with faults.FaultSchedule({'obligation.release.after': faults.DropResponse()}):
        receipt = store.release_obligation(ref, 'x', 'ack')
    assert receipt.unknown and receipt.remaining is None
    assert store.release_obligation(ref, 'x', 'ack').remaining == 1


def test_stale_generation_touches_nothing(fake):
    store = _store(fake)
    ref = store.put(FRAME, 'c', _contract(obligations = ('x',)))
    stale = ImmutablePayloadRef(ref.store, ref.key, ref.size, ref.digest, 'deadbeef', ref.content_id)
    before = len(fake.log)
    receipt = store.release_obligation(stale, 'x', 'late-ack-from-a-fenced-worker')
    assert receipt.stale and not receipt.reclaimed and receipt.remaining == 1
    assert fake.value(ref.key) == FRAME and _members(fake, ref.key) == {'x'}
    assert not [e for e in fake.log[before:] if e[0] in ('SREM', 'UNLINK', 'MULTI')]
    # The rightful generation still releases and reclaims.
    assert store.release_obligation(ref, 'x', 'ack').reclaimed


def test_unfenced_ref_honours_the_set_but_leaves_the_bytes_to_their_ttl(fake):
    store = _store(fake)
    ref = store.put(FRAME, 'c', _contract(obligations = ('x',)))
    fake.unlink(metadata_key(ref.key))   # metadata evicted; the set survives
    bare = store.ref_for_key(ref.key)
    assert bare.generation == ''
    receipt = store.release_obligation(bare, 'x', 'ack')
    assert receipt.remaining == 0 and not receipt.reclaimed and not receipt.stale
    assert fake.value(ref.key) == FRAME and fake.live_keys('vf-blobobl-*') == []


def test_concurrent_final_releases_reclaim_exactly_once(fake):
    a, b = _store(fake), _store(fake)
    ref = a.put(FRAME, 'c', _contract(obligations = ('x', 'y')))
    receipts = {}

    def other_process_releases_first(server):   # between A's WATCH and its EXEC
        receipts['y'] = b.release_obligation(ref, 'y', 'ack')
    fake.hook('EXEC', other_process_releases_first)
    receipts['x'] = a.release_obligation(ref, 'x', 'ack')
    assert receipts['y'].remaining == 1 and not receipts['y'].reclaimed
    assert receipts['x'].reclaimed and receipts['x'].remaining == 0 and not receipts['x'].unknown
    assert fake.live_keys('vf-blob*') == []
    # A's first EXEC came back nil and it re-read before deciding: B's WATCH plus two of A's.
    assert len([e for e in fake.log if e[0] == 'WATCH' and e[1] == obligation_key(ref.key)]) == 3


def test_watch_conflicts_are_bounded_then_unknown(fake):
    store = _store(fake)
    ref = store.put(FRAME, 'c', _contract(obligations = ('x',)))
    late = itertools.count()
    fake.hook('EXEC', lambda server: server.sadd(obligation_key(ref.key), f'late/{next(late)}'), times = None)
    receipt = store.release_obligation(ref, 'x', 'ack')
    assert receipt.unknown and receipt.remaining is None and not receipt.reclaimed
    assert fake.value(ref.key) == FRAME
    assert len(fake.commands('WATCH')) == rps.TRANSACTION_RETRIES


# -- RFC 0002 fallback (BLOB-5/6) ---------------------------------------------------------

def test_counter_only_blob_uses_rfc0002_counter_semantics(fake):
    legacy = _legacy_store(fake)
    key = legacy.put_with_readers(FRAME, 2, ttl_seconds = 60)
    store = _store(fake)
    ref = store.ref_for_key(key)
    receipt = store.release_obligation(ref, 'c1', 'ack')
    assert (receipt.remaining, receipt.reclaimed, receipt.unknown) == (None, False, False)
    assert int(fake.value(counter_key(key))) == 1 and fake.live_keys('vf-blobobl-*') == []
    receipt = store.release_obligation(ref, 'c2', 'ack')
    assert receipt.reclaimed and fake.live_keys('vf-blob*') == []
    # Mixed-version rollout: the counter store still understands what the new one leaves.
    key2 = legacy.put_with_readers(FRAME, 2)
    assert store.release_obligation(store.ref_for_key(key2), 'c1', 'ack').remaining is None
    legacy.release(key2)
    assert fake.live_keys('vf-blob*') == []


def test_counter_expiring_between_read_and_decrement_cannot_go_negative(fake):
    legacy = _legacy_store(fake)
    key = legacy.put_with_readers(FRAME, 1, ttl_seconds = 60)
    store = _store(fake)
    # The counter expires after the store read it and before its DECR reaches the server.
    fake.hook('EXEC', lambda server: server.expire_now(counter_key(key)))
    receipt = store.release_obligation(store.ref_for_key(key), 'c1', 'ack')
    assert (receipt.remaining, receipt.reclaimed, receipt.unknown) == (None, False, False)
    assert fake.value(key) == FRAME                   # the object survives: its TTL governs now
    assert fake.live_keys('vf-blobrc-*') == []        # no counter re-created at -1
    assert 'DECR' not in [e[0] for e in fake.log]     # the transaction aborted before applying
    # Negative control — the reviewed defect in RedisBlobStore.release under the same interleaving.
    key2 = legacy.put_with_readers(FRAME, 1, ttl_seconds = 60)
    fake.hook('DECR', lambda server: server.expire_now(counter_key(key2)))
    legacy.release(key2)
    assert fake.value(key2) is None and int(fake.value(counter_key(key2)) or 0) == 0


def test_release_with_neither_set_nor_counter_creates_nothing(fake):
    store = _store(fake)
    ref = store.put(FRAME, 'c', _contract(obligations = ()))   # TTL-only by contract
    receipt = store.release_obligation(ref, 'x', 'ack')
    assert (receipt.remaining, receipt.reclaimed, receipt.stale) == (None, False, False)
    assert fake.value(ref.key) == FRAME
    assert fake.live_keys('vf-blobobl-*') == [] and fake.live_keys('vf-blobrc-*') == []


# -- acquire (PAY-014) ----------------------------------------------------------------------

def test_acquire_extends_every_key_never_shortens_and_pins_a_dead_letter(fake):
    store = _store(fake)
    ref = store.put(FRAME, 'c', _contract(ttl = 30, horizon = 30, obligations = ('x',)))
    receipt = store.acquire_obligation(ref, 'dlq/flow', fake.now + 300)
    assert receipt.deadline == fake.now + 300 and receipt.obligation_id == 'dlq/flow' and receipt.ref == ref
    assert all(fake.ttl(k) == 300 for k in _keys(ref.key))
    assert _members(fake, ref.key) == {'x', 'dlq/flow'}
    store.renew_obligation(ref, 'dlq/flow', fake.now + 10)    # shorter: never shortens
    assert all(fake.ttl(k) == 300 for k in _keys(ref.key))
    store.renew_obligation(ref, 'dlq/flow', fake.now + 600)
    assert all(fake.ttl(k) == 600 for k in _keys(ref.key))
    # The ordinary reader finishes; the pin keeps the bytes for the forensic horizon.
    assert store.release_obligation(ref, 'x', 'ack').remaining == 1
    fake.advance(31)
    assert isinstance(store.read(ref), PayloadBytes)
    assert store.release_obligation(ref, 'dlq/flow', 'aged-out').reclaimed


def test_acquire_on_a_ttl_only_object_gives_the_new_set_the_objects_life(fake):
    store = _store(fake)
    plain = store.put(FRAME, 'c', _contract(ttl = 100, obligations = ()))
    store.acquire_obligation(plain, 'dlq/flow', fake.now + 50)
    assert fake.ttl(obligation_key(plain.key)) == 100 and fake.ttl(plain.key) == 100
    persistent = store.put(FRAME, 'c', _contract(ttl = 0, obligations = ()))
    store.acquire_obligation(persistent, 'dlq/flow', fake.now + 50)
    assert fake.ttl(obligation_key(persistent.key)) == -1 and fake.ttl(persistent.key) == -1


def test_acquire_is_fenced_and_types_its_failures(fake):
    store = _store(fake)
    ref = store.put(FRAME, 'c', _contract(obligations = ('x',)))
    stale = ImmutablePayloadRef(ref.store, ref.key, ref.size, ref.digest, 'deadbeef', ref.content_id)
    with pytest.raises(LookupError):
        store.acquire_obligation(stale, 'dlq/flow', fake.now + 10)
    with pytest.raises(LookupError):
        store.acquire_obligation(ImmutablePayloadRef('redis', 'vf-blob-nope', 0, '', '', ''), 'dlq/flow',
                                 fake.now + 10)
    fake.fail('WATCH', rexc.TimeoutError('slow'))
    with pytest.raises(TransientError):
        store.acquire_obligation(ref, 'dlq/flow', fake.now + 10)
    assert _members(fake, ref.key) == {'x'}
    # The last reader releases between the acquire's WATCH and its EXEC: nothing is resurrected.
    fake.hook('EXEC', lambda server: store.release_obligation(ref, 'x', 'ack'))
    with pytest.raises(LookupError):
        store.acquire_obligation(ref, 'dlq/flow', fake.now + 10)
    assert fake.live_keys('vf-blob*') == []


# -- reconcile (PAY-007, PAY-013) --------------------------------------------------------------

def test_reconcile_reclaims_orphans_past_the_grace_and_keeps_ttl_only_and_counted_blobs(fake):
    store = _store(fake, orphan_grace_seconds = 60)
    live = store.put(FRAME, 'c', _contract(obligations = ('det',)))
    orphan = store.put(FRAME, 'c', _contract(obligations = ('det',)))
    fake.unlink(obligation_key(orphan.key))   # a put that died after its metadata, before its set
    legacy = _legacy_store(fake)
    ttl_only = legacy.put(FRAME)              # RFC 0002, readers unknown: its TTL is the contract
    counted = legacy.put_with_readers(FRAME, 2)   # RFC 0002, readers own it through the counter
    ledger = StaticLedger({live.key: ('det',)})
    first = store.reconcile(ledger, 'op-1')
    assert first.reclaimed == () and first.unknown == ()
    assert set(first.retained) == {live.key, orphan.key, ttl_only, counted}
    fake.advance(61)
    second = store.reconcile(ledger, 'op-2')
    assert second.reclaimed == (orphan.key,) and second.unknown == ()
    assert set(second.retained) == {live.key, ttl_only, counted}
    assert fake.value(orphan.key) is None and fake.value(metadata_key(orphan.key)) is None
    assert fake.value(ttl_only) == FRAME and fake.value(counted) == FRAME and fake.value(live.key) == FRAME


def test_reconcile_cancels_unrequired_intents_and_honours_the_ledger_for_listed_keys(fake):
    store = _store(fake)
    rejected = store.put(FRAME, 'c', _contract(obligations = ('intent/pub-rejected',)))
    pending = store.put(FRAME, 'c', _contract(obligations = ('intent/pub-unknown',)))
    readers_only = store.put(FRAME, 'c', _contract(obligations = ('det',)))
    listed = store.put(FRAME, 'c', _contract(obligations = ('det', 'track/p0', 'intent/pub-ok')))
    drained = store.put(FRAME, 'c', _contract(obligations = ('det',)))
    ledger = StaticLedger({pending.key: ('intent/pub-unknown',), listed.key: ('det',), drained.key: ()})
    result = store.reconcile(ledger, 'op')
    assert set(result.reclaimed) == {rejected.key, drained.key} and result.unknown == ()
    assert set(result.retained) == {pending.key, readers_only.key, listed.key}
    assert _members(fake, listed.key) == {'det'}
    assert _members(fake, readers_only.key) == {'det'}   # not this ledger's to cancel
    assert _members(fake, pending.key) == {'intent/pub-unknown'}
    assert fake.value(rejected.key) is None and fake.value(drained.key) is None
    assert fake.live_keys(f'*{rejected.key}*') == []


def test_reconcile_reports_unknown_on_scan_and_read_failures(fake):
    store = _store(fake)
    ref = store.put(FRAME, 'c', _contract(obligations = ('det',)))
    fake.fail('SCAN', rexc.ConnectionError('gone'))
    assert store.reconcile(StaticLedger({}), 'op') == ReclamationObservation((), (), ('vf-blob-*',))
    # A key whose records cannot be read is unknown by name — never "nothing to reclaim".
    fake.fail('SMEMBERS', rexc.TimeoutError('slow'), times = rps.TRANSACTION_RETRIES)
    result = store.reconcile(StaticLedger({}), 'op')
    assert result.unknown == (ref.key,) and result.reclaimed == () and result.retained == ()
    assert fake.value(ref.key) == FRAME


def test_reconcile_yields_to_a_concurrent_acquire(fake):
    store = _store(fake, orphan_grace_seconds = 0)
    ref = store.put(FRAME, 'c', _contract(obligations = ()))   # reclaimable the moment it is scanned
    other = _store(fake)
    fake.hook('EXEC', lambda server: other.acquire_obligation(ref, 'dlq/flow', server.now + 300))
    result = store.reconcile(StaticLedger({}), 'op')
    assert result.retained == (ref.key,) and result.reclaimed == ()
    assert fake.value(ref.key) == FRAME and _members(fake, ref.key) == {'dlq/flow'}


# -- inventory / ref_for_key ---------------------------------------------------------------------

def test_inventory_completes_refs_from_metadata_and_is_unknown_when_it_cannot_read(fake):
    store = _store(fake)
    a = store.put(FRAME, 'c1', _contract())
    b = store.put(FRAME, 'c2', _contract(obligations = ()))
    legacy_key = _legacy_store(fake).put(FRAME)
    inventory = store.inventory()
    assert isinstance(inventory, Known)
    by_key = {r.key: r for r in inventory.value}
    assert by_key[a.key] == a and by_key[b.key] == b
    assert by_key[legacy_key].store == 'unknown' and by_key[legacy_key].digest == ''
    assert store.ref_for_key(a.key) == a
    fake.fail('SCAN', rexc.ConnectionError('gone'))
    failed = store.inventory()
    assert isinstance(failed, Unknown) and failed.reason == 'api'
    fake.fail('HGETALL', rexc.TimeoutError('slow'))
    with pytest.raises(TransientError):
        store.ref_for_key(a.key)
    fake.fail('HGETALL', rexc.TimeoutError('slow'))
    assert isinstance(store.inventory(), Unknown)


# -- capabilities ---------------------------------------------------------------------------------

@pytest.mark.parametrize('appendonly, save, policy, durable, evictable', [
    ('no', '', 'volatile-lru', False, True),          # the dev compose server: a cache
    ('no', '', 'noeviction', False, False),            # nothing evicts, nothing survives a restart
    ('yes', '', 'noeviction', True, False),
    ('no', '3600 1 300 100', 'noeviction', True, False),
    ('yes', '', 'allkeys-lru', False, True),           # persistence alone is not durability
])
def test_capabilities_read_persistence_and_eviction_back_live(appendonly, save, policy, durable, evictable):
    fake = FakeRedis(config = {'appendonly': appendonly, 'save': save, 'maxmemory-policy': policy})
    caps = redis_capabilities_observed(fake)
    assert caps.adapter == 'redis' and caps.reader_identities and caps.max_object_bytes == 512 << 20
    assert isinstance(caps.durable, Known) and caps.durable.value is durable
    assert isinstance(caps.evictable, Known) and caps.evictable.value is evictable
    assert isinstance(caps.atomic_multikey, Known) and caps.atomic_multikey.value is True
    assert [e[0] for e in fake.log] == ['CONFIG GET'] * 3 + ['INFO']
    assert RedisPayloadStore(client = fake).capabilities().durable.value is durable


def test_capabilities_are_unknown_when_config_is_denied_or_the_probe_fails():
    fake = FakeRedis(deny_config = True)
    caps = redis_capabilities_observed(fake)
    assert isinstance(caps.durable, Unknown) and caps.durable.reason == 'auth'
    assert isinstance(caps.evictable, Unknown) and caps.evictable.reason == 'auth'
    assert isinstance(caps.atomic_multikey, Known)
    fake = FakeRedis()
    fake.fail('CONFIG GET', rexc.ResponseError("ERR unknown command 'CONFIG'"))   # renamed on managed Redis
    assert redis_capabilities_observed(fake).durable.reason == 'auth'
    fake = FakeRedis()
    fake.fail('CONFIG GET', rexc.ConnectionError('gone'))
    assert redis_capabilities_observed(fake).durable.reason == 'unreachable'
    fake = FakeRedis()
    fake.fail('INFO', rexc.TimeoutError('slow'))
    caps = redis_capabilities_observed(fake)
    assert isinstance(caps.durable, Known)
    assert isinstance(caps.atomic_multikey, Unknown) and caps.atomic_multikey.reason == 'timeout'
    fake = FakeRedis(cluster_enabled = True)
    fake.fail('CLUSTER', rexc.ResponseError('ERR CLUSTER is not allowed here'))
    assert redis_capabilities_observed(fake).atomic_multikey.reason == 'unsupported'


def test_capabilities_on_a_cluster_check_that_the_three_keys_share_a_slot(monkeypatch):
    fake = FakeRedis(cluster_enabled = True)
    caps = redis_capabilities_observed(fake)
    assert isinstance(caps.atomic_multikey, Known) and caps.atomic_multikey.value is True
    assert len(fake.commands('CLUSTER')) == 3
    assert keyslot('foo') == 12182 and keyslot('somekey') == 11058   # the documented CLUSTER KEYSLOT answers
    assert keyslot('vf-blobobl-{vf-blob-x}') == keyslot('vf-blob-x')
    # An untagged layout lands the companions in other slots, and the probe says so.
    monkeypatch.setattr(rps, 'obligation_key', lambda key: 'vf-blobobl-' + key)
    monkeypatch.setattr(rps, 'metadata_key', lambda key: 'vf-blobmeta-' + key)
    caps = redis_capabilities_observed(fake)
    assert isinstance(caps.atomic_multikey, Known) and caps.atomic_multikey.value is False


# -- construction ---------------------------------------------------------------------------------

def test_default_client_is_built_from_the_url_or_the_environment(monkeypatch):
    store = RedisPayloadStore('redis://example.invalid:6390/3')   # redis-py connects lazily: no I/O here
    kwargs = store.client.connection_pool.connection_kwargs
    assert (kwargs['host'], kwargs['port'], kwargs['db']) == ('example.invalid', 6390, 3)
    monkeypatch.setenv('VIDEOFLOW_BLOB_REDIS_URL', 'redis://env.invalid:6391/1')
    kwargs = RedisPayloadStore().client.connection_pool.connection_kwargs
    assert (kwargs['host'], kwargs['port'], kwargs['db']) == ('env.invalid', 6391, 1)


def test_module_imports_without_the_redis_extra():
    code = ("import sys; sys.modules['redis'] = None; "
            "import videoflow.wire.redis_payload_store as m; print(m.RedisPayloadStore.__name__)")
    out = subprocess.run([sys.executable, '-c', code], capture_output = True, text = True, check = True)
    assert out.stdout.strip() == 'RedisPayloadStore'
