'''
RedisPayloadStore (RFC 0006 BLOB-13/14/15) against a live Redis: real
WATCH/MULTI/EXEC, real expiry, two real clients racing on the last two
obligations of one object, and the capability read-back of the dev compose
server (persistence off, ``volatile-lru``).

Gated like test_blob_reclamation.py: a raw socket probe of VF_TEST_REDIS_URL,
never a client connect at collection time. This bucket's conftest also skips
everything when NATS is down, even though nothing here talks to it.
'''
import os
import socket
import threading
import time
import uuid
from urllib.parse import urlparse

import pytest

from videoflow.backends.memory.payload import StaticLedger
from videoflow.backends.outcomes import Known
from videoflow.backends.payload import Corrupt, ImmutablePayloadRef, Missing, PayloadBytes, RetentionContract

REDIS_URL = os.environ.get('VF_TEST_REDIS_URL', 'redis://localhost:6379/0')

def _redis_available(url = REDIS_URL) -> bool:
    parsed = urlparse(url)
    try:
        with socket.create_connection((parsed.hostname or 'localhost', parsed.port or 6379),
                                      timeout = 1):
            return True
    except OSError:
        return False

pytestmark = pytest.mark.skipif(not _redis_available(),
                                reason = f'Redis not reachable at {REDIS_URL}')

FRAME = bytes(range(256)) * 64
READERS = ('det', 'track/p0', 'track/p1')
INTENT = 'intent/' + uuid.uuid4().hex

def _contract(ttl = 60, horizon = 60, obligations = READERS, durable = True):
    return RetentionContract(ttl, horizon, durable, tuple(obligations))

@pytest.fixture
def redis_module():
    return pytest.importorskip('redis')

@pytest.fixture
def rps(redis_module):
    from videoflow.wire import redis_payload_store
    return redis_payload_store

@pytest.fixture
def store(rps):
    return rps.RedisPayloadStore(REDIS_URL)

@pytest.fixture
def tracked(store, rps):
    '''Keys a test created; blob, metadata, obligation set and counter are unlinked at teardown.'''
    keys = []
    yield keys
    for key in keys:
        store.client.unlink(key, rps.metadata_key(key), rps.obligation_key(key), rps.counter_key(key))

def _members(store, rps, key):
    return {m.decode() for m in store.client.smembers(rps.obligation_key(key))}

def test_put_writes_the_three_keys_with_a_ttl_and_read_verifies(store, rps, tracked):
    ref = store.put(FRAME, 'cam:t1:5', _contract(ttl = 90, obligations = READERS + (INTENT,)))
    tracked.append(ref.key)
    blob, meta, obl = ref.key, rps.metadata_key(ref.key), rps.obligation_key(ref.key)
    assert store.client.get(blob) == FRAME
    record = {k.decode(): v.decode() for k, v in store.client.hgetall(meta).items()}
    assert record['size'] == str(len(FRAME)) and record['digest'] == ref.digest
    assert record['generation'] == ref.generation and record['content_id'] == 'cam:t1:5'
    assert abs(float(record['created_at']) - time.time()) < 5
    assert _members(store, rps, ref.key) == set(READERS) | {INTENT}
    for key in (blob, meta, obl):
        assert 80 < store.client.ttl(key) <= 90
    out = store.read(ref, reader = 'det')
    assert isinstance(out, PayloadBytes) and out.data == FRAME and out.digest_verified
    assert store.ref_for_key(ref.key) == ref
    inventory = store.inventory()
    assert isinstance(inventory, Known) and ref in inventory.value

def test_release_lifecycle_and_idempotency_on_the_real_server(store, rps, tracked):
    ref = store.put(FRAME, 'c', _contract(obligations = (INTENT,) + READERS))
    tracked.append(ref.key)
    assert store.release_obligation(ref, INTENT, 'puback').remaining == 3
    assert store.release_obligation(ref, 'det', 'ack').remaining == 2
    assert store.release_obligation(ref, 'det', 'ack-redelivered').remaining == 2   # idempotent by reader
    assert store.release_obligation(ref, 'track/p1', 'ack').remaining == 1
    assert isinstance(store.read(ref, reader = 'track/p0'), PayloadBytes)
    stale = ImmutablePayloadRef(ref.store, ref.key, ref.size, ref.digest, 'deadbeef', ref.content_id)
    assert store.release_obligation(stale, 'track/p0', 'late').stale
    assert _members(store, rps, ref.key) == {'track/p0'}
    last = store.release_obligation(ref, 'track/p0', 'ack-and-skip')
    assert last.reclaimed and last.remaining == 0 and not last.unknown
    assert store.client.exists(ref.key, rps.metadata_key(ref.key), rps.obligation_key(ref.key)) == 0
    after = store.release_obligation(ref, 'track/p0', 'ack-again')
    assert after.remaining is None and not after.reclaimed and not after.stale
    assert store.client.exists(rps.obligation_key(ref.key)) == 0   # a release never creates a set

def test_a_real_watch_conflict_aborts_the_stale_transaction(store, rps, tracked, redis_module):
    ref = store.put(FRAME, 'conflict', _contract(obligations = ('a', 'b')))
    tracked.append(ref.key)
    obl, meta = rps.obligation_key(ref.key), rps.metadata_key(ref.key)
    other = rps.RedisPayloadStore(REDIS_URL)   # its own connection pool: a second process
    with store.client.pipeline() as pipe:
        pipe.watch(obl, meta)
        assert {m.decode() for m in pipe.smembers(obl)} == {'a', 'b'}
        assert other.release_obligation(ref, 'b', 'ack').remaining == 1   # modifies the watched set
        pipe.multi()
        pipe.srem(obl, 'a')
        with pytest.raises(redis_module.exceptions.WatchError):
            pipe.execute()
    assert _members(store, rps, ref.key) == {'a'}   # the aborted SREM applied nothing
    assert store.release_obligation(ref, 'a', 'ack').reclaimed   # the store's own retry reclaims

def test_two_clients_racing_on_the_last_two_obligations_reclaim_exactly_once(store, rps, tracked):
    other = rps.RedisPayloadStore(REDIS_URL)
    for _ in range(20):
        ref = store.put(FRAME, 'race', _contract(obligations = ('x', 'y')))
        tracked.append(ref.key)
        gate = threading.Barrier(2)
        receipts = {}

        def release(handle, obligation, ref = ref, receipts = receipts, gate = gate):
            gate.wait()
            receipts[obligation] = handle.release_obligation(ref, obligation, 'ack')
        threads = [threading.Thread(target = release, args = (store, 'x')),
                   threading.Thread(target = release, args = (other, 'y'))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sum(1 for r in receipts.values() if r.reclaimed) == 1, receipts
        assert not any(r.unknown or r.stale for r in receipts.values()), receipts
        assert store.client.exists(ref.key, rps.metadata_key(ref.key), rps.obligation_key(ref.key)) == 0

def test_companion_keys_share_the_blob_key_slot(store, rps):
    key = f'vf-blob-{uuid.uuid4().hex}'
    caps = rps.redis_capabilities_observed(store.client)
    assert isinstance(caps.atomic_multikey, Known) and caps.atomic_multikey.value is True
    if int(store.client.info('cluster').get('cluster_enabled', 0)):
        slots = {store.client.cluster('KEYSLOT', k) for k in (key, rps.obligation_key(key), rps.metadata_key(key))}
        assert len(slots) == 1
    else:
        with pytest.raises(store._errors.ResponseError):   # standalone: the probe is not even attempted
            store.client.cluster('KEYSLOT', key)

def test_capabilities_observed_on_the_dev_server(store, rps):
    policy = store.client.config_get('maxmemory-policy')['maxmemory-policy']
    if policy == 'noeviction':
        pytest.skip(f'{REDIS_URL} is not the dev compose cache (maxmemory-policy = {policy})')
    caps = rps.redis_capabilities_observed(store.client)
    assert caps.adapter == 'redis' and caps.reader_identities and caps.max_object_bytes == 512 << 20
    assert isinstance(caps.evictable, Known) and caps.evictable.value is True
    assert isinstance(caps.durable, Known) and caps.durable.value is False
    assert store.capabilities().durable.value is False

def test_counter_only_blob_uses_the_rfc0002_fallback(store, rps, tracked):
    from videoflow.wire.serialization import RedisBlobStore
    legacy = RedisBlobStore(REDIS_URL)
    key = legacy.put_with_readers(FRAME, 2, ttl_seconds = 60)
    tracked.append(key)
    ref = store.ref_for_key(key)
    assert ref.digest == '' and ref.generation == ''
    out = store.read(ref)
    assert isinstance(out, PayloadBytes) and out.data == FRAME and not out.digest_verified
    first = store.release_obligation(ref, 'c1', 'ack')
    assert (first.remaining, first.reclaimed, first.unknown) == (None, False, False)
    assert int(store.client.get(rps.counter_key(key))) == 1
    assert store.client.exists(rps.obligation_key(key)) == 0
    assert store.release_obligation(ref, 'c2', 'ack').reclaimed
    assert store.client.exists(key, rps.counter_key(key)) == 0

def test_read_outcomes_on_the_real_server(store, rps, tracked):
    ref = store.put(FRAME, 'c', _contract())
    tracked.append(ref.key)
    store.client.set(ref.key, b'not the frame', keepttl = True)
    out = store.read(ref)
    assert isinstance(out, Corrupt) and out.expected_digest == ref.digest
    store.client.unlink(ref.key)
    assert isinstance(store.read(ref), Missing)

def test_acquire_pins_a_dead_letter_beyond_the_ordinary_ttl(store, rps, tracked):
    ref = store.put(FRAME, 'c', _contract(ttl = 30, obligations = ('det',)))
    tracked.append(ref.key)
    receipt = store.acquire_obligation(ref, 'dlq/flow', time.time() + 300)
    assert receipt.obligation_id == 'dlq/flow'
    for key in (ref.key, rps.metadata_key(ref.key), rps.obligation_key(ref.key)):
        assert 290 < store.client.ttl(key) <= 300
    store.renew_obligation(ref, 'dlq/flow', time.time() + 5)   # shorter: never shortens
    assert store.client.ttl(ref.key) > 290
    assert store.release_obligation(ref, 'det', 'ack').remaining == 1
    assert isinstance(store.read(ref), PayloadBytes)
    with pytest.raises(LookupError):
        store.acquire_obligation(ImmutablePayloadRef('redis', f'vf-blob-{uuid.uuid4().hex}', 0, '', '', ''),
                                 'dlq/flow', time.time() + 10)

def test_reconcile_on_the_real_server(store, rps, tracked):
    from videoflow.wire.serialization import RedisBlobStore
    live = store.put(FRAME, 'c', _contract(obligations = ('det',)))
    orphan = store.put(FRAME, 'c', _contract(obligations = ('det',)))
    rejected = store.put(FRAME, 'c', _contract(obligations = ('intent/' + uuid.uuid4().hex,)))
    ttl_only = RedisBlobStore(REDIS_URL).put(FRAME, ttl_seconds = 60)
    tracked.extend([live.key, orphan.key, rejected.key, ttl_only])
    store.client.unlink(rps.obligation_key(orphan.key))   # a put that died before its set...
    store.client.hset(rps.metadata_key(orphan.key), 'created_at', repr(time.time() - 120))   # ...two minutes ago
    result = store.reconcile(StaticLedger({live.key: ('det',)}), 'op')
    assert result.unknown == ()
    assert {orphan.key, rejected.key} <= set(result.reclaimed)
    assert {live.key, ttl_only} <= set(result.retained)
    assert store.client.exists(orphan.key, rejected.key) == 0
    assert store.client.get(live.key) == FRAME and store.client.get(ttl_only) == FRAME
