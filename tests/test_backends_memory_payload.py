'''The in-memory payload store's physics: obligations, generations, tiers, budgets, digests.'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.backends import faults
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.payload import TIER_EVICTABLE, MemoryPayloadStore, StaticLedger
from videoflow.backends.outcomes import Known, Unknown
from videoflow.backends.payload import Corrupt, Missing, PayloadBytes, RetentionContract, TransientFailure
from videoflow.core.errors import ResourceUnavailable
from videoflow.core.errors import TransientFailure as TransientError


def _contract(ttl = 3600, horizon = 3600, obligations = ('x', 'y'), durable = True):
    return RetentionContract(ttl, horizon, durable, tuple(obligations))

def test_release_is_idempotent_by_reader_and_reclaims_on_the_last_one():
    store = MemoryPayloadStore(FakeClock())
    ref = store.put(b'frame', 'c1', _contract())
    r1 = store.release_obligation(ref, 'x', 'done')
    r2 = store.release_obligation(ref, 'x', 'done')      # duplicate delivery handle, same reader
    assert (r1.remaining, r2.remaining) == (1, 1) and not r2.reclaimed
    assert isinstance(store.read(ref, reader = 'y'), PayloadBytes)
    r3 = store.release_obligation(ref, 'y', 'done')
    assert r3.reclaimed and store.object_count() == 0
    assert isinstance(store.read(ref), Missing)
    assert not store.release_obligation(ref, 'y', 'done').stale       # harmless after cleanup

def test_stale_generation_cannot_delete_a_newer_object():
    store = MemoryPayloadStore(FakeClock(), forward_unchanged = False)
    old = store.put(b'a', 'c1', _contract(obligations = ('x',)))
    store.release_obligation(old, 'x', 'done')                        # reclaimed
    new = store.put(b'a', 'c1', _contract(obligations = ('x',)))
    stale_ref = type(old)(new.store, new.key, new.size, new.digest, old.generation, new.content_id)
    receipt = store.release_obligation(stale_ref, 'x', 'done')
    assert receipt.stale and store.object_count() == 1

def test_lost_release_response_is_reported_unknown_and_safe_to_retry():
    store = MemoryPayloadStore(FakeClock())
    ref = store.put(b'a', 'c1', _contract(obligations = ('x', 'y')))
    with faults.FaultSchedule({'obligation.release.after': faults.DropResponse()}):
        first = store.release_obligation(ref, 'x', 'done')
    assert first.unknown and first.remaining is None
    again = store.release_obligation(ref, 'x', 'done')
    assert again.remaining == 1 and not again.unknown

def test_reads_classify_transient_missing_and_corrupt():
    store = MemoryPayloadStore(FakeClock())
    ref = store.put(b'frame', 'c1', _contract())
    store.fail_reads(['ConnectionError'])
    assert isinstance(store.read(ref), TransientFailure)
    with faults.FaultSchedule({'payload.read.before': faults.RaiseError(lambda: TransientError('blip'))}):
        assert isinstance(store.read(ref), TransientFailure)
    store.corrupt(ref.key)
    out = store.read(ref)
    assert isinstance(out, Corrupt) and out.expected_digest == ref.digest
    store.delete(ref.key)
    assert isinstance(store.read(ref), Missing)

def test_durable_tier_extends_life_under_obligation_and_refuses_over_budget():
    clock = FakeClock(); store = MemoryPayloadStore(clock, max_bytes = 10)
    ref = store.put(b'12345', 'c1', _contract(ttl = 10, horizon = 100, obligations = ('x',)))
    clock.advance(50)                                        # TTL passed, obligation outstanding
    assert isinstance(store.read(ref, reader = 'x'), PayloadBytes)
    with pytest.raises(ResourceUnavailable):
        store.put(b'123456', 'c2', _contract(obligations = ('x',)))   # 5 + 6 > 10: backpressure, not eviction
    assert store.object_count() == 1

def test_evictable_tier_expires_and_evicts_regardless_of_obligations():
    clock = FakeClock(); store = MemoryPayloadStore(clock, tier = TIER_EVICTABLE, max_bytes = 10)
    assert store.capabilities().durable.value is False and store.capabilities().evictable.value is True
    ref = store.put(b'12345', 'c1', _contract(ttl = 10, obligations = ('x',)))
    clock.advance(11)
    assert isinstance(store.read(ref), Missing)              # expired under an obligation
    a = store.put(b'12345', 'c2', _contract(ttl = 100, obligations = ('x',)))
    b = store.put(b'123456', 'c3', _contract(ttl = 100, obligations = ('y',)))   # evicts a (LRU) to fit
    assert isinstance(store.read(a), Missing) and isinstance(store.read(b), PayloadBytes)
    assert store.metrics['evicted'] == 1

def test_unchanged_bytes_forward_as_one_object_with_merged_obligations():
    store = MemoryPayloadStore(FakeClock())
    a = store.put(b'same', 'frame-1', _contract(obligations = ('x',)))
    b = store.put(b'same', 'frame-1', _contract(obligations = ('y',)))
    assert a.key == b.key and store.metrics['put_count'] == 1
    assert set(store.obligations(a.key)) == {'x', 'y'}
    c = store.put(b'changed', 'frame-1', _contract(obligations = ('z',)))
    assert c.key != a.key

def test_reconcile_cancels_unreferenced_obligations_and_reclaims_orphans():
    store = MemoryPayloadStore(FakeClock())
    live = store.put(b'a', 'c1', _contract(obligations = ('x', 'y')))
    orphan = store.put(b'b', 'c2', _contract(obligations = ('intent/p9',)))
    result = store.reconcile(StaticLedger({live.key: ('x',)}), 'op')
    assert result.reclaimed == (orphan.key,) and result.retained == (live.key,)
    assert set(store.obligations(live.key)) == {'x'}

def test_inventory_can_be_unknown():
    store = MemoryPayloadStore(FakeClock())
    assert isinstance(store.inventory(), Known)
    store.fail_inventory('auth')
    inv = store.inventory()
    assert isinstance(inv, Unknown) and inv.reason == 'auth'

def test_metrics_attribute_reads_to_readers():
    store = MemoryPayloadStore(FakeClock())
    ref = store.put(b'x' * 100, 'c1', _contract(obligations = ('owner',)))
    store.read(ref, reader = 'owner')
    assert store.metrics['get_count'] == {'owner': 1} and store.metrics['get_bytes'] == {'owner': 100}
