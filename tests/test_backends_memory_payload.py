'''
The in-memory payload store's physics: read classification, tiers, forwarding,
inventory and metrics. Its obligation arithmetic — idempotent release, stale
generations, lost responses, reconciliation — is the subject of the always-run
PAY-005/006/007/008/012/013 cases in tests/conformance, which drive it directly.
'''
from __future__ import absolute_import, division, print_function

from videoflow.backends import faults
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.payload import TIER_EVICTABLE, MemoryPayloadStore
from videoflow.backends.outcomes import Known, Unknown
from videoflow.backends.payload import Corrupt, Missing, PayloadBytes, RetentionContract, TransientFailure
from videoflow.core.errors import TransientFailure as TransientError


def _contract(ttl = 3600, horizon = 3600, obligations = ('x', 'y'), durable = True):
    return RetentionContract(ttl, horizon, durable, tuple(obligations))

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

def test_durable_tier_extends_life_under_obligation():
    # The over-budget refusal is test_store_backpressure's (the typed error and all).
    clock = FakeClock(); store = MemoryPayloadStore(clock, max_bytes = 10)
    ref = store.put(b'12345', 'c1', _contract(ttl = 10, horizon = 100, obligations = ('x',)))
    clock.advance(50)                                        # TTL passed, obligation outstanding
    assert isinstance(store.read(ref, reader = 'x'), PayloadBytes)

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
