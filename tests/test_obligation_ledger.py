'''
The runtime's own obligation ledger (videoflow/messaging/obligations.py, RFC 0006
BLOB-14 step 4): what a worker derives from its parents' outboxes and the
broker's retention and ack floors, and what a store may cancel on its word.

Pure/unit over the in-memory backends: the messaging backend supplies retention
facts and floors, the payload store holds the obligation sets, the runtime store
holds the outboxes — no test-built ledger anywhere.
'''
from __future__ import absolute_import, division, print_function

from videoflow.backends.capabilities import LIVE_LATEST, RELIABLE_WORK
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.messaging import MemoryMessagingBackend
from videoflow.backends.memory.payload import MemoryPayloadStore
from videoflow.backends.memory.runtime_store import MemoryRuntimeStore
from videoflow.backends.messaging import SUBSCRIPTION_DATA, ChannelId, Completed, Envelope, SubscriptionId
from videoflow.backends.outcomes import Accepted, Rejected
from videoflow.backends.payload import RetentionContract
from videoflow.backends.runtime import FlowRuntime
from videoflow.core.constants import BATCH, REALTIME
from videoflow.messaging.jetstream_backend import channel_spec_for, subscription_spec_for
from videoflow.messaging.obligations import RuntimeObligationLedger

CONTRACT = RetentionContract(ttl_seconds = 3600, horizon_seconds = 60, durable_required = True)


def _rig(flow_type = BATCH, readers = ('child',)):
    clock = FakeClock()
    backend = MemoryMessagingBackend(clock)
    store = MemoryPayloadStore(clock)
    runtime_store = MemoryRuntimeStore()
    channel = ChannelId('f', 'r', 'parent')
    profile = LIVE_LATEST if flow_type == REALTIME else RELIABLE_WORK
    subs = [SubscriptionId(channel, reader, None, SUBSCRIPTION_DATA) for reader in readers]
    backend.ensure_channel(channel_spec_for('f', 'r', 'parent', flow_type, profile, required = subs, realtime_buffer = 1),
                           'op')
    for sub in subs:
        backend.ensure_subscription(subscription_spec_for(sub, 30, 4, 4), 'op')
    parent = FlowRuntime(runtime_store, 'f', 'r', 'parent')
    return clock, backend, store, runtime_store, channel, subs, parent


def _publish(backend, store, parent, channel, n, readers = ('child',), rejected = False):
    '''The publisher's put + intent + publish + resolve, as the messenger does it.'''
    data = bytes([n]) * 1024
    ref = store.put(data, f'parent:{n}', CONTRACT)
    for reader in readers:
        store.acquire_obligation(ref, reader, 10 ** 9)
    pid = f'pub-{n}'
    store.acquire_obligation(ref, f'intent/{pid}', 10 ** 9)
    parent.intend_publication(pid, 'd', (ref.key,), readers = readers)
    if rejected:
        parent.resolve_publication(pid, Rejected(pid, 'full', True))
        return ref
    outcome = backend.publish(Envelope(channel, pid, {}, b'x', 1, pid, None, None, None, None, 1), 10 ** 9)
    assert isinstance(outcome, Accepted)
    parent.resolve_publication(pid, outcome)
    store.release_obligation(ref, f'intent/{pid}', 'accepted')
    return ref


def test_a_settled_reader_owes_nothing_and_an_unsettled_one_still_does():
    clock, backend, store, runtime_store, channel, (sub,), parent = _rig()
    a = _publish(backend, store, parent, channel, 1)
    b = _publish(backend, store, parent, channel, 2)
    deliveries = backend.receive(sub, 2, 0, clock.monotonic() + 1)
    backend.settle(deliveries[0].token, Completed(), 's1')           # A acked, but the release never happened
    child = FlowRuntime(runtime_store, 'f', 'r', 'child')
    ledger = RuntimeObligationLedger(child, backend, ['parent'])
    required = ledger.required_obligations()
    assert required[a.key] == () and required[b.key] == ('child',)
    observed = store.reconcile(ledger, 'child:start')
    assert a.key in observed.reclaimed and b.key in observed.retained
    assert set(store.obligations(b.key)) == {'child'}


def test_an_evicted_publication_owes_nothing_and_a_refused_intent_is_cancelled():
    clock, backend, store, runtime_store, channel, (sub,), parent = _rig(REALTIME)
    first = _publish(backend, store, parent, channel, 1)
    second = _publish(backend, store, parent, channel, 2)            # a one-slot live channel evicted the first
    refused = _publish(backend, store, parent, channel, 3, rejected = True)
    child = FlowRuntime(runtime_store, 'f', 'r', 'child')
    ledger = RuntimeObligationLedger(child, backend, ['parent'])
    required = ledger.required_obligations()
    assert required[first.key] == () and required[second.key] == ('child',) and required[refused.key] == ()
    observed = store.reconcile(ledger, 'child:periodic')
    assert first.key in observed.reclaimed and refused.key in observed.reclaimed and second.key in observed.retained


def test_the_ledger_never_cancels_a_pin_it_is_not_authoritative_for():
    clock, backend, store, runtime_store, channel, (sub,), parent = _rig()
    a = _publish(backend, store, parent, channel, 1)
    store.acquire_obligation(a, 'archive/f', 10 ** 9)
    deliveries = backend.receive(sub, 1, 0, clock.monotonic() + 1)
    backend.settle(deliveries[0].token, Completed(), 's1')
    child = FlowRuntime(runtime_store, 'f', 'r', 'child')
    ledger = RuntimeObligationLedger(child, backend, ['parent'])
    assert ledger.authoritative('child') and ledger.authoritative('intent/x') and not ledger.authoritative('archive/f')
    observed = store.reconcile(ledger, 'child:start')
    assert a.key in observed.retained and set(store.obligations(a.key)) == {'archive/f'}


def test_an_unobservable_floor_keeps_the_obligation_while_the_message_is_retained():
    clock, backend, store, runtime_store, channel, (sub,), parent = _rig()
    a = _publish(backend, store, parent, channel, 1, readers = ('child', 'other'))
    backend.receive(sub, 1, 0, clock.monotonic() + 1)               # child holds it, unsettled
    # 'other' never bound a subscription: its floor cannot be observed, so it still owes.
    child = FlowRuntime(runtime_store, 'f', 'r', 'child')
    required = RuntimeObligationLedger(child, backend, ['parent']).required_obligations()
    assert required[a.key] == ('child', 'other')
