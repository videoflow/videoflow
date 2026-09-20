'''
The in-memory messaging backend is the executable spec. Its physics — leases and
redelivery, exhaustion, overflow, per-key retention, dedup, unknown outcomes,
exact-ownership teardown, Core-NATS mode — are the subject of the always-run
memory variants of the MSG cases in tests/conformance, which drive it through
the same oracles as the broker. What is pinned here is the rest: fan-out,
credits, slot sharing, ambiguous acceptance's late resolution, ownership by
metadata, the optional capabilities, and the runtime stores' compare-and-swap.
'''
from __future__ import absolute_import, division, print_function

import os
import subprocess
import sys
import textwrap

import pytest

from videoflow.backends.capabilities import LIVE_LATEST, RELIABLE_WORK, RETENTION_INTEREST, RETENTION_LIMITS
from videoflow.backends.identity import owner_labels
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.messaging import (
    MemoryMessagingBackend,
    make_channel,
    make_envelope,
    make_subscription,
)
from videoflow.backends.memory.runtime_store import FileRuntimeStore, MemoryRuntimeStore
from videoflow.backends.messaging import ChannelId, Completed
from videoflow.backends.outcomes import (
    Accepted,
    PublicationUnknown,
    SettleConfirmed,
)
from videoflow.core.errors import IncompatibleProfile

CID = ChannelId('f', 'r', 'p')

def _reliable(clock = None, **kw):
    backend = MemoryMessagingBackend(clock or FakeClock(), **kw)
    return backend

def _setup(backend, retention = RETENTION_INTEREST, consumers = ('c',), max_msgs = 10_000, **channel_kw):
    subs = [make_subscription(CID, c) for c in consumers]
    backend.ensure_channel(make_channel('f', 'r', 'p', RELIABLE_WORK if retention == RETENTION_INTEREST else LIVE_LATEST,
                                        retention, required = [s.id for s in subs], max_msgs = max_msgs, **channel_kw), 'op')
    for s in subs:
        backend.ensure_subscription(s, 'op')
    return subs

def test_publish_deliver_settle_reclaims_under_interest_retention():
    clock = FakeClock(); backend = _reliable(clock)
    (sub,) = _setup(backend)
    out = backend.publish(make_envelope(CID, 'm1'), clock.now() + 5)
    assert isinstance(out, Accepted) and not out.duplicate and out.sequence == 1
    (d,) = backend.receive(sub.id, 8, 1 << 20, clock.now() + 1)
    assert d.token.attempt == 1
    assert isinstance(backend.settle(d.token, Completed(), 's1'), SettleConfirmed)
    assert backend.stored(CID) == []                       # every required subscription settled
    assert backend.observe_subscription(sub.id).value.available == 0

def test_fan_out_keeps_the_message_until_every_required_subscription_settles():
    clock = FakeClock(); backend = _reliable(clock)
    x, y = _setup(backend, consumers = ('x', 'y'))
    backend.publish(make_envelope(CID, 'm1'), clock.now() + 5)
    (dx,) = backend.receive(x.id, 8, 1 << 20, 0)
    backend.settle(dx.token, Completed(), 's')
    assert len(backend.stored(CID)) == 1                   # y has not settled
    (dy,) = backend.receive(y.id, 8, 1 << 20, 0)
    backend.settle(dy.token, Completed(), 's')
    assert backend.stored(CID) == []

def test_credit_bounds_leased_messages_across_competing_replicas():
    clock = FakeClock(); backend = _reliable(clock)
    sub = make_subscription(CID, 'c', item_credit = 3)
    backend.ensure_channel(make_channel('f', 'r', 'p', RELIABLE_WORK, RETENTION_INTEREST, [sub.id]), 'op')
    backend.ensure_subscription(sub, 'op')
    for i in range(10):
        backend.publish(make_envelope(CID, f'm{i}'), 0)
    a = backend.receive(sub.id, 10, 1 << 20, 0)             # replica 1 takes the whole credit
    b = backend.receive(sub.id, 10, 1 << 20, 0)             # replica 2 gets nothing
    assert (len(a), len(b)) == (3, 0)
    backend.settle(a[0].token, Completed(), 's')
    assert len(backend.receive(sub.id, 10, 1 << 20, 0)) == 1

def test_limits_retention_shares_one_slot_between_data_and_eos_unless_per_subject():
    clock = FakeClock(); backend = _reliable(clock)
    (sub,) = _setup(backend, retention = RETENTION_LIMITS, max_msgs = 1)
    backend.publish(make_envelope(CID, 'd1'), 0)
    backend.publish(make_envelope(CID, 'eos', kind = 'eos'), 0)
    assert [e.kind for e in backend.stored(CID)] == ['eos']          # EOS evicted the data
    assert backend.observe_subscription(sub.id).value.dropped == 1
    backend2 = _reliable(FakeClock())
    (sub2,) = _setup(backend2, retention = RETENTION_LIMITS, max_msgs = 1, per_subject_limits = True)
    backend2.publish(make_envelope(CID, 'd1'), 0)
    backend2.publish(make_envelope(CID, 'eos', kind = 'eos'), 0)
    assert sorted(e.kind for e in backend2.stored(CID)) == ['data', 'eos']

def test_paused_acceptance_yields_unknown_then_late_acceptance_or_definite_cancel():
    clock = FakeClock(); backend = _reliable(clock)
    _setup(backend)
    backend.pause_acceptance(CID)
    out = backend.publish(make_envelope(CID, 'A'), clock.now() + 1)
    assert isinstance(out, PublicationUnknown)
    assert isinstance(backend.observe_publication(make_envelope(CID, 'A')), PublicationUnknown)
    backend.publish(make_envelope(CID, 'B'), clock.now() + 1)
    assert backend.cancel_publication(CID, 'B') is True
    (late,) = backend.resume_acceptance(CID)
    assert isinstance(late, Accepted) and late.publication_id == 'A'
    assert [e.publication_id for e in backend.stored(CID)] == ['A']
    assert backend.cancel_publication(CID, 'A') is False            # already stored: cannot be undone

def test_close_leaves_a_channel_whose_labels_name_another_run():
    # Exact-name teardown and the incomplete-cleanup report are MSG-020's
    # (tests/conformance/test_msg_capabilities.py, memory variant).
    backend = _reliable()
    # Ownership is metadata, not name prefix: a channel whose labels name another run is left alone.
    foreign = make_channel('f', 'r', 'other', RELIABLE_WORK, RETENTION_INTEREST,
                           owner_labels = owner_labels('f', 'someone-else', 'other', 'stream'))
    backend.ensure_channel(foreign, 'op')
    result = backend.close([ChannelId('f', 'r', 'other')], 'g')
    assert not result.complete and result.remaining == ('other',)

def test_durable_control_and_archive_are_optional_capabilities():
    clock = FakeClock(); backend = _reliable(clock)
    _setup(backend)
    with pytest.raises(IncompatibleProfile):
        backend.commit_control(CID, 'run', None, b'{}')
    with pytest.raises(IncompatibleProfile):
        backend.replay(CID, 'x')
    strong = MemoryMessagingBackend(clock, durable_control = True, archive = True, archive_horizon_seconds = 10)
    strong.ensure_channel(make_channel('f', 'r', 'p', RELIABLE_WORK, RETENTION_INTEREST), 'op')
    assert strong.commit_control(CID, 'epoch', None, b'1') is True
    assert strong.commit_control(CID, 'epoch', None, b'2') is False      # CAS: stale version
    value, version = strong.control_state(CID, 'epoch')
    assert (value, version) == (b'1', '1')
    strong.publish(make_envelope(CID, 'ev1'), 0)
    assert strong.replay(CID, 'ev1') is not None
    clock.advance(11)
    assert strong.replay(CID, 'ev1') is None

# -- runtime stores --------------------------------------------------------------------

def test_memory_runtime_store_cas():
    store = MemoryRuntimeStore()
    assert store.cas('k', None, b'a') and not store.cas('k', None, b'b')
    value, version = store.get('k')
    assert (value, version) == (b'a', '1')
    assert store.cas('k', '1', b'b') and store.get('k')[0] == b'b'
    assert store.append('log', b'r1') == 1 and store.scan('k') == [('k', b'b', '2')]
    assert store.delete('k', '3') is False and store.delete('k', '2') is True

def test_file_runtime_store_is_durable_across_processes(tmp_path):
    root = str(tmp_path / 'ledger')
    store = FileRuntimeStore(root)
    assert store.cas('group/1', None, b'{"a":1}')
    child = textwrap.dedent(f'''
        from videoflow.backends.memory.runtime_store import FileRuntimeStore
        s = FileRuntimeStore({root!r})
        value, version = s.get('group/1')
        assert value == b'{{"a":1}}' and version == '1', (value, version)
        assert s.cas('group/1', '1', b'{{"a":2}}')
        assert not s.cas('group/1', '1', b'{{"a":3}}')
        s.append('events', b'child')
    ''')
    proc = subprocess.run([sys.executable, '-c', child], capture_output = True, text = True, env = os.environ)
    assert proc.returncode == 0, proc.stderr
    assert store.get('group/1') == (b'{"a":2}', '2')
    assert store.log_entries('events') == [b'child']
    assert store.capabilities().durable.value is True and store.capabilities().shared_across_processes
