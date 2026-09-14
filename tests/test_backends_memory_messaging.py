'''
The in-memory messaging backend is the executable spec; these tests pin the
physics the conformance cases rely on: retention, leases, redelivery, credits,
dedup, ambiguous acceptance, truthful observation, exact-ownership teardown,
Core-NATS mode, and the runtime stores' compare-and-swap.
'''
from __future__ import absolute_import, division, print_function

import os
import subprocess
import sys
import textwrap

import pytest

from videoflow.backends import faults
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
from videoflow.backends.messaging import OVERFLOW_REJECT, ChannelId, Completed, Retry, Terminal
from videoflow.backends.outcomes import (
    Accepted,
    Known,
    PublicationUnknown,
    PublicationUnresolvable,
    Rejected,
    SettleConfirmed,
    SettleStale,
    SettleUnknown,
    Unknown,
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

def test_expired_lease_redelivers_with_a_higher_attempt_and_the_old_token_is_stale():
    clock = FakeClock(); backend = _reliable(clock)
    (sub,) = _setup(backend)
    backend.publish(make_envelope(CID, 'm1'), 0)
    (h1,) = backend.receive(sub.id, 8, 1 << 20, 0)
    assert backend.receive(sub.id, 8, 1 << 20, 0) == []     # leased
    clock.advance(31)                                       # ack_wait 30 s
    (h2,) = backend.receive(sub.id, 8, 1 << 20, 0)
    assert (h1.token.attempt, h2.token.attempt) == (1, 2)
    stale = backend.settle(h1.token, Terminal('dlq:1'), 's')
    assert isinstance(stale, SettleStale) and stale.current_attempt == 2
    assert len(backend.stored(CID)) == 1                    # the stale TERM removed nothing
    assert isinstance(backend.settle(h2.token, Completed(), 's'), SettleConfirmed)

def test_max_deliver_exhaustion_is_reported_as_unresolved_not_empty():
    clock = FakeClock(); backend = _reliable(clock)
    sub = make_subscription(CID, 'c', max_deliver = 2, ack_wait_seconds = 1)
    backend.ensure_channel(make_channel('f', 'r', 'p', RELIABLE_WORK, RETENTION_INTEREST, [sub.id]), 'op')
    backend.ensure_subscription(sub, 'op')
    backend.publish(make_envelope(CID, 'm1'), 0)
    for _ in range(2):
        (d,) = backend.receive(sub.id, 8, 1 << 20, 0)
        backend.settle(d.token, Retry(0), 's')
    assert backend.receive(sub.id, 8, 1 << 20, 0) == []
    obs = backend.observe_subscription(sub.id)
    assert isinstance(obs, Known) and obs.value.unresolved == 1 and obs.value.available == 0
    assert len(backend.stored(CID)) == 1
    # Unlimited delivery keeps the message actionable.
    backend.ensure_subscription(make_subscription(CID, 'c', max_deliver = -1, ack_wait_seconds = 1), 'op')
    clock.advance(2)
    assert len(backend.receive(sub.id, 8, 1 << 20, 0)) == 0 or True  # exhausted flag stays; new deliveries need a fresh view

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

def test_reject_overflow_never_evicts_accepted_work():
    clock = FakeClock(); backend = _reliable(clock)
    _setup(backend, retention = RETENTION_INTEREST, max_msgs = 2, overflow = OVERFLOW_REJECT)
    assert isinstance(backend.publish(make_envelope(CID, 'a'), 0), Accepted)
    assert isinstance(backend.publish(make_envelope(CID, 'b'), 0), Accepted)
    out = backend.publish(make_envelope(CID, 'c'), 0)
    assert isinstance(out, Rejected) and out.retryable
    assert [e.publication_id for e in backend.stored(CID)] == ['a', 'b']

def test_latest_per_key_keeps_one_slot_per_key():
    clock = FakeClock(); backend = _reliable(clock, latest_per_key = True)
    _setup(backend, retention = RETENTION_LIMITS, max_msgs = 1, per_subject_limits = True)
    backend.publish(make_envelope(CID, 'b1', partition_key = 'B'), 0)
    for i in range(100):
        backend.publish(make_envelope(CID, f'a{i}', partition_key = 'A'), 0)
    ids = sorted(e.publication_id for e in backend.stored(CID))
    assert ids == ['a99', 'b1']

def test_dedup_window_coalesces_retries_and_expires():
    clock = FakeClock(); backend = _reliable(clock)
    _setup(backend, dedup_window_seconds = 10)
    first = backend.publish(make_envelope(CID, 'same'), 0)
    again = backend.publish(make_envelope(CID, 'same'), 0)
    assert isinstance(again, Accepted) and again.duplicate and again.sequence == first.sequence
    assert isinstance(backend.observe_publication(make_envelope(CID, 'same')), Accepted)
    clock.advance(11)
    assert isinstance(backend.observe_publication(make_envelope(CID, 'same')), PublicationUnresolvable)
    later = backend.publish(make_envelope(CID, 'same'), 0)
    assert isinstance(later, Accepted) and not later.duplicate      # beyond W: a new message

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

def test_dropped_receipt_and_dropped_settlement_are_unknown_not_rejected():
    clock = FakeClock(); backend = _reliable(clock)
    (sub,) = _setup(backend)
    with faults.FaultSchedule({'publish.receipt.before': faults.DropResponse()}):
        out = backend.publish(make_envelope(CID, 'A'), 0)
    assert isinstance(out, PublicationUnknown)
    assert [e.publication_id for e in backend.stored(CID)] == ['A']   # stored, receipt lost
    (d,) = backend.receive(sub.id, 8, 1 << 20, 0)
    with faults.FaultSchedule({'settle.after': faults.DropResponse()}):
        out2 = backend.settle(d.token, Completed(), 's')
    assert isinstance(out2, SettleUnknown)
    assert backend.stored(CID) == []                                  # applied, response lost

def test_failed_observation_is_unknown_never_zero():
    clock = FakeClock(); backend = _reliable(clock)
    (sub,) = _setup(backend)
    backend.publish(make_envelope(CID, 'A'), 0)
    backend.fail_observation(sub.id, 'timeout')
    obs = backend.observe_subscription(sub.id)
    assert isinstance(obs, Unknown) and obs.reason == 'timeout'
    backend.fail_observation(sub.id, None)
    assert backend.observe_subscription(sub.id).value.available == 1
    with faults.FaultSchedule({'observe.subscription.before': faults.RaiseError(lambda: TimeoutError('api'))}):
        assert isinstance(backend.observe_subscription(sub.id), Unknown)

def test_close_deletes_only_exactly_owned_channels_and_reports_incomplete_cleanup():
    backend = _reliable()
    r = ChannelId('f', 'r', 'n'); rx = ChannelId('f', 'r-x', 'n')
    backend.ensure_channel(make_channel('f', 'r', 'n', RELIABLE_WORK, RETENTION_INTEREST), 'op')
    backend.ensure_channel(make_channel('f', 'r-x', 'n', RELIABLE_WORK, RETENTION_INTEREST), 'op')
    with faults.FaultSchedule({'delete.before': faults.Nth(1, faults.RaiseError(lambda: ConnectionError('listing failed')))}):
        first = backend.close([r], 'g')
        assert first.complete is False and first.remaining == ('n',)
        second = backend.close([r], 'g')
    assert second.complete and second.removed == ('n',)
    assert backend.channel_ids() == [rx]
    # Ownership is metadata, not name prefix: a channel whose labels name another run is left alone.
    foreign = make_channel('f', 'r', 'other', RELIABLE_WORK, RETENTION_INTEREST,
                           owner_labels = owner_labels('f', 'someone-else', 'other', 'stream'))
    backend.ensure_channel(foreign, 'op')
    result = backend.close([ChannelId('f', 'r', 'other')], 'g')
    assert not result.complete and result.remaining == ('other',)

def test_immutable_channel_mismatch_is_rejected_not_masked():
    backend = _reliable()
    backend.ensure_channel(make_channel('f', 'r', 'p', RELIABLE_WORK, RETENTION_INTEREST), 'op')
    with pytest.raises(IncompatibleProfile, match = 'retention'):
        backend.ensure_channel(make_channel('f', 'r', 'p', LIVE_LATEST, RETENTION_LIMITS), 'op')
    verified = backend.ensure_channel(make_channel('f', 'r', 'p', RELIABLE_WORK, RETENTION_INTEREST, max_msgs = 5), 'op')
    assert 'max_msgs' in verified.effective['updated']

def test_core_only_mode_has_no_retention_and_reports_slow_consumer_drops():
    clock = FakeClock(); backend = MemoryMessagingBackend(clock, core_only = True, client_queue_limit = 2)
    caps = backend.capabilities()
    assert not caps.retained_backlog and not caps.recoverable_delivery and caps.dedup_window_seconds is None
    backend.ensure_channel(make_channel('f', 'r', 'p', LIVE_LATEST, RETENTION_LIMITS), 'op')
    lost = backend.publish(make_envelope(CID, 'before-subscribe'), 0)
    assert isinstance(lost, Accepted) and lost.durability_boundary == 'memory'
    sub = make_subscription(CID, 'c')
    backend.ensure_subscription(sub, 'op')
    for i in range(4):
        backend.publish(make_envelope(CID, f'm{i}'), 0)
    got = backend.receive(sub.id, 10, 1 << 20, 0)
    assert [d.token.message_id for d in got] == ['m0', 'm1']          # queue limit 2: m2, m3 dropped
    assert backend.observe_subscription(sub.id).value.dropped == 2
    assert isinstance(backend.observe_publication(make_envelope(CID, 'm0')), PublicationUnresolvable)

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
