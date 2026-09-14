'''
The runtime ledger (RFC 0006 ``CTRL-4``) over every store: the same facts, the
same compare-and-swap discipline, whether the store is a dict, a directory or a
Redis. The Redis variant runs against the fake in ``support_redis`` so the exact
``WATCH`` / ``MULTI`` / ``EXEC`` shape is pinned without a server.
'''
from __future__ import absolute_import, division, print_function

import threading

import pytest
from support_redis import FakeRedis

from videoflow.backends.memory.runtime_store import FileRuntimeStore, MemoryRuntimeStore
from videoflow.backends.outcomes import Accepted, Known, PublicationUnknown, Rejected, known, unknown
from videoflow.backends.runtime import (
    COMPLETION_ABORTED,
    COMPLETION_COMPLETE,
    COMPLETION_DRAINING,
    COMPLETION_OPEN,
    COMPLETION_UNKNOWN,
    LEASE_POLL_SECONDS,
    FlowRuntime,
    claim_replica_slot,
)
from videoflow.core.errors import OwnershipConflict, StaleAuthority
from videoflow.runtime.redis_runtime_store import RedisRuntimeStore
from videoflow.runtime.runtime_stores import make_runtime_store


@pytest.fixture(params = ['memory', 'file', 'redis'])
def store(request, tmp_path):
    if request.param == 'memory':
        return MemoryRuntimeStore()
    if request.param == 'file':
        return FileRuntimeStore(str(tmp_path / 'ledger'))
    return RedisRuntimeStore(client = FakeRedis(config = {'appendonly': 'yes', 'save': '',
                                                          'maxmemory-policy': 'noeviction'}))


class _Clock:
    '''A settable clock: a partition lease lapses when the test advances past it (a crashed holder).'''
    def __init__(self, now = 1000.0):
        self.now = now

    def __call__(self):
        return self.now


def _runtime(store, replica = 0, nb_tasks = 1, parents = None, clock = None):
    clock = clock or _Clock()

    def sleep(seconds):
        clock.now += seconds                                # waiting for a lease advances the fake clock
    return FlowRuntime(store, 'f', 'r', 'child', replica_id = replica, nb_tasks = nb_tasks,
                       parent_replicas = parents or {'parent': 2}, clock = clock, sleep = sleep)


# -- the store contract -------------------------------------------------------------------

def test_cas_versions_and_scan(store):
    assert store.get('k') == (None, None)
    assert store.cas('k', None, b'a')
    value, v1 = store.get('k')
    assert value == b'a' and v1 is not None
    assert not store.cas('k', None, b'b')                    # exists now
    assert not store.cas('k', 'not-the-version', b'b')
    assert store.cas('k', v1, b'b')
    assert store.get('k')[0] == b'b'
    assert store.cas('vf/x/1', None, b'1') and store.cas('vf/x/2', None, b'2') and store.cas('vg/y', None, b'3')
    assert [k for k, _v, _ver in store.scan('vf/x/')] == ['vf/x/1', 'vf/x/2']
    assert store.append('log', b'one') == 1 and store.append('log', b'two') == 2
    assert store.log_entries('log') == [b'one', b'two'] and store.log_entries('nope') == []
    _value, v2 = store.get('k')
    assert not store.delete('k', 'stale') and store.delete('k', v2) and store.get('k') == (None, None)
    assert store.delete('k', None)                          # absent + must-not-exist: true


def test_redis_store_is_one_watched_hash_per_key():
    client = FakeRedis()
    store = RedisRuntimeStore(client = client)
    assert store.cas('a/b', None, b'x')
    names = [c[0] for c in client.log]
    assert names[:4] == ['HGETALL', 'HGETALL', 'HSET', 'HGETALL'] or 'HSET' in names
    assert client.value('vfrt:a/b')[b'version'] == b'1'
    # A concurrent writer slips in around the read: the EXEC aborts, the retry
    # sees the newer version, and the caller's stale expectation is refused.
    client.hook('HGETALL', lambda server: server.hset('vfrt:a/b', mapping = {'value': 'y', 'version': '2'}), times = 1)
    assert not store.cas('a/b', '1', b'z')
    assert store.get('a/b') == (b'y', '2')
    assert store.cas('a/b', '2', b'z') and store.get('a/b') == (b'z', '3')


def test_redis_store_capabilities_are_read_back():
    durable = RedisRuntimeStore(client = FakeRedis(config = {'appendonly': 'yes', 'save': '',
                                                             'maxmemory-policy': 'noeviction'}))
    caps = durable.capabilities()
    assert isinstance(caps.durable, Known) and caps.durable.value and caps.shared_across_processes
    cache = RedisRuntimeStore(client = FakeRedis())
    assert cache.capabilities().durable.value is False
    denied = RedisRuntimeStore(client = FakeRedis(deny_config = True))
    assert not isinstance(denied.capabilities().durable, Known)


def test_make_runtime_store_dispatches_and_names_the_fix(tmp_path):
    assert isinstance(make_runtime_store(None), MemoryRuntimeStore)
    assert isinstance(make_runtime_store('memory://'), MemoryRuntimeStore)
    assert isinstance(make_runtime_store(f'file://{tmp_path}/ledger'), FileRuntimeStore)
    with pytest.raises(ValueError) as e:
        make_runtime_store('s3://bucket')
    assert 'register_runtime_store' in str(e.value) and 'redis' in str(e.value)


# -- the ledger ----------------------------------------------------------------------------

def test_ownership_epochs_fence_a_superseded_owner(store):
    clock = _Clock()
    a = _runtime(store, clock = clock)
    token_a = a.acquire_partition()
    assert token_a.epoch == 1 and a.current_epoch() == 1
    a.check_authority(token_a)
    clock.now += a.lease_seconds + 1                       # A died; its lease lapsed
    b = _runtime(store, clock = clock)
    token_b = b.acquire_partition()                        # a replacement takes over
    assert token_b.epoch == 2
    with pytest.raises(StaleAuthority) as e:
        a.check_authority(token_a)
    assert e.value.code == 'VF_STALE_AUTHORITY' and e.value.disposition == 'worker_fatal'
    with pytest.raises(StaleAuthority):
        a.acquire_partition(expected_epoch = 1)             # A believed it still held epoch 1
    b.check_authority(token_b)


def test_attempts_count_by_disposition_and_never_for_worker_fatal(store):
    rt = _runtime(store)
    assert rt.attempts_for('m1') == 0
    assert rt.record_attempt('m1', 'transient') == 1
    assert rt.record_attempt('m1', 'transient') == 2
    assert rt.record_attempt('m1', 'worker_fatal') == 2   # this worker's fault, not the message's
    assert rt.record_attempt('m1', 'poison') == 3
    assert rt.attempts_for('m1') == 3 and rt.attempts_for('m2') == 0
    # A replacement process reads the same count.
    assert _runtime(store).attempts_for('m1') == 3


def test_terminal_log_and_pending_handoffs_survive_a_restart(store):
    rt = _runtime(store)
    ref = rt.record_terminal({'message_id': 'm1', 'reason': 'sampled-out'})
    assert ref == 'ledger:child/terminal:1'
    assert rt.record_terminal({'message_id': 'm2', 'reason': 'undecodable'}) == 'ledger:child/terminal:2'
    rt.record_pending_handoff('dlq:f:r:child:7', {'VF-Code': 'VF_POISON_SCHEMA'}, b'\x00raw')
    rt.record_pending_handoff('dlq:f:r:child:7', {'VF-Code': 'VF_POISON_SCHEMA'}, b'\x00raw')
    again = _runtime(store)
    assert [e['message_id'] for e in again.terminal_entries()] == ['m1', 'm2']
    handoffs = again.pending_handoffs()
    assert len(handoffs) == 1 and handoffs[0].record_id == 'dlq:f:r:child:7'
    assert handoffs[0].raw == b'\x00raw' and handoffs[0].attempts == 2
    again.clear_pending_handoff('dlq:f:r:child:7')
    assert again.pending_handoffs() == []


def test_completion_barrier_needs_every_replica_every_id_and_a_known_zero(store):
    rt = _runtime(store, parents = {'parent': 2})
    durable = 'child--from--parent'
    assert rt.completion_state('parent', durable, known((0, 0))) == COMPLETION_OPEN
    assert rt.record_terminator('parent', 0, 'eos', 2)
    assert not rt.record_terminator('parent', 0, 'eos', 2)              # a duplicate marker counts once
    assert rt.completion_state('parent', durable, known((0, 0))) == COMPLETION_OPEN   # replica 1 missing
    assert rt.record_terminator('parent', 1, 'eos', 1)
    assert rt.completion_state('parent', durable, known((0, 0))) == COMPLETION_DRAINING  # 0 of 3 ids
    for mid in ('a', 'b', 'c', 'c'):
        rt.record_received('parent', durable, mid)
    assert len(rt.received_ids('parent', durable)) == 3
    assert rt.completion_state('parent', durable, unknown('timeout', 'x')) == COMPLETION_UNKNOWN
    assert rt.completion_state('parent', durable, known((1, 0))) == COMPLETION_DRAINING
    assert rt.completion_state('parent', durable, known((0, 0)), pending_halves = True) == COMPLETION_DRAINING
    assert rt.completion_state('parent', durable, known((0, 0))) == COMPLETION_COMPLETE
    token = rt.acquire_partition()
    receipt = rt.commit_completion('parent', token)
    assert receipt.final_sequence == 3 and rt.completed_parents() == {'parent'}


def test_competing_replicas_aggregate_their_received_sets(store):
    durable = 'child--from--parent'
    r0 = _runtime(store, replica = 0, nb_tasks = 2)
    r1 = _runtime(store, replica = 1, nb_tasks = 2)
    r0.record_received('parent', durable, 'a')
    r1.record_received('parent', durable, 'b')
    r1.record_received('parent', durable, 'a')                          # a redelivery landed elsewhere
    assert r0.received_ids('parent', durable) == {'a', 'b'}
    r0.record_terminator('parent', 0, 'eos', 1)
    r0.record_terminator('parent', 1, 'eos', 1)
    assert r1.completion_state('parent', durable, known((0, 0))) == COMPLETION_COMPLETE


def test_abort_outranks_eos_and_survives_a_restart(store):
    rt = _runtime(store, parents = {'parent': 2})
    durable = 'child--from--parent'
    rt.record_terminator('parent', 0, 'eos', 5)
    rt.record_terminator('parent', 0, 'abort', 3, {'code': 'VF_DEVICE', 'message': 'gone'})
    again = _runtime(store, parents = {'parent': 2})
    assert again.aborted_parents() == {'parent': {'code': 'VF_DEVICE', 'message': 'gone'}}
    assert again.completion_state('parent', durable, known((0, 0))) == COMPLETION_ABORTED
    assert again.completion_state('parent', durable, known((0, 1))) == COMPLETION_DRAINING


def test_a_live_lease_refuses_a_second_holder_and_lapses_or_is_released(store):
    # RUN-018/019: a singleton scaled out by a second process fails at bind time,
    # explicitly, because the holder keeps renewing; a crashed holder's lease
    # lapses and the replacement takes over; a graceful close releases at once.
    clock = _Clock()
    a = _runtime(store, clock = clock)
    token = a.acquire_partition()

    def renewing_sleep(seconds):                           # the holder is alive: it renews while b waits
        clock.now += seconds
        a.renew_partition(token)
    b = FlowRuntime(store, 'f', 'r', 'child', parent_replicas = {'parent': 2}, clock = clock, sleep = renewing_sleep)
    started = clock.now
    with pytest.raises(OwnershipConflict) as e:
        b.acquire_partition()
    assert 'renewed while this replica waited' in str(e.value) and 'nb_tasks' in e.value.remedy
    assert clock.now - started < a.lease_seconds           # refused as soon as the renewal showed
    a.release_partition(token)                             # graceful close: no wait at all
    token_b = b.acquire_partition()
    assert token_b.epoch == 2
    with pytest.raises(StaleAuthority):
        a.renew_partition(token)                           # the old holder cannot renew a superseded epoch
    a.release_partition(token)                             # ...and releases nothing that is not its own
    assert b.current_epoch() == 2
    # b dies (never renews again): c waits for the lease to lapse, then takes over.
    c = _runtime(store, clock = clock)
    started = clock.now
    token_c = c.acquire_partition()
    assert token_c.epoch == 3 and clock.now - started >= b.lease_seconds - LEASE_POLL_SECONDS
    with pytest.raises(StaleAuthority):
        b.check_authority(token_b)


def test_replica_slots_are_claimed_lowest_free_first_and_an_extra_process_is_refused(tmp_path):
    # ENV-5 step 3: processes with no declared identity (Deployment pods) take
    # the lowest free slot; a crashed holder's slot goes to the next claimant
    # once its lease lapses; every slot live means one process too many.
    store = FileRuntimeStore(str(tmp_path / 'ledger'))
    clock = _Clock()
    renewals : list = []

    def sleep(seconds):
        clock.now += seconds
        for rt, token in renewals:                          # the live holders renew while a claimant waits
            rt.renew_partition(token)

    def claim():
        return claim_replica_slot(store, 'f', 'r', 'work', 3, parent_replicas = {'parent': 1},
                                  clock = clock, lease_seconds = 6.0, sleep = sleep)
    a, b, c = claim(), claim(), claim()
    assert (a.replica_id, b.replica_id, c.replica_id) == (0, 1, 2)
    for rt in (a, b, c):
        assert rt.held_partition() is not None and rt.held_partition().partition_id == rt.partition_id()
    renewals[:] = [(rt, rt.held_partition()) for rt in (a, b, c)]
    started = clock.now
    with pytest.raises(OwnershipConflict) as e:
        claim()
    assert 'extra replica' in str(e.value) and 'nb_tasks' in e.value.remedy
    assert clock.now - started < 3 * 6.0                    # each live slot was refused on its first renewal
    # b crashes (stops renewing): the next claimant waits out b's lease and takes slot 1.
    renewals[:] = [(a, a.held_partition()), (c, c.held_partition())]
    started = clock.now
    d = claim()
    assert d.replica_id == 1 and clock.now - started >= 6.0 - LEASE_POLL_SECONDS
    with pytest.raises(StaleAuthority):
        b.renew_partition(b.held_partition())
    # A graceful release frees the slot at once.
    a.release_partition(a.held_partition())
    assert a.held_partition() is None
    started = clock.now
    assert claim().replica_id == 0 and clock.now == started
    # A single-replica node claims slot 0 without any of this being observable.
    assert claim_replica_slot(store, 'f', 'r', 'solo', 1, clock = clock, sleep = sleep).replica_id == 0


def test_stale_epoch_cannot_commit_a_completion(store):
    clock = _Clock()
    old = _runtime(store, clock = clock)
    token_old = old.acquire_partition()
    clock.now += old.lease_seconds + 1
    new = _runtime(store, clock = clock)
    token_new = new.acquire_partition()
    new.record_terminator('parent', 0, 'eos', 0)
    new.record_terminator('parent', 1, 'eos', 0)
    new.commit_completion('parent', token_new)
    with pytest.raises(StaleAuthority):
        old.commit_completion('parent', token_old)


def test_outbox_counts_distinct_accepted_ids_and_keeps_unknown_intents(store):
    rt = _runtime(store)
    rt.intend_publication('p1', 'd1', ('vf-blob-1',))
    rt.resolve_publication('p1', Accepted('p1', 1, False, 'stream'))
    rt.intend_publication('p1', 'd1')                                       # a retry of the same id
    rt.resolve_publication('p1', Accepted('p1', 1, True, 'stream'))         # deduplicated: still one
    rt.intend_publication('p2', 'd2')
    rt.resolve_publication('p2', PublicationUnknown('p2', 'timeout'))
    rt.intend_publication('p3', 'd3')
    rt.resolve_publication('p3', Rejected('p3', 'full', True))
    rt.intend_publication('p4', 'd4')
    assert rt.published_count() == 1
    assert sorted(e.publication_id for e in rt.unresolved_publications()) == ['p2', 'p4']
    assert rt.outbox_entry('p1').payload_refs == ('vf-blob-1',)
    # A definite acceptance is never downgraded by a later unknown.
    rt.resolve_publication('p1', PublicationUnknown('p1', 'late'))
    assert rt.outbox_entry('p1').outcome in ('accepted', 'duplicate')
    again = _runtime(store)
    assert again.published_count() == 1                                    # restored from the outbox
    assert sorted(e.publication_id for e in again.unresolved_publications()) == ['p2', 'p4']


def test_open_groups_are_recorded_by_logical_id_and_settled(store):
    rt = _runtime(store)
    rt.persist_group('t1', {'a': ('pa', 't1', 1)}, {'a': 'tok-a'})
    record = rt.persist_group('t1', {'b': ('pb', 't1', 1)}, {'b': 'tok-b'})
    assert record.members == {'a': 'pa:t1:1', 'b': 'pb:t1:1'}
    assert [g.group_id for g in _runtime(store).open_groups()] == ['t1']
    assert _runtime(store).group_members('t1') == {'a': ('pa', 't1', 1), 'b': ('pb', 't1', 1)}
    rt.settle_group('t1')
    assert rt.open_groups() == []


def test_checkpoint_and_effect_markers(store):
    now = [1000.0]
    rt = _runtime(store, clock = lambda: now[0])
    assert rt.restore_checkpoint() == (None, {})
    rt.checkpoint(b'state-1', {'src': 41})
    rt.checkpoint(b'state-2', {'src': 42})
    assert _runtime(store).restore_checkpoint() == (b'state-2', {'src': 42})
    assert rt.mark_effect('k', 60)
    assert not rt.mark_effect('k', 60) and rt.effect_seen('k')
    now[0] += 61
    assert not rt.effect_seen('k') and rt.mark_effect('k', 60)


def test_concurrent_attempts_never_lose_an_increment():
    store = MemoryRuntimeStore()
    rt = _runtime(store)
    errors = []

    def bump():
        try:
            for _ in range(25):
                rt.record_attempt('m', 'transient')
        except Exception as e:  # noqa: BLE001
            errors.append(e)
    threads = [threading.Thread(target = bump) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors and rt.attempts_for('m') == 100
