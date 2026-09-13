'''
The Phase-3 policy additions: the join working set (RUN-005), the partition-key
policy (RUN-020), the ordering policy with its reference interpreter (RUN-021),
the exactly-once admission rule over declared sink guarantees (RUN-017), the
requirement fields riding beside the specs (D8), and the runtime-store URL and
parent-replica rows the control planes emit only under the switch.
'''
from __future__ import absolute_import, division, print_function

import itertools
import random

import pytest

from videoflow.backends.capabilities import (
    EFFECT_IDEMPOTENT_KEY,
    LEDGER_WINDOW,
    RELIABLE_WORK,
    FlowRequirements,
    MessagingCapabilities,
    ProfileRequest,
    RuntimeCapabilities,
    plan_composition,
)
from videoflow.backends.outcomes import known, unknown
from videoflow.core import constants
from videoflow.core.compiler import compile_flow, parent_replicas, sink_guarantees
from videoflow.core.errors import ConfigError, IncompatibleProfile, UnobservableState
from videoflow.core.policies import (
    INVALID_KEY_FALLBACK,
    LATE_MARK,
    ORDER_SEQUENCE,
    JoinPolicy,
    OrderingPolicy,
    PartitionKeyPolicy,
    ReorderBuffer,
)
from videoflow.runtime.worker import parent_replicas_from_env


def _caps():
    return MessagingCapabilities(
        adapter = 'jetstream', version = '2.10', retained_backlog = True, recoverable_delivery = True,
        latest_per_key = False, dedup_window_seconds = 120, publication_ledger = LEDGER_WINDOW,
        replication_factor = known(1), persistent_storage = known(True), max_payload_bytes = known(1 << 20),
        credit_resizable = True, control_shares_data_slot = True)


# -- join working set ---------------------------------------------------------------------

def test_working_set_covers_every_half_of_every_pending_group():
    assert JoinPolicy(max_pending = 4).working_set(2) == 5
    assert JoinPolicy(max_pending = 4).working_set(3) == 9
    assert JoinPolicy(max_pending = 0).working_set(2) == 2


# -- partition keys -----------------------------------------------------------------------

def test_invalid_keys_are_named_not_hashed():
    for bad in (None, '', [], {}, ('a',)):
        assert not PartitionKeyPolicy.is_valid_key(bad), bad
    for good in ('cam-1', 0, 7, True, 2.5):
        assert PartitionKeyPolicy.is_valid_key(good), good
    policy = PartitionKeyPolicy.from_dict({'invalid': INVALID_KEY_FALLBACK, 'fallback_partition': 2})
    assert policy.to_dict() == {'invalid': 'fallback', 'fallback_partition': 2}
    with pytest.raises(ValueError):
        PartitionKeyPolicy(invalid = 'hash-none')


# -- ordering ------------------------------------------------------------------------------

def _reference(policy, arrivals, start = 1):
    '''An independent interpreter of the declared semantics: what the buffer must agree with.'''
    if policy.mode != ORDER_SEQUENCE:
        seen, out = set(), []
        for seq, rec in arrivals:
            if seq not in seen:
                seen.add(seq)
                out.append((seq, rec, False))
        return out
    applied, held, out, expected = set(), {}, [], start
    for seq, rec in arrivals:
        if seq in applied or seq in held:
            continue
        if seq < expected:
            applied.add(seq)
            if policy.late == LATE_MARK:
                out.append((seq, rec, True))
            continue
        held[seq] = rec
        while True:
            while expected in held:
                out.append((expected, held.pop(expected), False))
                applied.add(expected)
                expected += 1
            if held and len(held) > policy.horizon:
                expected = min(held)
                continue
            break
    for seq in sorted(held):
        out.append((seq, held[seq], seq != expected))
        expected = seq + 1
    return out


@pytest.mark.parametrize('late', ['drop', 'mark'])
def test_reorder_buffer_matches_the_reference_for_every_permutation(late):
    policy = OrderingPolicy(mode = ORDER_SEQUENCE, horizon = 2, late = late)
    for perm in itertools.permutations([1, 2, 3, 4]):
        arrivals = [(seq, f'r{seq}') for seq in perm] + [(3, 'r3-dup')]
        buffer = ReorderBuffer(policy)
        got = []
        for seq, rec in arrivals:
            got.extend(buffer.offer(seq, rec))
        got.extend(buffer.flush())
        assert got == _reference(policy, arrivals), (perm, got)


def test_reorder_buffer_random_sequences_agree_with_the_reference():
    rng = random.Random(7)
    for _ in range(200):
        policy = OrderingPolicy(mode = ORDER_SEQUENCE, horizon = rng.randint(0, 4), late = rng.choice(['drop', 'mark']))
        seqs = list(range(1, rng.randint(2, 9)))
        rng.shuffle(seqs)
        arrivals = [(s, s) for s in seqs] + [(rng.choice(seqs), 'dup')]
        buffer = ReorderBuffer(policy)
        got = []
        for seq, rec in arrivals:
            got.extend(buffer.offer(seq, rec))
        got.extend(buffer.flush())
        assert got == _reference(policy, arrivals)
        assert [g[0] for g in got if not g[2]] == sorted(g[0] for g in got if not g[2])


def test_arrival_mode_applies_in_order_of_arrival_but_never_twice():
    buffer = ReorderBuffer(OrderingPolicy())
    assert buffer.offer(3, 'a') == [(3, 'a', False)]
    assert buffer.offer(3, 'a') == []
    assert buffer.offer(1, 'b') == [(1, 'b', False)]


# -- exactly-once admission ----------------------------------------------------------------

def _req(sink = 'sink', **kw):
    return FlowRequirements(profiles = (ProfileRequest('p', RELIABLE_WORK),), exactly_once_effects = (sink,), **kw)


def test_exactly_once_needs_a_declared_external_key_and_markers_past_the_horizon():
    with pytest.raises(IncompatibleProfile) as e:
        plan_composition(_req(), _caps())
    assert 'runtime marker' in str(e.value)
    ok = _req(sink_guarantees = {'sink': EFFECT_IDEMPOTENT_KEY})
    assert plan_composition(ok, _caps())
    short = _req(sink_guarantees = {'sink': EFFECT_IDEMPOTENT_KEY}, effect_retention_seconds = 86400,
                 replay_horizon_seconds = 7 * 86400)
    with pytest.raises(IncompatibleProfile) as e:
        plan_composition(short, _caps())
    assert '86400s' in str(e.value)
    assert plan_composition(_req(sink_guarantees = {'sink': EFFECT_IDEMPOTENT_KEY}, effect_retention_seconds = 8 * 86400,
                                 replay_horizon_seconds = 7 * 86400), _caps())


def test_restart_safe_needs_a_durable_shared_store():
    req = FlowRequirements(profiles = (ProfileRequest('p', RELIABLE_WORK),), restart_safe = True)
    memory = RuntimeCapabilities('memory', durable = known(False), shared_across_processes = False,
                                 restart_safe_joins = False, elastic_state = False)
    with pytest.raises(IncompatibleProfile):
        plan_composition(req, _caps(), runtime = memory)
    unread = RuntimeCapabilities('redis', durable = unknown('auth', 'CONFIG denied'), shared_across_processes = True,
                                 restart_safe_joins = False, elastic_state = False)
    with pytest.raises(UnobservableState):
        plan_composition(req, _caps(), runtime = unread)
    durable = RuntimeCapabilities('redis', durable = known(True), shared_across_processes = True,
                                  restart_safe_joins = True, elastic_state = False)
    assert plan_composition(req, _caps(), runtime = durable).restart_safe


def test_requirement_fields_round_trip_and_stay_absent_by_default():
    assert FlowRequirements().to_dict().keys() == {'profiles', 'restart_safe', 'exactly_once_effects', 'resources',
                                                   'priority_class', 'rollout_policy'}
    full = FlowRequirements(sink_guarantees = {'s': EFFECT_IDEMPOTENT_KEY}, effect_retention_seconds = 10.0,
                            replay_horizon_seconds = 5.0, tolerated_failures = 1)
    assert FlowRequirements.from_dict(full.to_dict()) == full and not full.is_empty()


# -- the rows the control planes emit -------------------------------------------------------

def _flow():
    from videoflow.consumers import CommandlineConsumer
    from videoflow.core import Flow
    from videoflow.processors import IdentityProcessor
    from videoflow.producers import IntProducer
    producer = IntProducer(0, 3, name = 'producer')
    work = IdentityProcessor(nb_tasks = 2, name = 'work')(producer)
    sink = CommandlineConsumer(name = 'sink')(work)
    return Flow([sink])


def test_parent_replicas_follow_each_parents_task_count():
    specs = compile_flow(_flow())
    by_name = {s.name: s for s in specs}
    assert parent_replicas(by_name['sink'], specs) == [2]
    assert parent_replicas(by_name['work'], specs) == [1]
    assert parent_replicas(by_name['producer'], specs) == []


def test_parent_replicas_env_is_positional_and_fails_fast_on_a_mismatch():
    assert parent_replicas_from_env(['a', 'b'], '2,1') == {'a': 2, 'b': 1}
    assert parent_replicas_from_env(['a'], None) == {}
    with pytest.raises(ConfigError):
        parent_replicas_from_env(['a', 'b'], '2')
    with pytest.raises(ConfigError):
        parent_replicas_from_env(['a'], 'two')


def test_sink_guarantees_come_from_the_class_declaration():
    from videoflow.consumers import CommandlineConsumer
    from videoflow.core import Flow
    from videoflow.producers import IntProducer

    class KeyedSink(CommandlineConsumer):
        effect_guarantee = EFFECT_IDEMPOTENT_KEY

    producer = IntProducer(0, 3, name = 'producer')
    plain = CommandlineConsumer(name = 'plain')(producer)
    keyed = KeyedSink(name = 'keyed')(producer)
    assert sink_guarantees(Flow([plain, keyed])) == {'keyed': EFFECT_IDEMPOTENT_KEY}
    # A flow of undeclared sinks only: nothing to emit.
    other = IntProducer(0, 3, name = 'other')
    assert sink_guarantees(Flow([CommandlineConsumer(name = 'plain2')(other)])) == {}


def test_ledger_rows_are_emitted_only_under_the_switch(monkeypatch, tmp_path):
    from videoflow.deploy.manifests import render_manifests
    from videoflow.engines.local import _worker_env
    specs = compile_flow(_flow())
    sink = [s for s in specs if s.name == 'sink'][0]
    off = _worker_env(sink, 'nats://x:4222', 'demo', 'batch', 'run1', None, 0, 3)
    assert 'VF_PARENT_REPLICAS' not in off and 'VF_RUNTIME_STORE_URL' not in off
    on = _worker_env(sink, 'nats://x:4222', 'demo', 'batch', 'run1', None, 0, 3, parent_replicas = [2],
                     runtime_store_url = 'file:///tmp/ledger')
    assert on['VF_PARENT_REPLICAS'] == '2' and on['VF_RUNTIME_STORE_URL'] == 'file:///tmp/ledger'

    def node_cms(manifests):
        return {m['data']['VF_NODE_NAME']: m['data'] for m in manifests
                if m['kind'] == 'ConfigMap' and 'VF_NODE_NAME' in m.get('data', {})}

    def nats_cm(manifests):
        return next(m['data'] for m in manifests if m['kind'] == 'ConfigMap' and 'VF_NATS_URL' in m.get('data', {}))
    plain = render_manifests(specs, 'demo', 'batch', 'nats://x:4222', 'run1', default_image = 'img:1',
                             blob_redis_url = 'redis://r:6379/0')
    assert not any('VF_PARENT_REPLICAS' in d for d in node_cms(plain).values())
    assert 'VF_RUNTIME_STORE_URL' not in nats_cm(plain)
    monkeypatch.setattr(constants, 'RFC0006', True)
    switched = render_manifests(specs, 'demo', 'batch', 'nats://x:4222', 'run1', default_image = 'img:1',
                                blob_redis_url = 'redis://r:6379/0')
    assert node_cms(switched)['sink']['VF_PARENT_REPLICAS'] == '2'
    assert 'VF_PARENT_REPLICAS' not in node_cms(switched)['producer']
    assert nats_cm(switched)['VF_RUNTIME_STORE_URL'] == 'redis://r:6379/0'
