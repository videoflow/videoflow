'''
Unit tests for the backend contract foundations: truthful outcomes, the
observation log, fault barriers, capability profiles and the planner, and the
identity codec. No broker, no cluster.
'''
from __future__ import absolute_import, division, print_function

import json
import os
import subprocess
import sys
import textwrap

import pytest

from videoflow.backends import faults
from videoflow.backends.capabilities import (
    DURABLE_CONTROL,
    LIVE_LATEST,
    RELIABLE_WORK,
    REPLAY_ARCHIVE,
    FlowRequirements,
    MessagingCapabilities,
    PayloadCapabilities,
    ProfileRequest,
    RuntimeCapabilities,
    default_requirements,
    plan_composition,
)
from videoflow.backends.identity import collisions, collisions_across_runs, derived_names, owner_labels, owns
from videoflow.backends.observation import ObservationLog
from videoflow.backends.outcomes import Known, Unknown, is_known, known, unknown, value_or
from videoflow.backends.runtime import group_identity, replayable_trace_id, source_epoch_trace_id
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.compiler import compile_flow
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.errors import IncompatibleProfile, TransientFailure, UnobservableState
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer

# -- outcomes --------------------------------------------------------------------

def test_unknown_is_never_coerced_to_a_value():
    obs = unknown('timeout', 'consumer_info timed out')
    assert not is_known(obs)
    assert value_or(obs, 0) == 0          # display only
    assert isinstance(obs, Unknown) and obs.reason == 'timeout'
    k = known((3, 1), generation = '42')
    assert is_known(k) and k.value == (3, 1) and k.generation == '42'
    assert isinstance(k, Known)

# -- observation log -----------------------------------------------------------------

def test_observation_log_round_trips_through_jsonl(tmp_path):
    path = tmp_path / 'log.jsonl'
    log = ObservationLog(str(path))
    log.emit('publish', op_id = 'p1', attempt = 1, generation = 'g1')
    log.emit('settle', op_id = 'p1', attempt = 1, status = 'confirmed')
    assert [e.kind for e in log.events(op_id = 'p1')] == ['publish', 'settle']
    loaded = ObservationLog.load(str(path))
    assert [e.as_dict()['status'] for e in loaded if e.kind == 'settle'] == ['confirmed']
    with pytest.raises(TypeError, match = 'raw bytes'):
        log.emit('payload', data = b'\x00' * 10)

# -- fault barriers ----------------------------------------------------------------------

def test_barrier_is_free_when_nothing_is_installed():
    assert faults.installed() is None
    assert faults.barrier('settle.before').fired is False

def test_schedule_actions_fire_and_are_counted():
    calls = []
    schedule = faults.FaultSchedule({
        'settle.before': faults.RaiseError(lambda: TransientFailure('injected')),
        'publish.receipt.before': faults.DropResponse(),
        'payload.read.before': faults.Nth(2, faults.Delay(0.0)),
    })
    with schedule:
        with pytest.raises(TransientFailure):
            faults.barrier('settle.before', op_id = 'x')
        assert faults.barrier('publish.receipt.before').drop_response is True
        assert faults.barrier('payload.read.before').fired is False   # first hit: not the nth
        assert faults.barrier('payload.read.before').fired is True    # second hit
        calls.append(faults.barrier('group.commit.after'))           # unscheduled barrier
    assert calls[0].fired is False
    assert schedule.fired() == {'settle.before': 1, 'publish.receipt.before': 1,
                                'payload.read.before': 2, 'group.commit.after': 1}
    assert schedule.unfired() == []
    assert faults.installed() is None

def test_unfired_barriers_are_reported():
    schedule = faults.FaultSchedule({'delete.before': faults.DropResponse()})
    with schedule:
        pass
    assert schedule.unfired() == ['delete.before']

def test_unknown_barrier_names_are_rejected():
    with pytest.raises(faults.UnknownBarrier):
        faults.FaultSchedule({'not.a.barrier': faults.DropResponse()})
    with pytest.raises(faults.UnknownBarrier):
        faults.barrier('not.a.barrier')

def test_pause_blocks_until_released():
    import threading
    schedule = faults.FaultSchedule({'owner.update.before': faults.Pause('race', timeout_seconds = 5)})
    order = []
    def worker():
        with schedule:
            faults.barrier('owner.update.before')
            order.append('resumed')
    t = threading.Thread(target = worker)
    t.start()
    order.append('released')
    schedule.release('race')
    t.join(5)
    assert order == ['released', 'resumed']

def test_schedule_crosses_a_process_boundary_via_env_and_markers(tmp_path):
    schedule = faults.FaultSchedule(
        {'settle.after': faults.Crash(3),
         'publish.send.before': faults.RaiseError.typed('VF_TRANSIENT', 'boom', 'transient')},
        marker_dir = str(tmp_path / 'markers'))
    schedule.install()
    child = textwrap.dedent('''
        from videoflow.backends import faults
        from videoflow.core.errors import TransientFailure
        s = faults.FaultSchedule.from_env(); s.install()
        try:
            faults.barrier('publish.send.before')
        except TransientFailure as e:
            assert e.code == 'VF_TRANSIENT', e.code
        faults.barrier('settle.after')    # crashes with exit code 3
        raise SystemExit(99)              # never reached
    ''')
    env = {**os.environ, **schedule.to_env()}
    proc = subprocess.run([sys.executable, '-c', child], env = env, capture_output = True, text = True)
    schedule.uninstall()
    assert proc.returncode == 3, proc.stderr
    assert schedule.fired() == {'settle.after': 1, 'publish.send.before': 1}
    assert schedule.unfired() == []

def test_in_process_factory_cannot_be_serialised():
    schedule = faults.FaultSchedule({'settle.before': faults.RaiseError(lambda: RuntimeError())},
                                    marker_dir = '/tmp/x')
    with pytest.raises(ValueError, match = 'cannot cross a process boundary'):
        schedule.to_env()

# -- capabilities + planner ------------------------------------------------------------

def _jetstream(**overrides):
    base = dict(adapter = 'jetstream', version = '2.10', retained_backlog = True,
                recoverable_delivery = True, latest_per_key = False, dedup_window_seconds = 120,
                publication_ledger = 'window', replication_factor = known(1),
                persistent_storage = known(False), max_payload_bytes = known(8 << 20),
                credit_resizable = True, control_shares_data_slot = True)
    base.update(overrides)
    return MessagingCapabilities(**base)

def _core_nats():
    return _jetstream(adapter = 'core-nats', retained_backlog = False, recoverable_delivery = False,
                      dedup_window_seconds = None, publication_ledger = 'none')

def _demo_flow(flow_type):
    p = IntProducer(0, 5, name = 'p')
    a = IdentityProcessor(name = 'a')(p)
    c = CommandlineConsumer(name = 'c')(a)
    return Flow([c], flow_type = flow_type, flow_id = 'f')

def test_default_requirements_follow_the_flow_type_and_delivery_overrides():
    batch = default_requirements(BATCH, compile_flow(_demo_flow(BATCH)))
    assert {r.profile for r in batch.profiles} == {RELIABLE_WORK}
    assert {r.channel for r in batch.profiles} == {'p', 'a'}
    realtime = default_requirements(REALTIME, compile_flow(_demo_flow(REALTIME)))
    assert {r.profile for r in realtime.profiles} == {LIVE_LATEST}
    # An at-least-once sink inside a REALTIME flow asks for reliable work on its input.
    specs = compile_flow(_demo_flow(REALTIME))
    sink = next(s for s in specs if s.name == 'c')
    sink.delivery = {'delivery': 'at-least-once'}
    mixed = default_requirements(REALTIME, specs)
    assert (('a', RELIABLE_WORK) in {(r.channel, r.profile) for r in mixed.profiles})

def test_core_nats_rejects_every_reliable_profile_and_keeps_live():
    for profile in (RELIABLE_WORK, DURABLE_CONTROL, REPLAY_ARCHIVE):
        req = FlowRequirements(profiles = (ProfileRequest('p', profile),))
        with pytest.raises(IncompatibleProfile) as info:
            plan_composition(req, _core_nats())
        assert info.value.code == 'VF_INCOMPATIBLE_PROFILE'
        assert 'p' in info.value.context['channels']
        assert info.value.remedy
    plan = plan_composition(FlowRequirements(profiles = (ProfileRequest('p', LIVE_LATEST),)), _core_nats())
    assert plan.channel_profiles == {'p': LIVE_LATEST}
    assert plan.channel_retention == {'p': 'limits'}

def test_planner_lists_every_incompatibility_at_once():
    req = FlowRequirements(profiles = (ProfileRequest('p', RELIABLE_WORK), ProfileRequest('q', REPLAY_ARCHIVE)))
    with pytest.raises(IncompatibleProfile) as info:
        plan_composition(req, _core_nats())
    assert {d.node for d in info.value.diagnostics} == {'p', 'q'}

def test_unknown_store_durability_is_not_a_pass():
    req = FlowRequirements(profiles = (ProfileRequest('p', RELIABLE_WORK),))
    evictable = PayloadCapabilities('redis', durable = unknown('auth', 'CONFIG GET denied'),
                                    evictable = unknown('auth'), atomic_multikey = known(True),
                                    max_object_bytes = None, reader_identities = True)
    with pytest.raises(UnobservableState):
        plan_composition(req, _jetstream(), payload = evictable, payload_refs_in_use = True)
    definitely_evictable = PayloadCapabilities('redis', durable = known(False), evictable = known(True),
                                               atomic_multikey = known(True), max_object_bytes = None,
                                               reader_identities = True)
    with pytest.raises(IncompatibleProfile, match = 'evictable'):
        plan_composition(req, _jetstream(), payload = definitely_evictable, payload_refs_in_use = True)
    # Without payload refs in play the store's durability is irrelevant to the channel.
    plan_composition(req, _jetstream(), payload = definitely_evictable, payload_refs_in_use = False)

def test_mixed_retention_on_one_channel_is_rejected_unless_supported():
    req = FlowRequirements(profiles = (ProfileRequest('p', LIVE_LATEST), ProfileRequest('p', RELIABLE_WORK)))
    with pytest.raises(IncompatibleProfile, match = 'one retention class'):
        plan_composition(req, _jetstream())
    plan = plan_composition(req, _jetstream(mixed_retention_per_channel = True))
    assert plan.notes and 'mixed retention' in plan.notes[0]

def test_restart_safe_needs_a_durable_shared_store():
    req = FlowRequirements(restart_safe = True)
    with pytest.raises(IncompatibleProfile, match = 'runtime store'):
        plan_composition(req, _jetstream())
    memory = RuntimeCapabilities('memory', durable = known(False), shared_across_processes = False,
                                 restart_safe_joins = False, elastic_state = False)
    with pytest.raises(IncompatibleProfile, match = 'not durable'):
        plan_composition(req, _jetstream(), runtime = memory)
    redis = RuntimeCapabilities('redis', durable = known(True), shared_across_processes = True,
                                restart_safe_joins = True, elastic_state = False)
    assert plan_composition(req, _jetstream(), runtime = redis).restart_safe is True

def test_exactly_once_effects_are_never_admitted():
    with pytest.raises(IncompatibleProfile, match = 'exactly-once'):
        plan_composition(FlowRequirements(exactly_once_effects = ('sink',)), _jetstream())

def test_requirements_round_trip_and_empty_is_empty():
    req = FlowRequirements(profiles = (ProfileRequest('p', LIVE_LATEST, {'latest_per_key': True}),),
                           resources = {'a': {'cpu': '2', 'memory': '4Gi'}}, priority_class = 'cluster-batch')
    assert FlowRequirements.from_dict(json.loads(json.dumps(req.to_dict()))) == req
    assert FlowRequirements().is_empty() and not req.is_empty()

# -- identity -------------------------------------------------------------------------

def _flow_with(names):
    p = IntProducer(0, 5, name = names[0])
    nodes = [p]
    last = p
    for n in names[1:-1]:
        last = IdentityProcessor(name = n)(last)
        nodes.append(last)
    c = CommandlineConsumer(name = names[-1])(last)
    return Flow([c], flow_type = REALTIME, flow_id = 'f')

def test_derived_names_enumerate_every_physical_name_and_owner_labels_match_exactly():
    specs = compile_flow(_flow_with(['p', 'a', 'c']))
    names = derived_names(specs, 'f', 'r')
    kinds = {i.kind for ids in names.values() for i in ids}
    assert {'subject', 'stream', 'eos_subject', 'durable', 'dlq_stream', 'k8s_workload', 'control_subject'} <= kinds
    assert 'vf-f-r-a' in names and 'vf.f.r.a._eos' in names
    labels = owner_labels('f', 'r', node = 'a', kind = 'stream', generation = '1')
    assert owns(labels, 'f', 'r') and not owns(labels, 'f', 'r-x') and not owns(None, 'f', 'r')

def _spec(name, parents = ()):
    from videoflow.core.compiler import NodeSpec
    return NodeSpec(name = name, node_class = None, params = {}, parents = list(parents),
                    kind = 'processor', has_children = True, nb_tasks = 1, device_type = 'cpu',
                    is_finite = False)

def test_sanitize_collisions_are_detected_on_compiled_specs():
    # Hand-built specs: a Flow with these names is already rejected by the graph
    # validator (see test_graph_rejects_names_that_collide_after_encoding).
    specs = [_spec('p'), _spec('a.b', ['p']), _spec('a_b', ['p'])]
    found = collisions(specs, 'f', 'r')
    assert found, 'a.b and a_b must collide'
    assert any(c.physical == 'vf-f-r-a_b' for c in found)
    assert not collisions(compile_flow(_flow_with(['p', 'a', 'c'])), 'f', 'r')

def test_k8s_name_case_and_truncation_collisions_are_detected():
    from videoflow.backends.identity import node_name_collisions
    found = node_name_collisions(['p', 'Node', 'node'])
    assert any(c.physical == 'vf-f-node' and c.identities[0].kind == 'kubernetes' for c in found)
    long_a, long_b = 'x' * 70 + 'a', 'x' * 70 + 'b'
    assert any(c.identities[0].kind == 'kubernetes' for c in node_name_collisions([long_a, long_b]))
    assert node_name_collisions(['p', 'a', 'c']) == []

def test_hyphen_joined_tuples_collide_across_runs():
    specs = compile_flow(_flow_with(['p', 'n', 'c']))
    assert collisions_across_runs(specs, 'a', ['b-c']) == []
    # ('a-b','c') vs ('a','b-c'): different flows, same physical stream — reported by kind.
    a = derived_names(specs, 'a-b', 'c'); b = derived_names(specs, 'a', 'b-c')
    assert 'vf-a-b-c-n' in a and 'vf-a-b-c-n' in b
    # The prefix teardown selector of run r covers run r-x's streams: exact ownership is what tells them apart.
    assert collisions_across_runs(specs, 'f', ['r', 'r-x']) == []
    assert derived_names(specs, 'f', 'r-x')['vf-f-r-x-n'][0].parts == ('f', 'r-x', 'n')

# -- runtime identities ---------------------------------------------------------------------

def test_group_identity_depends_on_members_not_only_on_rounded_time():
    a = group_identity({'cam': ('cam', 't1', 5), 'imu': ('imu', 't9', 11)}, 'w1', rounded_micros = 1700000000000000)
    b = group_identity({'cam': ('cam', 't2', 6), 'imu': ('imu', 't9', 11)}, 'w1', rounded_micros = 1700000000000000)
    assert a != b and a.startswith('tw-1700000000000000-') and b.startswith('tw-1700000000000000-')
    replay = group_identity({'imu': ('imu', 't9', 11), 'cam': ('cam', 't1', 5)}, 'w1', rounded_micros = 1700000000000000)
    assert replay == a

def test_source_identities():
    assert source_epoch_trace_id('cam', 'e7', 3) == 'cam:e7:3'
    assert replayable_trace_id('file', 42) == 'file:42'
    assert replayable_trace_id('file', 42, 'v2') == 'file:v2:42'

# -- graph-level collision diagnostic and deterministic order ------------------------------

def test_graph_rejects_names_that_collide_after_encoding():
    from videoflow.core.errors import GraphError
    with pytest.raises(GraphError) as info:
        _flow_with(['p', 'a.b', 'a_b'])
    assert any(d.code == 'VF_GRAPH_NAME_COLLISION' for d in info.value.diagnostics)

def test_topological_order_is_stable_across_constructions():
    import random
    orders = set()
    for seed in range(12):
        rng = random.Random(seed)
        p = IntProducer(0, 5, name = 'p')
        names = ['a', 'b', 'c', 'd', 'e']
        rng.shuffle(names)
        mids = [IdentityProcessor(name = n)(p) for n in names]
        c = CommandlineConsumer(name = 'c-sink')(*mids)
        flow = Flow([c], flow_type = REALTIME, flow_id = 'f')
        orders.add(tuple(s.name for s in compile_flow(flow)))
    assert len(orders) == 1, orders
