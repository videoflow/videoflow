'''
Unit tests for ``videoflow.runtime.scaling``: demand is evaluated over every
parent, is invariant to parent order, treats an unobservable parent as unknown
rather than empty, and names the nodes that must not be autoscaled at all —
above all a Job-rendered node, whose parallelism a scaler cannot change.
No broker, no cluster.
'''
from __future__ import absolute_import, division, print_function

import random

import pytest

from videoflow.backends.messaging import SubscriptionObservation
from videoflow.backends.outcomes import known, unknown
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.errors import EXIT_USER, CapabilityError, ConfigError
from videoflow.runtime.scaling import (
    DEMAND_BACKLOG,
    DEMAND_IDLE,
    DEMAND_STARVED,
    DEMAND_UNKNOWN,
    MISSING_OBSERVATION,
    DemandDecision,
    ParentDemand,
    admit_autoscaling,
    job_autoscaling_error,
    observe_demand,
    parent_lag,
    replicas_for_lag,
    scaling_rejections,
)


def _sub(available : int, leased : int = 0, unresolved : int = 0) -> SubscriptionObservation:
    return SubscriptionObservation(available = available, leased = leased, unresolved = unresolved,
                                   dropped = 0, rejected_publications = 0, observed_at = 1.0)

def _lag(available : int, leased : int = 0):
    return known(_sub(available, leased))

# -- the per-parent rule -----------------------------------------------------------

def test_parent_lag_is_what_the_rendered_trigger_measures():
    # available + leased, as KEDA's nats-jetstream scaler sums num_pending + num_ack_pending;
    # unresolved work is not something another replica could take
    assert parent_lag(_sub(available = 7, leased = 3, unresolved = 100)) == 10

def test_replicas_for_lag_is_ceil_over_target_clamped():
    assert replicas_for_lag(0, 10, 1, 10) == 1
    assert replicas_for_lag(10, 10, 1, 10) == 1
    assert replicas_for_lag(11, 10, 1, 10) == 2
    assert replicas_for_lag(95, 10, 1, 10) == 10
    assert replicas_for_lag(10_000, 10, 1, 10) == 10
    assert replicas_for_lag(0, 10, 3, 10) == 3

def test_rule_parameters_are_validated_with_a_remedy():
    for kwargs in ({'target_lag_per_replica': 0}, {'min_replicas': 0}, {'min_replicas': 4, 'max_replicas': 3}):
        with pytest.raises(ConfigError) as info:
            observe_demand({'a': _lag(1)}, **kwargs)
        assert info.value.remedy
    with pytest.raises(ConfigError):
        observe_demand({})

# -- demand over every parent --------------------------------------------------------

def test_demand_is_the_maximum_over_parents():
    decision = observe_demand({'a': _lag(15), 'b': _lag(42), 'c': _lag(3)})
    assert isinstance(decision, DemandDecision) and decision.known
    assert decision.replicas == 5                       # ceil(42 / 10): parent b decides
    assert decision.diagnosis == DEMAND_BACKLOG         # every parent has work: capacity, not a missing input
    assert decision.starved_parents == () and decision.unknown_parents == ()
    assert decision.parents == (
        ParentDemand('a', 15, 2), ParentDemand('b', 42, 5), ParentDemand('c', 3, 1))

def test_idle_parents_hold_the_floor():
    decision = observe_demand({'a': _lag(0), 'b': _lag(0)}, min_replicas = 2)
    assert (decision.replicas, decision.diagnosis) == (2, DEMAND_IDLE)
    assert decision.starved_parents == ()

def test_drained_first_parent_does_not_hide_a_backlogged_second():
    '''
    The defect: one trigger keyed on the first declared parent read "no lag" for
    a join whose other parent was 100 messages behind. Every parent is evaluated
    now, and the decision also says which parent is empty, because for a join
    that requires it those 100 messages are waiting on input, not on capacity.
    '''
    observations = {'a': _lag(0), 'b': _lag(available = 95, leased = 5)}
    decision = observe_demand(observations)
    assert decision.replicas == 10
    assert decision.diagnosis == DEMAND_STARVED
    assert decision.starved_parents == ('a',)
    # the same physical workload declared the other way round
    assert observe_demand({'b': observations['b'], 'a': observations['a']}) == decision
    # what the first-parent-only reading used to conclude
    assert observe_demand({'a': observations['a']}).replicas == 1

def test_decision_is_invariant_to_parent_order():
    names = ['cam-1', 'cam-2', 'audio', 'lidar', 'gps', 'imu']
    lags = {'cam-1': 0, 'cam-2': 250, 'audio': 12, 'lidar': 9, 'gps': 31, 'imu': 0}
    canonical = observe_demand({n: _lag(lags[n]) for n in names}, required_parents = names)
    assert [p.parent for p in canonical.parents] == sorted(names)
    assert canonical.replicas == 10 and canonical.diagnosis == DEMAND_STARVED
    assert canonical.starved_parents == ('cam-1', 'imu')
    for seed in range(40):
        rng = random.Random(seed)
        shuffled = list(names)
        rng.shuffle(shuffled)
        observations = {n: _lag(lags[n]) for n in shuffled}
        required = list(names)
        rng.shuffle(required)
        permuted = observe_demand(observations, required_parents = required)
        assert permuted == canonical
        assert hash(permuted) == hash(canonical)

def test_unknown_parent_makes_the_demand_unknown_not_zero():
    decision = observe_demand({'a': _lag(1000), 'b': unknown('timeout', 'consumer_info timed out')})
    assert not decision.known
    assert decision.replicas is None
    assert decision.diagnosis == DEMAND_UNKNOWN
    assert decision.unknown_parents == ('b',)
    # the observed parent is still reported, so the operator sees the pressure that *is* known
    assert decision.parents == (ParentDemand('a', 1000, 10), ParentDemand('b', None, None, 'timeout'))
    # order-independent on this path too
    for seed in range(10):
        items = [('a', _lag(1000)), ('b', unknown('timeout', 'consumer_info timed out'))]
        random.Random(seed).shuffle(items)
        assert observe_demand(dict(items)) == decision

def test_required_parent_without_an_observation_is_unknown_not_absent():
    decision = observe_demand({'a': _lag(0)}, required_parents = ['b', 'a'])
    assert decision.replicas is None and decision.unknown_parents == ('b',)
    assert decision.parents[1] == ParentDemand('b', None, None, MISSING_OBSERVATION)

# -- eligibility --------------------------------------------------------------------

def test_a_job_rendered_processor_is_refused_not_quietly_skipped():
    reasons = scaling_rejections('processor', is_partitioned = False, is_job = True)
    assert len(reasons) == 1 and 'Job' in reasons[0] and 'parallelism' in reasons[0]
    with pytest.raises(CapabilityError) as info:
        admit_autoscaling('detector', BATCH, 'processor', is_partitioned = False, is_job = True)
    err = info.value
    assert err.code == 'VF_CAPABILITY' and err.exit_code == EXIT_USER
    assert 'Job' in err.message and 'parallelism' in err.message
    assert 'nb_tasks' in err.remedy and 'REALTIME' in err.remedy
    assert err.context == {'node': 'detector', 'flow_type': BATCH}
    assert str(err) == f'{err.message} {err.remedy}'
    # the factory and the admission raise the same words
    assert job_autoscaling_error('detector', BATCH).to_dict() == err.to_dict()

def test_partitioned_nodes_stay_fixed_scale_without_an_error():
    reasons = admit_autoscaling('router', REALTIME, 'processor', is_partitioned = True, is_job = False)
    assert len(reasons) == 1 and 'partition' in reasons[0] and 'fixed scale' in reasons[0]

def test_only_processors_with_parents_are_autoscaled():
    assert admit_autoscaling('p', REALTIME, 'processor', is_partitioned = False, is_job = False) == []
    assert scaling_rejections('producer', is_partitioned = False, is_job = False, has_parents = False)
    assert scaling_rejections('consumer', is_partitioned = False, is_job = False)
    assert scaling_rejections('processor', is_partitioned = False, is_job = False, has_parents = False)
    # a finite producer is a Job even in a REALTIME flow, and never a scaler target either way: no error
    assert admit_autoscaling('src', REALTIME, 'producer', is_partitioned = False, is_job = True,
                             has_parents = False)

def test_gpu_nodes_need_the_explicit_opt_in():
    assert scaling_rejections('processor', False, False, device_type = 'gpu')
    assert scaling_rejections('processor', False, False, device_type = 'gpu', gpu_autoscaling = True) == []

def test_every_rejection_names_its_rule():
    reasons = scaling_rejections('consumer', is_partitioned = True, is_job = True, device_type = 'gpu')
    assert len(reasons) == 4 and all(reason.endswith('.') for reason in reasons)

if __name__ == '__main__':
    pytest.main([__file__])
