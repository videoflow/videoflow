'''
Conformance cases: RUN-018, RUN-019, RUN-026, RUN-027, RUN-028, RUN-029, RUN-046.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. A ``pending`` marker means the case is a skeleton reporting
NOT_RUN until its implementation phase lands.
'''
from __future__ import absolute_import, division, print_function

import json
import random
import subprocess
from typing import Any, Dict

import defects
import pytest
from _status import not_run

from videoflow.backends.messaging import SubscriptionObservation
from videoflow.backends.outcomes import known, unknown
from videoflow.core.compiler import NODE_KIND_PROCESSOR, NODE_KIND_PRODUCER
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.errors import CapabilityError
from videoflow.runtime import scaling


@pytest.mark.case('RUN-018')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 3')
def test_run_018_singleton_joins_cannot_become_competing_multiworker_joins() -> None:
    '''
    RUN-018 (P0, deployment, kubernetes): Singleton joins cannot become competing multiworker
    joins through autoscaling.

    Acceptance: The split-half schedule either cannot be admitted or completes every group under
    a verified ownership protocol; waiting forever is failure.
    '''


@pytest.mark.case('RUN-019')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 3')
def test_run_019_partition_intent_survives_a_singleton_to_multiple_worker() -> None:
    '''
    RUN-019 (P0, deployment, kubernetes): Partition intent survives a singleton-to-multiple-
    worker scale request.

    Acceptance: One camera history is never split among independent unfenced states; unsupported
    singleton expansion fails explicitly.
    '''


@pytest.mark.case('RUN-026')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 3')
def test_run_026_live_video_autoscaling_observes_overload_that_lossy() -> None:
    '''
    RUN-026 (P1, deployment, kubernetes): Live-video autoscaling observes overload that lossy
    retention conceals.

    Acceptance: The configured objective breach is detected within its declared control window
    despite a shallow queue; no arbitrary FPS target is imposed by this test.
    '''


@pytest.mark.case('RUN-027')
@pytest.mark.level('kubernetes')
def test_run_027_batch_scaling_targets_a_supported_workload_controller(k3s) -> None:
    '''
    RUN-027 (P1, deployment, kubernetes): Batch scaling targets a supported workload controller.

    Acceptance: A supported scaling plan changes actual batch concurrency and completes;
    otherwise admission rejects it before creating a dangling Deployment-targeted scaler.
    '''
    if not _keda_installed():
        not_run('KEDA (scaledobjects.keda.sh) is not installed on the cluster; the operator decides whether to add it')
    pytest.fail('KEDA is present but the in-cluster half of RUN-027 lands with the Phase-5 cluster runs')


@pytest.mark.case('RUN-027')
@pytest.mark.level('model')
@pytest.mark.variant('admission')
def test_run_027_admission_refuses_a_job_rendered_scaler(evidence_dir) -> None:
    '''The render-time admission: a Job-rendered processor cannot be autoscaled, and says so before anything is applied.'''
    evidence : Dict[str, Any] = {}
    _oracle_run_027(evidence)
    (evidence_dir / 'admission.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-027')
def test_run_027_detects_a_job_blind_admission(monkeypatch) -> None:
    defects.job_blind_admission(monkeypatch)
    assert defects.detects(_oracle_run_027, {})


@pytest.mark.case('RUN-028')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 3')
def test_run_028_autoscaling_admission_reconciles_granted_accelerator() -> None:
    '''
    RUN-028 (P1, integration, kubernetes): Autoscaling admission reconciles granted accelerator
    capacity.

    Acceptance: Active concurrency never exceeds valid grants and the no-spare case reports a
    capacity constraint rather than silently claiming successful scale-out.
    '''


@pytest.mark.case('RUN-029')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 3')
def test_run_029_exact_capacity_gpu_rollout_uses_an_explicit_feasible() -> None:
    '''
    RUN-029 (P1, deployment, kubernetes): Exact-capacity GPU rollout uses an explicit feasible
    upgrade policy.

    Acceptance: The image update completes under its admitted availability policy or is rejected
    for insufficient spare capacity; indefinite Pending replacement is failure.
    '''


@pytest.mark.case('RUN-046')
@pytest.mark.level('model')
def test_run_046_multi_input_demand_evaluation_observes_every_required_parent(seed, evidence_dir) -> None:
    '''
    RUN-046 (P1, deployment, model): Multi-input demand evaluation observes every required
    parent.

    Acceptance: Equivalent graphs with permuted parent declarations yield the same pressure
    diagnosis, and missing required input is not misclassified as a GPU capacity solution.
    '''
    evidence : Dict[str, Any] = {}
    _oracle_run_046(seed, evidence)
    (evidence_dir / 'demand_decisions.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-046')
def test_run_046_detects_a_first_parent_only_scaler(monkeypatch, seed) -> None:
    defects.first_parent_demand(monkeypatch)
    assert defects.detects(_oracle_run_046, seed, {})


# -- shared helpers ------------------------------------------------------------------

def _keda_installed() -> bool:
    proc = subprocess.run(['kubectl', 'get', 'crd', 'scaledobjects.keda.sh', '-o', 'name'],
                          capture_output = True, text = True, check = False)
    return proc.returncode == 0 and bool(proc.stdout.strip())


def _observation(available : int, leased : int = 0) -> Any:
    return known(SubscriptionObservation(available = available, leased = leased, unresolved = 0, dropped = 0,
                                         rejected_publications = 0, observed_at = 0.0, generation = None))


def _oracle_run_027(evidence : Dict[str, Any]) -> None:
    # A finite batch processor renders as a Job: the flag cannot mean anything for it.
    try:
        scaling.admit_autoscaling('work', BATCH, NODE_KIND_PROCESSOR, is_partitioned = False, is_job = True)
    except CapabilityError as e:
        assert 'Job' in str(e) and e.remedy and e.context.get('node') == 'work'
        evidence['batch_processor'] = {'rejected': str(e)}
    else:
        raise AssertionError('a Job-rendered processor was admitted for autoscaling')
    reasons = scaling.scaling_rejections(NODE_KIND_PROCESSOR, False, True)
    assert scaling.JOB_REJECTION in reasons
    # A REALTIME processor renders as a Deployment: a scaler is legitimate.
    assert scaling.admit_autoscaling('work', REALTIME, NODE_KIND_PROCESSOR, is_partitioned = False, is_job = False) == []
    # A finite producer in a REALTIME flow is a Job too, but never autoscaled anyway: fixed scale, no scaler, no error.
    producer = scaling.admit_autoscaling('src', REALTIME, NODE_KIND_PRODUCER, is_partitioned = False, is_job = True)
    assert producer and not any('Job' in r and 'processor' in r for r in [])   # rejected by kind, not raised
    evidence['realtime_processor'] = 'scaler rendered'
    evidence['finite_producer'] = producer


def _oracle_run_046(seed : int, evidence : Dict[str, Any]) -> None:
    rng = random.Random(seed)
    a_lag = 5
    decisions = []
    # Second-parent pressure with the first parent held constant.
    for b_lag in (0, 50, 500):
        decision = scaling.observe_demand({'a': _observation(a_lag), 'b': _observation(b_lag)}, required_parents = ['a', 'b'])
        decisions.append({'b_lag': b_lag, 'replicas': decision.replicas, 'diagnosis': decision.diagnosis,
                          'starved': list(decision.starved_parents)})
    assert [d['replicas'] for d in decisions] == [1, 5, 10], decisions           # max over parents, clamped at 10
    assert decisions[0]['diagnosis'] == scaling.DEMAND_STARVED and decisions[0]['starved'] == ['b']
    assert decisions[1]['diagnosis'] == decisions[2]['diagnosis'] == scaling.DEMAND_BACKLOG
    evidence['second_parent_pressure'] = decisions
    # Permuted parent declarations are the same physical workload: identical decisions.
    parents = {'cam': _observation(3), 'imu': _observation(120), 'gps': _observation(0), 'lidar': _observation(40)}
    canonical = scaling.observe_demand(parents, required_parents = list(parents))
    for _ in range(25):
        names = list(parents)
        rng.shuffle(names)
        permuted = scaling.observe_demand({n: parents[n] for n in names}, required_parents = names)
        assert permuted == canonical, (names, permuted, canonical)
    assert canonical.replicas == 10 and canonical.diagnosis == scaling.DEMAND_STARVED
    assert canonical.starved_parents == ('gps',)
    evidence['permutation_invariant'] = {'replicas': canonical.replicas, 'diagnosis': canonical.diagnosis,
                                         'starved': list(canonical.starved_parents)}
    # A withheld required branch is unknown demand, not zero demand and not a capacity problem.
    withheld = scaling.observe_demand({'a': _observation(100)}, required_parents = ['a', 'b'])
    assert withheld.replicas is None and withheld.diagnosis == scaling.DEMAND_UNKNOWN
    assert withheld.unknown_parents == ('b',)
    unobservable = scaling.observe_demand({'a': _observation(100), 'b': unknown('timeout', 'consumer_info timed out')},
                                          required_parents = ['a', 'b'])
    assert unobservable.replicas is None and unobservable.unknown_parents == ('b',)
    # Missing input versus processing shortfall are different diagnoses.
    starved = scaling.observe_demand({'a': _observation(100), 'b': _observation(0)}, required_parents = ['a', 'b'])
    saturated = scaling.observe_demand({'a': _observation(100), 'b': _observation(100)}, required_parents = ['a', 'b'])
    assert (starved.diagnosis, saturated.diagnosis) == (scaling.DEMAND_STARVED, scaling.DEMAND_BACKLOG)
    # An elastic join that cannot rehash its keys stays at a fixed scale however much pressure there is.
    blocked = scaling.scaling_rejections(NODE_KIND_PROCESSOR, is_partitioned = True, is_job = False)
    assert blocked and 'partitioned' in blocked[0]
    evidence.update(withheld = str(withheld), starved = str(starved), saturated = str(saturated), blocked = blocked)
