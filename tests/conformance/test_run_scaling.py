'''
Conformance cases: RUN-018, RUN-019, RUN-026, RUN-027, RUN-028, RUN-029, RUN-046.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. A ``pending`` marker means the case is a skeleton reporting
NOT_RUN until its implementation phase lands.

The kubernetes-level primaries of RUN-018, RUN-019, RUN-026 and RUN-027 need a
cluster with KEDA and stay pending; RUN-028 and RUN-029 run on the shared k3s
cluster's GPU pool (``VF_K8S_GPU_NODES``) with inert holders of the base image, as
``test_alloc_capacity.py`` does.
What can be decided without a cluster is decided here as ``model`` variants:
the admission rules a scaler must obey (``runtime.scaling.scaling_rejections`` and
the renderer's half in ``deploy.manifests``), throughput-based demand
(``observe_rate_demand``) and the desired/granted/ready reconciliation
(``reconcile_capacity``), each with a negative control against the reviewed defect
(``defects_run3.py``).
'''
from __future__ import absolute_import, division, print_function

import json
import random
import subprocess
from typing import Any, Dict, List

import defects
import defects_run3
import pytest
from _msgdrivers import MemoryDriver, StubNode, spec
from _status import not_run

from videoflow.backends.allocation import (
    CLAIM_ALLOCATED,
    CLAIM_READY,
    RELEASE_RELEASED,
    SHARING_EXCLUSIVE,
    FeasiblePlan,
    Infeasible,
    WorkloadRequest,
)
from videoflow.backends.memory.allocation import MemoryAllocationBackend, NodeFixture
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.messaging import ChannelId, SubscriptionObservation
from videoflow.backends.outcomes import Known, known, unknown
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow, constants
from videoflow.core.compiler import NODE_KIND_PROCESSOR, NODE_KIND_PRODUCER, compile_flow
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.errors import CapabilityError, GraphError
from videoflow.core.supervision import SupervisionPolicy
from videoflow.deploy.manifests import render_manifests
from videoflow.messaging.nats_messenger import NATSMessenger
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer
from videoflow.runtime import scaling
from videoflow.runtime.health import HealthState

NATS_URL = 'nats://nats.videoflow-test.svc:4222'
IMAGE = 'videoflow-base:py3.12'


@pytest.mark.case('RUN-018')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 5')
def test_run_018_singleton_joins_cannot_become_competing_multiworker_joins() -> None:
    '''
    RUN-018 (P0, deployment, kubernetes): Singleton joins cannot become competing multiworker
    joins through autoscaling.

    Acceptance: The split-half schedule either cannot be admitted or completes every group under
    a verified ownership protocol; waiting forever is failure.

    Pending phase 5: the in-cluster half (a KEDA scale request against a deployed join,
    the split-half delivery schedule) needs the cluster scaling runs; the admission
    is decided in the ``admission`` variant below.
    '''


@pytest.mark.case('RUN-018')
@pytest.mark.level('model')
@pytest.mark.variant('admission')
def test_run_018_admission_refuses_to_scale_a_singleton_join(evidence_dir, monkeypatch) -> None:
    '''A join at one replica keeps its declared scale: refused by the rule and by the renderer, for trace and time joins; ``nb_tasks`` cannot bypass the graph or messenger checks.'''
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    _oracle_run_018(evidence)
    (evidence_dir / 'join_admission.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-018')
def test_run_018_detects_an_eligibility_read_off_the_replica_count(monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    defects_run3.replica_count_only_eligibility(monkeypatch)
    assert defects.detects(_oracle_run_018, {})


@pytest.mark.case('RUN-019')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 5')
def test_run_019_partition_intent_survives_a_singleton_to_multiple_worker() -> None:
    '''
    RUN-019 (P0, deployment, kubernetes): Partition intent survives a singleton-to-multiple-
    worker scale request.

    Acceptance: One camera history is never split among independent unfenced states; unsupported
    singleton expansion fails explicitly.

    Pending phase 5: the in-cluster half (a KEDA scale request 1→3 against a deployed
    tracker, the per-camera state revisions) needs the cluster scaling runs; the
    admission and the compiled-intent-versus-runtime-membership half is the
    ``admission`` variant below.
    '''


@pytest.mark.case('RUN-019')
@pytest.mark.level('model')
@pytest.mark.variant('admission')
def test_run_019_admission_keeps_partition_intent_at_one_replica(evidence_dir, monkeypatch) -> None:
    '''``partition_by`` at ``nb_tasks = 1`` is kept in the compiled intent and blocks a scaler; the runtime binds a competing durable at one replica and per-replica durables at three.'''
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    _oracle_run_019(evidence)
    (evidence_dir / 'partition_admission.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-019')
def test_run_019_detects_partition_intent_erased_by_the_replica_count(monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    defects_run3.replica_count_only_eligibility(monkeypatch)
    assert defects.detects(_oracle_run_019, {})


@pytest.mark.case('RUN-026')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 5')
def test_run_026_live_video_autoscaling_observes_overload_that_lossy() -> None:
    '''
    RUN-026 (P1, deployment, kubernetes): Live-video autoscaling observes overload that lossy
    retention conceals.

    Acceptance: The configured objective breach is detected within its declared control window
    despite a shallow queue; no arbitrary FPS target is imposed by this test.

    Pending phase 5: driving a deployed live source above one worker's capacity and
    watching a KEDA scaler act on the throughput metric needs the cluster scaling
    runs; the demand observation itself is the ``metrics`` variant below.
    '''


@pytest.mark.case('RUN-026')
@pytest.mark.level('model')
@pytest.mark.variant('metrics')
def test_run_026_throughput_counters_reveal_the_overload_a_shallow_queue_hides(evidence_dir) -> None:
    '''Health counters → ``observe_rate_demand``: the breach is detected in one control window while lag-based demand reads one replica; missing metrics are unknown, never idle.'''
    evidence : Dict[str, Any] = {}
    _oracle_run_026(evidence)
    (evidence_dir / 'rate_decisions.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-026')
def test_run_026_detects_a_lag_only_observer(monkeypatch) -> None:
    defects_run3.lag_only_rate_observer(monkeypatch)
    assert defects.detects(_oracle_run_026, {})


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


def _pod_claims(namespace : str, selector : str) -> Dict[str, Any]:
    '''One ``ClaimObservation`` per pod, from what the API reports — a Pending pod is a pending
    claim, a scheduled one is allocated, a ready one is ready. Never inferred from the spec.'''
    from _k8s import pod_conditions

    from videoflow.backends.allocation import CLAIM_PENDING, ClaimObservation, DeviceIdentity
    claims : Dict[str, Any] = {}
    for c in pod_conditions(namespace, selector):
        grant = (DeviceIdentity(c['node'], None, None, None, 'gpu', None),) if c['node'] else None
        status = CLAIM_READY if c['ready'] else CLAIM_ALLOCATED if c['node'] else CLAIM_PENDING
        claims[c['name']] = known(ClaimObservation(c['name'], 'test', 'g', 'g', status, grant, {'reason': c['message']}))
    return claims


@pytest.mark.case('RUN-028')
@pytest.mark.level('kubernetes')
def test_run_028_autoscaling_admission_reconciles_granted_accelerator(k3s, k3s_gpu_nodes, evidence_dir) -> None:
    '''
    RUN-028 (P1, integration, kubernetes): Autoscaling admission reconciles granted accelerator
    capacity.

    Acceptance: Active concurrency never exceeds valid grants and the no-spare case reports a
    capacity constraint rather than silently claiming successful scale-out.

    On the pool: a Deployment wants more one-GPU replicas than the node has free; the
    reconciliation over the pods' observed claims counts only scheduled pods as granted
    and only ready ones as capacity, names the shortfall, and follows a withdrawn and
    restored allocation (a competing holder taking, then freeing, one device).
    '''
    import time as _time

    from _brokers import unique_ids
    from _k8s import apply, delete_workload, free_gpus_on, gpu_holder_deployment, pod_conditions
    namespace = k3s['VF_K8S_NAMESPACE']
    node, free = max(((n, free_gpus_on(n)) for n in k3s_gpu_nodes), key = lambda pair: pair[1])
    if free < 2:
        not_run(f'RUN-028 needs a pool node with two free GPUs; the best has {free}')
    run = unique_ids('r028')[1][:8]
    name, rival = f'vf-conf-{run}', f'vf-conf-{run}-rival'
    desired = free + 2
    evidence : Dict[str, Any] = {'node': node, 'free': free, 'desired': desired, 'steps': []}

    def settle(expect_ready : int, timeout : float = 180.0) -> Any:
        deadline = _time.monotonic() + timeout
        while True:
            decision = scaling.reconcile_capacity(desired, _pod_claims(namespace, f'app={name}'))
            if decision.ready == expect_ready and decision.granted == expect_ready and decision.diagnosis != scaling.CAPACITY_UNKNOWN:
                return decision
            if _time.monotonic() > deadline:
                return decision
            _time.sleep(3)
    try:
        apply(gpu_holder_deployment(name, namespace, [node], 1, replicas = desired))
        decision = settle(free)
        evidence['steps'].append({'step': 'scaled-out', 'decision': decision, 'pods': pod_conditions(namespace, f'app={name}')})
        assert (decision.desired, decision.granted, decision.ready) == (desired, free, free), decision
        assert decision.diagnosis == scaling.CAPACITY_CONSTRAINED and '2 cannot be scheduled' in decision.reason
        # Withdraw one allocation: a rival takes the device one of ours gives up (our
        # holder is scaled down by one so the freed device is the rival's, not a
        # replacement's — the demand still wants ``desired``).
        apply(gpu_holder_deployment(rival, namespace, [node], 1, replicas = 1))
        # Below the running count: the controller removes the Pending replicas first, then one
        # that holds a device — which the rival, already waiting, takes.
        apply(gpu_holder_deployment(name, namespace, [node], 1, replicas = free - 1))
        from _k8s import wait_ready
        wait_ready(namespace, f'app={rival}', 1, timeout = 180)
        decision = settle(free - 1)
        evidence['steps'].append({'step': 'withdrawn', 'decision': decision, 'pods': pod_conditions(namespace, f'app={name}'),
                                  'rival': pod_conditions(namespace, f'app={rival}')})
        assert decision.ready == free - 1 and decision.diagnosis == scaling.CAPACITY_CONSTRAINED, decision
        assert decision.granted <= free - 1                      # never more than the valid grants
        # Restore it: the rival leaves, the scale request is admitted and the replica becomes ready.
        delete_workload(namespace, rival)
        apply(gpu_holder_deployment(name, namespace, [node], 1, replicas = desired))
        decision = settle(free)
        evidence['steps'].append({'step': 'restored', 'decision': decision, 'pods': pod_conditions(namespace, f'app={name}')})
        assert (decision.granted, decision.ready) == (free, free), decision
    finally:
        delete_workload(namespace, rival)
        delete_workload(namespace, name)
        (evidence_dir / 'capacity_reconciliation.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('RUN-028')
@pytest.mark.level('model')
@pytest.mark.variant('grants')
def test_run_028_desired_granted_and_ready_are_kept_apart(evidence_dir, record_faults) -> None:
    '''``reconcile_capacity`` over the memory allocator: a claim counts only once granted, is capacity only once ready, and a shortfall is named.'''
    evidence : Dict[str, Any] = {}
    _oracle_run_028(evidence, record_faults)
    (evidence_dir / 'capacity_decisions.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-028')
def test_run_028_detects_requests_counted_as_capacity(monkeypatch) -> None:
    defects_run3.requests_count_as_capacity(monkeypatch)
    assert defects.detects(_oracle_run_028, {})


def _oracle_run_029_model(evidence : Dict[str, Any]) -> None:
    '''The declared policy renders a feasible Deployment strategy; surge is refused without reserved
    capacity; nothing inherits the zero-unavailable-plus-surge default silently on a full pool.'''
    from videoflow.core.constants import GPU
    from videoflow.deploy.admission import rollout_problems
    from videoflow.deploy.manifests import rollout_strategy
    p = IntProducer(0, 5, name = 'producer')
    a = IdentityProcessor(name = 'infer', device_type = GPU)(p)
    specs = compile_flow(Flow([CommandlineConsumer(name = 'sink')(a)], flow_type = REALTIME, flow_id = 'r029'))
    rows = {}
    for replicas in (1, 2, 3):
        specs_n = [spec_ if spec_.name != 'infer' else __import__('dataclasses').replace(spec_, nb_tasks = replicas)
                   for spec_ in specs]
        drain = next(m for m in render_manifests(specs_n, 'r029', 'realtime', 'nats://x:4222', 'r', default_image = 'img:1',
                                                 rollout_policy = 'drain') if m['kind'] == 'Deployment'
                     and m['metadata']['name'] == 'vf-r029-infer')
        assert drain['spec']['strategy'] == {'type': 'Recreate'} and drain['spec']['replicas'] == replicas
        surge_refused = rollout_problems('surge', specs_n, REALTIME, known(0))
        assert surge_refused and 'spare' in surge_refused[0]
        assert rollout_problems('drain', specs_n, REALTIME, known(0)) == []
        default_flagged = rollout_problems(None, specs_n, REALTIME, known(0))
        assert default_flagged and 'default rolling update' in default_flagged[0]
        rows[replicas] = {'drain': drain['spec']['strategy'], 'surge_refused': surge_refused[0]}
    assert rollout_strategy('surge')['rollingUpdate'] == {'maxSurge': 1, 'maxUnavailable': 0}
    assert rollout_problems('surge', specs, REALTIME, known(1)) == []       # reserved capacity: admitted
    evidence['rows'] = rows


@pytest.mark.case('RUN-029')
@pytest.mark.level('kubernetes')
def test_run_029_exact_capacity_gpu_rollout_uses_an_explicit_feasible(k3s, k3s_gpu_nodes, evidence_dir) -> None:
    '''
    RUN-029 (P1, deployment, kubernetes): Exact-capacity GPU rollout uses an explicit feasible
    upgrade policy.

    Acceptance: The image update completes under its admitted availability policy or is rejected
    for insufficient spare capacity; indefinite Pending replacement is failure.

    On the pool: holders take every free GPU of a node (one, then two replicas where the
    node has them); a spec change under ``drain`` completes with the old pods gone before
    the replacements were scheduled; ``surge`` is refused by admission; the API default is
    shown to leave the replacement Pending within a bounded wait.
    '''
    from _brokers import unique_ids
    from _k8s import (
        apply,
        delete_workload,
        free_gpus_on,
        gpu_holder_deployment,
        kubectl_json,
        pod_conditions,
        rollout_status,
        wait_ready,
    )

    from videoflow.core.constants import GPU
    from videoflow.deploy.admission import free_gpu_devices_observed, rollout_problems
    from videoflow.deploy.manifests import rollout_strategy
    namespace = k3s['VF_K8S_NAMESPACE']
    node, free = max(((n, free_gpus_on(n)) for n in k3s_gpu_nodes), key = lambda pair: pair[1])
    if free < 1:
        not_run('RUN-029 needs a pool node with a free GPU; none has one')
    evidence : Dict[str, Any] = {'node': node, 'free': free, 'rounds': []}
    p = IntProducer(0, 5, name = 'producer')
    a = IdentityProcessor(name = 'infer', device_type = GPU, gpu_count = 1)(p)
    specs = compile_flow(Flow([CommandlineConsumer(name = 'sink')(a)], flow_type = REALTIME, flow_id = 'r029'))
    # Replica counts that take every free device (``replicas × per_pod == free``): the
    # case is about a fully allocated pool, and a leftover spare would admit surge.
    for replicas in [r for r in (1, 2, 3) if free % r == 0]:
        name = 'vf-conf-' + unique_ids('r029')[1][:8]
        per_pod = free // replicas
        round_evidence : Dict[str, Any] = {'replicas': replicas, 'gpus_per_pod': per_pod}
        try:
            apply(gpu_holder_deployment(name, namespace, [node], per_pod, replicas = replicas, generation = 'a'))
            wait_ready(namespace, f'app={name}', replicas, timeout = 240)
            old = {c['name'] for c in pod_conditions(namespace, f'app={name}')}
            old_uids = {p['metadata']['uid'] for p in kubectl_json('get', 'pods', '-n', namespace, '-l', f'app={name}').get('items', [])}
            # Admission over the pool as it is: no reserved capacity, surge is refused.
            observed = free_gpu_devices_observed()
            refused = rollout_problems('surge', specs, REALTIME, observed)
            round_evidence['surge_refused'] = refused
            round_evidence['free_observed'] = str(observed)
            assert refused and 'spare' in refused[0], (refused, str(observed), [(s.name, s.device_type) for s in specs])
            # Drain: the replacement is scheduled only after the old grant is gone.
            apply(gpu_holder_deployment(name, namespace, [node], per_pod, replicas = replicas,
                                        strategy = rollout_strategy('drain'), generation = 'b'))
            completed, output = rollout_status(namespace, name, timeout = 240)
            round_evidence['drain'] = {'completed': completed, 'output': output}
            assert completed, output
            # The old grants were released (their pods deleted) before any replacement was
            # scheduled: Recreate stops the old generation first. A pod still winding down
            # carries a deletionTimestamp; it is not a replacement.
            listing = kubectl_json('get', 'pods', '-n', namespace, '-l', f'app={name}').get('items', [])
            new_pods = [p for p in listing if p['metadata']['uid'] not in old_uids and not p['metadata'].get('deletionTimestamp')]
            leaving = [p for p in listing if p['metadata']['uid'] in old_uids]
            assert len(new_pods) == replicas, [p['metadata']['name'] for p in listing]
            assert all(p['metadata'].get('deletionTimestamp') for p in leaving), [p['metadata']['name'] for p in leaving]
            released_at = max((p['metadata']['deletionTimestamp'] for p in leaving), default = '')
            for pod in new_pods:
                scheduled = next((c for c in pod['status'].get('conditions', []) if c['type'] == 'PodScheduled'), {})
                round_evidence.setdefault('replacements', []).append(
                    {'pod': pod['metadata']['name'], 'scheduled_at': scheduled.get('lastTransitionTime'),
                     'old_released_at': released_at})
                assert not released_at or scheduled.get('lastTransitionTime', '') >= released_at, round_evidence
        finally:
            delete_workload(namespace, name)
            evidence['rounds'].append(round_evidence)
    (evidence_dir / 'rollout_policy.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('RUN-029')
@pytest.mark.level('model')
@pytest.mark.variant('policy-render')
def test_run_029_policies_render_feasible_strategies_and_refuse_blind_surge(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_run_029_model(evidence)
    (evidence_dir / 'rollout_policy_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-029')
def test_run_029_detects_the_inherited_default_strategy(monkeypatch) -> None:
    import defects_alloc
    defects_alloc.default_rolling_update(monkeypatch)
    assert defects.detects(_oracle_run_029_model, {})


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


def _render(flow : Flow, autoscaling : bool = True) -> List[dict]:
    specs = sorted(compile_flow(flow), key = lambda s: s.name)
    return render_manifests(specs, flow.flow_id, flow.flow_type, NATS_URL, 'run0', namespace = 'videoflow-test',
                            default_image = IMAGE, autoscaling = autoscaling, supervision = SupervisionPolicy())


def _scaled(manifests : List[dict]) -> Dict[str, dict]:
    return {m['spec']['scaleTargetRef']['name']: m for m in manifests if m['kind'] == 'ScaledObject'}


def _workloads(manifests : List[dict]) -> Dict[str, dict]:
    return {m['metadata']['name']: m for m in manifests if m['kind'] in ('Deployment', 'StatefulSet', 'Job')}


def _join_flow(join_policy : Any = None, nb_tasks : int = 1, partition_by : Any = None) -> Flow:
    a = IntProducer(0, 40, 0.1, name = 'cam-a')
    b = IntProducer(0, 40, 0.1, name = 'cam-b')
    joined = IdentityProcessor(name = 'joined', nb_tasks = nb_tasks, partition_by = partition_by,
                               join_policy = join_policy)(a, b)
    worker = IdentityProcessor(name = 'worker')(joined)
    return Flow([CommandlineConsumer(name = 'sink')(worker)], flow_type = REALTIME, flow_id = 'run018')


def _oracle_run_018(evidence : Dict[str, Any]) -> None:
    # The rule: a join at one replica is refused a scaler whatever its policy —
    # the runtime store advertises no elastic join state that would let group
    # ownership follow the replica count.
    reasons = scaling.scaling_rejections(NODE_KIND_PROCESSOR, False, False, is_join = True)
    assert scaling.JOIN_REJECTION in reasons, reasons
    admitted = scaling.admit_autoscaling('joined', REALTIME, NODE_KIND_PROCESSOR, False, False, is_join = True)
    assert admitted and any('competing multi-worker joins' in r for r in admitted), admitted
    assert scaling.admit_autoscaling('worker', REALTIME, NODE_KIND_PROCESSOR, False, False, is_join = False) == []
    evidence['rule'] = {'join': admitted, 'stateless': []}
    # The renderer's half, for a trace join and a time join: no ScaledObject
    # for the join, one for the stateless processor downstream of it.
    time_policy = {'mode': 'time', 'tolerance_ms': 50}
    for name, policy in (('trace', None), ('time', time_policy)):
        manifests = _render(_join_flow(policy))
        scaled = _scaled(manifests)
        workloads = _workloads(manifests)
        assert 'vf-run018-worker' in scaled, sorted(scaled)
        assert 'vf-run018-joined' not in scaled, f'{name} join rendered a scaler: {sorted(scaled)}'
        assert workloads['vf-run018-joined']['spec']['replicas'] == 1
        evidence[f'{name}_join'] = {'scaled': sorted(scaled), 'joined_replicas': workloads['vf-run018-joined']['spec']['replicas']}
    # A static nb_tasks cannot bypass the eligibility checks: an unpartitioned
    # replicated join is refused by the graph, a replicated time join by the
    # messenger before it binds anything.
    with pytest.raises(GraphError) as graph_error:
        _join_flow(nb_tasks = 2)
    assert 'VF_GRAPH_UNPARTITIONED_JOIN' in str(graph_error.value.render()) or 'partition_by' in str(graph_error.value)
    driver = MemoryDriver('f', 'r')
    try:
        with pytest.raises(ValueError) as messenger_error:
            NATSMessenger(StubNode('joined'), ['cam-a', 'cam-b'], 'memory://model', 'f', REALTIME, 'r', nb_tasks = 2,
                          partition_by = 'trace_id', join_policy = time_policy, backend = driver.backend)
    finally:
        driver.close()
    assert 'nb_tasks == 1' in str(messenger_error.value)
    evidence['static_nb_tasks'] = {'graph': str(graph_error.value), 'messenger': str(messenger_error.value)}


def _tracker_flow(nb_tasks : int) -> Flow:
    cam = IntProducer(0, 40, 0.1, name = 'cam')
    tracker = IdentityProcessor(name = 'tracker', nb_tasks = nb_tasks, partition_by = 'camera_id')(cam)
    stateless = IdentityProcessor(name = 'stateless')(cam)
    return Flow([CommandlineConsumer(name = 'sink')(tracker, stateless)], flow_type = REALTIME, flow_id = 'run019')


def _oracle_run_019(evidence : Dict[str, Any]) -> None:
    # Compiled intent: partition_by survives nb_tasks = 1.
    specs = {s.name: s for s in compile_flow(_tracker_flow(1))}
    assert specs['tracker'].partition_by == 'camera_id' and specs['tracker'].nb_tasks == 1
    # The rule: declared partition intent at one replica blocks a scaler, distinctly
    # from the nb_tasks > 1 (partitioned) rule.
    reasons = scaling.scaling_rejections(NODE_KIND_PROCESSOR, False, False, declares_partition = True)
    assert scaling.PARTITION_INTENT_REJECTION in reasons, reasons
    assert scaling.admit_autoscaling('tracker', REALTIME, NODE_KIND_PROCESSOR, False, False, declares_partition = True)
    assert scaling.admit_autoscaling('stateless', REALTIME, NODE_KIND_PROCESSOR, False, False) == []
    evidence['rule'] = reasons
    # The renderer: no ScaledObject for the tracker at one replica; the stateless sibling gets one.
    manifests = _render(_tracker_flow(1))
    scaled = _scaled(manifests)
    assert 'vf-run019-stateless' in scaled and 'vf-run019-tracker' not in scaled, sorted(scaled)
    evidence['one_replica'] = {'scaled': sorted(scaled), 'tracker_kind': _workloads(manifests)['vf-run019-tracker']['kind']}
    # The supported expansion is a redeploy at the owning replica count: three
    # fenced owners as a StatefulSet with per-replica durables, still no scaler.
    manifests = _render(_tracker_flow(3))
    scaled = _scaled(manifests)
    tracker = _workloads(manifests)['vf-run019-tracker']
    assert tracker['kind'] == 'StatefulSet' and tracker['spec']['replicas'] == 3, tracker['kind']
    assert 'vf-run019-tracker' not in scaled
    evidence['three_replicas'] = {'scaled': sorted(scaled), 'tracker_kind': tracker['kind'], 'replicas': 3}
    # Runtime membership versus compiled intent: at one replica the tracker binds
    # the *competing* durable — the reason a scale request from 1 must be blocked
    # before replicas diverge — and at three each replica owns its own durable.
    driver = MemoryDriver('f', 'r')
    try:
        driver.provision([spec('cam', [], 'producer', True), spec('tracker', ['cam'], 'processor', False, partition_by = 'camera_id')], REALTIME)
        channel = ChannelId(driver.flow_id, driver.run_id, 'cam')
        single = driver.messenger('tracker', ['cam'], REALTIME, nb_tasks = 1, partition_by = 'camera_id')
        membership_one = driver.subscriptions(channel)
        single.quiesce()
        single.close()
        replicas = [driver.messenger('tracker', ['cam'], REALTIME, nb_tasks = 3, partition_by = 'camera_id', replica_id = i)
                    for i in range(3)]
        membership_three = driver.subscriptions(channel)
        for m in replicas:
            m.quiesce()
            m.close()
    finally:
        driver.close()
    assert 'tracker' in membership_one and not any(name.startswith('tracker--p') for name in membership_one), membership_one
    assert {'tracker--p0', 'tracker--p1', 'tracker--p2'} <= set(membership_three), membership_three
    evidence['runtime_membership'] = {'nb_tasks_1': membership_one, 'nb_tasks_3': membership_three}


def _oracle_run_026(evidence : Dict[str, Any]) -> None:
    objective = scaling.RateObjective(min_delivered_fraction = 0.9, control_window_seconds = 10.0,
                                      stabilization_seconds = 30.0)
    # One worker under a source three times faster than it, behind a one-message
    # retention: over a 10 s window the node is handed 100 frames and processes
    # them all, while the channel evicts 200 before delivery.
    state = HealthState('detector')
    before = state.throughput(at = 0.0)
    for _ in range(100):
        state.record_offered()
        state.incr('messages_processed')
    after = state.throughput(at = 10.0)
    evicted_before, evicted_after = 0, 200
    sample = scaling.throughput_sample(before.offered, after.offered, before.processed, after.processed,
                                       before.dropped, after.dropped, evicted_before, evicted_after,
                                       after.at - before.at)
    assert (sample.offered, sample.processed, sample.dropped) == (300, 100, 200), sample
    # What the queue says: available never exceeds the retention of one.
    shallow = known(SubscriptionObservation(available = 1, leased = 0, unresolved = 0, dropped = 200,
                                            rejected_publications = 0, observed_at = 10.0, generation = None))
    lag = scaling.observe_demand({'cam': shallow}, required_parents = ['cam'])
    assert lag.replicas == 1, lag                                    # the overload the queue depth conceals
    rate = scaling.observe_rate_demand(known(sample), objective, current_replicas = 1)
    assert rate.diagnosis == scaling.DEMAND_BREACH and rate.replicas == 3, rate
    assert rate.delivered_fraction is not None and rate.delivered_fraction < objective.min_delivered_fraction
    evidence['overload'] = {'sample': sample, 'lag_decision': lag.replicas, 'rate_decision': rate}
    # Detected within one control window: a sample shorter than the window is not yet a decision.
    short = scaling.observe_rate_demand(known(scaling.ThroughputSample(30, 10, 20, 1.0)), objective, 1)
    assert short.diagnosis == scaling.DEMAND_UNKNOWN and short.replicas is None and short.unknown_reason == 'window'
    # Only admissible capacity is requested: the recommendation is clamped, never invented.
    flood = scaling.observe_rate_demand(known(scaling.ThroughputSample(3000, 100, 2900, 10.0)), objective, 1,
                                        max_replicas = 4)
    assert flood.replicas == 4 and flood.diagnosis == scaling.DEMAND_BREACH, flood
    # Stabilization: the same breach right after a replica change holds the count.
    settling = scaling.observe_rate_demand(known(sample), objective, current_replicas = 3, last_change_at = 100.0,
                                           now = 105.0)
    assert settling.diagnosis == scaling.DEMAND_STABILIZING and settling.replicas == 3, settling
    # Capacity added, load reduced: within the objective, the count is held.
    relieved = scaling.observe_rate_demand(known(scaling.ThroughputSample(300, 290, 10, 10.0)), objective,
                                           current_replicas = 3, last_change_at = 100.0, now = 200.0)
    assert relieved.diagnosis == scaling.DEMAND_WITHIN_OBJECTIVE and relieved.replicas == 3, relieved
    # Missing metrics are an explicit unknown, never healthy zero demand.
    missing = scaling.observe_rate_demand(None, objective, 1)
    timed_out = scaling.observe_rate_demand(unknown('timeout', 'scrape timed out'), objective, 1)
    assert missing.replicas is None and missing.diagnosis == scaling.DEMAND_UNKNOWN
    assert missing.unknown_reason == scaling.MISSING_OBSERVATION
    assert timed_out.replicas is None and timed_out.diagnosis == scaling.DEMAND_UNKNOWN and timed_out.unknown_reason == 'timeout'
    # A restarted exporter (counters went down) is a reset, not negative work.
    reset = scaling.throughput_sample(500, 20, 500, 20, 0, 0, 0, 0, 10.0)
    assert (reset.offered, reset.processed) == (20, 20), reset
    evidence.update(short_window = short, clamped = flood, stabilizing = settling, relieved = relieved,
                    missing = missing, timed_out = timed_out, reset = reset)


def _oracle_run_028(evidence : Dict[str, Any], record_faults : Any = None) -> None:
    from videoflow.backends import faults
    a100 = 'NVIDIA-A100-SXM4-80GB'

    def node(name : str, gpus : int) -> NodeFixture:
        return NodeFixture(name, a100, gpus, 80.0, {'nvidia.com/gpu.product': a100, 'nvidia.com/gpu.count': str(gpus)})

    def claims_for(backend : MemoryAllocationBackend, count : int, op : str) -> Dict[str, str]:
        snapshot = backend.inventory({})
        assert isinstance(snapshot, Known)
        plan = backend.plan([WorkloadRequest('flowA', 'run1', f'w{i}', 1, SHARING_EXCLUSIVE) for i in range(count)],
                            snapshot.value)
        assert isinstance(plan, FeasiblePlan), plan
        return {f'w{i}': backend.reserve(FeasiblePlan({f'w{i}': plan.assignments[f'w{i}']}, {}, snapshot.value.generation,
                                                       f'{plan.plan_id}-{i}'), f'flowA:{op}:{i}', None).claim_id
                for i in range(count)}

    def observed(backend : MemoryAllocationBackend, claims : Dict[str, str]) -> Dict[str, Any]:
        return {wid: backend.observe(cid) for wid, cid in claims.items()}

    desired = 10
    # Two matching allocations for a plan that wants ten: a plan for ten is
    # infeasible, two claims are granted, and the decision names the shortfall.
    backend = MemoryAllocationBackend([node('gpu-1', 2)], FakeClock())
    ten = backend.plan([WorkloadRequest('flowA', 'run1', f'w{i}', 1, SHARING_EXCLUSIVE) for i in range(desired)],
                       backend.inventory({}).value)
    assert isinstance(ten, Infeasible), ten
    claims = claims_for(backend, 2, 'grant')
    granted = scaling.reconcile_capacity(desired, observed(backend, claims))
    assert granted.diagnosis == scaling.CAPACITY_CONSTRAINED and (granted.granted, granted.ready, granted.admitted) == (2, 0, 2), granted
    assert '8 cannot be scheduled' in granted.reason, granted.reason
    evidence['two_grants'] = {'plan_for_ten': ten.reasons, 'decision': granted}
    # An allocated claim is not processing capacity until its workload is observed ready.
    backend.mark_workload_ready(claims['w0'])
    one_ready = scaling.reconcile_capacity(desired, observed(backend, claims))
    assert (one_ready.granted, one_ready.ready) == (2, 1), one_ready
    status = {wid: backend.observe(cid).value.status for wid, cid in claims.items()}   # type: ignore[union-attr]
    assert status == {'w0': CLAIM_READY, 'w1': CLAIM_ALLOCATED}, status
    evidence['one_ready'] = one_ready
    # A read failure is unknown capacity, neither counted nor discounted.
    backend.fail_reads('nodes', 'timeout')
    unobservable = scaling.reconcile_capacity(desired, observed(backend, claims))
    backend.fail_reads('nodes', None)
    assert unobservable.diagnosis == scaling.CAPACITY_UNKNOWN and unobservable.unknown_claims == ('w0', 'w1'), unobservable
    assert (unobservable.granted, unobservable.ready, unobservable.admitted) == (0, 0, 0)
    evidence['unobservable'] = unobservable
    # Withdraw one allocation while the request stays pending, then restore it: the
    # admitted count follows the grants, and a Pause at claim.create.after shows a
    # claim in the making is not yet a grant.
    release = backend.release(claims['w1'], 'flowA:withdraw', backend.observe(claims['w1']).value.desired_generation)   # type: ignore[union-attr]
    assert release.status == RELEASE_RELEASED, release
    remaining = {wid: cid for wid, cid in claims.items() if wid != 'w1'}     # a released claim is no claim
    withdrawn = scaling.reconcile_capacity(desired, observed(backend, remaining))
    assert (withdrawn.granted, withdrawn.admitted) == (1, 1) and withdrawn.diagnosis == scaling.CAPACITY_CONSTRAINED
    schedule = faults.FaultSchedule({'claim.create.after': faults.Nth(1, faults.Delay(0.0))})
    with schedule:
        restored_claims = dict(remaining)
        snapshot = backend.inventory({}).value                                                      # type: ignore[union-attr]
        plan = backend.plan([WorkloadRequest('flowA', 'run1', 'w1', 1, SHARING_EXCLUSIVE)], snapshot)
        assert isinstance(plan, FeasiblePlan)
        restored_claims['w1'] = backend.reserve(plan, 'flowA:restore', None).claim_id
    if record_faults is not None:
        record_faults(schedule)
    restored = scaling.reconcile_capacity(desired, observed(backend, restored_claims))
    assert (restored.granted, restored.admitted) == (2, 2) and restored.diagnosis == scaling.CAPACITY_CONSTRAINED
    evidence['withdraw_restore'] = {'withdrawn': withdrawn, 'restored': restored, 'faults': schedule.fired()}
    # No spare capacity at all: one allocation for a plan of ten reports the constraint, never a scale-out.
    spare_less = MemoryAllocationBackend([node('gpu-3', 1)], FakeClock())
    single = claims_for(spare_less, 1, 'single')
    no_spare = scaling.reconcile_capacity(desired, observed(spare_less, single))
    assert no_spare.diagnosis == scaling.CAPACITY_CONSTRAINED and no_spare.admitted == 1 and no_spare.ready == 0
    assert 'grants more capacity' in no_spare.reason, no_spare.reason
    # And the trivial case: desired within the grants is admitted, with the ready count still separate.
    fits = scaling.reconcile_capacity(1, observed(spare_less, single))
    assert fits.diagnosis == scaling.CAPACITY_ADMITTED and fits.admitted == 1 and fits.ready == 0
    evidence['no_spare'] = {'decision': no_spare, 'fits': fits}


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
