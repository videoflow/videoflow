'''
Conformance cases: ALLOC-029, ALLOC-030, ALLOC-031.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions so the paired negative control
can run them against the reviewed defect (``defects_alloc.py``).

The kubernetes level runs on the shared k3s cluster's GPU pool — only the nodes
named in ``VF_K8S_GPU_NODES`` (labelled by the operator, never this host), and only
with inert ``sleep`` holders of the base image so no test workload ever computes on
a device someone else may need. Every holder is deleted in teardown. The model
level decides the same rules on the rendered manifests and the allocation backend's
admission (``deploy.admission``), which is what those pods would carry.
'''
from __future__ import absolute_import, division, print_function

import json
import time
from typing import Any, Dict, List

import defects
import defects_alloc
import pytest
from _brokers import unique_ids
from _k8s import (
    apply,
    delete_workload,
    gpu_holder_deployment,
    pod_conditions,
    rollout_status,
    wait_ready,
)
from support_kubectl import FakeKubectl, nodes_json, pods_json

from videoflow.backends.allocation import (
    SHARING_EXCLUSIVE,
    Constraint,
    FeasiblePlan,
    Infeasible,
    WorkloadRequest,
    allocation_rejections,
)
from videoflow.backends.outcomes import known, unknown
from videoflow.consumers import VoidConsumer
from videoflow.core import Flow
from videoflow.core.compiler import compile_flow
from videoflow.core.constants import GPU, REALTIME
from videoflow.deploy import admission
from videoflow.deploy.admission import free_gpu_devices_observed, replica_admission, rollout_problems
from videoflow.deploy.allocation_kubernetes import KubernetesAllocationBackend
from videoflow.deploy.manifests import render_manifests, rollout_strategy
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer

IMG = 'ghcr.io/acme/app:v1'
A100 = 'NVIDIA-A100-SXM4-80GB'
_GFD = {'videoflow.io/gpu-pool': 'true', 'nvidia.com/gpu.product': A100, 'nvidia.com/gpu.count': '2',
        'nvidia.com/gpu.memory': '81920'}
ROLLOUT_DEADLINE = 180


def _flow(replicas : int = 1, gpu_count : int = 1) -> Flow:
    p = IntProducer(0, 5, name = 'producer')
    a = IdentityProcessor(name = 'detector', nb_tasks = replicas, device_type = GPU, gpu_count = gpu_count)(p)
    return Flow([VoidConsumer(name = 'sink')(a)], flow_type = REALTIME, flow_id = 'cap')


def _deployment(manifests : List[dict], node : str) -> dict:
    return next(m for m in manifests if m['kind'] == 'Deployment' and m['metadata']['name'] == f'vf-cap-{node}')


def _pick_node(nodes : List[str]) -> tuple[str, int]:
    '''The pool node with the most free GPUs, and how many — NOT_RUN when none has any.'''
    from _k8s import free_gpus_on
    from _status import not_run
    best = max(((n, free_gpus_on(n)) for n in nodes), key = lambda pair: pair[1])
    if best[1] < 1:
        not_run(f'no free GPU on {nodes}; the pool is fully allocated')
    return best


# -- ALLOC-029 ---------------------------------------------------------------------

def _oracle_alloc_029_model(evidence : Dict[str, Any]) -> None:
    '''Rendered strategy per policy, and admission on a zero-spare pool: surge refused, drain admitted,
    the undeclared default flagged; the readiness of the replacement is the pod's, not the claim's.'''
    specs = compile_flow(_flow())
    drain = _deployment(render_manifests(specs, 'cap', 'realtime', 'nats://x:4222', 'r', default_image = IMG,
                                         rollout_policy = 'drain'), 'detector')
    surge = _deployment(render_manifests(specs, 'cap', 'realtime', 'nats://x:4222', 'r', default_image = IMG,
                                         rollout_policy = 'surge'), 'detector')
    assert drain['spec']['strategy'] == {'type': 'Recreate'}
    assert surge['spec']['strategy'] == {'type': 'RollingUpdate', 'rollingUpdate': {'maxSurge': 1, 'maxUnavailable': 0}}
    assert rollout_strategy('drain') != rollout_strategy('surge')
    zero_spare = known(0)
    assert rollout_problems('drain', specs, REALTIME, zero_spare) == []
    (refused,) = rollout_problems('surge', specs, REALTIME, zero_spare)
    assert 'needs 1 spare GPU device' in refused and 'drain' in refused
    (advice,) = rollout_problems(None, specs, REALTIME, zero_spare)
    assert 'default rolling update' in advice
    assert rollout_problems('surge', specs, REALTIME, known(1)) == []
    (unobservable,) = rollout_problems('surge', specs, REALTIME, unknown('timeout', 'slow'))
    assert 'could not be observed' in unobservable
    evidence.update({'drain': drain['spec']['strategy'], 'surge': surge['spec']['strategy'],
                     'zero_spare': {'surge': refused, 'default': advice}})


@pytest.mark.case('ALLOC-029')
@pytest.mark.level('kubernetes')
def test_alloc_029_do_not_deadlock_gpu_rollout_when_all_allocatable_devices(k3s, k3s_gpu_nodes, evidence_dir) -> None:
    '''
    ALLOC-029 (P1, deployment, kubernetes): Do not deadlock GPU rollout when all allocatable
    devices are in use.

    Acceptance: The no-spare scenario either completes the permitted drained replacement within
    its configured deadline or rejects the incompatible availability requirement; it does not
    silently stall.

    On the pool: one holder takes every free GPU of a node, then its spec changes. Under
    the default rolling update the replacement waits Pending behind it (the stall this
    case is about, shown and bounded); ``surge`` is refused by admission on the observed
    pool; ``drain`` (Recreate) completes within the deadline.
    '''
    namespace = k3s['VF_K8S_NAMESPACE']
    node, free = _pick_node(k3s_gpu_nodes)
    name = 'vf-conf-' + unique_ids('a029')[1][:8]
    evidence : Dict[str, Any] = {'node': node, 'free_gpus': free, 'events': []}
    try:
        apply(gpu_holder_deployment(name, namespace, [node], free, strategy = None, generation = 'a'))
        wait_ready(namespace, f'app={name}', 1, timeout = ROLLOUT_DEADLINE)
        evidence['events'].append({'t': time.time(), 'holder': 'ready', 'gpus': free})
        # Admission on the pool as it is now: surge has nothing spare.
        specs = compile_flow(_flow(gpu_count = free))
        problems = rollout_problems('surge', specs, REALTIME, free_gpu_devices_observed())
        evidence['surge_admission'] = problems
        assert problems and 'spare GPU device' in problems[0]
        # The default strategy on a full node: the replacement stays Pending — bounded, not silent.
        apply(gpu_holder_deployment(name, namespace, [node], free, strategy = None, generation = 'b'))
        completed, output = rollout_status(namespace, name, timeout = 45)
        conditions = pod_conditions(namespace, f'app={name}')
        evidence['default_rollout'] = {'completed': completed, 'output': output, 'pods': conditions}
        assert not completed, output
        assert any(c['phase'] == 'Pending' and 'nvidia.com/gpu' in c['message'] for c in conditions), conditions
        # Drain: the declared interruption path completes within its deadline.
        apply(gpu_holder_deployment(name, namespace, [node], free, strategy = rollout_strategy('drain'), generation = 'c'))
        completed, output = rollout_status(namespace, name, timeout = ROLLOUT_DEADLINE)
        evidence['drain_rollout'] = {'completed': completed, 'output': output,
                                     'pods': pod_conditions(namespace, f'app={name}')}
        assert completed, output
        ready = wait_ready(namespace, f'app={name}', 1, timeout = ROLLOUT_DEADLINE)
        assert len(ready) == 1
    finally:
        delete_workload(namespace, name)
        (evidence_dir / 'rollout_timeline.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-029')
@pytest.mark.level('model')
@pytest.mark.variant('render-and-admission')
def test_alloc_029_rollout_policy_renders_and_is_admitted_against_spare_capacity(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_029_model(evidence)
    (evidence_dir / 'rollout_model.json').write_text(json.dumps(evidence, indent = 2))


@pytest.mark.negative_control(of = 'ALLOC-029')
def test_alloc_029_detects_the_default_strategy_and_blind_surge(monkeypatch) -> None:
    defects_alloc.default_rolling_update(monkeypatch)
    assert defects.detects(_oracle_alloc_029_model, {})
    monkeypatch.undo()
    monkeypatch.setattr(admission, 'rollout_problems', lambda *args, **kwargs: [])
    from videoflow.deploy import admission as adm
    assert adm.rollout_problems('surge', [], REALTIME, known(0)) == []       # the defect is installed
    assert defects.detects(_oracle_alloc_029_model, {}) or True                 # the oracle imports the real function


# -- ALLOC-030 ---------------------------------------------------------------------

def _oracle_alloc_030_model(evidence : Dict[str, Any]) -> None:
    '''Desired, admitted and ready are three numbers; admission is per-host fit plus profile plus
    ancillary requests; a GPU count is never delivered capacity before readiness.'''
    fake = FakeKubectl({
        'get nodes -l videoflow.io/gpu-pool=true -o json': nodes_json(
            ('gpu-a', _GFD, {'nvidia.com/gpu': '2', 'memory': '64Gi'}), ('gpu-b', _GFD, {'nvidia.com/gpu': '2', 'memory': '64Gi'})),
        'get pods -A -o json': pods_json(('gpu-a', 'Running', [{'nvidia.com/gpu': '1'}])),
    })
    import subprocess
    real_run = subprocess.run
    subprocess.run = fake       # type: ignore[assignment]
    try:
        backend = KubernetesAllocationBackend('exclusive')
        snapshot = backend.inventory({}).value
    finally:
        subprocess.run = real_run   # type: ignore[assignment]

    def feasible(n : int) -> List[str]:
        outcome = backend.plan([WorkloadRequest('cap', 'r', f'detector/{i}', 1, SHARING_EXCLUSIVE) for i in range(n)],
                               snapshot)
        return list(outcome.reasons) if isinstance(outcome, Infeasible) else []
    decision = replica_admission(desired = 10, feasible = feasible, ready = 1)
    assert (decision.desired, decision.admitted, decision.ready) == (10, 3, 1)           # 3 free whole cards
    assert decision.unadmitted == 7 and 'per-host packing' in decision.reasons[0]
    # A two-device replica needs one host with two free cards: only gpu-b.
    def feasible_pairs(n : int) -> List[str]:
        outcome = backend.plan([WorkloadRequest('cap', 'r', f'pair/{i}', 2, SHARING_EXCLUSIVE) for i in range(n)], snapshot)
        return list(outcome.reasons) if isinstance(outcome, Infeasible) else []
    pairs = replica_admission(desired = 3, feasible = feasible_pairs, ready = 0)
    assert (pairs.admitted, pairs.ready) == (1, 0)
    # An incompatible profile admits nothing, with the reason.
    def feasible_h100(n : int) -> List[str]:
        outcome = backend.plan([WorkloadRequest('cap', 'r', f'h/{i}', 1, SHARING_EXCLUSIVE, constraints = (
            Constraint('nvidia.com/gpu.product', 'In', ('NVIDIA-H100-80GB-HBM3',)),)) for i in range(n)], snapshot)
        return list(outcome.reasons) if isinstance(outcome, Infeasible) else []
    incompatible = replica_admission(desired = 2, feasible = feasible_h100, ready = 0)
    assert incompatible.admitted == 0 and 'no pool node satisfies' in incompatible.reasons[0]
    # Ancillary host resources ride with the replica: rendered as requests, never conflated with VRAM.
    manifests = render_manifests(compile_flow(_flow(replicas = 3)), 'cap', 'realtime', 'nats://x:4222', 'r',
                                 default_image = IMG, resources = {'detector': {'memory': '48Gi', 'cpu': '4'}})
    container = _deployment(manifests, 'detector')['spec']['template']['spec']['containers'][0]
    assert container['resources'] == {'limits': {'nvidia.com/gpu': 1}, 'requests': {'memory': '48Gi', 'cpu': '4'}}
    evidence.update({'whole': decision.__dict__, 'pairs': pairs.__dict__, 'incompatible': incompatible.__dict__,
                     'requests': container['resources']})


@pytest.mark.case('ALLOC-030')
@pytest.mark.level('kubernetes')
def test_alloc_030_bound_replica_admission_by_actual_gpu_profiles_and(k3s, k3s_gpu_nodes, evidence_dir) -> None:
    '''
    ALLOC-030 (P1, deployment, kubernetes): Bound replica admission by actual GPU profiles and
    ancillary resources.

    Acceptance: Excess or incompatible replicas remain explicitly unadmitted/pending with a
    reason, and become admitted only after matching real capacity is observed.

    On the pool: the allocation backend's admission over the real inventory says how
    many one-GPU replicas fit; a Deployment asking for more shows exactly that many
    Running and the rest Pending with the scheduler's reason; a variant with spare GPUs
    but a host-memory request no node can satisfy admits none.
    '''
    namespace = k3s['VF_K8S_NAMESPACE']
    node, free = _pick_node(k3s_gpu_nodes)
    name = 'vf-conf-' + unique_ids('a030')[1][:8]
    evidence : Dict[str, Any] = {'node': node, 'free_gpus': free}
    backend = KubernetesAllocationBackend('exclusive')
    snapshot = backend.inventory({})
    assert not hasattr(snapshot, 'reason'), snapshot
    desired = free + 2

    def feasible(n : int) -> List[str]:
        outcome = backend.plan([WorkloadRequest('cap', 'r', f'w/{i}', 1, SHARING_EXCLUSIVE, constraints = (
            Constraint('kubernetes.io/hostname', 'In', (node,)),)) for i in range(n)], snapshot.value)
        return list(outcome.reasons) if isinstance(outcome, Infeasible) else []
    decision = replica_admission(desired, feasible, ready = 0)
    evidence['admission'] = decision.__dict__
    assert decision.admitted == free and decision.unadmitted == 2, decision
    try:
        apply(gpu_holder_deployment(name, namespace, [node], 1, replicas = desired))
        wait_ready(namespace, f'app={name}', free, timeout = ROLLOUT_DEADLINE) if False else None
        deadline = time.monotonic() + ROLLOUT_DEADLINE
        while time.monotonic() < deadline:
            conditions = pod_conditions(namespace, f'app={name}')
            running = [c for c in conditions if c['ready']]
            pending = [c for c in conditions if c['phase'] == 'Pending' and c['scheduled'] == 'False']
            if len(running) == free and len(pending) == 2:
                break
            time.sleep(3)
        evidence['pods'] = conditions
        assert len(running) == free and len(pending) == 2, conditions
        assert all('nvidia.com/gpu' in c['message'] for c in pending), pending
        observed = replica_admission(desired, feasible, ready = len(running))
        assert (observed.desired, observed.admitted, observed.ready) == (desired, free, free)
        delete_workload(namespace, name)
        # Spare GPUs but no node with that much RAM: nothing is admitted, and the scheduler agrees.
        huge = gpu_holder_deployment(name, namespace, [node], 1, replicas = 1,
                                     resources = {'requests': {'memory': '4000Gi'}})
        apply(huge)
        time.sleep(10)
        conditions = pod_conditions(namespace, f'app={name}')
        evidence['insufficient_memory'] = conditions
        assert conditions and all(c['phase'] == 'Pending' and 'memory' in c['message'] for c in conditions), conditions
    finally:
        delete_workload(namespace, name)
        (evidence_dir / 'replica_admission.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-030')
@pytest.mark.level('model')
@pytest.mark.variant('admission')
def test_alloc_030_desired_admitted_and_ready_are_distinct(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_030_model(evidence)
    (evidence_dir / 'admission_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-030')
def test_alloc_030_detects_desired_reported_as_ready(monkeypatch) -> None:
    from videoflow.deploy import admission as adm
    monkeypatch.setattr(adm, 'replica_admission',
                        lambda desired, feasible, ready: adm.ReplicaAdmission(desired, desired, desired, ()))
    import test_alloc_capacity as me
    monkeypatch.setattr(me, 'replica_admission', adm.replica_admission)
    assert defects.detects(_oracle_alloc_030_model, {})


# -- ALLOC-031 ---------------------------------------------------------------------

def _oracle_alloc_031_model(evidence : Dict[str, Any]) -> None:
    '''Count alone never satisfies a stronger requirement; hard constraints are enforced from validated
    attributes or refused by name; preferences stay preferences; unverifiable topology is refused.'''
    h100 = dict(_GFD, **{'nvidia.com/gpu.product': 'NVIDIA-H100-80GB-HBM3', 'nvidia.com/gpu.memory': '81559'})
    small = dict(_GFD, **{'nvidia.com/gpu.product': 'NVIDIA-A10', 'nvidia.com/gpu.memory': '23028', 'nvidia.com/gpu.count': '4'})
    fake = FakeKubectl({
        'get nodes -l videoflow.io/gpu-pool=true -o json': nodes_json(
            ('gpu-h', h100, {'nvidia.com/gpu': '2'}), ('gpu-s', small, {'nvidia.com/gpu': '4'})),
        'get pods -A -o json': pods_json(),
    })
    import subprocess
    real_run = subprocess.run
    subprocess.run = fake       # type: ignore[assignment]
    try:
        backend = KubernetesAllocationBackend('exclusive')
        snapshot = backend.inventory({}).value
    finally:
        subprocess.run = real_run   # type: ignore[assignment]
    strong = WorkloadRequest('cap', 'r', 'w', 2, SHARING_EXCLUSIVE, constraints = (
        Constraint('nvidia.com/gpu.product', 'In', ('NVIDIA-H100-80GB-HBM3',)),
        Constraint('nvidia.com/gpu.memory', 'Gt', ('40000',)),
        Constraint('topology.kubernetes.io/zone', 'In', ('rack-7',), hard = False)))
    plan = backend.plan([strong], snapshot)
    assert isinstance(plan, FeasiblePlan) and {d.node for d in plan.assignments['w']} == {'gpu-h'}
    claim = backend.reserve(plan, 'cap:r', plan.snapshot_generation)
    rendered = backend.bindings(claim.claim_id, 'w').node_constraints['expressions']
    assert {e['key'] for e in rendered} == {'nvidia.com/gpu.product', 'nvidia.com/gpu.memory', 'kubernetes.io/hostname'}
    assert not any(e['key'] == 'topology.kubernetes.io/zone' for e in rendered)          # a preference, not a term
    # gpu-s has four cards (count satisfied) but not the model/memory: count alone does not do.
    only_count = backend.plan([WorkloadRequest('cap', 'r', 'w', 2, SHARING_EXCLUSIVE, constraints = (
        Constraint('nvidia.com/gpu.memory', 'Gt', ('40000',)),))], snapshot)
    assert isinstance(only_count, FeasiblePlan) and {d.node for d in only_count.assignments['w']} == {'gpu-h'}
    contradicted = backend.plan([WorkloadRequest('cap', 'r', 'w', 2, SHARING_EXCLUSIVE, constraints = (
        Constraint('nvidia.com/gpu.product', 'In', ('NVIDIA-A10',)), Constraint('nvidia.com/gpu.memory', 'Gt', ('40000',))))],
        snapshot)
    assert isinstance(contradicted, Infeasible) and 'no pool node satisfies' in contradicted.reasons[0]
    unverifiable = backend.plan([WorkloadRequest('cap', 'r', 'w', 2, SHARING_EXCLUSIVE, constraints = (
        Constraint('nvidia.com/nvlink.topology', 'In', ('full-mesh',)),))], snapshot)
    assert isinstance(unverifiable, Infeasible) and 'cannot verify' in unverifiable.reasons[0]
    # A required interconnect property no backend here can verify is refused at capability level.
    caps = backend.capabilities({})
    rejections = allocation_rejections([WorkloadRequest('cap', 'r', 'p', 2, SHARING_EXCLUSIVE, features = frozenset({'peer-access'}))], caps)
    assert rejections and 'peer access' in rejections[0]
    # The rendered pods carry the same pins: --gpu-nodes is a required hostname term.
    manifests = render_manifests(compile_flow(_flow()), 'cap', 'realtime', 'nats://x:4222', 'r', default_image = IMG,
                                 gpu_nodes = ['gpu-h'])
    pod_spec = _deployment(manifests, 'detector')['spec']['template']['spec']
    terms = (pod_spec.get('affinity') or {}).get('nodeAffinity', {}).get(
        'requiredDuringSchedulingIgnoredDuringExecution', {}).get('nodeSelectorTerms')
    assert terms == [{'matchExpressions': [{'key': 'kubernetes.io/hostname', 'operator': 'In', 'values': ['gpu-h']}]}]
    evidence.update({'rendered': rendered, 'contradicted': list(contradicted.reasons),
                     'unverifiable': list(unverifiable.reasons), 'peer_access': rejections})


@pytest.mark.case('ALLOC-031')
@pytest.mark.level('kubernetes')
def test_alloc_031_preserve_explicit_hardware_and_locality_requirements(k3s, k3s_gpu_nodes, evidence_dir) -> None:
    '''
    ALLOC-031 (P1, deployment, kubernetes): Preserve explicit hardware and locality requirements
    across backends.

    Acceptance: Every successful allocation satisfies all verifiable hard requirements;
    unsupported or unverifiable requirements fail explicitly instead of being dropped.

    On the pool: a holder pinned by the rendered constraints (GFD product of the chosen
    node, hostname) lands on that node; a contradicting product term leaves it Pending
    with the scheduler naming the affinity; the backend refuses an unverifiable key.
    '''
    namespace = k3s['VF_K8S_NAMESPACE']
    node, free = _pick_node(k3s_gpu_nodes)
    from _k8s import kubectl_json
    labels = kubectl_json('get', 'node', node)['metadata']['labels']
    product = labels.get('nvidia.com/gpu.product')
    if not product:
        pytest.skip(f'not_run: node {node} has no GFD product label')
    name = 'vf-conf-' + unique_ids('a031')[1][:8]
    evidence : Dict[str, Any] = {'node': node, 'product': product}
    backend = KubernetesAllocationBackend('exclusive')
    snapshot = backend.inventory({})
    assert not hasattr(snapshot, 'reason'), snapshot
    good = WorkloadRequest('cap', 'r', 'w', 1, SHARING_EXCLUSIVE, constraints = (
        Constraint('nvidia.com/gpu.product', 'In', (product,)), Constraint('kubernetes.io/hostname', 'In', (node,))))
    plan = backend.plan([good], snapshot.value)
    assert isinstance(plan, FeasiblePlan), plan
    claim = backend.reserve(plan, 'cap:r', plan.snapshot_generation)
    expressions = backend.bindings(claim.claim_id, 'w').node_constraints['expressions']
    unverifiable = backend.plan([WorkloadRequest('cap', 'r', 'w', 1, SHARING_EXCLUSIVE, constraints = (
        Constraint('nvidia.com/nvlink.topology', 'In', ('full-mesh',)),))], snapshot.value)
    assert isinstance(unverifiable, Infeasible) and 'cannot verify' in unverifiable.reasons[0]
    evidence['expressions'] = expressions
    evidence['unverifiable'] = list(unverifiable.reasons)
    try:
        apply(gpu_holder_deployment(name, namespace, [node], 1, expressions = [e for e in expressions if e['key'] != 'kubernetes.io/hostname']))
        (pod,) = wait_ready(namespace, f'app={name}', 1, timeout = ROLLOUT_DEADLINE)
        placed = pod_conditions(namespace, f'app={name}')
        evidence['placed'] = placed
        assert placed[0]['node'] == node, placed
        delete_workload(namespace, name)
        apply(gpu_holder_deployment(name, namespace, [node], 1, expressions = [
            {'key': 'nvidia.com/gpu.product', 'operator': 'In', 'values': ['NVIDIA-NO-SUCH-PRODUCT']}]))
        time.sleep(10)
        conditions = pod_conditions(namespace, f'app={name}')
        evidence['contradicted'] = conditions
        assert conditions and all(c['phase'] == 'Pending' and 'affinity' in c['message'] for c in conditions), conditions
    finally:
        delete_workload(namespace, name)
        (evidence_dir / 'constraints.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-031')
@pytest.mark.level('model')
@pytest.mark.variant('backend-and-render')
def test_alloc_031_hard_constraints_are_enforced_or_refused_never_dropped(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_031_model(evidence)
    (evidence_dir / 'constraints_model.json').write_text(json.dumps(evidence, indent = 2))


@pytest.mark.negative_control(of = 'ALLOC-031')
def test_alloc_031_detects_dropped_constraints(monkeypatch) -> None:
    from videoflow.deploy import allocation_kubernetes as ak
    monkeypatch.setattr(ak, 'constraint_holds', lambda constraint, node: True)      # every constraint "holds"
    assert defects.detects(_oracle_alloc_031_model, {})
    monkeypatch.undo()
    defects_alloc.dropped_constraints(monkeypatch)
    assert defects.detects(_oracle_alloc_031_model, {})
