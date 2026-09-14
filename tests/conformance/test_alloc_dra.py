'''
Conformance cases: ALLOC-017, ALLOC-018, ALLOC-019, ALLOC-020, ALLOC-021, ALLOC-022,
ALLOC-024, ALLOC-025, ALLOC-026, ALLOC-027, ALLOC-028.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions so the paired negative control
can run them against the reviewed defect (``defects_alloc.py``).

Dynamic Resource Allocation is render-only in this release (plan decision D9):
``deploy.allocation_dra`` renders ``resource.k8s.io/v1`` claims and judges requests
against the ``DraEnvironment`` matrix, and every lifecycle call refuses by name. So
the primaries here gate on a DRA driver publishing GPU ResourceSlices in the shared
cluster — none does, and the gate says so (NOT_RUN) — and the model-level variants
decide what can be decided without one: the version/gate matrix, the rendered claim
shapes, the readiness state machine on the reference allocator, the ownership
separation and the taxonomy halves (accounting is not isolation, MPS caps are not
throughput). Static MIG (ALLOC-017) gates on a pool node advertising slices.
'''
from __future__ import absolute_import, division, print_function

import json
import subprocess
from typing import Any, Dict, List

import defects
import defects_alloc
import pytest
from _status import not_run
from support_kubectl import FakeKubectl, nodes_json, pods_json

from videoflow.backends import faults
from videoflow.backends.allocation import (
    CLAIM_ALLOCATED,
    CLAIM_PREPARED,
    CLAIM_READY,
    FEATURE_CONSUMABLE_CAPACITY,
    FEATURE_DYNAMIC_MIG,
    FEATURE_MPS,
    RELEASE_PENDING_RECOVERY,
    RELEASE_RELEASED,
    SHARING_EXCLUSIVE,
    SHARING_ISOLATED_MIG,
    Constraint,
    FeasiblePlan,
    Infeasible,
    WorkloadRequest,
    allocation_rejections,
)
from videoflow.backends.capabilities import ENFORCEMENT_ACCOUNTING, ENFORCEMENT_HARDWARE
from videoflow.backends.memory.allocation import AUTHORITY_DRA, MemoryAllocationBackend, NodeFixture
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.outcomes import Known
from videoflow.core.errors import CapabilityError
from videoflow.deploy import gpu
from videoflow.deploy.allocation_dra import (
    GATE_CONSUMABLE,
    GATE_PARTITIONABLE,
    DraAllocationBackend,
    DraEnvironment,
    observe_environment,
    render_bindings,
)
from videoflow.deploy.allocation_kubernetes import KubernetesAllocationBackend

A100 = 'NVIDIA-A100-SXM4-80GB'
GIB = 1 << 30
NVIDIA_DRIVER = frozenset({FEATURE_DYNAMIC_MIG, FEATURE_MPS, FEATURE_CONSUMABLE_CAPACITY})
_GFD = {'videoflow.io/gpu-pool': 'true', 'nvidia.com/gpu.product': A100, 'nvidia.com/gpu.count': '2',
        'nvidia.com/gpu.memory': '81920'}


def _request(workload : str, count : int = 1, sharing : str = SHARING_EXCLUSIVE, features : Any = (),
             memory_gib : float | None = None) -> WorkloadRequest:
    return WorkloadRequest('flowA', 'run1', workload, count, sharing, features = frozenset(features),
                           minimum_usable_memory_bytes = int(memory_gib * GIB) if memory_gib else None)


@pytest.fixture
def dra_driver(k3s : Dict[str, str]) -> DraEnvironment:
    '''A DRA driver publishing GPU ResourceSlices in the cluster, or NOT_RUN naming what is missing.'''
    env = observe_environment()
    if not env.api_served:
        not_run(f'the cluster ({env.kubernetes_version or "unknown version"}) does not serve resource.k8s.io/v1')
    if env.driver is None:
        not_run('no DRA driver publishes GPU ResourceSlices (install the NVIDIA DRA driver to run the DRA cases)')
    return env


@pytest.fixture
def static_mig_node(k3s_gpu_nodes : List[str]) -> Dict[str, Any]:
    '''A pool node advertising administrator-carved MIG slices, or NOT_RUN.'''
    from _k8s import kubectl_json
    for node in k3s_gpu_nodes:
        info = kubectl_json('get', 'node', node)
        allocatable = (info.get('status') or {}).get('allocatable') or {}
        slices = {k: int(v) for k, v in allocatable.items() if k.startswith('nvidia.com/mig-') and str(v).isdigit()}
        if slices:
            return {'node': node, 'slices': slices, 'labels': info['metadata'].get('labels', {})}
    not_run(f'no pool node in {k3s_gpu_nodes} advertises static MIG slices (nvidia.com/mig-*)')
    return {}


def _with_fake(responses : Dict[str, str], build : Any) -> Any:
    fake = FakeKubectl(responses)
    real = subprocess.run
    subprocess.run = fake       # type: ignore[assignment]
    try:
        return build()
    finally:
        subprocess.run = real   # type: ignore[assignment]


# -- ALLOC-017 ---------------------------------------------------------------------

def _oracle_alloc_017_model(evidence : Dict[str, Any]) -> None:
    '''A free administrator-carved slice is consumed as advertised; foreign geometry and the used
    slice survive; nothing is written.'''
    sliced = dict(_GFD, **{'nvidia.com/mig.config': 'all-1g.10gb', 'nvidia.com/gpu.product': f'{A100}-MIG-1g.10gb'})
    fake = FakeKubectl({
        'get nodes -l videoflow.io/gpu-pool=true -o json': nodes_json(('gpu-m', sliced, {'nvidia.com/mig-1g.10gb': '3'})),
        'get pods -A -o json': pods_json(('gpu-m', 'Running', [{'nvidia.com/mig-1g.10gb': '1'}])),
    })
    real = subprocess.run
    subprocess.run = fake       # type: ignore[assignment]
    try:
        backend = KubernetesAllocationBackend('exclusive')
        snapshot = backend.inventory({}).value
        plan = backend.plan([_request('s', sharing = SHARING_ISOLATED_MIG, memory_gib = 10)], snapshot)
        assert isinstance(plan, FeasiblePlan), plan
        (slice_,) = plan.assignments['s']
        assert slice_.node == 'gpu-m' and slice_.mig_profile == '1g.10gb'
        claim = backend.reserve(plan, 'flowA:run1', plan.snapshot_generation)
        assert claim.evidence.get('written') is False
        bindings = backend.bindings(claim.claim_id, 's')
        assert bindings.container_fragment == {'resources': {'limits': {'nvidia.com/mig-1g.10gb': 1}}}
        released = backend.release(claim.claim_id, 'flowA:run1', claim.desired_generation)
        assert released.status == RELEASE_RELEASED
        # Three slices, one foreign: two sharers fit, three do not — and none of it repartitions.
        two = backend.plan([_request(w, sharing = SHARING_ISOLATED_MIG, memory_gib = 10) for w in 'st'], snapshot)
        three = backend.plan([_request(w, sharing = SHARING_ISOLATED_MIG, memory_gib = 10) for w in 'stu'], snapshot)
        assert isinstance(two, FeasiblePlan) and isinstance(three, Infeasible)
        assert 'static slices are consumed as advertised' in three.reasons[0]
    finally:
        subprocess.run = real   # type: ignore[assignment]
    assert fake.mutations() == [], fake.mutations()
    evidence.update({'slice': slice_.mig_uuid, 'three_refused': list(three.reasons), 'mutations': fake.mutations()})


@pytest.mark.case('ALLOC-017')
@pytest.mark.level('gpu')
def test_alloc_017_consume_externally_prepared_mig_slices_without_taking(k3s, static_mig_node, evidence_dir) -> None:
    '''
    ALLOC-017 (P1, allocation, gpu): Consume externally prepared MIG slices without taking over
    geometry.

    Acceptance: A compatible free static slice is used successfully and all non-owned geometry
    survives application lifecycle operations.

    On a pool node an administrator carved: a holder claims one advertised slice and runs
    on it; the node's slices, its ``mig.config`` label and every other slice are the same
    before and after, and videoflow's managed-MIG hooks never touched it.
    '''
    from _brokers import unique_ids
    from _k8s import apply, delete_workload, kubectl_json, wait_ready
    namespace = k3s['VF_K8S_NAMESPACE']
    node, slices = static_mig_node['node'], static_mig_node['slices']
    resource = sorted(slices)[0]
    name = 'vf-conf-' + unique_ids('a017')[1][:8]
    before = kubectl_json('get', 'node', node)
    evidence : Dict[str, Any] = {'node': node, 'slices': slices, 'resource': resource,
                                 'mig_config_before': before['metadata']['labels'].get('nvidia.com/mig.config')}
    holder = {'apiVersion': 'apps/v1', 'kind': 'Deployment',
              'metadata': {'name': name, 'namespace': namespace, 'labels': {'videoflow.io/conformance': 'true', 'app': name}},
              'spec': {'replicas': 1, 'selector': {'matchLabels': {'app': name}},
                       'template': {'metadata': {'labels': {'app': name, 'videoflow.io/conformance': 'true'}},
                                    'spec': {'priorityClassName': 'cluster-batch', 'runtimeClassName': 'nvidia',
                                             'nodeSelector': {'kubernetes.io/hostname': node},
                                             'tolerations': [{'key': 'nvidia.com/gpu', 'operator': 'Exists', 'effect': 'NoSchedule'}],
                                             'containers': [{'name': 'holder', 'image': __import__('_k8s').base_image(),
                                                             'command': ['sleep', 'infinity'],
                                                             'resources': {'limits': {resource: 1}}}]}}}}
    try:
        apply(holder)
        (pod,) = wait_ready(namespace, f'app={name}', 1, timeout = 300)
        evidence['pod'] = pod
    finally:
        delete_workload(namespace, name)
    after = kubectl_json('get', 'node', node)
    evidence['allocatable_after'] = {k: v for k, v in after['status']['allocatable'].items() if k.startswith('nvidia.com/')}
    assert after['metadata']['labels'].get('nvidia.com/mig.config') == evidence['mig_config_before']
    assert {k: int(v) for k, v in after['status']['allocatable'].items() if k.startswith('nvidia.com/mig-')} == slices
    assert gpu.GPU_OWNER_LABEL not in after['metadata']['labels']
    (evidence_dir / 'static_mig.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-017')
@pytest.mark.level('model')
@pytest.mark.variant('static-backend')
def test_alloc_017_static_slices_are_consumed_never_carved(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_017_model(evidence)
    (evidence_dir / 'static_mig_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-017')
def test_alloc_017_detects_a_static_request_that_repartitions(monkeypatch) -> None:
    defects_alloc.repartitioning_static(monkeypatch)
    assert defects.detects(_oracle_alloc_017_model, {})


# -- ALLOC-018 ---------------------------------------------------------------------

def _oracle_alloc_018_model(evidence : Dict[str, Any]) -> None:
    '''Disjoint pools plan through their own paths; a node both allocators expose is refused by the
    device-plugin and managed-MIG paths before any preparation.'''
    slices = '{"items": [{"spec": {"driver": "gpu.nvidia.com", "nodeName": "gpu-d"}}]}'
    responses = {
        'get nodes -l videoflow.io/gpu-pool=true -o json': nodes_json(
            ('gpu-a', _GFD, {'nvidia.com/gpu': '2'}), ('gpu-d', _GFD, {'nvidia.com/gpu': '2'})),
        'get pods -A -o json': pods_json(),
        'get resourceslices.resource.k8s.io -o json': slices,
    }
    verdicts = {}
    for mode in ('exclusive', 'mix'):
        fake = FakeKubectl(responses)
        real = subprocess.run
        subprocess.run = fake       # type: ignore[assignment]
        try:
            backend = KubernetesAllocationBackend(mode)
            snapshot = backend.inventory({}).value
            both = backend.plan([_request('a', 2), _request('b', 2)], snapshot)
            one = backend.plan([_request('a', 2)], snapshot)
            if mode == 'mix':
                carve = backend.plan([_request('s', sharing = SHARING_ISOLATED_MIG, memory_gib = 10) for _ in range(3)], snapshot)
                assert isinstance(carve, FeasiblePlan) and set(carve.geometry) == {'gpu-a'}, carve
        finally:
            subprocess.run = real   # type: ignore[assignment]
        assert isinstance(both, Infeasible) and 'gpu-d' in both.reasons[0] and 'DRA driver' in both.reasons[0], both
        assert isinstance(one, FeasiblePlan) and {d.node for d in one.assignments['a']} == {'gpu-a'}
        assert fake.mutations() == []
        verdicts[mode] = both.reasons[0]
    # The DRA path serves gpu-d's devices: a claim template, never a device-plugin limit.
    rendered = render_bindings(_request('a'), 'gpu.nvidia.com', 'ns')
    assert 'nvidia.com/gpu' not in json.dumps(rendered.container_fragment)
    evidence.update({'refusals': verdicts, 'dra_container': rendered.container_fragment})


@pytest.mark.case('ALLOC-018')
@pytest.mark.level('gpu')
def test_alloc_018_keep_device_plugin_and_dra_ownership_disjoint(k3s, dra_driver, evidence_dir) -> None:
    '''
    ALLOC-018 (P1, allocation, gpu): Keep device-plugin and DRA ownership disjoint.

    Acceptance: No physical GPU is prepared through both allocator paths; invalid overlap fails
    before enabling workload access.

    With a driver present: the pool's DRA-owned nodes (their ResourceSlices) must be
    disjoint from the nodes the device plugin advertises ``nvidia.com/gpu`` on, and the
    device-plugin backend refuses to plan on the former.
    '''
    from videoflow.deploy.cluster import dra_owned_nodes_observed, gpu_inventory_observed
    owned = dra_owned_nodes_observed()
    inventory = gpu_inventory_observed()
    assert isinstance(owned, Known) and isinstance(inventory, Known), (owned, inventory)
    overlap = [n.name for n in inventory.value if n.name in owned.value and n.allocatable.get('nvidia.com/gpu', 0) > 0]
    evidence = {'dra_nodes': sorted(owned.value), 'plugin_nodes': [n.name for n in inventory.value], 'overlap': overlap}
    (evidence_dir / 'ownership.json').write_text(json.dumps(evidence, indent = 2))
    assert not overlap, f'GPUs exposed through both allocators on {overlap}'
    backend = KubernetesAllocationBackend('exclusive')
    snapshot = backend.inventory({})
    assert isinstance(snapshot, Known)
    for node in owned.value:
        plan = backend.plan([WorkloadRequest('flowA', 'run1', 'w', 1, SHARING_EXCLUSIVE, constraints = (
            Constraint('kubernetes.io/hostname', 'In', (node,)),))], snapshot.value)
        assert isinstance(plan, Infeasible), plan


@pytest.mark.case('ALLOC-018')
@pytest.mark.level('model')
@pytest.mark.variant('disjoint-pools')
def test_alloc_018_dra_owned_nodes_are_refused_by_the_plugin_paths(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_018_model(evidence)
    (evidence_dir / 'ownership_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-018')
def test_alloc_018_detects_a_dra_blind_inventory(monkeypatch) -> None:
    defects_alloc.dra_blind_inventory(monkeypatch)
    assert defects.detects(_oracle_alloc_018_model, {})


# -- ALLOC-019 ---------------------------------------------------------------------

def _oracle_alloc_019_render(evidence : Dict[str, Any]) -> None:
    '''Three independent replicas render one ResourceClaimTemplate each pod instantiates; nothing
    references a shared claim by name; deleting one replica's claim is its own.'''
    from videoflow.consumers import VoidConsumer
    from videoflow.core import Flow
    from videoflow.core.compiler import compile_flow
    from videoflow.core.constants import GPU, REALTIME
    from videoflow.deploy.manifests import render_manifests
    from videoflow.processors import IdentityProcessor
    from videoflow.producers import IntProducer
    flow = Flow([VoidConsumer(name = 'sink')(IdentityProcessor(name = 'stage', nb_tasks = 3, device_type = GPU)(
        IntProducer(0, 3, name = 'p')))], flow_type = REALTIME, flow_id = 'a019')
    manifests = render_manifests(compile_flow(flow), 'a019', 'realtime', 'nats://x:4222', 'r', default_image = 'img:1',
                                 gpu_mode = 'dra')
    templates = [m for m in manifests if m['kind'] == 'ResourceClaimTemplate']
    claims = [m for m in manifests if m['kind'] == 'ResourceClaim']
    stage = next(m for m in manifests if m['kind'] == 'Deployment' and m['metadata']['name'] == 'vf-a019-stage')
    pod = stage['spec']['template']['spec']
    assert stage['spec']['replicas'] == 3 and len(templates) == 1 and claims == []
    assert pod['resourceClaims'] == [{'name': 'vf-gpu-stage', 'resourceClaimTemplateName': 'vf-gpu-stage-template'}]
    assert 'resourceClaimName' not in json.dumps(pod)
    assert pod['containers'][0]['resources'] == {'claims': [{'name': 'vf-gpu-stage'}]}
    bindings = render_bindings(_request('stage/0'), 'gpu.nvidia.com', 'ns')
    assert 'resourceClaimTemplateName' in bindings.pod_fragment['resourceClaims'][0]
    evidence.update({'template': templates[0]['metadata']['name'], 'pod_claims': pod['resourceClaims']})


@pytest.mark.case('ALLOC-019')
@pytest.mark.level('kubernetes')
def test_alloc_019_create_independent_per_pod_dra_claims_for_independent(k3s, dra_driver, evidence_dir) -> None:
    '''
    ALLOC-019 (P1, allocation, kubernetes): Create independent per-pod DRA claims for
    independent replicas.

    Acceptance: Three live independent replicas have three distinct owned claims, and
    deletion/recreation does not revoke surviving claims or leave extra orphan claims.
    '''
    from _brokers import unique_ids
    from _k8s import apply, delete_pods, delete_workload, kubectl_json, wait_ready
    namespace = k3s['VF_K8S_NAMESPACE']
    name = 'vf-conf-' + unique_ids('a019')[1][:8]
    bindings = render_bindings(_request(name), dra_driver.device_classes[0] if dra_driver.device_classes else 'gpu.nvidia.com',
                               namespace)
    (template,) = bindings.claim_manifests
    holder = {'apiVersion': 'apps/v1', 'kind': 'Deployment',
              'metadata': {'name': name, 'namespace': namespace, 'labels': {'videoflow.io/conformance': 'true', 'app': name}},
              'spec': {'replicas': 3, 'selector': {'matchLabels': {'app': name}},
                       'template': {'metadata': {'labels': {'app': name, 'videoflow.io/conformance': 'true'}},
                                    'spec': {'priorityClassName': 'cluster-batch',
                                             'containers': [{'name': 'holder', 'image': __import__('_k8s').base_image(),
                                                             'command': ['sleep', 'infinity'],
                                                             'resources': bindings.container_fragment['resources']}],
                                             **bindings.pod_fragment}}}}
    evidence : Dict[str, Any] = {}
    try:
        apply(template)
        apply(holder)
        pods = wait_ready(namespace, f'app={name}', 3, timeout = 300)
        claims = kubectl_json('get', 'resourceclaims', '-n', namespace).get('items', [])
        owned = {c['metadata']['name']: [o['name'] for o in c['metadata'].get('ownerReferences', [])] for c in claims
                 if any(o['name'] in pods for o in c['metadata'].get('ownerReferences', []))}
        evidence['claims'] = owned
        assert len(owned) == 3 and len({tuple(v) for v in owned.values()}) == 3
        delete_pods(namespace, pods[:1])
        pods_after = wait_ready(namespace, f'app={name}', 3, timeout = 300)
        claims_after = [c['metadata']['name'] for c in kubectl_json('get', 'resourceclaims', '-n', namespace).get('items', [])
                        if any(o['name'] in pods_after for o in c['metadata'].get('ownerReferences', []))]
        evidence['claims_after'] = claims_after
        assert len(claims_after) == 3
        assert len({c for c in owned if c in claims_after}) == 2      # the survivors kept theirs
    finally:
        delete_workload(namespace, name)
        subprocess.run(['kubectl', 'delete', 'resourceclaimtemplate', template['metadata']['name'], '-n', namespace,
                        '--ignore-not-found'], capture_output = True, check = False)
        (evidence_dir / 'claims.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-019')
@pytest.mark.level('model')
@pytest.mark.variant('render')
def test_alloc_019_replicas_render_a_template_not_a_shared_claim(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_019_render(evidence)
    (evidence_dir / 'claims_render.json').write_text(json.dumps(evidence, indent = 2))


@pytest.mark.negative_control(of = 'ALLOC-019')
def test_alloc_019_detects_replicas_coupled_to_one_claim(monkeypatch) -> None:
    defects_alloc.shared_claim_for_replicas(monkeypatch)
    from videoflow.deploy import gpu as gpu_module
    monkeypatch.setattr(gpu_module, 'render_bindings', __import__('videoflow.deploy.allocation_dra', fromlist = ['x']).render_bindings)
    assert defects.detects(_oracle_alloc_019_render, {})


# -- ALLOC-020 ---------------------------------------------------------------------

def _oracle_alloc_020_render(evidence : Dict[str, Any]) -> None:
    '''Sharing follows explicit intent: an execution group renders one ResourceClaim its members
    reference; independent pods of one template never share.'''
    group_a = render_bindings(_request('grp/a'), 'gpu.nvidia.com', 'ns', shared_claim = 'vf-group-vision')
    group_b = render_bindings(_request('grp/b'), 'gpu.nvidia.com', 'ns', shared_claim = 'vf-group-vision')
    assert group_a.claim_manifests[0]['kind'] == 'ResourceClaim' == group_b.claim_manifests[0]['kind']
    assert group_a.claim_manifests[0]['metadata']['name'] == group_b.claim_manifests[0]['metadata']['name']
    assert group_a.pod_fragment['resourceClaims'][0]['resourceClaimName'] == 'vf-group-vision'
    assert group_a.node_constraints['shared'] is True
    solo_a = render_bindings(_request('solo/a'), 'gpu.nvidia.com', 'ns')
    solo_b = render_bindings(_request('solo/b'), 'gpu.nvidia.com', 'ns')
    assert solo_a.claim_manifests[0]['kind'] == 'ResourceClaimTemplate'
    assert 'resourceClaimName' not in solo_a.pod_fragment['resourceClaims'][0]
    assert solo_a.node_constraints['shared'] is False and solo_b.node_constraints['shared'] is False
    evidence.update({'group': group_a.pod_fragment, 'solo': solo_a.pod_fragment})


@pytest.mark.case('ALLOC-020')
@pytest.mark.level('kubernetes')
def test_alloc_020_share_one_allocation_only_when_an_explicit_execution_group(k3s, dra_driver, evidence_dir) -> None:
    '''
    ALLOC-020 (P1, allocation, kubernetes): Share one allocation only when an explicit execution
    group requests it.

    Acceptance: Device sharing exactly follows explicit intent; no independent replica is
    accidentally coupled to another replica's claim lifetime.
    '''
    from _brokers import unique_ids
    from _k8s import apply, delete_workload, kubectl_json, wait_ready
    namespace = k3s['VF_K8S_NAMESPACE']
    device_class = dra_driver.device_classes[0] if dra_driver.device_classes else 'gpu.nvidia.com'
    run = unique_ids('a020')[1][:8]
    shared = f'vf-conf-{run}-group'
    grouped = render_bindings(_request('grp'), device_class, namespace, shared_claim = shared)
    solo = render_bindings(_request(f'vf-conf-{run}-solo'), device_class, namespace)
    evidence : Dict[str, Any] = {}

    def pod(name : str, bindings : Any, containers : int) -> Dict[str, Any]:
        return {'apiVersion': 'v1', 'kind': 'Pod',
                'metadata': {'name': name, 'namespace': namespace, 'labels': {'videoflow.io/conformance': 'true', 'app': name}},
                'spec': {'priorityClassName': 'cluster-batch', 'restartPolicy': 'Never',
                         'containers': [{'name': f'c{i}', 'image': __import__('_k8s').base_image(), 'command': ['sleep', 'infinity'],
                                         'resources': bindings.container_fragment['resources']} for i in range(containers)],
                         **bindings.pod_fragment}}
    names = [f'vf-conf-{run}-grp', f'vf-conf-{run}-solo-a', f'vf-conf-{run}-solo-b']
    try:
        for manifest in grouped.claim_manifests + solo.claim_manifests:
            apply(manifest)
        apply(pod(names[0], grouped, 2))
        apply(pod(names[1], solo, 1))
        apply(pod(names[2], solo, 1))
        for name in names:
            wait_ready(namespace, f'app={name}', 1, timeout = 300)
        claims = kubectl_json('get', 'resourceclaims', '-n', namespace).get('items', [])
        by_pod = {}
        for c in claims:
            for reserved in (c.get('status') or {}).get('reservedFor', []):
                by_pod.setdefault(reserved.get('name'), []).append(c['metadata']['name'])
        evidence['claims_by_pod'] = by_pod
        assert by_pod.get(names[0]) == [shared]
        assert by_pod.get(names[1]) and by_pod.get(names[2]) and by_pod[names[1]] != by_pod[names[2]]
    finally:
        for name in names:
            subprocess.run(['kubectl', 'delete', 'pod', name, '-n', namespace, '--ignore-not-found', '--wait=true'],
                           capture_output = True, check = False, timeout = 180)
        for manifest in grouped.claim_manifests + solo.claim_manifests:
            subprocess.run(['kubectl', 'delete', manifest['kind'].lower(), manifest['metadata']['name'], '-n', namespace,
                            '--ignore-not-found'], capture_output = True, check = False)
        delete_workload(namespace, names[0])
        (evidence_dir / 'sharing.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-020')
@pytest.mark.level('model')
@pytest.mark.variant('render')
def test_alloc_020_sharing_follows_declared_intent(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_020_render(evidence)
    (evidence_dir / 'sharing_render.json').write_text(json.dumps(evidence, indent = 2))


# -- ALLOC-021 ---------------------------------------------------------------------

def _oracle_alloc_021_model(evidence : Dict[str, Any], record_faults : Any = None) -> None:
    '''Allocated is not prepared is not ready: the claim holds at each barrier (the geometry write
    pending on the fake clock, the application's own readiness not yet reported), and a permanent
    preparation failure is a failure state, never readiness. The reference allocator's state
    machine is the contract every real adapter (the DRA driver's NodePrepareResources included)
    must map onto.'''
    from videoflow.backends.memory.allocation import AUTHORITY_MANAGED_MIG
    node = NodeFixture('n1', A100, 2, 80.0, dict(_GFD))
    backend = MemoryAllocationBackend([node], FakeClock(), authority = AUTHORITY_MANAGED_MIG, mig_apply_seconds = 5.0)
    snapshot = backend.inventory({}).value
    plan = backend.plan([_request('w', sharing = SHARING_ISOLATED_MIG, memory_gib = 10)], snapshot)
    assert isinstance(plan, FeasiblePlan) and plan.geometry, plan
    schedule = faults.FaultSchedule({'claim.schedule.after': faults.Delay(0.0), 'claim.ready.after': faults.Delay(0.0)})
    schedule.install()
    try:
        claim = backend.reserve(plan, 'flowA:op1', plan.snapshot_generation)
        assert claim.status == CLAIM_ALLOCATED and claim.grant, claim          # scheduler-allocated
        timeline = [('allocated', claim.status)]
        # Held before preparation: allocated is all the observer may say.
        observed = backend.observe(claim.claim_id)
        assert isinstance(observed, Known) and observed.value.status == CLAIM_ALLOCATED
        backend._clock.advance(5.0)
        backend.apply_pending_geometry()
        prepared = backend.observe(claim.claim_id)
        assert isinstance(prepared, Known) and prepared.value.status == CLAIM_PREPARED, prepared
        timeline.append(('prepared', prepared.value.status))
        # Prepared: the application's own readiness contract has not completed — still not ready.
        held = backend.observe(claim.claim_id)
        assert isinstance(held, Known) and held.value.status == CLAIM_PREPARED
        backend.mark_workload_ready(claim.claim_id)
        ready = backend.observe(claim.claim_id)
        assert isinstance(ready, Known) and ready.value.status == CLAIM_READY
        timeline.append(('ready', ready.value.status))
        evidence['timeline'] = timeline
    finally:
        schedule.uninstall()
    if record_faults is not None:
        record_faults(schedule)
    # A permanent preparation failure is an actionable failure state, never readiness.
    failing = MemoryAllocationBackend([NodeFixture('n1', A100, 2, 80.0, dict(_GFD))], FakeClock(),
                                      authority = AUTHORITY_MANAGED_MIG)
    failing.fail_geometry('n1')
    plan2 = failing.plan([_request('w', sharing = SHARING_ISOLATED_MIG, memory_gib = 10)], failing.inventory({}).value)
    assert isinstance(plan2, FeasiblePlan)
    claim2 = failing.reserve(plan2, 'flowA:op2', plan2.snapshot_generation)
    observed2 = failing.observe(claim2.claim_id)
    assert isinstance(observed2, Known) and observed2.value.status == 'failed', observed2
    failing.mark_workload_ready(claim2.claim_id)
    still_failed = failing.observe(claim2.claim_id)
    assert isinstance(still_failed, Known) and still_failed.value.status == 'failed'
    evidence['permanent_failure'] = observed2.value.status


@pytest.mark.case('ALLOC-021')
@pytest.mark.level('kubernetes')
def test_alloc_021_separate_dra_allocation_device_preparation_and(k3s, dra_driver, evidence_dir) -> None:
    '''
    ALLOC-021 (P0, allocation, kubernetes): Separate DRA allocation, device preparation and
    application readiness.

    Acceptance: The worker remains unready throughout both held barriers; retry succeeds once
    prerequisites complete and permanent preparation failure is surfaced without false
    readiness.

    With a driver present: a pod's ResourceClaim shows ``status.allocation`` before the
    pod's container is running, and the pod is not Ready until its own readiness probe
    passes — the two states are read separately and never conflated.
    '''
    import time

    from _brokers import unique_ids
    from _k8s import apply, kubectl_json
    namespace = k3s['VF_K8S_NAMESPACE']
    device_class = dra_driver.device_classes[0] if dra_driver.device_classes else 'gpu.nvidia.com'
    name = 'vf-conf-' + unique_ids('a021')[1][:8]
    bindings = render_bindings(_request(name), device_class, namespace)
    pod = {'apiVersion': 'v1', 'kind': 'Pod',
           'metadata': {'name': name, 'namespace': namespace, 'labels': {'videoflow.io/conformance': 'true', 'app': name}},
           'spec': {'priorityClassName': 'cluster-batch', 'restartPolicy': 'Never',
                    'containers': [{'name': 'holder', 'image': __import__('_k8s').base_image(),
                                    'command': ['sh', '-c', 'sleep 20; touch /tmp/ready; sleep infinity'],
                                    'readinessProbe': {'exec': {'command': ['cat', '/tmp/ready']}, 'periodSeconds': 2},
                                    'resources': bindings.container_fragment['resources']}],
                    **bindings.pod_fragment}}
    evidence : Dict[str, Any] = {'states': []}
    try:
        for manifest in bindings.claim_manifests:
            apply(manifest)
        apply(pod)
        deadline = time.monotonic() + 300
        seen_allocated_before_ready = False
        while time.monotonic() < deadline:
            current = kubectl_json('get', 'pod', name, '-n', namespace)
            claims = [c for c in kubectl_json('get', 'resourceclaims', '-n', namespace).get('items', [])
                      if any(r.get('name') == name for r in (c.get('status') or {}).get('reservedFor', []))]
            allocated = bool(claims and (claims[0].get('status') or {}).get('allocation'))
            statuses = (current.get('status') or {}).get('containerStatuses') or []
            ready = bool(statuses and statuses[0].get('ready'))
            running = bool(statuses and statuses[0].get('state', {}).get('running'))
            evidence['states'].append({'t': time.time(), 'allocated': allocated, 'running': running, 'ready': ready})
            if allocated and not ready:
                seen_allocated_before_ready = True
            if ready:
                break
            time.sleep(2)
        assert seen_allocated_before_ready, evidence['states'][-3:]
        assert evidence['states'][-1]['ready'] is True
    finally:
        subprocess.run(['kubectl', 'delete', 'pod', name, '-n', namespace, '--ignore-not-found', '--wait=true'],
                       capture_output = True, check = False, timeout = 180)
        for manifest in bindings.claim_manifests:
            subprocess.run(['kubectl', 'delete', manifest['kind'].lower(), manifest['metadata']['name'], '-n', namespace,
                            '--ignore-not-found'], capture_output = True, check = False)
        (evidence_dir / 'readiness.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-021')
@pytest.mark.level('model')
@pytest.mark.variant('state-machine')
def test_alloc_021_allocated_prepared_and_ready_are_three_states(evidence_dir, record_faults) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_021_model(evidence, record_faults)
    (evidence_dir / 'readiness_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-021')
def test_alloc_021_detects_allocated_reported_as_ready(monkeypatch) -> None:
    defects_alloc.allocated_is_ready(monkeypatch)
    assert defects.detects(_oracle_alloc_021_model, {})


# -- ALLOC-022 ---------------------------------------------------------------------

@pytest.mark.case('ALLOC-022')
@pytest.mark.level('gpu')
def test_alloc_022_reclaim_dra_allocations_correctly_after_pod_and_plugin(k3s, dra_driver, evidence_dir) -> None:
    '''
    ALLOC-022 (P1, allocation, gpu): Reclaim DRA allocations correctly after pod and plugin
    failures.

    Acceptance: After recovery all deleted claims release their exclusive resources exactly once
    with zero orphan instances/daemons, and surviving allocations remain functional.

    With a driver present: two claims on one node; the first pod crashes and its claim is
    deleted while the driver's kubelet plugin is restarted; the second claim stays bound
    and its pod stays Ready; the first claim is gone and a new claim can take the device.
    '''
    import time

    from _brokers import unique_ids
    from _k8s import apply, kubectl, kubectl_json, wait_ready
    namespace = k3s['VF_K8S_NAMESPACE']
    device_class = dra_driver.device_classes[0] if dra_driver.device_classes else 'gpu.nvidia.com'
    run = unique_ids('a022')[1][:8]
    names = [f'vf-conf-{run}-a', f'vf-conf-{run}-b']
    rendered = {n: render_bindings(_request(n), device_class, namespace) for n in names}

    def pod(name : str) -> Dict[str, Any]:
        b = rendered[name]
        return {'apiVersion': 'v1', 'kind': 'Pod',
                'metadata': {'name': name, 'namespace': namespace, 'labels': {'videoflow.io/conformance': 'true', 'app': name}},
                'spec': {'priorityClassName': 'cluster-batch', 'restartPolicy': 'Never',
                         'containers': [{'name': 'holder', 'image': __import__('_k8s').base_image(),
                                         'command': ['sleep', 'infinity'], 'resources': b.container_fragment['resources']}],
                         **b.pod_fragment}}
    evidence : Dict[str, Any] = {}
    try:
        for name in names:
            for manifest in rendered[name].claim_manifests:
                apply(manifest)
            apply(pod(name))
        for name in names:
            wait_ready(namespace, f'app={name}', 1, timeout = 300)
        node = kubectl_json('get', 'pod', names[0], '-n', namespace)['spec']['nodeName']
        before = [c['metadata']['name'] for c in kubectl_json('get', 'resourceclaims', '-n', namespace).get('items', [])]
        # Crash the first workload and delete it while the plugin restarts.
        kubectl('delete', 'pod', names[0], '-n', namespace, '--wait=false', timeout = 60)
        plugins = [p['metadata']['name'] for p in kubectl_json('get', 'pods', '-A', '-l', 'app.kubernetes.io/name=nvidia-dra-driver-gpu').get('items', [])
                   if p['spec'].get('nodeName') == node]
        evidence['plugin_pods'] = plugins
        for plugin in plugins:
            ns = next(p['metadata']['namespace'] for p in kubectl_json('get', 'pods', '-A').get('items', []) if p['metadata']['name'] == plugin)
            kubectl('delete', 'pod', plugin, '-n', ns, '--wait=false', timeout = 60)
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            remaining = [c['metadata']['name'] for c in kubectl_json('get', 'resourceclaims', '-n', namespace).get('items', [])]
            if not any(names[0] in c for c in remaining):
                break
            time.sleep(3)
        evidence['claims_before'] = before
        evidence['claims_after'] = remaining
        assert not any(names[0] in c for c in remaining), remaining
        assert any(names[1] in c for c in remaining)
        (survivor,) = wait_ready(namespace, f'app={names[1]}', 1, timeout = 300)
        assert survivor
        # The device is reusable: a new pod from the same template becomes Ready.
        apply(pod(names[0]))
        wait_ready(namespace, f'app={names[0]}', 1, timeout = 300)
    finally:
        for name in names:
            subprocess.run(['kubectl', 'delete', 'pod', name, '-n', namespace, '--ignore-not-found', '--wait=true'],
                           capture_output = True, check = False, timeout = 180)
            for manifest in rendered[name].claim_manifests:
                subprocess.run(['kubectl', 'delete', manifest['kind'].lower(), manifest['metadata']['name'], '-n', namespace,
                                '--ignore-not-found'], capture_output = True, check = False)
        (evidence_dir / 'reclaim.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-022')
@pytest.mark.level('model')
@pytest.mark.variant('idempotent-release')
def test_alloc_022_release_is_idempotent_and_never_reassigns_early(evidence_dir) -> None:
    '''On the reference allocator: a duplicate release is a no-op, a retained workload holds the device,
    and an unrelated claim on the same node survives the other's release.'''
    node = NodeFixture('n1', A100, 2, 80.0, dict(_GFD))
    backend = MemoryAllocationBackend([node], FakeClock(), authority = AUTHORITY_DRA)
    snapshot = backend.inventory({}).value
    plan = backend.plan([_request('a'), _request('b')], snapshot)
    assert isinstance(plan, FeasiblePlan)
    a = backend.reserve(FeasiblePlan({'a': plan.assignments['a']}, {}, snapshot.generation, 'p-a'), 'flowA:a', None)
    b = backend.reserve(FeasiblePlan({'b': plan.assignments['b']}, {}, snapshot.generation, 'p-b'), 'flowA:b', None)
    backend.bind_workload('n1', 'a/0', 1)
    held = backend.release(a.claim_id, 'flowA:rel', a.desired_generation)
    assert held.status == RELEASE_PENDING_RECOVERY                          # not before final release
    backend.unbind_workload('n1', 'a/0')
    first = backend.release(a.claim_id, 'flowA:rel', a.desired_generation)
    second = backend.release(a.claim_id, 'flowA:rel-dup', a.desired_generation)
    assert first.status == RELEASE_RELEASED and second.status == RELEASE_RELEASED and second.reason == 'already released'
    survivor = backend.observe(b.claim_id)
    assert isinstance(survivor, Known) and survivor.value.grant
    assert [m for m in backend.mutations(['release-owner']) if 'flowA:rel-dup' in m[0]] == []  # exactly once
    (evidence_dir / 'reclaim_model.json').write_text(json.dumps({'held': held.status, 'first': first.status,
                                                                 'second': second.reason}, indent = 2))


# -- ALLOC-024 ---------------------------------------------------------------------

def _oracle_alloc_024(evidence : Dict[str, Any]) -> None:
    '''Every requested capability runs only on a verified matrix row; base DRA GA promotes nothing;
    missing gates and versions fail by name before anything is written.'''
    rows = []
    matrix = {
        'k8s 1.33 (no v1 API)': DraEnvironment('v1.33.4', driver = 'gpu.nvidia.com', driver_features = NVIDIA_DRIVER),
        'k8s 1.34.2 gates off': DraEnvironment('v1.34.2', driver = 'gpu.nvidia.com', driver_features = NVIDIA_DRIVER),
        'k8s 1.34.2 gates on': DraEnvironment('v1.34.2', {GATE_PARTITIONABLE: True, GATE_CONSUMABLE: True},
                                              driver = 'gpu.nvidia.com', driver_features = NVIDIA_DRIVER),
        'k8s 1.35 gates off': DraEnvironment('v1.35.0', driver = 'gpu.nvidia.com', driver_features = NVIDIA_DRIVER),
        'k8s 1.36 defaults': DraEnvironment('v1.36.3', driver = 'gpu.nvidia.com', driver_features = NVIDIA_DRIVER),
        'k8s 1.36 no MPS in driver': DraEnvironment('v1.36.3', driver = 'gpu.nvidia.com',
                                                    driver_features = frozenset({FEATURE_DYNAMIC_MIG, FEATURE_CONSUMABLE_CAPACITY})),
        'k8s 1.36 no driver': DraEnvironment('v1.36.3'),
    }
    expected = {
        'k8s 1.33 (no v1 API)': (),
        'k8s 1.34.2 gates off': (FEATURE_MPS,),
        'k8s 1.34.2 gates on': (FEATURE_DYNAMIC_MIG, FEATURE_MPS, FEATURE_CONSUMABLE_CAPACITY),
        'k8s 1.35 gates off': (FEATURE_MPS,),
        'k8s 1.36 defaults': (FEATURE_DYNAMIC_MIG, FEATURE_MPS, FEATURE_CONSUMABLE_CAPACITY),
        'k8s 1.36 no MPS in driver': (FEATURE_DYNAMIC_MIG, FEATURE_CONSUMABLE_CAPACITY),
        'k8s 1.36 no driver': (),
    }
    requests = [_request('mig', features = {FEATURE_DYNAMIC_MIG}), _request('share', features = {FEATURE_CONSUMABLE_CAPACITY}),
                _request('mps', features = {FEATURE_MPS}), _request('plain')]
    for label, env in matrix.items():
        caps = DraAllocationBackend(env).capabilities({})
        available = tuple(caps.version_matrix['features'])
        assert available == expected[label], (label, available)
        rejections = allocation_rejections(requests, caps)
        rejected = {r.split(':')[0] for r in rejections}
        wanted = {'mig': FEATURE_DYNAMIC_MIG, 'share': FEATURE_CONSUMABLE_CAPACITY, 'mps': FEATURE_MPS}
        assert rejected == {w for w, f in wanted.items() if f not in available}, (label, rejections)
        assert 'plain' not in rejected
        for reason in rejections:
            assert 'not available on kubernetes-dra' in reason and 'capability snapshot' in reason
        rows.append({'row': label, 'available': available, 'rejections': rejections,
                     'unavailable': env.unavailable_reasons()})
    # The responsible requirement is named: the gate on 1.34/1.35, the driver on a driverless cluster.
    off = matrix['k8s 1.34.2 gates off'].unavailable_reasons()
    assert 'DRAPartitionableDevices' in off[FEATURE_DYNAMIC_MIG] and 'alpha' in off[FEATURE_DYNAMIC_MIG]
    assert 'DRAConsumableCapacity' in off[FEATURE_CONSUMABLE_CAPACITY]
    assert 'no DRA driver' in matrix['k8s 1.36 no driver'].unavailable_reasons()[FEATURE_MPS]
    # Nothing is ever written: the backend renders and refuses, in that order.
    backend = DraAllocationBackend(matrix['k8s 1.36 defaults'])
    with pytest.raises(CapabilityError):
        backend.reserve(FeasiblePlan({}, {}, 'g', 'p'), 'op', None)
    evidence['rows'] = rows


@pytest.mark.case('ALLOC-024')
@pytest.mark.level('kubernetes')
def test_alloc_024_use_tested_kubernetes_and_nvidia_dra_feature_version(k3s, dra_driver, evidence_dir) -> None:
    '''
    ALLOC-024 (P1, allocation, kubernetes): Use tested Kubernetes and NVIDIA DRA feature/version
    combinations.

    Acceptance: Each requested capability runs only on a verified supported matrix row; all
    missing-gate/version negatives fail before mutation with the responsible requirement
    identified.

    With a driver present: the observed environment (server version, device classes,
    driver) is judged by the same matrix; a feature the row lacks is rejected before any
    claim is rendered for it.
    '''
    evidence : Dict[str, Any] = {'observed': dra_driver.to_mapping()}
    _oracle_alloc_024(evidence)
    caps = DraAllocationBackend(dra_driver).capabilities({})
    evidence['cluster_row'] = caps.version_matrix
    rejections = allocation_rejections([_request('mig', features = {FEATURE_DYNAMIC_MIG})], caps)
    evidence['cluster_dynamic_mig'] = rejections
    assert (FEATURE_DYNAMIC_MIG in caps.version_matrix['features']) == (not rejections)
    (evidence_dir / 'matrix.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-024')
@pytest.mark.level('model')
@pytest.mark.variant('matrix')
def test_alloc_024_the_matrix_decides_per_row(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_024(evidence)
    (evidence_dir / 'matrix_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-024')
def test_alloc_024_detects_ga_promoted_to_every_feature(monkeypatch) -> None:
    defects_alloc.ga_means_everything(monkeypatch)
    assert defects.detects(_oracle_alloc_024, {})


# -- ALLOC-025 ---------------------------------------------------------------------

def _oracle_alloc_025_render(evidence : Dict[str, Any]) -> None:
    '''Consumable claims render capacity requests the scheduler accounts; an omitted capacity is
    rendered as such (the driver's default consumption, named), never as a bounded reservation.'''
    env = DraEnvironment('v1.36.3', driver = 'gpu.nvidia.com', driver_features = NVIDIA_DRIVER)
    caps = DraAllocationBackend(env).capabilities({})
    assert FEATURE_CONSUMABLE_CAPACITY in caps.version_matrix['features'] and caps.cooperative_sharing
    bounded = render_bindings(_request('a', features = {FEATURE_CONSUMABLE_CAPACITY}), 'gpu.nvidia.com', 'ns',
                              capacity = {'memory': '20Gi'})
    (req,) = bounded.claim_manifests[0]['spec']['spec']['devices']['requests']
    assert req['exactly']['capacity'] == {'requests': {'memory': '20Gi'}}
    omitted = render_bindings(_request('b', features = {FEATURE_CONSUMABLE_CAPACITY}), 'gpu.nvidia.com', 'ns')
    (req_b,) = omitted.claim_manifests[0]['spec']['spec']['devices']['requests']
    assert 'capacity' not in req_b['exactly']                                   # the driver's policy decides
    old = DraEnvironment('v1.35.0', driver = 'gpu.nvidia.com', driver_features = NVIDIA_DRIVER)
    rejected = allocation_rejections([_request('a', features = {FEATURE_CONSUMABLE_CAPACITY})],
                                     DraAllocationBackend(old).capabilities({}))
    assert rejected and 'consumable-capacity' in rejected[0]
    evidence.update({'bounded': req['exactly'], 'omitted': req_b['exactly'], 'rejected_on_1_35': rejected})


@pytest.mark.case('ALLOC-025')
@pytest.mark.level('kubernetes')
def test_alloc_025_reserve_consumable_gpu_capacity_across_independent_claims(k3s, dra_driver, evidence_dir) -> None:
    '''
    ALLOC-025 (P1, allocation, kubernetes): Reserve consumable GPU capacity across independent
    claims.

    Acceptance: All admitted accounted claims fit the observed policy, blocked claims progress
    only after release, and unlimited mode is never represented as bounded memory reservation.

    With a driver publishing multi-allocatable devices: three claims of 40Gi against one
    80Gi device — two are allocated, the third waits until one is released.
    '''
    if FEATURE_CONSUMABLE_CAPACITY not in DraAllocationBackend(dra_driver).capabilities({}).version_matrix['features']:
        pytest.skip('unsupported: the cluster row does not offer consumable capacity (declared, not assumed)')
    import time

    from _brokers import unique_ids
    from _k8s import apply, kubectl_json
    namespace = k3s['VF_K8S_NAMESPACE']
    device_class = dra_driver.device_classes[0] if dra_driver.device_classes else 'gpu.nvidia.com'
    run = unique_ids('a025')[1][:8]
    names = [f'vf-conf-{run}-{i}' for i in range(3)]
    rendered = {n: render_bindings(_request(n, features = {FEATURE_CONSUMABLE_CAPACITY}), device_class, namespace,
                                   capacity = {'memory': '40Gi'}) for n in names}
    evidence : Dict[str, Any] = {}
    try:
        for name in names:
            for manifest in rendered[name].claim_manifests:
                apply(manifest)
            apply({'apiVersion': 'v1', 'kind': 'Pod',
                   'metadata': {'name': name, 'namespace': namespace, 'labels': {'videoflow.io/conformance': 'true', 'app': name}},
                   'spec': {'priorityClassName': 'cluster-batch', 'restartPolicy': 'Never',
                            'containers': [{'name': 'holder', 'image': __import__('_k8s').base_image(), 'command': ['sleep', 'infinity'],
                                            'resources': rendered[name].container_fragment['resources']}],
                            **rendered[name].pod_fragment}})
        time.sleep(30)

        def allocated() -> List[str]:
            return sorted(c['metadata']['name'] for c in kubectl_json('get', 'resourceclaims', '-n', namespace).get('items', [])
                          if (c.get('status') or {}).get('allocation') and any(n in c['metadata']['name'] for n in names))
        first = allocated()
        evidence['allocated_initially'] = first
        assert len(first) == 2, first
        waiting = [n for n in names if not any(n in c for c in first)]
        subprocess.run(['kubectl', 'delete', 'pod', names[0], '-n', namespace, '--wait=true'], capture_output = True, check = False, timeout = 180)
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline and not any(waiting[0] in c for c in allocated()):
            time.sleep(3)
        evidence['allocated_after_release'] = allocated()
        assert any(waiting[0] in c for c in evidence['allocated_after_release'])
    finally:
        for name in names:
            subprocess.run(['kubectl', 'delete', 'pod', name, '-n', namespace, '--ignore-not-found', '--wait=true'],
                           capture_output = True, check = False, timeout = 180)
            for manifest in rendered[name].claim_manifests:
                subprocess.run(['kubectl', 'delete', manifest['kind'].lower(), manifest['metadata']['name'], '-n', namespace,
                                '--ignore-not-found'], capture_output = True, check = False)
        (evidence_dir / 'consumable.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-025')
@pytest.mark.level('model')
@pytest.mark.variant('render')
def test_alloc_025_capacity_requests_render_and_gate_by_row(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_025_render(evidence)
    (evidence_dir / 'consumable_render.json').write_text(json.dumps(evidence, indent = 2))


# -- ALLOC-026 ---------------------------------------------------------------------

def _oracle_alloc_026_taxonomy(evidence : Dict[str, Any]) -> None:
    '''Consumable accounting never advertises enforcement; an enforced-isolation request is routed to a
    capable profile or rejected — never satisfied by accounting alone.'''
    env = DraEnvironment('v1.36.3', driver = 'gpu.nvidia.com', driver_features = frozenset({FEATURE_CONSUMABLE_CAPACITY}))
    consumable = DraAllocationBackend(env).capabilities({})
    assert consumable.memory_enforcement == ENFORCEMENT_ACCOUNTING and not consumable.isolated_mig
    isolation_request = _request('iso', sharing = SHARING_ISOLATED_MIG, memory_gib = 10)
    rejected = allocation_rejections([isolation_request], consumable)
    assert rejected and 'hardware-isolated MIG slices' in rejected[0]
    static = KubernetesAllocationBackend('exclusive').capabilities({'static_mig': True})
    assert static.memory_enforcement == ENFORCEMENT_HARDWARE and static.isolated_mig
    assert allocation_rejections([isolation_request], static) == []
    local = __import__('videoflow.deploy.allocation_local', fromlist = ['LocalAllocationBackend']).LocalAllocationBackend('strict').capabilities({})
    assert local.memory_enforcement == ENFORCEMENT_ACCOUNTING and not local.isolated_mig
    evidence.update({'consumable': consumable.memory_enforcement, 'static_mig': static.memory_enforcement,
                     'isolation_rejected_by_consumable': rejected})


@pytest.mark.case('ALLOC-026')
@pytest.mark.level('gpu')
def test_alloc_026_distinguish_consumable_memory_accounting_from_runtime_gpu(k3s, dra_driver, evidence_dir) -> None:
    '''
    ALLOC-026 (P1, allocation, gpu): Distinguish consumable memory accounting from runtime GPU
    memory isolation.

    Acceptance: No consumable profile claims an enforcement guarantee; an enforced-isolation
    request is never silently satisfied by scheduler accounting alone.

    With a driver publishing consumable devices: a pod reserving 8Gi allocates 12Gi through
    the CUDA runtime (safe on an otherwise idle device) — proof that accounting is not
    isolation, reported as such.
    '''
    caps = DraAllocationBackend(dra_driver).capabilities({})
    if FEATURE_CONSUMABLE_CAPACITY not in caps.version_matrix['features']:
        pytest.skip('unsupported: the cluster row does not offer consumable capacity')
    evidence : Dict[str, Any] = {'enforcement_declared': caps.memory_enforcement}
    _oracle_alloc_026_taxonomy(evidence)
    from _brokers import unique_ids
    from _k8s import apply, kubectl, wait_ready
    namespace = k3s['VF_K8S_NAMESPACE']
    device_class = dra_driver.device_classes[0] if dra_driver.device_classes else 'gpu.nvidia.com'
    name = 'vf-conf-' + unique_ids('a026')[1][:8]
    bindings = render_bindings(_request(name, features = {FEATURE_CONSUMABLE_CAPACITY}), device_class, namespace,
                               capacity = {'memory': '8Gi'})
    probe = ('from cuda.bindings import runtime as r; import json\n'
             'err, p = r.cudaMalloc(12 << 30); free, total = r.cudaMemGetInfo()[1:]\n'
             'print(json.dumps({"malloc": str(err), "free": free, "total": total}))')
    try:
        for manifest in bindings.claim_manifests:
            apply(manifest)
        apply({'apiVersion': 'v1', 'kind': 'Pod',
               'metadata': {'name': name, 'namespace': namespace, 'labels': {'videoflow.io/conformance': 'true', 'app': name}},
               'spec': {'priorityClassName': 'cluster-batch', 'restartPolicy': 'Never',
                        'containers': [{'name': 'holder', 'image': __import__('_k8s').base_image(), 'command': ['sleep', 'infinity'],
                                        'resources': bindings.container_fragment['resources']}],
                        **bindings.pod_fragment}})
        wait_ready(namespace, f'app={name}', 1, timeout = 300)
        out = kubectl('exec', '-n', namespace, name, '--', 'python', '-c', probe, timeout = 300)
        evidence['over_reservation'] = json.loads(out.strip().splitlines()[-1])
        assert 'cudaSuccess' in evidence['over_reservation']['malloc']          # accounting did not enforce
    finally:
        subprocess.run(['kubectl', 'delete', 'pod', name, '-n', namespace, '--ignore-not-found', '--wait=true'],
                       capture_output = True, check = False, timeout = 180)
        for manifest in bindings.claim_manifests:
            subprocess.run(['kubectl', 'delete', manifest['kind'].lower(), manifest['metadata']['name'], '-n', namespace,
                            '--ignore-not-found'], capture_output = True, check = False)
        (evidence_dir / 'accounting_vs_isolation.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-026')
@pytest.mark.level('model')
@pytest.mark.variant('taxonomy')
def test_alloc_026_accounting_is_never_reported_as_isolation(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_026_taxonomy(evidence)
    (evidence_dir / 'taxonomy.json').write_text(json.dumps(evidence, indent = 2))


# -- ALLOC-027 ---------------------------------------------------------------------

def _oracle_alloc_027_taxonomy(evidence : Dict[str, Any]) -> None:
    '''MPS is a driver capability with declared caps; combinations with dynamic MIG or consumable shares
    fail before preparation; no throughput guarantee is derived from an active-thread percentage.'''
    env = DraEnvironment('v1.36.3', driver = 'gpu.nvidia.com', driver_features = NVIDIA_DRIVER)
    caps = DraAllocationBackend(env).capabilities({})
    assert FEATURE_MPS in caps.version_matrix['features'] and caps.cooperative_sharing
    combos = allocation_rejections([_request('a', features = {FEATURE_MPS, FEATURE_DYNAMIC_MIG}),
                                    _request('b', features = {FEATURE_MPS, FEATURE_CONSUMABLE_CAPACITY}),
                                    _request('c', features = {FEATURE_MPS})], caps)
    assert any(r.startswith('a:') and 'cannot be combined' in r for r in combos)
    assert any(r.startswith('b:') and 'cannot be combined' in r for r in combos)
    assert not any(r.startswith('c:') for r in combos)
    # The capability record carries no throughput guarantee of any kind.
    assert not any(k for k in caps.__dict__ if 'throughput' in k or 'fps' in k or 'latency' in k)
    no_mps = DraEnvironment('v1.36.3', driver = 'gpu.nvidia.com', driver_features = frozenset({FEATURE_DYNAMIC_MIG}))
    assert allocation_rejections([_request('c', features = {FEATURE_MPS})], DraAllocationBackend(no_mps).capabilities({}))
    evidence.update({'combos': combos, 'capability_fields': sorted(caps.__dict__)})


@pytest.mark.case('ALLOC-027')
@pytest.mark.level('gpu')
def test_alloc_027_validate_mps_client_budgets_without_claiming_throughput(k3s, dra_driver, evidence_dir) -> None:
    '''
    ALLOC-027 (P1, allocation, gpu): Validate MPS client budgets without claiming throughput
    isolation.

    Acceptance: Clients obey the configured tested caps, share only the intended allocation,
    and leave zero orphan daemon after final release; throughput guarantees remain explicitly
    unclaimed.

    With a driver whose row offers MPS: two clients of one shared claim, the daemon present
    while they run and gone after the last release.
    '''
    caps = DraAllocationBackend(dra_driver).capabilities({})
    if FEATURE_MPS not in caps.version_matrix['features']:
        pytest.skip('unsupported: the cluster row does not offer MPS (driver feature not declared)')
    evidence : Dict[str, Any] = {}
    _oracle_alloc_027_taxonomy(evidence)
    import time

    from _brokers import unique_ids
    from _k8s import apply, kubectl_json, wait_ready
    namespace = k3s['VF_K8S_NAMESPACE']
    device_class = dra_driver.device_classes[0] if dra_driver.device_classes else 'gpu.nvidia.com'
    run = unique_ids('a027')[1][:8]
    shared = f'vf-conf-{run}-mps'
    bindings = render_bindings(_request('grp', features = {FEATURE_MPS}), device_class, namespace, shared_claim = shared)
    name = f'vf-conf-{run}-clients'
    try:
        for manifest in bindings.claim_manifests:
            apply(manifest)
        apply({'apiVersion': 'v1', 'kind': 'Pod',
               'metadata': {'name': name, 'namespace': namespace, 'labels': {'videoflow.io/conformance': 'true', 'app': name}},
               'spec': {'priorityClassName': 'cluster-batch', 'restartPolicy': 'Never',
                        'containers': [{'name': f'client{i}', 'image': __import__('_k8s').base_image(), 'command': ['sleep', 'infinity'],
                                        'resources': bindings.container_fragment['resources']} for i in range(2)],
                        **bindings.pod_fragment}})
        wait_ready(namespace, f'app={name}', 1, timeout = 300)
        daemons = [p['metadata']['name'] for p in kubectl_json('get', 'pods', '-A').get('items', []) if 'mps' in p['metadata']['name']]
        evidence['daemons_while_running'] = daemons
        assert daemons, 'no MPS control daemon appeared for the shared claim'
    finally:
        subprocess.run(['kubectl', 'delete', 'pod', name, '-n', namespace, '--ignore-not-found', '--wait=true'],
                       capture_output = True, check = False, timeout = 180)
        for manifest in bindings.claim_manifests:
            subprocess.run(['kubectl', 'delete', manifest['kind'].lower(), manifest['metadata']['name'], '-n', namespace,
                            '--ignore-not-found'], capture_output = True, check = False)
        time.sleep(30)
        evidence['daemons_after_release'] = [p['metadata']['name'] for p in kubectl_json('get', 'pods', '-A').get('items', [])
                                             if 'mps' in p['metadata']['name'] and shared in json.dumps(p)]
        (evidence_dir / 'mps.json').write_text(json.dumps(evidence, indent = 2, default = str))
    assert evidence['daemons_after_release'] == []


@pytest.mark.case('ALLOC-027')
@pytest.mark.level('model')
@pytest.mark.variant('taxonomy')
def test_alloc_027_mps_combinations_fail_before_preparation(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_027_taxonomy(evidence)
    (evidence_dir / 'mps_taxonomy.json').write_text(json.dumps(evidence, indent = 2))


# -- ALLOC-028 ---------------------------------------------------------------------

def _oracle_alloc_028_model(evidence : Dict[str, Any]) -> None:
    '''Legacy geometry hooks are blocked on DRA-owned nodes; a node with live static partitions is never
    implicitly repartitioned; dynamic MIG needs the row's gate and the driver.'''
    from videoflow.deploy.mig import NodeInventory
    live_static = NodeInventory('gpu-s', A100, 2, 80.0, mig_config = 'all-1g.10gb', mig_partitioned = True,
                                used_units = {'nvidia.com/mig-1g.10gb': 1}, allocatable = {'nvidia.com/mig-1g.10gb': 14})
    dra_node = NodeInventory('gpu-d', A100, 2, 80.0, dra_owned = True)
    free_node = NodeInventory('gpu-f', A100, 2, 80.0)
    usable, excluded = gpu._partition_inventory([live_static, dra_node, free_node], 'flowA')
    assert [n.name for n in usable] == ['gpu-f']
    assert 'DRA driver' in excluded['gpu-d'] and 'already MIG-partitioned outside videoflow' in excluded['gpu-s']
    # A mix deploy that needs the static node's cards fails to lay out rather than carving it.
    from videoflow.core.compiler import NodeSpec
    from videoflow.deploy.mig import LayoutError, solve_layout
    sharers = [NodeSpec('s', 'x.Y', {}, [], 'processor', True, 5, 'gpu', True, gpu_memory_gib = 40)]
    with pytest.raises(LayoutError):
        solve_layout(usable, sharers)                                # two free cards hold four 40 GiB slices, not five
    # Dynamic MIG on the DRA path: gated by row and driver, explicitly.
    ampere_row = DraEnvironment('v1.35.0', driver = 'gpu.nvidia.com', driver_features = NVIDIA_DRIVER)
    hopper_row = DraEnvironment('v1.36.3', driver = 'gpu.nvidia.com', driver_features = NVIDIA_DRIVER)
    refused = allocation_rejections([_request('dyn', features = {FEATURE_DYNAMIC_MIG})], DraAllocationBackend(ampere_row).capabilities({}))
    admitted = allocation_rejections([_request('dyn', features = {FEATURE_DYNAMIC_MIG})], DraAllocationBackend(hopper_row).capabilities({}))
    assert refused and 'DRAPartitionableDevices' in ampere_row.unavailable_reasons()[FEATURE_DYNAMIC_MIG]
    assert admitted == []
    evidence.update({'excluded': excluded, 'dynamic_mig_1_35': refused})


@pytest.mark.case('ALLOC-028')
@pytest.mark.level('gpu')
def test_alloc_028_migrate_static_mig_nodes_to_dra_dynamic_ownership(k3s, dra_driver, static_mig_node, evidence_dir) -> None:
    '''
    ALLOC-028 (P1, allocation, gpu): Migrate static MIG nodes to DRA dynamic ownership
    deliberately.

    Acceptance: Active static users are never destroyed by an application deploy; post-drain
    transfer produces exactly one allocator owner and valid verified dynamic geometry.

    With a driver and a statically partitioned pool node: an application deploy cannot
    enable dynamic MIG over the live partitions (the row refuses or the node is excluded),
    and the node has exactly one allocator owner at any time.
    '''
    from videoflow.deploy.cluster import dra_owned_nodes_observed, gpu_inventory_observed
    node = static_mig_node['node']
    owned = dra_owned_nodes_observed()
    inventory = gpu_inventory_observed()
    assert isinstance(owned, Known) and isinstance(inventory, Known)
    record = next(n for n in inventory.value if n.name == node)
    evidence = {'node': node, 'dra_owned': node in owned.value, 'plugin_slices': record.allocatable,
                'mig_config': record.mig_config}
    (evidence_dir / 'migration.json').write_text(json.dumps(evidence, indent = 2, default = str))
    assert not (node in owned.value and any(k.startswith('nvidia.com/mig-') for k in record.allocatable)), \
        'the node is exposed by both allocators'
    usable, excluded = gpu._partition_inventory(inventory.value, 'flowA')
    assert node not in [n.name for n in usable] and node in excluded            # never carved by an application deploy


@pytest.mark.case('ALLOC-028')
@pytest.mark.level('model')
@pytest.mark.variant('ownership')
def test_alloc_028_legacy_hooks_are_blocked_and_static_partitions_survive(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_028_model(evidence)
    (evidence_dir / 'migration_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-028')
def test_alloc_028_detects_hooks_blind_to_dra_ownership(monkeypatch) -> None:
    defects_alloc.hooks_ignore_dra_ownership(monkeypatch)
    assert defects.detects(_oracle_alloc_028_model, {})
