'''
The render-only DRA backend (videoflow/deploy/allocation_dra.py): the version and
feature-gate matrix a request is judged against, the ``resource.k8s.io/v1``
objects it renders, and the refusal — by name, with the driver-install remedy —
of everything a claim lifecycle would need (plan Phase 4, decision D9;
ALLOC-019/020/021/023/024).
'''
from __future__ import absolute_import, division, print_function

import subprocess

import pytest
from support_kubectl import FakeKubectl

from videoflow.backends.allocation import (
    FEATURE_CONSUMABLE_CAPACITY,
    FEATURE_DYNAMIC_MIG,
    FEATURE_MPS,
    SHARING_EXCLUSIVE,
    Constraint,
    WorkloadRequest,
    allocation_rejections,
)
from videoflow.core.errors import CapabilityError
from videoflow.deploy import gpu
from videoflow.deploy.allocation_dra import (
    GATE_CONSUMABLE,
    GATE_PARTITIONABLE,
    DraAllocationBackend,
    DraEnvironment,
    gate_stage,
    observe_environment,
    parse_version,
    render_bindings,
)

NVIDIA = frozenset({FEATURE_DYNAMIC_MIG, FEATURE_MPS, FEATURE_CONSUMABLE_CAPACITY})


def _request(workload = 'w', count = 1, features = (), constraints = ()):
    return WorkloadRequest('flow1', 'run1', workload, count, SHARING_EXCLUSIVE, features = frozenset(features),
                           constraints = tuple(constraints))


def test_gate_schedule_follows_the_kubernetes_reference():
    assert parse_version('v1.36.3+k3s1') == (1, 36, 3)
    assert gate_stage(GATE_PARTITIONABLE, (1, 32, 0)) is None            # not yet a gate
    assert gate_stage(GATE_PARTITIONABLE, (1, 34, 2)) == ('alpha', False)
    assert gate_stage(GATE_PARTITIONABLE, (1, 36, 0)) == ('beta', True)
    assert gate_stage(GATE_CONSUMABLE, (1, 35, 0)) == ('alpha', False)
    assert gate_stage(GATE_CONSUMABLE, (1, 37, 0)) == ('beta', True)


def test_matrix_rejects_by_version_driver_gate_and_driver_feature():
    old = DraEnvironment('v1.33.0', driver = 'gpu.nvidia.com', driver_features = NVIDIA)
    assert not old.api_served
    assert all('needs Kubernetes >= 1.34' in r for r in old.unavailable_reasons().values())
    no_driver = DraEnvironment('v1.36.3')
    assert set(no_driver.unavailable_reasons()) == {FEATURE_DYNAMIC_MIG, FEATURE_MPS, FEATURE_CONSUMABLE_CAPACITY}
    assert 'no DRA driver' in no_driver.unavailable_reasons()[FEATURE_DYNAMIC_MIG]
    # 1.34/1.35: the partitionable-devices gate is alpha and off unless enabled.
    v134 = DraEnvironment('v1.34.2', driver = 'gpu.nvidia.com', driver_features = NVIDIA)
    assert 'DRAPartitionableDevices is not enabled (alpha, off by default' in v134.unavailable_reasons()[FEATURE_DYNAMIC_MIG]
    assert v134.available_features() == (FEATURE_MPS,)
    enabled = DraEnvironment('v1.34.2', {GATE_PARTITIONABLE: True, GATE_CONSUMABLE: True},
                             driver = 'gpu.nvidia.com', driver_features = NVIDIA)
    assert enabled.available_features() == (FEATURE_DYNAMIC_MIG, FEATURE_MPS, FEATURE_CONSUMABLE_CAPACITY)
    # 1.36: beta, on by default — but a driver without MPS still cannot do MPS.
    v136 = DraEnvironment('v1.36.3', driver = 'gpu.nvidia.com', driver_features = frozenset({FEATURE_DYNAMIC_MIG}))
    assert v136.available_features() == (FEATURE_DYNAMIC_MIG,)
    assert 'does not implement MPS' in v136.unavailable_reasons()[FEATURE_MPS]
    assert 'does not implement consumable-capacity' in v136.unavailable_reasons()[FEATURE_CONSUMABLE_CAPACITY]
    # Disabled explicitly on a version where the default is on.
    off = DraEnvironment('v1.36.3', {GATE_PARTITIONABLE: False}, driver = 'gpu.nvidia.com', driver_features = NVIDIA)
    assert FEATURE_DYNAMIC_MIG not in off.available_features()


def test_capabilities_carry_the_matrix_and_admission_rejects_by_name():
    env = DraEnvironment('v1.34.2', driver = 'gpu.nvidia.com', driver_features = NVIDIA)
    caps = DraAllocationBackend(env).capabilities({})
    assert caps.authority == 'dra' and caps.version_matrix['render_only'] is True
    assert caps.version_matrix['features'] == (FEATURE_MPS,)
    assert caps.version_matrix['gates'][GATE_PARTITIONABLE] is False
    assert not caps.isolated_mig and caps.cooperative_sharing
    reasons = allocation_rejections([_request('mig', features = {FEATURE_DYNAMIC_MIG}),
                                     _request('both', features = {FEATURE_MPS, FEATURE_DYNAMIC_MIG}),
                                     _request('mps', features = {FEATURE_MPS})], caps)
    assert any("feature 'dynamic-mig' is not available" in r and r.startswith('mig') for r in reasons)
    assert any("'mps' and 'dynamic-mig' cannot be combined" in r for r in reasons)
    assert not any(r.startswith('mps:') for r in reasons)
    # The same backend judged against a mapping (what a test or an operator declares).
    later = DraAllocationBackend().capabilities(env.to_mapping() | {'kubernetes_version': 'v1.36.3'})
    assert later.isolated_mig and later.memory_enforcement == 'hardware'
    assert later.version_matrix['features'] == (FEATURE_DYNAMIC_MIG, FEATURE_MPS, FEATURE_CONSUMABLE_CAPACITY)


def test_render_per_pod_template_and_shared_claim():
    request = _request('detector', count = 2, constraints = [
        Constraint('gpu.nvidia.com/productName', 'In', ('NVIDIA H100 80GB HBM3',)),
        Constraint('gpu.nvidia.com/memory', 'Gt', ('40Gi',), hard = False)])
    per_pod = render_bindings(request, 'gpu.nvidia.com', 'ns')
    (template,) = per_pod.claim_manifests
    assert template['kind'] == 'ResourceClaimTemplate' and template['apiVersion'] == 'resource.k8s.io/v1'
    (req,) = template['spec']['spec']['devices']['requests']
    assert req['exactly']['count'] == 2 and req['exactly']['deviceClassName'] == 'gpu.nvidia.com'
    assert req['exactly']['selectors'] == [{'cel': {'expression': 'device.attributes["gpu.nvidia.com/productName"] == "NVIDIA H100 80GB HBM3"'}}]
    assert per_pod.pod_fragment == {'resourceClaims': [{'name': 'vf-gpu-detector', 'resourceClaimTemplateName': 'vf-gpu-detector-template'}]}
    assert per_pod.container_fragment == {'resources': {'claims': [{'name': 'vf-gpu-detector'}]}}
    assert per_pod.node_constraints == {'device_class': 'gpu.nvidia.com', 'shared': False}
    shared = render_bindings(request, 'gpu.nvidia.com', 'ns', shared_claim = 'vf-group-a', capacity = {'memory': '40Gi'})
    (claim,) = shared.claim_manifests
    assert claim['kind'] == 'ResourceClaim' and claim['metadata']['name'] == 'vf-group-a'
    assert claim['spec']['devices']['requests'][0]['exactly']['capacity'] == {'requests': {'memory': '40Gi'}}
    assert shared.pod_fragment == {'resourceClaims': [{'name': 'vf-gpu-detector', 'resourceClaimName': 'vf-group-a'}]}


def test_lifecycle_calls_refuse_with_the_driver_remedy():
    backend = DraAllocationBackend(DraEnvironment('v1.36.3'))
    for call in (lambda: backend.inventory({}), lambda: backend.plan([], None), lambda: backend.observe('c'),
                 lambda: backend.bindings('c', 'w'), lambda: backend.release('c', 'op', 'g')):
        with pytest.raises(CapabilityError) as excinfo:
            call()
        assert 'renders' in str(excinfo.value) and 'DRA driver' in excinfo.value.remedy


def test_observe_environment_reads_only_and_dra_mode_preflight_refuses_without_a_driver(monkeypatch):
    fake = FakeKubectl({
        'version -o json': '{"serverVersion": {"gitVersion": "v1.36.3+k3s1"}}',
        'get deviceclasses.resource.k8s.io -o json': '{"items": []}',
        'get resourceslices.resource.k8s.io -o json': '{"items": []}',
    })
    monkeypatch.setattr(subprocess, 'run', fake)
    env = observe_environment()
    assert env.kubernetes_version == 'v1.36.3+k3s1' and env.api_served and env.driver is None
    assert fake.mutations() == []
    problems = gpu.get_gpu_mode('dra').preflight_problems()
    assert len(problems) == 1 and problems[0].startswith('IMPOSSIBLE_GPU_REQUEST') and 'DRA driver' in problems[0]
    assert fake.mutations() == []
    # A driver that serves the class: nothing to report.
    fake.responses['get deviceclasses.resource.k8s.io -o json'] = '{"items": [{"metadata": {"name": "gpu.nvidia.com"}}]}'
    fake.responses['get resourceslices.resource.k8s.io -o json'] = '{"items": [{"spec": {"driver": "gpu.nvidia.com"}}]}'
    assert gpu.get_gpu_mode('dra').preflight_problems() == []
