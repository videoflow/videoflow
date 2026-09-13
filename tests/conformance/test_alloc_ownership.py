'''
Conformance cases: ALLOC-004, ALLOC-007, ALLOC-008, ALLOC-012, ALLOC-013.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions so the paired negative control
can run them against the reviewed defect (``defects.py``). A ``pending`` marker means
the case is a skeleton reporting NOT_RUN until its implementation phase lands.

The kubernetes-level tests here run against the shared k3s cluster through the
``k3s`` gates in ``conftest.py`` and never mutate anything but their own labelled
RBAC objects; see the docstrings.
'''
from __future__ import absolute_import, division, print_function

import json
import os
import subprocess
from typing import Any, Dict, List

import defects
import pytest
from support_kubectl import FakeKubectl, fake_run, nodes_json

from videoflow.backends.allocation import (
    RELEASE_PENDING_RECOVERY,
    RELEASE_RELEASED,
    SHARING_EXCLUSIVE,
    SHARING_ISOLATED_MIG,
    FeasiblePlan,
    Infeasible,
    WorkloadRequest,
)
from videoflow.backends.memory.allocation import AUTHORITY_MANAGED_MIG, MemoryAllocationBackend, NodeFixture
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.outcomes import Known, Unknown
from videoflow.core.compiler import NodeSpec
from videoflow.core.errors import UnobservableState
from videoflow.deploy import cluster, gpu

A100 = 'NVIDIA-A100-SXM4-80GB'
_PHYSICAL = {'nvidia.com/gpu.product': A100, 'nvidia.com/gpu.count': '2', 'nvidia.com/gpu.memory': '81920',
             'nvidia.com/mig.capable': 'true', 'nvidia.com/mig.strategy': 'single'}
_SLICED = {'nvidia.com/gpu.product': 'NVIDIA-GeForce-RTX-3090-SHARED', 'nvidia.com/gpu.replicas': '4',
           'nvidia.com/gpu.sharing-strategy': 'time-slicing', 'nvidia.com/gpu.count': '1'}


def _gpu_spec(name : str, gpu_count : int = 1, gpu_memory_gib : float | None = None, nb_tasks : int = 1) -> NodeSpec:
    return NodeSpec(name, 'videoflow.processors.basic.IdentityProcessor', {}, [],
                    'processor', True, nb_tasks, 'gpu', True,
                    gpu_count = gpu_count, gpu_memory_gib = gpu_memory_gib)


# -- ALLOC-004 ---------------------------------------------------------------------

@pytest.mark.case('ALLOC-004')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 4 (Kubernetes allocation backend; needs VF_K8S_GPU_NODES)')
def test_alloc_004_enforce_exclusive_gpu_ownership_under_simultaneous_claim() -> None:
    '''
    ALLOC-004 (P0, allocation, kubernetes): Enforce exclusive GPU ownership under simultaneous
    claim attempts.

    Acceptance: Exactly one winner per contested node; the loser never mutates geometry and
    exits with a typed conflict; a stale rollback cannot remove a newer owner.
    '''


# -- ALLOC-007 ---------------------------------------------------------------------

def _oracle_alloc_007_memory(evidence : Dict[str, Any]) -> None:
    '''The reference allocator: unknown occupancy blocks, unknown config keeps ownership.'''
    node = NodeFixture('n1', A100, 4, 80.0, _PHYSICAL)
    backend = MemoryAllocationBackend([node], FakeClock(), authority = AUTHORITY_MANAGED_MIG)
    backend.bind_workload('n1', 'foreign/trainer', 4)              # a foreign pod holds every card
    backend.fail_reads('pods', 'auth')
    snapshot = backend.inventory({}).value
    assert snapshot.completeness == 'partial' and snapshot.occupancy == {}
    plan = backend.plan([WorkloadRequest('flowA', 'r', 'w', 1, SHARING_EXCLUSIVE)], snapshot)
    assert isinstance(plan, Infeasible) and 'occupancy unknown' in plan.reasons[0], plan
    mig = backend.plan([WorkloadRequest('flowA', 'r', 's', 1, SHARING_ISOLATED_MIG,
                                        minimum_usable_memory_bytes = 20 << 30)], snapshot)
    assert isinstance(mig, Infeasible), 'a partial snapshot admitted a repartition'
    assert backend.mutations() == [] and backend.node('n1').workloads == {'foreign/trainer': 4}
    backend.fail_reads('nodes', 'timeout')
    assert isinstance(backend.inventory({}), Unknown)
    backend.fail_reads('nodes', None)
    backend.fail_reads('pods', None)
    # A complete snapshot tells the truth: the foreign workload really holds all four.
    complete = backend.inventory({}).value
    assert complete.completeness == 'complete' and complete.occupancy == {'n1': 4}
    assert isinstance(backend.plan([WorkloadRequest('flowA', 'r', 'w', 1, SHARING_EXCLUSIVE)], complete), Infeasible)
    evidence['memory'] = {'partial_reasons': list(plan.reasons), 'mutations': backend.mutations(),
                          'complete_occupancy': dict(complete.occupancy)}

    # Cleanup half: an unreadable shared configuration keeps ownership for a retry.
    backend.unbind_workload('n1', 'foreign/trainer')
    plan2 = backend.plan([WorkloadRequest('flowA', 'r', 's', 1, SHARING_ISOLATED_MIG,
                                          minimum_usable_memory_bytes = 20 << 30)], backend.inventory({}).value)
    assert isinstance(plan2, FeasiblePlan)
    claim = backend.reserve(plan2, 'flowA:op', None)
    backend.fail_reads('config', 'auth')
    held = backend.release(claim.claim_id, 'flowA:release', claim.desired_generation)
    assert held.status == RELEASE_PENDING_RECOVERY and backend.node('n1').owner == 'flowA'
    backend.fail_reads('config', None)
    assert backend.release(claim.claim_id, 'flowA:release', claim.desired_generation).status == RELEASE_RELEASED
    evidence['memory']['release_after_unknown_config'] = held.status


def _oracle_alloc_007_mix(monkeypatch : pytest.MonkeyPatch, evidence : Dict[str, Any]) -> None:
    '''The real strategy at the kubectl boundary: pod listing denied, node listing fine.'''
    pool = nodes_json(('gpu-a', _PHYSICAL, {'nvidia.com/gpu': '2'}))
    fake = FakeKubectl({'version': '{}', 'gpu-pool=true -o name': 'node/gpu-a', 'gpu-pool=true -o json': pool,
                        'get pods -A -l app=nvidia-mig-manager': ''},
                       failing = ('pods -A -o json',))
    monkeypatch.setattr(subprocess, 'run', fake)
    observed = cluster.gpu_units_in_use_observed()
    assert isinstance(observed, Unknown) and 'Forbidden' in observed.detail
    inventory = cluster.gpu_inventory_observed()
    assert isinstance(inventory, Known) and [n.occupancy_known for n in inventory.value] == [False]
    with pytest.raises(UnobservableState) as info:
        gpu.MixGpu().resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)], flow_id = 'flow1')
    assert 'pods' in str(info.value) and info.value.remedy
    problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia', demand = {'nvidia.com/gpu': 1})
    assert any(p.startswith(gpu.UNOBSERVABLE_GPU_STATE) for p in problems), problems
    assert fake.mutations() == []
    evidence['mix'] = {'occupancy': str(observed), 'diagnostic': str(info.value), 'problems': problems}

    # Cleanup: an unreadable node listing or manager listing changes nothing.
    for needle in ('get nodes -o json', 'app=nvidia-mig-manager'):
        sweep = FakeKubectl({'get nodes -o json': json.dumps({'items': []}),
                             'get clusterpolicies': json.dumps({'items': [{
                                 'metadata': {'name': 'cluster-policy', 'annotations': {
                                     gpu.MIG_CONFIG_NAME_RESTORE_ANNOTATION: 'default-mig-parted-config'}},
                                 'spec': {'migManager': {'config': {'name': gpu.MIG_CONFIGMAP_NAME}}}}]}),
                             'get configmap videoflow-mig-parted-config': json.dumps(
                                 {'metadata': {'resourceVersion': '7'}, 'data': {'config.yaml': 'version: v1\nmig-configs: {}\n'}})},
                            failing = (needle,))
        monkeypatch.setattr(subprocess, 'run', sweep)
        gpu.MixGpu().cleanup(flow_id = 'flow1')
        assert sweep.mutations() == [], f'cleanup mutated with {needle!r} unreadable: {sweep.mutations()}'

    # Restored access: the same request plans normally.
    monkeypatch.setattr(subprocess, 'run', FakeKubectl({'gpu-pool=true -o json': pool, 'pods -A': '{"items": []}'}))
    resolved = gpu.MixGpu().resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)], flow_id = 'flow1')
    assert resolved[0].gpu_resource_name == 'nvidia.com/mig-1g.10gb'


_RBAC_USER = 'videoflow-test-nodes-only'
_RBAC_LABEL = 'videoflow.io/test=alloc-007'


def _rbac_manifests() -> str:
    labels = {'app.kubernetes.io/managed-by': 'videoflow', 'videoflow.io/test': 'alloc-007'}
    role = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'ClusterRole',
            'metadata': {'name': _RBAC_USER, 'labels': labels},
            'rules': [{'apiGroups': [''], 'resources': ['nodes'], 'verbs': ['get', 'list']}]}
    binding = {'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'ClusterRoleBinding',
               'metadata': {'name': _RBAC_USER, 'labels': labels},
               'subjects': [{'kind': 'User', 'name': _RBAC_USER, 'apiGroup': 'rbac.authorization.k8s.io'}],
               'roleRef': {'kind': 'ClusterRole', 'name': _RBAC_USER, 'apiGroup': 'rbac.authorization.k8s.io'}}
    return json.dumps({'apiVersion': 'v1', 'kind': 'List', 'items': [role, binding]})


@pytest.mark.case('ALLOC-007')
@pytest.mark.level('kubernetes')
def test_alloc_007_treat_failed_occupancy_and_ownership_reads_as_unknown(k3s_admin, tmp_path, evidence_dir) -> None:
    '''
    ALLOC-007 (P0, allocation, kubernetes): Treat failed occupancy and ownership reads as
    unknown.

    Acceptance: All read failures block the unsafe decision before mutation; the foreign
    allocation remains intact and a later complete snapshot permits normal progress.

    Cluster footprint: one ClusterRole + ClusterRoleBinding named
    ``videoflow-test-nodes-only`` (labelled ``videoflow.io/test=alloc-007``) that
    lets an impersonated user list nodes but not pods; both are deleted at the
    end. Nothing else is created, and every videoflow call runs read-only.
    '''
    env = dict(os.environ)
    subprocess.run(['kubectl', 'apply', '-f', '-'], input = _rbac_manifests(), text = True,
                   check = True, capture_output = True, env = env)
    try:
        wrapper = tmp_path / 'kubectl-restricted'
        wrapper.write_text(f'#!/bin/sh\nexec kubectl --as={_RBAC_USER} "$@"\n')
        wrapper.chmod(0o755)
        restricted = str(wrapper)
        foreign_before = cluster.gpu_units_in_use_observed()
        assert isinstance(foreign_before, Known), foreign_before
        observed = cluster.gpu_units_in_use_observed(restricted)
        assert isinstance(observed, Unknown) and 'Forbidden' in observed.detail and 'pods' in observed.detail
        assert cluster.gpu_units_in_use(restricted) == {}                # display-only wrapper
        inventory = cluster.gpu_inventory_observed(restricted)
        assert isinstance(inventory, Known), inventory                   # nodes are readable
        assert all(not n.occupancy_known for n in inventory.value)
        problems = cluster.gpu_preflight(restricted, gpu_runtime_class = 'nvidia', demand = {'nvidia.com/gpu': 1})
        assert any(p.startswith(gpu.UNOBSERVABLE_GPU_STATE) and 'Forbidden' in p for p in problems), problems
        if inventory.value:
            with pytest.raises(UnobservableState):
                gpu.MixGpu().resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)], kubectl = restricted,
                                           flow_id = 'vf-conformance-alloc-007')
        foreign_after = cluster.gpu_units_in_use_observed()
        assert isinstance(foreign_after, Known) and foreign_after.value == foreign_before.value
        (evidence_dir / 'api_traces.json').write_text(json.dumps({
            'restricted_occupancy': str(observed), 'inventory_completeness': [n.occupancy_known for n in inventory.value],
            'problems': problems, 'foreign_units_before': foreign_before.value, 'foreign_units_after': foreign_after.value,
        }, indent = 2, default = str))
    finally:
        subprocess.run(['kubectl', 'delete', 'clusterrole,clusterrolebinding', '-l', _RBAC_LABEL,
                        '--ignore-not-found'], check = False, capture_output = True, env = env)


@pytest.mark.case('ALLOC-007')
@pytest.mark.level('model')
@pytest.mark.variant('memory-backend')
def test_alloc_007_memory_backend_blocks_on_unknown_reads(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_007_memory(evidence)
    (evidence_dir / 'inventory_completeness.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-007')
@pytest.mark.level('model')
@pytest.mark.variant('mix-strategy')
def test_alloc_007_mix_strategy_blocks_on_unknown_reads(monkeypatch, evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_007_mix(monkeypatch, evidence)
    (evidence_dir / 'kubectl_traces.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-007')
def test_alloc_007_detects_fail_open_reads(monkeypatch) -> None:
    defects.fail_open_occupancy(monkeypatch)
    assert defects.detects(_oracle_alloc_007_memory, {})
    assert defects.detects(_oracle_alloc_007_mix, monkeypatch, {})


# -- ALLOC-008 ---------------------------------------------------------------------

def _oracle_alloc_008(monkeypatch : pytest.MonkeyPatch, evidence : Dict[str, Any]) -> None:
    pool_a = ('pool-a', dict(_PHYSICAL, **{'nvidia.com/gpu.count': '4'}), {'nvidia.com/gpu': '4'})
    pool_b = ('pool-b', dict(_PHYSICAL, **{'nvidia.com/gpu.count': '4'}), {'nvidia.com/gpu': '4'})
    outside = ('outside-sliced', _SLICED, {'nvidia.com/gpu': '4'})

    def decide(pool : List[tuple], everything : List[tuple]) -> Dict[str, Any]:
        responses = {'version': '{}', 'gpu-pool=true -o name': '\n'.join(f'node/{n[0]}' for n in pool),
                     'gpu-pool=true -o json': nodes_json(*pool), 'get nodes -o json': nodes_json(*everything)}
        monkeypatch.setattr(subprocess, 'run', fake_run(responses))
        kind = cluster.classify_gpu_resource()
        problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia', demand = {'nvidia.com/gpu': 2},
                                         max_per_pod = {'nvidia.com/gpu': 2})
        contributors = [n.get('metadata', {}).get('name') for n in (cluster._pool_nodes('kubectl') or [])]
        return {'classification': kind, 'problems': problems, 'contributing_nodes': contributors}

    # A valid multi-GPU demand against the pool, with a time-sliced node outside it.
    base = decide([pool_a, pool_b], [pool_a, pool_b, outside])
    assert base['classification'] == 'physical', base
    assert base['problems'] == [], base
    assert sorted(base['contributing_nodes']) == ['pool-a', 'pool-b']
    # Only out-of-scope nodes change: nothing about the decision may.
    flipped = ('outside-sliced', dict(_SLICED, **{'nvidia.com/gpu.sharing-strategy': 'mps'}), {'nvidia.com/gpu': '8'})
    again = decide([pool_a, pool_b], [pool_a, pool_b, flipped])
    assert again == base, (again, base)
    # A selected node's evidence changes: the decision must follow it.
    pool_a_sliced = ('pool-a', _SLICED, {'nvidia.com/gpu': '4'})
    changed = decide([pool_a_sliced, pool_b], [pool_a_sliced, pool_b, outside])
    assert changed['classification'] == 'time-sliced'
    assert any(p.startswith(gpu.IMPOSSIBLE_GPU_REQUEST) for p in changed['problems']), changed
    evidence.update({'base': base, 'outside_changed': again, 'selected_changed': changed})


@pytest.mark.case('ALLOC-008')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 5 (needs VF_K8S_GPU_NODES and the operator-applied pool label)')
def test_alloc_008_use_identical_eligibility_scope_for_gpu_classification_and() -> None:
    '''
    ALLOC-008 (P1, allocation, kubernetes): Use identical eligibility scope for GPU
    classification and capacity.

    Acceptance: The valid pool request remains valid despite outside-pool sharing;
    selected-node mutations cannot be silently ignored.
    '''


@pytest.mark.case('ALLOC-008')
@pytest.mark.level('model')
@pytest.mark.variant('kubectl-fake')
def test_alloc_008_classification_and_capacity_share_the_pool_snapshot(monkeypatch, evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_008(monkeypatch, evidence)
    (evidence_dir / 'scope_decisions.json').write_text(json.dumps(evidence, indent = 2))


@pytest.mark.negative_control(of = 'ALLOC-008')
def test_alloc_008_detects_an_unscoped_classifier(monkeypatch) -> None:
    defects.unscoped_classifier(monkeypatch)
    assert defects.detects(_oracle_alloc_008, monkeypatch, {})


# -- ALLOC-012 ---------------------------------------------------------------------

@pytest.mark.case('ALLOC-012')
@pytest.mark.level('gpu')
@pytest.mark.pending('phase 4 (managed-MIG hardware; needs VF_K8S_MIG_NODE)')
def test_alloc_012_prevent_foreign_allocations_arriving_between_planning_and() -> None:
    '''
    ALLOC-012 (P0, allocation, gpu): Prevent foreign allocations arriving between planning
    and repartitioning.

    Acceptance: No repartition ever destroys a workload admitted after planning; the
    reservation either blocks the intruder or aborts the geometry change.
    '''


# -- ALLOC-013 ---------------------------------------------------------------------

@pytest.mark.case('ALLOC-013')
@pytest.mark.level('gpu')
@pytest.mark.pending('phase 4 (managed-MIG hardware; needs VF_K8S_MIG_NODE)')
def test_alloc_013_delay_allocation_release_while_retained_or_terminating() -> None:
    '''
    ALLOC-013 (P0, allocation, gpu): Delay allocation release while retained or terminating
    workloads still use GPUs.

    Acceptance: Ownership and geometry outlive every workload that still holds a device;
    release completes only after the last holder is gone.
    '''
