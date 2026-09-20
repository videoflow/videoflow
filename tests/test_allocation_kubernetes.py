'''
The Kubernetes ``AcceleratorAllocationBackend`` (videoflow/deploy/allocation_kubernetes.py)
over the kubectl fake: inventory as the contract's snapshot, per-host packing
for whole-device requests, MIG planning and a reservation that writes the
geometry through ``MixGpu.apply_plan`` — with readiness judged by evidence of
*this* operation, release fenced on the claim's generation and
``keep_workloads`` retaining everything (plan Phase 4; ALLOC-003/011/013).
'''
from __future__ import absolute_import, division, print_function

import subprocess

import pytest
from support_kubectl import FakeKubectl, nodes_json, pods_json
from test_mix_strategy import _operator_responses

from videoflow.backends.allocation import (
    CLAIM_ALLOCATED,
    CLAIM_READY,
    RELEASE_PENDING_RECOVERY,
    RELEASE_RELEASED,
    RELEASE_STALE,
    SHARING_EXCLUSIVE,
    SHARING_ISOLATED_MIG,
    FeasiblePlan,
    Infeasible,
    WorkloadRequest,
)
from videoflow.backends.outcomes import Unknown
from videoflow.core.errors import OwnershipConflict
from videoflow.deploy import gpu
from videoflow.deploy.allocation_kubernetes import KubernetesAllocationBackend

GIB = 1 << 30
_GFD = {'videoflow.io/gpu-pool': 'true', 'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB',
        'nvidia.com/gpu.count': '2', 'nvidia.com/gpu.memory': '81920'}


@pytest.fixture(autouse = True)
def _no_poll_delay(monkeypatch):
    '''
    The MIG waits poll a fake that answers at once. Every wait checks its deadline
    before sleeping, so today each resolves on its first poll — but one more poll
    would cost the production 5 s, which is not what these tests measure.
    '''
    monkeypatch.setattr(gpu, 'MIG_APPLY_POLL_SECONDS', 0)


def _request(workload, count = 1, sharing = SHARING_EXCLUSIVE, memory_gib = None):
    return WorkloadRequest('flow1', 'run1', workload, count, sharing,
                           minimum_usable_memory_bytes = int(memory_gib * GIB) if memory_gib else None)


def _pool(monkeypatch, pods = None, extra = None, failing = ()):
    responses = {
        'get nodes -l videoflow.io/gpu-pool=true -o json': nodes_json(
            ('gpu-a', _GFD, {'nvidia.com/gpu': '2'}), ('gpu-b', _GFD, {'nvidia.com/gpu': '2'})),
        'get pods -A -o json': pods_json(*(pods or ())),
        **(extra or {}),
    }
    fake = FakeKubectl(responses, failing = failing)
    monkeypatch.setattr(subprocess, 'run', fake)
    return fake


def test_inventory_is_the_contracts_snapshot_of_the_pool(monkeypatch):
    _pool(monkeypatch, pods = [('gpu-b', 'Running', [{'nvidia.com/gpu': '1'}])])
    backend = KubernetesAllocationBackend('exclusive')
    snapshot = backend.inventory({}).value
    assert [(d.node, d.ordinal, d.memory_bytes) for d in snapshot.devices] == [
        ('gpu-a', 0, 80 * GIB), ('gpu-a', 1, 80 * GIB), ('gpu-b', 0, 80 * GIB), ('gpu-b', 1, 80 * GIB)]
    assert snapshot.occupancy == {'gpu-a': 0, 'gpu-b': 1}
    assert snapshot.sharing == {'gpu-a': 'physical', 'gpu-b': 'physical'}
    assert snapshot.completeness == 'complete'


def test_unreadable_pods_make_the_snapshot_partial_and_plans_nothing(monkeypatch):
    _pool(monkeypatch, failing = ('get pods -A',))
    backend = KubernetesAllocationBackend('exclusive')
    snapshot = backend.inventory({}).value
    assert snapshot.completeness == 'partial'
    outcome = backend.plan([_request('w')], snapshot)
    assert isinstance(outcome, Infeasible) and 'occupancy unknown' in outcome.reasons[0]


def test_unreadable_nodes_are_an_unknown_inventory(monkeypatch):
    _pool(monkeypatch, failing = ('get nodes',))
    assert isinstance(KubernetesAllocationBackend('exclusive').inventory({}), Unknown)


def test_exclusive_plans_per_host_and_reserves_without_writing(monkeypatch):
    fake = _pool(monkeypatch, pods = [('gpu-a', 'Running', [{'nvidia.com/gpu': '1'}])])
    backend = KubernetesAllocationBackend('exclusive')
    snapshot = backend.inventory({}).value
    # Two devices in one pod cannot straddle hosts: gpu-a has one free card, gpu-b two.
    plan = backend.plan([_request('span', 2), _request('one')], snapshot)
    assert isinstance(plan, FeasiblePlan)
    assert [d.node for d in plan.assignments['span']] == ['gpu-b', 'gpu-b']
    assert [d.node for d in plan.assignments['one']] == ['gpu-a']
    claim = backend.reserve(plan, 'flow1:run1', plan.snapshot_generation)
    assert claim.status == CLAIM_ALLOCATED and claim.evidence['written'] is False
    assert fake.mutations() == []                                  # the scheduler is the authority
    bindings = backend.bindings(claim.claim_id, 'span')
    assert bindings.container_fragment == {'resources': {'limits': {'nvidia.com/gpu': 2}}}
    assert bindings.env == {'VF_GPU_COUNT': '2', 'VF_GPU_RESOURCE_NAME': 'nvidia.com/gpu'}
    assert bindings.node_constraints['nodes'] == ['gpu-b']
    assert bindings.pod_fragment['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution'][
        'nodeSelectorTerms'][0]['matchExpressions'][0]['values'] == ['gpu-b']
    assert backend.release(claim.claim_id, 'flow1:run1', claim.desired_generation).status == RELEASE_RELEASED


def test_exclusive_cannot_place_what_does_not_fit_one_host(monkeypatch):
    _pool(monkeypatch)
    backend = KubernetesAllocationBackend('exclusive')
    outcome = backend.plan([_request('span', 3)], backend.inventory({}).value)
    assert isinstance(outcome, Infeasible) and 'per-host packing' in outcome.reasons[0]


def test_exclusive_refuses_mig_sharers_by_name(monkeypatch):
    _pool(monkeypatch)
    backend = KubernetesAllocationBackend('exclusive')
    outcome = backend.plan([_request('share', sharing = SHARING_ISOLATED_MIG, memory_gib = 10)],
                           backend.inventory({}).value)
    assert isinstance(outcome, Infeasible) and '--gpu-mode mix' in outcome.reasons[0]


def test_reserve_refuses_a_plan_from_another_snapshot(monkeypatch):
    _pool(monkeypatch)
    backend = KubernetesAllocationBackend('exclusive')
    plan = backend.plan([_request('one')], backend.inventory({}).value)
    with pytest.raises(OwnershipConflict):
        backend.reserve(plan, 'flow1:run1', 'some-other-generation')


def test_capabilities_name_the_authority():
    assert KubernetesAllocationBackend('exclusive').capabilities({}).authority == 'device-plugin'
    mix = KubernetesAllocationBackend('mix').capabilities({'kubernetes': 'v1.36.3'})
    assert mix.authority == 'managed-mig' and mix.isolated_mig and mix.version_matrix['kubernetes'] == 'v1.36.3'


def _mix_cluster(monkeypatch, manager_applies = False):
    '''One GFD-labelled A100 node with two whole cards advertised. With
    ``manager_applies`` the fake plays the MIG manager: once prepare labels the
    node with its entry, the slices appear in ``status.allocatable``.'''
    responses = {'mig\\.config\\.state': 'success', **_operator_responses()}
    fake = FakeKubectl(responses, nodes = {'gpu-a': {'labels': dict(_GFD), 'allocatable': {'nvidia.com/gpu': '2'}}})

    def run(cmd, **kwargs):
        result = fake(cmd, **kwargs)
        if manager_applies and cmd[1:3] == ['label', 'node'] and any(
                a.startswith(f'{gpu.MIG_CONFIG_LABEL}=videoflow-gpu-a-') for a in cmd):
            fake.allocatable('gpu-a').update({'nvidia.com/gpu': '1', 'nvidia.com/mig-1g.10gb': '1'})
            fake.nodes['gpu-a']['labels'][gpu.MIG_CONFIG_STATE_LABEL] = 'success'
        return result
    monkeypatch.setattr(subprocess, 'run', run)
    return fake


def test_mix_reserve_writes_the_geometry_and_readiness_is_correlated(monkeypatch):
    fake = _mix_cluster(monkeypatch)
    backend = KubernetesAllocationBackend('mix')
    snapshot = backend.inventory({}).value
    plan = backend.plan([_request('share', sharing = SHARING_ISOLATED_MIG, memory_gib = 10), _request('whole')],
                        snapshot)
    assert isinstance(plan, FeasiblePlan)
    assert plan.geometry == {'gpu-a': '1g.10gb:1'}
    (slice_,) = plan.assignments['share']
    assert (slice_.node, slice_.mig_profile) == ('gpu-a', '1g.10gb')
    assert [d.ordinal for d in plan.assignments['whole']] == [1]          # card 0 is MIG'd
    monkeypatch.setattr(gpu, 'MIG_APPLY_TIMEOUT_SECONDS', 0)
    # The stale success label and whole cards still advertised: the write goes
    # through but the geometry is not ready, which reserve reports rather than hides.
    with pytest.raises(Exception) as excinfo:
        backend.reserve(plan, 'flow1:run1', plan.snapshot_generation)
    assert 'did not become ready' in str(excinfo.value)
    assert gpu.GPU_OWNER_LABEL in fake.labels('gpu-a')                    # claimed and labelled
    assert fake.labels('gpu-a')[gpu.MIG_CONFIG_LABEL].startswith('videoflow-gpu-a-')


def test_mix_claim_becomes_ready_when_the_slices_are_advertised(monkeypatch):
    fake = _mix_cluster(monkeypatch, manager_applies = True)
    backend = KubernetesAllocationBackend('mix')
    snapshot = backend.inventory({}).value
    plan = backend.plan([_request('share', sharing = SHARING_ISOLATED_MIG, memory_gib = 10)], snapshot)
    claim = backend.reserve(plan, 'flow1:run1', plan.snapshot_generation)
    assert claim.status == CLAIM_READY and claim.evidence['nodes'] == {'gpu-a': 'ready'}
    assert claim.desired_generation == fake.labels('gpu-a')[gpu.GPU_OWNER_EPOCH_LABEL]
    bindings = backend.bindings(claim.claim_id, 'share')
    assert bindings.container_fragment == {'resources': {'limits': {'nvidia.com/mig-1g.10gb': 1}}}
    assert bindings.node_constraints['owner_label'] == {gpu.GPU_OWNER_LABEL: 'flow1'}
    # A stale generation releases nothing; keep_workloads retains everything.
    assert backend.release(claim.claim_id, 'flow1:run1', 'old').status == RELEASE_STALE
    kept = backend.release(claim.claim_id, 'flow1:run1', claim.desired_generation, keep_workloads = True)
    assert kept.status == RELEASE_PENDING_RECOVERY and kept.remaining == ('gpu-a',)
    assert gpu.GPU_OWNER_LABEL in fake.labels('gpu-a')
    assert fake.labels('gpu-a')[gpu.MIG_CONFIG_LABEL].startswith('videoflow-gpu-a-')
    # The node re-labelled by someone else: this operation's evidence is gone.
    fake.nodes['gpu-a']['labels'][gpu.GPU_OWNER_EPOCH_LABEL] = 'other'
    assert backend.observe(claim.claim_id).value.status == 'failed'


def test_exclusive_consumes_static_mig_slices_without_touching_geometry(monkeypatch):
    '''ALLOC-017: an administrator's slices are used as advertised — a free matching
    one is granted, the foreign one and the geometry are left alone, nothing is written.'''
    sliced = dict(_GFD, **{'nvidia.com/mig.config': 'all-1g.10gb', 'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB-MIG-1g.10gb'})
    responses = {
        'get nodes -l videoflow.io/gpu-pool=true -o json': nodes_json(
            ('gpu-a', _GFD, {'nvidia.com/gpu': '2'}), ('gpu-m', sliced, {'nvidia.com/mig-1g.10gb': '2'})),
        'get pods -A -o json': pods_json(('gpu-m', 'Running', [{'nvidia.com/mig-1g.10gb': '1'}])),
    }
    fake = FakeKubectl(responses)
    monkeypatch.setattr(subprocess, 'run', fake)
    backend = KubernetesAllocationBackend('exclusive')
    snapshot = backend.inventory({}).value
    assert snapshot.sharing['gpu-m'] == 'mig'
    plan = backend.plan([_request('s', sharing = SHARING_ISOLATED_MIG, memory_gib = 10), _request('w')], snapshot)
    assert isinstance(plan, FeasiblePlan)
    (slice_,) = plan.assignments['s']
    assert (slice_.node, slice_.mig_profile, slice_.memory_bytes) == ('gpu-m', '1g.10gb', 10 * GIB)
    assert [d.node for d in plan.assignments['w']] == ['gpu-a']              # whole cards never on the MIG'd node
    claim = backend.reserve(plan, 'flow1:run1', plan.snapshot_generation)
    assert claim.evidence['written'] is False and fake.mutations() == []
    assert backend.bindings(claim.claim_id, 's').container_fragment == {'resources': {'limits': {'nvidia.com/mig-1g.10gb': 1}}}
    # Two more sharers: only one free slice exists.
    outcome = backend.plan([_request(w, sharing = SHARING_ISOLATED_MIG, memory_gib = 10) for w in 'st'], snapshot)
    assert isinstance(outcome, Infeasible) and 't: no pool node advertises a free MIG slice' in outcome.reasons[0]
