'''
The in-memory allocator: correlated readiness, retained workloads, tombstones,
packing, geometry. Its Unknown-read, CAS-race, sharing-classification and
zero-mutation behaviour is the subject of the always-run ALLOC-004/007/009/023
model variants in tests/conformance, which exercise it more thoroughly.
'''
from __future__ import absolute_import, division, print_function

from videoflow.backends.allocation import (
    CLAIM_ALLOCATED,
    CLAIM_PREPARED,
    CLAIM_READY,
    RELEASE_PENDING_RECOVERY,
    RELEASE_RELEASED,
    RELEASE_STALE,
    SHARING_EXCLUSIVE,
    SHARING_ISOLATED_MIG,
    FeasiblePlan,
    WorkloadRequest,
)
from videoflow.backends.memory.allocation import (
    AUTHORITY_MANAGED_MIG,
    MemoryAllocationBackend,
    NodeFixture,
    pack_whole_devices,
)
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.mig_geometry import check_layout, smallest_profile
from videoflow.backends.outcomes import Known

H100 = 'NVIDIA-H100-80GB-HBM3'
BLACKWELL = 'NVIDIA-RTX-PRO-6000-Blackwell-Server-Edition'

def _node(name, count = 4, product = BLACKWELL, memory = 96.0, **labels):
    base = {'nvidia.com/gpu.product': product, 'nvidia.com/gpu.count': str(count),
            'nvidia.com/mig.capable': 'true', 'nvidia.com/mig.strategy': 'single',
            'nvidia.com/gpu.sharing-strategy': 'none', 'nvidia.com/gpu.replicas': '1'}
    base.update(labels)
    return NodeFixture(name, product, count, memory, base)

def _req(workload, count = 1, flow = 'flowA', sharing = SHARING_EXCLUSIVE, memory_bytes = None):
    return WorkloadRequest(flow, 'run1', workload, count, sharing, minimum_usable_memory_bytes = memory_bytes)

def _snapshot(backend, scope = None):
    obs = backend.inventory(scope or {})
    assert isinstance(obs, Known), obs
    return obs.value

# -- geometry oracle ------------------------------------------------------------------

def test_h100_regression_is_rejected_by_memory_slices_not_only_by_memory_sum():
    problems = check_layout(H100, {'1g.20gb': 4, '1g.10gb': 3})
    assert any('memory' in p for p in problems)
    assert check_layout(H100, {'1g.20gb': 4}) == []                     # 4 x 20 GiB = 80, positions 0/2/4/6
    assert check_layout(H100, {'3g.40gb': 2}) == []
    assert check_layout(H100, {'3g.40gb': 1, '4g.40gb': 1}) == []              # the classic legal split
    assert any('exceeds' in p for p in check_layout(H100, {'3g.40gb': 3}))
    assert any('memory slices' in p for p in check_layout(H100, {'3g.40gb': 2, '1g.10gb': 1}))
    assert check_layout(BLACKWELL, {'1g.24gb': 4}) == []
    assert any('exceeds' in p for p in check_layout(BLACKWELL, {'2g.48gb': 3}))
    assert smallest_profile(BLACKWELL, 30.0).name == '2g.48gb'
    assert check_layout('NVIDIA-L4', {'1g.24gb': 1}) == ['no independent geometry table for product \'NVIDIA-L4\'']

# -- packing oracle ---------------------------------------------------------------------

def test_aggregate_capacity_hides_per_host_fragmentation():
    assert pack_whole_devices([('a', 2), ('b', 2), ('c', 2)], {'h1': 3, 'h2': 3}) is None       # 6 free, 3x2 impossible
    assert pack_whole_devices([('a', 2), ('b', 2), ('c', 2)], {'h1': 2, 'h2': 2, 'h3': 2}) is not None
    assert pack_whole_devices([('a', 2), ('b', 2)], {'h1': 3, 'h2': 3}) is not None

# -- CAS ownership ---------------------------------------------------------------------------

def test_stale_rollback_cannot_remove_a_newer_owner():
    backend = MemoryAllocationBackend([_node('n1'), _node('n2')], FakeClock())
    snapshot = _snapshot(backend)
    winner = backend.reserve(backend.plan([_req('a', 4, flow = 'flowA')], snapshot), 'flowA:op', None)
    assert backend.node('n1').owner == 'flowA'
    stale = backend.release(winner.claim_id, 'flowA:old', 'gen-not-current')
    assert stale.status == RELEASE_STALE and backend.node('n1').owner == 'flowA'
    done = backend.release(winner.claim_id, 'flowA:op2', winner.desired_generation)
    assert done.status == RELEASE_RELEASED and backend.node('n1').owner is None

# -- readiness correlation and retained workloads (managed MIG) ---------------------------------

def test_stale_success_does_not_complete_a_new_geometry():
    clock = FakeClock()
    backend = MemoryAllocationBackend([_node('n1')], clock, authority = AUTHORITY_MANAGED_MIG, mig_apply_seconds = 30)
    plan = backend.plan([_req('s1', 1, sharing = SHARING_ISOLATED_MIG, memory_bytes = 20 << 30)], _snapshot(backend))
    assert isinstance(plan, FeasiblePlan) and plan.geometry == {'n1': '1g.24gb:1'}
    claim = backend.reserve(plan, 'flowA:op', None)
    assert claim.status == CLAIM_ALLOCATED                 # geometry written, not yet applied
    assert backend.node('n1').mig_state == 'pending'
    obs = backend.observe(claim.claim_id)
    assert isinstance(obs, Known) and obs.value.status == CLAIM_ALLOCATED
    clock.advance(31); backend.apply_pending_geometry()
    assert backend.observe(claim.claim_id).value.status == CLAIM_PREPARED
    backend.mark_workload_ready(claim.claim_id)
    assert backend.observe(claim.claim_id).value.status == CLAIM_READY
    assert backend.node('n1').mig_layout == {'1g.24gb': 1}

def test_release_waits_for_workloads_and_tombstones_the_shared_map():
    backend = MemoryAllocationBackend([_node('n1')], FakeClock(), authority = AUTHORITY_MANAGED_MIG)
    plan = backend.plan([_req('s1', 1, sharing = SHARING_ISOLATED_MIG, memory_bytes = 20 << 30)], _snapshot(backend))
    claim = backend.reserve(plan, 'flowA:op', None)
    entries, _, tombstone = backend.shared_config()
    assert list(entries) == [claim.evidence['n1']['mig_config']] and backend.pointer() == 'videoflow-map'
    backend.bind_workload('n1', 'flowA/s1', 1)
    held = backend.release(claim.claim_id, 'flowA:release', claim.desired_generation)
    assert held.status == RELEASE_PENDING_RECOVERY and backend.node('n1').owner == 'flowA'
    kept = backend.release(claim.claim_id, 'flowA:release', claim.desired_generation, keep_workloads = True)
    assert kept.status == RELEASE_PENDING_RECOVERY
    backend.unbind_workload('n1', 'flowA/s1')
    done = backend.release(claim.claim_id, 'flowA:release', claim.desired_generation)
    assert done.status == RELEASE_RELEASED
    entries, _, tombstone = backend.shared_config()
    assert entries == {} and tombstone and backend.pointer() == 'default-mig-parted-config'
    assert backend.node('n1').mig_config == 'all-disabled' and backend.node('n1').owner is None
    assert 'delete-map' not in {m[1] for m in backend.mutations()}
