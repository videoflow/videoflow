'''The in-memory allocator: Unknown reads, CAS ownership, correlated readiness, retained workloads, tombstones, packing, geometry.'''
from __future__ import absolute_import, division, print_function

import threading

from videoflow.backends import faults
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
    Infeasible,
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
from videoflow.backends.outcomes import Known, Unknown
from videoflow.core.errors import OwnershipConflict

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

# -- unknown reads ------------------------------------------------------------------------

def test_failed_pod_listing_is_not_idle_capacity():
    backend = MemoryAllocationBackend([_node('n1')], FakeClock())
    backend.bind_workload('n1', 'foreign', 4)
    backend.fail_reads('pods')
    snapshot = _snapshot(backend)
    assert snapshot.completeness == 'partial' and snapshot.occupancy == {}
    plan = backend.plan([_req('w', 1)], snapshot)
    assert isinstance(plan, Infeasible) and 'occupancy unknown' in plan.reasons[0]
    backend.fail_reads('pods', None)
    plan2 = backend.plan([_req('w', 1)], _snapshot(backend))
    assert isinstance(plan2, Infeasible)                 # the foreign workload really holds all four
    backend.fail_reads('nodes')
    assert isinstance(backend.inventory({}), Unknown)

# -- CAS ownership ---------------------------------------------------------------------------

def test_two_reservers_race_and_exactly_one_wins():
    backend = MemoryAllocationBackend([_node('n1')], FakeClock())
    snapshot = _snapshot(backend)
    plan_a = backend.plan([_req('a', 2, flow = 'flowA')], snapshot)
    plan_b = backend.plan([_req('b', 2, flow = 'flowB')], snapshot)
    assert isinstance(plan_a, FeasiblePlan) and isinstance(plan_b, FeasiblePlan)
    results = {}
    schedule = faults.FaultSchedule({'owner.update.before': faults.Pause('both-read', timeout_seconds = 10)})
    schedule.install()
    def run(name, plan):
        try:
            results[name] = backend.reserve(plan, f'{name}:op', None)
        except OwnershipConflict as e:
            results[name] = e
    ta = threading.Thread(target = run, args = ('flowA', plan_a)); ta.start()
    tb = threading.Thread(target = run, args = ('flowB', plan_b)); tb.start()
    schedule.release('both-read')
    ta.join(5); tb.join(5)
    schedule.uninstall()
    kinds = sorted(type(v).__name__ for v in results.values())
    assert kinds == ['ClaimObservation', 'OwnershipConflict'], results
    assert backend.node('n1').owner in ('flowA', 'flowB')
    assert len(backend.mutations(['stamp-owner'])) == 1

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

def test_unknown_config_read_keeps_ownership_for_retry():
    backend = MemoryAllocationBackend([_node('n1')], FakeClock(), authority = AUTHORITY_MANAGED_MIG)
    plan = backend.plan([_req('s1', 1, sharing = SHARING_ISOLATED_MIG, memory_bytes = 20 << 30)], _snapshot(backend))
    claim = backend.reserve(plan, 'flowA:op', None)
    backend.fail_reads('config', 'auth')
    out = backend.release(claim.claim_id, 'flowA:release', claim.desired_generation)
    assert out.status == RELEASE_PENDING_RECOVERY and backend.node('n1').owner == 'flowA'
    backend.fail_reads('config', None)
    assert backend.release(claim.claim_id, 'flowA:release', claim.desired_generation).status == RELEASE_RELEASED

def test_sharing_classification_blocks_exclusive_claims_on_shared_pools():
    ts = _node('ts', **{'nvidia.com/gpu.sharing-strategy': 'time-slicing', 'nvidia.com/gpu.replicas': '4'})
    mps = _node('mps', **{'nvidia.com/gpu.sharing-strategy': 'mps'})
    whole = _node('whole')
    backend = MemoryAllocationBackend([ts, mps, whole], FakeClock())
    snapshot = _snapshot(backend)
    assert snapshot.sharing == {'ts': 'time-sliced', 'mps': 'mps', 'whole': 'physical'}
    plan = backend.plan([_req('a', 2)], snapshot)
    assert isinstance(plan, FeasiblePlan) and all(d.node == 'whole' for d in plan.assignments['a'])
    only_shared = MemoryAllocationBackend([ts, mps], FakeClock())
    assert isinstance(only_shared.plan([_req('a', 2)], _snapshot(only_shared)), Infeasible)

def test_zero_mutation_audit_for_rejected_plans():
    backend = MemoryAllocationBackend([_node('n1', count = 1)], FakeClock())
    assert isinstance(backend.plan([_req('a', 2)], _snapshot(backend)), Infeasible)
    assert backend.mutations() == []
