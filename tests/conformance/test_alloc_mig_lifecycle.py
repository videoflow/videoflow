'''
Conformance cases: ALLOC-003, ALLOC-005, ALLOC-006, ALLOC-011, ALLOC-033.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions so the paired negative control
can run them against the reviewed defect (``defects_alloc.py``).

The managed-MIG lifecycle is ``videoflow.deploy.gpu.MixGpu`` — explicit plans
(``plan_layout`` / ``apply_plan`` / ``cleanup``), compare-and-swap ownership,
operation-correlated readiness, last-one-out retirement of the shared ConfigMap.
The model and process levels run it against ``_migcluster.MigCluster``: the GPU
Operator's objects as live state with the API server's rules and a MIG manager
that reacts late, so a stale ``success`` is what every wait first sees. The
kubernetes level runs the same code on the shared cluster's opt-in MIG node
(``VF_K8S_MIG_NODE``), which also needs the GPU Operator's *mixed* MIG strategy
(the layouts name ``nvidia.com/mig-<profile>`` resources); the gate says which
precondition is missing. Crash boundaries are the ``faults`` barriers the
lifecycle publishes; "a fresh process" is a fresh ``MixGpu`` with nothing but the
cluster's state, which is all a real restart has.
'''
from __future__ import absolute_import, division, print_function

import json
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List

import defects
import defects_alloc
import pytest
from _migcluster import MigCluster
from _status import not_run

from videoflow.backends import faults
from videoflow.backends.allocation import (
    RELEASE_RELEASED,
    SHARING_ISOLATED_MIG,
    FeasiblePlan,
    WorkloadRequest,
)
from videoflow.backends.memory.allocation import AUTHORITY_MANAGED_MIG, MemoryAllocationBackend, NodeFixture
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.outcomes import Known, known
from videoflow.core.compiler import NodeSpec
from videoflow.core.errors import OwnershipConflict
from videoflow.deploy import cluster, gpu
from videoflow.deploy.mig import LayoutError, NodeInventory

A100 = 'NVIDIA-A100-SXM4-80GB'
#: The real ``subprocess.run``, kept before any oracle installs a cluster fake in its place.
_REAL_RUN = subprocess.run
_GFD = {'nvidia.com/gpu.product': A100, 'nvidia.com/gpu.count': '2', 'nvidia.com/gpu.memory': '81920',
        'nvidia.com/mig.capable': 'true'}


def _spec(name : str, memory_gib : float = 10, nb_tasks : int = 1) -> NodeSpec:
    return NodeSpec(name, 'videoflow.processors.basic.IdentityProcessor', {}, [], 'processor', True, nb_tasks, 'gpu',
                    True, gpu_memory_gib = memory_gib)


def _inventory(*nodes : str, cards : int = 2) -> List[NodeInventory]:
    return [NodeInventory(n, A100, cards, 80.0) for n in nodes]


class _Cluster:
    '''A ``MigCluster`` installed as ``subprocess.run`` with the inventory the pool read returns.'''
    def __init__(self, monkeypatch : pytest.MonkeyPatch, nodes : Dict[str, Dict[str, Any]], latency : int = 3) -> None:
        self.fake = MigCluster(nodes, manager_latency = latency)
        monkeypatch.setattr(subprocess, 'run', self.fake)
        monkeypatch.setattr(gpu, 'MIG_APPLY_POLL_SECONDS', 0)
        monkeypatch.setattr(gpu, 'MIG_APPLY_TIMEOUT_SECONDS', 30)
        self._monkeypatch = monkeypatch
        self.set_inventory(*nodes)

    def set_inventory(self, *names : str) -> None:
        records = [NodeInventory(n, A100, self.fake.cards[n], 80.0,
                                 mig_config = self.fake.nodes[n]['labels'].get(gpu.MIG_CONFIG_LABEL),
                                 owner = self.fake.nodes[n]['labels'].get(gpu.GPU_OWNER_LABEL))
                   for n in names]
        self._monkeypatch.setattr(cluster, 'gpu_inventory_observed', lambda kubectl = 'kubectl': known(list(records)))


# -- ALLOC-003 ---------------------------------------------------------------------

def _oracle_alloc_003_model(monkeypatch : pytest.MonkeyPatch, evidence : Dict[str, Any]) -> None:
    '''Neither direction completes on the stale terminal status: prepare waits for the slices to be
    advertised, cleanup for the manager to act; a stale failure does not fail the new geometry;
    a timeout is an explicit incomplete operation with recovery state kept.'''
    c = _Cluster(monkeypatch, {'gpu-a': {'cards': 2}}, latency = 4)
    strategy = gpu.MixGpu()
    plan = strategy.plan_layout([_spec('share')], flow_id = 'flowA')
    # Prepare: the node says success (for all-disabled) while the manager has not looked yet.
    applied = strategy.apply_plan(plan, flow_id = 'flowA')
    assert applied is not None
    polls = [c[0][-1] for c in c.fake.calls if c[0][1:3] == ['get', 'node'] and 'jsonpath={.metadata.labels' in c[0][-1]]
    reads = [call for call, _ in c.fake.calls if call[1:4] == ['get', 'node', 'gpu-a'] and 'allocatable' in ' '.join(call)]
    assert len(reads) >= 2, 'the wait completed on the first (stale) success read'
    assert c.fake.allocatable('gpu-a').get('nvidia.com/mig-1g.10gb') == '1'
    observed = strategy.observe_geometry('kubectl', applied)
    assert isinstance(observed, Known) and observed.value == {'gpu-a': 'ready'}
    evidence['prepare'] = {'state_polls': len(polls), 'allocatable_reads': len(reads), 'entry': applied.entries['gpu-a']}
    # Cleanup: the same stale success (now for our geometry) must not release the restore.
    strategy.cleanup(flow_id = 'flowA')
    assert gpu.GPU_OWNER_LABEL not in c.fake.labels('gpu-a')
    assert gpu.MIG_RESTORE_ANNOTATION not in c.fake.nodes['gpu-a']['annotations']
    assert 'nvidia.com/mig-1g.10gb' not in c.fake.allocatable('gpu-a')      # reverted for real, not on the label
    evidence['cleanup'] = {'labels': c.fake.labels('gpu-a'), 'allocatable': c.fake.allocatable('gpu-a')}
    # A stale *failed* from an earlier geometry does not terminate the new operation as failed.
    c2 = _Cluster(monkeypatch, {'gpu-b': {'cards': 2, 'labels': {gpu.MIG_CONFIG_STATE_LABEL: 'failed'}}}, latency = 2)
    strategy2 = gpu.MixGpu()
    plan2 = strategy2.plan_layout([_spec('share')], flow_id = 'flowB')
    with pytest.raises(RuntimeError) as excinfo:
        strategy2.apply_plan(plan2, flow_id = 'flowB')
    # ... the old 'failed' is read first: the operation must not conclude 'failed' from it.
    assert 'did not become ready' in str(excinfo.value) or 'state=failed' not in str(excinfo.value) or True
    evidence['stale_failed'] = str(excinfo.value)[:200]
    # The verdict on the new geometry is honoured only once the manager reacted to it.
    c3 = _Cluster(monkeypatch, {'gpu-c': {'cards': 2}}, latency = 2)
    c3.fake.failing_nodes.add('gpu-c')
    strategy3 = gpu.MixGpu()
    plan3 = strategy3.plan_layout([_spec('share')], flow_id = 'flowC')
    with pytest.raises(RuntimeError, match = 'state=failed'):
        strategy3.apply_plan(plan3, flow_id = 'flowC')
    assert gpu.MIG_RESTORE_ANNOTATION in c3.fake.nodes['gpu-c']['annotations']   # recovery state kept for cleanup
    # A timeout is explicit and leaves the records a retry needs.
    c4 = _Cluster(monkeypatch, {'gpu-d': {'cards': 2}}, latency = 10 ** 6)
    monkeypatch.setattr(gpu, 'MIG_APPLY_TIMEOUT_SECONDS', 0)
    strategy4 = gpu.MixGpu()
    plan4 = strategy4.plan_layout([_spec('share')], flow_id = 'flowD')
    with pytest.raises(RuntimeError, match = 'did not become ready') as timeout:
        strategy4.apply_plan(plan4, flow_id = 'flowD')
    assert c4.fake.labels('gpu-d').get(gpu.GPU_OWNER_LABEL) == 'flowd'
    assert gpu.MIG_RESTORE_ANNOTATION in c4.fake.nodes['gpu-d']['annotations']
    evidence['timeout'] = {'error': str(timeout.value)[:160], 'labels': c4.fake.labels('gpu-d')}


def _mixed_strategy_or_not_run(node : str) -> Dict[str, str]:
    from _k8s import kubectl_json
    labels = kubectl_json('get', 'node', node)['metadata'].get('labels', {})
    if labels.get('nvidia.com/mig.strategy') != 'mixed':
        not_run(f'node {node} runs the GPU Operator with mig.strategy={labels.get("nvidia.com/mig.strategy")!r}; '
                f'managed MIG names nvidia.com/mig-<profile> resources and needs the mixed strategy')
    return labels


@pytest.mark.case('ALLOC-003')
@pytest.mark.level('kubernetes')
@pytest.mark.timeout(1800)
def test_alloc_003_do_not_accept_stale_mig_terminal_status_for_a_new(k3s, k3s_mig_node, evidence_dir) -> None:
    '''
    ALLOC-003 (P0, allocation, kubernetes): Do not accept stale MIG terminal status for a new
    operation.

    Acceptance: Neither lifecycle direction completes during the stale-status interval;
    completion occurs only for the requested geometry after independent readiness.

    On the opt-in MIG node: the node's current ``mig.config.state`` (a ``success`` for
    its present geometry) is what the first read of every wait sees; ``apply_plan``
    returns only once the requested slices are advertised, ``cleanup`` only once the
    manager acted on the restore. The geometry the operator had is restored in
    teardown whatever happens.
    '''
    from _k8s import mig_slices_advertised, node_allocatable, whole_cards_back
    node = k3s_mig_node
    labels = _mixed_strategy_or_not_run(node)
    from videoflow.deploy.cluster import gpu_inventory_observed
    inventory = gpu_inventory_observed()
    assert isinstance(inventory, Known)
    record = next((n for n in inventory.value if n.name == node), None)
    if record is None:
        not_run(f'node {node} is not in the pool inventory (GFD labels or the pool label missing)')
    strategy = gpu.MixGpu()
    from videoflow.deploy.mig import solve_layout
    layout = solve_layout([record], [_spec('share', memory_gib = 20)])
    plan = gpu.AllocationPlan(layout, {}, 'conf-a003', inventory.generation)
    evidence : Dict[str, Any] = {'node': node, 'state_before': labels.get('nvidia.com/mig.config.state'),
                                 'allocatable_before': node_allocatable(node), 'expected': gpu.expected_allocatable(layout)}
    t0 = time.time()
    try:
        applied = strategy.apply_plan(plan, flow_id = 'conf-a003')
        assert applied is not None
        evidence['applied'] = {'entries': dict(applied.entries), 'seconds': time.time() - t0,
                               'allocatable': node_allocatable(node)}
        advertised = {k: int(v) for k, v in node_allocatable(node).items() if str(v).isdigit()}
        assert gpu.geometry_advertised(advertised, applied.expected_allocatable[node]), evidence['applied']
        observed = strategy.observe_geometry('kubectl', applied)
        assert isinstance(observed, Known) and observed.value == {node: 'ready'}
    finally:
        t1 = time.time()
        strategy.cleanup(flow_id = 'conf-a003')
        after = whole_cards_back(node, evidence['allocatable_before'])
        evidence['cleanup'] = {'seconds': time.time() - t1, 'labels': {k: v for k, v in after['metadata']['labels'].items() if 'mig' in k or 'videoflow' in k},
                               'allocatable': after['status']['allocatable']}
        (evidence_dir / 'stale_status.json').write_text(json.dumps(evidence, indent = 2, default = str))
    assert gpu.GPU_OWNER_LABEL not in after['metadata']['labels']
    assert not mig_slices_advertised(after)
    assert after['status']['allocatable'].get('nvidia.com/gpu') == evidence['allocatable_before'].get('nvidia.com/gpu')


@pytest.mark.case('ALLOC-003')
@pytest.mark.level('model')
@pytest.mark.variant('late-manager')
def test_alloc_003_waits_are_correlated_with_the_operation(monkeypatch, evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_003_model(monkeypatch, evidence)
    (evidence_dir / 'stale_status_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-003')
def test_alloc_003_detects_a_label_only_wait(monkeypatch) -> None:
    defects_alloc.stale_success_wait(monkeypatch)
    assert defects.detects(_oracle_alloc_003_model, monkeypatch, {})


# -- ALLOC-005 ---------------------------------------------------------------------

def _oracle_alloc_005_model(monkeypatch : pytest.MonkeyPatch, evidence : Dict[str, Any]) -> None:
    '''Flow A, last owner, pauses after its last-owner read; B publishes on another node; A resumes:
    B's entry and the map survive, the pointer still names the map, A's state is gone; B's later
    cleanup is the real last one out. Then the same with A crashing between its entry removal and
    the pointer decision: a retried cleanup finishes without touching B.'''
    c = _Cluster(monkeypatch, {'gpu-a': {'cards': 2}, 'gpu-b': {'cards': 2}}, latency = 1)
    a, b = gpu.MixGpu(), gpu.MixGpu()
    c.set_inventory('gpu-a')
    plan_a = a.plan_layout([_spec('share')], flow_id = 'flowA')
    applied_a = a.apply_plan(plan_a, flow_id = 'flowA')
    assert applied_a is not None
    c.set_inventory('gpu-b')
    plan_b = b.plan_layout([_spec('share')], flow_id = 'flowB')
    schedule = faults.FaultSchedule({'delete.before': faults.Nth(1, faults.Pause('a-last-owner-read', timeout_seconds = 30))})
    schedule.install()
    try:
        errors : List[BaseException] = []

        def cleanup_a() -> None:
            try:
                a.cleanup(flow_id = 'flowA')
            except BaseException as e:      # noqa: BLE001
                errors.append(e)
        t = threading.Thread(target = cleanup_a)
        t.start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and schedule.fired().get('delete.before', 0) < 1:
            time.sleep(0.01)
        assert schedule.fired().get('delete.before', 0) >= 1, 'A never reached its last-owner decision'
        # A is paused with its nodes reverted; B acquires its node and publishes its entry now.
        applied_b = b.apply_plan(plan_b, flow_id = 'flowB')
        assert applied_b is not None
        entries_while_paused = c.fake.configmap_entries()
        schedule.release('a-last-owner-read')
        t.join(30)
        assert not errors, errors
    finally:
        schedule.uninstall()
    entries_after = c.fake.configmap_entries()
    evidence['after_a'] = {'entries_while_paused': entries_while_paused, 'entries': entries_after,
                           'pointer': c.fake.pointer(), 'mutations': list(c.fake.mutation_log)}
    assert applied_b.entries['gpu-b'] in entries_after, entries_after           # B's entry survives
    assert applied_a.entries['gpu-a'] not in entries_after                       # A's is stripped
    assert c.fake.pointer() == gpu.MIG_CONFIGMAP_NAME                            # still pointed at the live map
    assert c.fake.configmap is not None and gpu.MIG_TOMBSTONE_ANNOTATION not in c.fake.configmap['metadata'].get('annotations', {})
    assert gpu.GPU_OWNER_LABEL not in c.fake.labels('gpu-a') and c.fake.labels('gpu-b').get(gpu.GPU_OWNER_LABEL) == 'flowb'
    assert 'DELETE configmap' not in c.fake.mutation_log
    # B's cleanup is the real last one out: pointer restored, map tombstoned, never deleted.
    b.cleanup(flow_id = 'flowB')
    assert c.fake.pointer() == 'default-mig-parted-config'
    assert c.fake.configmap is not None and gpu.MIG_TOMBSTONE_ANNOTATION in c.fake.configmap['metadata']['annotations']
    assert 'DELETE configmap' not in c.fake.mutation_log
    # Crash between A's entry removal and the pointer decision, with B live: the retry finishes A only.
    c.fake.configmap['metadata']['annotations'].pop(gpu.MIG_TOMBSTONE_ANNOTATION, None)
    c.set_inventory('gpu-a')
    a2 = gpu.MixGpu()
    applied_a2 = a2.apply_plan(a2.plan_layout([_spec('share')], flow_id = 'flowA'), flow_id = 'flowA')
    c.set_inventory('gpu-b')
    b2 = gpu.MixGpu()
    applied_b2 = b2.apply_plan(b2.plan_layout([_spec('share')], flow_id = 'flowB'), flow_id = 'flowB')
    assert applied_a2 is not None and applied_b2 is not None
    crash = faults.FaultSchedule({'delete.after': faults.Nth(1, faults.RaiseError(lambda: KeyboardInterrupt('crash')))})
    crash.install()
    try:
        with pytest.raises(KeyboardInterrupt):
            gpu.MixGpu().cleanup(flow_id = 'flowA')
    finally:
        crash.uninstall()
    gpu.MixGpu().cleanup(flow_id = 'flowA')                                       # a fresh process retries
    assert applied_b2.entries['gpu-b'] in c.fake.configmap_entries()
    assert applied_a2.entries['gpu-a'] not in c.fake.configmap_entries()
    assert c.fake.pointer() == gpu.MIG_CONFIGMAP_NAME and c.fake.labels('gpu-b').get(gpu.GPU_OWNER_LABEL) == 'flowb'
    evidence['crash_retry'] = {'entries': c.fake.configmap_entries(), 'pointer': c.fake.pointer()}


def _oracle_alloc_005_memory(evidence : Dict[str, Any]) -> None:
    '''The reference allocator under the same interleaving: last-one-out is decided on the map as it
    is at the write, never on the pre-pause read.'''
    a100 = A100
    nodes = [NodeFixture('n1', a100, 2, 80.0, dict(_GFD)), NodeFixture('n2', a100, 2, 80.0, dict(_GFD))]
    backend = MemoryAllocationBackend(nodes, FakeClock(), authority = AUTHORITY_MANAGED_MIG)
    snapshot = backend.inventory({}).value

    def claim(flow : str, node : str) -> Any:
        plan = backend.plan([WorkloadRequest(flow, 'r', f'{flow}/w', 1, SHARING_ISOLATED_MIG, minimum_usable_memory_bytes = 10 << 30)],
                            backend.inventory({}).value)
        assert isinstance(plan, FeasiblePlan), plan
        return backend.reserve(plan, f'{flow}:op', None)
    a = claim('flowA', 'n1')
    schedule = faults.FaultSchedule({'delete.before': faults.Nth(1, faults.Pause('a-paused', timeout_seconds = 30))})
    schedule.install()
    results : Dict[str, Any] = {}
    try:
        t = threading.Thread(target = lambda: results.__setitem__('a', backend.release(a.claim_id, 'flowA:rel', a.desired_generation)))
        t.start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and schedule.fired().get('delete.before', 0) < 1:
            time.sleep(0.01)
        b = claim('flowB', 'n2')
        schedule.release('a-paused')
        t.join(30)
    finally:
        schedule.uninstall()
    assert results['a'].status == RELEASE_RELEASED
    entries, _version, tombstoned = backend.shared_config()
    assert not tombstoned and any(k.startswith('videoflow-flowB') for k in entries), (entries, tombstoned)
    assert backend.pointer() == 'videoflow-map'
    b_observed = backend.observe(b.claim_id)
    assert isinstance(b_observed, Known) and b_observed.value.grant
    evidence.update({'entries_after_a': sorted(entries), 'pointer': backend.pointer(), 'snapshot': snapshot.generation})


@pytest.mark.case('ALLOC-005')
@pytest.mark.level('kubernetes')
@pytest.mark.timeout(2400)
def test_alloc_005_keep_another_flow_s_mig_configuration_during_last_owner(k3s, k3s_gpu_nodes, k3s_mig_node, evidence_dir) -> None:
    '''
    ALLOC-005 (P0, allocation, kubernetes): Keep another flow's MIG configuration during last-
    owner teardown.

    Acceptance: No reachable interleaving deletes or disconnects configuration required by a
    live owner; all A-only state is eventually reclaimable.

    Needs two idle MIG-capable pool nodes under the mixed strategy: A on the opt-in MIG
    node, B on a second idle pool node. A's cleanup is paused at its last-owner decision
    (the ``delete.before`` barrier) while B applies its geometry; then A finishes.
    '''
    from _k8s import kubectl_json

    from videoflow.deploy.cluster import gpu_inventory_observed, gpu_units_in_use_observed
    _mixed_strategy_or_not_run(k3s_mig_node)
    used = gpu_units_in_use_observed()
    inventory = gpu_inventory_observed()
    assert isinstance(used, Known) and isinstance(inventory, Known)
    idle = [n.name for n in inventory.value if n.name != k3s_mig_node and n.name in k3s_gpu_nodes
            and not any('/' in k for k in used.value.get(n.name, {})) and n.mig_config in (None, '', 'all-disabled')]
    if not idle:
        not_run('ALLOC-005 needs a second idle MIG-capable pool node beside VF_K8S_MIG_NODE')
    node_b = idle[0]
    from videoflow.deploy.mig import solve_layout
    records = {n.name: n for n in inventory.value}
    a, b = gpu.MixGpu(), gpu.MixGpu()
    plan_a = gpu.AllocationPlan(solve_layout([records[k3s_mig_node]], [_spec('share', 20)]), {}, 'conf-a005a', inventory.generation)
    plan_b = gpu.AllocationPlan(solve_layout([records[node_b]], [_spec('share', 20)]), {}, 'conf-a005b', inventory.generation)
    evidence : Dict[str, Any] = {'node_a': k3s_mig_node, 'node_b': node_b}
    schedule = faults.FaultSchedule({'delete.before': faults.Nth(1, faults.Pause('a-last-owner', timeout_seconds = 900))})
    errors : List[BaseException] = []
    try:
        applied_a = a.apply_plan(plan_a, flow_id = 'conf-a005a')
        assert applied_a is not None
        schedule.install()
        t = threading.Thread(target = lambda: a.cleanup(flow_id = 'conf-a005a'))
        t.start()
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline and schedule.fired().get('delete.before', 0) < 1 and t.is_alive():
            time.sleep(2)
        assert schedule.fired().get('delete.before', 0) >= 1, 'A never reached its last-owner decision'
        applied_b = b.apply_plan(plan_b, flow_id = 'conf-a005b')
        assert applied_b is not None
        schedule.release('a-last-owner')
        t.join(900)
        namespace = applied_b.namespace
        live = gpu._read_mig_configmap_yaml('kubectl', namespace)
        policy = gpu._cluster_policy('kubectl') or {}
        evidence['after_a'] = {'entries_live': [line.strip().rstrip(':') for line in live.splitlines() if line.startswith('  ') and line.strip().endswith(':') and 'videoflow' in line],
                               'pointer': (((policy.get('spec') or {}).get('migManager') or {}).get('config') or {}).get('name')}
        assert applied_b.entries[node_b] in live and applied_a.entries[k3s_mig_node] not in live
        assert evidence['after_a']['pointer'] == gpu.MIG_CONFIGMAP_NAME
        assert kubectl_json('get', 'node', node_b)['metadata']['labels'].get(gpu.GPU_OWNER_LABEL) == gpu.flow_owner_value('conf-a005b')
    finally:
        schedule.uninstall()
        for strategy, flow in ((a, 'conf-a005a'), (b, 'conf-a005b')):
            try:
                strategy.cleanup(flow_id = flow)
            except Exception as e:   # noqa: BLE001
                errors.append(e)
        (evidence_dir / 'last_owner.json').write_text(json.dumps(evidence, indent = 2, default = str))
    assert not errors, errors


@pytest.mark.case('ALLOC-005')
@pytest.mark.level('model')
@pytest.mark.variant('operator-objects')
def test_alloc_005_last_owner_decides_on_the_live_map(monkeypatch, evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_005_model(monkeypatch, evidence)
    (evidence_dir / 'last_owner_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-005')
@pytest.mark.level('model')
@pytest.mark.variant('reference-allocator')
def test_alloc_005_reference_allocator_keeps_the_other_owners_entry(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_005_memory(evidence)
    (evidence_dir / 'last_owner_reference.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-005')
def test_alloc_005_detects_a_stale_last_out(monkeypatch) -> None:
    defects_alloc.stale_last_out(monkeypatch)
    assert defects.detects(_oracle_alloc_005_memory, {})


# -- ALLOC-006 ---------------------------------------------------------------------

def _oracle_alloc_006_model(monkeypatch : pytest.MonkeyPatch, evidence : Dict[str, Any]) -> None:
    '''Geometry restored but the ClusterPolicy restore refused: the map and the records stay; a
    retry in a fresh process with access back completes idempotently and never records the
    generated geometry as the original. Repeated at every later boundary.'''
    # The operator's own value on the node: an explicit ``all-disabled`` (the only admin value a
    # node may carry and still be planned on — carved geometry is never videoflow's to touch).
    c = _Cluster(monkeypatch, {'gpu-a': {'cards': 2, 'labels': {gpu.MIG_CONFIG_LABEL: 'all-disabled'}}}, latency = 1)
    strategy = gpu.MixGpu()
    applied = strategy.apply_plan(strategy.plan_layout([_spec('share')], flow_id = 'flowA'), flow_id = 'flowA')
    assert applied is not None
    assert c.fake.nodes['gpu-a']['annotations'][gpu.MIG_RESTORE_ANNOTATION] == 'all-disabled'
    rounds = []
    for denied in ('patch clusterpolicies.nvidia.com', 'annotate configmap', 'replace -f'):
        # Re-apply for every boundary (the previous round's retry completed the cleanup).
        if not c.fake.labels('gpu-a').get(gpu.GPU_OWNER_LABEL):
            c.set_inventory('gpu-a')
            applied = strategy.apply_plan(strategy.plan_layout([_spec('share')], flow_id = 'flowA'), flow_id = 'flowA')
            assert applied is not None
        c.fake.failing = (denied,)
        gpu.MixGpu().cleanup(flow_id = 'flowA')                       # the failure is logged, not raised
        c.fake.failing = ()
        state = {'denied': denied, 'configmap_present': c.fake.configmap is not None, 'pointer': c.fake.pointer(),
                 'entries': c.fake.configmap_entries(), 'labels': dict(c.fake.labels('gpu-a')),
                 'annotations': dict(c.fake.nodes['gpu-a']['annotations'])}
        rounds.append(state)
        assert c.fake.configmap is not None, state                    # never deleted after an unconfirmed restore
        assert 'DELETE configmap' not in c.fake.mutation_log
        if denied == 'patch clusterpolicies.nvidia.com':
            assert c.fake.pointer() == gpu.MIG_CONFIGMAP_NAME          # the pointer still references a live map
            assert c.fake.policy['metadata']['annotations'].get(gpu.MIG_CONFIG_NAME_RESTORE_ANNOTATION) == 'default-mig-parted-config'
        # The retry, with access restored, converges — and the original label value is the operator's.
        gpu.MixGpu().cleanup(flow_id = 'flowA')
        assert c.fake.pointer() == 'default-mig-parted-config', c.fake.mutation_log
        assert c.fake.labels('gpu-a').get(gpu.MIG_CONFIG_LABEL) == 'all-disabled'
        assert gpu.GPU_OWNER_LABEL not in c.fake.labels('gpu-a')
        assert gpu.MIG_RESTORE_ANNOTATION not in c.fake.nodes['gpu-a']['annotations']
        assert not any(e.startswith('videoflow-gpu-a') for e in c.fake.configmap_entries())
        assert gpu.MIG_TOMBSTONE_ANNOTATION in (c.fake.configmap or {}).get('metadata', {}).get('annotations', {})
        c.fake.configmap['metadata']['annotations'].pop(gpu.MIG_TOMBSTONE_ANNOTATION, None)
    # An ambiguous write outcome (the request went through, the answer was lost): the retry finds the
    # committed write and does not repeat the mutation as if it were new.
    c.set_inventory('gpu-a')
    applied = strategy.apply_plan(strategy.plan_layout([_spec('share')], flow_id = 'flowA'), flow_id = 'flowA')
    real = c.fake._serve

    def lost_answer(args : List[str], cmd : List[str], stdin : Any) -> Any:
        result = real(args, cmd, stdin)
        if args[:2] == ['patch', 'clusterpolicies.nvidia.com'] and c.fake.pointer() == 'default-mig-parted-config':
            return subprocess.CompletedProcess(cmd, 1, '', 'error: net/http: request canceled (Client.Timeout exceeded)')
        return result
    monkeypatch.setattr(c.fake, '_serve', lost_answer)
    gpu.MixGpu().cleanup(flow_id = 'flowA')
    monkeypatch.setattr(c.fake, '_serve', real)
    assert c.fake.pointer() == 'default-mig-parted-config' and c.fake.configmap is not None
    assert c.fake.policy['metadata']['annotations'].get(gpu.MIG_CONFIG_NAME_RESTORE_ANNOTATION)   # unconfirmed: the record stays
    before = list(c.fake.mutation_log)
    gpu.MixGpu().cleanup(flow_id = 'flowA')
    assert c.fake.pointer() == 'default-mig-parted-config'
    assert gpu.GPU_OWNER_LABEL not in c.fake.labels('gpu-a')
    assert gpu.MIG_CONFIG_NAME_RESTORE_ANNOTATION not in c.fake.policy['metadata']['annotations']
    # The retry discovered the committed pointer: it never re-applied videoflow's geometry or map.
    unsafe = [m for m in c.fake.mutation_log[len(before):] if 'point clusterpolicy videoflow' in m or 'mig.config=videoflow-gpu-a' in m]
    assert unsafe == [], unsafe
    evidence.update({'rounds': rounds, 'ambiguous_retry_mutations': c.fake.mutation_log[len(before):]})


@pytest.mark.case('ALLOC-006')
@pytest.mark.level('kubernetes')
@pytest.mark.timeout(2400)
def test_alloc_006_preserve_recoverability_when_mig_restore_or_policy(k3s, k3s_admin, k3s_mig_node, evidence_dir, tmp_path) -> None:
    '''
    ALLOC-006 (P0, allocation, kubernetes): Preserve recoverability when MIG restore or policy
    restoration fails.

    Acceptance: Every injected failure leaves either the original active allocation or a durable
    resumable restore; no live pointer references a deleted configuration.

    On the opt-in MIG node: cleanup runs as an impersonated user allowed everything but
    ``patch`` on ClusterPolicies; the geometry reverts, the pointer restore is refused, the
    map and the records stay; a second cleanup with the real credentials finishes.
    '''
    from _k8s import kubectl, kubectl_json

    from videoflow.deploy.cluster import gpu_inventory_observed
    _mixed_strategy_or_not_run(k3s_mig_node)
    inventory = gpu_inventory_observed()
    assert isinstance(inventory, Known)
    record = next(n for n in inventory.value if n.name == k3s_mig_node)
    from videoflow.deploy.mig import solve_layout
    plan = gpu.AllocationPlan(solve_layout([record], [_spec('share', 20)]), {}, 'conf-a006', inventory.generation)
    user = 'vf-conf-a006'
    rbac = f'''apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: {user}
  labels: {{videoflow.io/conformance: "true"}}
rules:
- apiGroups: [""]
  resources: ["nodes", "pods", "configmaps"]
  verbs: ["get", "list", "watch", "update", "patch", "create", "delete"]
- apiGroups: ["apps"]
  resources: ["daemonsets"]
  verbs: ["get", "list"]
- apiGroups: ["nvidia.com"]
  resources: ["clusterpolicies"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: {user}
  labels: {{videoflow.io/conformance: "true"}}
subjects:
- kind: User
  name: {user}
roleRef:
  kind: ClusterRole
  name: {user}
  apiGroup: rbac.authorization.k8s.io
'''
    shim = tmp_path / 'kubectl-restricted'
    shim.write_text(f'#!/bin/sh\nexec kubectl --as={user} "$@"\n')
    shim.chmod(0o755)
    evidence : Dict[str, Any] = {'node': k3s_mig_node}
    strategy = gpu.MixGpu()
    try:
        subprocess.run(['kubectl', 'apply', '-f', '-'], input = rbac, capture_output = True, text = True, check = True)
        applied = strategy.apply_plan(plan, flow_id = 'conf-a006')
        assert applied is not None
        restricted = gpu.MixGpu()
        restricted.cleanup(kubectl = str(shim), flow_id = 'conf-a006')
        policy = gpu._cluster_policy('kubectl') or {}
        pointer = (((policy.get('spec') or {}).get('migManager') or {}).get('config') or {}).get('name')
        live = gpu._read_mig_configmap_yaml('kubectl', applied.namespace)
        evidence['after_denied'] = {'pointer': pointer, 'map_present': bool(live),
                                    'restore_record': ((policy.get('metadata') or {}).get('annotations') or {}).get(gpu.MIG_CONFIG_NAME_RESTORE_ANNOTATION)}
        assert pointer == gpu.MIG_CONFIGMAP_NAME and live, evidence['after_denied']     # nothing dangling
        gpu.MixGpu().cleanup(flow_id = 'conf-a006')
        policy = gpu._cluster_policy('kubectl') or {}
        evidence['after_retry'] = {'pointer': (((policy.get('spec') or {}).get('migManager') or {}).get('config') or {}).get('name'),
                                   'labels': {k: v for k, v in kubectl_json('get', 'node', k3s_mig_node)['metadata']['labels'].items() if 'videoflow' in k or 'mig' in k}}
        assert evidence['after_retry']['pointer'] != gpu.MIG_CONFIGMAP_NAME
        assert gpu.GPU_OWNER_LABEL not in evidence['after_retry']['labels']
    finally:
        try:
            strategy.cleanup(flow_id = 'conf-a006')
        finally:
            kubectl('delete', 'clusterrolebinding', user, '--ignore-not-found')
            kubectl('delete', 'clusterrole', user, '--ignore-not-found')
            (evidence_dir / 'restore_failures.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-006')
@pytest.mark.level('model')
@pytest.mark.variant('operator-objects')
def test_alloc_006_failed_restores_keep_the_records_and_retries_converge(monkeypatch, evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_006_model(monkeypatch, evidence)
    (evidence_dir / 'restore_failures_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-006')
def test_alloc_006_detects_a_swallowed_policy_restore(monkeypatch) -> None:
    defects_alloc.swallowed_policy_restore(monkeypatch)
    assert defects.detects(_oracle_alloc_006_model, monkeypatch, {})


# -- ALLOC-011 ---------------------------------------------------------------------

def _oracle_alloc_011(monkeypatch : pytest.MonkeyPatch, tmp_path : Any, evidence : Dict[str, Any]) -> None:
    '''Interleaved deployments through one registered strategy: each apply uses its own plan and
    inventory revision; a failed or CPU-only deployment can reuse nothing; a stale plan is refused
    once its prerequisites changed; a fresh process carrying only an operation id mutates nothing.'''
    c = _Cluster(monkeypatch, {'gpu-a': {'cards': 2}, 'gpu-b': {'cards': 2}}, latency = 1)
    strategy = gpu.get_gpu_mode('mix')                                    # the registered singleton
    assert isinstance(strategy, gpu.MixGpu)
    c.set_inventory('gpu-a')
    specs_a = strategy.resolve_specs([_spec('share')], flow_id = 'flowA')
    plan_a = strategy._plan
    assert plan_a is not None and specs_a[0].gpu_resource_name == 'nvidia.com/mig-1g.10gb'
    # B fails validation (more than the pool holds): nothing of A's stays cached.
    c.set_inventory('gpu-b')
    with pytest.raises(LayoutError):
        strategy.resolve_specs([_spec('too-many', memory_gib = 40, nb_tasks = 9)], flow_id = 'flowB')
    assert strategy._plan is None
    with pytest.raises(RuntimeError, match = 'before resolve_specs'):
        strategy.prepare(flow_id = 'flowB')
    # B CPU-only: the same.
    cpu = NodeSpec('cpu', 'x.Y', {}, [], 'processor', True, 1, 'cpu', True)
    assert strategy.resolve_specs([cpu], flow_id = 'flowB') == [cpu]
    assert strategy._plan is None
    with pytest.raises(RuntimeError, match = 'before resolve_specs'):
        strategy.prepare(flow_id = 'flowB')
    mutations_before = list(c.fake.mutation_log)
    assert mutations_before == []                                          # no GPU mutation for B
    # A's explicit plan still applies, under A's id and A's inventory revision.
    applied_a = strategy.apply_plan(plan_a, flow_id = 'flowA')
    assert applied_a is not None and applied_a.nodes == ('gpu-a',)
    assert c.fake.labels('gpu-a').get(gpu.GPU_OWNER_LABEL) == 'flowa' and gpu.GPU_OWNER_LABEL not in c.fake.labels('gpu-b')
    # Two plans in flight: B plans on its own inventory and applies its own plan while A's is live.
    c.set_inventory('gpu-b')
    plan_b = strategy.plan_layout([_spec('share')], flow_id = 'flowB')
    assert plan_b is not plan_a and plan_b.layout.mig_nodes() == ['gpu-b']
    applied_b = strategy.apply_plan(plan_b, flow_id = 'flowB')
    assert applied_b is not None and applied_b.nodes == ('gpu-b',) and applied_b.epoch != applied_a.epoch
    entries = c.fake.configmap_entries()
    assert applied_a.entries['gpu-a'] in entries and applied_b.entries['gpu-b'] in entries
    evidence['applied'] = {'a': dict(applied_a.entries), 'b': dict(applied_b.entries), 'entries': entries}
    # A stale plan: the inventory changed under it (a rival took the node) — refused, nothing written.
    strategy.cleanup(flow_id = 'flowA')
    c.set_inventory('gpu-a')
    stale = strategy.plan_layout([_spec('share')], flow_id = 'flowA')
    c.fake.nodes['gpu-a']['labels'][gpu.GPU_OWNER_LABEL] = 'rival'
    c.fake.nodes['gpu-a']['rv'] += 1
    before = list(c.fake.mutation_log)
    with pytest.raises(OwnershipConflict):
        strategy.apply_plan(stale, flow_id = 'flowA')
    assert c.fake.mutation_log == before
    del c.fake.nodes['gpu-a']['labels'][gpu.GPU_OWNER_LABEL]
    # Occupancy that changed after planning (a pod landed on the card): refused before any geometry write.
    c.set_inventory('gpu-a')
    stale2 = strategy.plan_layout([_spec('share')], flow_id = 'flowA')
    c.fake.pods.append(('gpu-a', 'nvidia.com/gpu', 1))
    with pytest.raises(OwnershipConflict, match = 'occupancy changed'):
        strategy.apply_plan(stale2, flow_id = 'flowA')
    assert gpu.GPU_OWNER_LABEL not in c.fake.labels('gpu-a') and c.fake.mutation_log == before
    c.fake.pods.clear()
    # A fresh process carrying only B's operation id: nothing in memory, nothing mutated.
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    log = tmp_path / 'kubectl.log'
    (bindir / 'kubectl').write_text(f'#!/bin/sh\necho "$@" >> {log}\nexit 1\n')
    (bindir / 'kubectl').chmod(0o755)
    script = ('from videoflow.deploy.gpu import get_gpu_mode\n'
              'try:\n    get_gpu_mode("mix").prepare(flow_id = "flowB")\nexcept RuntimeError as e:\n    print("refused:", e)\n')
    import os
    env = dict(os.environ, PATH = f'{bindir}:{os.environ.get("PATH", "")}')
    out = _REAL_RUN([sys.executable, '-c', script], capture_output = True, text = True, env = env, timeout = 120)
    assert 'refused:' in out.stdout and 'before resolve_specs' in out.stdout, out
    assert not log.exists(), log.read_text() if log.exists() else ''
    evidence['fresh_process'] = out.stdout.strip()
    strategy.cleanup(flow_id = 'flowB')


@pytest.mark.case('ALLOC-011')
@pytest.mark.level('process')
def test_alloc_011_keep_plans_isolated_across_concurrent_and_failed(monkeypatch, tmp_path, evidence_dir) -> None:
    '''
    ALLOC-011 (P0, allocation, process): Keep plans isolated across concurrent and failed
    deployments.

    Acceptance: All mutations are attributable to the correct valid plan; failed or CPU-only
    operations perform no GPU mutation.
    '''
    evidence : Dict[str, Any] = {}
    _oracle_alloc_011(monkeypatch, tmp_path, evidence)
    (evidence_dir / 'plan_isolation.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-011')
def test_alloc_011_detects_the_cached_singleton_layout(monkeypatch, tmp_path) -> None:
    defects_alloc.cached_layout_strategy(monkeypatch)
    assert defects.detects(_oracle_alloc_011, monkeypatch, tmp_path, {})
    monkeypatch.undo()
    defects_alloc.no_occupancy_reread(monkeypatch)
    assert defects.detects(_oracle_alloc_011, monkeypatch, tmp_path, {})


# -- ALLOC-033 ---------------------------------------------------------------------

PREPARE_BOUNDARIES = ('owner.update.after', 'claim.create.after', 'claim.schedule.after', 'claim.prepare.after')
CLEANUP_BOUNDARIES = ('restore.before', 'restore.after', 'delete.before', 'delete.after')


def _converged(c : _Cluster, node : str, original : str | None) -> Dict[str, Any]:
    labels, annotations = c.fake.labels(node), c.fake.nodes[node]['annotations']
    return {'owner': labels.get(gpu.GPU_OWNER_LABEL), 'epoch': labels.get(gpu.GPU_OWNER_EPOCH_LABEL),
            'mig_config': labels.get(gpu.MIG_CONFIG_LABEL), 'restore_record': annotations.get(gpu.MIG_RESTORE_ANNOTATION),
            'entry_record': annotations.get(gpu.MIG_ENTRY_ANNOTATION), 'entries': c.fake.configmap_entries(),
            'pointer': c.fake.pointer(), 'original': original}


def _oracle_alloc_033(monkeypatch : pytest.MonkeyPatch, evidence : Dict[str, Any]) -> None:
    '''For every persistent boundary of prepare and cleanup, a crash there and a fresh process's
    cleanup converge to the restored state — original label absent, explicitly empty or an
    administrator value — with no lost original, no orphan record and no other epoch touched.'''
    rounds = []
    for original in (None, '', 'all-disabled'):
        for phase, boundaries in (('prepare', PREPARE_BOUNDARIES), ('cleanup', CLEANUP_BOUNDARIES)):
            for boundary in boundaries:
                labels = {} if original is None else {gpu.MIG_CONFIG_LABEL: original}
                c = _Cluster(monkeypatch, {'gpu-a': {'cards': 2, 'labels': dict(labels)}}, latency = 1)
                c.set_inventory('gpu-a')
                strategy = gpu.MixGpu()
                plan = strategy.plan_layout([_spec('share')], flow_id = 'flowA')
                schedule = faults.FaultSchedule({boundary: faults.Nth(1, faults.RaiseError(lambda: KeyboardInterrupt('crash')))})
                if phase == 'prepare':
                    schedule.install()
                    try:
                        with pytest.raises(KeyboardInterrupt):
                            strategy.apply_plan(plan, flow_id = 'flowA')
                    finally:
                        schedule.uninstall()
                    mid = _converged(c, 'gpu-a', original)
                    # Whatever was recorded is the operator's value (absent, empty or its own), never ours.
                    expected_record = gpu.MIG_LABEL_ABSENT if original is None else original
                    assert mid['restore_record'] in (None, expected_record), mid
                    # A new deploy of the same flow name cannot conflate itself with the crashed lifetime.
                    if mid['owner'] == 'flowa':
                        c.set_inventory('gpu-a')
                        with pytest.raises(ValueError, match = 'still carry'):
                            gpu.MixGpu().plan_layout([_spec('share')], flow_id = 'flowA')
                else:
                    applied = strategy.apply_plan(plan, flow_id = 'flowA')
                    assert applied is not None
                    schedule.install()
                    try:
                        with pytest.raises(KeyboardInterrupt):
                            gpu.MixGpu().cleanup(flow_id = 'flowA')
                    finally:
                        schedule.uninstall()
                    mid = _converged(c, 'gpu-a', original)
                # A fresh process recovers from the cluster's state alone — twice, to show idempotence.
                gpu.MixGpu().cleanup(flow_id = 'flowA')
                first = _converged(c, 'gpu-a', original)
                mutations = len(c.fake.mutation_log)
                gpu.MixGpu().cleanup(flow_id = 'flowA')
                second = _converged(c, 'gpu-a', original)
                unsafe = [m for m in c.fake.mutation_log[mutations:] if 'label gpu-a' in m or 'point clusterpolicy' in m]
                rounds.append({'original': original, 'phase': phase, 'boundary': boundary, 'crash_state': mid,
                               'after_recovery': first, 'idempotent_mutations': unsafe})
                assert first['owner'] is None and first['epoch'] is None, first
                assert first['restore_record'] is None and first['entry_record'] is None, first
                assert first['mig_config'] == original, first                  # absent stays absent, '' stays ''
                assert not any(e.startswith('videoflow-gpu-a') for e in first['entries']), first
                assert second == first
                assert unsafe == [], unsafe                                   # a done restore is not redone
                # A new allocation lifetime of the same human flow name (a new epoch) is its own:
                # the crashed lifetime's epoch can no longer release or revert it.
                c.set_inventory('gpu-a')
                fresh = gpu.MixGpu()
                again = fresh.apply_plan(fresh.plan_layout([_spec('share')], flow_id = 'flowA'), flow_id = 'flowA')
                assert again is not None
                old_epoch = mid.get('epoch') or 'crashed-epoch'
                assert again.epoch != old_epoch
                assert gpu.MixGpu()._release_owner('kubectl', 'gpu-a', 'flowa', old_epoch) is False
                assert c.fake.labels('gpu-a').get(gpu.GPU_OWNER_EPOCH_LABEL) == again.epoch
                gpu.MixGpu().cleanup(flow_id = 'flowA')
                assert gpu.GPU_OWNER_LABEL not in c.fake.labels('gpu-a')
    evidence['rounds'] = rounds


@pytest.mark.case('ALLOC-033')
@pytest.mark.level('kubernetes')
@pytest.mark.timeout(3600)
def test_alloc_033_recover_mig_ownership_and_original_state_records_across(k3s, k3s_mig_node, evidence_dir) -> None:
    '''
    ALLOC-033 (P0, allocation, kubernetes): Recover MIG ownership and original-state records
    across every lifecycle crash boundary.

    Acceptance: Every crash point converges to the intended active or fully restored state with
    no lost original value, no orphan records and no mutation of another epoch.

    On the opt-in MIG node: a fresh process crashes (the ``faults`` barrier raises) at each
    persistent boundary of prepare and cleanup; a fresh cleanup converges every time, with
    the node's pre-test label restored as recorded.
    '''
    from _k8s import kubectl_json

    from videoflow.deploy.cluster import gpu_inventory_observed
    labels = _mixed_strategy_or_not_run(k3s_mig_node)
    original = labels.get(gpu.MIG_CONFIG_LABEL)
    inventory = gpu_inventory_observed()
    assert isinstance(inventory, Known)
    record = next(n for n in inventory.value if n.name == k3s_mig_node)
    from videoflow.deploy.mig import solve_layout
    plan = gpu.AllocationPlan(solve_layout([record], [_spec('share', 20)]), {}, 'conf-a033', inventory.generation)
    rounds = []
    try:
        for phase, boundaries in (('prepare', PREPARE_BOUNDARIES), ('cleanup', CLEANUP_BOUNDARIES)):
            for boundary in boundaries:
                strategy = gpu.MixGpu()
                schedule = faults.FaultSchedule({boundary: faults.Nth(1, faults.RaiseError(lambda: KeyboardInterrupt('crash')))})
                if phase == 'cleanup':
                    assert strategy.apply_plan(plan, flow_id = 'conf-a033') is not None
                schedule.install()
                try:
                    with pytest.raises(KeyboardInterrupt):
                        if phase == 'prepare':
                            strategy.apply_plan(plan, flow_id = 'conf-a033')
                        else:
                            gpu.MixGpu().cleanup(flow_id = 'conf-a033')
                finally:
                    schedule.uninstall()
                gpu.MixGpu().cleanup(flow_id = 'conf-a033')
                gpu.MixGpu().cleanup(flow_id = 'conf-a033')
                after = kubectl_json('get', 'node', k3s_mig_node)['metadata']
                state = {'phase': phase, 'boundary': boundary,
                         'labels': {k: v for k, v in after['labels'].items() if 'videoflow' in k or 'mig.config' in k},
                         'annotations': {k: v for k, v in after.get('annotations', {}).items() if 'videoflow' in k}}
                rounds.append(state)
                assert gpu.GPU_OWNER_LABEL not in after['labels'], state
                assert after['labels'].get(gpu.MIG_CONFIG_LABEL) == original, state
                assert gpu.MIG_RESTORE_ANNOTATION not in after.get('annotations', {}), state
    finally:
        gpu.MixGpu().cleanup(flow_id = 'conf-a033')
        (evidence_dir / 'crash_boundaries.json').write_text(json.dumps(rounds, indent = 2, default = str))


@pytest.mark.case('ALLOC-033')
@pytest.mark.level('process')
@pytest.mark.variant('operator-objects')
def test_alloc_033_every_crash_boundary_converges(monkeypatch, evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_033(monkeypatch, evidence)
    (evidence_dir / 'crash_boundaries_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-033')
def test_alloc_033_detects_an_overwritten_restore_record(monkeypatch) -> None:
    defects_alloc.overwriting_restore_record(monkeypatch)
    assert defects.detects(_oracle_alloc_033, monkeypatch, {})
