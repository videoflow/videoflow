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
import sys
import threading
import time
from typing import Any, Dict, List

import defects
import defects_alloc
import pytest
from _migcluster import MigCluster
from support_kubectl import FakeKubectl, fake_run, nodes_json

from videoflow.backends import faults
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
from videoflow.backends.outcomes import Known, Unknown, known
from videoflow.core.compiler import NodeSpec
from videoflow.core.errors import OwnershipConflict, UnobservableState
from videoflow.deploy import cluster, gpu
from videoflow.deploy.mig import NodeInventory

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

def _race_two_claimants(stamp : Any, node : str, owners : tuple, pause : str = 'after-read') -> Dict[str, Any]:
    '''Two claimants read the node at the same resourceVersion (both paused after their read),
    then write concurrently; returns each one's outcome (its epoch, or the conflict it saw).'''
    schedule = faults.FaultSchedule({'owner.read.after': faults.Pause(pause, timeout_seconds = 60)})
    schedule.install()
    outcomes : Dict[str, Any] = {}

    def claim(owner : str) -> None:
        try:
            outcomes[owner] = {'epoch': stamp(owner)}
        except OwnershipConflict as e:
            outcomes[owner] = {'conflict': str(e)}
        except Exception as e:      # noqa: BLE001
            outcomes[owner] = {'error': f'{type(e).__name__}: {e}'}
    threads = [threading.Thread(target = claim, args = (o,)) for o in owners]
    try:
        for t in threads:
            t.start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and schedule.fired().get('owner.read.after', 0) < len(owners):
            time.sleep(0.01)
        assert schedule.fired().get('owner.read.after', 0) >= len(owners), 'a claimant never read the node'
        schedule.release(pause)                                  # both writes go now
        for t in threads:
            t.join(60)
    finally:
        schedule.uninstall()
    return outcomes


def _oracle_alloc_004_model(evidence : Dict[str, Any]) -> None:
    '''Two claimants at the same resourceVersion: exactly one epoch wins, the other sees the API's
    conflict and mutates nothing; a stale rollback cannot remove the winner's claim; ownership is
    what the API holds, not what a process remembers.'''
    fake = MigCluster({'gpu-a': {'cards': 2}}, manager_latency = 1)
    real = subprocess.run
    subprocess.run = fake       # type: ignore[assignment]
    try:
        for order in ((('flowA', 'flowB'), ('flowB', 'flowA'))):
            fake.nodes['gpu-a']['labels'].pop(gpu.GPU_OWNER_LABEL, None)
            fake.nodes['gpu-a']['labels'].pop(gpu.GPU_OWNER_EPOCH_LABEL, None)
            outcomes = _race_two_claimants(
                lambda owner: gpu.MixGpu()._stamp_node_owners('kubectl', ['gpu-a'], gpu.flow_owner_value(owner)),
                'gpu-a', order)
            winners = [o for o, r in outcomes.items() if 'epoch' in r]
            losers = [o for o, r in outcomes.items() if 'conflict' in r]
            assert len(winners) == 1 and len(losers) == 1, outcomes
            (winner,), (loser,) = winners, losers
            assert 'Conflict' in outcomes[loser]['conflict'] or 'owned by' in outcomes[loser]['conflict'], outcomes
            labels = fake.labels('gpu-a')
            assert labels[gpu.GPU_OWNER_LABEL] == gpu.flow_owner_value(winner)
            assert labels[gpu.GPU_OWNER_EPOCH_LABEL] == outcomes[winner]['epoch']
            # The loser's rollback (a stale release) cannot remove the winner's claim —
            # and does not even attempt the write: a mismatch is decided on the read.
            writes_before = len([c for c, _ in fake.calls if c[1:3] == ['label', 'node']])
            assert gpu.MixGpu()._release_owner('kubectl', 'gpu-a', gpu.flow_owner_value(loser), 'stale') is False
            assert gpu.MixGpu()._release_owner('kubectl', 'gpu-a', gpu.flow_owner_value(winner), 'stale-epoch') is False
            assert fake.labels('gpu-a')[gpu.GPU_OWNER_LABEL] == gpu.flow_owner_value(winner)
            assert len([c for c, _ in fake.calls if c[1:3] == ['label', 'node']]) == writes_before
            # A successor epoch after a real release: the old epoch's release is refused.
            assert gpu.MixGpu()._release_owner('kubectl', 'gpu-a', gpu.flow_owner_value(winner), outcomes[winner]['epoch']) is True
            successor = gpu.MixGpu()._stamp_node_owners('kubectl', ['gpu-a'], gpu.flow_owner_value('flowC'))
            assert gpu.MixGpu()._release_owner('kubectl', 'gpu-a', gpu.flow_owner_value(winner), outcomes[winner]['epoch']) is False
            assert fake.labels('gpu-a')[gpu.GPU_OWNER_EPOCH_LABEL] == successor
            # Ownership survives a process restart: a fresh strategy reads it off the API.
            assert gpu.MixGpu()._release_owner('kubectl', 'gpu-a', gpu.flow_owner_value('flowC'), successor) is True
            evidence.setdefault('interleavings', []).append({'order': list(order), 'outcomes': outcomes})
    finally:
        subprocess.run = real   # type: ignore[assignment]
    # No write ever went out without the server-side precondition.
    writes = [c for c, _ in fake.calls if c[1:3] == ['label', 'node'] and any(a.startswith(f'{gpu.GPU_OWNER_LABEL}=') for a in c)]
    assert writes and all(any(a.startswith('--resource-version=') for a in c) for c in writes), writes


def _oracle_alloc_004_memory(evidence : Dict[str, Any]) -> None:
    '''The reference allocator under the same race: one winner, one typed conflict, no double stamp.'''
    node = NodeFixture('n1', A100, 2, 80.0, dict(_PHYSICAL))
    backend = MemoryAllocationBackend([node], FakeClock(), authority = AUTHORITY_MANAGED_MIG)
    snapshot = backend.inventory({}).value
    plan = backend.plan([WorkloadRequest('flowA', 'r', 'w', 1, SHARING_EXCLUSIVE)], snapshot)
    assert isinstance(plan, FeasiblePlan)

    def reserve(owner : str) -> str:
        return backend.reserve(plan, f'{owner}:op', None).desired_generation
    outcomes = _race_two_claimants(reserve, 'n1', ('flowA', 'flowB'))
    assert sorted(('epoch' in r, 'conflict' in r) for r in outcomes.values()) == [(False, True), (True, False)], outcomes
    stamps = [m for m in backend.mutations(['stamp-owner'])]
    assert len(stamps) == 1, stamps
    evidence['reference'] = outcomes


@pytest.mark.case('ALLOC-004')
@pytest.mark.level('kubernetes')
@pytest.mark.timeout(600)
def test_alloc_004_enforce_exclusive_gpu_ownership_under_simultaneous_claim(k3s, k3s_gpu_nodes, evidence_dir) -> None:
    '''
    ALLOC-004 (P0, allocation, kubernetes): Enforce exclusive GPU ownership under simultaneous
    claim attempts.

    Acceptance: Exactly one winner per contested node; the loser never mutates geometry and
    exits with a typed conflict; a stale rollback cannot remove a newer owner.

    On a pool node: two processes claim it, both held after reading the same
    ``resourceVersion`` (a marker-file pause), released together; the API server decides.
    Only the owner labels are written, and they are released at the end.
    '''
    import tempfile

    from _k8s import kubectl_json
    node = k3s_gpu_nodes[0]
    labels = kubectl_json('get', 'node', node)['metadata'].get('labels', {})
    if gpu.GPU_OWNER_LABEL in labels:
        pytest.skip(f'not_run: node {node} is owned by {labels[gpu.GPU_OWNER_LABEL]!r}; the race needs an unowned node')
    marker_dir = tempfile.mkdtemp(prefix = 'vf-a004-')
    schedule = faults.FaultSchedule({'owner.read.after': faults.Pause('after-read', timeout_seconds = 120)}, marker_dir = marker_dir)
    script = ('import json, sys\nfrom videoflow.backends import faults\nfrom videoflow.deploy import gpu\n'
              'from videoflow.core.errors import OwnershipConflict\n'
              'schedule = faults.FaultSchedule.from_env(); schedule.install()\n'
              'try:\n    epoch = gpu.MixGpu()._stamp_node_owners("kubectl", [sys.argv[1]], sys.argv[2])\n'
              '    print(json.dumps({"epoch": epoch}))\n'
              'except OwnershipConflict as e:\n    print(json.dumps({"conflict": str(e)}))\n')
    owners = ('conf-a004-a', 'conf-a004-b')
    evidence : Dict[str, Any] = {'node': node, 'runs': []}
    try:
        for order in (owners, tuple(reversed(owners))):
            env = dict(os.environ, **schedule.to_env())
            procs = [subprocess.Popen([sys.executable, '-c', script, node, o],
                                                                       stdout = subprocess.PIPE, stderr = subprocess.PIPE,
                                                                       text = True, env = env) for o in order]
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline and schedule.fired().get('owner.read.after', 0) < 2:
                time.sleep(0.2)
            assert schedule.fired().get('owner.read.after', 0) >= 2, 'a claimant never read the node'
            schedule.release('after-read')
            outs = {o: json.loads((p.communicate(timeout = 120)[0] or '{}').strip().splitlines()[-1] or '{}') for o, p in zip(order, procs)}
            winners = [o for o, r in outs.items() if 'epoch' in r]
            losers = [o for o, r in outs.items() if 'conflict' in r]
            labels = kubectl_json('get', 'node', node)['metadata']['labels']
            evidence['runs'].append({'order': list(order), 'outcomes': outs,
                                     'owner': labels.get(gpu.GPU_OWNER_LABEL), 'epoch': labels.get(gpu.GPU_OWNER_EPOCH_LABEL)})
            assert len(winners) == 1 and len(losers) == 1, outs
            assert labels.get(gpu.GPU_OWNER_LABEL) == gpu.flow_owner_value(winners[0])
            # A stale rollback cannot remove the winner; a fresh process releases it by its epoch.
            assert gpu.MixGpu()._release_owner('kubectl', node, gpu.flow_owner_value(losers[0]), 'stale') is False
            assert gpu.MixGpu()._release_owner('kubectl', node, gpu.flow_owner_value(winners[0]), outs[winners[0]]['epoch']) is True
            for name in [f'{marker}.release' for marker in ('after-read',)]:
                try:
                    os.remove(os.path.join(marker_dir, name))
                except FileNotFoundError:
                    pass
            for entry in os.listdir(marker_dir):
                if entry.endswith('.fired'):
                    os.remove(os.path.join(marker_dir, entry))
    finally:
        for owner in owners:
            try:
                gpu.MixGpu()._release_owner('kubectl', node, gpu.flow_owner_value(owner), None)
            except RuntimeError:
                pass
        (evidence_dir / 'claim_race.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-004')
@pytest.mark.level('model')
@pytest.mark.variant('operator-objects')
def test_alloc_004_one_winner_per_node_under_the_api_servers_cas(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_004_model(evidence)
    (evidence_dir / 'claim_race_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-004')
@pytest.mark.level('model')
@pytest.mark.variant('reference-allocator')
def test_alloc_004_reference_allocator_races(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_004_memory(evidence)
    (evidence_dir / 'claim_race_reference.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-004')
def test_alloc_004_detects_a_client_side_claim(monkeypatch) -> None:
    defects_alloc.client_side_claim(monkeypatch)
    assert defects.detects(_oracle_alloc_004_model, {})


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
def test_alloc_008_use_identical_eligibility_scope_for_gpu_classification_and(k3s, k3s_gpu_nodes, evidence_dir) -> None:
    '''
    ALLOC-008 (P1, allocation, kubernetes): Use identical eligibility scope for GPU
    classification and capacity.

    Acceptance: The valid pool request remains valid despite outside-pool sharing;
    selected-node mutations cannot be silently ignored.

    On the shared cluster: every GPU node advertises the same ``nvidia.com/gpu``
    resource, and only the operator-labelled pool nodes contribute evidence —
    the snapshot names them by identity and resourceVersion, the classification
    and a valid multi-GPU plan are decided on that snapshot alone, and the
    out-of-scope nodes' labels (whatever they are) never enter. A selected
    node's evidence then changes for real — an inert holder takes one of its
    cards — and the refreshed snapshot carries a new generation: the plan made
    on the old one is refused at reserve, never applied against stale evidence.
    '''
    from _brokers import unique_ids
    from _k8s import apply, delete_workload, gpu_holder_deployment, kubectl_json, wait_ready

    from videoflow.backends.allocation import FeasiblePlan, WorkloadRequest
    from videoflow.deploy.allocation_kubernetes import KubernetesAllocationBackend
    from videoflow.deploy.cluster import classify_gpu_resource
    evidence : Dict[str, Any] = {'pool': list(k3s_gpu_nodes)}
    all_gpu_nodes = {n['metadata']['name']: {'resourceVersion': n['metadata']['resourceVersion'],
                                             'labels': {k: v for k, v in n['metadata']['labels'].items()
                                                        if k.startswith('nvidia.com/gpu.') or k.startswith('videoflow.io/')},
                                             'allocatable': n['status']['allocatable'].get('nvidia.com/gpu')}
                     for n in kubectl_json('get', 'nodes').get('items', [])
                     if 'nvidia.com/gpu.count' in n['metadata']['labels']}
    outside = sorted(n for n in all_gpu_nodes if n not in k3s_gpu_nodes)
    evidence['all_gpu_nodes'] = all_gpu_nodes
    evidence['outside_pool_advertising_the_same_resource'] = outside
    assert outside, 'the case needs at least one GPU node outside the pool advertising nvidia.com/gpu'
    backend = KubernetesAllocationBackend('exclusive')
    snapshot = backend.inventory({})
    assert isinstance(snapshot, Known), snapshot
    contributing = sorted({d.node for d in snapshot.value.devices if d.node})
    evidence['snapshot'] = {'generation': snapshot.value.generation, 'contributing_nodes': contributing,
                            'sharing': dict(snapshot.value.sharing), 'occupancy': dict(snapshot.value.occupancy)}
    assert contributing == sorted(k3s_gpu_nodes), (contributing, k3s_gpu_nodes)
    assert not set(contributing) & set(outside)
    evidence['classification'] = classify_gpu_resource()
    node = k3s_gpu_nodes[0]
    free = int(all_gpu_nodes[node]['allocatable'] or 0) - int(snapshot.value.occupancy.get(node, 0))
    if free < 2:
        pytest.skip(f'not_run: pool node {node} has {free} free GPU(s); the multi-GPU request needs 2')
    request = WorkloadRequest(flow_id = 'conf-a008', run_id = 'r', workload_id = 'span', device_count = 2,
                              sharing = 'exclusive', minimum_usable_memory_bytes = None, reserved_memory_bytes = None,
                              hard_memory_limit_bytes = None, declared_peak_memory_bytes = None, features = frozenset(),
                              constraints = (), elasticity = 'fixed', provenance = {})
    plan = backend.plan([request], snapshot.value)
    assert isinstance(plan, FeasiblePlan), plan
    planned_nodes = sorted({d.node for d in plan.assignments['span'] if d.node})
    evidence['plan'] = {'nodes': planned_nodes, 'generation': plan.snapshot_generation}
    assert set(planned_nodes) <= set(k3s_gpu_nodes)
    # Nothing outside the pool changed the decision: the same request against a
    # fresh read of the same pool state plans identically.
    again = backend.inventory({})
    assert isinstance(again, Known) and again.value.generation == snapshot.value.generation
    plan_again = backend.plan([request], again.value)
    assert isinstance(plan_again, FeasiblePlan) and plan_again.assignments == plan.assignments
    # A selected node's evidence changes: a holder takes one card on it.
    namespace = k3s['VF_K8S_NAMESPACE']
    holder = 'vf-conf-' + unique_ids('a008')[1][:8]
    try:
        apply(gpu_holder_deployment(holder, namespace, [node], 1))
        wait_ready(namespace, f'app={holder}', 1, timeout = 300)
        refreshed = backend.inventory({})
        assert isinstance(refreshed, Known)
        evidence['refreshed'] = {'generation': refreshed.value.generation, 'occupancy': dict(refreshed.value.occupancy)}
        assert refreshed.value.generation != snapshot.value.generation, 'the snapshot generation ignored the selected node\'s change'
        assert refreshed.value.occupancy.get(node, 0) == snapshot.value.occupancy.get(node, 0) + 1
        with pytest.raises(OwnershipConflict) as e:
            backend.reserve(plan, 'conf-a008:stale', expected_generation = refreshed.value.generation)
        evidence['stale_plan_refused'] = str(e.value)[:200]
        replanned = backend.plan([request], refreshed.value)
        evidence['replanned'] = ('feasible' if isinstance(replanned, FeasiblePlan) else f'infeasible: {replanned.reasons}')
    finally:
        delete_workload(namespace, holder)
        (evidence_dir / 'scope_decisions.json').write_text(json.dumps(evidence, indent = 2, default = str))


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

def _oracle_alloc_012_model(monkeypatch : pytest.MonkeyPatch, evidence : Dict[str, Any]) -> None:
    '''A foreign pod bound between the claim and the geometry write aborts the operation without
    disrupting it; a pod bound before planning keeps its card out of the plan; the label alone is
    never treated as a scheduler lock.'''
    fake = MigCluster({'gpu-a': {'cards': 2}}, manager_latency = 1)
    monkeypatch.setattr(subprocess, 'run', fake)
    monkeypatch.setattr(gpu, 'MIG_APPLY_POLL_SECONDS', 0)
    monkeypatch.setattr(cluster, 'gpu_inventory_observed', lambda kubectl = 'kubectl': known([NodeInventory('gpu-a', A100, 2, 80.0)]))
    strategy = gpu.MixGpu()
    plan = strategy.plan_layout([_gpu_spec('share', gpu_memory_gib = 10)], flow_id = 'flowA')
    schedule = faults.FaultSchedule({'owner.update.after': faults.Nth(1, faults.Pause('claimed', timeout_seconds = 30))})
    schedule.install()
    outcome : Dict[str, Any] = {}
    try:
        def apply() -> None:
            try:
                outcome['applied'] = strategy.apply_plan(plan, flow_id = 'flowA')
            except OwnershipConflict as e:
                outcome['conflict'] = str(e)
        import threading
        t = threading.Thread(target = apply)
        t.start()
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline and schedule.fired().get('owner.update.after', 0) < 1:
            time.sleep(0.01)
        # The claim is on the node, and the scheduler ignores it: a foreign pod binds a card now.
        fake.pods.append(('gpu-a', 'nvidia.com/gpu', 1))
        schedule.release('claimed')
        t.join(30)
    finally:
        schedule.uninstall()
    assert 'conflict' in outcome and 'occupancy changed' in outcome['conflict'], outcome
    assert 'gpu-a: 1 GPU unit(s) now in use' in outcome['conflict']            # the card and the count, named
    assert gpu.GPU_OWNER_LABEL not in fake.labels('gpu-a')                      # claim released
    assert fake.configmap is None and fake.pointer() == 'default-mig-parted-config'   # no geometry mutation
    assert fake.pods == [('gpu-a', 'nvidia.com/gpu', 1)]                        # the foreign pod is untouched
    evidence['foreign_after_claim'] = outcome['conflict'][:200]
    # Reverse order: the foreign pod is there before planning — the plan never carves its card.
    fake.pods[:] = [('gpu-a', 'nvidia.com/gpu', 1)]
    monkeypatch.setattr(cluster, 'gpu_inventory_observed', lambda kubectl = 'kubectl': known(
        [NodeInventory('gpu-a', A100, 2, 80.0, used_units = {'nvidia.com/gpu': 1})]))
    from videoflow.deploy.mig import LayoutError
    with pytest.raises(LayoutError):                                             # a busy node is spanner-only: no MIG
        strategy.plan_layout([_gpu_spec('share', gpu_memory_gib = 10)], flow_id = 'flowA')
    whole = strategy.plan_layout([_gpu_spec('span')], flow_id = 'flowA')
    assert whole.layout.mig_nodes() == [] and not fake.configmap
    evidence['foreign_before_plan'] = 'busy card excluded from MIG; whole-card claims only'


@pytest.mark.case('ALLOC-012')
@pytest.mark.level('gpu')
@pytest.mark.timeout(1800)
def test_alloc_012_prevent_foreign_allocations_arriving_between_planning_and(k3s, k3s_mig_node, evidence_dir) -> None:
    '''
    ALLOC-012 (P0, allocation, gpu): Prevent foreign allocations arriving between planning
    and repartitioning.

    Acceptance: No repartition ever destroys a workload admitted after planning; the
    reservation either blocks the intruder or aborts the geometry change.

    On the opt-in MIG node (mixed strategy): the operation is held after its claim; a
    foreign holder pod takes a whole card; the resumed operation aborts and releases its
    claim, and the holder is still Running on its device afterwards. Then the reverse: with
    the holder bound first, the plan never carves that card.
    '''
    from _brokers import unique_ids
    from _k8s import (
        apply,
        delete_workload,
        gpu_holder_deployment,
        kubectl_json,
        mig_slices_advertised,
        pod_conditions,
        wait_ready,
    )

    from videoflow.deploy.cluster import gpu_inventory_observed
    from videoflow.deploy.mig import solve_layout
    labels = kubectl_json('get', 'node', k3s_mig_node)['metadata'].get('labels', {})
    if labels.get('nvidia.com/mig.strategy') != 'mixed':
        pytest.skip(f'not_run: node {k3s_mig_node} runs mig.strategy={labels.get("nvidia.com/mig.strategy")!r}; managed MIG needs mixed')
    inventory = gpu_inventory_observed()
    assert isinstance(inventory, Known)
    record = next(n for n in inventory.value if n.name == k3s_mig_node)
    plan = gpu.AllocationPlan(solve_layout([record], [_gpu_spec('share', gpu_memory_gib = 20)]), {}, 'conf-a012', inventory.generation)
    namespace = k3s['VF_K8S_NAMESPACE']
    holder = 'vf-conf-' + unique_ids('a012')[1][:8]
    marker_dir = str(evidence_dir / 'markers')
    schedule = faults.FaultSchedule({'owner.update.after': faults.Nth(1, faults.Pause('claimed', timeout_seconds = 600))}, marker_dir = marker_dir)
    evidence : Dict[str, Any] = {'node': k3s_mig_node}
    strategy = gpu.MixGpu()
    outcome : Dict[str, Any] = {}
    try:
        schedule.install()
        import threading

        def run() -> None:
            try:
                outcome['applied'] = strategy.apply_plan(plan, flow_id = 'conf-a012')
            except OwnershipConflict as e:
                outcome['conflict'] = str(e)
            except Exception as e:      # noqa: BLE001
                outcome['error'] = f'{type(e).__name__}: {e}'
        t = threading.Thread(target = run)
        t.start()
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline and schedule.fired().get('owner.update.after', 0) < 1:
            time.sleep(1)
        assert schedule.fired().get('owner.update.after', 0) >= 1, 'the operation never claimed the node'
        apply(gpu_holder_deployment(holder, namespace, [k3s_mig_node], 1))
        wait_ready(namespace, f'app={holder}', 1, timeout = 300)          # the label was no lock for the scheduler
        schedule.release('claimed')
        t.join(900)
        evidence['outcome'] = outcome
        assert 'conflict' in outcome and 'occupancy changed' in outcome['conflict'], outcome
        after = kubectl_json('get', 'node', k3s_mig_node)['metadata']['labels']
        assert gpu.GPU_OWNER_LABEL not in after
        assert not mig_slices_advertised(kubectl_json('get', 'node', k3s_mig_node))   # no geometry mutation
        evidence['holder'] = pod_conditions(namespace, f'app={holder}')
        assert evidence['holder'][0]['ready'] is True                       # never disrupted
        # Reverse: the holder is bound first; planning excludes its card from MIG.
        inventory2 = gpu_inventory_observed()
        assert isinstance(inventory2, Known)
        record2 = next(n for n in inventory2.value if n.name == k3s_mig_node)
        usable, excluded = gpu._partition_inventory([record2], 'conf-a012')
        evidence['reverse'] = {'usable': [(n.name, n.card_count, n.mig_allowed) for n in usable], 'excluded': excluded}
        assert all(not n.mig_allowed for n in usable) or not usable
    finally:
        schedule.uninstall()
        delete_workload(namespace, holder)
        strategy.cleanup(flow_id = 'conf-a012')
        (evidence_dir / 'foreign_between.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-012')
@pytest.mark.level('model')
@pytest.mark.variant('operator-objects')
def test_alloc_012_a_foreign_pod_aborts_the_repartition(monkeypatch, evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_012_model(monkeypatch, evidence)
    (evidence_dir / 'foreign_between_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-012')
def test_alloc_012_detects_a_prepare_that_trusts_the_plan(monkeypatch) -> None:
    defects_alloc.no_occupancy_reread(monkeypatch)
    assert defects.detects(_oracle_alloc_012_model, monkeypatch, {})


# -- ALLOC-013 ---------------------------------------------------------------------

def _oracle_alloc_013_model(monkeypatch : pytest.MonkeyPatch, evidence : Dict[str, Any]) -> None:
    '''A retained workload retains its geometry (``keep_workloads``); cleanup with a pod still holding
    the slice keeps everything and says so; once the last holder is gone one cleanup completes and
    an unrelated node's allocation is untouched.'''
    from videoflow.backends.allocation import RELEASE_PENDING_RECOVERY, SHARING_ISOLATED_MIG, WorkloadRequest
    from videoflow.deploy.allocation_kubernetes import KubernetesAllocationBackend
    fake = MigCluster({'gpu-a': {'cards': 2, 'labels': {'nvidia.com/gpu.product': A100, 'nvidia.com/gpu.count': '2', 'nvidia.com/gpu.memory': '81920'}},
                       'gpu-b': {'cards': 2, 'labels': {'nvidia.com/gpu.product': A100, 'nvidia.com/gpu.count': '2', 'nvidia.com/gpu.memory': '81920'}}},
                      manager_latency = 1)
    monkeypatch.setattr(subprocess, 'run', fake)
    monkeypatch.setattr(gpu, 'MIG_APPLY_POLL_SECONDS', 0)
    # The pool read reflects the fake's live labels, as the real one reads them off the API.
    monkeypatch.setattr(cluster, 'gpu_inventory_observed', lambda kubectl = 'kubectl': known(
        [NodeInventory(n, A100, 2, 80.0, owner = fake.labels(n).get(gpu.GPU_OWNER_LABEL),
                       mig_config = fake.labels(n).get(gpu.MIG_CONFIG_LABEL),
                       used_units = {r: u for node, r, u in fake.pods if node == n}) for n in ('gpu-a', 'gpu-b')]))
    fake.nodes['gpu-b']['labels'].update({gpu.GPU_OWNER_LABEL: 'flowz', gpu.GPU_OWNER_EPOCH_LABEL: 'zz', gpu.MIG_CONFIG_LABEL: 'videoflow-gpu-b-zzzzzz'})
    fake.nodes['gpu-b']['annotations'][gpu.MIG_RESTORE_ANNOTATION] = gpu.MIG_LABEL_ABSENT
    backend = KubernetesAllocationBackend('mix')
    snapshot = backend.inventory({})
    assert isinstance(snapshot, Known)
    plan = backend.plan([WorkloadRequest('flowA', 'r', 'w', 1, SHARING_ISOLATED_MIG, minimum_usable_memory_bytes = 10 << 30)], snapshot.value)
    assert isinstance(plan, FeasiblePlan) and set(plan.geometry) == {'gpu-a'}, plan
    claim = backend.reserve(plan, 'flowA:r', plan.snapshot_generation)
    assert claim.status == 'ready', claim
    geometry_live = dict(fake.allocatable('gpu-a'))
    pointer_live, mutations_live = fake.pointer(), len(fake.mutation_log)
    # The workload holds its slice; the controller leaves with keep_workloads: everything stays.
    fake.pods.append(('gpu-a', 'nvidia.com/mig-1g.10gb', 1))
    kept = backend.release(claim.claim_id, 'flowA:r', claim.desired_generation, keep_workloads = True)
    assert kept.status == RELEASE_PENDING_RECOVERY and fake.allocatable('gpu-a') == geometry_live
    assert fake.labels('gpu-a').get(gpu.GPU_OWNER_LABEL) == 'flowa'
    # Deletion requested (the pod still running): cleanup keeps geometry, records and ownership.
    released = backend.release(claim.claim_id, 'flowA:r', claim.desired_generation)
    assert released.status == RELEASE_PENDING_RECOVERY, released
    assert fake.allocatable('gpu-a') == geometry_live and fake.labels('gpu-a').get(gpu.GPU_OWNER_LABEL) == 'flowa'
    assert fake.nodes['gpu-a']['annotations'].get(gpu.MIG_RESTORE_ANNOTATION) is not None
    # ...and the shared map stays wired: nothing was written at all while the slice was held.
    assert fake.pointer() == pointer_live and fake.mutation_log[mutations_live:] == []
    evidence['while_held'] = {'release': released.status, 'allocatable': fake.allocatable('gpu-a')}
    # The final user exits: one cleanup completes; the unrelated allocation on gpu-b is untouched.
    fake.pods.clear()
    done = backend.release(claim.claim_id, 'flowA:r', claim.desired_generation)
    assert done.status == 'released', done
    assert gpu.GPU_OWNER_LABEL not in fake.labels('gpu-a') and 'nvidia.com/mig-1g.10gb' not in fake.allocatable('gpu-a')
    assert fake.labels('gpu-b').get(gpu.GPU_OWNER_LABEL) == 'flowz' and fake.labels('gpu-b').get(gpu.MIG_CONFIG_LABEL) == 'videoflow-gpu-b-zzzzzz'
    assert fake.configmap is not None and 'DELETE configmap' not in fake.mutation_log
    evidence['after_release'] = {'gpu_a': fake.labels('gpu-a'), 'gpu_b': fake.labels('gpu-b')}


@pytest.mark.case('ALLOC-013')
@pytest.mark.level('gpu')
@pytest.mark.timeout(1800)
def test_alloc_013_delay_allocation_release_while_retained_or_terminating(k3s, k3s_mig_node, evidence_dir) -> None:
    '''
    ALLOC-013 (P0, allocation, gpu): Delay allocation release while retained or terminating
    workloads still use GPUs.

    Acceptance: Ownership and geometry outlive every workload that still holds a device;
    release completes only after the last holder is gone.

    On the opt-in MIG node (mixed strategy): geometry applied, a holder pod bound to one of
    its slices; cleanup while it runs reverts nothing; once the holder is deleted and gone,
    cleanup completes.
    '''
    from _brokers import unique_ids
    from _k8s import delete_workload, kubectl_json, mig_slices_advertised, wait_ready, whole_cards_back

    from videoflow.deploy.cluster import gpu_inventory_observed
    from videoflow.deploy.mig import solve_layout
    labels = kubectl_json('get', 'node', k3s_mig_node)['metadata'].get('labels', {})
    if labels.get('nvidia.com/mig.strategy') != 'mixed':
        pytest.skip(f'not_run: node {k3s_mig_node} runs mig.strategy={labels.get("nvidia.com/mig.strategy")!r}; managed MIG needs mixed')
    inventory = gpu_inventory_observed()
    assert isinstance(inventory, Known)
    record = next(n for n in inventory.value if n.name == k3s_mig_node)
    layout = solve_layout([record], [_gpu_spec('share', gpu_memory_gib = 20)])
    plan = gpu.AllocationPlan(layout, {}, 'conf-a013', inventory.generation)
    resource = next(iter(layout.slice_demand))
    namespace = k3s['VF_K8S_NAMESPACE']
    holder = 'vf-conf-' + unique_ids('a013')[1][:8]
    evidence : Dict[str, Any] = {'node': k3s_mig_node, 'resource': resource,
                                 'allocatable_before': kubectl_json('get', 'node', k3s_mig_node)['status']['allocatable']}
    strategy = gpu.MixGpu()
    try:
        applied = strategy.apply_plan(plan, flow_id = 'conf-a013')
        assert applied is not None
        from _k8s import apply
        manifest = {'apiVersion': 'apps/v1', 'kind': 'Deployment',
                    'metadata': {'name': holder, 'namespace': namespace, 'labels': {'videoflow.io/conformance': 'true', 'app': holder}},
                    'spec': {'replicas': 1, 'selector': {'matchLabels': {'app': holder}},
                             'template': {'metadata': {'labels': {'app': holder, 'videoflow.io/conformance': 'true'}},
                                          'spec': {'priorityClassName': 'cluster-batch', 'runtimeClassName': 'nvidia',
                                                   'terminationGracePeriodSeconds': 20,
                                                   'nodeSelector': {'kubernetes.io/hostname': k3s_mig_node},
                                                   'tolerations': [{'key': 'nvidia.com/gpu', 'operator': 'Exists', 'effect': 'NoSchedule'}],
                                                   'containers': [{'name': 'holder', 'image': __import__('_k8s').base_image(),
                                                                   'command': ['sleep', 'infinity'],
                                                                   'resources': {'limits': {resource: 1}}}]}}}}
        apply(manifest)
        wait_ready(namespace, f'app={holder}', 1, timeout = 300)
        strategy.cleanup(flow_id = 'conf-a013')                              # deletion not even requested: kept
        after = kubectl_json('get', 'node', k3s_mig_node)
        evidence['while_held'] = {'labels': {k: v for k, v in after['metadata']['labels'].items() if 'videoflow' in k or 'mig' in k},
                                  'allocatable': after['status']['allocatable']}
        assert after['metadata']['labels'].get(gpu.GPU_OWNER_LABEL) == gpu.flow_owner_value('conf-a013')
        assert int(after['status']['allocatable'].get(resource, 0)) >= 1
        delete_workload(namespace, holder)                                    # requested, and waited for
        strategy.cleanup(flow_id = 'conf-a013')
        final = whole_cards_back(k3s_mig_node, evidence['allocatable_before'])
        evidence['after_release'] = {'labels': {k: v for k, v in final['metadata']['labels'].items() if 'videoflow' in k or 'mig' in k},
                                     'allocatable': final['status']['allocatable']}
        assert gpu.GPU_OWNER_LABEL not in final['metadata']['labels']
        assert resource not in mig_slices_advertised(final)
        assert final['status']['allocatable'].get('nvidia.com/gpu') == evidence['allocatable_before'].get('nvidia.com/gpu')
    finally:
        delete_workload(namespace, holder)
        strategy.cleanup(flow_id = 'conf-a013')
        (evidence_dir / 'retained_release.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('ALLOC-013')
@pytest.mark.level('model')
@pytest.mark.variant('operator-objects')
def test_alloc_013_geometry_outlives_every_holder(monkeypatch, evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_alloc_013_model(monkeypatch, evidence)
    (evidence_dir / 'retained_release_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'ALLOC-013')
def test_alloc_013_detects_a_cleanup_blind_to_holders(monkeypatch) -> None:
    defects_alloc.no_occupancy_reread(monkeypatch)
    assert defects.detects(_oracle_alloc_013_model, monkeypatch, {})
