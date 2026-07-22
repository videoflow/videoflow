'''
The mix GPU strategy (videoflow.deploy.gpu.MixGpu, RFC 0004): spec resolution
through the solver, its preflight, and the prepare/cleanup geometry lifecycle
against the GPU Operator's MIG manager.

Pure/unit: cluster access is monkeypatched (gpu_inventory for resolution,
subprocess for the lifecycle) — no cluster, no GPUs.
'''
from __future__ import absolute_import, division, print_function

import json
import logging
import subprocess

import pytest

from videoflow.core.compiler import NodeSpec
from videoflow.deploy import cluster, gpu
from videoflow.deploy.mig import LayoutError, NodeInventory


def _gpu_spec(name, gpu_count = 1, gpu_memory_gib = None, nb_tasks = 1):
    return NodeSpec(name, 'videoflow.processors.basic.IdentityProcessor', {}, [],
                    'processor', True, nb_tasks, 'gpu', True,
                    gpu_count = gpu_count, gpu_memory_gib = gpu_memory_gib)


def _a100_inventory(monkeypatch, cards = 2):
    monkeypatch.setattr(cluster, 'gpu_inventory',
                        lambda kubectl = 'kubectl': [
                            NodeInventory('gpu-a', 'NVIDIA-A100-SXM4-80GB', cards, 80)])


class _FakeKubectl:
    '''subprocess.run stand-in: canned stdout keyed by command substring, plus a
    call log so lifecycle tests can assert what was mutated.'''
    def __init__(self, responses):
        self.responses = responses
        self.calls = []

    def __call__(self, cmd, **kwargs):
        self.calls.append((list(cmd), kwargs.get('input')))
        joined = ' '.join(cmd)
        for needle, out in self.responses.items():
            if needle in joined:
                return subprocess.CompletedProcess(cmd, 0, out, '')
        return subprocess.CompletedProcess(cmd, 0, '', '')

    def joined_calls(self):
        return [' '.join(c) for c, _stdin in self.calls]


#: The operator's pre-videoflow mig-parted file — merged entries must survive.
_BASE_MIG_CONFIG = ('version: v1\n'
                    'mig-configs:\n'
                    '  all-balanced:\n'
                    '    - devices: all\n'
                    '      mig-enabled: false\n')


def _cluster_policy_json(config_name = 'default-mig-parted-config', annotations = None):
    spec = {'migManager': {'config': {'name': config_name}}} if config_name else {'migManager': {}}
    return json.dumps({'items': [{
        'metadata': {'name': 'cluster-policy', 'annotations': annotations or {}},
        'spec': spec,
    }]})


def _mig_manager_daemonset_json(configmap_name = 'videoflow-mig-parted-config'):
    return json.dumps({'items': [{
        'metadata': {'generation': 2},
        'spec': {'template': {'spec': {'volumes': [
            {'name': 'mig-parted-config', 'configMap': {'name': configmap_name}}]}}},
        'status': {'observedGeneration': 2, 'desiredNumberScheduled': 1,
                   'updatedNumberScheduled': 1, 'numberReady': 1},
    }]})


def _operator_responses(config_name = 'default-mig-parted-config', annotations = None):
    '''Canned kubectl responses for a healthy stock GPU Operator cluster.'''
    return {
        'get pods -A': 'gpu-operator mig-manager-abc',
        'get clusterpolicies': _cluster_policy_json(config_name, annotations),
        'get configmap default-mig-parted-config': json.dumps(
            {'data': {'config.yaml': _BASE_MIG_CONFIG}}),
        'get daemonsets': _mig_manager_daemonset_json(),
    }


# -- resolution ------------------------------------------------------------

def test_resolve_specs_stamps_sharer_resources_and_leaves_spanners(monkeypatch):
    _a100_inventory(monkeypatch, cards = 3)
    strategy = gpu.MixGpu()
    specs = [_gpu_spec('span', gpu_count = 2), _gpu_spec('share', gpu_memory_gib = 10)]
    resolved = strategy.resolve_specs(specs)
    by_name = {s.name: s for s in resolved}
    assert by_name['share'].gpu_resource_name == 'nvidia.com/mig-1g.10gb'
    assert by_name['span'].gpu_resource_name is None
    # The originals are untouched — resolve returns replacements, not mutations.
    assert specs[1].gpu_resource_name is None
    # The resolved specs drive pod resources through the inherited exclusive path.
    assert strategy.pod_resources(by_name['share']) == {'limits': {'nvidia.com/mig-1g.10gb': 1}}
    assert strategy.pod_resources(by_name['span']) == {'limits': {'nvidia.com/gpu': 2}}


def test_resolve_specs_is_identity_for_cpu_flows(monkeypatch):
    monkeypatch.setattr(cluster, 'gpu_inventory',
                        lambda kubectl = 'kubectl': (_ for _ in ()).throw(AssertionError('no probe')))
    spec = NodeSpec('c', 'videoflow.processors.basic.IdentityProcessor', {}, [],
                    'processor', True, 1, 'cpu', True)
    strategy = gpu.MixGpu()
    assert strategy.resolve_specs([spec]) == [spec]


def test_resolve_specs_surfaces_layout_errors(monkeypatch):
    _a100_inventory(monkeypatch, cards = 1)
    with pytest.raises(LayoutError, match = 'whole GPU'):
        gpu.MixGpu().resolve_specs([_gpu_spec('span', gpu_count = 2, nb_tasks = 2)])


# -- multi-tenant inventory partitioning -----------------------------------

def _inventory(monkeypatch, *nodes):
    monkeypatch.setattr(cluster, 'gpu_inventory', lambda kubectl = 'kubectl': list(nodes))


def test_resolve_specs_excludes_other_flows_nodes(monkeypatch):
    '''Multi-tenant pool: a node another flow MIG'd is simply not ours — planning
    proceeds on the rest, and when that falls short the exclusion is named.'''
    _inventory(monkeypatch,
               NodeInventory('gpu-a', 'NVIDIA-A100-SXM4-80GB', 2, 80, owner = 'other'),
               NodeInventory('gpu-b', 'NVIDIA-A100-SXM4-80GB', 1, 80))
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)], flow_id = 'mine')
    assert {c.node for c in strategy._layout.cards} == {'gpu-b'}
    # Infeasible on what remains: the error lists the excluded node and why.
    with pytest.raises(LayoutError) as excinfo:
        gpu.MixGpu().resolve_specs([_gpu_spec('span', gpu_count = 2)], flow_id = 'mine')
    assert 'Nodes excluded from mix planning' in str(excinfo.value)
    assert 'gpu-a: owned by another videoflow flow' in str(excinfo.value)


def test_resolve_specs_refuses_this_flows_leftover_geometry(monkeypatch):
    '''A node still stamped with OUR owner label means a previous run died before
    cleanup: its gpu.count no longer maps card positions, so replanning is unsafe
    and the fix is a teardown, not an exclusion.'''
    _inventory(monkeypatch,
               NodeInventory('gpu-a', 'NVIDIA-A100-SXM4-80GB', 2, 80,
                             owner = gpu.flow_owner_value('mine')))
    with pytest.raises(ValueError, match = 'teardown --flow-id mine'):
        gpu.MixGpu().resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)],
                                   flow_id = 'mine')


def test_resolve_specs_excludes_time_sliced_and_foreign_mig_nodes(monkeypatch):
    _inventory(monkeypatch,
               NodeInventory('sliced', 'NVIDIA-A100-SXM4-80GB-SHARED', 2, 80,
                             time_sliced = True),
               NodeInventory('migd', 'NVIDIA-A100-SXM4-80GB', 2, 80,
                             mig_config = 'all-1g.10gb'),
               NodeInventory('carved', 'NVIDIA-A100-SXM4-80GB', 2, 80,
                             mig_partitioned = True))
    with pytest.raises(LayoutError) as excinfo:
        gpu.MixGpu().resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    message = str(excinfo.value)
    assert 'sliced: time-sliced' in message
    assert 'migd' in message and 'all-1g.10gb' in message
    assert 'carved' in message and 'nvidia.com/mig-*' in message
    assert 'videoflow.io/gpu-pool-' in message      # the copy-pasteable fix


def test_resolve_specs_accepts_the_disabled_config_labels(monkeypatch):
    '''The GPU Operator's stock default labels pristine nodes all-disabled (and a
    crashed videoflow cleanup can leave videoflow-all-disabled): both mean "no
    geometry" and must not be refused.'''
    _inventory(monkeypatch,
               NodeInventory('gpu-a', 'NVIDIA-A100-SXM4-80GB', 1, 80,
                             mig_config = 'all-disabled'),
               NodeInventory('gpu-b', 'NVIDIA-A100-SXM4-80GB', 1, 80,
                             mig_config = gpu.MIG_DISABLED_CONFIG))
    resolved = gpu.MixGpu().resolve_specs(
        [_gpu_spec('share', gpu_memory_gib = 10, nb_tasks = 2)])
    assert resolved[0].gpu_resource_name == 'nvidia.com/mig-1g.10gb'


def test_resolve_specs_shrinks_busy_nodes_to_their_free_cards(monkeypatch):
    '''Running pods hold 1 of gpu-a's 2 cards: the free card can still take a
    spanner, but MIG geometry is off the table — repartitioning would destroy
    the running workloads, and which physical card they hold is unknowable.'''
    _inventory(monkeypatch,
               NodeInventory('gpu-a', 'NVIDIA-A100-SXM4-80GB', 2, 80,
                             used_units = {'nvidia.com/gpu': 1}))
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('span')])
    assert [c.node for c in strategy._layout.cards] == ['gpu-a']   # the one free card
    with pytest.raises(LayoutError, match = 'MIG-capable'):
        gpu.MixGpu().resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    # Fully busy: excluded outright, and the error says why.
    _inventory(monkeypatch,
               NodeInventory('gpu-a', 'NVIDIA-A100-SXM4-80GB', 2, 80,
                             used_units = {'nvidia.com/gpu': 2}))
    with pytest.raises(LayoutError, match = 'no free cards'):
        gpu.MixGpu().resolve_specs([_gpu_spec('span')])


# -- preflight -------------------------------------------------------------

def test_mix_preflight_requires_a_resolved_layout():
    problems = gpu.MixGpu().preflight_problems()
    assert len(problems) == 1 and 'resolve_specs' in problems[0]


def test_mix_preflight_reports_unapplied_geometry_with_the_config(monkeypatch):
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10, nb_tasks = 2)])
    # Cluster advertises no MIG slices yet: geometry not applied.
    monkeypatch.setattr(cluster, 'allocatable_gpus', lambda kubectl, resource: 0)
    monkeypatch.setattr(cluster, 'nvidia_runtimeclass', lambda kubectl: None)
    problems = strategy.preflight_problems()
    assert len(problems) == 1
    assert 'needs 2 x nvidia.com/mig-1g.10gb' in problems[0]
    assert 'mig-devices' in problems[0]          # the mig-parted config is embedded
    # Once the slices are advertised the same check is clean.
    monkeypatch.setattr(cluster, 'allocatable_gpus', lambda kubectl, resource: 2)
    assert strategy.preflight_problems(gpu_runtime_class = 'nvidia') == []


def test_mix_preflight_checks_spanner_capacity(monkeypatch):
    '''Bug #7: the slice check alone passed while the device plugin was broken or
    other workloads held the pool's whole cards — spanner demand must be compared
    against free whole-device capacity too.'''
    _a100_inventory(monkeypatch, cards = 3)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('span', gpu_count = 2),
                            _gpu_spec('share', gpu_memory_gib = 10)])
    monkeypatch.setattr(cluster, 'allocatable_gpus', lambda kubectl, resource: 1)  # slices applied
    monkeypatch.setattr(cluster, 'nvidia_runtimeclass', lambda kubectl: None)
    monkeypatch.setattr(cluster, 'gpu_units_in_use', lambda kubectl = 'kubectl': {})

    def _mostly_taken(kubectl = 'kubectl', resource = 'nvidia.com/gpu',
                      in_use = None, exclude_nodes = frozenset()):
        return cluster.GpuAvailability(per_node_allocatable = {'gpu-a': 2},
                                       per_node_in_use = {'gpu-a': 1})
    monkeypatch.setattr(cluster, 'gpu_availability', _mostly_taken)
    problems = strategy.preflight_problems(demand = {'nvidia.com/gpu': 2,
                                                     'nvidia.com/mig-1g.10gb': 1})
    assert len(problems) == 1
    assert 'nvidia.com/gpu' in problems[0] and 'only 1 free' in problems[0]

    # No advertiser at all: the broken/absent device-plugin case.
    monkeypatch.setattr(cluster, 'gpu_availability',
                        lambda kubectl = 'kubectl', resource = 'nvidia.com/gpu',
                               in_use = None, exclude_nodes = frozenset():
                        cluster.GpuAvailability())
    problems = strategy.preflight_problems(demand = {'nvidia.com/gpu': 2,
                                                     'nvidia.com/mig-1g.10gb': 1})
    assert len(problems) == 1 and 'nvidia-device-plugin' in problems[0]


# -- prepare/cleanup lifecycle ---------------------------------------------

def test_prepare_without_a_mig_manager_fails_with_the_config(monkeypatch):
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    fake = _FakeKubectl({})                       # no mig-manager pods anywhere
    monkeypatch.setattr(subprocess, 'run', fake)
    with pytest.raises(RuntimeError) as excinfo:
        strategy.prepare(flow_id = 'flow1')
    assert 'nvidia-mig-manager' in str(excinfo.value)
    assert 'mig-devices' in str(excinfo.value)    # actionable: config included


def test_prepare_wires_the_config_through_cluster_policy(monkeypatch):
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    responses = _operator_responses()
    responses.update({
        'mig\\.config\\.state': 'success',
        # The node had no previous mig.config label and no recorded restore value.
        'mig\\.config}': '',
        'mig-config-restore}': '',
    })
    fake = _FakeKubectl(responses)
    monkeypatch.setattr(subprocess, 'run', fake)
    strategy.prepare(flow_id = 'flow1')
    joined = fake.joined_calls()
    # The published ConfigMap merges the operator's entries with videoflow's
    # (plus the disabled config cleanup points un-labeled nodes at)...
    apply_call = next((stdin for c, stdin in fake.calls if c[1] in ('create', 'replace')), None)
    assert apply_call and 'videoflow-mig-parted-config' in apply_call
    assert '"namespace": "gpu-operator"' in apply_call
    assert 'all-balanced' in apply_call            # merged, not replaced
    assert 'videoflow-gpu-a' in apply_call
    assert 'videoflow-all-disabled' in apply_call
    # ...the original config name is recorded before the policy is repointed...
    annotate_idx = next(i for i, c in enumerate(joined)
                        if 'annotate clusterpolicies.nvidia.com cluster-policy' in c
                        and 'mig-config-name-restore=default-mig-parted-config' in c)
    patch_idx = next(i for i, c in enumerate(joined)
                     if 'patch clusterpolicies.nvidia.com cluster-policy' in c
                     and 'videoflow-mig-parted-config' in c)
    assert annotate_idx < patch_idx
    # ...the mig-manager rollout completes before any node is labeled...
    rollout_idx = max(i for i, c in enumerate(joined) if 'get daemonsets' in c)
    label_idx = next(i for i, c in enumerate(joined)
                     if 'label node gpu-a --overwrite nvidia.com/mig.config=videoflow-gpu-a' in c)
    assert patch_idx < rollout_idx < label_idx
    # ...and the node-side record/label protocol is unchanged.
    assert any('annotate node gpu-a --overwrite videoflow.io/mig-config-restore=' in c
               for c in joined)


def test_prepare_without_a_cluster_policy_fails_with_the_config(monkeypatch):
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    fake = _FakeKubectl({'get pods -A': 'gpu-operator mig-manager-abc',
                         'get clusterpolicies': json.dumps({'items': []})})
    monkeypatch.setattr(subprocess, 'run', fake)
    with pytest.raises(RuntimeError) as excinfo:
        strategy.prepare(flow_id = 'flow1')
    assert 'ClusterPolicy' in str(excinfo.value)
    assert 'mig-devices' in str(excinfo.value)     # actionable: config included
    joined = fake.joined_calls()
    assert not any('patch' in c or 'label node' in c for c in joined)


def test_prepare_records_an_absent_config_name_as_the_sentinel(monkeypatch):
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    responses = _operator_responses(config_name = None)
    responses.update({'mig\\.config\\.state': 'success',
                      'mig\\.config}': '', 'mig-config-restore}': ''})
    fake = _FakeKubectl(responses)
    monkeypatch.setattr(subprocess, 'run', fake)
    strategy.prepare(flow_id = 'flow1')
    joined = fake.joined_calls()
    # No original to record: the sentinel goes in, and the merge base is the
    # operator's stock ConfigMap.
    assert any(f'mig-config-name-restore={gpu.MIG_CONFIG_NAME_ABSENT}' in c for c in joined)
    assert any('get configmap default-mig-parted-config' in c for c in joined)


def test_prepare_leaves_an_existing_restore_record_alone(monkeypatch):
    # An unclean earlier run left the policy patched and its record in place:
    # re-preparing must not overwrite the true pre-videoflow value.
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    responses = _operator_responses(
        config_name = 'videoflow-mig-parted-config',
        annotations = {gpu.MIG_CONFIG_NAME_RESTORE_ANNOTATION: 'custom-config'})
    responses.update({'mig\\.config\\.state': 'success',
                      'mig\\.config}': '', 'mig-config-restore}': ''})
    fake = _FakeKubectl(responses)
    monkeypatch.setattr(subprocess, 'run', fake)
    strategy.prepare(flow_id = 'flow1')
    joined = fake.joined_calls()
    assert not any('annotate clusterpolicies' in c for c in joined)
    assert not any('patch clusterpolicies' in c for c in joined)   # already pointed at ours
    assert any('label node gpu-a' in c for c in joined)            # the rest still runs


def test_prepare_rollout_timeout_fails_before_labeling(monkeypatch):
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    # The DaemonSet never remounts videoflow's ConfigMap.
    responses = _operator_responses()
    responses['get daemonsets'] = _mig_manager_daemonset_json('default-mig-parted-config')
    fake = _FakeKubectl(responses)
    monkeypatch.setattr(subprocess, 'run', fake)
    monkeypatch.setattr(gpu, 'MIG_MANAGER_ROLLOUT_TIMEOUT_SECONDS', 0)
    monkeypatch.setattr(gpu.time, 'sleep', lambda seconds: None)
    with pytest.raises(RuntimeError, match = 'remount'):
        strategy.prepare(flow_id = 'flow1')
    # The owner stamp may land (it precedes the geometry), but no node may be
    # pointed at a config the manager has not mounted yet.
    assert not any('mig.config=videoflow-' in c for c in fake.joined_calls())


def test_prepare_is_a_noop_without_mig_cards(monkeypatch):
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('span', gpu_count = 2)])
    fake = _FakeKubectl({})
    monkeypatch.setattr(subprocess, 'run', fake)
    strategy.prepare(flow_id = 'flow1')            # whole cards only: nothing to apply
    assert fake.calls == []


def test_prepare_before_resolve_is_a_lifecycle_error():
    with pytest.raises(RuntimeError, match = 'resolve_specs'):
        gpu.MixGpu().prepare()


def test_prepare_with_mig_nodes_requires_a_flow_id(monkeypatch):
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    with pytest.raises(RuntimeError, match = 'flow_id'):
        strategy.prepare()


def test_prepare_stamps_ownership_before_any_geometry(monkeypatch):
    '''The owner claim is the multi-tenant fence: it must land before the
    ConfigMap or any mig.config label, without --overwrite (compare-and-swap),
    and come off again in cleanup only after the node reverted.'''
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    responses = _operator_responses()
    responses.update({'mig\\.config\\.state': 'success',
                      'mig\\.config}': '', 'mig-config-restore}': ''})
    fake = _FakeKubectl(responses)
    monkeypatch.setattr(subprocess, 'run', fake)
    strategy.prepare(flow_id = 'flow1')
    joined = fake.joined_calls()
    stamp_idx = next(i for i, c in enumerate(joined)
                     if 'label node gpu-a videoflow.io/gpu-owner=flow1' in c)
    assert '--overwrite' not in joined[stamp_idx]
    apply_idx = next(i for i, (c, _stdin) in enumerate(fake.calls)
                     if c[1] in ('create', 'replace'))
    mig_label_idx = next(i for i, c in enumerate(joined)
                         if 'nvidia.com/mig.config=videoflow-gpu-a' in c)
    assert stamp_idx < apply_idx < mig_label_idx


def test_prepare_releases_its_claims_when_a_node_is_taken(monkeypatch):
    '''Two deploys race for the same nodes: the loser must abort before touching
    geometry and release whatever it had already stamped, or it would strand
    capacity the winner planned against.'''
    _inventory(monkeypatch,
               NodeInventory('gpu-a', 'NVIDIA-A100-SXM4-80GB', 1, 80),
               NodeInventory('gpu-b', 'NVIDIA-A100-SXM4-80GB', 1, 80))
    strategy = gpu.MixGpu()
    # 8 slices of 1g.10gb exceed one card (max 7): the layout spans both nodes.
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10, nb_tasks = 8)])
    responses = _operator_responses()
    responses.update({
        'node gpu-a -o jsonpath={.metadata.labels.videoflow\\.io/gpu-owner}': '',
        'node gpu-b -o jsonpath={.metadata.labels.videoflow\\.io/gpu-owner}': 'otherflow',
    })
    fake = _FakeKubectl(responses)
    monkeypatch.setattr(subprocess, 'run', fake)
    with pytest.raises(RuntimeError) as excinfo:
        strategy.prepare(flow_id = 'flow1')
    assert 'gpu-b' in str(excinfo.value) and 'otherflow' in str(excinfo.value)
    joined = fake.joined_calls()
    assert any('label node gpu-a videoflow.io/gpu-owner=flow1' in c for c in joined)
    assert any('label node gpu-a videoflow.io/gpu-owner-' in c for c in joined)  # released
    assert not any(c[1] in ('create', 'replace') for c, _stdin in fake.calls)  # no geometry touched
    assert not any('mig.config=videoflow-' in c for c in joined)


#: A published map carrying another flow's entry, as prepare/cleanup would find it.
_LIVE_MIG_CONFIG = ('version: v1\n'
                    'mig-configs:\n'
                    '  all-balanced:\n'
                    '    - devices: all\n'
                    '      mig-enabled: false\n'
                    '  videoflow-gpu-z:\n'
                    '    - devices: [0]\n'
                    '      mig-enabled: true\n'
                    '      mig-devices:\n'
                    '        2g.20gb: 1\n'
                    '  videoflow-all-disabled:\n'
                    '    - devices: all\n'
                    '      mig-enabled: false\n')


def test_prepare_preserves_other_flows_published_entries(monkeypatch):
    '''Two flows share MIG_CONFIGMAP_NAME: flow B's publish must carry flow A's
    videoflow-<node> entries forward (or A's nodes go state=failed on the next
    manager pass), and must replace with the read resourceVersion so a racing
    writer conflicts instead of being clobbered.'''
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    responses = _operator_responses()
    responses.update({
        'get configmap videoflow-mig-parted-config': json.dumps(
            {'metadata': {'resourceVersion': '41'},
             'data': {'config.yaml': _LIVE_MIG_CONFIG}}),
        'mig\\.config\\.state': 'success',
        'mig\\.config}': '', 'mig-config-restore}': '',
    })
    fake = _FakeKubectl(responses)
    monkeypatch.setattr(subprocess, 'run', fake)
    strategy.prepare(flow_id = 'flow1')
    published = next(stdin for c, stdin in fake.calls if c[1] == 'replace')
    payload = json.loads(published)
    assert payload['metadata']['resourceVersion'] == '41'
    merged = payload['data']['config.yaml']
    assert 'videoflow-gpu-z' in merged       # flow A's geometry survives
    assert 'videoflow-gpu-a' in merged       # ours lands
    assert 'all-balanced' in merged          # operator entries come from the base


def test_prepare_reretries_a_conflicted_publish(monkeypatch):
    '''A racing flow bumps the map between our read and replace: the publish must
    re-read and re-merge, not fail and not overwrite.'''
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    responses = _operator_responses()
    responses.update({
        'get configmap videoflow-mig-parted-config': json.dumps(
            {'metadata': {'resourceVersion': '41'},
             'data': {'config.yaml': _LIVE_MIG_CONFIG}}),
        'mig\\.config\\.state': 'success',
        'mig\\.config}': '', 'mig-config-restore}': '',
    })

    class _Conflicting(_FakeKubectl):
        def __init__(self, responses):
            super().__init__(responses)
            self.replace_attempts = 0

        def __call__(self, cmd, **kwargs):
            if len(cmd) > 1 and cmd[1] == 'replace':
                self.replace_attempts += 1
                if self.replace_attempts == 1:
                    self.calls.append((list(cmd), kwargs.get('input')))
                    return subprocess.CompletedProcess(
                        cmd, 1, '', 'Operation cannot be fulfilled: the object has '
                                    'been modified (Conflict)')
            return super().__call__(cmd, **kwargs)

    fake = _Conflicting(responses)
    monkeypatch.setattr(subprocess, 'run', fake)
    strategy.prepare(flow_id = 'flow1')
    assert fake.replace_attempts == 2        # first conflicted, second landed


def test_cleanup_restores_the_recorded_label_state(monkeypatch):
    nodes = {'items': [
        {'metadata': {'name': 'gpu-a',
                      'annotations': {gpu.MIG_RESTORE_ANNOTATION: 'all-balanced'}}},
        {'metadata': {'name': 'gpu-b',
                      'annotations': {gpu.MIG_RESTORE_ANNOTATION: ''}}},
        {'metadata': {'name': 'cpu-1', 'annotations': {}}},
    ]}
    fake = _FakeKubectl({
        'get nodes -o json': json.dumps(nodes),
        'get pods -A': 'gpu-operator mig-manager-abc',
        'get clusterpolicies': _cluster_policy_json(
            config_name = 'videoflow-mig-parted-config',
            annotations = {gpu.MIG_CONFIG_NAME_RESTORE_ANNOTATION: 'default-mig-parted-config'}),
        'mig\\.config\\.state': 'success',
    })
    monkeypatch.setattr(subprocess, 'run', fake)
    gpu.MixGpu().cleanup()
    joined = fake.joined_calls()
    # gpu-a had a previous config: restored directly.
    assert any('label node gpu-a --overwrite nvidia.com/mig.config=all-balanced' in c
               for c in joined)
    # gpu-b had none: first pointed at the disabled config so the geometry
    # actually reverts, unlabeled only after the manager confirms.
    disable_idx = next(i for i, c in enumerate(joined)
                       if 'label node gpu-b --overwrite nvidia.com/mig.config=videoflow-all-disabled' in c)
    state_idx = next(i for i, c in enumerate(joined) if 'mig\\.config\\.state' in c)
    remove_idx = next(i for i, c in enumerate(joined)
                      if 'label node gpu-b nvidia.com/mig.config-' in c)
    assert disable_idx < state_idx < remove_idx
    # Both annotations dropped; the untouched CPU node is never mutated.
    assert sum('annotate node' in c and 'mig-config-restore-' in c for c in joined) == 2
    assert not any('cpu-1' in c and ('label' in c or 'annotate' in c) for c in joined)
    # ClusterPolicy is repointed at the recorded original, its record dropped...
    assert any('patch clusterpolicies.nvidia.com cluster-policy' in c
               and 'default-mig-parted-config' in c for c in joined)
    assert any('annotate clusterpolicies.nvidia.com cluster-policy '
               'videoflow.io/mig-config-name-restore-' in c for c in joined)
    # ...and the published ConfigMap goes too.
    assert any('delete configmap videoflow-mig-parted-config -n gpu-operator' in c
               for c in joined)


def test_cleanup_restores_the_sentinel_to_the_operator_default(monkeypatch):
    # A prepare that recorded "the field was absent" restores the documented
    # operator default explicitly.
    fake = _FakeKubectl({
        'get nodes -o json': json.dumps({'items': []}),
        'get pods -A': 'gpu-operator mig-manager-abc',
        'get clusterpolicies': _cluster_policy_json(
            config_name = 'videoflow-mig-parted-config',
            annotations = {gpu.MIG_CONFIG_NAME_RESTORE_ANNOTATION: gpu.MIG_CONFIG_NAME_ABSENT}),
    })
    monkeypatch.setattr(subprocess, 'run', fake)
    gpu.MixGpu().cleanup()
    joined = fake.joined_calls()
    assert any('patch clusterpolicies.nvidia.com cluster-policy' in c
               and 'default-mig-parted-config' in c for c in joined)
    assert any('mig-config-name-restore-' in c for c in joined)


def test_cleanup_keeps_retry_state_when_the_revert_fails(monkeypatch, caplog):
    nodes = {'items': [{'metadata': {'name': 'gpu-a',
                                     'annotations': {gpu.MIG_RESTORE_ANNOTATION: 'all-balanced'}}}]}
    fake = _FakeKubectl({
        'get nodes -o json': json.dumps(nodes),
        'get pods -A': 'gpu-operator mig-manager-abc',
        'get clusterpolicies': _cluster_policy_json(
            config_name = 'videoflow-mig-parted-config',
            annotations = {gpu.MIG_CONFIG_NAME_RESTORE_ANNOTATION: 'default-mig-parted-config'}),
        'mig\\.config\\.state': 'failed',
    })
    monkeypatch.setattr(subprocess, 'run', fake)
    monkeypatch.setattr(gpu, 'MIG_APPLY_TIMEOUT_SECONDS', 0)
    monkeypatch.setattr(gpu.time, 'sleep', lambda seconds: None)
    with caplog.at_level(logging.WARNING):
        gpu.MixGpu().cleanup()                     # warns, never raises
    joined = fake.joined_calls()
    # The node's restore annotation, the policy patch and the ConfigMap all
    # survive so a retried teardown can resume where this one failed.
    assert not any('annotate node gpu-a' in c for c in joined)
    assert not any('patch clusterpolicies' in c for c in joined)
    assert not any('delete configmap' in c for c in joined)
    assert any('gpu-a' in record.getMessage() and 'teardown' in record.getMessage()
               for record in caplog.records)


def test_cleanup_scoped_to_a_flow_restores_only_its_nodes(monkeypatch):
    '''Two flows share the pool: flow1's teardown must revert flow1's nodes and
    release their claims, and leave flow2's geometry standing.'''
    nodes = {'items': [
        {'metadata': {'name': 'gpu-a',
                      'labels': {gpu.GPU_OWNER_LABEL: 'flow1'},
                      'annotations': {gpu.MIG_RESTORE_ANNOTATION: ''}}},
        {'metadata': {'name': 'gpu-b',
                      'labels': {gpu.GPU_OWNER_LABEL: 'flow2'},
                      'annotations': {gpu.MIG_RESTORE_ANNOTATION: ''}}},
        # Orphan claim: flow1's prepare crashed between stamping and labeling.
        {'metadata': {'name': 'gpu-c',
                      'labels': {gpu.GPU_OWNER_LABEL: 'flow1'}, 'annotations': {}}},
    ]}
    fake = _FakeKubectl({
        'get nodes -o json': json.dumps(nodes),
        'get pods -A': 'gpu-operator mig-manager-abc',
        'get clusterpolicies': _cluster_policy_json(
            config_name = 'videoflow-mig-parted-config',
            annotations = {gpu.MIG_CONFIG_NAME_RESTORE_ANNOTATION: 'default-mig-parted-config'}),
        'mig\\.config\\.state': 'success',
    })
    monkeypatch.setattr(subprocess, 'run', fake)
    gpu.MixGpu().cleanup(flow_id = 'flow1')
    joined = fake.joined_calls()
    # gpu-a reverted, unrecorded and released.
    assert any('label node gpu-a --overwrite nvidia.com/mig.config=videoflow-all-disabled' in c
               for c in joined)
    assert any('label node gpu-a videoflow.io/gpu-owner-' in c for c in joined)
    # gpu-c had no geometry: only its claim is released.
    assert any('label node gpu-c videoflow.io/gpu-owner-' in c for c in joined)
    assert not any('gpu-c' in c and 'mig.config' in c for c in joined)
    # flow2's node is untouched.
    assert not any('gpu-b' in c and ('label' in c or 'annotate' in c) for c in joined)


def test_cleanup_is_not_last_out_while_other_flows_hold_geometry(monkeypatch):
    '''flow1's teardown with flow2's entries still published: strip only flow1's
    entry, and leave the ClusterPolicy pointer, its restore annotation and the
    ConfigMap standing — flow2's node labels must keep resolving in the mounted
    file until the last flow out.'''
    nodes = {'items': [
        {'metadata': {'name': 'gpu-a',
                      'labels': {gpu.GPU_OWNER_LABEL: 'flow1'},
                      'annotations': {gpu.MIG_RESTORE_ANNOTATION: ''}}},
    ]}
    live = ('version: v1\n'
            'mig-configs:\n'
            '  videoflow-gpu-a:\n'
            '    - devices: [0]\n'
            '      mig-enabled: true\n'
            '      mig-devices:\n'
            '        1g.10gb: 1\n'
            '  videoflow-gpu-z:\n'
            '    - devices: [0]\n'
            '      mig-enabled: true\n'
            '      mig-devices:\n'
            '        2g.20gb: 1\n'
            '  videoflow-all-disabled:\n'
            '    - devices: all\n'
            '      mig-enabled: false\n')
    fake = _FakeKubectl({
        'get nodes -o json': json.dumps(nodes),
        'get pods -A': 'gpu-operator mig-manager-abc',
        'get clusterpolicies': _cluster_policy_json(
            config_name = 'videoflow-mig-parted-config',
            annotations = {gpu.MIG_CONFIG_NAME_RESTORE_ANNOTATION: 'default-mig-parted-config'}),
        'get configmap videoflow-mig-parted-config': json.dumps(
            {'metadata': {'resourceVersion': '7'},
             'data': {'config.yaml': live}}),
        'mig\\.config\\.state': 'success',
    })
    monkeypatch.setattr(subprocess, 'run', fake)
    gpu.MixGpu().cleanup(flow_id = 'flow1')
    joined = fake.joined_calls()
    stripped = json.loads(next(stdin for c, stdin in fake.calls if c[1] == 'replace'))
    assert 'videoflow-gpu-a' not in stripped['data']['config.yaml']
    assert 'videoflow-gpu-z' in stripped['data']['config.yaml']
    assert not any('patch clusterpolicies' in c for c in joined)
    assert not any('mig-config-name-restore-' in c for c in joined)
    assert not any('delete configmap' in c for c in joined)


def test_cleanup_without_prepare_is_a_tolerant_noop(monkeypatch):
    fake = _FakeKubectl({'get nodes -o json': json.dumps({'items': []}),
                         'get clusterpolicies': _cluster_policy_json()})
    monkeypatch.setattr(subprocess, 'run', fake)
    gpu.MixGpu().cleanup()                         # must not raise
    assert not any('label' in ' '.join(c) for c, _ in fake.calls)
    # An unannotated ClusterPolicy is not videoflow's to touch.
    assert not any('patch clusterpolicies' in c for c in fake.joined_calls())


if __name__ == '__main__':
    pytest.main([__file__])
