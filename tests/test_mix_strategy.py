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


# -- prepare/cleanup lifecycle ---------------------------------------------

def test_prepare_without_a_mig_manager_fails_with_the_config(monkeypatch):
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    fake = _FakeKubectl({})                       # no mig-manager pods anywhere
    monkeypatch.setattr(subprocess, 'run', fake)
    with pytest.raises(RuntimeError) as excinfo:
        strategy.prepare()
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
    strategy.prepare()
    joined = fake.joined_calls()
    # The published ConfigMap merges the operator's entries with videoflow's
    # (plus the disabled config cleanup points un-labeled nodes at)...
    apply_call = next((stdin for c, stdin in fake.calls if 'apply' in c), None)
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
        strategy.prepare()
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
    strategy.prepare()
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
    strategy.prepare()
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
        strategy.prepare()
    assert not any('label node' in c for c in fake.joined_calls())


def test_prepare_is_a_noop_without_mig_cards(monkeypatch):
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('span', gpu_count = 2)])
    fake = _FakeKubectl({})
    monkeypatch.setattr(subprocess, 'run', fake)
    strategy.prepare()                             # whole cards only: nothing to apply
    assert fake.calls == []


def test_prepare_before_resolve_is_a_lifecycle_error():
    with pytest.raises(RuntimeError, match = 'resolve_specs'):
        gpu.MixGpu().prepare()


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
