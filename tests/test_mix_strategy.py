'''
The mix GPU strategy (videoflow.deploy.gpu.MixGpu, RFC 0004): spec resolution
through the solver, its preflight, and the prepare/cleanup geometry lifecycle
against the GPU Operator's MIG manager.

Pure/unit: cluster access is monkeypatched (gpu_inventory for resolution,
subprocess for the lifecycle) — no cluster, no GPUs.
'''
from __future__ import absolute_import, division, print_function

import json
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


def test_prepare_applies_geometry_via_the_mig_config_label(monkeypatch):
    _a100_inventory(monkeypatch)
    strategy = gpu.MixGpu()
    strategy.resolve_specs([_gpu_spec('share', gpu_memory_gib = 10)])
    fake = _FakeKubectl({
        'get pods -A': 'gpu-operator mig-manager-abc',
        'mig\\.config\\.state': 'success',
        # The node had no previous mig.config label and no recorded restore value.
        'mig\\.config}': '',
        'mig-config-restore}': '',
    })
    monkeypatch.setattr(subprocess, 'run', fake)
    strategy.prepare()
    joined = fake.joined_calls()
    # The generated config was published where the MIG manager lives...
    apply_call = next((stdin for c, stdin in fake.calls if 'apply' in c), None)
    assert apply_call and 'videoflow-mig-parted-config' in apply_call
    assert '"namespace": "gpu-operator"' in apply_call
    # ...the previous (absent) label value was recorded for a stateless cleanup...
    assert any('annotate node gpu-a --overwrite videoflow.io/mig-config-restore=' in c
               for c in joined)
    # ...and the node was pointed at its videoflow config.
    assert any('label node gpu-a --overwrite nvidia.com/mig.config=videoflow-gpu-a' in c
               for c in joined)


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
    })
    monkeypatch.setattr(subprocess, 'run', fake)
    gpu.MixGpu().cleanup()
    joined = fake.joined_calls()
    # gpu-a had a previous config: restored. gpu-b had none: label removed.
    assert any('label node gpu-a --overwrite nvidia.com/mig.config=all-balanced' in c
               for c in joined)
    assert any('label node gpu-b nvidia.com/mig.config-' in c for c in joined)
    # Both annotations dropped; the untouched CPU node is never mutated.
    assert sum('annotate node' in c and 'mig-config-restore-' in c for c in joined) == 2
    assert not any('cpu-1' in c and ('label' in c or 'annotate' in c) for c in joined)
    # The published ConfigMap goes too.
    assert any('delete configmap videoflow-mig-parted-config -n gpu-operator' in c
               for c in joined)


def test_cleanup_without_prepare_is_a_tolerant_noop(monkeypatch):
    fake = _FakeKubectl({'get nodes -o json': json.dumps({'items': []})})
    monkeypatch.setattr(subprocess, 'run', fake)
    gpu.MixGpu().cleanup()                         # must not raise
    assert not any('label' in ' '.join(c) for c, _ in fake.calls)


if __name__ == '__main__':
    pytest.main([__file__])
