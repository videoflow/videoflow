'''
GPU allocation strategies (videoflow.deploy.gpu).

The mode used to be a bare string branched on in four places -- pod resources,
manifest validation, cluster preflight, and the CLI's choices -- so a third mode
meant finding all four. These tests pin the registry, the built-ins' unchanged
behavior, and the prepare/cleanup lifecycle that the planned time-slicing and
measured-partitioning strategies need.
'''
from __future__ import absolute_import, division, print_function

import subprocess

import pytest

from videoflow.deploy import cluster, gpu, manifests


class _Spec:
    '''Minimal NodeSpec stand-in for pod_resources.'''
    def __init__(self, gpu_count = 1, gpu_resource_name = None):
        self.gpu_count = gpu_count
        self.gpu_resource_name = gpu_resource_name


@pytest.fixture
def registry_sandbox(monkeypatch):
    monkeypatch.setattr(gpu, '_GPU_STRATEGIES', dict(gpu._GPU_STRATEGIES))
    return gpu


# -- built-ins: behavior must be exactly what the string branches did ------

def test_builtin_modes_are_registered():
    assert gpu.registered_gpu_modes() == ['exclusive', 'shared']


def test_exclusive_claims_whole_devices():
    resources = gpu.get_gpu_mode('exclusive').pod_resources(_Spec(gpu_count = 2))
    assert resources == {'limits': {'nvidia.com/gpu': 2}}


def test_exclusive_honours_the_nodes_own_resource_name():
    spec = _Spec(gpu_count = 1, gpu_resource_name = 'nvidia.com/mig-1g.10gb')
    resources = gpu.get_gpu_mode('exclusive').pod_resources(spec, 'nvidia.com/gpu.shared')
    assert resources == {'limits': {'nvidia.com/mig-1g.10gb': 1}}


def test_exclusive_falls_back_to_the_deploy_default():
    resources = gpu.get_gpu_mode('exclusive').pod_resources(_Spec(), 'nvidia.com/gpu.shared')
    assert resources == {'limits': {'nvidia.com/gpu.shared': 1}}


def test_shared_emits_no_resource_limit():
    '''Shared pods land on the pool by selector alone -- a limit would defeat it.'''
    assert gpu.get_gpu_mode('shared').pod_resources(_Spec(gpu_count = 4)) == {}


def test_unknown_mode_names_the_known_ones_and_the_fix():
    with pytest.raises(ValueError) as excinfo:
        gpu.get_gpu_mode('mps')
    msg = str(excinfo.value)
    assert 'exclusive' in msg and 'shared' in msg and 'register_gpu_mode' in msg


def test_render_manifests_rejects_an_unknown_mode_before_building_anything():
    from videoflow.core.compiler import NodeSpec

    spec = NodeSpec('n', 'videoflow.processors.basic.IdentityProcessor', {}, [], 'processor',
                    False, 1, 'cpu', True, image = 'img:1')
    with pytest.raises(ValueError, match = 'gpu_mode'):
        manifests.render_manifests([spec], 'f', 'realtime', 'nats://x', 'r', gpu_mode = 'nope')


# -- registry --------------------------------------------------------------

def test_registering_a_strategy_makes_it_usable_end_to_end(registry_sandbox):
    class _Mps(gpu.GpuStrategy):
        name = 'mps'
        description = 'NVIDIA MPS'

        def pod_resources(self, spec, gpu_resource_name = None):
            return {'limits': {'nvidia.com/gpu.mps': spec.gpu_count}}

    registry_sandbox.register_gpu_mode(_Mps())
    assert 'mps' in registry_sandbox.registered_gpu_modes()
    assert registry_sandbox.get_gpu_mode('mps').pod_resources(_Spec(3)) == {
        'limits': {'nvidia.com/gpu.mps': 3}}


def test_registered_mode_becomes_a_cli_choice(registry_sandbox):
    '''--gpu-mode choices come from the registry, so a new mode needs no CLI edit.'''
    class _Mps(gpu.GpuStrategy):
        name = 'mps'

    registry_sandbox.register_gpu_mode(_Mps())
    from videoflow.deploy.cli import build_parser
    parser = build_parser()
    args = parser.parse_args(['deploy', 'graph.py', '--gpu-mode', 'mps'])
    assert args.gpu_mode == 'mps'


def test_a_strategy_must_have_a_name(registry_sandbox):
    with pytest.raises(ValueError, match = 'name'):
        registry_sandbox.register_gpu_mode(gpu.GpuStrategy())


def test_strategy_drives_the_rendered_pod_spec(registry_sandbox):
    '''The registry is wired into manifests, not merely parallel to it.'''
    class _Half(gpu.GpuStrategy):
        name = 'half'

        def pod_resources(self, spec, gpu_resource_name = None):
            return {'limits': {'example.com/half-gpu': spec.gpu_count}}

    registry_sandbox.register_gpu_mode(_Half())

    from videoflow.core.compiler import NodeSpec
    spec = NodeSpec('g', 'videoflow.processors.basic.IdentityProcessor', {}, [], 'processor',
                    False, 1, 'gpu', True, image = 'img:1', gpu_count = 2)
    pod = manifests.workload(spec, 'f', 'realtime', 'img:1', 'cm', gpu_mode = 'half')
    container = pod['spec']['template']['spec']['containers'][0]
    assert container['resources'] == {'limits': {'example.com/half-gpu': 2}}
    # Selector and toleration are mode-independent: every GPU pod needs the pool.
    assert pod['spec']['template']['spec']['nodeSelector'] == {'videoflow.io/gpu-pool': 'true'}


# -- preflight delegation --------------------------------------------------

def _fake_kubectl(monkeypatch, outputs):
    def run(cmd, **kwargs):
        joined = ' '.join(cmd)
        for needle, out in outputs.items():
            if needle in joined:
                return subprocess.CompletedProcess(cmd, 0, out, '')
        return subprocess.CompletedProcess(cmd, 0, '', '')
    monkeypatch.setattr(subprocess, 'run', run)


def test_preflight_delegates_capacity_math_to_the_strategy(monkeypatch):
    _fake_kubectl(monkeypatch, {'version': '{}', 'gpu-pool=true': 'node/gpu-box',
                                'allocatable': '2'})
    problems = cluster.gpu_preflight(demand = {'nvidia.com/gpu': 9}, gpu_mode = 'exclusive')
    assert any('demands 9' in p for p in problems)


def test_shared_mode_skips_capacity_and_escalates_the_runtime_class(monkeypatch):
    _fake_kubectl(monkeypatch, {'version': '{}', 'gpu-pool=true': 'node/gpu-box',
                                'get runtimeclass': 'nvidia'})
    problems = cluster.gpu_preflight(demand = {'nvidia.com/gpu': 9}, gpu_mode = 'shared')
    assert len(problems) == 1
    assert gpu.SHARED_NEEDS_RUNTIME_CLASS in problems[0]


def test_preflight_uses_a_registered_strategys_checks(monkeypatch, registry_sandbox):
    class _Picky(gpu.GpuStrategy):
        name = 'picky'

        def preflight_problems(self, kubectl = 'kubectl', demand = None, gpu_runtime_class = None):
            return ['picky mode says no']

    registry_sandbox.register_gpu_mode(_Picky())
    _fake_kubectl(monkeypatch, {'version': '{}', 'gpu-pool=true': 'node/gpu-box'})
    assert cluster.gpu_preflight(gpu_mode = 'picky') == ['picky mode says no']


# -- lifecycle hooks -------------------------------------------------------

def test_builtin_lifecycle_hooks_are_noops():
    for mode in ('exclusive', 'shared'):
        strategy = gpu.get_gpu_mode(mode)
        assert strategy.prepare(demand = {'nvidia.com/gpu': 1}) is None
        assert strategy.cleanup() is None


def test_prepare_receives_the_flows_demand(registry_sandbox):
    '''
    The hook a per-run time-slicing strategy needs: it must know how many devices
    the flow wants before it retunes the device plugin.
    '''
    seen = {}

    class _Recording(gpu.GpuStrategy):
        name = 'recording'

        def prepare(self, demand = None, kubectl = 'kubectl'):
            seen['demand'] = demand

        def cleanup(self, kubectl = 'kubectl'):
            seen['cleaned'] = True

    registry_sandbox.register_gpu_mode(_Recording())
    strategy = registry_sandbox.get_gpu_mode('recording')
    strategy.prepare(demand = {'nvidia.com/gpu': 6})
    strategy.cleanup()
    assert seen == {'demand': {'nvidia.com/gpu': 6}, 'cleaned': True}


if __name__ == '__main__':
    pytest.main([__file__])
