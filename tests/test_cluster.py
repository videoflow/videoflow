'''
Cluster auto-detection and image loading: classification from the kubectl
context / node labels, the per-flavor load command, the remote-cluster error,
and the GPU preflight messages.

Pure/unit: subprocess is monkeypatched — no cluster, no docker.
'''
import subprocess

import pytest

from videoflow import cluster


class _Proc:
    def __init__(self, stdout = '', returncode = 0):
        self.stdout = stdout
        self.returncode = returncode


def _fake_run(responses):
    '''Returns a subprocess.run stand-in serving canned stdout keyed by a substring of the command.'''
    calls = []
    def run(cmd, **kwargs):
        calls.append(cmd)
        joined = ' '.join(cmd)
        for key, out in responses.items():
            if key in joined:
                return _Proc(stdout = out)
        return _Proc()
    return run, calls


@pytest.mark.parametrize('ctx,expected', [
    ('kind-dev', cluster.KIND),
    ('minikube', cluster.MINIKUBE),
    ('docker-desktop', cluster.DOCKER_DESKTOP),
])
def test_detect_by_context_name(monkeypatch, ctx, expected):
    run, _ = _fake_run({'current-context': ctx})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.detect_cluster() == expected


def test_detect_k3s_by_node_labels(monkeypatch):
    run, _ = _fake_run({'current-context': 'default', 'get nodes': 'k3s \n'})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.detect_cluster() == cluster.K3S


def test_detect_falls_back_to_remote(monkeypatch):
    run, _ = _fake_run({'current-context': 'gke_my-proj_us-east1_prod'})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.detect_cluster() == cluster.GENERIC_REMOTE


def test_load_images_kind_uses_context_cluster_name(monkeypatch):
    run, calls = _fake_run({'current-context': 'kind-dev'})
    monkeypatch.setattr(subprocess, 'run', run)
    cluster.load_images(cluster.KIND, ['img:1', 'img:2'])
    load = [c for c in calls if c[0] == 'kind'][0]
    assert load == ['kind', 'load', 'docker-image', 'img:1', 'img:2', '--name', 'dev']


def test_load_images_minikube_one_per_image(monkeypatch):
    run, calls = _fake_run({})
    monkeypatch.setattr(subprocess, 'run', run)
    cluster.load_images(cluster.MINIKUBE, ['img:1', 'img:2'])
    loads = [c for c in calls if c[0] == 'minikube']
    assert loads == [['minikube', 'image', 'load', 'img:1'],
                     ['minikube', 'image', 'load', 'img:2']]


def test_load_images_remote_raises_with_push_hint(monkeypatch):
    run, _ = _fake_run({})
    monkeypatch.setattr(subprocess, 'run', run)
    with pytest.raises(RuntimeError, match = 'docker push'):
        cluster.load_images(cluster.GENERIC_REMOTE, ['img:1'])


def test_load_images_docker_desktop_is_a_noop(monkeypatch, capsys):
    def boom(cmd, **kwargs):
        raise AssertionError('no subprocess expected')
    monkeypatch.setattr(subprocess, 'run', boom)
    cluster.load_images(cluster.DOCKER_DESKTOP, ['img:1'])
    assert 'no loading' in capsys.readouterr().out


def test_hostpath_warning_only_for_vm_backed_clusters():
    assert 'extraMounts' in cluster.hostpath_warning(cluster.KIND)
    assert 'minikube mount' in cluster.hostpath_warning(cluster.MINIKUBE)
    assert cluster.hostpath_warning(cluster.K3S) is None
    assert cluster.hostpath_warning(cluster.DOCKER_DESKTOP) is None
    assert cluster.hostpath_warning(cluster.GENERIC_REMOTE) is None


def test_gpu_preflight_reports_both_problems_with_fixes(monkeypatch):
    run, _ = _fake_run({'version': '{}',
                        'get nodes -l videoflow.io/gpu-pool=true': '',
                        'get nodes -o name': 'node/gpu-box\n'})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight()
    assert len(problems) == 2
    assert 'kubectl label node gpu-box videoflow.io/gpu-pool=true' in problems[0]
    assert 'nvidia-device-plugin' in problems[1]


def test_gpu_preflight_ok(monkeypatch):
    run, _ = _fake_run({'version': '{}', 'gpu-pool=true': 'node/gpu-box', 'allocatable': '1'})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.gpu_preflight() == []


def test_gpu_preflight_unreachable_cluster_says_so(monkeypatch):
    '''An unreachable cluster answers every query with '', which must not be
    misreported as a missing label + missing device plugin.'''
    run, _ = _fake_run({})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight()
    assert len(problems) == 1
    assert 'cannot reach the cluster' in problems[0]


def test_gpu_preflight_flags_opt_in_nvidia_runtimeclass(monkeypatch):
    '''k3s registers an 'nvidia' RuntimeClass but leaves runc the node default, so a
    GPU pod without runtimeClassName schedules and then runs with no device.'''
    run, _ = _fake_run({'version': '{}', 'gpu-pool=true': 'node/gpu-box', 'allocatable': '1',
                        'get runtimeclass': 'crun nvidia nvidia-experimental'})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight()
    assert len(problems) == 1
    assert '--gpu-runtime-class nvidia' in problems[0]
    # ...and passing it silences the warning.
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.gpu_preflight(gpu_runtime_class = 'nvidia') == []


def test_gpu_preflight_reports_demand_over_capacity(monkeypatch):
    '''The offside case: 9 exclusive GPU claims against 1 allocatable device. The
    scheduler would bind one pod and strand the rest Pending — preflight must say
    so with the exact numbers, before anything is applied.'''
    run, _ = _fake_run({'version': '{}', 'gpu-pool=true': 'node/gpu-box',
                        'allocatable': '1'})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia',
                                     demand = {'nvidia.com/gpu': 9})
    assert len(problems) == 1
    assert 'demands 9 x nvidia.com/gpu' in problems[0]
    assert 'only 1 allocatable' in problems[0]
    assert '8 pod(s) will stay Pending' in problems[0]
    # Demand within capacity is clean.
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.gpu_preflight(gpu_runtime_class = 'nvidia',
                                 demand = {'nvidia.com/gpu': 1}) == []


def test_gpu_preflight_checks_each_requested_resource(monkeypatch):
    '''A node that requests a MIG profile the cluster does not expose must be
    reported against that resource name, not nvidia.com/gpu.'''
    run, _ = _fake_run({'version': '{}', 'gpu-pool=true': 'node/gpu-box',
                        'nvidia\\.com/gpu}': '2'})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia',
                                     demand = {'nvidia.com/gpu': 2,
                                               'nvidia.com/mig-1g.10gb': 1})
    assert len(problems) == 1
    assert 'nvidia.com/mig-1g.10gb' in problems[0]
    assert 'does not expose it' in problems[0]


def test_gpu_preflight_shared_mode(monkeypatch):
    '''Shared pods carry no resource limit, so capacity math and the device plugin
    are irrelevant — but without a runtime class on an opt-in cluster the pods
    would run device-less, which is the one shared-mode blocker.'''
    run, calls = _fake_run({'version': '{}', 'gpu-pool=true': 'node/gpu-box',
                            'get runtimeclass': 'nvidia'})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight(demand = {'nvidia.com/gpu': 9}, gpu_mode = 'shared')
    assert len(problems) == 1
    assert '--gpu-mode shared without --gpu-runtime-class' in problems[0]
    assert not any('allocatable' in ' '.join(c) for c in calls)  # no capacity query
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.gpu_preflight(gpu_runtime_class = 'nvidia',
                                 demand = {'nvidia.com/gpu': 9}, gpu_mode = 'shared') == []


def test_allocatable_gpus_sums_across_nodes(monkeypatch):
    run, _ = _fake_run({'version': '{}', 'allocatable': '1 4'})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.allocatable_gpus() == 5
