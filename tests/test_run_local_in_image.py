'''
`run-local` with the workers inside the solution image: the path a graph takes
when its dependencies are not installed on this host (an ML solution whose stack
lives only in its image) or under --in-image. The prepare hook and the compile
run in the image, the engine gets the compiled specs plus the image and its
mounts, and the run loop is the same one a host run uses.

Pure/unit: the graph load, the build, docker and the engine are all monkeypatched.
'''
import json
import os
import sys

import pytest

from videoflow.core.errors import EXIT_FLOW_FAILED, EXIT_USER
from videoflow.deploy import admission, cli, localinfra, solution
from videoflow.deploy.compile import compile_to_dict

GRAPH = '''
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer

def build_flow(cfg = None):
    p = IntProducer(0, 3, name = 'producer')
    out = CommandlineConsumer(name = 'printer')(p)
    return Flow([out], flow_type = BATCH, flow_id = 'demo')
'''

TEMPLATE = '''
work_dir: ./out
input_video: /data/clip.mp4
device: cpu
x-questions: []
x-mounts:
  - '{input_video}:ro'
  - '{work_dir}'
  - 'pvc:shared:/mnt/shared'
x-gpu:
  - '{device}'
'''


class _FakeEngine:
    instances: list = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls = []
        self._failures = []
        _FakeEngine.instances.append(self)

    def allocate_and_run_tasks(self, tasks_data, flow_id, flow_type, run_id):
        self.calls.append(('run', tasks_data, flow_id, flow_type, run_id))

    def join_task_processes(self):
        self.calls.append('join')

    def signal_flow_termination(self):
        self.calls.append('stop')

    def cleanup_containers(self):
        self.calls.append('cleanup')

    def failures(self):
        return self._failures

    def report_failures(self):
        print('reported', file = sys.stderr)


@pytest.fixture
def staged(tmp_path, monkeypatch):
    '''
    A solution on disk (graph, template, config, prepare hook, Dockerfile) whose
    graph does NOT import here, plus stubs recording the build, every docker run
    and the engine. Returns (solution dir, recorder).
    '''
    (tmp_path / 'graph.py').write_text(GRAPH)
    (tmp_path / 'config.template.yaml').write_text(TEMPLATE)
    (tmp_path / 'config.yaml').write_text('work_dir: ./out\ninput_video: /data/clip.mp4\ndevice: cpu\n')
    (tmp_path / 'prepare.py').write_text('print("prep")\n')
    (tmp_path / 'Dockerfile').write_text('ARG BASE_IMAGE=videoflow-base:py3.12\nFROM ${BASE_IMAGE}\n')
    calls = []
    _FakeEngine.instances.clear()
    document = json.dumps(compile_to_dict(str(tmp_path / 'graph.py')))

    def missing_deps(target):
        calls.append('load')
        raise ImportError("No module named 'videoflow_contrib'")

    def run_in_image(image, command, mounts = None, workdir = None, gpus = False,
                     capture = False, interactive = False, env = None):
        calls.append(('docker', image, list(command), [f'{m.host_path}:{m.container_path}' for m in mounts or []],
                      workdir, gpus, capture, dict(env or {})))
        return document if capture else None

    monkeypatch.setattr(cli, '_load_flow', missing_deps)
    monkeypatch.setattr(cli, 'autobuild', lambda graph_dir, **kw: calls.append(('build', kw)) or 'videoflow-sol:abc123')
    monkeypatch.setattr(cli, 'run_in_image', run_in_image)
    monkeypatch.setattr(cli, 'docker_gpus_available', lambda: False)
    monkeypatch.setattr(solution, 'run_prepare_local',
                        lambda d, c = None: calls.append('prepare-on-host') or True)
    monkeypatch.setattr('videoflow.engines.local.LocalProcessEngine', _FakeEngine)
    monkeypatch.setattr(localinfra, 'ensure_local_infra',
                        lambda **kw: calls.append(('ensure', kw)) or (localinfra.local_infra_urls(), ['nats', 'redis']))
    monkeypatch.setattr(localinfra, 'wait_local_infra_ready', lambda *a, **kw: None)
    monkeypatch.setattr(localinfra, 'teardown_local_infra', lambda c: calls.append(('teardown', c)))
    monkeypatch.setattr(cli, 'jetstream_capabilities_observed',
                        lambda url, **kw: admission.jetstream_capabilities(None))
    monkeypatch.setattr(cli, 'redis_payload_capabilities_observed',
                        lambda url, **kw: admission.redis_payload_capabilities(None))
    monkeypatch.setenv(solution.CONFIG_ENV, 'reset-by-fixture')
    monkeypatch.delenv(solution.CONFIG_ENV)
    return tmp_path, calls


def _run(tmp_path, *extra):
    return cli.main(['run-local', str(tmp_path / 'graph.py'), *extra])


def _docker_calls(calls):
    return [c for c in calls if isinstance(c, tuple) and c[0] == 'docker']


def test_a_graph_that_does_not_import_here_runs_in_its_image(staged):
    tmp_path, calls = staged
    assert _run(tmp_path) == 0
    # Built once, from the flow's declaration (x-gpu says cpu), never from the daemon.
    assert [c for c in calls if isinstance(c, tuple) and c[0] == 'build'] == [('build', {'needs_gpu': False, 'context_override': None})]
    # The prepare hook ran in the image, not on this host; then the compile did.
    assert 'prepare-on-host' not in calls
    prepare, compile_ = _docker_calls(calls)
    assert prepare[1] == 'videoflow-sol:abc123'
    assert prepare[2] == ['python', 'prepare.py', '--config', str(tmp_path / 'config.yaml')]
    assert prepare[4] == str(tmp_path) and prepare[6] is False
    assert compile_[2][:3] == ['python', '-m', 'videoflow.compile'] and compile_[6] is True
    # Both containers see the solution directory and the solution's host mounts;
    # the claim entry is not a docker mount.
    for call in (prepare, compile_):
        assert call[3] == [f'{tmp_path}:{tmp_path}', '/data/clip.mp4:/data/clip.mp4', f'{tmp_path}/out:{tmp_path}/out']
        assert call[7] == {solution.CONFIG_ENV: str(tmp_path / 'config.yaml')}
    # The engine got the compiled specs, the image and the worker mounts, and ran
    # them through the same run/join loop a host run uses.
    engine = _FakeEngine.instances[-1]
    assert engine.kwargs['worker_image'] == 'videoflow-sol:abc123'
    assert engine.kwargs['default_image'] == 'videoflow-sol:abc123'
    assert [s.name for s in engine.kwargs['specs']] == ['producer', 'printer']
    assert [m.container_path for m in engine.kwargs['worker_mounts']] == ['/data/clip.mp4', f'{tmp_path}/out']
    assert engine.kwargs['docker_gpus'] is False
    assert engine.calls[0][:4] == ('run', None, 'demo', 'batch') and engine.calls[1:] == ['join', 'cleanup']
    assert ('teardown', ['nats', 'redis']) in calls
    assert os.environ[solution.CONFIG_ENV] == str(tmp_path / 'config.yaml')


def test_the_claim_entry_is_reported_not_mounted(staged, capsys):
    tmp_path, _calls = staged
    _run(tmp_path)
    assert 'shared:/mnt/shared' in capsys.readouterr().err


def test_in_image_forces_the_image_for_an_importable_graph(staged, monkeypatch):
    tmp_path, calls = staged
    loaded = []
    monkeypatch.setattr(cli, '_load_flow', lambda target: loaded.append(target) or cli.load_flow(target))
    assert _run(tmp_path, '--in-image') == 0
    assert loaded == []                                  # never probed, never loaded here
    assert _FakeEngine.instances[-1].kwargs['worker_image'] == 'videoflow-sol:abc123'
    assert len(_docker_calls(calls)) == 2


def test_an_explicit_image_is_used_without_a_build(staged, calls_ = None):
    tmp_path, calls = staged
    assert _run(tmp_path, '--image', 'ghcr.io/acme/sol:1') == 0
    assert not any(isinstance(c, tuple) and c[0] == 'build' for c in calls)
    assert _FakeEngine.instances[-1].kwargs['worker_image'] == 'ghcr.io/acme/sol:1'


def test_no_build_without_an_image_is_a_config_error(staged, capsys):
    tmp_path, calls = staged
    assert _run(tmp_path, '--no-build') == EXIT_USER
    err = capsys.readouterr().err
    assert 'videoflow_contrib' in err and '--image' in err
    assert not _FakeEngine.instances and not _docker_calls(calls)


def test_no_dockerfile_means_the_host_path_and_its_own_error(staged, capsys):
    tmp_path, calls = staged
    (tmp_path / 'Dockerfile').unlink()
    # Nothing could run in an image, so the graph is loaded on the host (once,
    # after prepare) and its ImportError is what the operator sees.
    with pytest.raises(ImportError):
        _run(tmp_path)
    assert calls.index('prepare-on-host') < calls.index('load')
    assert not _docker_calls(calls)


def test_no_prepare_skips_the_in_image_hook(staged):
    tmp_path, calls = staged
    assert _run(tmp_path, '--no-prepare') == 0
    assert [c[2][:3] for c in _docker_calls(calls)] == [['python', '-m', 'videoflow.compile']]


def test_a_config_outside_the_solution_is_mounted_read_only(staged, tmp_path_factory):
    tmp_path, calls = staged
    elsewhere = tmp_path_factory.mktemp('cfg') / 'other.yaml'
    elsewhere.write_text('work_dir: ./out\ninput_video: /data/clip.mp4\ndevice: cpu\n')
    assert _run(tmp_path, '--config', str(elsewhere)) == 0
    prepare = _docker_calls(calls)[0]
    assert f'{os.path.realpath(elsewhere)}:{elsewhere}' in prepare[3]
    assert prepare[7] == {solution.CONFIG_ENV: str(elsewhere)}


def test_failed_workers_exit_four_and_containers_are_cleaned_up(staged, monkeypatch):
    tmp_path, _calls = staged
    original = _FakeEngine.__init__

    def failing_init(self, **kwargs):
        original(self, **kwargs)
        self._failures = [('printer', 0, 1)]
    monkeypatch.setattr(_FakeEngine, '__init__', failing_init)
    assert _run(tmp_path) == EXIT_FLOW_FAILED
    assert 'cleanup' in _FakeEngine.instances[-1].calls


def test_ctrl_c_stops_the_flow_and_still_cleans_up(staged, monkeypatch):
    tmp_path, calls = staged

    def interrupted(self):
        self.calls.append('join')
        if self.calls.count('join') == 1:
            raise KeyboardInterrupt()
    monkeypatch.setattr(_FakeEngine, 'join_task_processes', interrupted)
    assert _run(tmp_path) == 0
    engine = _FakeEngine.instances[-1]
    assert engine.calls[-3:] == ['stop', 'join', 'cleanup']
    assert ('teardown', ['nats', 'redis']) in calls


def test_the_gpu_image_is_built_when_the_config_says_gpu(staged):
    tmp_path, calls = staged
    (tmp_path / 'gpu.Dockerfile').write_text('ARG BASE_IMAGE=videoflow-base:py3.12-cuda\nFROM ${BASE_IMAGE}\n')
    (tmp_path / 'config.yaml').write_text('work_dir: ./out\ninput_video: /data/clip.mp4\ndevice: gpu\n')
    assert _run(tmp_path) == 0
    assert ('build', {'needs_gpu': True, 'context_override': None}) in calls
