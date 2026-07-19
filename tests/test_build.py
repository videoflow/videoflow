'''
Image auto-build: Dockerfile selection (gpu.Dockerfile for GPU flows), build
context via the enclosing git root, ARG BASE_IMAGE parsing, and the default tag.

Pure/unit: subprocess is monkeypatched — no docker.
'''
import subprocess

import pytest

from videoflow.deploy import build


class _Proc:
    def __init__(self, returncode = 0, stdout = '', stderr = ''):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_find_dockerfile_prefers_gpu_for_gpu_flows(tmp_path):
    (tmp_path / 'Dockerfile').write_text('FROM x')
    (tmp_path / 'gpu.Dockerfile').write_text('FROM y')
    assert build.find_dockerfile(str(tmp_path), needs_gpu = True).endswith('gpu.Dockerfile')
    assert build.find_dockerfile(str(tmp_path), needs_gpu = False).endswith('/Dockerfile')


def test_find_dockerfile_falls_back_and_none(tmp_path):
    assert build.find_dockerfile(str(tmp_path), needs_gpu = True) is None
    (tmp_path / 'Dockerfile').write_text('FROM x')
    # GPU flow with only a CPU Dockerfile still builds it (image may support both).
    assert build.find_dockerfile(str(tmp_path), needs_gpu = True).endswith('/Dockerfile')


def test_build_context_git_root_and_override(monkeypatch, tmp_path):
    monkeypatch.setattr(subprocess, 'run',
                        lambda cmd, **kw: _Proc(stdout = '/repo/root\n'))
    assert build.build_context_for('/repo/root/solutions/x') == '/repo/root'
    assert build.build_context_for('/repo/root/solutions/x', override = str(tmp_path)) == str(tmp_path)
    monkeypatch.setattr(subprocess, 'run', lambda cmd, **kw: _Proc(returncode = 128))
    assert build.build_context_for('/not/a/repo') == '/not/a/repo'


def test_base_image_for_parses_arg(tmp_path):
    df = tmp_path / 'gpu.Dockerfile'
    df.write_text('# comment\nARG BASE_IMAGE=videoflow-base:py3.12-cuda\nFROM ${BASE_IMAGE}\n')
    assert build.base_image_for(str(df)) == 'videoflow-base:py3.12-cuda'
    df.write_text('FROM python:3.12-slim\n')
    assert build.base_image_for(str(df)) is None


def test_default_tag_from_graph_dir_name():
    assert build.default_tag('/repo/solutions/offside') == 'videoflow-offside:latest'


def test_autobuild_builds_base_then_solution(monkeypatch, tmp_path):
    (tmp_path / 'gpu.Dockerfile').write_text('ARG BASE_IMAGE=videoflow-base:py3.12-cuda\nFROM ${BASE_IMAGE}\n')
    calls = []
    def run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[:3] == ['docker', 'image', 'inspect']:
            return _Proc(returncode = 1)                      # base missing → build it
        if cmd[0] == 'git':
            return _Proc(stdout = str(tmp_path) + '\n')
        return _Proc()
    monkeypatch.setattr(subprocess, 'run', run)
    tag = build.autobuild(str(tmp_path), needs_gpu = True)
    assert tag == f'videoflow-{tmp_path.name}:latest'
    builds = [c for c in calls if c[:2] == ['docker', 'build']]
    assert len(builds) == 2
    assert '-t' in builds[0] and 'videoflow-base:py3.12-cuda' in builds[0]
    assert 'Dockerfile.gpu' in builds[0][builds[0].index('-f') + 1]
    assert tag in builds[1] and str(tmp_path) == builds[1][-1]


def test_autobuild_returns_none_without_dockerfile(tmp_path):
    assert build.autobuild(str(tmp_path), needs_gpu = False) is None


def test_ensure_base_errors_for_wheel_installs(monkeypatch, tmp_path):
    monkeypatch.setattr(build, 'image_exists', lambda ref: False)
    import videoflow
    # Simulate a wheel install: point the package at a tree without docker/base/.
    monkeypatch.setattr(videoflow, '__file__', str(tmp_path / 'videoflow' / '__init__.py'))
    with pytest.raises(RuntimeError, match = 'build-images.sh'):
        build.ensure_base_image('videoflow-base:py3.12')


# -- run_in_image: the docker -v flags built from Mount records ---------------
# This is the second consumer of parse_mounts output (the first is _pod_spec).
# It had no coverage, so a change to the Mount shape broke it silently at deploy
# time rather than in CI.

def _capture_argv(monkeypatch, returncode = 0, stdout = 'out'):
    seen = {}
    def run(cmd, **kw):
        seen['cmd'] = cmd
        return _Proc(returncode = returncode, stdout = stdout)
    monkeypatch.setattr(subprocess, 'run', run)
    return seen


def _v_flags(cmd):
    return [cmd[i + 1] for i, a in enumerate(cmd) if a == '-v']


def test_run_in_image_builds_v_flags_from_mounts(monkeypatch):
    from videoflow.deploy.manifests import parse_mounts
    seen = _capture_argv(monkeypatch)
    mounts = parse_mounts(['/data/in:/data/in:ro', '/work', '/a:/b'])
    build.run_in_image('img:1', ['python', '-c', 'pass'], mounts = mounts)
    # read-only keeps its :ro suffix; read-write has none; shorthand maps both sides.
    assert _v_flags(seen['cmd']) == ['/data/in:/data/in:ro', '/work:/work', '/a:/b']


def test_run_in_image_accepts_concatenated_mounts_with_duplicate_names(monkeypatch):
    '''
    cli.py builds ``parse_mounts([graph_dir]) + mounts``, and each parse_mounts call
    numbers from vf-mount-0, so the concatenation repeats volume names. That is fine
    here — docker addresses mounts by path, not name — and this is the only consumer
    allowed to receive such a list. ``_pod_spec`` rejects it instead.
    '''
    from videoflow.deploy.manifests import parse_mounts
    seen = _capture_argv(monkeypatch)
    container_mounts = parse_mounts(['/graph/dir']) + parse_mounts(['/data:/data:ro'])
    assert len({m.name for m in container_mounts}) == 1   # names collide...
    build.run_in_image('img:1', ['python'], mounts = container_mounts)
    # ...but both mounts still reach docker, addressed by path.
    assert _v_flags(seen['cmd']) == ['/graph/dir:/graph/dir', '/data:/data:ro']


def test_run_in_image_argv_shape_and_capture(monkeypatch):
    seen = _capture_argv(monkeypatch, stdout = 'compiled')
    out = build.run_in_image('img:1', ['videoflow-compile', '--x'], mounts = None,
                             workdir = '/w', gpus = True, capture = True)
    cmd = seen['cmd']
    assert out == 'compiled'
    assert cmd[:3] == ['docker', 'run', '--rm']
    assert ['--gpus', 'all'] == cmd[cmd.index('--gpus'):cmd.index('--gpus') + 2]
    assert ['-w', '/w'] == cmd[cmd.index('-w'):cmd.index('-w') + 2]
    # The base image bakes in the worker entrypoint; it must be overridden.
    assert cmd[cmd.index('--entrypoint') + 1] == 'videoflow-compile'
    assert cmd[-2:] == ['img:1', '--x']


def test_run_in_image_raises_with_stderr_on_failure(monkeypatch):
    _capture_argv(monkeypatch, returncode = 1)
    with pytest.raises(RuntimeError, match = 'command failed in img:1'):
        build.run_in_image('img:1', ['boom'], capture = True)
