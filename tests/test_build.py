'''
Image auto-build: Dockerfile selection (gpu.Dockerfile for GPU flows), build
context via the enclosing git root, ARG BASE_IMAGE parsing, and the default tag.

Pure/unit: subprocess is monkeypatched — no docker.
'''
import subprocess

import pytest

from videoflow.deploy import build


class _Proc:
    def __init__(self, returncode = 0, stdout = ''):
        self.returncode = returncode
        self.stdout = stdout


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
