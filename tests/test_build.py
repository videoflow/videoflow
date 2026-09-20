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
        if cmd[:4] == ['docker', 'image', 'inspect', '--format']:
            return _Proc(stdout = 'sha256:0123456789abcdef0123456789abcdef\n')   # the built image's id
        if cmd[:3] == ['docker', 'image', 'inspect']:
            return _Proc(returncode = 1)                      # base missing → build it
        if cmd[0] == 'git':
            return _Proc(stdout = str(tmp_path) + '\n')
        return _Proc()
    monkeypatch.setattr(subprocess, 'run', run)
    tag = build.autobuild(str(tmp_path), needs_gpu = True)
    # Deployed under a content-addressed tag; :latest keeps the layer cache warm.
    assert tag == f'videoflow-{tmp_path.name}:0123456789ab'
    builds = [c for c in calls if c[:2] == ['docker', 'build']]
    assert len(builds) == 2
    assert '-t' in builds[0] and 'videoflow-base:py3.12-cuda' in builds[0]
    assert 'Dockerfile.gpu' in builds[0][builds[0].index('-f') + 1]
    assert f'videoflow-{tmp_path.name}:latest' in builds[1] and str(tmp_path) == builds[1][-1]
    assert ['docker', 'tag', f'videoflow-{tmp_path.name}:latest', tag] in calls


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


# -- which Dockerfile: the flow decides, never the docker daemon ----------------------

def test_resolve_needs_gpu_follows_the_declaration_then_the_files_then_the_specs(tmp_path):
    class _Spec:
        def __init__(self, device_type):
            self.device_type = device_type
    # Declared wins over everything, including a solution that ships only one file.
    (tmp_path / 'Dockerfile').write_text('FROM x')
    assert build.resolve_needs_gpu(str(tmp_path), True, None) == (True, None)
    assert build.resolve_needs_gpu(str(tmp_path), False, [_Spec('gpu')]) == (False, None)
    # One file: that one.
    assert build.resolve_needs_gpu(str(tmp_path), None, [_Spec('gpu')]) == (False, None)
    (tmp_path / 'Dockerfile').unlink()
    (tmp_path / 'gpu.Dockerfile').write_text('FROM y')
    assert build.resolve_needs_gpu(str(tmp_path), None, None) == (True, None)
    # Both files: the compiled graph's device placement when it is known...
    (tmp_path / 'Dockerfile').write_text('FROM x')
    assert build.resolve_needs_gpu(str(tmp_path), None, [_Spec('cpu'), _Spec('gpu')]) == (True, None)
    assert build.resolve_needs_gpu(str(tmp_path), None, [_Spec('cpu')]) == (False, None)
    # ...else CPU, with a note that names the fix.
    needs_gpu, note = build.resolve_needs_gpu(str(tmp_path), None, None)
    assert needs_gpu is False and 'x-gpu' in note


def test_resolve_needs_gpu_without_any_dockerfile_is_cpu_and_quiet(tmp_path):
    assert build.resolve_needs_gpu(str(tmp_path), None, None) == (False, None)


# -- the machine's docker flags: VF_DOCKER_BUILD_ARGS / VF_DOCKER_RUN_ARGS ------------

def test_build_args_env_is_spliced_into_every_docker_build(monkeypatch):
    seen = _capture_argv(monkeypatch)
    monkeypatch.setenv('VF_DOCKER_BUILD_ARGS', '--build-arg http_proxy=http://proxy:3128 --network=host')
    build.build_image('/sol/Dockerfile', '/ctx', 'sol:latest')
    assert seen['cmd'] == ['docker', 'build', '--build-arg', 'http_proxy=http://proxy:3128', '--network=host',
                           '-f', '/sol/Dockerfile', '-t', 'sol:latest', '/ctx']
    monkeypatch.delenv('VF_DOCKER_BUILD_ARGS')
    build.build_image('/sol/Dockerfile', '/ctx', 'sol:latest')
    assert seen['cmd'] == ['docker', 'build', '-f', '/sol/Dockerfile', '-t', 'sol:latest', '/ctx']


def test_run_args_env_and_env_pairs_reach_run_in_image(monkeypatch):
    seen = _capture_argv(monkeypatch)
    monkeypatch.setenv('VF_DOCKER_RUN_ARGS', '--network host')
    build.run_in_image('img:1', ['python', 'prepare.py'], env = {'VF_SOLUTION_CONFIG': '/sol/config.yaml'})
    cmd = seen['cmd']
    assert cmd[:5] == ['docker', 'run', '--rm', '--network', 'host']
    assert cmd[cmd.index('-e') + 1] == 'VF_SOLUTION_CONFIG=/sol/config.yaml'
    monkeypatch.delenv('VF_DOCKER_RUN_ARGS')
    build.run_in_image('img:1', ['python', 'prepare.py'])
    assert seen['cmd'][:4] == ['docker', 'run', '--rm', '--entrypoint']


# -- content-addressed tags and the registry push --------------------------------------

def test_content_tag_names_the_image_by_its_id(monkeypatch):
    calls = []
    def run(cmd, **kw):
        calls.append(cmd)
        if cmd[:4] == ['docker', 'image', 'inspect', '--format']:
            return _Proc(stdout = 'sha256:fedcba9876543210fedcba9876543210\n')
        return _Proc()
    monkeypatch.setattr(subprocess, 'run', run)
    assert build.content_tag('videoflow-x:latest') == 'videoflow-x:fedcba987654'
    assert ['docker', 'tag', 'videoflow-x:latest', 'videoflow-x:fedcba987654'] in calls
    # A registry port is not a tag.
    assert build.content_tag('10.0.0.1:5000/videoflow-x:latest') == '10.0.0.1:5000/videoflow-x:fedcba987654'
    assert build.content_tag('10.0.0.1:5000/videoflow-x') == '10.0.0.1:5000/videoflow-x:fedcba987654'


def test_content_tag_falls_back_to_the_mutable_tag_with_a_warning(monkeypatch, capsys):
    monkeypatch.setattr(subprocess, 'run', lambda cmd, **kw: _Proc(returncode = 1))
    assert build.content_tag('videoflow-x:latest') == 'videoflow-x:latest'
    assert 'mutable tag' in capsys.readouterr().err


def test_push_image_with_docker_tags_and_pushes(monkeypatch):
    calls = []
    monkeypatch.setattr(subprocess, 'run', lambda cmd, **kw: calls.append(cmd) or _Proc())
    ref = build.push_image('videoflow-x:abc', 'ghcr.io/acme')
    assert ref == 'ghcr.io/acme/videoflow-x:abc'
    assert calls == [['docker', 'tag', 'videoflow-x:abc', ref], ['docker', 'push', ref]]


def test_push_image_with_crane_goes_through_a_tarball(monkeypatch, tmp_path):
    monkeypatch.setenv('VF_IMAGE_TMPDIR', str(tmp_path))
    calls = []
    def run(cmd, **kw):
        calls.append(cmd)
        if cmd[:2] == ['docker', 'save']:
            open(cmd[cmd.index('-o') + 1], 'wb').close()          # the tarball crane pushes
        return _Proc()
    monkeypatch.setattr(subprocess, 'run', run)
    ref = build.push_image('videoflow-x:abc', '10.0.0.1:5000/', tool = 'crane')
    tarball = str(tmp_path / 'videoflow-x-abc.tar')
    assert ref == '10.0.0.1:5000/videoflow-x:abc'
    assert calls == [['docker', 'save', 'videoflow-x:abc', '-o', tarball],
                     ['crane', 'push', '--insecure', tarball, ref],
                     ['crane', 'manifest', '--insecure', ref],
                     ['docker', 'tag', 'videoflow-x:abc', ref]]           # so `docker run <ref>` finds it
    assert not (tmp_path / 'videoflow-x-abc.tar').exists()                # cleaned up


def test_push_image_names_the_missing_tool_and_the_failed_step(monkeypatch, tmp_path):
    monkeypatch.setenv('VF_IMAGE_TMPDIR', str(tmp_path))
    def no_crane(cmd, **kw):
        if cmd[0] == 'crane':
            raise FileNotFoundError('crane')
        return _Proc()
    monkeypatch.setattr(subprocess, 'run', no_crane)
    with pytest.raises(RuntimeError, match = 'go-containerregistry'):
        build.push_image('videoflow-x:abc', 'r:5000', tool = 'crane')
    monkeypatch.setattr(subprocess, 'run', lambda cmd, **kw: _Proc(returncode = 1))
    with pytest.raises(RuntimeError, match = 'docker tag'):
        build.push_image('videoflow-x:abc', 'r:5000')
    with pytest.raises(ValueError, match = 'unknown push tool'):
        build.push_image('videoflow-x:abc', 'r:5000', tool = 'skopeo')
