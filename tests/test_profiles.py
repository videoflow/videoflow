'''
Cluster profiles: the per-cluster deploy values in ~/.config/videoflow/clusters.yaml
— loading and validation, selection by name or by kubectl context, the mapping
onto each command's flags, and the precedence (flag > env > profile > default).

Pure/unit: the file lives in tmp_path; kubectl, the build and the engine are stubbed.
'''
import os

import pytest

from videoflow.core.errors import ConfigError
from videoflow.deploy import cli, profiles

FILE = '''
docker:
  build_args: '--build-arg http_proxy=http://proxy:3128'
clusters:
  lab:
    context: default
    namespace: videoflow
    registry: 10.0.0.1:5000
    push_tool: crane
    mount_pvc: ['work-share:/shared/videoflow']
    mount_home: /shared/videoflow/home
    priority_class: cluster-batch
    gpu_nodes: [gpu-01, gpu-02]
    nats: null
  laptop:
    context: kind-dev
'''


@pytest.fixture
def clusters_file(tmp_path, monkeypatch):
    path = tmp_path / 'clusters.yaml'
    path.write_text(FILE)
    monkeypatch.setenv(profiles.PROFILES_FILE_ENV, str(path))
    monkeypatch.delenv('VF_DOCKER_BUILD_ARGS', raising = False)
    monkeypatch.delenv('VF_DOCKER_RUN_ARGS', raising = False)
    return path


def test_load_validates_and_coerces(clusters_file):
    loaded = profiles.load_profiles()
    assert loaded.path == str(clusters_file)
    assert loaded.docker == {'build_args': '--build-arg http_proxy=http://proxy:3128'}
    lab = loaded.clusters['lab']
    assert lab['mount_pvc'] == ['work-share:/shared/videoflow'] and lab['gpu_nodes'] == ['gpu-01', 'gpu-02']
    assert 'nats' not in lab                          # null means unset
    assert loaded.clusters['laptop'] == {'context': 'kind-dev'}


def test_a_missing_file_is_empty_unless_named(tmp_path, monkeypatch):
    monkeypatch.setenv(profiles.PROFILES_FILE_ENV, str(tmp_path / 'nope.yaml'))
    assert profiles.load_profiles().clusters == {}
    with pytest.raises(ConfigError, match = 'not found'):
        profiles.load_profiles(str(tmp_path / 'nope.yaml'))
    monkeypatch.delenv(profiles.PROFILES_FILE_ENV)
    monkeypatch.setenv('XDG_CONFIG_HOME', str(tmp_path))
    assert profiles.default_profiles_path() == str(tmp_path / 'videoflow' / 'clusters.yaml')


@pytest.mark.parametrize('body, message', [
    ('clusters:\n  x:\n    regsitry: r\n', 'unknown key'),
    ('clusters:\n  x:\n    push_tool: skopeo\n', 'push_tool'),
    ('clusters:\n  x:\n    namespace: [a, b]\n', 'single value'),
    ('clusters:\n  x: 3\n', 'must be a mapping'),
    ('clustres: {}\n', 'unknown top-level'),
    ('docker:\n  proxy: x\n', 'docker section'),
    ('- a\n', 'must be a mapping'),
])
def test_load_rejects_bad_files_with_the_fix(tmp_path, body, message):
    path = tmp_path / 'clusters.yaml'
    path.write_text(body)
    with pytest.raises(ConfigError, match = message):
        profiles.load_profiles(str(path))


def test_select_by_name_or_by_context(clusters_file):
    loaded = profiles.load_profiles()
    assert profiles.select_profile(loaded, 'lab', lambda: 'other')[0] == 'lab'
    assert profiles.select_profile(loaded, None, lambda: 'kind-dev')[0] == 'laptop'
    assert profiles.select_profile(loaded, None, lambda: 'elsewhere') is None
    with pytest.raises(ConfigError, match = "No cluster profile named 'prod'"):
        profiles.select_profile(loaded, 'prod', lambda: 'default')
    # No profile names a context: kubectl is never asked.
    assert profiles.select_profile(profiles.Profiles(clusters = {'x': {}}), None, lambda: 1 / 0) is None


def test_command_defaults_map_onto_the_flags(clusters_file):
    lab = profiles.load_profiles().clusters['lab']
    deploy = profiles.command_defaults(lab, 'deploy')
    assert deploy['mount_pvc'] == ['work-share:/shared/videoflow'] and deploy['gpu_nodes'] == 'gpu-01,gpu-02'
    assert 'context' not in deploy
    assert profiles.command_defaults(lab, 'teardown') == {'namespace': 'videoflow'}
    assert profiles.command_defaults(lab, 'run-local') == {}


def test_docker_env_from_the_file_never_overrides_the_environment(clusters_file):
    loaded = profiles.load_profiles()
    env = {}
    assert profiles.apply_docker_env(loaded, env) == ['VF_DOCKER_BUILD_ARGS']
    assert env == {'VF_DOCKER_BUILD_ARGS': '--build-arg http_proxy=http://proxy:3128'}
    env = {'VF_DOCKER_BUILD_ARGS': '--network=host'}
    assert profiles.apply_docker_env(loaded, env) == []
    assert env == {'VF_DOCKER_BUILD_ARGS': '--network=host'}


# -- through the CLI --------------------------------------------------------------------

GRAPH = '''
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer

def build_flow(cfg = None):
    p = IntProducer(0, 3, name = 'numbers')
    out = CommandlineConsumer(name = 'printer')(p)
    return Flow([out], flow_type = BATCH, flow_id = 'render')
'''


@pytest.fixture
def deploy_stubs(tmp_path, monkeypatch, clusters_file):
    (tmp_path / 'mygraph.py').write_text(GRAPH)
    (tmp_path / 'Dockerfile').write_text('FROM x\n')
    seen = {'pushed': [], 'context': 'default'}
    monkeypatch.setattr(cli, 'current_context', lambda kubectl: seen['context'])
    monkeypatch.setattr(cli, 'docker_gpus_available', lambda: False)
    monkeypatch.setattr(cli, 'autobuild', lambda graph_dir, **kw: 'videoflow-mygraph:abc123')
    monkeypatch.setattr(cli, 'image_exists', lambda ref: ref == 'videoflow-mygraph:abc123')
    monkeypatch.setattr(cli, 'push_image',
                        lambda ref, registry, tool: seen['pushed'].append((ref, registry, tool)) or f'{registry}/{ref}')
    return tmp_path, seen


def _deploy(tmp_path, *extra):
    return cli.main(['deploy', str(tmp_path / 'mygraph.py'), '--non-interactive', '--run-id', 'r1', '--dry-run', *extra])


def test_the_profile_matching_the_context_feeds_deploy(deploy_stubs, capsys):
    tmp_path, seen = deploy_stubs
    assert _deploy(tmp_path) == 0
    out, err = capsys.readouterr()
    assert "Using cluster profile 'lab' (context 'default')" in err
    assert seen['pushed'] == [] and 'not pushed to 10.0.0.1:5000' in err     # a dry run never pushes
    assert 'image: 10.0.0.1:5000/videoflow-mygraph:abc123' in out
    assert 'namespace: videoflow' in out and 'priorityClassName: cluster-batch' in out
    assert 'claimName: work-share' in out
    assert os.environ['VF_DOCKER_BUILD_ARGS'] == '--build-arg http_proxy=http://proxy:3128'


def test_an_explicit_flag_beats_the_profile(deploy_stubs, capsys):
    tmp_path, seen = deploy_stubs
    assert _deploy(tmp_path, '--namespace', 'mine', '--registry', 'ghcr.io/acme', '--push-tool', 'docker') == 0
    out = capsys.readouterr().out
    assert 'namespace: mine' in out and 'namespace: videoflow' not in out
    assert 'image: ghcr.io/acme/videoflow-mygraph:abc123' in out


def test_cluster_by_name_and_no_match_by_context(deploy_stubs, capsys):
    tmp_path, seen = deploy_stubs
    seen['context'] = 'somewhere-else'
    assert _deploy(tmp_path) == 0
    out, err = capsys.readouterr()
    assert 'Using cluster profile' not in err and 'not pushed' not in err and 'namespace: default' in out
    assert _deploy(tmp_path, '--cluster', 'lab') == 0
    err = capsys.readouterr().err
    assert "Using cluster profile 'lab'" in err and 'not pushed to 10.0.0.1:5000' in err
    assert _deploy(tmp_path, '--cluster', 'prod') == 2
    assert "No cluster profile named 'prod'" in capsys.readouterr().err


def test_run_local_takes_only_the_docker_section(tmp_path, monkeypatch, clusters_file):
    seen = {}
    monkeypatch.setattr(cli, '_cmd_run_local', lambda args: seen.update(vars(args)))
    monkeypatch.setattr(cli, 'current_context', lambda kubectl: 1 / 0)      # never consulted
    (tmp_path / 'graph.py').write_text(GRAPH)
    assert cli.main(['run-local', str(tmp_path / 'graph.py')]) == 0
    assert seen['cluster'] is None and 'namespace' not in seen
    assert os.environ['VF_DOCKER_BUILD_ARGS'] == '--build-arg http_proxy=http://proxy:3128'


def test_teardown_takes_its_namespace_and_broker_from_the_profile(tmp_path, monkeypatch, clusters_file):
    seen = {}
    monkeypatch.setattr(cli, '_cmd_teardown', lambda args: seen.update(vars(args)))
    monkeypatch.setattr(cli, 'current_context', lambda kubectl: 'default')
    assert cli.main(['teardown', '--flow-id', 'f', '--run-id', 'r', '--nats', 'nats://x:4222']) == 0
    assert seen['namespace'] == 'videoflow' and seen['nats'] == 'nats://x:4222'
    # The profile can name the broker too; without either, teardown says what it needs.
    clusters_file.write_text(FILE.replace('nats: null', 'nats: nats://nats.videoflow.svc:4222'))
    assert cli.main(['teardown', '--flow-id', 'f', '--run-id', 'r']) == 0
    assert seen['nats'] == 'nats://nats.videoflow.svc:4222'


def test_teardown_without_a_broker_url_is_a_config_error(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv(profiles.PROFILES_FILE_ENV, str(tmp_path / 'none.yaml'))
    assert cli.main(['teardown', '--flow-id', 'f', '--run-id', 'r']) == 2
    assert '--nats' in capsys.readouterr().err


def test_other_commands_never_read_the_file(tmp_path, monkeypatch):
    monkeypatch.setenv(profiles.PROFILES_FILE_ENV, str(tmp_path / 'broken.yaml'))
    (tmp_path / 'broken.yaml').write_text('- not a mapping\n')
    (tmp_path / 'c.yaml').write_text('apiVersion: videoflow.io/v1\n')
    # `component validate` parses fine (and fails on the descriptor, not the profiles file).
    assert cli.main(['component', 'validate', str(tmp_path / 'c.yaml')]) == 2
    (tmp_path / 'mygraph.py').write_text(GRAPH)
    # deploy does read it, and reports the file rather than a traceback.
    assert cli.main(['deploy', str(tmp_path / 'mygraph.py'), '--dry-run', '--non-interactive']) == 2


def test_broker_replicas_is_handed_to_the_parser_as_an_integer(tmp_path, monkeypatch):
    path = tmp_path / 'clusters.yaml'
    path.write_text('clusters:\n  lab:\n    context: default\n    broker_profile: durable\n    broker_replicas: 1\n')
    monkeypatch.setenv(profiles.PROFILES_FILE_ENV, str(path))
    lab = profiles.load_profiles().clusters['lab']
    assert lab['broker_replicas'] == 1 and isinstance(lab['broker_replicas'], int)
    assert profiles.command_defaults(lab, 'deploy')['broker_replicas'] == 1
    # teardown never takes the profile name from the file: the Service records what was rendered.
    assert profiles.command_defaults(lab, 'teardown') == {}
    path.write_text('clusters:\n  lab:\n    context: default\n    broker_replicas: three\n')
    with pytest.raises(ConfigError, match = 'broker_replicas must be an integer'):
        profiles.load_profiles()
