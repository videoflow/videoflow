'''
CLI wiring for `run-local` (and the shared prepare hook it now has in common with
`deploy`): infra ownership, step ordering, blob-redis defaulting, and the
non-zero exit when a node fails.

Pure/unit: infra, the engine and flow execution are all monkeypatched.
'''
import subprocess
import sys

import pytest

from videoflow.core.errors import EXIT_ENVIRONMENT, EXIT_FLOW_FAILED, EXIT_USER
from videoflow.deploy import admission, cli, localinfra, solution
from videoflow.deploy.broker_profiles import RedisProfile

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


class _FakeEngine:
    instances: list = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self._failures = []
        _FakeEngine.instances.append(self)

    def failures(self):
        return self._failures

    def report_failures(self):
        print('reported', file = sys.stderr)

    # The in-image path runs precompiled specs through the engine directly.
    def allocate_and_run_tasks(self, *args):
        self.ran = args

    def join_task_processes(self):
        pass

    def cleanup_containers(self):
        pass


class _FakeFlow:
    flow_type = 'realtime'          # run-local admits the composition per flow type

    def __init__(self, tasks_data = None):
        self.flow_id = 'demo'
        self.run_id = 'run1'
        self.joined = False
        self._tasks_data = tasks_data or []

    def tasks_data(self):
        '''run-local inspects the flow to decide whether it must build an image.'''
        return self._tasks_data

    def run(self, engine, run_id = None):
        self.engine = engine

    def join(self):
        self.joined = True

    def stop(self):
        pass


@pytest.fixture
def wiring(tmp_path, monkeypatch):
    '''A graph on disk plus stubbed engine/flow/infra; returns a call recorder.'''
    (tmp_path / 'graph.py').write_text(GRAPH)
    calls = []
    _FakeEngine.instances.clear()

    monkeypatch.setattr(cli, '_load_flow', lambda t: calls.append('load') or _FakeFlow())
    monkeypatch.setattr('videoflow.engines.local.LocalProcessEngine', _FakeEngine)
    monkeypatch.setattr(localinfra, 'ensure_local_infra',
                        lambda **kw: calls.append(('ensure', kw)) or
                        (localinfra.local_infra_urls(), ['nats', 'redis']))
    monkeypatch.setattr(localinfra, 'wait_local_infra_ready', lambda *a, **kw: None)
    monkeypatch.setattr(localinfra, 'teardown_local_infra',
                        lambda c: calls.append(('teardown', c)))
    monkeypatch.setattr(solution, 'run_prepare_local',
                        lambda d, c = None: calls.append('prepare') or True)
    # The live read-back of a bring-your-own broker/store, recorded rather than run.
    monkeypatch.setattr(cli, 'jetstream_capabilities_observed',
                        lambda url, **kw: calls.append(('probe-nats', url)) or admission.jetstream_capabilities(None))
    monkeypatch.setattr(cli, 'redis_payload_capabilities_observed',
                        lambda url, **kw: calls.append(('probe-redis', url)) or admission.redis_payload_capabilities(None))
    # The GPU-runtime probe is a real `docker info` (a third of a second per call,
    # and a daemon dependency); nothing here is about GPUs.
    monkeypatch.setattr(cli, 'docker_gpus_available', lambda: False)
    return tmp_path, calls


def _run(tmp_path, *extra):
    '''Runs the CLI and returns its exit status (main() no longer raises SystemExit).'''
    return cli.main(['run-local', str(tmp_path / 'graph.py'), *extra])


def test_provisions_and_tears_down_by_default(wiring):
    tmp_path, calls = wiring
    _run(tmp_path)
    assert ('ensure', {'need_redis': True}) in calls
    assert ('teardown', ['nats', 'redis']) in calls
    # The dev containers are judged by the shape localinfra declared for them,
    # never probed...
    assert not any(c[0].startswith('probe-') for c in calls if isinstance(c, tuple))
    # ...and the prepare hook runs before the graph is loaded: the factory reads
    # prepare's outputs, so the ordering is load-bearing.
    assert calls.index('prepare') < calls.index('load')


def test_keep_infra_skips_teardown(wiring):
    tmp_path, calls = wiring
    _run(tmp_path, '--keep-infra')
    assert not any(c[0] == 'teardown' for c in calls if isinstance(c, tuple))


def test_explicit_nats_never_provisions_and_is_read_back(wiring):
    tmp_path, calls = wiring
    _run(tmp_path, '--nats', 'nats://elsewhere:4222')
    assert not any(c[0] == 'ensure' for c in calls if isinstance(c, tuple))
    assert _FakeEngine.instances[0].kwargs['nats_url'] == 'nats://elsewhere:4222'
    # A bring-your-own broker is read back, not assumed; there is no store to probe.
    assert ('probe-nats', 'nats://elsewhere:4222') in calls
    assert not any(c[0] == 'probe-redis' for c in calls if isinstance(c, tuple))


def test_no_infra_uses_the_default_url_without_docker(wiring):
    tmp_path, calls = wiring
    _run(tmp_path, '--no-infra')
    assert not any(c[0] == 'ensure' for c in calls if isinstance(c, tuple))
    assert _FakeEngine.instances[0].kwargs['nats_url'] == localinfra.DEFAULT_NATS_URL
    assert ('probe-nats', localinfra.DEFAULT_NATS_URL) in calls   # whatever listens there is read back


def test_whatever_already_answers_on_the_dev_ports_is_read_back(wiring, monkeypatch):
    # localinfra started nothing: a compose server (or a --keep-infra leftover)
    # answered, whose shape is not this run's to declare.
    tmp_path, calls = wiring
    monkeypatch.setattr(localinfra, 'ensure_local_infra',
                        lambda **kw: calls.append(('ensure', kw)) or (localinfra.local_infra_urls(), []))
    _run(tmp_path)
    assert ('probe-nats', localinfra.DEFAULT_NATS_URL) in calls
    assert ('probe-redis', localinfra.DEFAULT_REDIS_URL) in calls
    assert not any(isinstance(c, tuple) and c[0] == 'teardown' for c in calls)


def test_a_bring_your_own_store_is_read_back_beside_dev_infra(wiring, monkeypatch):
    tmp_path, calls = wiring
    monkeypatch.setenv('VIDEOFLOW_BLOB_REDIS_URL', 'redis://env:6379/2')
    _run(tmp_path, '--blob-redis-url', 'redis://flag:6379/3')
    # The flag wins over the environment, reaches the engine, and is read back.
    assert _FakeEngine.instances[0].kwargs['blob_redis_url'] == 'redis://flag:6379/3'
    assert ('probe-redis', 'redis://flag:6379/3') in calls
    assert not any(c[0] == 'probe-nats' for c in calls if isinstance(c, tuple))
    calls.clear()
    _run(tmp_path)
    # Without the flag the environment's store is the one used and read back.
    assert _FakeEngine.instances[-1].kwargs['blob_redis_url'] == 'redis://env:6379/2'
    assert ('probe-redis', 'redis://env:6379/2') in calls


def test_no_prepare_skips_the_hook(wiring):
    tmp_path, calls = wiring
    _run(tmp_path, '--no-prepare')
    assert 'prepare' not in calls


def test_a_batch_flow_is_admitted_on_the_dev_containers(wiring, monkeypatch):
    # The dev Redis persists and never evicts (RedisProfile.dev()), which is what
    # every BATCH channel's reliable_work asks of a payload store.
    tmp_path, calls = wiring
    monkeypatch.setattr(_FakeFlow, 'flow_type', 'batch')
    assert _run(tmp_path) == 0
    assert _FakeEngine.instances and _FakeEngine.instances[-1].kwargs['blob_redis_url'] == localinfra.DEFAULT_REDIS_URL


def test_a_refused_flow_tears_its_dev_containers_down(wiring, monkeypatch, capsys):
    # A bring-your-own store that turns out evictable refuses a BATCH flow before
    # anything runs; the containers this run started for it do not outlive it.
    tmp_path, calls = wiring
    monkeypatch.setattr(_FakeFlow, 'flow_type', 'batch')
    # The stub flow has no nodes; give admission the real graph's channels to judge.
    from videoflow.core.compiler import compile_flow
    from videoflow.deploy.compile import load_flow
    real_specs = compile_flow(load_flow(str(tmp_path / 'graph.py')))
    monkeypatch.setattr(cli, 'compile_flow', lambda flow, **kw: real_specs)
    monkeypatch.setattr(cli, 'redis_payload_capabilities_observed',
                        lambda url, **kw: admission.redis_payload_capabilities(
                            RedisProfile(persistence = 'none', eviction = 'volatile-lru')))
    assert _run(tmp_path, '--blob-redis-url', 'redis://cache:6379/0') == 2
    assert 'VF_INCOMPATIBLE_PROFILE' in capsys.readouterr().err
    assert ('teardown', ['nats', 'redis']) in calls
    assert not _FakeEngine.instances
    calls.clear()
    assert _run(tmp_path, '--blob-redis-url', 'redis://cache:6379/0', '--keep-infra') == 2
    assert not any(isinstance(c, tuple) and c[0] == 'teardown' for c in calls)


def test_infra_is_torn_down_even_when_the_run_raises(wiring, monkeypatch):
    tmp_path, calls = wiring
    def boom(self):
        raise RuntimeError('worker exploded')
    monkeypatch.setattr(_FakeFlow, 'join', boom)
    with pytest.raises(RuntimeError):
        _run(tmp_path)
    assert ('teardown', ['nats', 'redis']) in calls


def test_failed_node_exits_non_zero(wiring, monkeypatch):
    tmp_path, _calls = wiring
    original = _FakeEngine.__init__
    def failing_init(self, **kwargs):
        original(self, **kwargs)
        self._failures = [('work', 0, 1)]
    monkeypatch.setattr(_FakeEngine, '__init__', failing_init)
    # The flow ran and lost a node: exit 4, distinct from a bad flow (2) or a
    # broken environment (3), so CI can triage without parsing stderr.
    assert _run(tmp_path) == EXIT_FLOW_FAILED


def test_prepare_failure_is_a_clean_error(wiring, monkeypatch, capsys):
    tmp_path, _calls = wiring
    def boom(graph_dir, config_path = None):
        raise subprocess.CalledProcessError(1, 'prepare.py')
    monkeypatch.setattr(solution, 'run_prepare_local', boom)
    assert _run(tmp_path) == EXIT_ENVIRONMENT
    assert 'prepare.py failed' in capsys.readouterr().err


def test_missing_graph_is_reported(tmp_path, capsys):
    assert cli.main(['run-local', str(tmp_path / 'nope.py')]) == EXIT_USER
    err = capsys.readouterr().err
    assert 'Graph module not found' in err
    assert 'Traceback' not in err       # a message, not a stack of framework internals


# -- <repo>://<name> solution references ---------------------------------------

def test_solution_ref_is_resolved_then_run(wiring, monkeypatch):
    tmp_path, calls = wiring
    from videoflow.deploy import solution_refs

    def resolve(arg, cache_root = None):
        assert arg == 'videoflow://mygraph'
        return solution_refs.ResolvedSolution(graph_path = str(tmp_path / 'graph.py'),
                                              factory = None, build_context = str(tmp_path))
    monkeypatch.setattr(solution_refs, 'resolve_solution_ref', resolve)
    assert cli.main(['run-local', 'videoflow://mygraph']) == 0
    assert 'load' in calls


def test_a_solution_directory_names_its_graph_module(wiring):
    # `run-local path/to/<name>` is `path/to/<name>/<name>.py`: the convention a
    # <repo>://<name> reference resolves to, available to a local directory too.
    tmp_path, calls = wiring
    solution = tmp_path / 'demo'
    solution.mkdir()
    (tmp_path / 'graph.py').rename(solution / 'demo.py')
    assert cli.main(['run-local', str(solution)]) == 0
    assert 'load' in calls
    assert cli.main(['run-local', f'{solution}:build_flow']) == 0


def test_a_directory_without_its_graph_module_is_a_config_error(tmp_path, capsys):
    (tmp_path / 'demo').mkdir()
    (tmp_path / 'demo' / 'other.py').write_text('')
    assert cli.main(['run-local', str(tmp_path / 'demo')]) == EXIT_USER
    err = capsys.readouterr().err
    assert 'no demo.py' in err and 'other.py' in err


def test_malformed_solution_ref_is_reported_as_a_ref_error(tmp_path, capsys):
    # Anything with '://' is meant as a reference. Before, a ref the strict parser
    # rejected fell through to the path branch and was reported as a missing file
    # called 'videoflow' — the same message a pre-1.0.3 install prints, and no
    # help either way. The parse fails before any clone, so no network here.
    assert cli.main(['run-local', 'videoflow://solutions/toy_calculator']) == EXIT_USER
    err = capsys.readouterr().err
    assert 'is not a solution reference' in err and '<repo>://<name>' in err
    assert 'Graph module not found' not in err


def _spec(name, node_class = None, image = None, descriptor = None):
    '''A NodeSpec with the routing fields defaulted; only the image-related ones vary.'''
    from videoflow.core.compiler import NODE_KIND_PROCESSOR, NodeSpec
    return NodeSpec(name = name, node_class = node_class, params = {}, parents = [],
                    kind = NODE_KIND_PROCESSOR, has_children = False, nb_tasks = 1,
                    device_type = 'cpu', is_finite = False,
                    image = image, descriptor = descriptor)


def test_pure_python_flow_never_builds(wiring, monkeypatch):
    # The common case: every node is a host subprocess, so building the solution
    # image (potentially multi-GB CUDA) would cost minutes and buy nothing.
    tmp_path, _calls = wiring
    built = []
    monkeypatch.setattr(cli, 'autobuild', lambda *a, **kw: built.append(a) or 'img:1')
    monkeypatch.setattr(cli, 'specs_from_tasks_data',
                        lambda td: [_spec('producer', node_class = 'pkg.P'),
                                    _spec('printer', node_class = 'pkg.C')])
    _run(tmp_path)
    assert built == []
    assert _FakeEngine.instances[-1].kwargs['default_image'] is None


def test_native_component_without_image_triggers_a_build(wiring, monkeypatch):
    tmp_path, _calls = wiring
    built = []
    monkeypatch.setattr(cli, 'autobuild', lambda *a, **kw: built.append(a) or 'img:1')
    monkeypatch.setattr(cli, 'specs_from_tasks_data',
                        lambda td: [_spec('native')])          # no node_class, no image
    _run(tmp_path)
    assert len(built) == 1
    assert _FakeEngine.instances[-1].kwargs['default_image'] == 'img:1'


def test_native_component_with_a_local_command_does_not_build(wiring, monkeypatch):
    # runtime.localCommand runs the binary directly — no container, no image.
    tmp_path, _calls = wiring
    built = []
    monkeypatch.setattr(cli, 'autobuild', lambda *a, **kw: built.append(a) or 'img:1')
    descriptor = {'spec': {'runtime': {'localCommand': ['./run.sh']}}}
    monkeypatch.setattr(cli, 'specs_from_tasks_data',
                        lambda td: [_spec('native', descriptor = descriptor)])
    _run(tmp_path)
    assert built == []


def test_explicit_image_suppresses_the_build(wiring, monkeypatch):
    tmp_path, _calls = wiring
    built = []
    monkeypatch.setattr(cli, 'autobuild', lambda *a, **kw: built.append(a) or 'img:1')
    monkeypatch.setattr(cli, 'specs_from_tasks_data', lambda td: [_spec('native')])
    _run(tmp_path, '--image', 'ghcr.io/acme/app:v1')
    assert built == []
    assert _FakeEngine.instances[-1].kwargs['default_image'] == 'ghcr.io/acme/app:v1'


def test_in_image_without_a_dockerfile_runs_in_the_base_image(wiring, monkeypatch):
    # --in-image asks for a container; with nothing to build, the base image is it
    # (a flow of built-in nodes), and the compile happens inside it.
    tmp_path, _calls = wiring
    monkeypatch.setattr(cli, 'autobuild', lambda *a, **kw: None)
    monkeypatch.setattr(cli, 'default_image', lambda needs_gpu: 'videoflow-base:abc123')
    from videoflow.deploy.compile import requirements_from_document
    monkeypatch.setattr(cli, '_compile_in_image',
                        lambda args, target, graph_dir, image, *a, **kw:
                            ('demo', 'batch', [_spec('producer', node_class = 'videoflow.producers.IntProducer',
                                                     image = image)], requirements_from_document({})))
    assert _run(tmp_path, '--in-image') == 0
    assert _FakeEngine.instances[-1].kwargs['worker_image'] == 'videoflow-base:abc123'


def test_in_image_with_own_nodes_is_refused_for_the_base_image(wiring, monkeypatch, capsys):
    tmp_path, _calls = wiring
    monkeypatch.setattr(cli, 'autobuild', lambda *a, **kw: None)
    monkeypatch.setattr(cli, 'default_image', lambda needs_gpu: 'videoflow-base:abc123')
    from videoflow.deploy.compile import requirements_from_document
    monkeypatch.setattr(cli, '_compile_in_image',
                        lambda *a, **kw: ('demo', 'batch', [_spec('mine', node_class = 'my_flow.Mine')],
                                          requirements_from_document({})))
    assert _run(tmp_path, '--in-image') == EXIT_USER
    assert 'my_flow.Mine' in capsys.readouterr().err


def test_no_build_skips_the_build_even_when_needed(wiring, monkeypatch):
    tmp_path, _calls = wiring
    built = []
    monkeypatch.setattr(cli, 'autobuild', lambda *a, **kw: built.append(a) or 'img:1')
    monkeypatch.setattr(cli, 'specs_from_tasks_data', lambda td: [_spec('native')])
    _run(tmp_path, '--no-build')
    assert built == []
    assert _FakeEngine.instances[-1].kwargs['default_image'] is None
