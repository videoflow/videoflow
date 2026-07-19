'''
CLI wiring for `run-local` (and the shared prepare hook it now has in common with
`deploy`): infra ownership, step ordering, blob-redis defaulting, and the
non-zero exit when a node fails.

Pure/unit: infra, the engine and flow execution are all monkeypatched.
'''
import subprocess
import sys

import pytest

from videoflow import cli, localinfra, solution

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


class _FakeFlow:
    def __init__(self):
        self.flow_id = 'demo'
        self.run_id = 'run1'
        self.joined = False

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
    return tmp_path, calls


def _run(tmp_path, *extra):
    cli.main(['run-local', str(tmp_path / 'graph.py'), *extra])


def test_provisions_and_tears_down_by_default(wiring):
    tmp_path, calls = wiring
    _run(tmp_path)
    assert ('ensure', {'need_redis': True}) in calls
    assert ('teardown', ['nats', 'redis']) in calls


def test_keep_infra_skips_teardown(wiring):
    tmp_path, calls = wiring
    _run(tmp_path, '--keep-infra')
    assert not any(c[0] == 'teardown' for c in calls if isinstance(c, tuple))


def test_explicit_nats_never_provisions(wiring):
    tmp_path, calls = wiring
    _run(tmp_path, '--nats', 'nats://elsewhere:4222')
    assert not any(c[0] == 'ensure' for c in calls if isinstance(c, tuple))
    assert _FakeEngine.instances[0].kwargs['nats_url'] == 'nats://elsewhere:4222'


def test_no_infra_uses_the_default_url_without_docker(wiring):
    tmp_path, calls = wiring
    _run(tmp_path, '--no-infra')
    assert not any(c[0] == 'ensure' for c in calls if isinstance(c, tuple))
    assert _FakeEngine.instances[0].kwargs['nats_url'] == localinfra.DEFAULT_NATS_URL


def test_prepare_runs_before_the_graph_is_loaded(wiring):
    tmp_path, calls = wiring
    _run(tmp_path)
    # The factory reads prepare's outputs, so ordering is load-bearing.
    assert calls.index('prepare') < calls.index('load')


def test_no_prepare_skips_the_hook(wiring):
    tmp_path, calls = wiring
    _run(tmp_path, '--no-prepare')
    assert 'prepare' not in calls


def test_blob_redis_url_defaults_from_environment(wiring, monkeypatch):
    tmp_path, _calls = wiring
    monkeypatch.setenv('VIDEOFLOW_BLOB_REDIS_URL', 'redis://env:6379/2')
    _run(tmp_path)
    assert _FakeEngine.instances[0].kwargs['blob_redis_url'] == 'redis://env:6379/2'


def test_explicit_blob_redis_url_wins_over_environment(wiring, monkeypatch):
    tmp_path, _calls = wiring
    monkeypatch.setenv('VIDEOFLOW_BLOB_REDIS_URL', 'redis://env:6379/2')
    _run(tmp_path, '--blob-redis-url', 'redis://flag:6379/3')
    assert _FakeEngine.instances[0].kwargs['blob_redis_url'] == 'redis://flag:6379/3'


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
    with pytest.raises(SystemExit, match = 'work'):
        _run(tmp_path)


def test_prepare_failure_is_a_clean_error(wiring, monkeypatch):
    tmp_path, _calls = wiring
    def boom(graph_dir, config_path = None):
        raise subprocess.CalledProcessError(1, 'prepare.py')
    monkeypatch.setattr(solution, 'run_prepare_local', boom)
    with pytest.raises(SystemExit, match = 'prepare.py failed'):
        _run(tmp_path)


def test_missing_graph_is_reported(tmp_path):
    with pytest.raises(SystemExit, match = 'Graph module not found'):
        cli.main(['run-local', str(tmp_path / 'nope.py')])
