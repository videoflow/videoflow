'''
LocalProcessEngine worker launching: the PYTHONPATH re-export that lets a worker
subprocess import node classes living next to the graph, and the exit-code
reporting that makes a dead worker visible instead of silent.

Pure/unit: subprocess.Popen and broker provisioning are monkeypatched — no NATS.
'''
import os
import signal
import subprocess
import sysconfig

from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.compiler import compile_flow
from videoflow.core.constants import BATCH
from videoflow.engines.local import LocalProcessEngine, _worker_env, inherited_python_path
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer


def _flow():
    p = IntProducer(0, 3, name = 'producer')
    a = IdentityProcessor(name = 'work')(p)
    out = CommandlineConsumer(name = 'printer')(a)
    return Flow([out], flow_type = BATCH, flow_id = 'demo')


class _FakeProc:
    def __init__(self, returncode = 0):
        self.returncode = returncode
        self.pid = 4242

    def wait(self):
        return self.returncode


def _run_engine(monkeypatch, engine = None, returncodes = None):
    '''Runs the engine with Popen stubbed; returns (envs, engine).'''
    envs = []
    codes = list(returncodes or [])
    def fake_popen(cmd, env = None, **kwargs):
        envs.append(env or {})
        return _FakeProc(codes.pop(0) if codes else 0)
    monkeypatch.setattr(subprocess, 'Popen', fake_popen)
    monkeypatch.setattr('videoflow.messaging.topology.provision_flow_sync',
                        lambda *a, **kw: None)
    engine = engine or LocalProcessEngine()
    monkeypatch.setattr(engine, '_teardown_streams', lambda: None)
    flow = _flow()
    engine.allocate_and_run_tasks(flow.tasks_data(), 'demo', BATCH, 'run1')
    return envs, engine


def test_inherited_python_path_excludes_stdlib_and_site_packages(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    entries = inherited_python_path()
    assert str(tmp_path) in entries
    purelib = os.path.abspath(sysconfig.get_paths()['purelib'])
    assert purelib not in entries
    assert os.path.abspath(sysconfig.get_paths()['stdlib']) not in entries
    assert '' not in entries


def test_worker_env_prepends_python_path(monkeypatch):
    monkeypatch.setenv('PYTHONPATH', '/pre')
    spec = compile_flow(_flow())[0]
    env = _worker_env(spec, 'nats://x:4222', 'demo', BATCH, 'run1', None, 0, 3,
                      python_path = ['/a', '/b'])
    assert env['PYTHONPATH'] == os.pathsep.join(['/a', '/b', '/pre'])


def test_worker_env_without_python_path_is_untouched(monkeypatch):
    monkeypatch.delenv('PYTHONPATH', raising = False)
    spec = compile_flow(_flow())[0]
    env = _worker_env(spec, 'nats://x:4222', 'demo', BATCH, 'run1', None, 0, 3)
    assert 'PYTHONPATH' not in env


def test_workers_get_the_graph_dir_on_pythonpath(tmp_path, monkeypatch):
    '''
    The regression test for the original bug: load_flow puts the graph's directory
    on sys.path, and every worker must inherit it or node classes defined next to
    the graph raise ModuleNotFoundError in the worker.
    '''
    monkeypatch.syspath_prepend(str(tmp_path))
    envs, _engine = _run_engine(monkeypatch)
    assert envs, 'expected workers to be launched'
    for env in envs:
        assert str(tmp_path) in env['PYTHONPATH'].split(os.pathsep)


def test_inherit_can_be_disabled(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delenv('PYTHONPATH', raising = False)
    envs, _engine = _run_engine(monkeypatch,
                                engine = LocalProcessEngine(inherit_python_path = False))
    for env in envs:
        assert 'PYTHONPATH' not in env


def test_docker_branch_does_not_leak_pythonpath():
    '''A native component runs its own image; the host's PYTHONPATH must not follow.'''
    engine = LocalProcessEngine(python_path = ['/host/only'])

    class _NativeSpec:
        node_class = None
        descriptor = {'spec': {'runtime': {}}}
        image = 'vendor/img:1'
        command = None

    env = {'VF_NODE_NAME': 'x', 'PYTHONPATH': '/host/only', 'VF_NATS_URL': 'nats://localhost:4222'}
    argv, _run_env = engine._launch_command(_NativeSpec(), env)
    assert argv[:2] == ['docker', 'run']
    assert not any(arg.startswith('PYTHONPATH=') for arg in argv)


def test_failures_are_reported_and_deduped_per_node(monkeypatch):
    # producer ok, work fails, printer ok (order follows the topological sort).
    _envs, engine = _run_engine(monkeypatch, returncodes = [0, 1, 0])
    failed = engine.wait_for_completion()
    assert len(failed) == 1
    assert engine.failures() and engine.failures()[0][2] == 1


def test_clean_run_reports_no_failures(monkeypatch):
    _envs, engine = _run_engine(monkeypatch, returncodes = [0, 0, 0])
    assert engine.wait_for_completion() == []
    assert engine.failures() == []


def test_sigint_exit_is_not_a_failure(monkeypatch):
    '''Ctrl-C / flow.stop() kills workers by signal — that is not a crash.'''
    _envs, engine = _run_engine(monkeypatch,
                                returncodes = [-signal.SIGINT, -signal.SIGTERM, 0])
    assert engine.wait_for_completion() == []


def test_report_failures_names_node_and_code(monkeypatch, capsys):
    _envs, engine = _run_engine(monkeypatch, returncodes = [0, 3, 0])
    engine.wait_for_completion()
    engine.report_failures()
    err = capsys.readouterr().err
    assert 'exited with code 3' in err
