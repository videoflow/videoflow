'''
The GPU strategy prepare()/cleanup() lifecycle across the deploy and teardown CLI
paths (videoflow.deploy.cli).

A GPU strategy may reconfigure the cluster in prepare() and must be able to undo
it. These tests cover the pieces reachable without a cluster: the _gpu_prepare
rollback (including on Ctrl-C), the _gpu_cleanup helper's best-effort contract,
and the teardown path's --gpu-mode handling.

Not covered here, and worth knowing: _cmd_deploy itself is never invoked. It runs
docker and kubectl before reaching the GPU block, so a test would need a dozen
monkeypatches to get there and would pin the mocks more than the code. That is
why the prepare-rollback logic was extracted into _gpu_prepare — the risky part
is testable, but the wiring that calls it with the flow's demand, and the
deliberate absence of cleanup on the REALTIME return, are verified by reading
cli.py rather than by a test.
'''
from __future__ import absolute_import, division, print_function

import argparse

import pytest

from videoflow.deploy import cli, gpu


@pytest.fixture
def registry_sandbox(monkeypatch):
    monkeypatch.setattr(gpu, '_GPU_STRATEGIES', dict(gpu._GPU_STRATEGIES))
    return gpu


def _register_recording_strategy(registry, events, prepare_error = None, cleanup_error = None):
    class _Recording(gpu.GpuStrategy):
        name = 'recording'

        def prepare(self, demand = None, kubectl = 'kubectl'):
            events.append(('prepare', demand))
            if prepare_error:
                raise prepare_error

        def cleanup(self, kubectl = 'kubectl'):
            events.append(('cleanup', None))
            if cleanup_error:
                raise cleanup_error

    registry.register_gpu_mode(_Recording())
    return registry.get_gpu_mode('recording')


# -- the shared helper -----------------------------------------------------

def test_gpu_cleanup_is_a_noop_for_no_strategy():
    cli._gpu_cleanup(None, 'kubectl')  # must not raise


def test_gpu_cleanup_reports_a_failing_cleanup_without_raising(registry_sandbox, capsys):
    events = []
    strategy = _register_recording_strategy(registry_sandbox, events,
                                            cleanup_error = RuntimeError('boom'))
    cli._gpu_cleanup(strategy, 'kubectl')            # must swallow the error
    assert ('cleanup', None) in events
    assert 'cleanup failed' in capsys.readouterr().err


# -- prepare rolls back when it does not complete --------------------------

def test_prepare_success_does_not_clean_up(registry_sandbox):
    events = []
    strategy = _register_recording_strategy(registry_sandbox, events)
    cli._gpu_prepare(strategy, {'nvidia.com/gpu': 4}, 'kubectl')
    assert events == [('prepare', {'nvidia.com/gpu': 4})]


def test_failed_prepare_rolls_back_then_exits(registry_sandbox):
    '''
    A prepare that fails partway may already have changed the cluster, so
    cleanup() must run before the CLI gives up.
    '''
    events = []
    strategy = _register_recording_strategy(registry_sandbox, events,
                                            prepare_error = RuntimeError('half-applied'))
    with pytest.raises(SystemExit) as excinfo:
        cli._gpu_prepare(strategy, {'nvidia.com/gpu': 1}, 'kubectl')
    assert [e[0] for e in events] == ['prepare', 'cleanup']
    assert 'recording' in str(excinfo.value)          # names the mode
    assert 'half-applied' in str(excinfo.value)       # keeps the cause


def test_interrupted_prepare_rolls_back_and_propagates(registry_sandbox):
    '''
    Ctrl-C is not a RuntimeError/ValueError. It must still roll back, and must
    stay a KeyboardInterrupt rather than becoming a SystemExit.
    '''
    events = []
    strategy = _register_recording_strategy(registry_sandbox, events,
                                            prepare_error = KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        cli._gpu_prepare(strategy, None, 'kubectl')
    assert [e[0] for e in events] == ['prepare', 'cleanup']


# -- teardown carries --gpu-mode and undoes the run ------------------------

def test_teardown_parser_accepts_gpu_mode():
    parser = cli.build_parser()
    args = parser.parse_args(['teardown', '--flow-id', 'f', '--run-id', 'r',
                              '--nats', 'nats://x', '--gpu-mode', 'shared'])
    assert args.gpu_mode == 'shared'


def test_teardown_calls_cleanup_for_the_named_mode(registry_sandbox, monkeypatch):
    events = []
    _register_recording_strategy(registry_sandbox, events)

    # Stub out the broker/k8s work so the test is about the GPU hook only.
    monkeypatch.setattr(cli, '_cmd_teardown', cli._cmd_teardown)  # keep real fn
    import asyncio

    def _run_stub(coro, *a, **k):
        coro.close()          # consume the coroutine so no un-awaited warning fires
    monkeypatch.setattr(asyncio, 'run', _run_stub)

    args = argparse.Namespace(flow_id = 'f', run_id = 'r', nats = 'nats://x',
                              namespace = None, kubectl = 'kubectl', infra = False,
                              gpu_mode = 'recording')
    cli._cmd_teardown(args)
    assert ('cleanup', None) in events


def test_teardown_survives_an_unresolvable_gpu_mode(registry_sandbox, monkeypatch, capsys):
    '''
    The real teardown work runs before the GPU hook, so an unknown mode -- most
    likely the plugin that registered it is not installed in this shell -- must
    warn, not raise. A traceback here reads as "teardown failed" and gets re-run.
    '''
    import asyncio

    def _run_stub(coro, *a, **k):
        coro.close()
    monkeypatch.setattr(asyncio, 'run', _run_stub)

    args = argparse.Namespace(flow_id = 'f', run_id = 'r', nats = 'nats://x',
                              namespace = None, kubectl = 'kubectl', infra = False,
                              gpu_mode = 'not-registered')
    cli._cmd_teardown(args)                            # must not raise
    err = capsys.readouterr().err
    assert 'skipping GPU cleanup' in err
    assert 'not-registered' in err


def test_teardown_without_gpu_mode_touches_no_strategy(registry_sandbox, monkeypatch):
    events = []
    _register_recording_strategy(registry_sandbox, events)
    import asyncio

    def _run_stub(coro, *a, **k):
        coro.close()          # consume the coroutine so no un-awaited warning fires
    monkeypatch.setattr(asyncio, 'run', _run_stub)

    args = argparse.Namespace(flow_id = 'f', run_id = 'r', nats = 'nats://x',
                              namespace = None, kubectl = 'kubectl', infra = False,
                              gpu_mode = None)
    cli._cmd_teardown(args)
    assert events == []


if __name__ == '__main__':
    pytest.main([__file__])
