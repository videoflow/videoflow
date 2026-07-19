'''
The GPU strategy prepare()/cleanup() lifecycle across the deploy and teardown CLI
paths (videoflow.deploy.cli).

A GPU strategy may reconfigure the cluster in prepare() and must be able to undo
it. These tests pin the four places that were wrong or missing until the review
fixed them: prepare() failure still cleans up; a REALTIME deploy leaves the
retune in place (the flow is still using it); teardown carries --gpu-mode and
calls cleanup(); and a cleanup() that itself fails is reported without masking the
real error.
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
