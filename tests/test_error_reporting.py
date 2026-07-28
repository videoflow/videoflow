'''
How a failure reaches a human: the CLI's exit codes and rendering, the worker's
termination log, the DLQ sampler that bounds best-effort dead-lettering, and the
error metric that makes "what is failing" answerable.

The common thread is that a failure is useless if nobody can act on it. A uniform
exit 1 cannot be triaged, an undimensioned failure counter cannot be alerted on,
and a crash-looping pod whose reason is buried in a log cannot be diagnosed from
the API.

Pure/unit: no broker, no cluster.
'''
from __future__ import absolute_import, division, print_function

import json

import pytest

from videoflow.core.errors import (
    EXIT_ENVIRONMENT,
    EXIT_FLOW_FAILED,
    EXIT_FLOW_STALLED,
    EXIT_INTERRUPTED,
    EXIT_USER,
    ClusterError,
    ConfigError,
    DeviceError,
    FlowFailed,
    FlowStalled,
    GraphError,
    SchemaError,
)
from videoflow.deploy import cli
from videoflow.messaging.nats_messenger import _DlqSampler
from videoflow.runtime.health import HealthState
from videoflow.runtime.worker import write_termination_reason

# -- CLI exit codes ----------------------------------------------------------

@pytest.mark.parametrize('error, expected', [
    (ConfigError('bad flag'), EXIT_USER),
    (GraphError('cycle'), EXIT_USER),
    (ClusterError('kubectl missing'), EXIT_ENVIRONMENT),
    (FlowFailed('two nodes died'), EXIT_FLOW_FAILED),
    (FlowStalled('unschedulable'), EXIT_FLOW_STALLED),
])
def test_main_returns_the_class_specific_exit_code(monkeypatch, capsys, error, expected):
    def boom(_args):
        raise error

    monkeypatch.setattr(cli, 'build_parser', _parser_running(boom))
    assert cli.main(['explain', 'x']) == expected
    capsys.readouterr()


def test_main_returns_zero_on_success(monkeypatch, capsys):
    monkeypatch.setattr(cli, 'build_parser', _parser_running(lambda _args: None))
    assert cli.main(['explain', 'x']) == 0


def test_interrupt_is_its_own_code(monkeypatch, capsys):
    def boom(_args):
        raise KeyboardInterrupt()

    monkeypatch.setattr(cli, 'build_parser', _parser_running(boom))
    assert cli.main(['explain', 'x']) == EXIT_INTERRUPTED


def _parser_running(func):
    '''A build_parser stand-in whose only command runs ``func``.'''
    import argparse

    def build():
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(dest = 'command', required = True)
        cmd = sub.add_parser('explain')
        cmd.add_argument('graph')
        cmd.set_defaults(func = func)
        return parser

    return build


def test_rendering_shows_the_problem_then_the_fix_then_the_context(capsys):
    cli.render_error(DeviceError('CUDA out of memory',
                                remedy = 'Lower the batch size.',
                                node = 'detector', replica = 2))
    err = capsys.readouterr().err
    assert 'ERROR [VF_DEVICE]: CUDA out of memory' in err
    assert 'Lower the batch size.' in err
    assert 'node: detector' in err
    assert 'replica: 2' in err


def test_no_traceback_unless_asked(monkeypatch, capsys):
    def boom(_args):
        raise ConfigError('bad flag', remedy = 'Use --help.')

    monkeypatch.setattr(cli, 'build_parser', _parser_running(boom))
    monkeypatch.delenv('VF_DEBUG', raising = False)
    cli.main(['explain', 'x'])
    assert 'Traceback' not in capsys.readouterr().err

    monkeypatch.setenv('VF_DEBUG', '1')
    cli.main(['explain', 'x'])
    assert 'Traceback' in capsys.readouterr().err


# -- termination log ---------------------------------------------------------

def test_termination_reason_is_machine_readable(tmp_path):
    '''
    Kubernetes surfaces this file's contents in containerStatuses, which is how
    the deploy watchdog learns *why* a pod died without scraping 80 lines of log.
    '''
    path = tmp_path / 'termination-log'
    write_termination_reason(
        DeviceError('CUDA out of memory', remedy = 'Lower the batch size.',
                    node = 'detector'),
        path = str(path))
    record = json.loads(path.read_text())
    assert record['code'] == 'VF_DEVICE'
    assert record['disposition'] == 'worker_fatal'
    assert record['remedy'] == 'Lower the batch size.'
    assert record['context']['node'] == 'detector'


def test_an_unwritable_termination_log_never_masks_the_real_failure(tmp_path):
    '''
    The file does not exist outside Kubernetes. A worker that cannot explain its
    death must still die of the original cause, not of a logging problem.
    '''
    write_termination_reason(DeviceError('gone'),
                            path = str(tmp_path / 'no' / 'such' / 'dir' / 'log'))


def test_a_bare_exception_still_produces_a_reason(tmp_path):
    path = tmp_path / 'log'
    write_termination_reason(RuntimeError('something'), path = str(path))
    assert json.loads(path.read_text())['code'] == 'VF_RUNTIMEERROR'


# -- DLQ sampling ------------------------------------------------------------

class _Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def test_sampler_admits_a_bounded_number_per_code_per_window():
    clock = _Clock()
    sampler = _DlqSampler(per_minute = 2, clock = clock)
    assert [sampler.admit('VF_POISON_SCHEMA', 'n') for _ in range(5)] == \
        [True, True, False, False, False]


def test_distinct_codes_get_their_own_budget():
    '''One noisy failure must not hide the specimen of a different one.'''
    clock = _Clock()
    sampler = _DlqSampler(per_minute = 1, clock = clock)
    assert sampler.admit('VF_A', 'n') is True
    assert sampler.admit('VF_A', 'n') is False
    assert sampler.admit('VF_B', 'n') is True


def test_distinct_nodes_get_their_own_budget():
    clock = _Clock()
    sampler = _DlqSampler(per_minute = 1, clock = clock)
    assert sampler.admit('VF_A', 'left') is True
    assert sampler.admit('VF_A', 'right') is True


def test_the_window_rolls_over():
    clock = _Clock()
    sampler = _DlqSampler(per_minute = 1, clock = clock)
    assert sampler.admit('VF_A', 'n') is True
    assert sampler.admit('VF_A', 'n') is False
    clock.now += 61.0
    assert sampler.admit('VF_A', 'n') is True


def test_zero_admits_nothing():
    assert _DlqSampler(per_minute = 0, clock = _Clock()).admit('VF_A', 'n') is False


# -- error metric ------------------------------------------------------------

def test_errors_are_counted_by_code_and_disposition():
    '''
    "How many failed" is nearly useless; "what is failing" is the question an
    alert asks, and an undimensioned counter cannot answer it.
    '''
    state = HealthState('detector')
    state.record_error('VF_DEVICE', 'worker_fatal')
    state.record_error('VF_DEVICE', 'worker_fatal')
    state.record_error('VF_POISON_SCHEMA', 'poison')
    text = state.render_metrics()
    assert 'videoflow_errors_total{node="detector",code="VF_DEVICE",disposition="worker_fatal"} 2' in text
    assert 'videoflow_errors_total{node="detector",code="VF_POISON_SCHEMA",disposition="poison"} 1' in text


def test_a_sampled_failure_is_still_counted():
    '''
    The DLQ is bounded on purpose; the counter is not. A suppressed specimen must
    still move the number, or best-effort failures become invisible again.
    '''
    from support_messenger import RecordingMessenger

    from videoflow.runtime.health import InstrumentedMessenger

    state = HealthState('n')
    messenger = InstrumentedMessenger(RecordingMessenger(), state)
    for _ in range(50):
        messenger.fail_inputs(SchemaError('bad row'))
    assert 'videoflow_errors_total{node="n",code="VF_POISON_SCHEMA",disposition="poison"} 50' \
        in state.render_metrics()


if __name__ == '__main__':
    pytest.main([__file__])
