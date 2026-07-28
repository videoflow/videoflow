'''
The three "when to give up" policies: the circuit breaker, the progress deadline,
and the restart policy both engines share.

All pure — a fake clock and an injected probe stand in for time and the broker, so
the interesting question (does this trip when it should, and *not* when it
shouldn't) is answered in milliseconds rather than by watching a real flow hang.
'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.core.errors import (
    POISON,
    TRANSIENT,
    WORKER_FATAL,
    DeviceError,
    ProgressStalled,
    SchemaError,
    WorkerUnhealthy,
)
from videoflow.core.supervision import (
    ConsecutiveFailureBreaker,
    EventLog,
    NodeExited,
    NodeGaveUp,
    NodeRestarted,
    ProgressDeadline,
    SupervisionPolicy,
    render_event,
)


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds : float) -> None:
        self.now += seconds


# -- circuit breaker ---------------------------------------------------------

def test_breaker_trips_at_the_threshold():
    breaker = ConsecutiveFailureBreaker(threshold = 3, node_name = 'detector')
    for _ in range(2):
        breaker.record_failure(DeviceError('gone'))
        breaker.check()                      # not yet
    breaker.record_failure(DeviceError('gone'))
    with pytest.raises(WorkerUnhealthy) as exc:
        breaker.check()
    assert 'detector' in str(exc.value)
    assert '3 messages in a row' in str(exc.value)
    assert exc.value.disposition == WORKER_FATAL   # so the inputs are never blamed


def test_any_success_resets_the_count():
    breaker = ConsecutiveFailureBreaker(threshold = 3)
    breaker.record_failure(DeviceError('x'))
    breaker.record_failure(DeviceError('x'))
    breaker.record_success()
    breaker.record_failure(DeviceError('x'))
    breaker.check()                          # must not raise
    assert breaker.consecutive_failures == 1


def test_interleaved_poison_never_trips_the_breaker():
    '''
    The distinction the breaker exists to draw: bad *data* is sparse and
    independent, and a stream containing some of it is not a sick worker.
    '''
    breaker = ConsecutiveFailureBreaker(threshold = 3)
    for _ in range(20):
        breaker.record_failure(SchemaError('bad row'))
        breaker.check()
        breaker.record_success()
    assert breaker.consecutive_failures == 0


def test_threshold_zero_disables_the_breaker():
    breaker = ConsecutiveFailureBreaker(threshold = 0)
    for _ in range(100):
        breaker.record_failure(DeviceError('x'))
    breaker.check()
    assert breaker.tripped is False


def test_the_trip_carries_the_originating_error():
    breaker = ConsecutiveFailureBreaker(threshold = 1, node_name = 'n')
    breaker.record_failure(DeviceError('CUDA out of memory'))
    with pytest.raises(WorkerUnhealthy, match = 'CUDA out of memory'):
        breaker.check()


# -- progress deadline -------------------------------------------------------

def test_deadline_does_not_trip_while_work_is_being_acked():
    clock = _Clock()
    deadline = ProgressDeadline(10.0, pending_probe = lambda: 5, clock = clock)
    for _ in range(10):
        clock.advance(9.0)
        deadline.record_progress()
        deadline.check()                     # must not raise


def test_deadline_does_not_trip_when_nothing_is_pending():
    '''
    Idle is not stalled. A node with no upstream work has acked nothing for a
    reason, and killing it would be a pure false positive — which is exactly what
    a wall-clock deadline would do here.
    '''
    clock = _Clock()
    deadline = ProgressDeadline(10.0, pending_probe = lambda: 0, clock = clock)
    clock.advance(1000.0)
    deadline.check()


def test_deadline_trips_when_work_is_pending_and_nothing_is_acked():
    clock = _Clock()
    deadline = ProgressDeadline(10.0, pending_probe = lambda: 3, clock = clock,
                                node_name = 'aggregator')
    clock.advance(11.0)
    with pytest.raises(ProgressStalled) as exc:
        deadline.check()
    assert 'aggregator' in str(exc.value)
    assert 'not slow, it is stuck' in str(exc.value)
    assert exc.value.context['pending'] == 3


def test_idle_resets_the_window_so_a_later_arrival_gets_a_full_one():
    clock = _Clock()
    pending = [0]
    deadline = ProgressDeadline(10.0, pending_probe = lambda: pending[0], clock = clock)
    clock.advance(100.0)
    deadline.check()                         # idle: window reset
    pending[0] = 1
    clock.advance(5.0)
    deadline.check()                         # only 5s since the reset — not stalled
    clock.advance(6.0)
    with pytest.raises(ProgressStalled):
        deadline.check()


def test_timeout_zero_disables_the_deadline():
    clock = _Clock()
    deadline = ProgressDeadline(0.0, pending_probe = lambda: 99, clock = clock)
    clock.advance(10_000.0)
    deadline.check()


def test_a_slow_but_healthy_node_never_trips_either_watchdog():
    '''
    The false-positive control. A node that takes minutes per message but keeps
    acking is slow, not stuck, and neither watchdog may touch it.
    '''
    clock = _Clock()
    breaker = ConsecutiveFailureBreaker(threshold = 3)
    deadline = ProgressDeadline(60.0, pending_probe = lambda: 10, clock = clock)
    for _ in range(50):
        clock.advance(59.0)
        breaker.record_success()
        deadline.record_progress()
        deadline.check()
        breaker.check()


# -- supervision policy ------------------------------------------------------

def test_poison_is_never_restarted():
    '''A worker that died of a bad message will die of it again.'''
    policy = SupervisionPolicy()
    assert policy.should_restart(0, POISON) is False
    assert policy.should_restart(0, TRANSIENT) is True
    assert policy.should_restart(0, WORKER_FATAL) is True


def test_an_unexplained_death_is_restarted():
    '''
    A worker killed too abruptly to write a reason is far more often a crash worth
    retrying than a poison message, so the unknown case restarts.
    '''
    assert SupervisionPolicy().should_restart(0, None) is True


def test_restarts_are_bounded():
    policy = SupervisionPolicy(max_restarts = 2)
    assert policy.should_restart(0, TRANSIENT) is True
    assert policy.should_restart(1, TRANSIENT) is True
    assert policy.should_restart(2, TRANSIENT) is False


def test_backoff_repeats_its_last_value():
    policy = SupervisionPolicy(backoff_seconds = (1.0, 2.0))
    assert [policy.delay_for(i) for i in range(4)] == [1.0, 2.0, 2.0, 2.0]


def test_local_and_cluster_agree_on_the_restart_count():
    '''
    The parity guarantee: only the backoff differs, so a crash that recovers in
    the cluster recovers locally too — and after the same number of attempts.
    '''
    assert SupervisionPolicy.local().max_restarts == SupervisionPolicy().max_restarts
    assert SupervisionPolicy.local().backoff_seconds < SupervisionPolicy().backoff_seconds


def test_disabled_policy_never_restarts():
    assert SupervisionPolicy.disabled().should_restart(0, TRANSIENT) is False


# -- lifecycle events --------------------------------------------------------

def test_event_log_summarizes_a_run():
    log = EventLog()
    log.emit(NodeRestarted('work', 0, 0, 1.0))
    log.emit(NodeRestarted('work', 0, 1, 2.0))
    log.emit(NodeGaveUp('work', 0, 2, {'code': 'VF_DEVICE', 'message': 'gone'}))
    assert log.restart_count('work') == 2
    assert log.restart_count('other') == 0
    assert log.failed_nodes() == ['work']


def test_render_event_prints_what_is_worth_reading():
    assert 'restarting' in render_event(NodeRestarted('w', 0, 0, 1.5))
    assert 'gave up' in render_event(NodeGaveUp('w', 0, 3, None))
    assert 'exited with code 7' in render_event(NodeExited('w', 0, 7, None))
    # A clean exit is not news.
    assert render_event(NodeExited('w', 0, 0, None)) is None


def test_exited_event_exposes_the_reported_disposition():
    event = NodeExited('w', 0, 4, {'disposition': POISON, 'code': 'VF_POISON_SCHEMA'})
    assert event.disposition == POISON
    assert NodeExited('w', 0, 1, None).disposition is None


if __name__ == '__main__':
    pytest.main([__file__])
