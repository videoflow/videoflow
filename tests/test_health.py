import time

import pytest

from videoflow.core.engine import Messenger
from videoflow.health import HealthState, InstrumentedMessenger, LIVENESS_STALL_SECONDS

class _FakeInner(Messenger):
    def __init__(self):
        self.published = []
        self.received = 0
        self.acked = 0
        self.failed = []

    def publish_message(self, message, metadata = None):
        self.published.append((message, metadata))

    def publish_stop_signal(self):
        self.published.append(('STOP', None))

    def check_for_termination(self):
        return False

    def receive_message(self):
        self.received += 1
        return {'p': {'message': 1, 'metadata': None, 'is_stop_signal': False}}

    def ack_inputs(self):
        self.acked += 1

    def fail_inputs(self, exc):
        self.failed.append(exc)

    def close(self):
        pass

def test_not_ready_until_activity():
    state = HealthState('n')
    assert state.is_ready() is False
    assert state.is_live() is True  # fresh heartbeat
    im = InstrumentedMessenger(_FakeInner(), state)
    im.publish_message('x', {'proctime': 0.1, 'actual_proctime': 0.2})
    assert state.is_ready() is True

def test_liveness_stalls():
    state = HealthState('n')
    # backdate the heartbeat past the stall threshold
    state._last_beat = time.time() - (LIVENESS_STALL_SECONDS + 5)
    assert state.is_live() is False
    state.beat()
    assert state.is_live() is True

def test_metrics_accumulate():
    state = HealthState('detector')
    im = InstrumentedMessenger(_FakeInner(), state)
    im.publish_message('a', {'proctime': 0.1, 'actual_proctime': 0.3})
    im.publish_message('b', {'proctime': 0.2, 'actual_proctime': 0.4})
    text = state.render_metrics()
    assert 'videoflow_proctime_seconds_count{node="detector"} 2' in text
    assert 'videoflow_proctime_seconds_sum{node="detector"} 0.3' in text or \
           'videoflow_proctime_seconds_sum{node="detector"} 0.30000000000000004' in text

def test_instrumented_delegates():
    inner = _FakeInner()
    im = InstrumentedMessenger(inner, HealthState('n'))
    im.receive_message()
    im.publish_stop_signal()
    assert inner.received == 1
    assert ('STOP', None) in inner.published

def test_ack_fail_delegate_and_count():
    inner = _FakeInner()
    state = HealthState('n')
    im = InstrumentedMessenger(inner, state)
    im.receive_message()
    im.ack_inputs()
    err = RuntimeError('boom')
    im.fail_inputs(err)
    assert inner.acked == 1
    assert inner.failed == [err]
    text = state.render_metrics()
    assert 'videoflow_messages_received_total{node="n"} 1' in text
    assert 'videoflow_messages_processed_total{node="n"} 1' in text
    assert 'videoflow_messages_failed_total{node="n"} 1' in text

if __name__ == "__main__":
    pytest.main([__file__])
