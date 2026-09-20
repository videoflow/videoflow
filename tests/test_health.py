import math
import random
import re
import time

import pytest

from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.engine import Messenger
from videoflow.core.errors import ConfigError, SchemaError, TransientFailure, WorkerFatal
from videoflow.core.policies import DeliveryPolicy
from videoflow.runtime.health import (
    LATENCY_BUCKETS_SECONDS,
    LIVENESS_STALL_SECONDS,
    HealthState,
    InstrumentedMessenger,
    parse_histogram,
    quantile_bounds,
)


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

def test_instrumented_delegates_and_counts():
    inner = _FakeInner()
    state = HealthState('n')
    im = InstrumentedMessenger(inner, state)
    im.receive_message()
    im.publish_stop_signal()
    im.ack_inputs()
    err = RuntimeError('boom')
    im.fail_inputs(err)
    assert inner.received == 1
    assert ('STOP', None) in inner.published
    assert inner.acked == 1
    assert inner.failed == [err]
    text = state.render_metrics()
    assert 'videoflow_messages_received_total{node="n"} 1' in text
    assert 'videoflow_messages_processed_total{node="n"} 1' in text
    assert 'videoflow_messages_failed_total{node="n"} 1' in text

# -- histograms and throughput (RUN-045): additions that must leave the old lines alone --

def _exercise(state):
    '''Publish, receive, ack and fail through the wrapper: every family the old renderer knew.'''
    im = InstrumentedMessenger(_FakeInner(), state)
    im.publish_message('a', {'proctime': 0.25, 'actual_proctime': 0.5})
    im.publish_message('b', {'proctime': 0.5, 'actual_proctime': 1.0})
    im.receive_message()
    im.ack_inputs()
    im.fail_inputs(RuntimeError('boom'))

# The exact text the count/sum-only renderer produced for _exercise, in order.
# Extend this list when a family is added; never edit or reorder an entry.
_PINNED_LINES = [
    'videoflow_proctime_seconds_count{node="detector"} 2',
    'videoflow_proctime_seconds_sum{node="detector"} 0.75',
    'videoflow_actual_proctime_seconds_count{node="detector"} 2',
    'videoflow_actual_proctime_seconds_sum{node="detector"} 1.5',
    'videoflow_messages_published_total{node="detector"} 2',
    'videoflow_messages_received_total{node="detector"} 1',
    'videoflow_messages_processed_total{node="detector"} 1',
    'videoflow_messages_failed_total{node="detector"} 1',
    'videoflow_errors_total{node="detector",code="VF_RUNTIMEERROR",disposition=""} 1',
]

def test_pre_existing_rendering_is_a_byte_identical_prefix():
    state = HealthState('detector')
    _exercise(state)
    text = state.render_metrics()
    lines = text.split('\n')
    assert lines[:len(_PINNED_LINES)] == _PINNED_LINES
    assert text.endswith('\n') and lines[-1] == ''
    # and what follows is only ever a new family
    for line in lines[len(_PINNED_LINES):-1]:
        assert '_bucket{' in line or line.startswith('videoflow_messages_offered_total')

def test_histogram_buckets_are_cumulative_and_agree_with_count_and_sum():
    state = HealthState('detector')
    im = InstrumentedMessenger(_FakeInner(), state)
    values = [0.001, 0.004, 0.005, 0.0051, 0.02, 0.09, 0.3, 0.9, 2.0, 7.0, 45.0, 120.0]
    for v in values:
        im.publish_message('x', {'proctime': v})
    text = state.render_metrics()
    bounds, cumulative = parse_histogram(text, 'proctime_seconds')
    assert bounds == LATENCY_BUCKETS_SECONDS
    assert len(cumulative) == len(bounds) + 1
    assert list(cumulative) == sorted(cumulative)                      # monotone non-decreasing
    assert cumulative[-1] == len(values)                                # +Inf holds everything
    assert f'videoflow_proctime_seconds_count{{node="detector"}} {len(values)}' in text   # _count == count
    assert f'videoflow_proctime_seconds_sum{{node="detector"}} {sum(values)}' in text     # _sum == sum
    # le is inclusive: 0.005 lands in the 0.005 bucket, 0.0051 in the next one
    assert cumulative[0] == 3 and cumulative[1] == 4
    assert 'videoflow_proctime_seconds_bucket{node="detector",le="0.005"} 3' in text
    assert 'videoflow_proctime_seconds_bucket{node="detector",le="+Inf"} 12' in text
    # the in-memory snapshot is what was exported
    assert state.histogram('proctime_seconds') == cumulative
    assert state.histogram('never_observed') is None

_METRIC_NAME = re.compile(r'^[a-zA-Z_:][a-zA-Z0-9_:]*$')

def test_bucket_labels_are_what_prometheus_parses():
    state = HealthState('cam"1')      # a quote in a node name is stripped, as the old lines do
    state.observe('proctime_seconds', 0.2)
    seen = []
    for line in state.render_metrics().splitlines():
        if '_bucket{' not in line:
            continue
        name, rest = line.split('{', 1)
        labels, value = rest.rsplit('} ', 1)
        assert _METRIC_NAME.match(name) and value.isdigit()
        pairs = re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"\\\n]*)"', labels)
        assert ','.join(f'{k}="{v}"' for k, v in pairs) == labels       # nothing unparseable in between
        pairs = dict(pairs)
        assert set(pairs) == {'node', 'le'} and pairs['node'] == 'cam1'
        le = pairs['le']
        assert le == '+Inf' or (float(le) > 0 and repr(float(le)) == le)   # round-trips exactly
        seen.append(le)
    assert seen == [repr(b) for b in LATENCY_BUCKETS_SECONDS] + ['+Inf']

def test_p99_estimated_from_the_buckets_brackets_the_true_p99():
    rng = random.Random(1234)
    values = [rng.lognormvariate(-3.0, 1.2) for _ in range(5000)]       # a long-tailed latency population
    state = HealthState('n')
    for v in values:
        state.observe('proctime_seconds', v)
    bounds, cumulative = parse_histogram(state.render_metrics(), 'proctime_seconds')
    lower, upper = quantile_bounds(cumulative, 0.99, bounds)
    ordered = sorted(values)
    true_p99 = ordered[math.ceil(0.99 * len(ordered)) - 1]              # nearest rank
    assert lower < true_p99 <= upper
    assert 0.0 < lower and upper < math.inf                             # a real bucket, not a tail
    # the bracket is one bucket wide: that is the declared error bound
    assert (lower, upper) in list(zip(bounds, bounds[1:]))

def test_percentile_is_unavailable_without_buckets_rather_than_a_mean():
    empty = (0,) * (len(LATENCY_BUCKETS_SECONDS) + 1)
    assert quantile_bounds(empty, 0.95) is None
    # an older worker's count/sum-only exposition has no histogram to offer
    old = 'videoflow_proctime_seconds_count{node="n"} 100\nvideoflow_proctime_seconds_sum{node="n"} 1.0\n'
    assert parse_histogram(old, 'proctime_seconds') == ((), ())
    with pytest.raises(ValueError):
        quantile_bounds((), 0.95, ())
    with pytest.raises(ValueError):
        quantile_bounds((1,) * len(empty), 1.5)
    with pytest.raises(ConfigError):
        HealthState('n', buckets = [1.0, 0.5])
    with pytest.raises(ConfigError):
        HealthState('n', buckets = [0.5, math.inf])

class _EosInner(_FakeInner):
    def receive_message(self):
        self.received += 1
        return {'p': {'message': None, 'metadata': None, 'is_stop_signal': True}}

def test_offered_counts_work_not_end_of_stream():
    state = HealthState('n')
    im = InstrumentedMessenger(_FakeInner(), state)
    im.receive_message()
    im.receive_message()
    assert 'videoflow_messages_offered_total{node="n"} 2' in state.render_metrics()
    state = HealthState('n')
    InstrumentedMessenger(_EosInner(), state).receive_message()
    text = state.render_metrics()
    assert 'videoflow_messages_received_total{node="n"} 1' in text
    assert 'messages_offered' not in text

def test_drops_are_counted_from_the_delivery_policy_verdict():
    # best-effort (the REALTIME preset) discards a transient failure; at-least-once naks it
    for flow_type, dropped in ((REALTIME, True), (BATCH, False)):
        state = HealthState('n')
        im = InstrumentedMessenger(_FakeInner(), state, delivery_policy = DeliveryPolicy.default_for(flow_type))
        im.fail_inputs(TransientFailure('blip'))
        text = state.render_metrics()
        assert ('videoflow_messages_dropped_total{node="n",reason="best_effort"} 1' in text) is dropped
        assert 'videoflow_messages_failed_total{node="n"} 1' in text
    # poison is dead-lettered on its first failure in every mode, so it counts even without a policy
    for policy in (None, DeliveryPolicy.default_for(BATCH), DeliveryPolicy.default_for(REALTIME)):
        state = HealthState('n')
        InstrumentedMessenger(_FakeInner(), state, delivery_policy = policy).fail_inputs(SchemaError('bad row'))
        assert 'videoflow_messages_dropped_total{node="n",reason="poison"} 1' in state.render_metrics()
    # worker-fatal hands the message back to another replica: not a drop
    state = HealthState('n')
    InstrumentedMessenger(_FakeInner(), state,
                          delivery_policy = DeliveryPolicy.default_for(REALTIME)).fail_inputs(WorkerFatal('gpu gone'))
    assert 'messages_dropped' not in state.render_metrics()
    # a drop decided below the Messenger seam is reported by the messenger itself
    state.record_drop('join_evicted')
    assert 'videoflow_messages_dropped_total{node="n",reason="join_evicted"} 1' in state.render_metrics()

if __name__ == "__main__":
    pytest.main([__file__])
