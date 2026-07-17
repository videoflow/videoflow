'''
Unit tests for time-synchronized joins: the extended JoinPolicy and the two
group assemblers (trace-id and event-time). These run against fake ack handles —
no broker needed — because the assemblers were factored out of the messenger
precisely so this logic is testable in isolation.
'''
import time

import pytest

from videoflow.core.policies import (
    JOIN_TIME,
    JOIN_TRACE,
    MISSING_DROP,
    MISSING_ERROR,
    JoinPolicy,
)
from videoflow.messaging.grouping import (
    TimeGroupAssembler,
    TraceGroupAssembler,
    make_assembler,
)


class FakeHandle:
    def __init__(self):
        self.state = None  # None (unresolved) | 'acked' | 'naked' | 'termed'

    def ack(self):
        self.state = 'acked'

    def nak(self, delay = None):
        self.state = 'naked'

    def term(self):
        self.state = 'termed'


def entry(trace_id, seq, event_ts = None, message = 'm', metadata = None):
    return {
        'trace_id': trace_id,
        'seq': seq,
        'event_ts': event_ts,
        'message': message,
        'metadata': metadata,
        'is_stop_signal': False,
    }

# -- JoinPolicy ------------------------------------------------------------

def test_policy_round_trip_preserves_time_fields():
    p = JoinPolicy(timeout_seconds = 1.5, missing = MISSING_DROP, mode = JOIN_TIME,
                tolerance_ms = 10, quorum = 2, collect = {'imu': 25})
    q = JoinPolicy.from_dict(p.to_dict())
    assert q.mode == JOIN_TIME
    assert q.tolerance_ms == 10
    assert q.quorum == 2
    assert q.collect == {'imu': 25}
    assert q.timeout_seconds == 1.5

def test_policy_defaults_to_trace_mode():
    p = JoinPolicy.from_dict(JoinPolicy(timeout_seconds = 5).to_dict())
    assert p.mode == JOIN_TRACE

def test_policy_validation():
    with pytest.raises(ValueError):
        JoinPolicy(mode = JOIN_TIME)  # tolerance required
    with pytest.raises(ValueError):
        JoinPolicy(tolerance_ms = 10)  # time-only field in trace mode
    with pytest.raises(ValueError):
        JoinPolicy(mode = JOIN_TIME, tolerance_ms = 10, quorum = 2)  # quorum needs timeout
    with pytest.raises(ValueError):
        JoinPolicy(mode = JOIN_TIME, tolerance_ms = 10, collect = {'imu': 0})

def test_make_assembler_dispatches_on_mode():
    trace = make_assembler('n', ['a', 'b'], JoinPolicy())
    timed = make_assembler('n', ['a', 'b'], JoinPolicy(mode = JOIN_TIME, tolerance_ms = 5))
    assert isinstance(trace, TraceGroupAssembler)
    assert isinstance(timed, TimeGroupAssembler)

# -- TraceGroupAssembler ---------------------------------------------------

def test_trace_group_completes_on_matching_trace_id():
    asm = TraceGroupAssembler('n', ['a', 'b'], JoinPolicy())
    ha, hb = FakeHandle(), FakeHandle()
    asm.add('a', entry('t1', 3, message = 'A'), ha)
    assert asm.pop_ready() is None
    asm.add('b', entry('t1', 7, message = 'B'), hb)
    ready = asm.pop_ready()
    assert ready is not None
    assert ready.trace_id == 't1'
    assert ready.seq == 3  # min over the group
    assert ready.entries['a']['message'] == 'A'
    assert ready.entries['b']['message'] == 'B'
    assert set(ready.handles) == {ha, hb}
    assert ha.state is None and hb.state is None  # emitted unresolved
    assert not asm.has_pending_from('a')

def test_trace_groups_from_different_producers_never_match():
    asm = TraceGroupAssembler('n', ['a', 'b'], JoinPolicy())
    asm.add('a', entry('cam1:1', 1), FakeHandle())
    asm.add('b', entry('cam2:1', 1), FakeHandle())
    assert asm.pop_ready() is None  # this is exactly what time mode exists to fix

def test_trace_timeout_evicts_with_drop_and_error():
    for missing, expected in ((MISSING_DROP, 'acked'), (MISSING_ERROR, 'naked')):
        asm = TraceGroupAssembler('n', ['a', 'b'],
                                JoinPolicy(timeout_seconds = 5, missing = missing))
        h = FakeHandle()
        asm.add('a', entry('t1', 1), h)
        asm.sweep(now = time.monotonic() + 6)
        assert h.state == expected
        assert not asm.has_pending_from('a')

def test_trace_max_pending_evicts_oldest():
    asm = TraceGroupAssembler('n', ['a', 'b'], JoinPolicy(max_pending = 2))
    handles = [FakeHandle() for _ in range(3)]
    for i, h in enumerate(handles):
        asm.add('a', entry(f't{i}', i), h)
    assert handles[0].state == 'acked'  # oldest dropped
    assert handles[1].state is None and handles[2].state is None

def test_trace_redelivery_supersedes_buffered_half():
    asm = TraceGroupAssembler('n', ['a', 'b'], JoinPolicy())
    stale, fresh = FakeHandle(), FakeHandle()
    asm.add('a', entry('t1', 1), stale)
    asm.add('a', entry('t1', 1), fresh)
    assert stale.state == 'termed'
    asm.add('b', entry('t1', 2), FakeHandle())
    ready = asm.pop_ready()
    assert fresh in ready.handles and stale not in ready.handles

# -- TimeGroupAssembler ----------------------------------------------------

def _time_policy(**kwargs):
    kwargs.setdefault('mode', JOIN_TIME)
    kwargs.setdefault('tolerance_ms', 10)
    return JoinPolicy(**kwargs)

def test_time_group_joins_within_tolerance():
    asm = TimeGroupAssembler('n', ['cam1', 'cam2'], _time_policy())
    t0 = 1000.0
    asm.add('cam1', entry('cam1:5', 5, event_ts = t0, message = 'F1'), FakeHandle())
    assert asm.pop_ready() is None
    asm.add('cam2', entry('cam2:9', 9, event_ts = t0 + 0.004, message = 'F2'), FakeHandle())
    ready = asm.pop_ready()
    assert ready is not None
    assert ready.entries['cam1']['message'] == 'F1'
    assert ready.entries['cam2']['message'] == 'F2'
    assert ready.event_ts == t0  # min over members
    assert ready.trace_id.startswith('tw-')

def test_time_group_outside_tolerance_stays_separate():
    asm = TimeGroupAssembler('n', ['cam1', 'cam2'], _time_policy())
    asm.add('cam1', entry('cam1:1', 1, event_ts = 1000.0), FakeHandle())
    asm.add('cam2', entry('cam2:1', 1, event_ts = 1000.5), FakeHandle())
    assert asm.pop_ready() is None
    assert asm.has_pending_from('cam1') and asm.has_pending_from('cam2')

def test_time_group_picks_nearest_candidate():
    asm = TimeGroupAssembler('n', ['cam1', 'cam2'], _time_policy(tolerance_ms = 100))
    asm.add('cam1', entry('cam1:1', 1, event_ts = 1000.00, message = 'early'), FakeHandle())
    asm.add('cam1', entry('cam1:2', 2, event_ts = 1000.08, message = 'late'), FakeHandle())
    asm.add('cam2', entry('cam2:1', 1, event_ts = 1000.07, message = 'match'), FakeHandle())
    ready = asm.pop_ready()
    assert ready.entries['cam1']['message'] == 'late'  # nearest in time wins

def test_time_quorum_emits_partial_after_timeout():
    asm = TimeGroupAssembler('n', ['cam1', 'cam2', 'cam3'],
                            _time_policy(timeout_seconds = 2, quorum = 2))
    t0 = 1000.0
    asm.add('cam1', entry('cam1:1', 1, event_ts = t0, message = 'F1'), FakeHandle())
    asm.add('cam2', entry('cam2:1', 1, event_ts = t0 + 0.002, message = 'F2'), FakeHandle())
    assert asm.pop_ready() is None  # incomplete, not yet timed out
    asm.sweep(now = time.monotonic() + 3)
    ready = asm.pop_ready()
    assert ready is not None
    assert ready.entries['cam1']['message'] == 'F1'
    assert ready.entries['cam2']['message'] == 'F2'
    assert ready.entries['cam3'] is None  # missing parent surfaced as None
    assert len(ready.handles) == 2

def test_time_below_quorum_evicted_after_timeout():
    asm = TimeGroupAssembler('n', ['cam1', 'cam2', 'cam3'],
                            _time_policy(timeout_seconds = 2, quorum = 2,
                                        missing = MISSING_DROP))
    h = FakeHandle()
    asm.add('cam1', entry('cam1:1', 1, event_ts = 1000.0), h)
    asm.sweep(now = time.monotonic() + 3)
    assert asm.pop_ready() is None
    assert h.state == 'acked'

def test_time_collect_parent_delivers_window_as_list():
    asm = TimeGroupAssembler('n', ['cam1', 'imu'],
                            _time_policy(collect = {'imu': 30}))
    t0 = 1000.0
    # IMU samples straddling the frame time; 2 in-window (±30ms), 1 outside.
    imu_handles = [FakeHandle() for _ in range(3)]
    asm.add('imu', entry('imu:1', 1, event_ts = t0 - 0.020, message = 's1'), imu_handles[0])
    asm.add('imu', entry('imu:2', 2, event_ts = t0 + 0.010, message = 's2'), imu_handles[1])
    asm.add('imu', entry('imu:3', 3, event_ts = t0 + 0.200, message = 's3'), imu_handles[2])
    asm.add('cam1', entry('cam1:1', 1, event_ts = t0, message = 'F1'), FakeHandle())
    # Complete, but held for the settle window (30ms) for trailing samples.
    assert asm.pop_ready(now = time.monotonic()) is None
    ready = asm.pop_ready(now = time.monotonic() + 1)
    assert ready is not None
    assert ready.entries['cam1']['message'] == 'F1'
    assert ready.entries['imu']['message'] == ['s1', 's2']  # sorted by event time
    assert ready.entries['imu']['event_ts'] == [t0 - 0.020, t0 + 0.010]
    assert imu_handles[0] in ready.handles and imu_handles[1] in ready.handles
    assert imu_handles[2] not in ready.handles      # out-of-window sample kept buffered
    assert asm.has_pending_from('imu')

def test_time_collect_stale_samples_pruned():
    asm = TimeGroupAssembler('n', ['cam1', 'imu'],
                            _time_policy(timeout_seconds = 2, collect = {'imu': 30}))
    h = FakeHandle()
    asm.add('imu', entry('imu:1', 1, event_ts = 1000.0), h)
    asm.sweep(now = time.monotonic() + 10)  # far beyond timeout + settle retention
    assert h.state == 'acked'
    assert not asm.has_pending_from('imu')

def test_time_redelivery_supersedes_in_group():
    asm = TimeGroupAssembler('n', ['cam1', 'cam2'], _time_policy())
    stale, fresh = FakeHandle(), FakeHandle()
    asm.add('cam1', entry('cam1:1', 1, event_ts = 1000.0), stale)
    asm.add('cam1', entry('cam1:1', 1, event_ts = 1000.0), fresh)
    assert stale.state == 'termed'
    asm.add('cam2', entry('cam2:1', 1, event_ts = 1000.001), FakeHandle())
    ready = asm.pop_ready()
    assert fresh in ready.handles and stale not in ready.handles

def test_time_group_seq_is_deterministic_for_dedup():
    def build():
        asm = TimeGroupAssembler('n', ['cam1', 'cam2'], _time_policy())
        asm.add('cam1', entry('cam1:1', 1, event_ts = 1000.0), FakeHandle())
        asm.add('cam2', entry('cam2:1', 1, event_ts = 1000.002), FakeHandle())
        return asm.pop_ready()

    a, b = build(), build()
    # The same members regrouping after a crash derive the same identity.
    assert a.trace_id == b.trace_id and a.seq == b.seq

def test_time_assembler_validation():
    with pytest.raises(ValueError):  # collect names a non-parent
        TimeGroupAssembler('n', ['cam1'], _time_policy(collect = {'ghost': 10}))
    with pytest.raises(ValueError):  # every parent collected: nothing anchors groups
        TimeGroupAssembler('n', ['imu'], _time_policy(collect = {'imu': 10}))
    with pytest.raises(ValueError):  # quorum larger than sync parent count
        TimeGroupAssembler('n', ['cam1', 'cam2'],
                        _time_policy(timeout_seconds = 1, quorum = 3))

if __name__ == '__main__':
    pytest.main([__file__])
