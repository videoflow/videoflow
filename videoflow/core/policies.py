'''
Policies that tune how a multi-parent (join) node aligns its inputs.

A ``JoinPolicy`` is attached to a node and travels with it as a plain dict (so it
survives ``get_params()`` serialization into a worker). It decides two things:

- **How input groups are formed** (``mode``): by lineage (``trace``, the default —
  inputs that descend from the same originating message of a single producer) or
  by **event time** (``time`` — inputs whose ``event_ts`` fall within a tolerance
  of each other, which is how streams from *independent* producers such as
  multiple cameras and sensors are fused).
- **What to do with a group that never completes** — a real possibility when one
  branch drops a message (REALTIME) or stalls: timeout + missing policy, and for
  time-aligned joins, an optional ``quorum`` that lets a late group emit with the
  parents it has.
'''
from __future__ import absolute_import, division, print_function

from typing import Optional

from .constants import BATCH

#: What to do with an incomplete join group once it times out.
MISSING_DROP = 'drop'    # give up on the group and ack the partial inputs
MISSING_WAIT = 'wait'    # never time out; wait indefinitely (bounded by max_pending)
MISSING_ERROR = 'error'  # nak the partial inputs so they redeliver / eventually dead-letter
MISSING_POLICIES = (MISSING_DROP, MISSING_WAIT, MISSING_ERROR)

#: How input groups are formed at a multi-parent node.
JOIN_TRACE = 'trace'  # by lineage: exact trace_id match (single-producer diamonds)
JOIN_TIME = 'time'    # by event time: event_ts within tolerance (independent producers)
JOIN_MODES = (JOIN_TRACE, JOIN_TIME)

class JoinPolicy:
    '''
    - Arguments:
        - timeout_seconds: how long to wait for the rest of a join group before \
            applying ``missing`` (or emitting a quorum group). ``None`` means no \
            timeout (wait forever). For ``mode='time'`` this is the lateness bound: \
            the answer to "how long after a window's first message may a straggler \
            still arrive".
        - missing: one of ``drop`` / ``wait`` / ``error`` (see constants above).
        - max_pending: hard cap on buffered incomplete groups; the oldest is \
            evicted (as ``drop``) beyond this, protecting against unbounded memory.
        - mode: ``trace`` (default) or ``time``. ``time`` groups inputs whose \
            ``event_ts`` (stamped by the producers) are within ``tolerance_ms`` of \
            each other, instead of requiring a shared upstream trace id — required \
            to join branches that descend from different producers.
        - tolerance_ms: (``time`` mode only, required) two messages from different \
            parents belong to the same group when their event times differ by at \
            most this much. Pick it below the fastest parent's inter-message period \
            (e.g. for 50fps cameras, < 20ms).
        - quorum: (``time`` mode only) minimum number of synchronized parents that \
            must be present for a *timed-out* group to still be emitted (missing \
            parents are passed to ``process()`` as ``None``). ``None`` (default) \
            means all parents are required and a timed-out group is handled by \
            ``missing``. With N cameras, ``quorum=k`` gives "emit with at least k \
            views". Requires ``timeout_seconds``.
        - collect: (``time`` mode only) dict ``{parent_name: window_ms}`` marking \
            high-rate parents (e.g. a 500Hz sensor vs 50fps cameras) that should \
            not join 1:1: every message of that parent whose ``event_ts`` is within \
            ``window_ms`` of the group's time is delivered *as a list* in that \
            parent's position. Collect parents never gate completeness and don't \
            count toward ``quorum``; a group holds for the largest collect window \
            after completing so trailing samples can arrive.
    '''
    def __init__(self, timeout_seconds = None, missing = MISSING_DROP, max_pending = 256,
                mode = JOIN_TRACE, tolerance_ms = None, quorum = None, collect = None) -> None:
        if missing not in MISSING_POLICIES:
            raise ValueError(f'missing must be one of {MISSING_POLICIES}, got {missing!r}')
        if mode not in JOIN_MODES:
            raise ValueError(f'mode must be one of {JOIN_MODES}, got {mode!r}')
        if missing == MISSING_WAIT:
            timeout_seconds = None
        if mode == JOIN_TIME:
            if not tolerance_ms or tolerance_ms <= 0:
                raise ValueError("mode='time' requires a positive tolerance_ms")
        elif tolerance_ms is not None or quorum is not None or collect:
            raise ValueError("tolerance_ms/quorum/collect only apply to mode='time'")
        if quorum is not None:
            if quorum < 1:
                raise ValueError(f'quorum must be >= 1, got {quorum}')
            if timeout_seconds is None:
                raise ValueError('quorum requires timeout_seconds (a quorum group is only '
                                'emitted once the timeout says no more parents are coming)')
        if collect:
            for parent, window_ms in collect.items():
                if not window_ms or window_ms <= 0:
                    raise ValueError(f'collect window for {parent!r} must be a positive '
                                    f'number of milliseconds, got {window_ms!r}')
        self.timeout_seconds = timeout_seconds
        self.missing = missing
        self.max_pending = max_pending
        self.mode = mode
        self.tolerance_ms = tolerance_ms
        self.quorum = quorum
        self.collect = dict(collect) if collect else {}

    def to_dict(self) -> dict:
        return {
            'timeout_seconds': self.timeout_seconds,
            'missing': self.missing,
            'max_pending': self.max_pending,
            'mode': self.mode,
            'tolerance_ms': self.tolerance_ms,
            'quorum': self.quorum,
            'collect': self.collect,
        }

    @classmethod
    def from_dict(cls, d) -> Optional["JoinPolicy"]:
        if d is None:
            return None
        return cls(
            timeout_seconds = d.get('timeout_seconds'),
            missing = d.get('missing', MISSING_DROP),
            max_pending = d.get('max_pending', 256),
            mode = d.get('mode', JOIN_TRACE),
            tolerance_ms = d.get('tolerance_ms'),
            quorum = d.get('quorum'),
            collect = d.get('collect'),
        )

    @classmethod
    def default_for(cls, flow_type) -> "JoinPolicy":
        '''
        BATCH waits (completeness matters; bounded by max_pending). REALTIME times
        out and drops (a dropped sibling frame must not stall the join forever).
        '''
        if flow_type == BATCH:
            return cls(timeout_seconds = None, missing = MISSING_WAIT)
        return cls(timeout_seconds = 10.0, missing = MISSING_DROP)
