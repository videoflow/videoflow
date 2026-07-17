'''
Policies that tune how a multi-parent (join) node aligns its inputs.

A ``JoinPolicy`` is attached to a node and travels with it as a plain dict (so it
survives ``get_params()`` serialization into a worker). It decides what to do with
a join group that never completes — a real possibility when one branch drops a
message (REALTIME) or stalls.
'''
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from .constants import BATCH, REALTIME

#: What to do with an incomplete join group once it times out.
MISSING_DROP = 'drop'    # give up on the group and ack the partial inputs
MISSING_WAIT = 'wait'    # never time out; wait indefinitely (bounded by max_pending)
MISSING_ERROR = 'error'  # nak the partial inputs so they redeliver / eventually dead-letter
MISSING_POLICIES = (MISSING_DROP, MISSING_WAIT, MISSING_ERROR)

class JoinPolicy:
    '''
    - Arguments:
        - timeout_seconds: how long to wait for the rest of a join group before \
            applying ``missing``. ``None`` means no timeout (wait forever).
        - missing: one of ``drop`` / ``wait`` / ``error`` (see constants above).
        - max_pending: hard cap on buffered incomplete groups; the oldest is \
            evicted (as ``drop``) beyond this, protecting against unbounded memory.
    '''
    def __init__(self, timeout_seconds = None, missing = MISSING_DROP, max_pending = 256):
        if missing not in MISSING_POLICIES:
            raise ValueError(f'missing must be one of {MISSING_POLICIES}, got {missing!r}')
        if missing == MISSING_WAIT:
            timeout_seconds = None
        self.timeout_seconds = timeout_seconds
        self.missing = missing
        self.max_pending = max_pending

    def to_dict(self):
        return {
            'timeout_seconds': self.timeout_seconds,
            'missing': self.missing,
            'max_pending': self.max_pending,
        }

    @classmethod
    def from_dict(cls, d):
        if d is None:
            return None
        return cls(
            timeout_seconds = d.get('timeout_seconds'),
            missing = d.get('missing', MISSING_DROP),
            max_pending = d.get('max_pending', 256),
        )

    @classmethod
    def default_for(cls, flow_type):
        '''
        BATCH waits (completeness matters; bounded by max_pending). REALTIME times
        out and drops (a dropped sibling frame must not stall the join forever).
        '''
        if flow_type == BATCH:
            return cls(timeout_seconds = None, missing = MISSING_WAIT)
        return cls(timeout_seconds = 10.0, missing = MISSING_DROP)
