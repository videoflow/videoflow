'''
Policies that tune how a node treats its inputs: how a multi-parent node *aligns*
them (``JoinPolicy``), and what happens to one when the node fails on it
(``DeliveryPolicy``).

Both are attached to a node and travel with it as a plain dict, so they survive
``get_params()`` serialization into a worker that never sees the object graph.

A ``JoinPolicy`` decides two things:

- **How input groups are formed** (``mode``): by lineage (``trace``, the default —
  inputs that descend from the same originating message of a single producer) or
  by **event time** (``time`` — inputs whose ``event_ts`` fall within a tolerance
  of each other, which is how streams from *independent* producers such as
  multiple cameras and sensors are fused).
- **What to do with a group that never completes** — a real possibility when one
  branch drops a message (REALTIME) or stalls: timeout + missing policy, and for
  time-aligned joins, an optional ``quorum`` that lets a late group emit with the
  parents it has.

A ``DeliveryPolicy`` decides what a *failure* costs. Its defaults come from the
flow type, because the correlation is real — a REALTIME flow wants freshest-wins
and a BATCH flow wants completeness — but each axis is separately overridable per
node, because loss tolerance is really a property of *what a node does with a
message*, not of the flow it happens to live in. The case that forces the split: a
REALTIME flow whose frame pipeline wants drop-on-failure but whose final sink
writes alerts to a database, where a dropped message is a missed incident.
'''
from __future__ import absolute_import, division, print_function

from typing import Any, Dict, Optional

from .constants import BATCH
from .errors import DISPOSITIONS, POISON, TRANSIENT, WORKER_FATAL

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
    def __init__(self, timeout_seconds : Optional[float] = None, missing : str = MISSING_DROP,
                max_pending : int = 256, mode : str = JOIN_TRACE,
                tolerance_ms : Optional[float] = None, quorum : Optional[int] = None,
                collect : Optional[dict] = None) -> None:
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

    def to_dict(self) -> Dict[str, Any]:
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
    def from_dict(cls, d : Optional[Dict[str, Any]]) -> Optional["JoinPolicy"]:
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
    def default_for(cls, flow_type : str) -> "JoinPolicy":
        '''
        BATCH waits (completeness matters; bounded by max_pending). REALTIME times
        out and drops (a dropped sibling frame must not stall the join forever).
        '''
        if flow_type == BATCH:
            return cls(timeout_seconds = None, missing = MISSING_WAIT)
        return cls(timeout_seconds = 10.0, missing = MISSING_DROP)


#: How much a node is willing to lose. ``at-least-once`` retries and dead-letters;
#: ``best-effort`` drops a failed message so the freshest one wins.
AT_LEAST_ONCE = 'at-least-once'
BEST_EFFORT = 'best-effort'
DELIVERY_MODES = (AT_LEAST_ONCE, BEST_EFFORT)

#: What reaches the dead-letter queue. ``sampled`` exists so best-effort failures
#: stop being invisible: "we drop things" is true of load shedding and false of
#: exceptions, and deleting the evidence of a bug is not a retention policy.
DLQ_FULL = 'full'
DLQ_SAMPLED = 'sampled'
DLQ_OFF = 'off'
DLQ_MODES = (DLQ_FULL, DLQ_SAMPLED, DLQ_OFF)

#: What the messenger does with a failed input group. Returned by
#: ``DeliveryPolicy.action_for``; executed by ``NATSMessenger.fail_inputs``.
ACTION_NAK = 'nak'                  # redeliver (to this or another replica)
ACTION_TERM = 'term'                # drop it; no redelivery, no dead-letter
ACTION_DLQ = 'dlq'                  # dead-letter, then terminate the original
ACTION_DLQ_SAMPLED = 'dlq_sampled'  # dead-letter if under the sample cap, then terminate
ACTIONS = (ACTION_NAK, ACTION_TERM, ACTION_DLQ, ACTION_DLQ_SAMPLED)

#: Default number of times a message is *retried* after the first delivery
#: attempt, for an at-least-once node. ``max_deliver = retries + 1``.
DEFAULT_MAX_RETRIES = 3

#: Dead-letters admitted per (code, node) per minute under ``DLQ_SAMPLED``.
DEFAULT_DLQ_SAMPLE_PER_MINUTE = 5

class DeliveryPolicy:
    '''
    What a node's failure costs: whether the message is retried, dropped or
    dead-lettered, and how many attempts it gets.

    The whole point of separating this from the flow type is that the *action*
    now depends on the error's disposition (see ``videoflow.core.errors``), not
    only on how patient the flow is. A message that will never parse should not
    burn four attempts and fourteen seconds of backoff on its way to the same
    dead-letter queue; and a worker whose GPU has wedged should not blame — and
    dead-letter — every message it touches.

    - Arguments:
        - delivery: ``at-least-once`` (retry, then dead-letter) or \
            ``best-effort`` (drop on failure). Defaults per flow type.
        - max_retries: redelivery attempts after the first, for an at-least-once \
            node. Ignored for best-effort, which never redelivers.
        - dlq: ``full`` (every exhausted message), ``sampled`` (a bounded number \
            per code per minute — the best-effort default) or ``off``.
        - on_error: the disposition assumed for an exception nothing has \
            classified. ``None`` means the framework default (transient).
        - sample_per_minute: cap for ``dlq = 'sampled'``.
    '''
    def __init__(self, delivery : str = AT_LEAST_ONCE,
                max_retries : int = DEFAULT_MAX_RETRIES, dlq : str = DLQ_FULL,
                on_error : Optional[str] = None,
                sample_per_minute : int = DEFAULT_DLQ_SAMPLE_PER_MINUTE) -> None:
        if delivery not in DELIVERY_MODES:
            raise ValueError(f'delivery must be one of {DELIVERY_MODES}, got {delivery!r}')
        if dlq not in DLQ_MODES:
            raise ValueError(f'dlq must be one of {DLQ_MODES}, got {dlq!r}')
        if on_error is not None and on_error not in DISPOSITIONS:
            raise ValueError(f'on_error must be one of {DISPOSITIONS} or None, got {on_error!r}')
        if max_retries < 0:
            raise ValueError(f'max_retries must be >= 0, got {max_retries}')
        if sample_per_minute < 0:
            raise ValueError(f'sample_per_minute must be >= 0, got {sample_per_minute}')
        self.delivery = delivery
        self.max_retries = max_retries
        self.dlq = dlq
        self.on_error = on_error
        self.sample_per_minute = sample_per_minute

    @property
    def max_deliver(self) -> int:
        '''
        Broker-side delivery cap for this node's durable consumers. A best-effort
        node never redelivers (1); an at-least-once node gets ``retries + 1``.
        Provisioning and the messenger must agree on this, so both read it here.
        '''
        return 1 if self.delivery == BEST_EFFORT else self.max_retries + 1

    def action_for(self, disposition : str, num_delivered : int) -> str:
        '''
        The action for a failed input group, given how the error was classified
        and how many times the broker has delivered it.

        The table, which ``spec/PROTOCOL.md`` §7.3 states normatively:

        =============  ====================  ==============================
        disposition    best-effort           at-least-once
        =============  ====================  ==============================
        poison         sampled DLQ, term     DLQ immediately, term
        transient      term                  nak until budget, then DLQ
        worker_fatal   nak                   nak
        =============  ====================  ==============================

        ``worker_fatal`` never dead-letters in either mode: the message is fine,
        this worker is not, so it goes back for another replica (or this pod's
        replacement) while the circuit breaker takes the worker out.

        - Arguments:
            - disposition: one of ``videoflow.core.errors.DISPOSITIONS``.
            - num_delivered: the broker's delivery count for this message, \
                starting at 1 for the first attempt.

        - Raises:
            - ValueError: on an unknown disposition (the message names the known ones).
        '''
        if disposition not in DISPOSITIONS:
            raise ValueError(f'Unknown disposition {disposition!r}. Known: '
                            f'{", ".join(DISPOSITIONS)}.')
        if disposition == WORKER_FATAL:
            return ACTION_NAK
        if disposition == POISON:
            # Never retried in either mode: it failed on its content, and its
            # content will not change.
            return self._dead_letter_action()
        assert disposition == TRANSIENT, disposition   # exhaustive over DISPOSITIONS
        if self.delivery == BEST_EFFORT:
            return ACTION_TERM if self.dlq != DLQ_SAMPLED else ACTION_DLQ_SAMPLED
        if num_delivered >= self.max_deliver:
            return self._dead_letter_action()
        return ACTION_NAK

    def _dead_letter_action(self) -> str:
        if self.dlq == DLQ_OFF:
            return ACTION_TERM
        if self.dlq == DLQ_SAMPLED:
            return ACTION_DLQ_SAMPLED
        return ACTION_DLQ

    def retry_delay(self, num_delivered : int, jitter : float = 1.0) -> float:
        '''
        Backoff before a redelivery. Jittered because a deterministic schedule
        makes N replicas that failed together retry together — a thundering herd
        against whatever just recovered.

        - Arguments:
            - num_delivered: the broker's delivery count (1 for the first attempt).
            - jitter: multiplier in [0.5, 1.5]; the caller supplies the random \
                draw so the function stays pure and testable.
        '''
        return min(2 ** max(1, num_delivered), 30) * jitter

    def to_dict(self) -> Dict[str, Any]:
        return {
            'delivery': self.delivery,
            'max_retries': self.max_retries,
            'dlq': self.dlq,
            'on_error': self.on_error,
            'sample_per_minute': self.sample_per_minute,
        }

    @classmethod
    def from_dict(cls, d : Optional[Dict[str, Any]]) -> Optional["DeliveryPolicy"]:
        if d is None:
            return None
        return cls(
            delivery = d.get('delivery', AT_LEAST_ONCE),
            max_retries = d.get('max_retries', DEFAULT_MAX_RETRIES),
            dlq = d.get('dlq', DLQ_FULL),
            on_error = d.get('on_error'),
            sample_per_minute = d.get('sample_per_minute', DEFAULT_DLQ_SAMPLE_PER_MINUTE),
        )

    @classmethod
    def default_for(cls, flow_type : str) -> "DeliveryPolicy":
        '''
        BATCH is at-least-once with a full DLQ (completeness matters). REALTIME is
        best-effort with a *sampled* DLQ — it drops failed messages, but keeps a
        bounded specimen of each distinct failure so a bug is still diagnosable.
        '''
        if flow_type == BATCH:
            return cls(delivery = AT_LEAST_ONCE, max_retries = DEFAULT_MAX_RETRIES,
                    dlq = DLQ_FULL)
        return cls(delivery = BEST_EFFORT, max_retries = 0, dlq = DLQ_SAMPLED)

    @classmethod
    def resolve(cls, flow_type : str, override : Optional[Dict[str, Any]] = None,
                max_retries : Optional[int] = None) -> "DeliveryPolicy":
        '''
        The effective policy for one node: the flow-type preset, with a node's own
        ``delivery=``/``on_error=`` applied on top, and a deployment-level retry
        count (``VF_MAX_RETRIES``) applied last.

        Overriding ``delivery`` alone also moves the DLQ mode and the retry
        budget with it, because those are what the mode *means* — an
        at-least-once sink in a REALTIME flow wants a real DLQ, not a sampled
        one, and would otherwise silently keep ``max_retries = 0``.
        '''
        policy = cls.default_for(flow_type)
        if override:
            delivery = override.get('delivery')
            if delivery and delivery != policy.delivery:
                policy = (cls.default_for(BATCH) if delivery == AT_LEAST_ONCE
                        else cls(delivery = BEST_EFFORT, max_retries = 0, dlq = DLQ_SAMPLED))
            for field in ('max_retries', 'dlq', 'on_error', 'sample_per_minute'):
                if override.get(field) is not None:
                    setattr(policy, field, override[field])
        if max_retries is not None and policy.delivery == AT_LEAST_ONCE:
            policy.max_retries = max_retries
        return policy
