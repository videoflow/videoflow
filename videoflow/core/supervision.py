'''
When to give up — on a message, on a worker, or on a whole flow.

Three policies live here, sharing one reason to change: they all answer "has this
stopped being worth waiting for?", and they are all pure (no I/O, no optional
dependencies) so both engines and the task loop can hold them.

- ``ConsecutiveFailureBreaker`` — worker-scoped, and specifically for the
  failures nothing classified. An error that *says* it is worker-fatal already
  ends its worker on the spot (see ``videoflow.core.task``); the breaker is what
  catches the same illness when it arrives unlabelled. Data failures are sparse
  and independent, worker failures are dense and correlated, and counting a run
  of them tells the two apart without needing the taxonomy to be right.
- ``ProgressDeadline`` — node-scoped. Distinguishes "slow" from "wedged" by
  watching acks against pending work rather than the wall clock, because a
  legitimately slow model and a hung one look identical to a timer.
- ``SupervisionPolicy`` — engine-scoped. How a dead worker is restarted. Held here
  rather than in either engine so the local engine and the Kubernetes engine agree
  by construction: the same object is honoured by a supervisor thread in one and
  rendered into a Job ``backoffLimit`` in the other. They used to diverge, and the
  divergence meant the development environment was the one place recovery was
  never exercised.

The lifecycle events at the bottom are the other half of that parity: both engines
emit the same records, so one renderer serves both.
'''
from __future__ import absolute_import, division, print_function

import time
from dataclasses import dataclass, field
from typing import Callable, Dict, FrozenSet, Optional, Tuple

from .errors import POISON, TRANSIENT, WORKER_FATAL, ProgressStalled, WorkerUnhealthy

#: Consecutive failures before a worker declares itself sick. Ten is high enough
#: that a run of unlucky-but-independent bad messages does not trip it, and low
#: enough that a wedged worker is out in seconds rather than after it has
#: dead-lettered a stream.
DEFAULT_BREAKER_THRESHOLD = 10

#: Seconds a node may ack nothing while work is pending before it is declared
#: stalled. Generous, because the false positive (killing a slow but healthy node)
#: is worse than the false negative (a wedged node found a minute later).
DEFAULT_PROGRESS_TIMEOUT_SECONDS = 300

#: Dispositions a fresh worker might survive. Everything else is final *by
#: nature* — no policy, cluster or backoff changes the outcome.
RECOVERABLE_DISPOSITIONS = frozenset({TRANSIENT, WORKER_FATAL})

def is_terminal(disposition : Optional[str]) -> bool:
    '''
    Whether a death of this kind is beyond any restart's help.

    This is what decides who gets to announce a worker's death. A dying worker
    must not tell its children the stream is over when a replacement is about to
    take its place — that would turn a recoverable crash into a flow-wide
    failure, which is the opposite of the point. So a worker publishes an ABORT
    only for a death nothing can fix (a poison message will poison the
    replacement identically), and every other death is announced by the
    supervisor, once it has actually given up (``ABORT-6``).

    ``None`` — a worker that died without saying why — is treated as recoverable,
    for the same reason ``SupervisionPolicy.should_restart`` does: an unexplained
    death is far more often a crash worth retrying than a poison message.
    '''
    return disposition is not None and disposition not in RECOVERABLE_DISPOSITIONS

class ConsecutiveFailureBreaker:
    '''
    Trips when a worker fails ``threshold`` messages in a row — the signature of a
    sick *worker* rather than of bad *data*. Any successful ack resets the count,
    so poison messages interleaved with successful ones never trip it.

    This is the backstop for failures that arrive **unclassified**. A node that
    raises an explicitly worker-fatal error is believed immediately; the breaker
    is what notices the same illness when it shows up as a run of ordinary-looking
    exceptions instead.

    The failure mode it exists for: one pod's GPU wedges at 03:00 behind a library
    error nothing recognizes and, by 03:20, ten thousand perfectly good messages
    are in the DLQ while the flow looks healthy from outside. With the breaker the
    pod dies after ten, Kubernetes replaces it, and those ten are redelivered.

    - Arguments:
        - threshold: consecutive failures that trip it. 0 disables the breaker.
        - node_name: named in the raised error, since that is what an operator reads.
    '''
    def __init__(self, threshold : int = DEFAULT_BREAKER_THRESHOLD,
                node_name : str = '') -> None:
        self._threshold = threshold
        self._node_name = node_name
        self._consecutive = 0
        self._last_error : Optional[BaseException] = None

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive

    def record_success(self) -> None:
        '''A message was processed and acked: the worker is demonstrably alive.'''
        self._consecutive = 0
        self._last_error = None

    def record_failure(self, exc : BaseException) -> None:
        '''
        A message failed. Counts toward the trip regardless of disposition — the
        breaker deliberately does *not* consult the taxonomy, because its whole
        value is catching the failures the taxonomy got wrong.
        '''
        self._consecutive += 1
        self._last_error = exc

    @property
    def tripped(self) -> bool:
        return self._threshold > 0 and self._consecutive >= self._threshold

    def check(self) -> None:
        '''
        - Raises:
            - WorkerUnhealthy: if the breaker has tripped. The caller lets this \
                out of the run loop so the un-acked inputs return to the broker \
                for a healthy replica.
        '''
        if not self.tripped:
            return
        raise WorkerUnhealthy(
            f'{self._node_name or "node"} failed {self._consecutive} messages in a row, '
            f'which means this worker is unhealthy rather than the messages being bad '
            f'(last error: {self._last_error!r}).',
            remedy = ('The inputs are left un-acked so another replica retries them. '
                    'Check this worker for a wedged device, an exhausted resource, or a '
                    'dependency it alone cannot reach.'),
            node = self._node_name,
            consecutive_failures = self._consecutive,
        )

class ProgressDeadline:
    '''
    Trips when a node has acked nothing for ``timeout`` seconds **while work was
    pending** — work is available and none is being done.

    Pending-aware on purpose. A wall-clock deadline cannot tell a slow model from
    a hung one, and an idle node (nothing upstream to do) is not stalled at all;
    checking acks against the broker's pending count separates all three. This is
    the only stall detection a BATCH flow has: every BATCH node is a Job, Job pods
    have their probes stripped, and neither ``wait_for_completion`` nor the Job
    itself carries an overall timeout.

    - Arguments:
        - timeout_seconds: allowed silence while work is pending. 0 disables it.
        - pending_probe: returns how many messages are waiting for this node. \
            Injected rather than imported so this stays pure — the messenger \
            passes its own broker query.
        - node_name: named in the raised error.
        - clock: monotonic time source; injected for tests.
    '''
    def __init__(self, timeout_seconds : float = DEFAULT_PROGRESS_TIMEOUT_SECONDS,
                pending_probe : Optional[Callable[[], int]] = None,
                node_name : str = '',
                clock : Callable[[], float] = time.monotonic) -> None:
        self._timeout = timeout_seconds
        self._probe = pending_probe
        self._node_name = node_name
        self._clock = clock
        self._last_progress = clock()

    def record_progress(self) -> None:
        '''Called on every ack (and on every failure — a failure is still work being done).'''
        self._last_progress = self._clock()

    def silent_for(self) -> float:
        return self._clock() - self._last_progress

    def check(self) -> None:
        '''
        - Raises:
            - ProgressStalled: if nothing has been acked for ``timeout`` while \
                the broker still reports pending work. The probe is consulted \
                only after the silence threshold is crossed, so the common path \
                costs nothing.
        '''
        if self._timeout <= 0 or self._probe is None:
            return
        silence = self.silent_for()
        if silence < self._timeout:
            return
        pending = self._probe()
        if pending <= 0:
            # Idle, not stalled: there is simply nothing to do. Reset so the next
            # arrival gets a full window.
            self._last_progress = self._clock()
            return
        raise ProgressStalled(
            f'{self._node_name or "node"} has acknowledged nothing for {silence:.0f}s '
            f'while {pending} message(s) are pending — it is not slow, it is stuck.',
            remedy = ('Look for a blocking call in process()/consume() with no timeout: '
                    'an unbounded read, a lock, or a request to a service that never '
                    'answers. Raise VF_PROGRESS_TIMEOUT_SECONDS if the node is '
                    'legitimately this slow.'),
            node = self._node_name,
            silent_seconds = round(silence, 1),
            pending = pending,
        )

@dataclass(frozen = True)
class SupervisionPolicy:
    '''
    How an engine responds to a worker exiting non-zero.

    The defaults mirror the Kubernetes Job semantics the manifests already encode
    (``backoffLimit: 3``), so the local engine and the cluster agree by
    construction rather than by coincidence. Only the backoff differs — see
    ``local()``.

    - Attributes:
        - max_restarts: attempts after the first launch. 0 disables restarts.
        - backoff_seconds: delay before each restart; the last value repeats.
        - restart_on: dispositions worth restarting for. ``POISON`` is \
            deliberately absent — a worker that died of a bad message will die \
            of it again, and three more identical crashes help nobody.
    '''
    max_restarts : int = 3
    backoff_seconds : Tuple[float, ...] = (10.0, 20.0, 40.0)
    restart_on : FrozenSet[str] = frozenset({TRANSIENT, WORKER_FATAL})

    @classmethod
    def local(cls) -> "SupervisionPolicy":
        '''
        Same restart count as the cluster, compressed backoff. A developer
        watching a terminal should not wait seventy seconds to learn that a node
        is genuinely broken, but the recovery path still has to be exercised —
        that is the entire reason the local supervisor exists.
        '''
        return cls(max_restarts = 3, backoff_seconds = (1.0, 2.0, 4.0))

    @classmethod
    def disabled(cls) -> "SupervisionPolicy":
        '''No restarts — ``run-local --no-restart``, for a tight debug loop.'''
        return cls(max_restarts = 0, backoff_seconds = ())

    def should_restart(self, attempt : int, disposition : Optional[str] = None) -> bool:
        '''
        - Arguments:
            - attempt: how many restarts have already been spent (0 on the first failure).
            - disposition: the classified cause, when the worker managed to \
                report one (via its termination log). ``None`` — a worker that \
                died without saying why — is restarted: an unexplained death is \
                far more often a crash worth retrying than a poison message.
        '''
        if attempt >= self.max_restarts:
            return False
        if disposition is None:
            return True
        return disposition in self.restart_on

    def delay_for(self, attempt : int) -> float:
        '''Backoff before restart number ``attempt`` (0-based); the last value repeats.'''
        if not self.backoff_seconds:
            return 0.0
        return self.backoff_seconds[min(attempt, len(self.backoff_seconds) - 1)]

# -- lifecycle events --------------------------------------------------------

@dataclass(frozen = True)
class NodeStarted:
    '''A worker was launched. ``attempt`` is 0 for the first launch.'''
    node : str
    replica : int
    attempt : int = 0

@dataclass(frozen = True)
class NodeExited:
    '''
    A worker exited. ``code`` is the process exit status; ``reason`` is the
    structured cause it reported (from its termination log), when it managed to.
    '''
    node : str
    replica : int
    code : int
    reason : Optional[Dict[str, object]] = None

    @property
    def disposition(self) -> Optional[str]:
        value = (self.reason or {}).get('disposition')
        return value if isinstance(value, str) else None

    @property
    def clean(self) -> bool:
        return self.code == 0

@dataclass(frozen = True)
class NodeRestarted:
    node : str
    replica : int
    attempt : int
    delay : float

@dataclass(frozen = True)
class NodeGaveUp:
    '''
    A worker will not be restarted again — the flow cannot complete on its own
    from here, so this is what triggers the supervisor's control-abort.
    '''
    node : str
    replica : int
    attempts : int
    reason : Optional[Dict[str, object]] = None

@dataclass(frozen = True)
class FlowStalled:
    '''The flow can never finish: unschedulable pods, or a node that stopped progressing.'''
    detail : str

@dataclass
class EventLog:
    '''
    Collects lifecycle events for rendering. Deliberately a plain list rather than
    a callback interface: there is exactly one consumer (the CLI renderer), and
    ``dump_failed_logs``/``report_failures`` becoming one function is the point.
    '''
    events : list = field(default_factory = list)

    def emit(self, event : object) -> None:
        self.events.append(event)

    def failed_nodes(self) -> list:
        '''Node names that gave up, in first-seen order.'''
        seen, out = set(), []
        for event in self.events:
            if isinstance(event, NodeGaveUp) and event.node not in seen:
                seen.add(event.node)
                out.append(event.node)
        return out

    def restart_count(self, node : Optional[str] = None) -> int:
        return sum(1 for e in self.events
                if isinstance(e, NodeRestarted) and (node is None or e.node == node))

def render_event(event : object) -> Optional[str]:
    '''One human line per event, or None for events not worth printing.'''
    if isinstance(event, NodeRestarted):
        return (f'node {event.node} replica {event.replica} restarting '
                f'(attempt {event.attempt + 1}) after {event.delay:g}s')
    if isinstance(event, NodeExited) and not event.clean:
        reason = (event.reason or {}).get('code', '')
        detail = f' [{reason}]' if reason else ''
        return f'node {event.node} replica {event.replica} exited with code {event.code}{detail}'
    if isinstance(event, NodeGaveUp):
        reason = (event.reason or {}).get('message', '')
        detail = f': {reason}' if reason else ''
        return (f'node {event.node} replica {event.replica} gave up after '
                f'{event.attempts} attempt(s){detail}')
    if isinstance(event, FlowStalled):
        return f'flow stalled: {event.detail}'
    return None

#: Re-exported so callers that already import this module for the policies do not
#: need a second import to name the dispositions the policy branches on.
__all__ = [
    'ConsecutiveFailureBreaker', 'ProgressDeadline', 'SupervisionPolicy',
    'NodeStarted', 'NodeExited', 'NodeRestarted', 'NodeGaveUp', 'FlowStalled',
    'EventLog', 'render_event',
    'DEFAULT_BREAKER_THRESHOLD', 'DEFAULT_PROGRESS_TIMEOUT_SECONDS',
    'POISON', 'TRANSIENT', 'WORKER_FATAL',
]
