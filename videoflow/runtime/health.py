'''
A tiny stdlib HTTP server exposing Kubernetes health probes and Prometheus
metrics for a running worker, plus an ``InstrumentedMessenger`` that feeds it.

Endpoints (default port 8080):
  /readyz   200 once the node has started processing (see readiness note below), else 503
  /healthz  200 while the run loop is beating, 503 if it has stalled
  /metrics  Prometheus text exposition of per-node processing metrics

Kept dependency-free (no prometheus_client) so the base image stays lean; the
metrics text format is simple enough to emit by hand.

What ``/metrics`` can and cannot answer is decided here, so it is worth being
precise about it:

- Every observed latency (``proctime_seconds``, ``actual_proctime_seconds``) is
  exported as a Prometheus **histogram**: cumulative ``_bucket{le="..."}`` lines
  over ``LATENCY_BUCKETS_SECONDS`` next to the ``_count``/``_sum`` pair. Count and
  sum alone yield a mean, and two populations with the same mean can have tails
  ten times apart; a tail-latency objective (p95, p99) needs the buckets, and
  ``quantile_bounds`` turns them into an honest *bracket* — the bucket the
  quantile falls in — rather than an invented point estimate. Because the
  bucket bounds are one module-level constant, histograms from different
  workers add element-wise and a restarted worker's reset is an ordinary
  counter reset, so ``rate()`` and ``histogram_quantile()`` compose across the
  fleet.
- Throughput is three counters with a conservation law behind them:
  ``messages_offered_total`` (input groups of real work handed to the node),
  ``messages_processed_total`` (acked) and ``messages_dropped_total{reason}``
  (given up on by policy, never to be redelivered). ``offered - processed -
  dropped`` is the node's outstanding work, which is what a demand observer
  wants and what a lag gauge alone cannot separate from loss.

The renderer only ever *appends*: every line an older worker emitted is still
emitted first, unchanged and in the same order, so a scrape config or a pinned
test written against the count/sum-only output keeps working.
'''
from __future__ import absolute_import, division, print_function

import bisect
import itertools
import logging
import math
import re
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional, Sequence

from ..core.engine import Messenger
from ..core.errors import DEFAULT_DISPOSITION, POISON, TRANSIENT, ConfigError, classify
from ..core.policies import BEST_EFFORT, DeliveryPolicy

logger = logging.getLogger(__package__)

DEFAULT_HEALTH_PORT = 8080

# If the run loop hasn't beaten within this many seconds, liveness fails and
# Kubernetes restarts the pod (e.g. a wedged broker connection).
LIVENESS_STALL_SECONDS = 60

#: Upper bounds, in seconds, of the latency histogram buckets. Prometheus buckets
#: are cumulative: a bucket counts every observation ``<= le``. The set is tuned
#: for per-message processing time — dense from 5 ms to 1 s, where a video
#: pipeline's objectives live (a 30 fps budget is 33 ms), then sparse up to the
#: minute-scale stages of a batch flow. A ``+Inf`` bucket is always appended, so
#: the largest bound need not exceed every observation. Changing this set changes
#: the ``le`` label values that dashboards and recording rules key on.
LATENCY_BUCKETS_SECONDS : tuple[float, ...] = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0)

#: Why a node gave up on an input group: the values of the ``reason`` label on
#: ``videoflow_messages_dropped_total``. Stable, since alerts key on them.
DROP_REASON_POISON = 'poison'            # dead-lettered on its first failure: the data is bad
DROP_REASON_BEST_EFFORT = 'best_effort'  # the delivery mode discards a failed message instead of retrying

_INF_LABEL = '+Inf'
_BUCKET_LINE = re.compile(
    r'^videoflow_(?P<metric>[A-Za-z_:][A-Za-z0-9_:]*)_bucket\{(?P<labels>[^}]*)\} (?P<value>\d+)$')
_LE_LABEL = re.compile(r'(?:^|,)le="(?P<le>[^"]*)"')

@dataclass
class _MetricAggregate:
    '''
    The running aggregate behind one observed metric: how many observations have
    arrived, their running total, and how many fell in each histogram bucket.
    Rendered as the Prometheus histogram ``videoflow_<metric>_count`` /
    ``videoflow_<metric>_sum`` / ``videoflow_<metric>_bucket{le=...}``, hence
    these three fields and no others — this is a fixed-shape record, not a bag of
    metric keys.

    ``total`` starts as a float so a metric's ``_sum`` renders as ``0.0`` rather
    than ``0`` on the (unreachable in practice) zero-observation path, and so the
    accumulation is float arithmetic from the first observation on.

    ``buckets`` holds one *non-cumulative* count per bound plus the ``+Inf``
    overflow bucket (so its length is ``len(bounds) + 1``); the cumulative form
    Prometheus expects is computed at render time, which keeps ``observe`` a
    single increment.
    '''
    count : int = 0
    total : float = 0.0
    buckets : list[int] = field(default_factory = list)

def _le_label(bound : float) -> str:
    '''
    The ``le`` label value for a bucket bound: ``+Inf`` for the overflow bucket,
    else the shortest decimal that round-trips (``0.005``, ``1.0``) — what the
    reference Prometheus clients emit and what ``strconv.ParseFloat`` reads back
    exactly.
    '''
    return _INF_LABEL if math.isinf(bound) else repr(float(bound))

def quantile_bounds(cumulative : Sequence[int], q : float,
                    bounds : Sequence[float] = LATENCY_BUCKETS_SECONDS) -> tuple[float, float] | None:
    '''
    The bracket a nearest-rank quantile of a histogram lies in: the bucket that
    holds the ``ceil(q * count)``-th observation, as ``(lower, upper)`` with the
    true quantile in ``(lower, upper]``. ``upper`` is ``inf`` when the rank falls
    in the overflow bucket; ``lower`` is ``-inf`` for the first bucket.

    This is the histogram's declared error bound, and it is the most a bucketed
    export can truthfully say: an objective "p95 <= 50 ms" is met when
    ``upper <= 0.05``, violated when ``lower >= 0.05``, and *undecidable* when the
    threshold splits the bucket — which is a different answer from a guess.

    - Arguments:
        - cumulative: the cumulative bucket counts, one per bound plus the final \
            ``+Inf`` bucket, so ``cumulative[-1]`` is the observation count \
            (``HealthState.histogram`` and ``parse_histogram`` both produce this).
        - q: the quantile, in ``(0, 1]``.
        - bounds: the finite bucket bounds the counts were taken over.

    - Returns:
        - ``(lower, upper)``, or ``None`` when there are no observations — a \
            percentile of nothing is unavailable, not zero.

    - Raises:
        - ValueError: ``q`` outside ``(0, 1]``, or counts and bounds of \
            mismatched length.
    '''
    if not 0.0 < q <= 1.0:
        raise ValueError(f'q must be in (0, 1], got {q!r}; pass 0.95 for p95.')
    if len(cumulative) != len(bounds) + 1:
        raise ValueError(f'{len(cumulative)} cumulative counts for {len(bounds)} bounds; expected one '
                        'count per bound plus the +Inf bucket (see HealthState.histogram).')
    count = cumulative[-1]
    if count == 0:
        return None
    # Nearest rank; the epsilon keeps a product like 0.95 * 20 that lands a hair
    # above 19.0 in float from rounding up to the 20th observation.
    rank = max(1, math.ceil(q * count - 1e-9))
    edges = tuple(bounds) + (math.inf,)
    for i, seen in enumerate(cumulative):
        if seen >= rank:
            return (edges[i - 1] if i > 0 else -math.inf, edges[i])
    raise ValueError('cumulative counts are not cumulative: the last bucket must hold every observation.')

def parse_histogram(text : str, metric : str) -> tuple[tuple[float, ...], tuple[int, ...]]:
    '''
    Read one histogram family back out of an exposition text, as the scrape side
    sees it: the finite bucket bounds (ascending) and the cumulative counts, one
    per bound plus the ``+Inf`` bucket last — the shape ``quantile_bounds`` takes.
    Both empty when the family has no ``_bucket`` lines: a count/sum-only
    exposition has no percentile to offer, and the empty answer is how a caller
    learns that instead of dividing a sum by a count.

    - Arguments:
        - text: a ``/metrics`` body (one worker, so one node).
        - metric: the family name without the ``videoflow_`` prefix or the \
            ``_bucket`` suffix, e.g. ``proctime_seconds``.
    '''
    found : list[tuple[float, int]] = []
    for line in text.splitlines():
        m = _BUCKET_LINE.match(line)
        if m is None or m.group('metric') != metric:
            continue
        le = _LE_LABEL.search(m.group('labels'))
        if le is None:
            continue
        raw = le.group('le')
        found.append((math.inf if raw == _INF_LABEL else float(raw), int(m.group('value'))))
    found.sort()
    bounds = tuple(b for b, _ in found if not math.isinf(b))
    return bounds, tuple(c for _, c in found)

class HealthState:
    '''Thread-safe holder for readiness/liveness/metrics, shared between the run loop (via the messenger) and the HTTP handler.'''
    def __init__(self, node_name : str, buckets : Sequence[float] = LATENCY_BUCKETS_SECONDS) -> None:
        '''
        - Arguments:
            - node_name: the ``node`` label on every exported sample.
            - buckets: finite, strictly increasing histogram bounds in seconds. \
                The default suits per-message processing time; override only for \
                a stage whose latencies live outside 5 ms..60 s, knowing it \
                changes the ``le`` values a dashboard keys on.

        - Raises:
            - ConfigError: bounds that are not finite and strictly increasing.
        '''
        self._node_name = node_name
        self._bounds = tuple(float(b) for b in buckets)
        if not self._bounds or any(not math.isfinite(b) for b in self._bounds) or \
                any(a >= b for a, b in zip(self._bounds, self._bounds[1:])):
            raise ConfigError(
                f'Histogram buckets must be finite and strictly increasing, got {list(buckets)!r}.',
                remedy = 'Pass ascending finite upper bounds in seconds (the +Inf bucket is added for you), '
                         'or leave the default LATENCY_BUCKETS_SECONDS.')
        self._lock = threading.Lock()
        self._ready = False
        self._last_beat = time.time()
        # metric name -> its running aggregate. Arbitrary keys, fixed-shape values.
        self._metrics : dict[str, _MetricAggregate] = {}
        # counter name -> int (rendered as videoflow_<name>_total)
        self._counters : dict[str, int] = {}
        # (code, disposition) -> int, rendered as videoflow_errors_total with those
        # labels. Kept apart from _counters because "how many failed" is nearly
        # useless on its own — *what* is failing is the question an alert asks, and
        # an undimensioned counter cannot answer it.
        self._errors : dict[tuple, int] = {}
        # Input groups of real work handed to the node, and the groups it gave up
        # on, by reason. Kept apart from _counters so they render after every
        # pre-existing line: an old scrape is a byte-identical prefix of a new one.
        self._offered = 0
        self._drops : dict[str, int] = {}

    def mark_ready(self) -> None:
        with self._lock:
            self._ready = True

    def beat(self) -> None:
        with self._lock:
            self._last_beat = time.time()

    def observe(self, metric : str, value : float | None) -> None:
        if value is None:
            return
        with self._lock:
            m = self._metrics.get(metric)
            if m is None:
                m = _MetricAggregate(buckets = [0] * (len(self._bounds) + 1))
                self._metrics[metric] = m
            m.count += 1
            m.total += value
            # The first bound >= value is its bucket (Prometheus buckets are
            # ``<= le``); past the last bound is the +Inf overflow bucket.
            m.buckets[bisect.bisect_left(self._bounds, value)] += 1

    def incr(self, counter : str, amount : int = 1) -> None:
        with self._lock:
            self._counters[counter] = self._counters.get(counter, 0) + amount

    def record_error(self, code : str, disposition : str) -> None:
        '''Counts one failure under its stable code, so an alert can say *what* is failing.'''
        with self._lock:
            key = (code or 'VF_UNKNOWN', disposition or '')
            self._errors[key] = self._errors.get(key, 0) + 1

    def record_offered(self) -> None:
        '''One input group of real work handed to the node (end-of-stream and abort markers are not work).'''
        with self._lock:
            self._offered += 1

    def record_drop(self, reason : str, count : int = 1) -> None:
        '''
        The node gave up on an input group: it will produce no output for it and
        the broker will not redeliver it. ``reason`` is one of the
        ``DROP_REASON_*`` values, or — for a drop decided below the ``Messenger``
        seam, such as a join group evicted on timeout — a short stable word of
        the messenger's own.
        '''
        with self._lock:
            self._drops[reason] = self._drops.get(reason, 0) + count

    def histogram(self, metric : str) -> tuple[int, ...] | None:
        '''
        The cumulative bucket counts of one observed metric — one per bound in
        ``LATENCY_BUCKETS_SECONDS`` (or the override) plus the ``+Inf`` bucket,
        so the last entry equals the observation count. ``None`` before the first
        observation. Feed it to ``quantile_bounds``.
        '''
        with self._lock:
            m = self._metrics.get(metric)
            if m is None:
                return None
            return tuple(itertools.accumulate(m.buckets))

    def is_ready(self) -> bool:
        with self._lock:
            return self._ready

    def is_live(self) -> bool:
        with self._lock:
            return (time.time() - self._last_beat) < LIVENESS_STALL_SECONDS

    def render_metrics(self) -> str:
        with self._lock:
            lines = []
            safe_node = self._node_name.replace('"', '')
            labels = f'{{node="{safe_node}"}}'
            for metric, m in self._metrics.items():
                lines.append(f'videoflow_{metric}_count{labels} {m.count}')
                lines.append(f'videoflow_{metric}_sum{labels} {m.total}')
            for counter, value in self._counters.items():
                lines.append(f'videoflow_{counter}_total{labels} {value}')
            for (code, disposition), value in self._errors.items():
                safe_code = code.replace('"', '')
                safe_disposition = disposition.replace('"', '')
                lines.append(
                    f'videoflow_errors_total{{node="{safe_node}",code="{safe_code}",'
                    f'disposition="{safe_disposition}"}} {value}')
            # Everything below is appended after the pre-existing families, so a
            # scrape of an older worker is a byte-identical prefix of this one.
            for metric, m in self._metrics.items():
                cumulative = 0
                for bound, n in zip(self._bounds + (math.inf,), m.buckets):
                    cumulative += n
                    lines.append(
                        f'videoflow_{metric}_bucket{{node="{safe_node}",le="{_le_label(bound)}"}} {cumulative}')
            if self._offered:
                lines.append(f'videoflow_messages_offered_total{labels} {self._offered}')
            for reason, value in self._drops.items():
                safe_reason = reason.replace('"', '')
                lines.append(
                    f'videoflow_messages_dropped_total{{node="{safe_node}",reason="{safe_reason}"}} {value}')
            return '\n'.join(lines) + '\n'

def _make_handler(state : HealthState) -> type:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args : Any) -> None:
            pass  # silence per-request stderr logging

        def _respond(self, code : int, body : str, content_type : str = 'text/plain') -> None:
            payload = body.encode('utf-8')
            self.send_response(code)
            self.send_header('Content-Type', content_type)
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:
            if self.path == '/readyz':
                ok = state.is_ready()
                self._respond(200 if ok else 503, 'ready' if ok else 'not-ready')
            elif self.path == '/healthz':
                ok = state.is_live()
                self._respond(200 if ok else 503, 'ok' if ok else 'stalled')
            elif self.path == '/metrics':
                self._respond(200, state.render_metrics())
            else:
                self._respond(404, 'not found')

    return Handler

class HealthServer:
    def __init__(self, state : HealthState, port : int = DEFAULT_HEALTH_PORT) -> None:
        self._state = state
        self._httpd = ThreadingHTTPServer(('0.0.0.0', port), _make_handler(state))
        self._thread = threading.Thread(target = self._httpd.serve_forever, daemon = True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._httpd.shutdown()

def _is_work(group : dict) -> bool:
    '''A received input group is work unless an entry is an end-of-stream or abort marker (the task stops on those).'''
    return not any(entry.get('is_stop_signal') or entry.get('is_abort') for entry in group.values())

class InstrumentedMessenger(Messenger):
    '''
    Wraps a real ``Messenger`` and updates a ``HealthState`` as messages flow,
    without any change to the ``Task`` classes. Readiness note: the node is marked
    ready on its first messenger activity (first send or receive), which happens
    only after ``node.open()`` has returned inside ``NodeTask.run()`` — so a slow
    model-loading ``open()`` correctly keeps the pod un-ready until it finishes.

    Drop accounting happens here too, from the delivery policy's verdict on a
    failed input: a poison message is dead-lettered on its first failure in every
    mode, and best-effort delivery discards a transient failure instead of
    retrying it, so both count as dropped the moment ``fail_inputs`` sees them.
    What this seam cannot see is a message the *messenger* gives up on later —
    an at-least-once retry budget running out, a join group evicted on timeout, a
    collect buffer overflowing — because those verdicts are taken below
    ``Messenger`` with the delivery count and the group state in hand; they reach
    the counter only when the messenger reports them through
    ``HealthState.record_drop``.
    '''
    def __init__(self, inner : Messenger, state : HealthState,
                delivery_policy : DeliveryPolicy | None = None) -> None:
        '''
        - Arguments:
            - inner: the real messenger every call is forwarded to.
            - state: the health/metrics holder to update.
            - delivery_policy: the node's effective ``DeliveryPolicy`` — the one \
                the inner messenger was built with — so drops can be counted from \
                its verdicts. ``None`` counts only the mode-independent drop \
                (poison).
        '''
        self._inner = inner
        self._state = state
        self._delivery_policy = delivery_policy

    def publish_message(self, message : Any, metadata : dict | None = None) -> None:
        self._state.mark_ready()
        self._state.beat()
        if metadata:
            self._state.observe('proctime_seconds', metadata.get('proctime'))
            self._state.observe('actual_proctime_seconds', metadata.get('actual_proctime'))
        self._state.incr('messages_published')
        try:
            return self._inner.publish_message(message, metadata)
        finally:
            self._collect_drops()

    def _collect_drops(self) -> None:
        '''Fold in the drops the messenger decided below this seam since the last call.'''
        for reason, count in self._inner.take_drops().items():
            self._state.record_drop(reason, count)

    def publish_stop_signal(self) -> None:
        return self._inner.publish_stop_signal()

    def publish_abort(self, error : Any) -> None:
        return self._inner.publish_abort(error)

    def pending_count(self) -> int:
        return self._inner.pending_count()

    def pending_observation(self) -> Any:
        return self._inner.pending_observation()

    def quiesce(self) -> None:
        self._inner.quiesce()

    def check_for_termination(self) -> bool:
        self._state.beat()
        return self._inner.check_for_termination()

    def receive_message(self) -> dict:
        self._state.mark_ready()
        self._state.beat()
        self._state.incr('messages_received')
        group = self._inner.receive_message()
        self._collect_drops()
        if _is_work(group):
            self._state.record_offered()
        return group

    def ack_inputs(self) -> None:
        self._state.incr('messages_processed')
        return self._inner.ack_inputs()

    def fail_inputs(self, exc : BaseException) -> None:
        self._state.incr('messages_failed')
        # Counted here rather than in the messenger so a sampled (and therefore
        # undelivered) dead letter is still visible in the metric: the DLQ is
        # bounded on purpose, the counter is not.
        self._state.record_error(getattr(exc, 'code', f'VF_{type(exc).__name__.upper()}'),
                                getattr(exc, 'disposition', ''))
        reason = self._drop_reason(exc)
        if reason is not None:
            self._state.record_drop(reason)
        try:
            return self._inner.fail_inputs(exc)
        finally:
            self._collect_drops()

    def _drop_reason(self, exc : BaseException) -> str | None:
        '''
        Whether the delivery policy's verdict on ``exc`` discards the input, as
        far as this seam can tell. Mirrors ``DeliveryPolicy.action_for`` for the
        verdicts that do not depend on the delivery count; a nak (worker-fatal,
        or transient under at-least-once) hands the message back, and whether a
        later attempt exhausts the budget is the messenger's to report.
        '''
        policy = self._delivery_policy
        default = policy.on_error if policy is not None and policy.on_error else DEFAULT_DISPOSITION
        disposition = classify(exc, default)
        if disposition == POISON:
            return DROP_REASON_POISON
        if disposition == TRANSIENT and policy is not None and policy.delivery == BEST_EFFORT:
            return DROP_REASON_BEST_EFFORT
        return None

    def set_output_partition_key(self, value : Any) -> None:
        return self._inner.set_output_partition_key(value)

    def set_output_event_timestamp(self, value : float) -> None:
        return self._inner.set_output_event_timestamp(value)

    def last_input_key(self) -> Optional[str]:
        return self._inner.last_input_key()

    def last_input_info(self) -> Optional[dict]:
        return self._inner.last_input_info()

    def close(self) -> None:
        return self._inner.close()
