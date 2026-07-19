'''
A tiny stdlib HTTP server exposing Kubernetes health probes and Prometheus
metrics for a running worker, plus an ``InstrumentedMessenger`` that feeds it.

Endpoints (default port 8080):
  /readyz   200 once the node has started processing (see readiness note below), else 503
  /healthz  200 while the run loop is beating, 503 if it has stalled
  /metrics  Prometheus text exposition of per-node processing metrics

Kept dependency-free (no prometheus_client) so the base image stays lean; the
metrics text format is simple enough to emit by hand.
'''
from __future__ import absolute_import, division, print_function

import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Optional

from ..core.engine import Messenger

logger = logging.getLogger(__package__)

DEFAULT_HEALTH_PORT = 8080

# If the run loop hasn't beaten within this many seconds, liveness fails and
# Kubernetes restarts the pod (e.g. a wedged broker connection).
LIVENESS_STALL_SECONDS = 60

class HealthState:
    '''Thread-safe holder for readiness/liveness/metrics, shared between the run loop (via the messenger) and the HTTP handler.'''
    def __init__(self, node_name : str) -> None:
        self._node_name = node_name
        self._lock = threading.Lock()
        self._ready = False
        self._last_beat = time.time()
        # metric name -> {'count': int, 'sum': float}
        self._metrics: dict = {}
        # counter name -> int (rendered as videoflow_<name>_total)
        self._counters: dict = {}

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
            m = self._metrics.setdefault(metric, {'count': 0, 'sum': 0.0})
            m['count'] += 1
            m['sum'] += value

    def incr(self, counter : str, amount : int = 1) -> None:
        with self._lock:
            self._counters[counter] = self._counters.get(counter, 0) + amount

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
                lines.append(f'videoflow_{metric}_count{labels} {m["count"]}')
                lines.append(f'videoflow_{metric}_sum{labels} {m["sum"]}')
            for counter, value in self._counters.items():
                lines.append(f'videoflow_{counter}_total{labels} {value}')
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

class InstrumentedMessenger(Messenger):
    '''
    Wraps a real ``Messenger`` and updates a ``HealthState`` as messages flow,
    without any change to the ``Task`` classes. Readiness note: the node is marked
    ready on its first messenger activity (first send or receive), which happens
    only after ``node.open()`` has returned inside ``NodeTask.run()`` — so a slow
    model-loading ``open()`` correctly keeps the pod un-ready until it finishes.
    '''
    def __init__(self, inner : Messenger, state : HealthState) -> None:
        self._inner = inner
        self._state = state

    def publish_message(self, message : Any, metadata : dict | None = None) -> None:
        self._state.mark_ready()
        self._state.beat()
        if metadata:
            self._state.observe('proctime_seconds', metadata.get('proctime'))
            self._state.observe('actual_proctime_seconds', metadata.get('actual_proctime'))
        self._state.incr('messages_published')
        return self._inner.publish_message(message, metadata)

    def publish_stop_signal(self) -> None:
        return self._inner.publish_stop_signal()

    def check_for_termination(self) -> bool:
        self._state.beat()
        return self._inner.check_for_termination()

    def receive_message(self) -> dict:
        self._state.mark_ready()
        self._state.beat()
        self._state.incr('messages_received')
        return self._inner.receive_message()

    def ack_inputs(self) -> None:
        self._state.incr('messages_processed')
        return self._inner.ack_inputs()

    def fail_inputs(self, exc : BaseException) -> None:
        self._state.incr('messages_failed')
        return self._inner.fail_inputs(exc)

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
