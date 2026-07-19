'''
Instrumented toy nodes used by the white-paper experiments (``run_experiments.py``).

These nodes exist to *measure* the framework rather than to do useful work:

- ``StampedProducer`` emits small dict payloads carrying a produce-time wall-clock
  stamp (``t0``) so the sink can compute end-to-end latency per message. It also
  stamps ``event_ts`` via the runtime context so time-aligned joins can group on it.
- ``WorkProcessor`` simulates a stage with a fixed service time (``work_ms``) —
  a stand-in for model inference — while passing the stamped payload through
  unchanged.
- ``PairJoiner`` is a two-parent join stage that records which parents were
  present (time-mode joins with a quorum may deliver ``None`` for missing ones).
- ``RecordingConsumer`` appends one JSON line per consumed item with the consume
  wall-clock stamp; the driver parses these files to compute latency/throughput.

They live in their own module (not the driver script) so worker subprocesses can
import them by their fully-qualified path: ``LocalProcessEngine`` re-exports the
driver's ``sys.path`` additions as ``PYTHONPATH``, which makes ``expnodes.<Class>``
resolvable inside each worker. All constructor arguments are JSON-serializable and
stored verbatim as ``self._<name>`` so ``get_params()`` round-trips them.
'''
from __future__ import absolute_import, division, print_function

import json
import time

from videoflow.core.node import ConsumerNode, ProcessorNode, ProducerNode


class StampedProducer(ProducerNode):
    '''
    Produces ``n`` dict payloads ``{'i': i, 't0': <produce wall-clock>}``.

    - Arguments:
        - n: how many messages to produce before raising ``StopIteration``.
        - fps: messages per second. Values <= 0 mean "as fast as possible".
        - payload_bytes: optional padding added under key ``'pad'`` to grow the \
            payload to a target size.
        - drop_every: if > 1, indices where ``i % drop_every == 0`` are *skipped* \
            while time still advances — simulating a camera that drops frames. \
            Used by the time-join quorum experiment.
    '''
    def __init__(self, n : int = 100, fps : float = -1, payload_bytes : int = 0,
                drop_every : int = 0, **kwargs) -> None:
        self._n = n
        self._fps = fps
        self._payload_bytes = payload_bytes
        self._drop_every = drop_every
        self._i = 0
        super(StampedProducer, self).__init__(**kwargs)

    def next(self, ctx = None) -> dict:
        while True:
            if self._i >= self._n:
                raise StopIteration()
            if self._fps and self._fps > 0:
                time.sleep(1.0 / self._fps)
            i = self._i
            self._i += 1
            if self._drop_every and self._drop_every > 1 and i % self._drop_every == 0:
                continue                     # dropped frame: time advanced, no message
            item = {'i': i, 't0': time.time()}
            if self._payload_bytes:
                item['pad'] = 'x' * self._payload_bytes
            if ctx is not None:
                ctx.set_event_timestamp(item['t0'])
            return item


class WorkProcessor(ProcessorNode):
    '''
    Identity pass-through with a fixed service time of ``work_ms`` milliseconds —
    a controllable stand-in for a real inference/transform stage.
    '''
    def __init__(self, work_ms : float = 0, **kwargs) -> None:
        self._work_ms = work_ms
        super(WorkProcessor, self).__init__(**kwargs)

    def process(self, item) -> dict:
        if self._work_ms > 0:
            time.sleep(self._work_ms / 1000.0)
        return item


class PairJoiner(ProcessorNode):
    '''
    Two-parent join stage with a fixed service time. Emits a merged record that
    keeps the earliest stamp of the inputs that were present (either side may be
    ``None`` under a time-mode quorum join) plus which sides arrived.
    '''
    def __init__(self, work_ms : float = 0, **kwargs) -> None:
        self._work_ms = work_ms
        super(PairJoiner, self).__init__(**kwargs)

    def process(self, a, b) -> dict:
        if self._work_ms > 0:
            time.sleep(self._work_ms / 1000.0)
        present = [x for x in (a, b) if isinstance(x, dict)]
        out = {
            'i': present[0]['i'] if present else None,
            't0': min(x['t0'] for x in present) if present else None,
            'has_a': isinstance(a, dict),
            'has_b': isinstance(b, dict),
        }
        if isinstance(a, dict) and isinstance(b, dict):
            out['b_i'] = b['i']
        return out


class RecordingConsumer(ConsumerNode):
    '''
    Appends one JSON line per consumed item to ``filepath``:
    ``{'t1': <consume wall-clock>, ...selected fields of the item...}``.
    The driver computes latency (``t1 - t0``), delivered counts, and sink-side
    makespan from these lines. Line-buffered so a stopped run still leaves
    complete records.
    '''
    _KEEP = ('i', 't0', 'b_i', 'has_a', 'has_b')

    def __init__(self, filepath : str, **kwargs) -> None:
        self._filepath = filepath
        self._f = None
        super(RecordingConsumer, self).__init__(**kwargs)

    def open(self) -> None:
        self._f = open(self._filepath, 'a', buffering = 1)

    def consume(self, item) -> None:
        rec = {'t1': time.time()}
        if isinstance(item, dict):
            for k in self._KEEP:
                if k in item:
                    rec[k] = item[k]
        self._f.write(json.dumps(rec) + '\n')

    def close(self) -> None:
        if self._f is not None:
            self._f.close()
            self._f = None
