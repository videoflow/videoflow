'''
The importable glue nodes for the toy-recovery solution.

A real module so distributed workers can reconstruct each node from its
``toy_recovery_nodes.<Class>`` path; every constructor argument is stored
verbatim as ``self._<name>`` and is JSON-serializable, which is what makes the
round trip through ``get_params()`` work.

The failures here are *deliberate and typed*. That is the whole subject of this
solution: videoflow treats a bad message and a sick worker differently, and the
only way to show that end to end is to produce one of each on purpose.
'''
from __future__ import annotations

import json
import os
import time
from typing import Any, Iterator

from videoflow.core.errors import DeviceError, SchemaError
from videoflow.core.node import ConsumerNode, ProcessorNode, ProducerNode


def iter_events(count: int) -> Iterator[int]:
    '''The stream, as both the producer and prepare.py walk it.'''
    return iter(range(count))


class EventProducer(ProducerNode):
    '''
    Emits ``0..count-1``, paced by ``rate_fps`` (<= 0 = as fast as possible).

    - Arguments:
        - count: how many events to emit.
        - rate_fps: pacing, in messages per second.
    '''
    def __init__(self, count: int = 40, rate_fps: float = 200.0, **kwargs: Any) -> None:
        self._count = count
        self._rate_fps = rate_fps
        self._next = 0
        super().__init__(**kwargs)

    def next(self) -> int:                      # type: ignore[override]
        if self._next >= self._count:
            raise StopIteration()
        if self._rate_fps > 0 and self._next:
            time.sleep(1.0 / self._rate_fps)
        value = self._next
        self._next += 1
        return value


class FragileProcessor(ProcessorNode):
    '''
    Fails in the two ways videoflow distinguishes, so the difference is visible
    in the artifacts rather than only in the docs.

    - ``poison_values`` raise ``SchemaError`` — a **bad message**. It is
      dead-lettered on its first failure (retrying something that failed on its
      own content cannot help), and the rest of the stream is unaffected.
    - ``crash_at`` raises ``DeviceError`` once — a **sick worker**. The message
      is handed back rather than blamed, the worker exits, the supervisor
      restarts it, and the un-acked message is redelivered to the fresh one. It
      ends up delivered, not dead-lettered.

    The crash is once-per-*run*, not once-per-process: a marker file in the work
    dir survives the restart, so the restarted worker succeeds where its
    predecessor died. Without that it would crash identically forever and the
    run would only demonstrate giving up.

    - Arguments:
        - poison_values: values to reject as unparseable.
        - crash_at: value at which the worker dies once (``None`` disables).
        - marker_path: file recording that the crash already happened.
    '''
    def __init__(self, poison_values: list | None = None, crash_at: int | None = None,
                 marker_path: str = '', **kwargs: Any) -> None:
        self._poison_values = list(poison_values or [])
        self._crash_at = crash_at
        self._marker_path = marker_path
        super().__init__(**kwargs)

    def _already_crashed(self) -> bool:
        return bool(self._marker_path) and os.path.exists(self._marker_path)

    def process(self, item: int) -> int:        # type: ignore[override]
        if item in self._poison_values:
            raise SchemaError(
                f'event {item} is malformed and will never parse',
                remedy = 'Fix the producer, then `videoflow dlq replay` these events.')
        if self._crash_at is not None and item == self._crash_at and not self._already_crashed():
            if self._marker_path:
                with open(self._marker_path, 'w') as f:
                    f.write(str(item))
            raise DeviceError(
                f'the accelerator fell over while handling event {item}',
                remedy = 'This worker is unhealthy; the message goes back to a healthy one.')
        return item


class RecoveryLedger(ConsumerNode):
    '''
    Records everything that arrived and, in ``close()``, writes the self-checking
    report the test asserts on.

    The claim ``matches_expected`` makes is precise and is the point of the whole
    solution: **every event either arrived exactly once or was dead-lettered —
    never both, never neither.** The events that failed on their own content are
    absent; the event whose worker died is present, because the broker gave it
    back instead of losing it.

    - Arguments:
        - ledger_path: append-only record of arrivals.
        - report_path: the success artifact.
        - expected_path: what prepare.py baked before the run.
    '''
    def __init__(self, ledger_path: str, report_path: str, expected_path: str,
                 **kwargs: Any) -> None:
        self._ledger_path = ledger_path
        self._report_path = report_path
        self._expected_path = expected_path
        self._seen: list = []
        super().__init__(**kwargs)

    def open(self) -> None:
        # A restarted *sink* would otherwise append to a stale ledger; truncating
        # in open() keeps the artifact a record of this run.
        with open(self._ledger_path, 'w'):
            pass

    def consume(self, item: int) -> None:       # type: ignore[override]
        self._seen.append(item)
        with open(self._ledger_path, 'a') as f:
            f.write(json.dumps({'value': item}) + '\n')

    def close(self) -> None:
        with open(self._expected_path) as f:
            expected = json.load(f)
        delivered = sorted(set(self._seen))
        dead_lettered = sorted(set(expected['produced']) - set(delivered))
        report = {
            'delivered': delivered,
            'delivered_count': len(delivered),
            # Duplicates are allowed by at-least-once delivery and are not a
            # failure — but they are worth showing, since a redelivered message
            # is exactly what recovery produces.
            'duplicate_deliveries': len(self._seen) - len(delivered),
            'dead_lettered': dead_lettered,
            'dlq_count': len(dead_lettered),
            'expected_delivered': expected['delivered'],
            'expected_dead_lettered': expected['dead_lettered'],
            'matches_expected': (delivered == expected['delivered']
                                 and dead_lettered == expected['dead_lettered']),
        }
        with open(self._report_path, 'w') as f:
            json.dump(report, f, indent = 2)
