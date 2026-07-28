'''
Nodes that fail on demand, for driving the error paths end to end.

Failure is injected through **constructor params**, not environment variables:
params already round-trip ``get_params()`` → ``VF_NODE_PARAMS_JSON`` → worker,
which is the only channel that reaches a subprocess or a pod. A node here behaves
identically in-process and inside a worker, which is what makes the same fixture
usable by both the unit and the integration suites.

Mirrors ``tests/support_nodes.py``; kept separate so the healthy fixtures stay
readable.
'''
from __future__ import absolute_import, division, print_function

import os
import time
from typing import Any, List, Optional

from videoflow.core.errors import (
    DeviceError,
    PoisonMessage,
    SchemaError,
    TransientFailure,
)
from videoflow.core.node import ConsumerNode, ProcessorNode, ProducerNode

#: How ``error_kind`` maps onto what gets raised. ``untyped`` is the important
#: one: a node that knows nothing about the taxonomy must still behave sanely.
ERROR_KINDS = {
    'poison': lambda m: SchemaError(f'unusable input {m!r}'),
    'transient': lambda m: TransientFailure(f'upstream hiccup on {m!r}'),
    'worker_fatal': lambda m: DeviceError(f'device gone while handling {m!r}'),
    'untyped': lambda m: ValueError(f'plain failure on {m!r}'),
}

class DyingProducer(ProducerNode):
    '''
    Raises part-way through its stream — the exact shape of the bug ABORT exists
    for. A producer has no input to fail and nothing to retry, so the exception
    ends it *before* ``publish_stop_signal()``; without an abort marker its
    children wait forever on an end-of-stream that will never be published.

    - Arguments:
        - count: how many values it would have produced.
        - die_at: the value at which it raises instead.
    '''
    def __init__(self, count : int = 20, die_at : int = 3, **kwargs : Any) -> None:
        self._count = count
        self._die_at = die_at
        self._n = 0
        super(DyingProducer, self).__init__(**kwargs)

    def next(self) -> Any:                     # type: ignore[override]
        value = self._n
        self._n += 1
        if value == self._die_at:
            raise DeviceError(f'the camera fell over at {value}',
                            remedy = 'Check the capture device.')
        if value >= self._count:
            raise StopIteration()
        time.sleep(0.01)
        return value

class FlakyProcessor(ProcessorNode):
    '''
    Fails on selected values, optionally recovering after N failures.

    - Arguments:
        - fail_on_values: input values that trigger a failure. Empty ⇒ every input.
        - fail_times: stop failing after this many failures (0 ⇒ never recover).
        - error_kind: one of ``ERROR_KINDS``.
        - marker_path: makes ``fail_times`` count across **processes**. Without \
            it the tally lives in the worker's memory, so a restarted worker \
            starts over and fails identically forever — which models a permanent \
            fault, not a recoverable one. Any test about *recovery* needs the \
            marker, because recovery means the second attempt differs from the \
            first.
    '''
    def __init__(self, fail_on_values : Optional[List[Any]] = None, fail_times : int = 0,
                error_kind : str = 'transient', marker_path : str = '',
                **kwargs : Any) -> None:
        self._fail_on_values = list(fail_on_values) if fail_on_values else []
        self._fail_times = fail_times
        self._error_kind = error_kind
        self._marker_path = marker_path
        self._failures_so_far = 0
        super(FlakyProcessor, self).__init__(**kwargs)

    def _tally(self) -> int:
        if not self._marker_path:
            return self._failures_so_far
        try:
            with open(self._marker_path) as f:
                return int(f.read() or 0)
        except OSError:
            return 0

    def _record(self) -> None:
        self._failures_so_far += 1
        if self._marker_path:
            # Read *before* opening for write: open(..., 'w') truncates
            # immediately, so reading inside the with-block always sees an empty
            # file and the tally silently resets to 1 on every failure.
            nxt = self._tally() + 1
            with open(self._marker_path, 'w') as f:
                f.write(str(nxt))

    def process(self, item : Any) -> Any:      # type: ignore[override]
        targeted = (not self._fail_on_values) or item in self._fail_on_values
        exhausted = self._fail_times and self._tally() >= self._fail_times
        if targeted and not exhausted:
            self._record()
            raise ERROR_KINDS[self._error_kind](item)
        return item

class WedgedProcessor(ProcessorNode):
    '''
    Blocks forever after ``block_after`` messages — the progress-deadline bait.
    Distinct from ``SlowProcessor`` on purpose: one is stuck and one is merely
    slow, and no watchdog that cannot tell them apart is worth having.
    '''
    def __init__(self, block_after : int = 1, **kwargs : Any) -> None:
        self._block_after = block_after
        self._seen = 0
        super(WedgedProcessor, self).__init__(**kwargs)

    def process(self, item : Any) -> Any:      # type: ignore[override]
        self._seen += 1
        if self._seen > self._block_after:
            while True:
                time.sleep(3600)
        return item

class SlowProcessor(ProcessorNode):
    '''Slow but healthy — the false-positive control for every watchdog.'''
    def __init__(self, delay_s : float = 0.5, **kwargs : Any) -> None:
        self._delay_s = delay_s
        super(SlowProcessor, self).__init__(**kwargs)

    def process(self, item : Any) -> Any:      # type: ignore[override]
        time.sleep(self._delay_s)
        return item

class CrashingProcessor(ProcessorNode):
    '''
    Dies on a chosen value. With ``hard = True`` it calls ``os._exit``, which no
    ``except`` can catch and no ABORT marker can precede — the case that exists to
    prove the supervisor's control-abort covers what the in-band marker cannot.
    '''
    def __init__(self, crash_on_value : Any = None, hard : bool = False,
                **kwargs : Any) -> None:
        self._crash_on_value = crash_on_value
        self._hard = hard
        super(CrashingProcessor, self).__init__(**kwargs)

    def process(self, item : Any) -> Any:      # type: ignore[override]
        if item == self._crash_on_value:
            if self._hard:
                os._exit(1)
            raise DeviceError(f'unrecoverable while handling {item!r}')
        return item

class CountingSink(ConsumerNode):
    '''
    Appends everything it consumes to a file, one value per line — the ledger the
    conservation law is checked against (every produced message is either here
    exactly once or in the DLQ exactly once).
    '''
    def __init__(self, path : str, **kwargs : Any) -> None:
        self._path = path
        super(CountingSink, self).__init__(**kwargs)

    def consume(self, item : Any) -> None:     # type: ignore[override]
        with open(self._path, 'a') as f:
            f.write(f'{item}\n')

class FailingSink(ConsumerNode):
    '''A sink that fails on selected values — the consumer-side failure path.'''
    def __init__(self, fail_on_values : Optional[List[Any]] = None,
                error_kind : str = 'transient', **kwargs : Any) -> None:
        self._fail_on_values = list(fail_on_values) if fail_on_values else []
        self._error_kind = error_kind
        super(FailingSink, self).__init__(**kwargs)

    def consume(self, item : Any) -> None:     # type: ignore[override]
        if (not self._fail_on_values) or item in self._fail_on_values:
            raise ERROR_KINDS[self._error_kind](item)

class FailingOpenProcessor(ProcessorNode):
    '''
    Fails in ``open()``. The failure site matters: ``open()`` runs before the run
    loop, so there is no input to fail and nothing to retry — the worker can only
    die, and its children can only be told.
    '''
    def __init__(self, **kwargs : Any) -> None:
        super(FailingOpenProcessor, self).__init__(**kwargs)

    def open(self) -> None:
        raise DeviceError('could not acquire the device',
                        remedy = 'Check that the node has a GPU grant.')

    def process(self, item : Any) -> Any:      # type: ignore[override]
        return item

class PoisonProducer(ProcessorNode):
    '''
    Emits a value the wire cannot encode, at a chosen index — the transport-level
    poison path (``DELIV-9``), which never reaches a node's ``process()`` at all.
    '''
    def __init__(self, bad_at : int = 1, **kwargs : Any) -> None:
        self._bad_at = bad_at
        self._seen = 0
        super(PoisonProducer, self).__init__(**kwargs)

    def process(self, item : Any) -> Any:      # type: ignore[override]
        self._seen += 1
        if self._seen == self._bad_at:
            return PoisonMessage      # a class object: not encodable for the wire
        return item
