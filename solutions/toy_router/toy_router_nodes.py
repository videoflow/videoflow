'''
Glue nodes for the toy-router solution.

These live in their own importable module (not in the graph module) because
distributed workers reconstruct each node from its class path — recorded as
``toy_router_nodes.<Class>`` — and importing the graph module in a worker would
re-run graph-level code. Every constructor argument is stored as
``self._<name>`` so the node round-trips through ``get_params()`` unchanged.

``iter_events`` is the deterministic event stream shared by the producer and
``prepare.py``: both sides walk the same PRNG, which is what lets the prep hook
write the expected per-sensor totals *before* the flow runs.
'''
from __future__ import annotations

import asyncio
import json
import random
import time
from collections.abc import Iterator
from typing import Any, TextIO

from videoflow.core.context import RuntimeContext
from videoflow.core.node import ConsumerNode, ProcessorNode, ProducerNode


def iter_events(seed: int, count: int, sensors: int) -> Iterator[dict]:
    '''
    Yields ``count`` events of the form ``{'seq', 'sensor', 'value'}``,
    deterministically from ``seed`` — the single source of truth for both the
    producer and the ground truth ``prepare.py`` bakes.
    '''
    rng = random.Random(seed)
    for seq in range(count):
        yield {
            'seq': seq,
            'sensor': f'sensor-{rng.randrange(sensors)}',
            'value': round(rng.uniform(0.0, 100.0), 3),
        }


class EventProducer(ProducerNode):
    '''
    Emits the deterministic event stream, paced at ``rate_fps`` messages per
    second (``<= 0`` = unpaced). Raises ``StopIteration`` when the stream is
    exhausted — the finite-producer contract.
    '''

    def __init__(self, seed: int, count: int, sensors: int, rate_fps: float = 100.0,
                 **kwargs: Any) -> None:
        self._seed = seed
        self._count = count
        self._sensors = sensors
        self._rate_fps = rate_fps
        self._events: Iterator[dict] | None = None
        super().__init__(**kwargs)

    def open(self) -> None:
        self._events = iter_events(self._seed, self._count, self._sensors)

    def next(self) -> dict:
        assert self._events is not None
        event = next(self._events)  # StopIteration at the end ends the stream
        if self._rate_fps > 0:
            time.sleep(1.0 / self._rate_fps)
        return event


class AsyncEnricher(ProcessorNode):
    '''
    An ``async def process`` node: videoflow awaits coroutine results on a
    task-owned event loop, so a node that needs async I/O (an HTTP lookup, an
    async DB client) can await it directly without blocking broker fetches and
    acks. The "lookup" here is simulated with ``asyncio.sleep``.

    This node also stamps the routing key: ``ctx.set_partition_key`` attaches
    the sensor id to the *next published message's* metadata (field
    ``_partition_key``). Partition keys do not propagate through nodes on their
    own — whichever node feeds a partitioned edge must stamp the key on its own
    output, which is why the stamp lives here and not on the producer.
    '''

    def __init__(self, threshold: float = 50.0, lookup_ms: float = 1.0, **kwargs: Any) -> None:
        self._threshold = threshold
        self._lookup_ms = lookup_ms
        super().__init__(**kwargs)

    async def process(self, event: dict, ctx: RuntimeContext | None = None) -> dict:
        await asyncio.sleep(self._lookup_ms / 1000.0)  # the pretend remote lookup
        if ctx is not None:
            ctx.set_partition_key(event['sensor'])
        return {**event, 'bucket': 'high' if event['value'] >= self._threshold else 'low'}


class KeyedCounter(ProcessorNode):
    '''
    A *stateful* processor that is nevertheless replicated — the combination
    that is only correct under partitioned routing. The graph sets
    ``nb_tasks=N, partition_by='_partition_key'``, so every message with the
    same sensor id hashes to the same replica and each replica's per-sensor
    counts are complete for the sensors it owns.

    Flip the graph to ``partition_by='trace_id'`` and the pinning is by lineage
    instead: every event has its own trace, one sensor's events spread across
    replicas, and each replica counts only its share. The ledger consumer sums
    the shares (so totals stay right either way), but records which replicas
    touched each sensor — the visible difference between the two routings.
    '''

    def __init__(self, **kwargs: Any) -> None:
        self._counts: dict = {}
        super().__init__(**kwargs)

    def open(self, ctx: RuntimeContext | None = None) -> None:
        if ctx is not None:
            ctx.logger.info(f'counter replica {ctx.replica_id} open '
                            f'(flow {ctx.flow_id}, run {ctx.run_id})')

    def process(self, event: dict, ctx: RuntimeContext | None = None) -> dict:
        count = self._counts.get(event['sensor'], 0) + 1
        self._counts[event['sensor']] = count
        return {
            'seq': event['seq'],
            'sensor': event['sensor'],
            'bucket': event['bucket'],
            'count': count,
            'replica': ctx.replica_id if ctx is not None else 0,
        }


class LedgerWriter(ConsumerNode):
    '''
    The sink: appends every count update to ``ledger.jsonl`` and, from
    ``close()``, writes ``counts.json`` — per-sensor totals (each replica's
    last count, summed across replicas), which replicas touched each sensor,
    whether the routing was sticky (exactly one replica per sensor), and
    whether the totals match the ground truth ``prepare.py`` baked.

    Construct with ``idempotent=True`` (the graph reads it from the config) and
    the framework deduplicates ``consume()`` effects across broker redelivery,
    when the flow has a Redis idempotency store.
    '''

    def __init__(self, ledger_path: str, counts_path: str,
                 expected_path: str | None = None, **kwargs: Any) -> None:
        self._ledger_path = ledger_path
        self._counts_path = counts_path
        self._expected_path = expected_path
        self._shares: dict = {}    # (sensor, replica) -> that replica's last count
        self._fh: TextIO | None = None
        super().__init__(**kwargs)

    def open(self) -> None:
        self._fh = open(self._ledger_path, 'a')

    def consume(self, update: dict) -> None:
        assert self._fh is not None
        self._fh.write(json.dumps(update) + '\n')
        self._fh.flush()
        self._shares[(update['sensor'], update['replica'])] = update['count']

    def close(self) -> None:
        if self._fh is not None:
            self._fh.close()
            self._fh = None
        totals: dict = {}
        replicas: dict = {}
        for (sensor, replica), count in self._shares.items():
            totals[sensor] = totals.get(sensor, 0) + count
            replicas.setdefault(sensor, set()).add(replica)
        report: dict = {
            'totals': dict(sorted(totals.items())),
            'replicas': {sensor: sorted(reps) for sensor, reps in sorted(replicas.items())},
            'sticky': all(len(reps) == 1 for reps in replicas.values()) if replicas else None,
            'matches_expected': None,
        }
        if self._expected_path:
            try:
                with open(self._expected_path) as f:
                    report['matches_expected'] = report['totals'] == json.load(f)
            except FileNotFoundError:
                pass
        with open(self._counts_path, 'w') as f:
            json.dump(report, f, indent=2)
