'''
Shared helpers for the broker-level conformance variants: unique flow/run
identities, a started ``JetStreamMessagingBackend`` that is torn down with exact
ownership afterwards, raw JetStream access for specimens the adapter would never
publish (poison bytes, foreign headers), and a Redis client whose keys are swept.

Every helper here is deliberately small: the oracles live in the case modules,
next to the assertion they decide. What is shared is only the plumbing that two
modules would otherwise duplicate — and the cleanup discipline, which is the one
thing a broker-level test must never get wrong on a shared dev broker.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import contextlib
import uuid
from typing import Any, Callable, Iterator, Optional

from videoflow.core.constants import BATCH
from videoflow.messaging import topology
from videoflow.messaging.jetstream_backend import JetStreamMessagingBackend


def unique_ids(prefix : str = 'conf') -> tuple[str, str]:
    '''A ``(flow_id, run_id)`` pair no other test (or leftover) can collide with.'''
    return f'{prefix}-{uuid.uuid4().hex[:6]}', uuid.uuid4().hex[:8]


def run_async(coro_factory : Callable[[], Any], timeout : float = 30.0) -> Any:
    '''``asyncio.run`` with a bound — the test tree has no pytest-asyncio, on purpose.'''
    async def _bounded() -> Any:
        return await asyncio.wait_for(coro_factory(), timeout = timeout)
    return asyncio.run(_bounded())


def delete_run(nats_url : str, flow_id : str, run_id : str) -> Any:
    '''Tear a run's streams down by exact ownership (never by prefix) and return the observation.'''
    import nats  # optional dep (distributed extras)

    async def _go() -> Any:
        nc = await nats.connect(nats_url)
        try:
            return await topology.delete_run_streams(nc, flow_id, run_id)
        finally:
            await nc.drain()
    return run_async(_go)


def delete_dlq(nats_url : str, flow_id : str) -> None:
    '''Remove a flow's DLQ stream, which no run's teardown owns.'''
    import nats  # optional dep (distributed extras)

    async def _go() -> None:
        nc = await nats.connect(nats_url)
        try:
            with contextlib.suppress(Exception):
                await nc.jetstream().delete_stream(topology.dlq_stream_name(flow_id))
        finally:
            await nc.drain()
    run_async(_go)


@contextlib.contextmanager
def jetstream_backend(nats_url : str, flow_id : str, run_id : str,
                      flow_type : str = BATCH) -> Iterator[JetStreamMessagingBackend]:
    '''A started adapter for one run; shut down and its streams removed on exit, whatever happened.'''
    backend = JetStreamMessagingBackend(nats_url, flow_id, run_id, flow_type)
    backend.start()
    try:
        yield backend
    finally:
        backend.shutdown()
        delete_run(nats_url, flow_id, run_id)
        delete_dlq(nats_url, flow_id)


def publish_raw(nats_url : str, subject : str, payload : bytes, headers : Optional[dict] = None,
                timeout : float = 5.0) -> Any:
    '''Publish bytes the adapter would never emit (poison, a foreign header) and return the PubAck.'''
    import nats  # optional dep (distributed extras)

    async def _go() -> Any:
        nc = await nats.connect(nats_url)
        try:
            return await nc.jetstream().publish(subject, payload, headers = headers, timeout = timeout)
        finally:
            await nc.drain()
    return run_async(_go)


def stream_state(nats_url : str, stream : str) -> Any:
    '''``stream_info().state`` — messages, first/last sequence, consumer count.'''
    import nats  # optional dep (distributed extras)

    async def _go() -> Any:
        nc = await nats.connect(nats_url)
        try:
            return (await nc.jetstream().stream_info(stream)).state
        finally:
            await nc.drain()
    return run_async(_go)


@contextlib.contextmanager
def redis_client(url : str, sweep : Optional[str] = None) -> Iterator[Any]:
    '''
    A connected client. The dev Redis is shared with every other suite that may be
    running, so nothing is swept by default: a test removes the keys *it* created
    (``sweep_refs``), and only an isolated instance (``redis-small``, ``redis-durable``)
    may pass a ``sweep`` pattern to clear everything matching it on exit.
    '''
    import redis  # optional dep (redis extra)
    client = redis.Redis.from_url(url)
    try:
        yield client
    finally:
        if sweep:
            with contextlib.suppress(Exception):
                keys = list(client.scan_iter(match = sweep, count = 500))
                if keys:
                    client.delete(*keys)
        with contextlib.suppress(Exception):
            client.close()


def sweep_refs(client : Any, refs : Any) -> None:
    '''Remove the blob, metadata, obligation and legacy counter keys of the given payload refs (or raw keys).'''
    from videoflow.wire.redis_payload_store import counter_key, metadata_key, obligation_key
    keys : list[str] = []
    for ref in refs:
        key = ref if isinstance(ref, str) else ref.key
        keys += [key, metadata_key(key), obligation_key(key), counter_key(key)]
    if keys:
        with contextlib.suppress(Exception):
            client.delete(*keys)
