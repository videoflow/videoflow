'''
Broker helpers shared by the error-path integration tests.

These drive a ``NATSMessenger`` directly rather than through worker subprocesses,
which makes the delivery ladder deterministic: no end-of-stream timing, no process
scheduling, just "publish one message, fail it, look at what the broker holds".
The full-flow behaviour that *does* depend on EOS timing is exercised separately by
the tests that run real flows.

Grown out of the helpers in ``test_ack_semantics.py``, which every new error test
would otherwise have copied.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import os
import uuid
from typing import Any, Dict, List, Optional

from videoflow.core.compiler import NodeSpec
from videoflow.messaging import topology
from videoflow.wire.serialization import MSG_TYPE_DATA, derive_message_id, encode_envelope

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')


class StubNode:
    '''The minimum a messenger needs from a node: a name.'''
    def __init__(self, name : str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass


def spec(name : str, parents : List[str], kind : str, has_children : bool,
        nb_tasks : int = 1, partition_by : Optional[str] = None,
        delivery : Optional[dict] = None) -> NodeSpec:
    '''A real NodeSpec — provisioning reads name/parents/nb_tasks/partition_by/delivery.'''
    return NodeSpec(name = name, node_class = 'videoflow.processors.basic.IdentityProcessor',
                    params = {}, parents = parents, kind = kind, has_children = has_children,
                    nb_tasks = nb_tasks, device_type = 'cpu', is_finite = True,
                    partition_by = partition_by, delivery = delivery)


def ids(prefix : str = 'err') -> tuple:
    '''A fresh (flow_id, run_id). Flow ids are unique per test so DLQs never mix.'''
    return f'{prefix}-{uuid.uuid4().hex[:6]}', uuid.uuid4().hex[:8]


def publish_parent_message(flow_id : str, run_id : str, parent : str, trace : str,
                        seq : int, payload : Any) -> None:
    '''Publishes one data envelope on a parent's stream, as that parent would.'''
    import nats

    async def _go() -> None:
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        subject = topology.subject_for(flow_id, run_id, parent)
        buf = encode_envelope(parent, flow_id, run_id, trace, seq, MSG_TYPE_DATA, None, payload)
        mid = derive_message_id(flow_id, run_id, parent, trace, seq, MSG_TYPE_DATA)
        await js.publish(subject, buf, headers = {'Nats-Msg-Id': mid})
        await nc.drain()

    asyncio.run(_go())


def publish_raw(flow_id : str, run_id : str, parent : str, data : bytes,
                msg_id : Optional[str] = None) -> None:
    '''Publishes arbitrary bytes on a parent's data subject — for poison-payload tests.'''
    import nats

    async def _go() -> None:
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        await js.publish(topology.subject_for(flow_id, run_id, parent), data,
                        headers = {'Nats-Msg-Id': msg_id or uuid.uuid4().hex})
        await nc.drain()

    asyncio.run(_go())


def read_dlq(flow_id : str, run_id : Optional[str] = None,
            node : Optional[str] = None) -> List[Dict[str, Any]]:
    '''
    Every dead letter matching the filter, consumed (so a second call in one test
    sees only what arrived since).
    '''
    import nats

    async def _go() -> list:
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        stream = topology.dlq_stream_name(flow_id)
        out : list = []
        try:
            info = await js.stream_info(stream)
        except Exception:
            await nc.drain()
            return out
        if info.state.messages:
            subject = topology.dlq_subject_filter(flow_id, run_id, node)
            sub = await js.pull_subscribe(subject, durable = f'dlqreader{uuid.uuid4().hex[:6]}',
                                        stream = stream)
            try:
                msgs = await sub.fetch(info.state.messages, timeout = 3)
            except Exception:
                msgs = []
            for m in msgs:
                out.append({'headers': dict(m.headers or {}), 'data': m.data,
                            'subject': m.subject})
                await m.ack()
        await nc.drain()
        return out

    return asyncio.run(_go())


def consumer_state(flow_id : str, run_id : str, child : str, parent : str,
                replica : Optional[int] = None) -> tuple:
    '''``(num_pending, num_ack_pending)`` for one edge's durable.'''
    import nats

    durable = (topology.partitioned_durable_name_for(child, parent, replica)
            if replica is not None else topology.durable_name_for(child, parent))

    async def _go() -> tuple:
        nc = await nats.connect(NATS_URL)
        try:
            js = nc.jetstream()
            info = await js.consumer_info(topology.stream_name_for(flow_id, run_id, parent),
                                        durable)
            return info.num_pending, info.num_ack_pending
        finally:
            await nc.drain()

    return asyncio.run(_go())


def stream_exists(name : str) -> bool:
    import nats

    async def _go() -> bool:
        nc = await nats.connect(NATS_URL)
        try:
            await nc.jetstream().stream_info(name)
            return True
        except Exception:
            return False
        finally:
            await nc.drain()

    return asyncio.run(_go())


def cleanup(flow_id : str, run_id : str, drop_dlq : bool = True) -> None:
    '''
    Deletes the run's streams, and (unless a test is asserting the opposite) the
    flow's DLQ too — which run teardown deliberately does *not* touch.
    '''
    import nats

    async def _go() -> None:
        nc = await nats.connect(NATS_URL)
        await topology.delete_run_streams(nc, flow_id, run_id)
        if drop_dlq:
            try:
                await nc.jetstream().delete_stream(topology.dlq_stream_name(flow_id))
            except Exception:
                pass
        await nc.drain()

    asyncio.run(_go())
