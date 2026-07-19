'''
Broker-level correctness tests for the retention/discard/dedup semantics that
give REALTIME vs BATCH their behavior. These talk directly to JetStream (no
workers) so they isolate the transport contract from the rest of the framework.

Needs a reachable NATS JetStream server.
'''
import asyncio
import os
import uuid

import pytest

from videoflow.core.constants import BATCH, REALTIME
from videoflow.messaging import topology
from videoflow.serialization import MSG_TYPE_DATA, derive_message_id, encode_envelope

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')

def _run(coro):
    return asyncio.run(coro)

def test_publish_dedup_drops_duplicate_message_id():
    '''
    Two publishes with the same Nats-Msg-Id collapse to one stored message. Uses a
    LIMITS-retention stream (with room for several messages) so the effect is
    visible independent of consumer interest — the point here is dedup, not
    retention.
    '''
    import nats
    flow_id, run_id, node = 'dedup', uuid.uuid4().hex[:8], 'n'

    async def _go():
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        try:
            # realtime_buffer>1 keeps a few messages under LIMITS retention.
            await js.add_stream(topology.stream_config_for(flow_id, run_id, node, REALTIME,
                                                        realtime_buffer = 10))
            subject = topology.subject_for(flow_id, run_id, node)
            mid = derive_message_id(flow_id, run_id, node, 'trace-1', 1, MSG_TYPE_DATA)
            buf = encode_envelope(node, flow_id, run_id, 'trace-1', 1, MSG_TYPE_DATA, None, 42)
            await js.publish(subject, buf, headers = {'Nats-Msg-Id': mid})
            await js.publish(subject, buf, headers = {'Nats-Msg-Id': mid})
            info = await js.stream_info(topology.stream_name_for(flow_id, run_id, node))
            assert info.state.messages == 1
        finally:
            await topology.delete_run_streams(nc, flow_id, run_id)
            await nc.drain()

    _run(_go())

def test_realtime_keeps_only_freshest():
    '''REALTIME (max_msgs=1, Discard=OLD): a stream retains only the latest message.'''
    import nats
    flow_id, run_id, node = 'rt', uuid.uuid4().hex[:8], 'n'

    async def _go():
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        try:
            await js.add_stream(topology.stream_config_for(flow_id, run_id, node, REALTIME))
            subject = topology.subject_for(flow_id, run_id, node)
            for i in range(5):
                buf = encode_envelope(node, flow_id, run_id, f't{i}', i, MSG_TYPE_DATA, None, i)
                await js.publish(subject, buf, headers = {'Nats-Msg-Id': f'{node}-{i}'})
            info = await js.stream_info(topology.stream_name_for(flow_id, run_id, node))
            assert info.state.messages == 1  # only the freshest survives
        finally:
            await topology.delete_run_streams(nc, flow_id, run_id)
            await nc.drain()

    _run(_go())

def test_batch_interest_rejects_when_full_and_frees_on_ack():
    '''
    BATCH (Interest + Discard=NEW): a full stream rejects new publishes (the basis
    of backpressure), and acking a message frees space so publishing resumes —
    proving no silent data loss.
    '''
    import nats
    flow_id, run_id, node, child = 'bp', uuid.uuid4().hex[:8], 'n', 'c'

    async def _go():
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        try:
            # Small bounded stream so we can force the full condition quickly.
            cfg = topology.stream_config_for(flow_id, run_id, node, BATCH, batch_max_msgs = 2)
            await js.add_stream(cfg)
            subject = topology.subject_for(flow_id, run_id, node)
            durable = topology.durable_name_for(child, node)
            await js.add_consumer(topology.stream_name_for(flow_id, run_id, node),
                                topology.consumer_config_for(flow_id, run_id, child, node))
            sub = await js.pull_subscribe(subject, durable = durable)

            async def _pub(i):
                buf = encode_envelope(node, flow_id, run_id, f't{i}', i, MSG_TYPE_DATA, None, i)
                await js.publish(subject, buf, headers = {'Nats-Msg-Id': f'{node}-{i}'})

            await _pub(0)
            await _pub(1)
            # Stream is full (max_msgs=2, nothing acked yet) → next publish rejected.
            with pytest.raises(Exception):
                await _pub(2)
            # Consume+ack one → frees a slot under interest retention.
            msgs = await sub.fetch(1, timeout = 5)
            await msgs[0].ack()
            await asyncio.sleep(0.2)
            # Now a publish succeeds again — data was paced, not dropped.
            await _pub(2)
            info = await js.stream_info(topology.stream_name_for(flow_id, run_id, node))
            assert info.state.messages == 2
        finally:
            await topology.delete_run_streams(nc, flow_id, run_id)
            await nc.drain()

    _run(_go())

if __name__ == '__main__':
    pytest.main([__file__])
