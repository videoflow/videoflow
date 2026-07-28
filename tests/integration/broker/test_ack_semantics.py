'''
At-least-once delivery: ack-after-process, redelivery, DLQ, and crash recovery.

These drive the NATSMessenger and JetStream directly (no worker subprocesses) so
they are deterministic and independent of end-of-stream timing — the full-flow
crash/DLQ behaviour that also depends on the EOS drain protocol is covered in
Phase 3's tests. Needs a reachable NATS JetStream server.
'''
import asyncio
import os
import uuid

import pytest

from videoflow.core.compiler import NodeSpec
from videoflow.core.constants import BATCH, REALTIME
from videoflow.messaging import topology
from videoflow.messaging.nats_messenger import NATSMessenger
from videoflow.messaging.topology import provision_flow_sync
from videoflow.wire.serialization import MSG_TYPE_DATA, derive_message_id, encode_envelope

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')

class _StubNode:
    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name

    def open(self):
        pass

    def close(self):
        pass

def _spec(name, parents, kind, has_children):
    '''A real NodeSpec — provisioning reads name/parents/has_children/nb_tasks/partition_by.'''
    return NodeSpec(name = name, node_class = 'videoflow.processors.basic.IdentityProcessor',
                    params = {}, parents = parents, kind = kind, has_children = has_children,
                    nb_tasks = 1, device_type = 'cpu', is_finite = True)

def _ids():
    return 'ack', uuid.uuid4().hex[:8]

def _publish_parent_message(flow_id, run_id, parent, trace, seq, payload):
    '''Publish one data envelope onto the parent node's stream (simulating the parent).'''
    import nats

    async def _go():
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        subject = topology.subject_for(flow_id, run_id, parent)
        buf = encode_envelope(parent, flow_id, run_id, trace, seq, MSG_TYPE_DATA, None, payload)
        mid = derive_message_id(flow_id, run_id, parent, trace, seq, MSG_TYPE_DATA)
        await js.publish(subject, buf, headers = {'Nats-Msg-Id': mid})
        await nc.drain()

    asyncio.run(_go())

def _read_dlq(flow_id, run_id):
    import nats

    async def _go():
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        # Flow-scoped since RFC 0005; the run id lives in the subject instead.
        stream = topology.dlq_stream_name(flow_id)
        info = await js.stream_info(stream)
        n = info.state.messages
        out = []
        if n:
            sub = await js.pull_subscribe(f'vf.{flow_id}._dlq.{run_id}.>', durable = 'dlqreader',
                                        stream = stream)
            msgs = await sub.fetch(n, timeout = 3)
            for m in msgs:
                out.append({'headers': dict(m.headers or {}), 'data': m.data})
                await m.ack()
        await nc.drain()
        return out

    return asyncio.run(_go())

def _cleanup(flow_id, run_id):
    import nats

    async def _go():
        nc = await nats.connect(NATS_URL)
        await topology.delete_run_streams(nc, flow_id, run_id)
        # delete_run_streams deliberately spares the flow's DLQ; a test that
        # asserts on its emptiness has to remove it itself.
        try:
            await nc.jetstream().delete_stream(topology.dlq_stream_name(flow_id))
        except Exception:
            pass
        await nc.drain()

    asyncio.run(_go())

def test_dlq_on_exhausted_retries():
    '''
    With max_retries=0 (max_deliver=1) a single failure is already the final
    attempt, so fail_inputs dead-letters the message: it lands on the DLQ stream
    with a VF-Error header, and the worker keeps running.
    '''
    flow_id, run_id = _ids()
    specs = [_spec('parent', [], 'producer', True), _spec('child', ['parent'], 'consumer', False)]
    provision_flow_sync(NATS_URL, specs, flow_id, run_id, BATCH, max_retries = 0)
    m = NATSMessenger(_StubNode('child'), ['parent'], NATS_URL, flow_id, BATCH, run_id, max_retries = 0)
    try:
        _publish_parent_message(flow_id, run_id, 'parent', 't1', 1, {'value': 42})
        inputs = m.receive_message()
        assert inputs['parent']['is_stop_signal'] is False
        assert inputs['parent']['message'] == {'value': 42}
        m.fail_inputs(ValueError('permanent boom'))

        dlq = _read_dlq(flow_id, run_id)
        assert len(dlq) == 1
        assert 'boom' in dlq[0]['headers'].get('VF-Error', '')
        assert dlq[0]['headers'].get('VF-Origin-Node') == 'child'
    finally:
        m.close()
        _cleanup(flow_id, run_id)

def test_realtime_failure_drops_the_message_but_not_the_evidence():
    '''
    REALTIME never redelivers — a failed message is terminated, freshest wins.

    What it does *not* do any more is delete the evidence. "It's realtime, we drop
    things" is true of load shedding and false of exceptions, so a bounded sample
    of each distinct failure is dead-lettered: enough to diagnose the bug, never
    enough to fill a stream. The bounding itself is asserted in
    ``test_failure_invariants.py``; here the claim is simply that one failure
    leaves a trace and does not come back.
    '''
    flow_id, run_id = _ids()
    specs = [_spec('parent', [], 'producer', True), _spec('child', ['parent'], 'consumer', False)]
    provision_flow_sync(NATS_URL, specs, flow_id, run_id, REALTIME)
    m = NATSMessenger(_StubNode('child'), ['parent'], NATS_URL, flow_id, REALTIME, run_id)
    try:
        _publish_parent_message(flow_id, run_id, 'parent', 't1', 1, {'value': 7})
        inputs = m.receive_message()
        assert inputs['parent']['message'] == {'value': 7}
        m.fail_inputs(ValueError('boom'))

        dlq = _read_dlq(flow_id, run_id)
        assert len(dlq) == 1
        assert 'boom' in dlq[0]['headers'].get('VF-Error', '')

        # And it is gone from the flow: no redelivery, whatever the sample says.
        stream = topology.stream_name_for(flow_id, run_id, 'parent')
        durable = topology.durable_name_for('child', 'parent')

        import nats

        async def _pending():
            nc = await nats.connect(NATS_URL)
            try:
                info = await nc.jetstream().consumer_info(stream, durable)
                return info.num_pending, info.num_ack_pending
            finally:
                await nc.drain()

        assert asyncio.run(_pending()) == (0, 0)
    finally:
        m.close()
        _cleanup(flow_id, run_id)

def test_unacked_message_survives_restart():
    '''
    Simulates a worker crash: a message received but never acked is redelivered to
    a fresh messenger (a restarted worker binding the same durable). This is the
    core at-least-once guarantee of ack-after-process.
    '''
    flow_id, run_id = _ids()
    specs = [_spec('parent', [], 'producer', True), _spec('child', ['parent'], 'consumer', False)]
    provision_flow_sync(NATS_URL, specs, flow_id, run_id, BATCH)

    _publish_parent_message(flow_id, run_id, 'parent', 't1', 1, {'value': 99})

    # First worker: receive but "crash" before acking (close abandons the handle).
    m1 = NATSMessenger(_StubNode('child'), ['parent'], NATS_URL, flow_id, BATCH, run_id, ack_wait = 2)
    got1 = m1.receive_message()
    assert got1['parent']['message'] == {'value': 99}
    m1.close()  # no ack_inputs() → the message stays un-acked

    # Restarted worker binds the same durable and gets the message redelivered.
    m2 = NATSMessenger(_StubNode('child'), ['parent'], NATS_URL, flow_id, BATCH, run_id, ack_wait = 2)
    try:
        got2 = m2.receive_message()
        assert got2['parent']['message'] == {'value': 99}
        m2.ack_inputs()  # now processed successfully
    finally:
        m2.close()
        _cleanup(flow_id, run_id)

def test_nak_redelivers_up_to_max_deliver():
    '''Broker contract: a naked message is redelivered, and num_delivered climbs each time up to max_deliver.'''
    import nats
    flow_id, run_id = _ids()
    node, child = 'n', 'c'

    async def _go():
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        try:
            await js.add_stream(topology.stream_config_for(flow_id, run_id, node, BATCH))
            durable = topology.durable_name_for(child, node)
            await js.add_consumer(topology.stream_name_for(flow_id, run_id, node),
                                topology.consumer_config_for(flow_id, run_id, child, node,
                                                            max_deliver = 3, ack_wait = 2))
            subject = topology.subject_for(flow_id, run_id, node)
            sub = await js.pull_subscribe(subject, durable = durable)

            buf = encode_envelope(node, flow_id, run_id, 't1', 1, MSG_TYPE_DATA, None, 1)
            await js.publish(subject, buf, headers = {'Nats-Msg-Id': f'{node}-1'})

            deliveries = []
            for _ in range(3):
                msgs = await sub.fetch(1, timeout = 5)
                deliveries.append(msgs[0].metadata.num_delivered)
                await msgs[0].nak()
            assert deliveries == [1, 2, 3]  # redelivered each time
        finally:
            await topology.delete_run_streams(nc, flow_id, run_id)
            await nc.drain()

    asyncio.run(_go())

if __name__ == '__main__':
    pytest.main([__file__])
