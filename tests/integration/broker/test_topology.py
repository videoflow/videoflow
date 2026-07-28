'''
Provisioning against a live JetStream server (videoflow.messaging.topology).

Only the provisioning half lives here — the naming and stream/config functions
are pure and sit in tests/test_topology_naming.py, so they run without a broker.
'''
import asyncio
import os
import uuid

import pytest

from videoflow.core.compiler import NodeSpec
from videoflow.core.constants import BATCH
from videoflow.messaging import topology

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')

def _spec(name, parents, kind, has_children):
    '''A real NodeSpec — provisioning reads name/parents/has_children/nb_tasks/partition_by.'''
    return NodeSpec(name = name, node_class = 'videoflow.processors.basic.IdentityProcessor',
                    params = {}, parents = parents, kind = kind, has_children = has_children,
                    nb_tasks = 1, device_type = 'cpu', is_finite = True)

def test_provision_is_idempotent():
    import nats
    flow_id = 'topo'
    run_id = uuid.uuid4().hex[:8]
    specs = [_spec('producer', [], 'producer', True), _spec('proc', ['producer'], 'processor', True),
             _spec('sink', ['proc'], 'consumer', False)]

    async def _go():
        nc = await nats.connect(NATS_URL)
        try:
            # Provision twice — the second must not raise.
            await topology.provision_flow(nc, specs, flow_id, run_id, BATCH)
            await topology.provision_flow(nc, specs, flow_id, run_id, BATCH)
            js = nc.jetstream()
            # All three node streams plus the DLQ stream exist.
            for name in ('producer', 'proc', 'sink'):
                info = await js.stream_info(topology.stream_name_for(flow_id, run_id, name))
                assert info is not None
            await js.stream_info(topology.dlq_stream_name(flow_id))
            # Durable consumers exist on parent streams.
            cinfo = await js.consumer_info(
                topology.stream_name_for(flow_id, run_id, 'producer'),
                topology.durable_name_for('proc', 'producer'))
            assert cinfo is not None
        finally:
            await topology.delete_run_streams(nc, flow_id, run_id)
            await nc.drain()

    asyncio.run(_go())

if __name__ == '__main__':
    pytest.main([__file__])
