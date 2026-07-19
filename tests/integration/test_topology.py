'''
Tests for broker naming, stream/consumer config, and provisioning
(videoflow.messaging.topology). The naming/config tests are pure; the
provisioning test needs a reachable NATS JetStream server.
'''
import asyncio
import os
import uuid

import pytest
from nats.js.api import DiscardPolicy, RetentionPolicy

from videoflow.core.constants import BATCH, REALTIME
from videoflow.messaging import topology

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')

# -- pure naming / config --------------------------------------------------

def test_names_are_run_scoped():
    assert topology.subject_for('f', 'r', 'node') == 'vf.f.r.node'
    assert topology.stream_name_for('f', 'r', 'node') == 'vf-f-r-node'
    assert topology.control_subject_for('f', 'r') == 'vf.f.r._control.stop'
    assert topology.dlq_stream_name('f', 'r') == 'vf-f-r-dlq'
    # Different runs never collide.
    assert topology.stream_name_for('f', 'r1', 'n') != topology.stream_name_for('f', 'r2', 'n')

def test_durable_is_child_from_parent():
    assert topology.durable_name_for('child', 'parent') == 'child--from--parent'

def test_realtime_stream_config_drops_old():
    cfg = topology.stream_config_for('f', 'r', 'n', REALTIME)
    assert cfg.retention == RetentionPolicy.LIMITS
    assert cfg.discard == DiscardPolicy.OLD
    assert cfg.max_msgs == 1

def test_batch_stream_config_is_interest_discard_new():
    cfg = topology.stream_config_for('f', 'r', 'n', BATCH)
    # Interest retention frees acked messages; Discard=NEW rejects publishes when
    # full → real backpressure instead of silent loss.
    assert cfg.retention == RetentionPolicy.INTEREST
    assert cfg.discard == DiscardPolicy.NEW
    assert cfg.max_msgs == topology.DEFAULT_BATCH_MAX_MSGS

def test_names_sanitize_illegal_chars():
    # dots in a node name would break subject tokenization → sanitized to underscore.
    assert '.' not in topology.subject_for('f', 'r', 'a.b').split('vf.f.r.')[1]

# -- provisioning (needs NATS) ---------------------------------------------

class _Spec:
    def __init__(self, name, parents):
        self.name = name
        self.parents = parents

def test_provision_is_idempotent():
    import nats
    flow_id = 'topo'
    run_id = uuid.uuid4().hex[:8]
    specs = [_Spec('producer', []), _Spec('proc', ['producer']), _Spec('sink', ['proc'])]

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
            await js.stream_info(topology.dlq_stream_name(flow_id, run_id))
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
