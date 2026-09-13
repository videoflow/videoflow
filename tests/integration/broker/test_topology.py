'''
Provisioning against a live JetStream server (videoflow.messaging.topology).

Only the provisioning half lives here — the naming and stream/config functions
are pure and sit in tests/test_topology_naming.py, so they run without a broker.
'''
import asyncio
import os
import uuid

import pytest

from videoflow.backends.capabilities import LIVE_LATEST, RELIABLE_WORK, ProfileRequest
from videoflow.backends.outcomes import Known, Unknown
from videoflow.core.compiler import NodeSpec
from videoflow.core.constants import BATCH
from videoflow.core.errors import IncompatibleProfile
from videoflow.deploy.admission import jetstream_capabilities_observed
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

def test_read_back_and_live_capabilities_against_the_provisioned_run():
    '''
    What the provision entrypoint and a worker do after provisioning: read the
    run's streams back over a connection of their own, bind explicit profile
    requests to them, and observe the broker's capabilities live — against the
    real server, so the fields the fakes model (``config.retention`` / ``discard``
    / ``storage`` / ``num_replicas``, ``limits.max_storage``, ``max_payload``)
    are the ones nats-py 2.15.0 actually returns.
    '''
    import nats
    flow_id = 'topo-rb'
    run_id = uuid.uuid4().hex[:8]
    specs = [_spec('producer', [], 'producer', True), _spec('proc', ['producer'], 'processor', True),
             _spec('sink', ['proc'], 'consumer', False)]
    topology.provision_flow_sync(NATS_URL, specs, flow_id, run_id, BATCH,
                                 connect_options = {'allow_reconnect': False, 'connect_timeout': 5}, timeout = 30)
    try:
        read = topology.read_back_streams(NATS_URL, flow_id, run_id, ['producer', 'proc', 'ghost'], BATCH, timeout = 10)
        producer = read['producer']
        assert isinstance(producer, topology.VerifiedStream) and producer.effective is not None
        assert producer.mismatches == ()
        assert topology.profile_mismatches(RELIABLE_WORK, producer.effective) == ()
        assert topology.profile_mismatches(LIVE_LATEST, producer.effective) != ()
        assert isinstance(read['ghost'], Unknown) and read['ghost'].reason == 'missing'
        requests = [ProfileRequest('producer', RELIABLE_WORK), ProfileRequest('proc', RELIABLE_WORK)]
        topology.verify_channel_profiles(read, requests, unknown_is_fatal = True, where = 'test', config_mismatches = True)
        with pytest.raises(IncompatibleProfile, match = "channel 'proc' requests live_latest"):
            topology.verify_channel_profiles(read, [ProfileRequest('proc', LIVE_LATEST)], unknown_is_fatal = True, where = 'test')

        names = [topology.stream_name_for(flow_id, run_id, s.name) for s in specs]
        caps = jetstream_capabilities_observed(NATS_URL, timeout = 10, stream_names = names)
        assert caps.adapter == 'jetstream' and caps.version.startswith('2.')
        assert caps.retained_backlog and caps.recoverable_delivery
        assert isinstance(caps.max_payload_bytes, Known) and caps.max_payload_bytes.value > 0
        assert isinstance(caps.persistent_storage, Known) and caps.persistent_storage.value is True   # a file store
        assert isinstance(caps.replication_factor, Known) and caps.replication_factor.value == 1
        # Before any stream of a run exists only the account's allowance is known.
        fresh = jetstream_capabilities_observed(NATS_URL, timeout = 10,
                                                stream_names = [topology.stream_name_for(flow_id, 'nope', 'x')])
        assert fresh.persistent_storage.value is True and fresh.replication_factor.reason == 'unread'
    finally:
        async def _cleanup():
            nc = await nats.connect(NATS_URL)
            try:
                await topology.delete_run_streams(nc, flow_id, run_id)
            finally:
                await nc.drain()
        asyncio.run(_cleanup())

def test_read_back_and_capabilities_report_an_unreachable_broker_as_unknown():
    '''Nothing listens on port 1: the answer is "unreachable", within the deadline, never a config.'''
    read = topology.read_back_streams('nats://localhost:1', 'topo-rb', 'r', ['a', 'b'], BATCH, timeout = 2)
    assert {v.reason for v in read.values()} == {'unreachable'}
    caps = jetstream_capabilities_observed('nats://localhost:1', timeout = 2)
    assert isinstance(caps.persistent_storage, Unknown) and caps.persistent_storage.reason == 'unreachable'
    assert isinstance(caps.max_payload_bytes, Unknown) and caps.max_payload_bytes.reason == 'unreachable'

if __name__ == '__main__':
    pytest.main([__file__])
