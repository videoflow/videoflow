'''
Join policies and partition_by scale-out: policy serialization, how partitioning
shapes the compiled specs and Kubernetes manifests, and an end-to-end check that
a partitioned node delivers every message exactly once (each handled by one
replica).
'''
import json
import os
import tempfile

import pytest
import yaml

from videoflow.core import Flow
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.policies import JoinPolicy, MISSING_DROP, MISSING_WAIT
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer, FileAppenderConsumer
from videoflow.compiler import compile_flow
from videoflow.manifests import render_manifests

# -- unit: policy + params -------------------------------------------------

def test_join_policy_round_trips_through_get_params():
    j = JoinerProcessor(name = 'j', nb_tasks = 2, partition_by = 'trace_id',
                        join_policy = JoinPolicy(timeout_seconds = 5, missing = MISSING_DROP))
    params = j.get_params()
    json.dumps(params)  # must be JSON-serializable
    j2 = JoinerProcessor(**params)
    assert j2.partition_by == 'trace_id'
    assert j2.join_policy.timeout_seconds == 5
    assert j2.join_policy.missing == MISSING_DROP

def test_join_policy_defaults_per_flow_type():
    assert JoinPolicy.default_for(BATCH).missing == MISSING_WAIT
    assert JoinPolicy.default_for(REALTIME).timeout_seconds == 10.0

# -- unit: compiler + manifests --------------------------------------------

def _partitioned_flow():
    p = IntProducer(0, 5, name = 'producer')
    a = IdentityProcessor(name = 'a')(p)
    joined = JoinerProcessor(name = 'joined', nb_tasks = 3, partition_by = 'trace_id')(p, a)
    out = CommandlineConsumer(name = 'out')(joined)
    return Flow([out], flow_type = REALTIME, flow_id = 'part')

def test_compiler_carries_partition_and_join_policy():
    specs = {s.name: s for s in compile_flow(_partitioned_flow())}
    assert specs['joined'].partition_by == 'trace_id'
    assert specs['joined'].nb_tasks == 3

def test_partitioned_node_renders_statefulset_and_headless_service():
    specs = compile_flow(_partitioned_flow())
    manifests = render_manifests(specs, 'part', 'realtime', 'nats://x:4222', 'run1', autoscaling = True)
    by = {(m['kind'], m['metadata']['name']): m for m in manifests}
    assert ('StatefulSet', 'vf-part-joined') in by
    assert ('Service', 'vf-part-joined-hl') in by
    # Non-partitioned processor 'a' stays a Deployment.
    assert ('Deployment', 'vf-part-a') in by
    # Partitioned nodes are not KEDA-autoscaled (rehash on scale is unsafe).
    scaled = [m for m in manifests if m['kind'] == 'ScaledObject']
    scaled_names = {m['metadata']['name'] for m in scaled}
    assert 'vf-part-joined-scaler' not in scaled_names
    # The StatefulSet pod gets POD_NAME via the downward API for its replica id.
    ss = by[('StatefulSet', 'vf-part-joined')]
    env = ss['spec']['template']['spec']['containers'][0].get('env', [])
    assert any(e['name'] == 'POD_NAME' for e in env)

# -- integration: partition correctness ------------------------------------

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')

def _nats_available():
    import asyncio
    try:
        import nats
    except ImportError:
        return False

    async def _try():
        nc = await nats.connect(NATS_URL, connect_timeout = 2)
        await nc.drain()

    try:
        asyncio.run(_try())
        return True
    except Exception:
        return False

@pytest.mark.skipif(not _nats_available(), reason = f'NATS not reachable at {NATS_URL}')
def test_partitioned_processor_delivers_each_message_once():
    '''
    A partitioned processor (nb_tasks=2, partition_by='trace_id') must deliver every
    item to the sink exactly once: if two replicas both owned an item the sink would
    see a duplicate; if neither did, it would be missing.
    '''
    from videoflow.engines.local import LocalProcessEngine
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        producer = IntProducer(0, 25, 0.01, name = 'producer')
        part = IdentityProcessor(name = 'part', nb_tasks = 2, partition_by = 'trace_id')(producer)
        sink = FileAppenderConsumer(out, name = 'sink')(part)
        flow = Flow([sink], flow_type = BATCH)
        flow.run(LocalProcessEngine(nats_url = NATS_URL))
        flow.join()
        with open(out) as f:
            got = sorted(int(line) for line in f if line.strip())
        assert got == list(range(26))  # every item, exactly once (no dup, no loss)

if __name__ == '__main__':
    pytest.main([__file__])
