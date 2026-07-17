'''
Integration tests that run small flows end-to-end through the distributed
NATS-backed path (LocalProcessEngine spawns one worker subprocess per node).
They do not assert on data correctness — only that a representative graph runs
to completion without raising.

Skipped automatically if a NATS server is not reachable at VF_TEST_NATS_URL
(default nats://localhost:4222); start one with `nats-server -js` or
`docker compose up -d nats`.
'''
import os
import tempfile

import pytest

from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.processors.aggregators import SumAggregator
from videoflow.consumers import VoidConsumer, FileAppenderConsumer

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

pytestmark = pytest.mark.skipif(not _nats_available(), reason = f'NATS not reachable at {NATS_URL}')

def _run(flow):
    from videoflow.engines.local import LocalProcessEngine
    flow.run(LocalProcessEngine(nats_url = NATS_URL))
    flow.join()

def test_linear_flow():
    producer = IntProducer(0, 10, 0.02, name = 'producer')
    identity = IdentityProcessor(name = 'identity')(producer)
    printer = VoidConsumer(name = 'printer')(identity)
    _run(Flow([printer], flow_type = BATCH))

def test_diamond_join_flow():
    producer = IntProducer(0, 10, 0.02, name = 'producer')
    identity = IdentityProcessor(name = 'identity')(producer)
    identity1 = IdentityProcessor(name = 'identity1')(identity)
    joined = JoinerProcessor(name = 'joined')(identity, identity1)
    printer = VoidConsumer(name = 'printer')(joined)
    _run(Flow([printer], flow_type = BATCH))

def test_aggregator_flow():
    producer = IntProducer(0, 10, 0.02, name = 'producer')
    sum_agg = SumAggregator(name = 'sum')(producer)
    printer = VoidConsumer(name = 'printer')(sum_agg)
    _run(Flow([printer], flow_type = BATCH))

def test_fanout_flow():
    # nb_tasks=3 replicas compete for the producer's output; the consumer must
    # still receive every item exactly once, and all 3 replicas must terminate
    # (EOS reaching every replica) so flow.join() returns. Asserting the collected
    # data is the real regression gate for replica-safe EOS.
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        producer = IntProducer(0, 10, 0.02, name = 'producer')
        identity = IdentityProcessor(name = 'identity', nb_tasks = 3)(producer)
        sink = FileAppenderConsumer(out, name = 'sink')(identity)
        _run(Flow([sink], flow_type = BATCH))
        with open(out) as f:
            got = sorted(int(line) for line in f if line.strip())
        assert got == list(range(11))

if __name__ == "__main__":
    pytest.main([__file__])
