'''
API expressivity: async component methods and optional ctx injection, exercised
end-to-end through worker subprocesses. Needs a reachable NATS JetStream server.
'''
import os
import sys
import tempfile

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)  # so this process can import support_nodes as the workers will

from support_nodes import AsyncDoubler, CtxPartitionTagger  # noqa: E402

from videoflow.consumers import FileAppenderConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.producers import IntProducer

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
    # Workers (subprocesses) must be able to import support_nodes too.
    os.environ['PYTHONPATH'] = TESTS_DIR + os.pathsep + os.environ.get('PYTHONPATH', '')
    flow.run(LocalProcessEngine(nats_url = NATS_URL))
    flow.join()

def _collected(path):
    with open(path) as f:
        return sorted(int(line) for line in f if line.strip())

def test_async_processor_runs_end_to_end():
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        producer = IntProducer(0, 10, 0.01, name = 'producer')
        doubler = AsyncDoubler(name = 'doubler')(producer)
        sink = FileAppenderConsumer(out, name = 'sink')(doubler)
        _run(Flow([sink], flow_type = BATCH))
        assert _collected(out) == [2 * i for i in range(11)]

def test_ctx_partition_key_routes_downstream():
    '''
    A ctx-using node sets a partition key that a downstream partitioned node routes
    by — proving ctx injection reaches the method and the key flows through the
    metadata into partition ownership. Correctness check: every item arrives once.
    '''
    from videoflow.processors import IdentityProcessor
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        producer = IntProducer(0, 20, 0.01, name = 'producer')
        tagger = CtxPartitionTagger(name = 'tagger')(producer)
        routed = IdentityProcessor(name = 'routed', nb_tasks = 2,
                                partition_by = '_partition_key')(tagger)
        sink = FileAppenderConsumer(out, name = 'sink')(routed)
        _run(Flow([sink], flow_type = BATCH))
        assert _collected(out) == list(range(21))

if __name__ == '__main__':
    pytest.main([__file__])
