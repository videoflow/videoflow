'''
Replica-safe end-of-stream: every replica of a node observes EOS and terminates,
and a flow whose parent is replicated drains all of that parent's replicas' data
before the child stops. Runs real flows via LocalProcessEngine (one subprocess per
node/replica) against a NATS server, asserting data completeness — which fails if
EOS is mishandled for any replica.
'''
import os
import tempfile

import pytest

from videoflow.consumers import FileAppenderConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')

def _run(flow):
    from videoflow.engines.local import LocalProcessEngine
    flow.run(LocalProcessEngine(nats_url = NATS_URL))
    flow.join()

def _collected(path):
    with open(path) as f:
        return sorted(int(line) for line in f if line.strip())

def test_replicated_processor_all_replicas_terminate_and_deliver():
    '''nb_tasks=3 processor: flow.join() returns only if all 3 replicas see EOS and exit, and the sink must have every item.'''
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        producer = IntProducer(0, 15, 0.01, name = 'producer')
        proc = IdentityProcessor(name = 'proc', nb_tasks = 3)(producer)
        sink = FileAppenderConsumer(out, name = 'sink')(proc)
        _run(Flow([sink], flow_type = BATCH))
        assert _collected(out) == list(range(16))

def test_replicated_parent_child_drains_all_replicas():
    '''
    A replicated parent (nb_tasks=2) feeds a single child. The child must drain
    both parent replicas' output before it stops — the drain-quiescence protocol,
    not an EOS count, is what guarantees this.
    '''
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        producer = IntProducer(0, 20, 0.01, name = 'producer')
        parent = IdentityProcessor(name = 'parent', nb_tasks = 2)(producer)
        child = IdentityProcessor(name = 'child')(parent)
        sink = FileAppenderConsumer(out, name = 'sink')(child)
        _run(Flow([sink], flow_type = BATCH))
        assert _collected(out) == list(range(21))

if __name__ == '__main__':
    pytest.main([__file__])
