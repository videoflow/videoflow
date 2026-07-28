'''
End-to-end partition correctness: a ``partition_by`` node with several replicas
must hand each message to exactly one of them.

The policy/compiler/manifest half of partitioning is pure and lives in
tests/test_compiler_manifests.py — it was here until the directory grew its NATS
gate, which skipped it for want of a broker it never touched.
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
