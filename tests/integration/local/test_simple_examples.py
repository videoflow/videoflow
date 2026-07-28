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

from videoflow.consumers import FileAppenderConsumer, VoidConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.processors.aggregators import SumAggregator
from videoflow.producers import IntProducer

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')

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

def test_time_synchronized_join_flow():
    # Two *independent* producers (no shared lineage) fused by event time. Their
    # published messages carry wall-clock event timestamps by default, so a
    # generous tolerance groups near-simultaneous emissions — a trace-id join
    # could never complete across different producers. Exercises the full
    # distributed path (envelope event_ts + TimeGroupAssembler in the worker).
    from videoflow.core.policies import JoinPolicy
    producer_a = IntProducer(0, 8, 0.02, name = 'a')
    producer_b = IntProducer(0, 8, 0.02, name = 'b')
    fused = JoinerProcessor(name = 'fused', join_policy = JoinPolicy(
        mode = 'time', tolerance_ms = 100, timeout_seconds = 1.0, quorum = 1,
    ))(producer_a, producer_b)
    printer = VoidConsumer(name = 'printer')(fused)
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
