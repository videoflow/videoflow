'''
Failure where more than one worker is involved: replicas, partitioning and joins.

Everything the other error tests assert holds for a single worker on a single
edge. These check the properties that only exist once there is a second one — a
sibling picking up what a dead replica held, a dead letter that does not disturb
partition ownership, a join whose two halves fail and redeliver together.

Needs a reachable NATS JetStream server.
'''
from __future__ import absolute_import, division, print_function

import os
import pathlib
import sys
import tempfile

import pytest
from support_broker import (
    NATS_URL,
    StubNode,
    cleanup,
    consumer_state,
    ids,
    publish_parent_message,
    read_dlq,
    spec,
)

TESTS_DIR = str(pathlib.Path(__file__).resolve().parents[2])
if TESTS_DIR not in sys.path:
    sys.path.insert(0, TESTS_DIR)

from support_errors import CountingSink, FlakyProcessor  # noqa: E402

from videoflow.core import Flow  # noqa: E402
from videoflow.core.constants import BATCH  # noqa: E402
from videoflow.core.errors import DeviceError, SchemaError  # noqa: E402
from videoflow.core.supervision import SupervisionPolicy  # noqa: E402
from videoflow.engines.local import LocalProcessEngine  # noqa: E402
from videoflow.messaging.nats_messenger import NATSMessenger  # noqa: E402
from videoflow.messaging.topology import provision_flow_sync  # noqa: E402
from videoflow.processors import IdentityProcessor, JoinerProcessor  # noqa: E402
from videoflow.producers import IntProducer  # noqa: E402

pytestmark = pytest.mark.timeout(150)


def test_a_sibling_replica_picks_up_what_a_dead_one_held():
    '''
    Competing replicas share one durable, so a message un-acked by a replica that
    died is owed to *any* of them. This is what makes handing a message back on a
    worker-fatal error worth anything: somebody else is there to take it.
    '''
    flow_id, run_id = ids('scale')
    specs = [spec('parent', [], 'producer', True),
            spec('child', ['parent'], 'processor', False, nb_tasks = 2)]
    provision_flow_sync(NATS_URL, specs, flow_id, run_id, BATCH)

    first = NATSMessenger(StubNode('child'), ['parent'], NATS_URL, flow_id, BATCH, run_id,
                        replica_id = 0, nb_tasks = 2, ack_wait = 2)
    try:
        publish_parent_message(flow_id, run_id, 'parent', 't1', 1, {'v': 1})
        assert first.receive_message()['parent']['message'] == {'v': 1}
        first.fail_inputs(DeviceError('this replica is done for'))
    finally:
        first.close()

    second = NATSMessenger(StubNode('child'), ['parent'], NATS_URL, flow_id, BATCH, run_id,
                        replica_id = 1, nb_tasks = 2, ack_wait = 2)
    try:
        # The sibling gets it — not the dead-letter queue.
        assert second.receive_message()['parent']['message'] == {'v': 1}
        second.ack_inputs()
        assert read_dlq(flow_id) == []
    finally:
        second.close()
        cleanup(flow_id, run_id)


def test_a_dead_letter_from_one_partition_leaves_ownership_alone():
    '''
    A partitioned node's replicas each bind their own durable and filter by hash.
    Dead-lettering must not disturb that: the entry is attributed to the replica's
    node, and every other partition keeps receiving exactly what it owns.
    '''
    flow_id, run_id = ids('scale')
    specs = [spec('parent', [], 'producer', True),
            spec('child', ['parent'], 'processor', False, nb_tasks = 3,
                partition_by = 'trace_id')]
    provision_flow_sync(NATS_URL, specs, flow_id, run_id, BATCH, max_retries = 0)

    messengers = [
        NATSMessenger(StubNode('child'), ['parent'], NATS_URL, flow_id, BATCH, run_id,
                    replica_id = i, nb_tasks = 3, partition_by = 'trace_id', ack_wait = 3)
        for i in range(3)
    ]
    try:
        for i in range(9):
            publish_parent_message(flow_id, run_id, 'parent', f't{i}', i, {'v': i})
        # Whichever replica owns t0 fails it; the others must still see their own.
        owned = []
        for m in messengers:
            got = m.receive_message()
            owned.append(got['parent']['message'])
            m.fail_inputs(SchemaError('unparseable'))
        assert len(owned) == 3                    # each replica got one of its own
        dlq = read_dlq(flow_id)
        assert len(dlq) == 3
        assert {e['headers']['VF-Origin-Node'] for e in dlq} == {'child'}
    finally:
        for m in messengers:
            m.close()
        cleanup(flow_id, run_id)


def test_both_halves_of_a_failed_join_go_back_together():
    '''
    A join's input group spans two parents, so failing it must NAK *both* halves —
    NAKing one would leave the other stranded, and the redelivered pair could never
    re-assemble into the group it came from.
    '''
    flow_id, run_id = ids('scale')
    specs = [spec('left', [], 'producer', True), spec('right', [], 'producer', True),
            spec('joined', ['left', 'right'], 'processor', False)]
    provision_flow_sync(NATS_URL, specs, flow_id, run_id, BATCH)

    m = NATSMessenger(StubNode('joined'), ['left', 'right'], NATS_URL, flow_id, BATCH,
                    run_id, ack_wait = 3)
    try:
        publish_parent_message(flow_id, run_id, 'left', 'shared', 1, {'side': 'L'})
        publish_parent_message(flow_id, run_id, 'right', 'shared', 1, {'side': 'R'})
        group = m.receive_message()
        assert group['left']['message'] == {'side': 'L'}
        assert group['right']['message'] == {'side': 'R'}
        m.fail_inputs(DeviceError('the joiner is unwell'))

        # Both halves are owed again, and they re-assemble into the same group.
        regroup = m.receive_message()
        assert regroup['left']['message'] == {'side': 'L'}
        assert regroup['right']['message'] == {'side': 'R'}
        m.ack_inputs()
        for parent in ('left', 'right'):
            pending, unacked = consumer_state(flow_id, run_id, 'joined', parent)
            assert (pending, unacked) == (0, 0)
    finally:
        m.close()
        cleanup(flow_id, run_id)


@pytest.mark.timeout(150)
def test_a_replicated_stage_survives_one_replica_crashing():
    '''
    The whole-flow version: three replicas, one dies once, and the run still
    delivers every message exactly once because the broker owed the dead
    replica's work to the others.
    '''
    flow_id, run_id = ids('scale')
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        p = IntProducer(0, 20, name = 'producer')
        work = FlakyProcessor(fail_times = 1, error_kind = 'worker_fatal',
                            marker_path = os.path.join(d, 'failures'),
                            nb_tasks = 3, name = 'work')(p)
        sink = CountingSink(out, name = 'sink')(work)
        flow = Flow([sink], flow_type = BATCH, flow_id = flow_id)
        flow._run_id = run_id

        engine = LocalProcessEngine(
            nats_url = NATS_URL,
            supervision = SupervisionPolicy(max_restarts = 3, backoff_seconds = (0.5,)))
        flow.run(engine, run_id = run_id)
        failed = engine.wait_for_completion()
        engine._teardown_streams()

        assert failed == []
        with open(out) as f:
            delivered = [int(line) for line in f if line.strip()]
        assert sorted(set(delivered)) == list(range(21))
        assert read_dlq(flow_id) == []          # nothing was blamed
        cleanup(flow_id, run_id)


@pytest.mark.timeout(150)
def test_a_join_still_completes_when_one_branch_had_to_retry():
    '''
    A transient failure in one branch of a diamond delays that half; the join must
    still re-align it with its sibling rather than emitting a partial group.
    '''
    flow_id, run_id = ids('scale')
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        p = IntProducer(0, 10, name = 'producer')
        left = FlakyProcessor(fail_on_values = [4], fail_times = 1,
                            error_kind = 'transient',
                            marker_path = os.path.join(d, 'left'), name = 'left')(p)
        right = IdentityProcessor(name = 'right')(p)
        joined = JoinerProcessor(name = 'joined')(left, right)
        sink = CountingSink(out, name = 'sink')(joined)
        flow = Flow([sink], flow_type = BATCH, flow_id = flow_id)
        flow._run_id = run_id

        engine = LocalProcessEngine(nats_url = NATS_URL,
                                    supervision = SupervisionPolicy.disabled())
        flow.run(engine, run_id = run_id)
        failed = engine.wait_for_completion()
        engine._teardown_streams()

        assert failed == []
        with open(out) as f:
            groups = [line for line in f if line.strip()]
        # Every event produced a complete group, including the one that retried.
        assert len(groups) == 11
        assert read_dlq(flow_id) == []
        cleanup(flow_id, run_id)


if __name__ == '__main__':
    pytest.main([__file__])
