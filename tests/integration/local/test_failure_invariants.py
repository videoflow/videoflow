'''
The properties that have to hold no matter which failures a run happens to hit.

The other error tests each pin one mechanism. These check what the mechanisms are
*for*, over a run that mixes them:

- the conservation law — every produced message is either delivered or
  dead-lettered, never both and never neither;
- an idempotent sink applies its effects once even across redelivery;
- a best-effort node's failures stay visible without its dead-letter queue
  growing without bound.

Needs a reachable NATS JetStream server (and Redis, for the idempotency test).
'''
from __future__ import absolute_import, division, print_function

import os
import pathlib
import random
import socket
import sys
import tempfile
from urllib.parse import urlparse

import pytest
from support_broker import NATS_URL, StubNode, cleanup, ids, publish_parent_message, read_dlq, spec

TESTS_DIR = str(pathlib.Path(__file__).resolve().parents[2])
if TESTS_DIR not in sys.path:
    sys.path.insert(0, TESTS_DIR)

from support_errors import CountingSink, FlakyProcessor  # noqa: E402

from videoflow.core import Flow  # noqa: E402
from videoflow.core.constants import BATCH, REALTIME  # noqa: E402
from videoflow.core.errors import SchemaError  # noqa: E402
from videoflow.core.supervision import SupervisionPolicy  # noqa: E402
from videoflow.engines.local import LocalProcessEngine  # noqa: E402
from videoflow.messaging.nats_messenger import NATSMessenger  # noqa: E402
from videoflow.messaging.topology import provision_flow_sync  # noqa: E402
from videoflow.producers import IntProducer  # noqa: E402

pytestmark = pytest.mark.timeout(180)

REDIS_URL = os.environ.get('VF_TEST_REDIS_URL', 'redis://localhost:6379/0')


def _redis_available(url = REDIS_URL) -> bool:
    parsed = urlparse(url)
    try:
        with socket.create_connection((parsed.hostname or 'localhost',
                                    parsed.port or 6379), timeout = 1):
            return True
    except OSError:
        return False


@pytest.mark.timeout(180)
def test_every_message_is_delivered_or_dead_lettered_never_both_never_neither():
    '''
    The conservation law, over a run mixing poison, transient and worker-fatal
    failures with a restart.

    This is the strongest single statement the error handling makes, and the one
    an operator actually relies on: after a messy run you can account for every
    message. It is checked as a set identity rather than as counts, so a message
    delivered *and* dead-lettered fails it just as loudly as one that vanished.

    Seeded and printed on failure, so a bad run is reproducible.
    '''
    seed = int(os.environ.get('VF_TEST_SEED', '1729'))
    rng = random.Random(seed)
    events = 30
    poison = sorted(rng.sample(range(events), 3))
    transient = sorted(rng.sample([v for v in range(events) if v not in poison], 3))

    flow_id, run_id = ids('invariant')
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        p = IntProducer(0, events - 1, name = 'producer')
        # Poison first: dead-lettered on the first failure, never delivered.
        bad = FlakyProcessor(fail_on_values = poison, error_kind = 'poison',
                            name = 'bad')(p)
        # Then transient: retried, and (with a budget of 3) eventually succeeding
        # because the marker stops the failures after one attempt each.
        flaky = FlakyProcessor(fail_on_values = transient, fail_times = len(transient),
                            error_kind = 'transient',
                            marker_path = os.path.join(d, 'transient'),
                            name = 'flaky')(bad)
        sink = CountingSink(out, name = 'sink')(flaky)
        flow = Flow([sink], flow_type = BATCH, flow_id = flow_id)
        flow._run_id = run_id

        engine = LocalProcessEngine(
            nats_url = NATS_URL,
            supervision = SupervisionPolicy(max_restarts = 3, backoff_seconds = (0.5,)))
        flow.run(engine, run_id = run_id)
        engine.wait_for_completion()
        engine._teardown_streams()

        with open(out) as f:
            delivered = [int(line) for line in f if line.strip()]
        dead = read_dlq(flow_id)
        cleanup(flow_id, run_id)

        produced = set(range(events))
        delivered_set = set(delivered)
        context = (f'seed={seed} poison={poison} transient={transient} '
                f'delivered={sorted(delivered_set)} dlq={len(dead)}')

        # Never both.
        assert len(delivered) == len(delivered_set), f'a message was delivered twice: {context}'
        # Never neither: what was not delivered is exactly what was dead-lettered.
        missing = produced - delivered_set
        assert len(dead) == len(missing), f'unaccounted messages: {context} missing={sorted(missing)}'
        # And the poison ones are precisely the missing ones.
        assert missing == set(poison), context


@pytest.mark.skipif(not _redis_available(), reason = 'needs Redis for the idempotency store')
def test_an_idempotent_sink_applies_its_effects_once_across_redelivery():
    '''
    At-least-once delivery means a sink *will* see a message twice eventually.
    ``idempotent=True`` is what turns that into once-applied effects, and this is
    the property it exists for.
    '''
    flow_id, run_id = ids('invariant')
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        p = IntProducer(0, 10, name = 'producer')
        # Fails once, which forces the message it held to be redelivered.
        flaky = FlakyProcessor(fail_times = 1, error_kind = 'worker_fatal',
                            marker_path = os.path.join(d, 'failures'),
                            name = 'flaky')(p)
        sink = CountingSink(out, name = 'sink', idempotent = True)(flaky)
        flow = Flow([sink], flow_type = BATCH, flow_id = flow_id)
        flow._run_id = run_id

        engine = LocalProcessEngine(
            nats_url = NATS_URL, blob_redis_url = REDIS_URL,
            supervision = SupervisionPolicy(max_restarts = 3, backoff_seconds = (0.5,)))
        flow.run(engine, run_id = run_id)
        assert engine.wait_for_completion() == []
        engine._teardown_streams()

        with open(out) as f:
            delivered = [int(line) for line in f if line.strip()]
        assert sorted(delivered) == list(range(11))
        assert len(delivered) == len(set(delivered))     # once each, despite the retry
        cleanup(flow_id, run_id)


def test_best_effort_failures_are_sampled_not_silent_and_not_unbounded():
    '''
    Dropping a message under load shedding is a policy; dropping the evidence of
    an exception is losing the bug report. A best-effort node keeps a bounded
    specimen of each distinct failure — enough to diagnose, never enough to fill
    a stream.
    '''
    flow_id, run_id = ids('invariant')
    specs = [spec('parent', [], 'producer', True),
            spec('child', ['parent'], 'consumer', False)]
    provision_flow_sync(NATS_URL, specs, flow_id, run_id, REALTIME)
    m = NATSMessenger(StubNode('child'), ['parent'], NATS_URL, flow_id, REALTIME, run_id,
                    ack_wait = 3)
    try:
        for i in range(30):
            publish_parent_message(flow_id, run_id, 'parent', f't{i}', i, {'v': i})
            m.receive_message()
            m.fail_inputs(SchemaError('always the same problem'))
        dlq = read_dlq(flow_id)
        # Not silent...
        assert dlq, 'a realtime failure left no trace at all'
        # ...and not one per failure either.
        assert len(dlq) < 30, f'sampling did not bound the queue: {len(dlq)} entries'
        assert {e['headers']['VF-Code'] for e in dlq} == {'VF_POISON_SCHEMA'}
    finally:
        m.close()
        cleanup(flow_id, run_id)


if __name__ == '__main__':
    pytest.main([__file__])
