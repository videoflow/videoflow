'''
Failure propagates across the graph instead of hanging it.

The bug these exist for: a producer that raised never reached
``publish_stop_signal()``, so every descendant blocked forever on an
end-of-stream that was never coming. A BATCH flow in that state looked healthy —
the workers were alive, the broker was fine — and only a human noticing hours later
ended it.

**Every test here carries an explicit timeout**, because the failure mode under
test *is* a hang: without one, a regression would not fail the suite, it would stop
it. Each was confirmed to hang against the pre-ABORT code.

Runs real flows through ``LocalProcessEngine`` (one subprocess per node) against a
NATS server, so the whole chain — worker, wire, drain, propagation — is involved.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import os
import pathlib
import sys
import tempfile
import time

import nats
import pytest
from support_broker import NATS_URL, cleanup, ids

TESTS_DIR = str(pathlib.Path(__file__).resolve().parents[2])
if TESTS_DIR not in sys.path:
    sys.path.insert(0, TESTS_DIR)      # so the workers can import support_errors

from support_errors import (  # noqa: E402
    CountingSink,
    CrashingProcessor,
    DyingProducer,
    FlakyProcessor,
)

from videoflow.core import Flow  # noqa: E402
from videoflow.core.constants import BATCH  # noqa: E402
from videoflow.core.supervision import SupervisionPolicy  # noqa: E402
from videoflow.engines.local import ABORT_REANNOUNCE_SECONDS, LocalProcessEngine  # noqa: E402
from videoflow.messaging.topology import control_subject_for  # noqa: E402
from videoflow.processors import IdentityProcessor, JoinerProcessor  # noqa: E402
from videoflow.producers import IntProducer  # noqa: E402

pytestmark = pytest.mark.timeout(120)

# No restarts: these tests are about whether a *terminal* failure ends the flow,
# and retrying first would only add latency to the same question.
NO_RESTART = SupervisionPolicy.disabled()


def _run(flow, engine):
    '''Runs to completion and returns the nodes that failed.'''
    flow.run(engine, run_id = flow.run_id)
    failed = engine.wait_for_completion()
    engine._teardown_streams()
    return failed


def _wait_for_stop(flow_id, run_id, timeout):
    '''True when a control stop for this run arrives within ``timeout`` of subscribing.'''
    async def _go():
        nc = await nats.connect(NATS_URL)
        try:
            sub = await nc.subscribe(control_subject_for(flow_id, run_id))
            try:
                await sub.next_msg(timeout = timeout)
                return True
            except Exception:
                return False
        finally:
            await nc.drain()

    return asyncio.run(_go())


@pytest.mark.timeout(90)
def test_a_dying_producer_ends_the_whole_chain():
    '''
    The motivating bug, exactly. The producer raises part-way through, so it never
    reaches ``publish_stop_signal()`` — and before ABORT both downstream nodes
    waited forever on an end-of-stream that was never coming.
    '''
    flow_id, run_id = ids('abort')
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        p = DyingProducer(count = 20, die_at = 3, name = 'producer')
        relay = IdentityProcessor(name = 'relay')(p)
        sink = CountingSink(out, name = 'sink')(relay)
        flow = Flow([sink], flow_type = BATCH, flow_id = flow_id)
        flow._run_id = run_id

        engine = LocalProcessEngine(nats_url = NATS_URL, supervision = NO_RESTART)
        failed = _run(flow, engine)

        # The flow *ended* — that is the whole point — and it ended as a failure.
        assert 'producer' in failed
        # What it managed to produce still arrived: an abort ends the stream, it
        # does not discard the work that preceded it (ABORT-7).
        with open(out) as f:
            assert [int(line) for line in f if line.strip()] == [0, 1, 2]
        cleanup(flow_id, flow.run_id)


@pytest.mark.timeout(90)
def test_a_worker_fatal_error_ends_the_worker_and_then_the_flow():
    '''
    A node declaring itself unusable is believed: the message goes back to the
    broker, the worker stops, and the abort reaches the sink instead of leaving
    it waiting.
    '''
    flow_id, run_id = ids('abort')
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        p = IntProducer(0, 20, name = 'producer')
        crash = CrashingProcessor(crash_on_value = 3, name = 'crash')(p)
        sink = CountingSink(out, name = 'sink')(crash)
        flow = Flow([sink], flow_type = BATCH, flow_id = flow_id)
        flow._run_id = run_id

        engine = LocalProcessEngine(nats_url = NATS_URL, supervision = NO_RESTART)
        assert 'crash' in _run(flow, engine)
        cleanup(flow_id, flow.run_id)


@pytest.mark.timeout(90)
def test_a_hard_kill_still_ends_the_flow_via_the_supervisor():
    '''
    The layer behind the in-band marker: ``os._exit`` cannot publish anything, so
    only the supervisor's control-abort can end the run.
    '''
    flow_id, run_id = ids('abort')
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        p = IntProducer(0, 20, name = 'producer')
        crash = CrashingProcessor(crash_on_value = 3, hard = True, name = 'crash')(p)
        sink = CountingSink(out, name = 'sink')(crash)
        flow = Flow([sink], flow_type = BATCH, flow_id = flow_id)
        flow._run_id = run_id

        engine = LocalProcessEngine(nats_url = NATS_URL, supervision = NO_RESTART)
        failed = _run(flow, engine)
        assert 'crash' in failed
        cleanup(flow_id, flow.run_id)


@pytest.mark.timeout(60)
def test_the_supervisors_abort_reaches_a_worker_that_was_still_connecting():
    '''
    The control stop rides core NATS, so a single publish reaches only whoever is
    subscribed at that instant — and the worker that most needs to hear it is
    typically the one still connecting when its parent died on its first message.
    A missed announcement used to hang the run until the test's own timeout.

    So the supervisor keeps announcing until every worker is reaped. This drives
    that repeat directly: subscribe *after* the first publish is long gone and
    still expect a stop, which is the guarantee a real late-starting worker
    depends on.
    '''
    flow_id, run_id = ids('abort')
    engine = LocalProcessEngine(nats_url = NATS_URL, supervision = NO_RESTART)
    engine._flow_id, engine._run_id = flow_id, run_id
    engine._abort_flow('left')
    try:
        # Comfortably after the first announcement, comfortably before the second.
        time.sleep(ABORT_REANNOUNCE_SECONDS / 2)
        assert _wait_for_stop(flow_id, run_id, timeout = 15)
    finally:
        engine._stop_abort_announcer()


@pytest.mark.timeout(120)
def test_a_join_aborts_without_waiting_for_its_surviving_parent():
    '''
    Once one parent has died, no further input group involving it can ever
    complete — so waiting for the other parent's end-of-stream is waiting for
    nothing. This is the case a naive "stop when *all* parents stop" rule hangs on.
    '''
    flow_id, run_id = ids('abort')
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        p = IntProducer(0, 30, name = 'producer')
        left = CrashingProcessor(crash_on_value = 4, name = 'left')(p)
        right = IdentityProcessor(name = 'right')(p)
        joined = JoinerProcessor(name = 'joined')(left, right)
        sink = CountingSink(out, name = 'sink')(joined)
        flow = Flow([sink], flow_type = BATCH, flow_id = flow_id)
        flow._run_id = run_id

        engine = LocalProcessEngine(nats_url = NATS_URL, supervision = NO_RESTART)
        failed = _run(flow, engine)
        assert 'left' in failed
        cleanup(flow_id, flow.run_id)


@pytest.mark.timeout(90)
def test_a_clean_run_is_untouched_by_any_of_this():
    '''
    The control. Every mechanism above must be invisible when nothing fails —
    no spurious aborts, no premature stops, every message delivered.
    '''
    flow_id, run_id = ids('abort')
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        p = IntProducer(0, 10, name = 'producer')
        work = IdentityProcessor(name = 'work')(p)
        sink = CountingSink(out, name = 'sink')(work)
        flow = Flow([sink], flow_type = BATCH, flow_id = flow_id)
        flow._run_id = run_id

        engine = LocalProcessEngine(nats_url = NATS_URL, supervision = NO_RESTART)
        assert _run(flow, engine) == []
        with open(out) as f:
            delivered = [int(line) for line in f if line.strip()]
        assert sorted(delivered) == list(range(11))   # IntProducer's range is inclusive
        cleanup(flow_id, flow.run_id)


@pytest.mark.timeout(120)
def test_a_recoverable_crash_recovers_instead_of_ending_the_flow():
    '''
    Supervision parity: the same crash that Kubernetes survives (new pod, same
    durable, un-acked messages redelivered) must be survived here too. Before the
    local supervisor this run simply hung.
    '''
    flow_id, run_id = ids('abort')
    with tempfile.TemporaryDirectory() as d:
        out = os.path.join(d, 'out.txt')
        p = IntProducer(0, 10, name = 'producer')
        # Fails once *across processes* (the marker outlives the worker), so the
        # restarted worker succeeds where its predecessor died. Without the marker
        # the tally would reset and this would model a permanent fault instead.
        flaky = FlakyProcessor(fail_times = 1, error_kind = 'worker_fatal',
                               marker_path = os.path.join(d, 'failures'),
                               name = 'flaky')(p)
        sink = CountingSink(out, name = 'sink', idempotent = False)(flaky)
        flow = Flow([sink], flow_type = BATCH, flow_id = flow_id)
        flow._run_id = run_id

        engine = LocalProcessEngine(
            nats_url = NATS_URL,
            supervision = SupervisionPolicy(max_restarts = 3, backoff_seconds = (0.5,)))
        failed = _run(flow, engine)
        assert failed == []
        with open(out) as f:
            delivered = {int(line) for line in f if line.strip()}
        # Every message arrives: the one the failing worker held was un-acked, so
        # the broker gave it back rather than losing it.
        assert delivered == set(range(11))            # IntProducer's range is inclusive
        cleanup(flow_id, flow.run_id)


if __name__ == '__main__':
    pytest.main([__file__])
