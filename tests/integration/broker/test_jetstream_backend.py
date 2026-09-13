'''
The JetStream messaging adapter's own contract against a live broker: the
pieces ``NATSMessenger`` composes but no messenger-level test isolates —
settlement outcomes, the admission filter, and what a receiver does with the
deliveries it still holds when it goes away (a retirement hands them back at
once; a crash leaves them to their lease). Needs a reachable NATS JetStream
server (conftest skips otherwise).
'''
from __future__ import absolute_import, division, print_function

import os
import time
import uuid

from support_broker import cleanup

from videoflow.backends.capabilities import RELIABLE_WORK
from videoflow.backends.messaging import SUBSCRIPTION_DATA, ChannelId, Completed, Envelope, SubscriptionId
from videoflow.backends.outcomes import Accepted, Known, SettleConfirmed
from videoflow.core.constants import BATCH
from videoflow.messaging.jetstream_backend import JetStreamMessagingBackend, channel_spec_for, subscription_spec_for

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')


def _envelope(channel, pid, body = b'x'):
    return Envelope(channel = channel, publication_id = pid, headers = {'Nats-Msg-Id': pid}, body = body,
                    size = len(body), event_id = pid, partition_key = None, event_ts = None, source_epoch = None,
                    source_offset = None, schema_version = 4, kind = 'data')


def _receiver(flow_id, run_id, ack_wait = 30):
    '''A started adapter bound to the shared durable of ``child`` under ``parent``; prefetch 1, no keepalive.'''
    backend = JetStreamMessagingBackend(NATS_URL, flow_id, run_id, BATCH, prefetch = 1, keepalive = False)
    backend.start()
    channel = ChannelId(flow_id, run_id, 'parent')
    sub = SubscriptionId(channel, 'child', None, SUBSCRIPTION_DATA)
    backend.ensure_channel(channel_spec_for(flow_id, run_id, 'parent', BATCH, RELIABLE_WORK, required = (sub,)), 'op')
    backend.ensure_subscription(subscription_spec_for(sub, ack_wait, 4, 8), 'op')
    return backend, channel, sub


def _wait(pred, timeout = 15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(0.2)
    return pred()


def _collect(backend, sub, count, timeout):
    '''Receive until ``count`` deliveries arrived or ``timeout`` passed.'''
    got = []
    deadline = time.monotonic() + timeout
    while len(got) < count and time.monotonic() < deadline:
        got.extend(backend.receive(sub, count, 1 << 20, min(deadline, time.monotonic() + 1.0)))
    return got


def test_a_retired_receiver_hands_its_unsettled_deliveries_back_at_once():
    '''
    A replica scaled away must not keep leases it will never settle: everything
    it held — parked in its prefetch queue or received and unsettled — is NAKed
    at shutdown, so a survivor gets it immediately rather than after ack_wait.
    '''
    flow_id, run_id = 'jsadapter', uuid.uuid4().hex[:8]
    leaving, channel, sub = _receiver(flow_id, run_id, ack_wait = 60)
    survivor = None
    try:
        for i in range(3):
            assert isinstance(leaving.publish(_envelope(channel, f'{run_id}:{i}'), time.monotonic() + 10), Accepted)
        held = leaving.receive(sub, 1, 1 << 20, time.monotonic() + 5)
        assert len(held) == 1                                        # one received, one parked, one pending
        assert _wait(lambda: leaving.prefetched(sub) == 1)
        leaving.shutdown()                                           # graceful: hands both back
        survivor, _channel, sub2 = _receiver(flow_id, run_id, ack_wait = 60)
        got = _collect(survivor, sub2, 3, 20.0)
        assert sorted(d.headers['Nats-Msg-Id'] for d in got) == sorted(f'{run_id}:{i}' for i in range(3))
        # The handed-back ones arrive as a second attempt, never as a first.
        attempts = {d.headers['Nats-Msg-Id']: d.token.attempt for d in got}
        assert attempts[held[0].headers['Nats-Msg-Id']] == 2
        for d in got:
            assert isinstance(survivor.settle(d.token, Completed(), f'ack:{d.token.message_id}'), SettleConfirmed)
        observed = survivor.observe_subscription(sub2)
        assert isinstance(observed, Known) and (observed.value.available, observed.value.leased) == (0, 0)
    finally:
        if survivor is not None:
            survivor.shutdown()
        cleanup(flow_id, run_id)


def test_a_crashed_receiver_leaves_its_deliveries_to_their_lease():
    '''
    The other half of the contract: a dead process settles nothing, so nothing is
    handed back for it — its deliveries stay leased until ack_wait lapses. The
    lease is short here so the test can watch it lapse.
    '''
    flow_id, run_id = 'jsadapter', uuid.uuid4().hex[:8]
    dying, channel, sub = _receiver(flow_id, run_id, ack_wait = 3)
    survivor = None
    try:
        assert isinstance(dying.publish(_envelope(channel, f'{run_id}:0'), time.monotonic() + 10), Accepted)
        assert len(dying.receive(sub, 1, 1 << 20, time.monotonic() + 5)) == 1
        dying.shutdown(hand_back = False)                            # a crash
        survivor, _channel, sub2 = _receiver(flow_id, run_id, ack_wait = 3)
        t0 = time.monotonic()
        got = survivor.receive(sub2, 1, 1 << 20, time.monotonic() + 1.0)
        assert got == []                                             # still leased by the dead process
        got = _collect(survivor, sub2, 1, 15.0)
        assert got and time.monotonic() - t0 >= 2.0 and got[0].token.attempt == 2
        survivor.settle(got[0].token, Completed(), 'ack:0')
    finally:
        if survivor is not None:
            survivor.shutdown()
        cleanup(flow_id, run_id)
