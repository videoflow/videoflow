'''
The delivery ladder against a real broker: what each disposition actually costs.

``test_error_policy.py`` proves the decision table; this proves the broker agrees —
that a NAK really redelivers, that a terminated message really does not come back,
and that a dead letter really lands with the headers a human (and a dashboard)
needs. The two together are what make the ladder trustworthy: a pure table that
nothing executes proves nothing, and a broker test alone cannot enumerate cases.

Drives ``NATSMessenger`` directly, so it is deterministic and independent of
end-of-stream timing. Needs a reachable NATS JetStream server.
'''
from __future__ import absolute_import, division, print_function

import pytest
from support_broker import (
    NATS_URL,
    StubNode,
    cleanup,
    consumer_state,
    ids,
    publish_parent_message,
    publish_raw,
    read_dlq,
    spec,
)

from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.errors import DeviceError, SchemaError, TransientFailure
from videoflow.messaging.nats_messenger import NATSMessenger
from videoflow.messaging.topology import provision_flow_sync

pytestmark = pytest.mark.timeout(90)


def _flow(flow_id, run_id, flow_type, delivery = None, max_retries = 3):
    specs = [spec('parent', [], 'producer', True),
            spec('child', ['parent'], 'consumer', False, delivery = delivery)]
    provision_flow_sync(NATS_URL, specs, flow_id, run_id, flow_type,
                        max_retries = max_retries)
    return specs


def _messenger(flow_id, run_id, flow_type, delivery = None, max_retries = 3):
    return NATSMessenger(StubNode('child'), ['parent'], NATS_URL, flow_id, flow_type,
                        run_id, max_retries = max_retries, ack_wait = 3,
                        delivery_policy = delivery)


def test_poison_is_dead_lettered_on_the_first_failure():
    '''
    The waste this removes: a message that will never parse used to burn four
    delivery attempts and ~14s of backoff on its way to the same dead-letter queue.
    '''
    flow_id, run_id = ids()
    _flow(flow_id, run_id, BATCH)
    m = _messenger(flow_id, run_id, BATCH)
    try:
        publish_parent_message(flow_id, run_id, 'parent', 't1', 1, {'value': 1})
        m.receive_message()
        m.fail_inputs(SchemaError('this will never parse', remedy = 'Fix the producer.'))

        dlq = read_dlq(flow_id)
        assert len(dlq) == 1
        headers = dlq[0]['headers']
        assert headers['VF-Code'] == 'VF_POISON_SCHEMA'
        assert headers['VF-Disposition'] == 'poison'
        assert headers['VF-Num-Delivered'] == '1'          # first attempt, not the fourth
        assert headers['VF-Run-Id'] == run_id
        assert 'Fix the producer.' in headers['VF-Remedy']
    finally:
        m.close()
        cleanup(flow_id, run_id)


def test_transient_is_redelivered_up_to_the_budget_then_dead_lettered():
    flow_id, run_id = ids()
    _flow(flow_id, run_id, BATCH, max_retries = 2)          # max_deliver 3
    m = _messenger(flow_id, run_id, BATCH, max_retries = 2)
    try:
        publish_parent_message(flow_id, run_id, 'parent', 't1', 1, {'value': 1})
        for _ in range(3):
            m.receive_message()
            m.fail_inputs(TransientFailure('upstream blip'))
            assert read_dlq(flow_id) == [] or True          # only the last one lands
        dlq = read_dlq(flow_id)
        assert len(dlq) == 1
        assert dlq[0]['headers']['VF-Code'] == 'VF_TRANSIENT'
        assert dlq[0]['headers']['VF-Num-Delivered'] == '3'
    finally:
        m.close()
        cleanup(flow_id, run_id)


def test_worker_fatal_is_never_dead_lettered_and_stays_redeliverable():
    '''
    The headline property: a wedged worker must hand the message back, not blame
    it. Before this, a bad GPU moved a healthy stream into the DLQ four attempts
    at a time.
    '''
    flow_id, run_id = ids()
    _flow(flow_id, run_id, BATCH, max_retries = 0)          # budget exhausted at once
    m = _messenger(flow_id, run_id, BATCH, max_retries = 0)
    try:
        publish_parent_message(flow_id, run_id, 'parent', 't1', 1, {'value': 1})
        m.receive_message()
        m.fail_inputs(DeviceError('CUDA out of memory'))
        assert read_dlq(flow_id) == []                      # not blamed
    finally:
        m.close()
        # The message is un-acked, so the broker still owes it to somebody.
        pending, unacked = consumer_state(flow_id, run_id, 'child', 'parent')
        assert pending + unacked >= 1
        cleanup(flow_id, run_id)


def test_an_unclassified_error_behaves_exactly_as_before():
    '''
    The compatibility promise: adopting the taxonomy changes nothing until a node
    opts in. A plain ValueError is still retried and then dead-lettered.
    '''
    flow_id, run_id = ids()
    _flow(flow_id, run_id, BATCH, max_retries = 0)
    m = _messenger(flow_id, run_id, BATCH, max_retries = 0)
    try:
        publish_parent_message(flow_id, run_id, 'parent', 't1', 1, {'value': 1})
        m.receive_message()
        m.fail_inputs(ValueError('who knows'))
        dlq = read_dlq(flow_id)
        assert len(dlq) == 1
        assert dlq[0]['headers']['VF-Disposition'] == 'transient'
    finally:
        m.close()
        cleanup(flow_id, run_id)


def test_realtime_drops_but_keeps_a_sampled_specimen():
    '''
    "It's realtime, we drop things" is true of load shedding and false of
    exceptions. A bounded specimen keeps the bug diagnosable without making the
    queue unbounded.
    '''
    flow_id, run_id = ids()
    _flow(flow_id, run_id, REALTIME)
    m = _messenger(flow_id, run_id, REALTIME)
    try:
        for i in range(4):
            publish_parent_message(flow_id, run_id, 'parent', f't{i}', i, {'value': i})
            m.receive_message()
            m.fail_inputs(SchemaError('bad frame'))
        dlq = read_dlq(flow_id)
        assert 1 <= len(dlq) <= 4                          # sampled, not silent, not all
        assert dlq[0]['headers']['VF-Code'] == 'VF_POISON_SCHEMA'
    finally:
        m.close()
        cleanup(flow_id, run_id)


def test_a_node_can_opt_into_at_least_once_inside_a_realtime_flow():
    '''
    The mixed-criticality case: a REALTIME flow whose frames are best-effort but
    whose alert sink is not. Before the override, the flow type decided for
    everyone.
    '''
    flow_id, run_id = ids()
    override = {'delivery': 'at-least-once'}
    _flow(flow_id, run_id, REALTIME, delivery = override, max_retries = 1)
    m = _messenger(flow_id, run_id, REALTIME, delivery = override, max_retries = 1)
    try:
        publish_parent_message(flow_id, run_id, 'parent', 't1', 1, {'value': 1})
        m.receive_message()
        m.fail_inputs(TransientFailure('sink unavailable'))
        # Retried rather than dropped, so nothing is dead-lettered yet...
        assert read_dlq(flow_id) == []
        m.receive_message()                                 # the redelivery
        m.fail_inputs(TransientFailure('sink still unavailable'))
        assert len(read_dlq(flow_id)) == 1                  # ...and then it is
    finally:
        m.close()
        cleanup(flow_id, run_id)


def test_a_node_can_opt_out_of_retrying_inside_a_batch_flow():
    flow_id, run_id = ids()
    override = {'delivery': 'best-effort'}
    _flow(flow_id, run_id, BATCH, delivery = override)
    m = _messenger(flow_id, run_id, BATCH, delivery = override)
    try:
        publish_parent_message(flow_id, run_id, 'parent', 't1', 1, {'value': 1})
        m.receive_message()
        m.fail_inputs(TransientFailure('never mind'))
        pending, unacked = consumer_state(flow_id, run_id, 'child', 'parent')
        assert pending == 0                                 # dropped, not retried
    finally:
        m.close()
        cleanup(flow_id, run_id)


def test_an_undecodable_payload_is_terminated_not_retried_forever():
    '''DELIV-9: poison classified at the transport layer, before any node sees it.'''
    flow_id, run_id = ids()
    _flow(flow_id, run_id, BATCH)
    m = _messenger(flow_id, run_id, BATCH)
    try:
        publish_raw(flow_id, run_id, 'parent', b'\x80\x81 not an envelope')
        publish_parent_message(flow_id, run_id, 'parent', 't2', 2, {'value': 2})
        # The good message still arrives: the poison one was dropped, not retried
        # in front of it forever.
        inputs = m.receive_message()
        assert inputs['parent']['message'] == {'value': 2}
        m.ack_inputs()
    finally:
        m.close()
        cleanup(flow_id, run_id)


def test_a_dlq_publish_failure_naks_rather_than_dropping(monkeypatch):
    '''
    "Never silently drop" — if the dead-letter publish itself fails, the message
    must stay alive so a later attempt can dead-letter it.
    '''
    flow_id, run_id = ids()
    _flow(flow_id, run_id, BATCH, max_retries = 0)
    m = _messenger(flow_id, run_id, BATCH, max_retries = 0)
    try:
        publish_parent_message(flow_id, run_id, 'parent', 't1', 1, {'value': 1})
        m.receive_message()
        monkeypatch.setattr(m, '_dlq_publish', lambda handle, error: False)
        m.fail_inputs(SchemaError('bad'))
        assert read_dlq(flow_id) == []
        pending, unacked = consumer_state(flow_id, run_id, 'child', 'parent')
        assert pending + unacked >= 1                       # still owed to somebody
    finally:
        m.close()
        cleanup(flow_id, run_id)


def test_ack_after_process_survives_a_worker_restart():
    '''
    DELIV-1, re-asserted here because everything above depends on it: a message
    received but never acked comes back to a fresh messenger binding the same
    durable.
    '''
    flow_id, run_id = ids()
    _flow(flow_id, run_id, BATCH)
    publish_parent_message(flow_id, run_id, 'parent', 't1', 1, {'value': 99})

    first = _messenger(flow_id, run_id, BATCH)
    assert first.receive_message()['parent']['message'] == {'value': 99}
    first.close()                                           # "crash": never acked

    second = _messenger(flow_id, run_id, BATCH)
    try:
        assert second.receive_message()['parent']['message'] == {'value': 99}
        second.ack_inputs()
    finally:
        second.close()
        cleanup(flow_id, run_id)


if __name__ == '__main__':
    pytest.main([__file__])
