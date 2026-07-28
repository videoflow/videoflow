'''
The failure ladder: what a node's DeliveryPolicy does with a failed message.

This is the heart of the redesign, so it is asserted as a **table** rather than as
prose — the same table ``spec/PROTOCOL.md`` §7.3 states normatively. If the two
ever disagree, one of them is a lie, and a test that mirrors the doc's shape is
the cheapest way to notice.

Pure: no broker, no worker, no clock.
'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.errors import POISON, TRANSIENT, WORKER_FATAL
from videoflow.core.policies import (
    ACTION_DLQ,
    ACTION_DLQ_SAMPLED,
    ACTION_NAK,
    ACTION_TERM,
    AT_LEAST_ONCE,
    BEST_EFFORT,
    DLQ_FULL,
    DLQ_OFF,
    DLQ_SAMPLED,
    DeliveryPolicy,
)

# (delivery, disposition, num_delivered) -> expected action, for a policy with a
# budget of 3 retries (max_deliver 4).
LADDER = [
    # A poison message is never retried in either mode: it failed on its content,
    # and its content will not change. The old ladder burned four attempts and
    # ~14s of backoff getting it to the same place.
    (AT_LEAST_ONCE, POISON, 1, ACTION_DLQ),
    (AT_LEAST_ONCE, POISON, 4, ACTION_DLQ),
    (BEST_EFFORT, POISON, 1, ACTION_DLQ_SAMPLED),

    # Transient: retry until the budget, then dead-letter. Best-effort keeps a
    # bounded specimen instead of retrying.
    (AT_LEAST_ONCE, TRANSIENT, 1, ACTION_NAK),
    (AT_LEAST_ONCE, TRANSIENT, 3, ACTION_NAK),
    (AT_LEAST_ONCE, TRANSIENT, 4, ACTION_DLQ),
    (AT_LEAST_ONCE, TRANSIENT, 9, ACTION_DLQ),
    (BEST_EFFORT, TRANSIENT, 1, ACTION_DLQ_SAMPLED),

    # Worker-fatal never dead-letters in either mode. The message is fine; this
    # worker is not, so it goes back for a healthy replica.
    (AT_LEAST_ONCE, WORKER_FATAL, 1, ACTION_NAK),
    (AT_LEAST_ONCE, WORKER_FATAL, 99, ACTION_NAK),
    (BEST_EFFORT, WORKER_FATAL, 1, ACTION_NAK),
    (BEST_EFFORT, WORKER_FATAL, 99, ACTION_NAK),
]


@pytest.mark.parametrize('delivery, disposition, num_delivered, expected', LADDER)
def test_ladder(delivery, disposition, num_delivered, expected):
    policy = DeliveryPolicy(delivery = delivery, max_retries = 3,
                            dlq = DLQ_FULL if delivery == AT_LEAST_ONCE else DLQ_SAMPLED)
    assert policy.action_for(disposition, num_delivered) == expected


def test_worker_fatal_is_never_dead_lettered_under_any_configuration():
    '''
    The property the whole redesign exists for: one wedged worker must not be able
    to move a healthy stream into the dead-letter queue a few messages at a time.
    '''
    for delivery in (AT_LEAST_ONCE, BEST_EFFORT):
        for dlq in (DLQ_FULL, DLQ_SAMPLED, DLQ_OFF):
            for retries in (0, 1, 3, 10):
                policy = DeliveryPolicy(delivery = delivery, max_retries = retries, dlq = dlq)
                for delivered in (1, 2, 5, 100):
                    assert policy.action_for(WORKER_FATAL, delivered) == ACTION_NAK


def test_dlq_off_drops_instead_of_dead_lettering():
    policy = DeliveryPolicy(delivery = AT_LEAST_ONCE, max_retries = 0, dlq = DLQ_OFF)
    assert policy.action_for(POISON, 1) == ACTION_TERM
    assert policy.action_for(TRANSIENT, 1) == ACTION_TERM


def test_flow_type_presets():
    batch = DeliveryPolicy.default_for(BATCH)
    assert (batch.delivery, batch.dlq, batch.max_deliver) == (AT_LEAST_ONCE, DLQ_FULL, 4)
    realtime = DeliveryPolicy.default_for(REALTIME)
    # Best-effort, but *sampled* rather than silent: dropping a message under load
    # shedding is a policy, dropping the evidence of an exception is losing the bug.
    assert (realtime.delivery, realtime.dlq, realtime.max_deliver) == (BEST_EFFORT, DLQ_SAMPLED, 1)


def test_max_deliver_follows_the_delivery_mode_not_the_flow_type():
    '''
    An at-least-once sink in a REALTIME flow must actually get retries — its
    durables' max_deliver has to move with it, or the override would be a lie.
    '''
    policy = DeliveryPolicy.resolve(REALTIME, {'delivery': AT_LEAST_ONCE})
    assert policy.max_deliver > 1
    assert policy.dlq == DLQ_FULL          # a real DLQ, not a sampled one
    assert DeliveryPolicy.resolve(BATCH, {'delivery': BEST_EFFORT}).max_deliver == 1


def test_resolve_layers_preset_then_node_then_deployment():
    policy = DeliveryPolicy.resolve(BATCH, {'on_error': POISON}, max_retries = 7)
    assert policy.delivery == AT_LEAST_ONCE      # from the preset
    assert policy.on_error == POISON             # from the node
    assert policy.max_retries == 7               # from the deployment
    assert policy.max_deliver == 8


def test_deployment_retries_do_not_resurrect_a_best_effort_node():
    # VF_MAX_RETRIES is a flow-wide knob; it must not quietly turn a node that
    # opted out of retrying back into one that retries.
    policy = DeliveryPolicy.resolve(BATCH, {'delivery': BEST_EFFORT}, max_retries = 7)
    assert policy.max_deliver == 1


def test_retry_delay_is_bounded_and_jittered():
    policy = DeliveryPolicy.default_for(BATCH)
    assert policy.retry_delay(1, jitter = 1.0) == 2
    assert policy.retry_delay(10, jitter = 1.0) == 30        # capped
    # Jitter exists so N replicas that failed together do not retry together.
    assert policy.retry_delay(3, jitter = 0.5) < policy.retry_delay(3, jitter = 1.5)


def test_round_trips_through_the_worker_boundary():
    policy = DeliveryPolicy(delivery = BEST_EFFORT, max_retries = 2, dlq = DLQ_SAMPLED,
                            on_error = POISON, sample_per_minute = 9)
    restored = DeliveryPolicy.from_dict(policy.to_dict())
    assert restored.to_dict() == policy.to_dict()


@pytest.mark.parametrize('kwargs, expected', [
    ({'delivery': 'eventually'}, 'delivery must be one of'),
    ({'dlq': 'maybe'}, 'dlq must be one of'),
    ({'on_error': 'catastrophic'}, 'on_error must be one of'),
    ({'max_retries': -1}, 'max_retries must be >= 0'),
])
def test_invalid_values_name_the_valid_ones(kwargs, expected):
    with pytest.raises(ValueError, match = expected):
        DeliveryPolicy(**kwargs)


def test_unknown_disposition_is_rejected_by_the_ladder():
    with pytest.raises(ValueError, match = 'Unknown disposition'):
        DeliveryPolicy.default_for(BATCH).action_for('sideways', 1)


if __name__ == '__main__':
    pytest.main([__file__])
