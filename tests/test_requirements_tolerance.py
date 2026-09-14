'''
``FlowRequirements.tolerated_failures``: the planner certifies a fault model from
what was *read back* — persistent storage and ``2f + 1`` stream copies — never
from durable names, pod counts or a broker's survival standing in for the
payload store's (MSG-021, PAY-011 at the model level).
'''
from __future__ import absolute_import, division, print_function

import dataclasses

import pytest

from videoflow.backends.capabilities import (
    LEDGER_WINDOW,
    RELIABLE_WORK,
    FlowRequirements,
    MessagingCapabilities,
    PayloadCapabilities,
    ProfileRequest,
    plan_composition,
)
from videoflow.backends.outcomes import Unknown, known, unknown
from videoflow.core.errors import IncompatibleProfile, UnobservableState
from videoflow.deploy.admission import redis_payload_capabilities
from videoflow.deploy.broker_profiles import RedisProfile


def _caps(replicas = 3, persistent = True):
    return MessagingCapabilities(
        adapter = 'jetstream', version = '2.10', retained_backlog = True, recoverable_delivery = True,
        latest_per_key = False, dedup_window_seconds = 120, publication_ledger = LEDGER_WINDOW,
        replication_factor = known(replicas), persistent_storage = known(persistent),
        max_payload_bytes = known(1 << 20), credit_resizable = True, control_shares_data_slot = True)


def _req(f = 1):
    return FlowRequirements(profiles = (ProfileRequest('p', RELIABLE_WORK),), tolerated_failures = f)


def test_three_persistent_copies_certify_one_loss_and_five_certify_two():
    assert plan_composition(_req(1), _caps(3, True)).channel_profiles == {'p': RELIABLE_WORK}
    assert plan_composition(_req(2), _caps(5, True)).channel_profiles == {'p': RELIABLE_WORK}
    with pytest.raises(IncompatibleProfile) as e:
        plan_composition(_req(2), _caps(3, True))
    assert 'needs 5' in str(e.value)


def test_pods_are_not_copies_and_emptydir_is_not_storage():
    with pytest.raises(IncompatibleProfile) as e:
        plan_composition(_req(1), _caps(1, True))          # three brokers, one stream copy
    assert 'broker pods are not stream replicas' in str(e.value)
    with pytest.raises(IncompatibleProfile) as e:
        plan_composition(_req(1), _caps(3, False))         # replicated in memory / emptyDir
    assert 'ephemeral' in str(e.value)


def test_unread_storage_or_replication_cannot_certify():
    for field in ('persistent_storage', 'replication_factor'):
        caps = dataclasses.replace(_caps(), **{field: unknown('auth', 'stream_info denied')})
        with pytest.raises(UnobservableState):
            plan_composition(_req(1), caps)


def test_broker_survival_does_not_substitute_for_payload_durability():
    evictable = PayloadCapabilities('redis', durable = known(False), evictable = known(True),
                                    atomic_multikey = known(True), max_object_bytes = None, reader_identities = True)
    with pytest.raises(IncompatibleProfile) as e:
        plan_composition(_req(1), _caps(3, True), payload = evictable, payload_refs_in_use = True)
    assert 'broker survival does not substitute' in str(e.value)
    # Durably written is not enough either: the dev Redis writes its append-only
    # file to an emptyDir, which the pod takes with it; a read-back cannot tell
    # (the wire names a path, not what backs it), and unread is not a pass.
    durable = dataclasses.replace(evictable, durable = known(True))
    with pytest.raises(UnobservableState) as unread:
        plan_composition(_req(1), _caps(3, True), payload = durable, payload_refs_in_use = True)
    assert 'storage persistence' in str(unread.value)
    with pytest.raises(IncompatibleProfile, match = 'ephemeral storage'):
        plan_composition(_req(1), _caps(3, True), payload_refs_in_use = True,
                         payload = dataclasses.replace(durable, persistent_storage = known(False)))
    on_a_claim = dataclasses.replace(durable, persistent_storage = known(True))
    assert plan_composition(_req(1), _caps(3, True), payload = on_a_claim, payload_refs_in_use = True)
    # The shipped profiles say which they are; a bring-your-own store cannot.
    assert redis_payload_capabilities(RedisProfile.dev()).persistent_storage.value is False
    assert redis_payload_capabilities(RedisProfile.durable()).persistent_storage.value is True
    assert isinstance(redis_payload_capabilities(None).persistent_storage, Unknown)


def test_the_field_round_trips_and_is_absent_when_unset():
    assert 'tolerated_failures' not in FlowRequirements().to_dict()
    assert FlowRequirements().is_empty()
    req = FlowRequirements(tolerated_failures = 1)
    assert not req.is_empty()
    assert FlowRequirements.from_dict(req.to_dict()) == req
