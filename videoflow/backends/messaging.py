'''
The ``MessagingBackend`` contract: how an envelope reaches its required consumers.

A messaging backend owns channels, publication outcomes, logical subscriptions,
delivery leases, settlement semantics, transport retention and replay
capabilities. It does **not** own joining inputs, GPU reservations, application
effects or whole-run completion — those belong to the runtime, which composes a
backend with a payload store and a durable ledger.

The vocabulary that matters:

- A *subscription* is a **logical consumer**. Ten replicas serving one consumer
  compete for its work; two distinct downstream consumers each have their own
  obligations. Fan-out and competing delivery are never interchangeable defaults.
- A *delivery token* names the run, the subscription, the message, the attempt
  and the ownership generation. A stale attempt cannot decide a newer attempt's
  outcome: ``settle`` on a superseded token returns ``SettleStale`` without a
  broker call.
- ``Terminal`` settlement requires a reference to a durable record (a dead-letter
  acceptance, a ledger entry). An adapter must refuse to terminate a message
  whose only trace would be its own disappearance.
- Observations distinguish zero from unknown. A subscription observation that
  could not be made is ``Unknown``; queue counters are metrics, not a completion
  protocol.
'''
from __future__ import absolute_import, division, print_function

import abc
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Optional, Sequence, Union

from .capabilities import MessagingCapabilities
from .outcomes import CleanupObservation, Observation, PublicationOutcome, SettlementOutcome
from .payload import ImmutablePayloadRef

SUBSCRIPTION_DATA = 'data'
SUBSCRIPTION_EOS = 'eos'

#: ``Envelope.kind`` values: what the bytes are, which decides the subject they
#: ride and what a receiver does with them. ``dlq`` is a dead-letter publication
#: of raw wire bytes (``DELIV-8``), addressed by the *origin* node's channel.
KIND_DATA = 'data'
KIND_EOS = 'eos'
KIND_ABORT = 'abort'
KIND_DLQ = 'dlq'

OVERFLOW_EVICT_OLDEST = 'evict_oldest'
OVERFLOW_REJECT = 'reject'

@dataclass(frozen = True)
class ChannelId:
    flow_id : str
    run_id : str
    node : str

@dataclass(frozen = True)
class SubscriptionId:
    '''
    - Arguments:
        - partition: the replica index of a partitioned consumer (every replica \
            has its own logical subscription); None for a competing subscription \
            shared by all replicas.
        - kind: ``data`` or ``eos``.
        - instance: a per-process observer instance — end-of-stream markers are \
            observed by every replica through its own subscription, named by the \
            process that created it. None for the logical work subscription.
    '''
    channel : ChannelId
    consumer_node : str
    partition : int | None
    kind : str = SUBSCRIPTION_DATA
    instance : str | None = None

@dataclass(frozen = True)
class ChannelSpec:
    '''
    What a channel must provide, verified against the broker before production
    starts. ``owner_labels`` is the exact-ownership metadata teardown matches on.
    '''
    id : ChannelId
    profile : str
    retention : str
    max_msgs : int
    max_bytes : int | None
    max_age_seconds : float | None
    overflow : str
    dedup_window_seconds : int
    replicas : int
    persistence : bool
    required_subscriptions : tuple[SubscriptionId, ...]
    owner_labels : Mapping[str, str] = field(default_factory = dict)
    per_subject_limits : bool = False

@dataclass(frozen = True)
class SubscriptionSpec:
    '''``max_deliver == -1`` means the broker never strands a message; the runtime's ledger budgets attempts.'''
    id : SubscriptionId
    competing : bool
    ack_wait_seconds : float
    max_deliver : int
    item_credit : int
    byte_credit : int
    owner_labels : Mapping[str, str] = field(default_factory = dict)

@dataclass(frozen = True)
class Envelope:
    '''
    A publication. ``publication_id`` is the stable operation identity a retry
    re-uses (it becomes the broker's dedup key); ``event_id`` is the logical
    event identity the runtime reasons about. Physical stream sequences are
    diagnostics, not identity.
    '''
    channel : ChannelId
    publication_id : str
    headers : Mapping[str, str]
    body : bytes
    size : int
    event_id : str
    partition_key : str | None
    event_ts : float | None
    source_epoch : str | None
    source_offset : int | None
    schema_version : int
    payload_refs : tuple[ImmutablePayloadRef, ...] = ()
    kind : str = 'data'

@dataclass(frozen = True)
class DeliveryToken:
    subscription : SubscriptionId
    message_id : str
    stream_sequence : int | None
    attempt : int
    generation : str

@dataclass(frozen = True)
class Delivery:
    token : DeliveryToken
    envelope_bytes : bytes
    size : int
    received_at : float
    headers : Mapping[str, str] = field(default_factory = dict)

@dataclass(frozen = True)
class LeaseObservation:
    token : DeliveryToken
    renewed : bool
    expires_at : float | None
    reason : str = ''

@dataclass(frozen = True)
class SubscriptionObservation:
    '''Counts are for this observation only; ``unresolved`` is work the broker holds but can no longer deliver.'''
    available : int
    leased : int
    unresolved : int
    dropped : int
    rejected_publications : int
    observed_at : float
    generation : str | None = None

@dataclass(frozen = True)
class VerifiedChannel:
    spec : ChannelSpec
    effective : Mapping[str, Any]
    mismatches : tuple[str, ...] = ()

@dataclass(frozen = True)
class VerifiedSubscription:
    spec : SubscriptionSpec
    effective : Mapping[str, Any]
    mismatches : tuple[str, ...] = ()

@dataclass(frozen = True)
class Completed:
    pass

@dataclass(frozen = True)
class Retry:
    delay_seconds : float | None = None

@dataclass(frozen = True)
class Terminal:
    '''``record_ref`` names the durable record (DLQ acceptance, ledger id) that outlives the message.'''
    record_ref : str

Settlement = Union[Completed, Retry, Terminal]

class MessagingBackend(abc.ABC):
    '''
    The transport contract. Every method returns an outcome type rather than
    raising for the expected failure modes; exceptions are for programming
    errors and for faults the caller could not have anticipated.
    '''
    @abc.abstractmethod
    def capabilities(self) -> MessagingCapabilities:
        ...

    @abc.abstractmethod
    def ensure_channel(self, spec : ChannelSpec, operation_id : str) -> VerifiedChannel:
        '''
        Create or reconcile the channel and read back its effective configuration.
        An immutable mismatch (retention, replicas) raises ``IncompatibleProfile``;
        "already exists" is never accepted without inspection.
        '''

    @abc.abstractmethod
    def ensure_subscription(self, spec : SubscriptionSpec, operation_id : str) -> VerifiedSubscription:
        ...

    @abc.abstractmethod
    def publish(self, envelope : Envelope, deadline : float) -> PublicationOutcome:
        '''Publish with a monotonic deadline; on expiry the outcome is ``PublicationUnknown``, never a guess.'''

    @abc.abstractmethod
    def observe_publication(self, envelope : Envelope) -> PublicationOutcome:
        '''What the backend can say later about ``envelope.publication_id``; ``Unresolvable`` when it keeps no ledger.'''

    @abc.abstractmethod
    def receive(self, subscription : SubscriptionId, item_credit : int, byte_credit : int,
                deadline : float) -> list[Delivery]:
        ...

    @abc.abstractmethod
    def renew(self, token : DeliveryToken) -> LeaseObservation:
        ...

    @abc.abstractmethod
    def settle(self, token : DeliveryToken, outcome : Settlement, settlement_id : str) -> SettlementOutcome:
        ...

    @abc.abstractmethod
    def observe_subscription(self, subscription : SubscriptionId) -> Observation[SubscriptionObservation]:
        ...

    def receive_any(self, subscriptions : Sequence[SubscriptionId], item_credit : int, byte_credit : int,
                    deadline : float) -> list[tuple[SubscriptionId, Delivery]]:
        '''
        Deliveries from whichever of ``subscriptions`` has any, waiting until
        ``deadline`` (monotonic) for at least one. The default polls each
        subscription in turn; an adapter that can wait on all of them at once
        overrides it. Returns ``[]`` at the deadline, so a caller can re-check
        its own termination conditions rather than block forever.
        '''
        import time
        while True:
            out : list[tuple[SubscriptionId, Delivery]] = []
            for subscription in subscriptions:
                for delivery in self.receive(subscription, item_credit, byte_credit, time.monotonic()):
                    out.append((subscription, delivery))
            if out or time.monotonic() >= deadline:
                return out
            time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))

    def subscribe_control(self, callback : Callable[[], None]) -> None:
        '''Invoke ``callback`` when the run's flow-wide stop is published; a backend without a control channel ignores it.'''
        return None

    def shutdown(self) -> None:
        '''Release connections and threads; the channels stay (``close`` removes those).'''
        return None

    def set_admission(self, subscription : SubscriptionId, admit : Callable[[Delivery], bool],
                      on_skip : Optional[Callable[[Delivery], None]] = None) -> bool:
        '''
        Ask the backend to run ``admit`` on every delivery of ``subscription`` \
            before it is parked for ``receive``: a delivery it refuses is settled \
            ``Completed`` by the backend at once — so a message this consumer will \
            never process does not occupy its ack window — and, once that \
            settlement is confirmed, handed to ``on_skip`` off the backend's own \
            threads. ``admit`` runs on the backend's thread and must be cheap and \
            must not block. Returns False when the backend keeps no such filter; \
            the caller then decides on the receiving side.
        '''
        return False

    def prefetched(self, subscription : SubscriptionId) -> int:
        '''Deliveries fetched from the broker and parked locally for ``subscription`` (0 when the backend keeps none).'''
        return 0

    def supersede(self, token : DeliveryToken) -> bool:
        '''
        Retire a delivery token locally — the receiver now holds a newer attempt
        of the same logical message — without any broker settlement. Returns
        whether the token was live. A later ``settle`` of the retired token is
        ``SettleStale``. The default is a no-op for adapters whose tokens are
        already fenced by attempt and generation.
        '''
        return False

    @abc.abstractmethod
    def close(self, owned : Sequence[ChannelId], expected_generation : str) -> CleanupObservation:
        '''Remove exactly the owned channels; report what could not be confirmed removed.'''
