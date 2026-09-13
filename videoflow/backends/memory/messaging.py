'''
The in-memory ``MessagingBackend``: a faithful model of a JetStream-like broker
(and, in ``core_only`` mode, of Core NATS) under a fake clock.

What it models deliberately, because the conformance cases turn on it:

- **Retention.** ``limits`` channels keep at most ``max_msgs`` messages **per
  stream** — data, EOS and control share the slot unless ``per_subject_limits``
  is set — and either evict the oldest (recording a drop for every subscription
  that had not settled it) or reject the publish. ``interest`` channels keep a
  message until every required subscription has settled it.
- **Leases and redelivery.** A delivery is leased for ``ack_wait_seconds``; an
  expired lease redelivers with ``attempt + 1`` until ``max_deliver`` (``-1`` =
  unlimited). Past the cap the message is *unresolved*: retained, undeliverable,
  and reported as such — never as "zero pending".
- **Credit.** ``item_credit`` is the server-side cap on leased messages per
  subscription, shared by every replica competing on it; ``byte_credit`` bounds
  a single receive.
- **Dedup.** A ``publication_id`` seen within the window is accepted as a
  duplicate and stored once; beyond it a retry is a new message.
- **Ambiguous acceptance.** ``pause_acceptance`` holds publications so a caller's
  deadline expires with the outcome unknown; ``resume`` then stores them (late
  acceptance) unless ``cancel_publication`` removed them first.
- **Truthful observation.** ``fail_observation`` makes ``observe_subscription``
  return ``Unknown``; nothing in this model turns a failed read into zero.
- **Core-only mode**: no retention, no leases, bounded client queues with
  slow-consumer drops, nothing for a subscription that was not attached at
  publish time.
- **Subjects.** A subscription sees only the messages of its own kind — a
  ``data`` subscription never receives a terminator and an ``eos`` one never
  receives data — exactly as a JetStream durable filters to one subject of the
  node's stream. A dead letter (``kind == 'dlq'``) is stored on the origin
  node's channel for inspection but belongs to the flow's DLQ retention, so it
  never occupies, evicts or fills the run channel's slot.
- **Archive.** An archived envelope outlives ``close`` of its run's channels
  (replay is a new execution against retained history, not the working queue).
'''
from __future__ import absolute_import, division, print_function

import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Mapping, Sequence

from ...core.errors import IncompatibleProfile
from .. import faults
from ..capabilities import LEDGER_NONE, LEDGER_WINDOW, RETENTION_INTEREST, RETENTION_LIMITS, MessagingCapabilities
from ..identity import owns
from ..messaging import (
    KIND_ABORT,
    KIND_DATA,
    KIND_DLQ,
    KIND_EOS,
    OVERFLOW_EVICT_OLDEST,
    SUBSCRIPTION_DATA,
    SUBSCRIPTION_EOS,
    ChannelId,
    ChannelSpec,
    Completed,
    Delivery,
    DeliveryToken,
    Envelope,
    LeaseObservation,
    MessagingBackend,
    Retry,
    Settlement,
    SubscriptionId,
    SubscriptionObservation,
    SubscriptionSpec,
    Terminal,
    VerifiedChannel,
    VerifiedSubscription,
)
from ..observation import ObservationLog
from ..outcomes import (
    DURABILITY_MEMORY,
    DURABILITY_STREAM,
    Accepted,
    CleanupObservation,
    Observation,
    PublicationOutcome,
    PublicationUnknown,
    PublicationUnresolvable,
    Rejected,
    SettleConfirmed,
    SettlementOutcome,
    SettleStale,
    SettleUnknown,
    known,
    unknown,
)
from .clock import FakeClock

IMMUTABLE_CHANNEL_FIELDS = ('retention', 'replicas', 'persistence')
MUTABLE_CHANNEL_FIELDS = ('max_msgs', 'max_bytes', 'max_age_seconds', 'overflow', 'dedup_window_seconds',
                          'per_subject_limits')
IMMUTABLE_SUBSCRIPTION_FIELDS = ('competing',)
MUTABLE_SUBSCRIPTION_FIELDS = ('ack_wait_seconds', 'max_deliver', 'item_credit', 'byte_credit')

@dataclass
class _Perspective:
    '''One subscription's view of one stored message.'''
    attempt : int = 0
    leased_until : float | None = None
    next_available_at : float = 0.0
    settled : str | None = None          # 'completed' | 'terminal'
    generation : str = ''
    exhausted : bool = False

@dataclass
class _Stored:
    sequence : int
    envelope : Envelope
    stored_at : float
    subject_key : str
    views : dict[SubscriptionId, _Perspective] = field(default_factory = dict)

@dataclass
class _Subscription:
    spec : SubscriptionSpec
    cursor : int = 0
    dropped : int = 0
    rejected_publications : int = 0
    queue : list[Delivery] = field(default_factory = list)   # core-only client queue
    generation : str = 'g1'

@dataclass
class _Channel:
    spec : ChannelSpec
    messages : list[_Stored] = field(default_factory = list)
    next_sequence : int = 1
    subscriptions : dict[SubscriptionId, _Subscription] = field(default_factory = dict)
    dedup : dict[str, tuple[float, int]] = field(default_factory = dict)
    held : list[tuple[Envelope, float]] = field(default_factory = list)
    accepting : bool = True
    rejected_publications : int = 0
    dropped : int = 0
    bytes_stored : int = 0
    control : dict[str, tuple[bytes, int]] = field(default_factory = dict)

def _visible(subscription : SubscriptionId, subject_key : str) -> bool:
    '''Whether a stored message rides the subject ``subscription`` filters to (a durable sees one subject of its stream).'''
    if subscription.kind == SUBSCRIPTION_EOS:
        return subject_key in (KIND_EOS, KIND_ABORT)
    if subscription.kind == SUBSCRIPTION_DATA:
        return subject_key.startswith(KIND_DATA)
    return subject_key == subscription.kind

class MemoryMessagingBackend(MessagingBackend):
    '''
    - Arguments:
        - clock: the fake clock every timer reads.
        - core_only: model Core NATS (no retention, no leases, client queues).
        - latest_per_key: advertise and implement per-key latest-value slots.
        - durable_control / archive: advertise the optional profiles (MSG-018, MSG-024).
        - client_queue_limit: core-only slow-consumer bound per subscription.
        - log: an observation log to record publish/deliver/settle/drop events into.
    '''
    def __init__(self, clock : FakeClock | None = None, core_only : bool = False,
                 latest_per_key : bool = False, durable_control : bool = False, archive : bool = False,
                 archive_horizon_seconds : float = 3600.0, dedup_window_seconds : int = 120,
                 replication_factor : int = 1, persistent : bool = False,
                 max_payload_bytes : int = 8 * 1024 * 1024, mixed_retention : bool = False,
                 client_queue_limit : int = 64, log : ObservationLog | None = None) -> None:
        self._clock = clock or FakeClock()
        self._core_only = core_only
        self._latest_per_key = latest_per_key
        self._durable_control = durable_control
        self._archive = archive
        self._archive_horizon = archive_horizon_seconds
        self._dedup_window = dedup_window_seconds
        self._replication = replication_factor
        self._persistent = persistent
        self._max_payload = max_payload_bytes
        self._mixed = mixed_retention
        self._queue_limit = client_queue_limit
        self._log = log
        self._channels : dict[ChannelId, _Channel] = {}
        # Kept apart from the channels so a run's teardown cannot erase its history (MSG-024).
        self._archives : dict[ChannelId, list[tuple[Envelope, float]]] = {}
        self._failed_observations : dict[SubscriptionId, str] = {}
        self._lock = threading.RLock()
        self._generation = 1

    # -- capabilities -------------------------------------------------------------------

    def capabilities(self) -> MessagingCapabilities:
        if self._core_only:
            return MessagingCapabilities(
                adapter = 'memory-core', version = 'model', retained_backlog = False,
                recoverable_delivery = False, latest_per_key = False, dedup_window_seconds = None,
                publication_ledger = LEDGER_NONE, replication_factor = known(1),
                persistent_storage = known(False), max_payload_bytes = known(self._max_payload),
                credit_resizable = False, control_shares_data_slot = False)
        return MessagingCapabilities(
            adapter = 'memory-jetstream', version = 'model', retained_backlog = True,
            recoverable_delivery = True, latest_per_key = self._latest_per_key,
            dedup_window_seconds = self._dedup_window, publication_ledger = LEDGER_WINDOW,
            replication_factor = known(self._replication), persistent_storage = known(self._persistent),
            max_payload_bytes = known(self._max_payload), credit_resizable = True,
            control_shares_data_slot = True, durable_control = self._durable_control,
            archive = self._archive, mixed_retention_per_channel = self._mixed)

    # -- provisioning -------------------------------------------------------------------

    def ensure_channel(self, spec : ChannelSpec, operation_id : str) -> VerifiedChannel:
        faults.barrier('provision.channel.before', channel = spec.id, op_id = operation_id)
        with self._lock:
            existing = self._channels.get(spec.id)
            if existing is None:
                self._channels[spec.id] = _Channel(spec)
                effective = _channel_effective(spec)
                faults.barrier('provision.channel.after', channel = spec.id, op_id = operation_id)
                return VerifiedChannel(spec, effective, ())
            mismatches = tuple(f for f in IMMUTABLE_CHANNEL_FIELDS
                               if getattr(existing.spec, f) != getattr(spec, f))
            if mismatches:
                raise IncompatibleProfile(
                    f'channel {spec.id.node!r} already exists with a different '
                    f'{", ".join(mismatches)}: {_channel_effective(existing.spec)}',
                    remedy = 'Tear the old run down, or use a new run id; immutable channel '
                             'settings cannot be reconciled in place.',
                    channel = spec.id.node, fields = list(mismatches))
            converged = {f: getattr(spec, f) for f in MUTABLE_CHANNEL_FIELDS}
            existing.spec = spec
            faults.barrier('provision.channel.after', channel = spec.id, op_id = operation_id)
            return VerifiedChannel(spec, {**_channel_effective(spec), 'updated': sorted(converged)}, ())

    def ensure_subscription(self, spec : SubscriptionSpec, operation_id : str) -> VerifiedSubscription:
        faults.barrier('provision.subscription.before', subscription = spec.id, op_id = operation_id)
        with self._lock:
            channel = self._channels.get(spec.id.channel)
            if channel is None:
                raise IncompatibleProfile(f'no channel {spec.id.channel.node!r} to subscribe to',
                                          remedy = 'Provision the channel first.')
            existing = channel.subscriptions.get(spec.id)
            if existing is None:
                channel.subscriptions[spec.id] = _Subscription(spec, cursor = channel.next_sequence
                                                               if self._core_only else 0)
                faults.barrier('provision.subscription.after', subscription = spec.id, op_id = operation_id)
                return VerifiedSubscription(spec, _subscription_effective(spec), ())
            mismatches = tuple(f for f in IMMUTABLE_SUBSCRIPTION_FIELDS
                               if getattr(existing.spec, f) != getattr(spec, f))
            if mismatches:
                raise IncompatibleProfile(
                    f'subscription {spec.id.consumer_node!r} on {spec.id.channel.node!r} exists with a '
                    f'different {", ".join(mismatches)}',
                    remedy = 'Delete the stale subscription or use a new run id.')
            existing.spec = spec
            existing.generation = f'g{self._bump()}'
            faults.barrier('provision.subscription.after', subscription = spec.id, op_id = operation_id)
            return VerifiedSubscription(spec, _subscription_effective(spec), ())

    def _bump(self) -> int:
        self._generation += 1
        return self._generation

    # -- publication ----------------------------------------------------------------------

    def publish(self, envelope : Envelope, deadline : float) -> PublicationOutcome:
        pid = envelope.publication_id
        if envelope.size > self._max_payload:
            return Rejected(pid, f'payload {envelope.size} exceeds max_payload {self._max_payload}', False)
        faults.barrier('publish.send.before', op_id = pid, channel = envelope.channel)
        with self._lock:
            channel = self._channels.get(envelope.channel)
            if channel is None:
                return Rejected(pid, 'no such channel', False)
            if not channel.accepting:
                channel.held.append((envelope, self._clock.now()))
                self._emit('publish', op_id = pid, status = 'unknown', reason = 'acceptance paused')
                return PublicationUnknown(pid, 'broker acceptance paused past the caller deadline')
            outcome = self._store(channel, envelope)
        faults.barrier('publish.send.after', op_id = pid, channel = envelope.channel)
        hit = faults.barrier('publish.receipt.before', op_id = pid, channel = envelope.channel)
        if hit.drop_response:
            self._emit('publish', op_id = pid, status = 'unknown', reason = 'receipt dropped')
            return PublicationUnknown(pid, 'publication receipt lost')
        faults.barrier('publish.receipt.after', op_id = pid, channel = envelope.channel)
        return outcome

    def _store(self, channel : _Channel, envelope : Envelope) -> PublicationOutcome:
        pid = envelope.publication_id
        now = self._clock.now()
        self._sweep(channel, now)
        subject_key = self._subject_key(channel, envelope)
        if self._core_only:
            delivered = 0
            for sub in channel.subscriptions.values():
                if not _visible(sub.spec.id, subject_key):
                    continue
                if len(sub.queue) >= self._queue_limit:
                    sub.dropped += 1
                    self._emit('drop', op_id = pid, consumer = sub.spec.id.consumer_node, reason = 'slow-consumer')
                    continue
                token = DeliveryToken(sub.spec.id, envelope.event_id, None, 1, sub.generation)
                sub.queue.append(Delivery(token, envelope.body, envelope.size, now, dict(envelope.headers)))
                delivered += 1
            if delivered == 0:
                channel.dropped += 1
            self._emit('publish', op_id = pid, status = 'accepted', boundary = DURABILITY_MEMORY)
            return Accepted(pid, None, False, DURABILITY_MEMORY)
        seen = channel.dedup.get(pid)
        if seen is not None and now - seen[0] < channel.spec.dedup_window_seconds:
            self._emit('publish', op_id = pid, status = 'accepted', duplicate = True)
            return Accepted(pid, seen[1], True, DURABILITY_STREAM)
        if envelope.kind == KIND_DLQ:
            # A dead letter is the flow's record, not the run channel's backlog: it is
            # stored for inspection but takes no slot and evicts nothing (STREAM-8).
            pass
        elif channel.spec.retention == RETENTION_LIMITS:
            limit_scope = [m for m in channel.messages if m.subject_key == subject_key] \
                if channel.spec.per_subject_limits else [m for m in channel.messages if m.subject_key != KIND_DLQ]
            over_count = len(limit_scope) >= channel.spec.max_msgs
            over_bytes = (channel.spec.max_bytes is not None
                          and channel.bytes_stored + envelope.size > channel.spec.max_bytes)
            if over_count or over_bytes:
                if channel.spec.overflow == OVERFLOW_EVICT_OLDEST:
                    victim = limit_scope[0] if limit_scope else channel.messages[0]
                    self._evict(channel, victim, 'limits')
                else:
                    channel.rejected_publications += 1
                    self._emit('publish', op_id = pid, status = 'rejected', reason = 'maximum messages')
                    return Rejected(pid, 'maximum messages exceeded', True)
        else:
            if channel.spec.max_bytes is not None and channel.bytes_stored + envelope.size > channel.spec.max_bytes:
                channel.rejected_publications += 1
                return Rejected(pid, 'maximum bytes exceeded', True)
            if sum(1 for m in channel.messages if m.subject_key != KIND_DLQ) >= channel.spec.max_msgs:
                channel.rejected_publications += 1
                self._emit('publish', op_id = pid, status = 'rejected', reason = 'maximum messages')
                return Rejected(pid, 'maximum messages exceeded', True)
        stored = _Stored(channel.next_sequence, envelope, now, subject_key)
        channel.next_sequence += 1
        channel.messages.append(stored)
        if envelope.kind != KIND_DLQ:
            channel.bytes_stored += envelope.size
        channel.dedup[pid] = (now, stored.sequence)
        if self._archive and envelope.kind != KIND_DLQ:
            self._archives.setdefault(channel.spec.id, []).append((envelope, now))
        self._emit('publish', op_id = pid, status = 'accepted', sequence = stored.sequence,
                   boundary = DURABILITY_STREAM)
        return Accepted(pid, stored.sequence, False, DURABILITY_STREAM)

    def _subject_key(self, channel : _Channel, envelope : Envelope) -> str:
        if self._latest_per_key and envelope.kind == 'data' and envelope.partition_key is not None:
            return f'data:{envelope.partition_key}'
        return envelope.kind

    def observe_publication(self, envelope : Envelope) -> PublicationOutcome:
        pid = envelope.publication_id
        with self._lock:
            channel = self._channels.get(envelope.channel)
            if channel is None or self._core_only:
                return PublicationUnresolvable(pid, 'no publication ledger')
            if any(e.publication_id == pid for e, _ in channel.held):
                return PublicationUnknown(pid, 'held by paused acceptance')
            seen = channel.dedup.get(pid)
            if seen is not None and self._clock.now() - seen[0] < channel.spec.dedup_window_seconds:
                return Accepted(pid, seen[1], True, DURABILITY_STREAM)
            return PublicationUnresolvable(pid, 'outside the deduplication window')

    # -- publication ambiguity controls (for tests) --------------------------------------------

    def pause_acceptance(self, channel_id : ChannelId) -> None:
        with self._lock:
            self._channels[channel_id].accepting = False

    def resume_acceptance(self, channel_id : ChannelId) -> list[PublicationOutcome]:
        '''Store every held publication (late acceptance); returns their outcomes in order.'''
        with self._lock:
            channel = self._channels[channel_id]
            channel.accepting = True
            outcomes = [self._store(channel, envelope) for envelope, _ in channel.held]
            channel.held = []
            return outcomes

    def cancel_publication(self, channel_id : ChannelId, publication_id : str) -> bool:
        '''Definitely cancel a held publication. False when it was not held (it may already be stored).'''
        with self._lock:
            channel = self._channels[channel_id]
            before = len(channel.held)
            channel.held = [(e, t) for e, t in channel.held if e.publication_id != publication_id]
            return len(channel.held) != before

    # -- delivery -----------------------------------------------------------------------------

    def receive(self, subscription : SubscriptionId, item_credit : int, byte_credit : int,
                deadline : float) -> list[Delivery]:
        with self._lock:
            channel = self._channels.get(subscription.channel)
            if channel is None or subscription not in channel.subscriptions:
                return []
            sub = channel.subscriptions[subscription]
            now = self._clock.now()
            if self._core_only:
                out : list[Delivery] = []
                size = 0
                while sub.queue and len(out) < item_credit and size + sub.queue[0].size <= byte_credit:
                    out.append(sub.queue.pop(0))
                    size += out[-1].size
                return out
            self._sweep(channel, now)
            # Two bounds, kept apart: the subscription's credit caps what is leased
            # across every receiver competing on it (the broker's max_ack_pending);
            # ``item_credit`` is how many this receiver takes now. Folding them into
            # one minimum let a single receiver's unsettled input starve its siblings.
            credit = sub.spec.item_credit
            leased = sum(1 for m in channel.messages
                         if (v := m.views.get(subscription)) is not None
                         and v.leased_until is not None and v.leased_until > now and v.settled is None)
            deliveries : list[Delivery] = []
            bytes_out = 0
            for stored in channel.messages:
                if not _visible(subscription, stored.subject_key):
                    continue
                if leased + len(deliveries) >= credit or len(deliveries) >= item_credit:
                    break
                view = stored.views.setdefault(subscription, _Perspective(generation = sub.generation))
                if view.settled is not None or view.exhausted:
                    continue
                if view.leased_until is not None and view.leased_until > now:
                    continue
                if view.next_available_at > now:
                    continue
                if bytes_out + stored.envelope.size > byte_credit and deliveries:
                    break
                cap = sub.spec.max_deliver
                if cap != -1 and view.attempt >= cap:
                    if not view.exhausted:
                        view.exhausted = True
                        self._emit('exhausted', op_id = stored.envelope.publication_id,
                                   consumer = subscription.consumer_node, attempts = view.attempt)
                    continue
                view.attempt += 1
                view.leased_until = now + sub.spec.ack_wait_seconds
                view.generation = sub.generation
                token = DeliveryToken(subscription, stored.envelope.event_id, stored.sequence,
                                      view.attempt, sub.generation)
                deliveries.append(Delivery(token, stored.envelope.body, stored.envelope.size, now,
                                           dict(stored.envelope.headers)))
                bytes_out += stored.envelope.size
                self._emit('deliver', op_id = stored.envelope.publication_id, event_id = stored.envelope.event_id,
                           consumer = subscription.consumer_node, attempt = view.attempt,
                           generation = sub.generation)
            return deliveries

    def renew(self, token : DeliveryToken) -> LeaseObservation:
        with self._lock:
            stored, view = self._find(token)
            if stored is None or view is None:
                return LeaseObservation(token, False, None, 'no such delivery')
            if view.attempt != token.attempt or view.settled is not None:
                return LeaseObservation(token, False, view.leased_until, 'stale attempt')
            sub = self._channels[token.subscription.channel].subscriptions[token.subscription]
            view.leased_until = self._clock.now() + sub.spec.ack_wait_seconds
            return LeaseObservation(token, True, view.leased_until)

    def settle(self, token : DeliveryToken, outcome : Settlement, settlement_id : str) -> SettlementOutcome:
        if isinstance(outcome, Terminal) and not outcome.record_ref:
            raise ValueError('Terminal settlement requires a durable record reference')
        faults.barrier('settle.before', op_id = token.message_id, consumer = token.subscription.consumer_node,
                       attempt = token.attempt, outcome = type(outcome).__name__)
        with self._lock:
            if self._core_only:
                return SettleConfirmed(token, settlement_id)
            stored, view = self._find(token)
            if stored is None or view is None:
                return SettleUnknown(token, 'delivery no longer exists (evicted or already reclaimed)')
            if view.attempt != token.attempt or view.generation != token.generation:
                self._emit('settle', op_id = stored.envelope.publication_id, attempt = token.attempt,
                           status = 'stale', current_attempt = view.attempt)
                return SettleStale(token, view.attempt, view.generation)
            channel = self._channels[token.subscription.channel]
            if isinstance(outcome, Completed):
                view.settled = 'completed'
                view.leased_until = None
            elif isinstance(outcome, Terminal):
                view.settled = 'terminal'
                view.leased_until = None
            elif isinstance(outcome, Retry):
                view.leased_until = None
                view.next_available_at = self._clock.now() + (outcome.delay_seconds or 0.0)
            self._emit('settle', op_id = stored.envelope.publication_id, attempt = token.attempt,
                       status = type(outcome).__name__.lower(), settlement_id = settlement_id)
            self._reclaim_if_settled(channel, stored)
        hit = faults.barrier('settle.after', op_id = token.message_id, consumer = token.subscription.consumer_node)
        if hit.drop_response:
            return SettleUnknown(token, 'settlement response lost')
        return SettleConfirmed(token, settlement_id)

    def _find(self, token : DeliveryToken) -> tuple[_Stored | None, _Perspective | None]:
        channel = self._channels.get(token.subscription.channel)
        if channel is None:
            return None, None
        for stored in channel.messages:
            if stored.sequence == token.stream_sequence:
                return stored, stored.views.get(token.subscription)
        return None, None

    def _reclaim_if_settled(self, channel : _Channel, stored : _Stored) -> None:
        if channel.spec.retention != RETENTION_INTEREST:
            return
        required = [s for s in (channel.spec.required_subscriptions or tuple(channel.subscriptions))
                    if _visible(s, stored.subject_key)]
        if not required:
            return
        if all((v := stored.views.get(s)) is not None and v.settled is not None for s in required):
            channel.messages.remove(stored)
            if stored.envelope.kind != KIND_DLQ:
                channel.bytes_stored -= stored.envelope.size
            self._emit('reclaim', op_id = stored.envelope.publication_id)

    # -- observation ----------------------------------------------------------------------------

    def fail_observation(self, subscription : SubscriptionId, reason : str | None = 'timeout') -> None:
        '''Make ``observe_subscription`` return ``Unknown(reason)`` until called with ``None``.'''
        with self._lock:
            if reason is None:
                self._failed_observations.pop(subscription, None)
            else:
                self._failed_observations[subscription] = reason

    def observe_subscription(self, subscription : SubscriptionId) -> Observation[SubscriptionObservation]:
        try:
            faults.barrier('observe.subscription.before', subscription = subscription)
        except Exception as e:  # noqa: BLE001 — an injected API failure is reported, not raised
            return unknown('api', f'{type(e).__name__}: {e}')
        with self._lock:
            if subscription in self._failed_observations:
                return unknown(self._failed_observations[subscription], 'observation injected to fail')
            channel = self._channels.get(subscription.channel)
            if channel is None or subscription not in channel.subscriptions:
                return unknown('unreachable', 'no such subscription')
            sub = channel.subscriptions[subscription]
            now = self._clock.now()
            if self._core_only:
                return known(SubscriptionObservation(len(sub.queue), 0, 0, sub.dropped,
                                                     channel.rejected_publications, now), sub.generation)
            self._sweep(channel, now)
            available = leased = unresolved = 0
            for stored in channel.messages:
                if not _visible(subscription, stored.subject_key):
                    continue
                view = stored.views.get(subscription)
                if view is None:
                    available += 1
                    continue
                if view.settled is not None:
                    continue
                if view.exhausted or (sub.spec.max_deliver != -1 and view.attempt >= sub.spec.max_deliver
                                      and (view.leased_until is None or view.leased_until <= now)):
                    unresolved += 1
                elif view.leased_until is not None and view.leased_until > now:
                    leased += 1
                else:
                    available += 1
            return known(SubscriptionObservation(available, leased, unresolved, sub.dropped,
                                                 channel.rejected_publications, now), sub.generation)

    # -- teardown ----------------------------------------------------------------------------------

    def close(self, owned : Sequence[ChannelId], expected_generation : str) -> CleanupObservation:
        removed : list[str] = []
        remaining : list[str] = []
        try:
            faults.barrier('delete.before', owned = [c.node for c in owned])
        except Exception as e:  # noqa: BLE001 — a failed listing/delete is an incomplete cleanup
            return CleanupObservation(False, (), tuple(c.node for c in owned), f'{type(e).__name__}: {e}')
        with self._lock:
            for channel_id in owned:
                channel = self._channels.get(channel_id)
                if channel is None:
                    removed.append(channel_id.node)
                    continue
                if not owns(channel.spec.owner_labels, channel_id.flow_id, channel_id.run_id):
                    remaining.append(channel_id.node)
                    continue
                del self._channels[channel_id]
                removed.append(channel_id.node)
        faults.barrier('delete.after', removed = removed)
        return CleanupObservation(not remaining, tuple(removed), tuple(remaining),
                                  '' if not remaining else 'ownership mismatch')

    # -- optional profiles: durable control and archive -------------------------------------------

    def control_state(self, channel_id : ChannelId, key : str) -> tuple[bytes | None, str | None]:
        with self._lock:
            entry = self._channels[channel_id].control.get(key)
            return (None, None) if entry is None else (entry[0], str(entry[1]))

    def commit_control(self, channel_id : ChannelId, key : str, expected_version : str | None,
                       value : bytes) -> bool:
        if not self._durable_control:
            raise IncompatibleProfile('durable control is not advertised by this backend',
                                      remedy = 'Construct the backend with durable_control=True.')
        with self._lock:
            control = self._channels[channel_id].control
            entry = control.get(key)
            current = None if entry is None else str(entry[1])
            if current != expected_version:
                return False
            control[key] = (value, (entry[1] + 1) if entry else 1)
            return True

    def replay(self, channel_id : ChannelId, event_id : str) -> Envelope | None:
        '''The archived envelope for ``event_id`` while within the archive horizon; None once expired.'''
        if not self._archive:
            raise IncompatibleProfile('replay_archive is not advertised by this backend',
                                      remedy = 'Construct the backend with archive=True.')
        with self._lock:
            now = self._clock.now()
            for envelope, at in self._archives.get(channel_id, ()):
                if envelope.event_id == event_id and now - at < self._archive_horizon:
                    return envelope
            return None

    # -- introspection for tests ----------------------------------------------------------------------

    def stored(self, channel_id : ChannelId) -> list[Envelope]:
        with self._lock:
            channel = self._channels.get(channel_id)
            return [] if channel is None else [m.envelope for m in channel.messages]

    def channel_ids(self) -> list[ChannelId]:
        with self._lock:
            return list(self._channels)

    def subscription_ids(self, channel_id : ChannelId) -> list[SubscriptionId]:
        '''The logical subscriptions bound on a channel — the inventory a teardown or a replacement worker reads.'''
        with self._lock:
            channel = self._channels.get(channel_id)
            return [] if channel is None else list(channel.subscriptions)

    # -- internals --------------------------------------------------------------------------------------

    def _sweep(self, channel : _Channel, now : float) -> None:
        window = channel.spec.dedup_window_seconds
        for pid, (at, _) in list(channel.dedup.items()):
            if now - at >= window:
                del channel.dedup[pid]
        if channel.spec.max_age_seconds is not None:
            for stored in list(channel.messages):
                if now - stored.stored_at >= channel.spec.max_age_seconds:
                    self._evict(channel, stored, 'max_age')
        if self._archive and channel.spec.id in self._archives:
            self._archives[channel.spec.id] = [(e, at) for e, at in self._archives[channel.spec.id]
                                               if now - at < self._archive_horizon]

    def _evict(self, channel : _Channel, stored : _Stored, reason : str) -> None:
        channel.messages.remove(stored)
        if stored.envelope.kind != KIND_DLQ:
            channel.bytes_stored -= stored.envelope.size
        channel.dropped += 1
        for sub_id, sub in channel.subscriptions.items():
            if not _visible(sub_id, stored.subject_key):
                continue
            view = stored.views.get(sub_id)
            if view is None or view.settled is None:
                sub.dropped += 1
        self._emit('drop', op_id = stored.envelope.publication_id, event_id = stored.envelope.event_id,
                   reason = reason)

    def _emit(self, kind : str, **fields : Any) -> None:
        if self._log is not None:
            self._log.emit(kind, **fields)

def _channel_effective(spec : ChannelSpec) -> dict[str, Any]:
    return {f: getattr(spec, f) for f in IMMUTABLE_CHANNEL_FIELDS + MUTABLE_CHANNEL_FIELDS}

def _subscription_effective(spec : SubscriptionSpec) -> dict[str, Any]:
    return {f: getattr(spec, f) for f in IMMUTABLE_SUBSCRIPTION_FIELDS + MUTABLE_SUBSCRIPTION_FIELDS}

def make_channel(flow_id : str, run_id : str, node : str, profile : str, retention : str,
                 required : Sequence[SubscriptionId] = (), max_msgs : int = 10_000,
                 max_bytes : int | None = None, max_age_seconds : float | None = None,
                 overflow : str = OVERFLOW_EVICT_OLDEST, dedup_window_seconds : int = 120,
                 replicas : int = 1, persistence : bool = False, per_subject_limits : bool = False,
                 owner_labels : Mapping[str, str] | None = None) -> ChannelSpec:
    '''Convenience constructor used by tests and by the composition of the reference runtime.'''
    from ..identity import owner_labels as labels_for
    cid = ChannelId(flow_id, run_id, node)
    return ChannelSpec(cid, profile, retention, max_msgs, max_bytes, max_age_seconds, overflow,
                       dedup_window_seconds, replicas, persistence, tuple(required),
                       owner_labels if owner_labels is not None else labels_for(flow_id, run_id, node, 'stream'),
                       per_subject_limits)

def make_subscription(channel : ChannelId, consumer : str, competing : bool = True, partition : int | None = None,
                      ack_wait_seconds : float = 30.0, max_deliver : int = 4, item_credit : int = 8,
                      byte_credit : int = 64 * 1024 * 1024, kind : str = 'data') -> SubscriptionSpec:
    sid = SubscriptionId(channel, consumer, partition, kind)
    return SubscriptionSpec(sid, competing, ack_wait_seconds, max_deliver, item_credit, byte_credit)

def make_envelope(channel : ChannelId, publication_id : str, body : bytes = b'x', event_id : str | None = None,
                  kind : str = 'data', partition_key : str | None = None, event_ts : float | None = None,
                  source_epoch : str | None = None, source_offset : int | None = None,
                  headers : Mapping[str, str] | None = None) -> Envelope:
    return Envelope(channel, publication_id, dict(headers or {}), body, len(body), event_id or publication_id,
                    partition_key, event_ts, source_epoch, source_offset, 4, (), kind)

EvictionCallback = Callable[[Envelope, str], None]
