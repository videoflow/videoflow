'''
Drivers for the messaging-family conformance cases: one interface, two backends.

An oracle in ``test_msg_*.py`` is a plain function over a *driver* — the reference
``MemoryMessagingBackend`` under a fake clock, or the real
``JetStreamMessagingBackend`` on the dev broker — so the same assertions decide the
model-level variant, the broker-level primary and the negative control. The driver
owns only what differs between the two: how a receiver (a worker process) comes
into being and dies, how time passes, how a conflicting resource is seeded from
outside, and how the flow's dead letters are read back.

Vocabulary: a *receiver* is one worker process — its own adapter instance on the
broker, the shared model in memory; ``kill`` abandons whatever the receiver holds
without settling it; ``wait`` advances the fake clock or sleeps; ``until`` polls a
condition while time passes. Everything the driver provisioned is torn down by
exact ownership in ``close`` (``_brokers.delete_run`` / ``delete_dlq``), whatever
happened before.

The messenger helpers run the real ``NATSMessenger`` — the delivery ladder, the
dead-letter publish, the poison classification — over either backend, which is
what lets a negative control for a defect in ``nats_messenger.py`` run against
the model in milliseconds.
'''
from __future__ import absolute_import, division, print_function

import contextlib
import hashlib
import threading
import time
import types
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence

from _brokers import delete_dlq, delete_run, publish_raw, run_async, stream_state

from videoflow.backends.capabilities import (
    LIVE_LATEST,
    RELIABLE_WORK,
    RETENTION_INTEREST,
    RETENTION_LIMITS,
    MessagingCapabilities,
)
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.messaging import MemoryMessagingBackend
from videoflow.backends.messaging import (
    KIND_DATA,
    KIND_DLQ,
    OVERFLOW_EVICT_OLDEST,
    OVERFLOW_REJECT,
    SUBSCRIPTION_DATA,
    ChannelId,
    ChannelSpec,
    Delivery,
    DeliveryToken,
    Envelope,
    MessagingBackend,
    Retry,
    Settlement,
    SubscriptionId,
    SubscriptionObservation,
    SubscriptionSpec,
)
from videoflow.backends.outcomes import Observation, PublicationOutcome, SettlementOutcome, known
from videoflow.core.compiler import NodeSpec
from videoflow.core.constants import BATCH, REALTIME
from videoflow.messaging import nats_messenger, topology
from videoflow.messaging.jetstream_backend import JetStreamMessagingBackend, channel_spec_for
from videoflow.messaging.nats_messenger import NATSMessenger
from videoflow.wire.serialization import MSG_TYPE_DATA, BlobStore, derive_message_id, encode_envelope

DEFAULT_BYTE_CREDIT = 64 * 1024 * 1024


class StubNode:
    '''The minimum a messenger needs from a node: a name.'''
    def __init__(self, name : str) -> None:
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass


def spec(name : str, parents : List[str], kind : str, has_children : bool, nb_tasks : int = 1,
         partition_by : Optional[str] = None, delivery : Optional[dict] = None) -> NodeSpec:
    '''A real NodeSpec — provisioning reads name/parents/nb_tasks/partition_by/delivery.'''
    return NodeSpec(name = name, node_class = 'videoflow.processors.basic.IdentityProcessor',
                    params = {}, parents = parents, kind = kind, has_children = has_children,
                    nb_tasks = nb_tasks, device_type = 'cpu', is_finite = True,
                    partition_by = partition_by, delivery = delivery)


def digest(data : bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]


@dataclass
class Bound:
    '''One receiver's binding of one logical subscription.'''
    receiver : str
    backend : MessagingBackend
    subscription : SubscriptionId
    spec : SubscriptionSpec
    effective : Dict[str, Any]


class Driver:
    '''The interface every oracle drives; ``MemoryDriver`` and ``JetStreamDriver`` fill it in.'''
    name : str = ''
    flow_id : str = ''
    run_id : str = ''
    clock : Optional[FakeClock] = None

    # -- provisioning ----------------------------------------------------------------
    def capabilities(self) -> MessagingCapabilities:
        raise NotImplementedError

    def channel(self, node : str, profile : str, retention : str, max_msgs : int = 10_000,
                max_bytes : int | None = None, max_age : float | None = None, per_subject : bool = False,
                dedup_window : int = topology.DUPLICATE_WINDOW_SECONDS,
                required : Sequence[SubscriptionId] = ()) -> ChannelId:
        raise NotImplementedError

    def channel_spec(self, node : str, profile : str, retention : str, max_msgs : int = 10_000,
                     max_bytes : int | None = None, max_age : float | None = None, per_subject : bool = False,
                     dedup_window : int = topology.DUPLICATE_WINDOW_SECONDS,
                     required : Sequence[SubscriptionId] = (), persistent : bool = False) -> ChannelSpec:
        cid = ChannelId(self.flow_id, self.run_id, node)
        return ChannelSpec(cid, profile, retention, max_msgs, max_bytes, max_age,
                           OVERFLOW_EVICT_OLDEST if retention == RETENTION_LIMITS else OVERFLOW_REJECT,
                           dedup_window, 1, persistent, tuple(required),
                           topology.owner_labels(self.flow_id, self.run_id, node = node, kind = 'stream'), per_subject)

    def channel_ids(self) -> List[ChannelId]:
        raise NotImplementedError

    def channel_verified(self, node : str, profile : str, retention : str, max_msgs : int = 10_000,
                         max_bytes : int | None = None, per_subject : bool = False, persistent : bool = False) -> Any:
        '''``ensure_channel`` itself: the ``VerifiedChannel`` (spec, effective, mismatches) the adapter read back.'''
        raise NotImplementedError

    def provision_subscription(self, channel : ChannelId, consumer : str, ack_wait : float = 2.0,
                               max_deliver : int = 4, credit : int = 8, partition : int | None = None) -> SubscriptionId:
        '''The logical subscription, created with no receiver online (the deploy-time durable).'''
        raise NotImplementedError

    def subscription_effective(self, channel : ChannelId, consumer : str, partition : int | None = None) -> Dict[str, Any]:
        '''The subscription's configuration as the backend holds it now (``max_ack_pending``, ``ack_wait``, ``max_deliver``).'''
        raise NotImplementedError

    def subscriptions(self, channel : ChannelId) -> List[str]:
        '''Inventory: the names of the data subscriptions bound on a channel.'''
        raise NotImplementedError

    def retained(self, channel : ChannelId) -> int:
        '''Messages the channel currently holds (dead letters excluded).'''
        raise NotImplementedError

    def retained_floor(self, channel : ChannelId) -> int:
        '''The lowest sequence the channel still holds: everything below it was evicted or reclaimed.'''
        raise NotImplementedError

    def retained_ids(self, channel : ChannelId, kind : str = KIND_DATA) -> List[str]:
        '''Publication ids of the messages of ``kind`` the channel holds, oldest first (model only).'''
        raise NotImplementedError

    def effective_dedup_window(self, channel : ChannelId) -> float:
        '''The deduplication window the backend read back for the channel, in seconds.'''
        effective = self._verified.get(channel)
        assert effective is not None, f'{channel} was not provisioned through this driver'
        return float(effective.get('duplicate_window', effective.get('dedup_window_seconds', 0)) or 0)

    # -- receivers ---------------------------------------------------------------------
    def receiver(self, name : str, prefetch : int = 1, keepalive : bool = False) -> MessagingBackend:
        raise NotImplementedError

    def bind(self, channel : ChannelId, consumer : str, receiver : str = 'r0', ack_wait : float = 2.0,
             max_deliver : int = 4, credit : int = 8, byte_credit : int = DEFAULT_BYTE_CREDIT,
             partition : int | None = None, prefetch : int = 1, kind : str = SUBSCRIPTION_DATA,
             instance : str | None = None) -> Bound:
        backend = self.receiver(receiver, prefetch = prefetch)
        sub = SubscriptionId(channel, consumer, partition, kind, instance)
        spec_ = SubscriptionSpec(sub, partition is None, ack_wait, max_deliver, credit, byte_credit)
        verified = backend.ensure_subscription(spec_, f'{receiver}:{uuid.uuid4().hex[:6]}')
        effective = getattr(self, '_effective', None)
        if effective is not None:
            effective[verified.spec.id] = dict(verified.effective)
        return Bound(receiver, backend, verified.spec.id, verified.spec, dict(verified.effective))

    def kill(self, receiver : str) -> None:
        '''The receiver crashes: nothing it held is settled or handed back; its leases lapse.'''
        raise NotImplementedError

    def retire(self, receiver : str) -> None:
        '''The receiver is scaled down gracefully: every delivery it still held is handed back at once.'''
        raise NotImplementedError

    # -- traffic -------------------------------------------------------------------------
    def envelope(self, channel : ChannelId, pid : str, body : bytes = b'x', kind : str = KIND_DATA,
                 partition_key : str | None = None, headers : Dict[str, str] | None = None,
                 event_id : str | None = None, event_ts : float | None = None) -> Envelope:
        hdrs = {'Nats-Msg-Id': pid, **(headers or {})}
        return Envelope(channel, pid, hdrs, body, len(body), event_id or pid, partition_key, event_ts,
                        None, None, 4, (), kind)

    def publish(self, channel : ChannelId, pid : str, body : bytes = b'x', kind : str = KIND_DATA,
                partition_key : str | None = None, headers : Dict[str, str] | None = None,
                event_id : str | None = None, deadline : float | None = None,
                event_ts : float | None = None) -> PublicationOutcome:
        raise NotImplementedError

    def receive(self, bound : Bound, timeout : float = 1.0, item_credit : int = 1,
                byte_credit : int = DEFAULT_BYTE_CREDIT) -> List[Delivery]:
        raise NotImplementedError

    def settle(self, bound : Bound, token : DeliveryToken, outcome : Settlement,
               settlement_id : str | None = None) -> SettlementOutcome:
        return bound.backend.settle(token, outcome, settlement_id or f'{type(outcome).__name__}:{token.message_id}:{token.attempt}')

    def observe(self, bound : Bound) -> Observation[SubscriptionObservation]:
        return bound.backend.observe_subscription(bound.subscription)

    # -- time ---------------------------------------------------------------------------
    def wait(self, seconds : float) -> None:
        raise NotImplementedError

    def until(self, condition : Callable[[], bool], timeout : float, step : float = 0.1) -> bool:
        raise NotImplementedError

    def drain(self, bounds : Sequence[Bound], timeout : float, on_delivery : Callable[[Bound, Delivery], Settlement] | None = None,
              stop : Callable[[], bool] | None = None) -> Dict[str, List[str]]:
        '''
        Receive and settle on every binding until ``stop()`` (or the timeout);
        returns the message ids each receiver settled. ``on_delivery`` decides the
        settlement (``Completed`` by default).
        '''
        from videoflow.backends.messaging import Completed
        settled : Dict[str, List[str]] = {b.receiver: [] for b in bounds}

        def step_once() -> bool:
            progressed = False
            for bound in bounds:
                for delivery in self.receive(bound, timeout = 0.2):
                    progressed = True
                    outcome = on_delivery(bound, delivery) if on_delivery is not None else Completed()
                    self.settle(bound, delivery.token, outcome)
                    settled[bound.receiver].append(delivery.token.message_id)
            return progressed

        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if stop is not None and stop():
                break
            if not step_once():
                self.wait(0.2) if self.clock is not None else time.sleep(0.05)
        return settled

    # -- messengers ----------------------------------------------------------------------
    def messenger(self, node : str, parents : List[str], flow_type : str, max_retries : int = 3,
                  ack_wait : int = 2, delivery : dict | None = None, blob_store : BlobStore | None = None,
                  replica_id : int = 0, nb_tasks : int = 1, partition_by : str | None = None) -> NATSMessenger:
        raise NotImplementedError

    def receive_group(self, messenger : NATSMessenger, timeout : float = 30.0) -> dict:
        '''``receive_message`` bounded: a group that never arrives fails the test instead of hanging it.'''
        box : Dict[str, Any] = {}

        def run() -> None:
            try:
                box['group'] = messenger.receive_message()
            except BaseException as e:  # noqa: BLE001 — surfaced to the test thread
                box['error'] = e

        thread = threading.Thread(target = run, daemon = True, name = 'vf-conf-receive')
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            raise AssertionError(f'no input group reached the messenger within {timeout:.0f}s')
        if 'error' in box:
            raise box['error']
        return box['group']

    def publish_parent(self, parent : str, trace : str, seq : int, payload : Any,
                       headers : Dict[str, str] | None = None, blob_store : BlobStore | None = None,
                       inline_threshold : int | None = None, msg_id : str | None = None) -> str:
        '''Publish one data envelope on a parent's channel, as that parent would; returns its publication id.'''
        buf = encode_envelope(parent, self.flow_id, self.run_id, trace, seq, MSG_TYPE_DATA, None, payload,
                              blob_store = blob_store, inline_threshold = inline_threshold)
        mid = msg_id or derive_message_id(self.flow_id, self.run_id, parent, trace, seq, MSG_TYPE_DATA)
        self.publish_bytes(parent, buf, mid, headers)
        return mid

    def publish_bytes(self, parent : str, data : bytes, msg_id : str, headers : Dict[str, str] | None = None) -> None:
        '''Arbitrary bytes on a parent's data subject — poison specimens, foreign versions.'''
        raise NotImplementedError

    def dlq(self, node : str | None = None) -> List[Dict[str, Any]]:
        '''The flow's dead letters for this run (optionally one origin node): ``{'headers', 'data'}`` each.'''
        raise NotImplementedError

    def dlq_outage(self, node : str) -> None:
        '''Make dead-letter publication fail for ``node`` until ``dlq_restore``.'''
        raise NotImplementedError

    def dlq_restore(self, node : str) -> None:
        raise NotImplementedError

    def consumer_state(self, child : str, parent : str) -> Observation[tuple[int, int]]:
        '''``(pending, leased)`` of the child's data subscription on the parent, or Unknown.'''
        raise NotImplementedError

    def provision(self, specs : List[NodeSpec], flow_type : str, max_retries : int = 3) -> None:
        '''What ``topology.provision_flow`` does before any worker starts: channels, DLQ, one durable per edge.'''
        raise NotImplementedError

    # -- publication lifecycle ------------------------------------------------------------
    def observe_publication(self, channel : ChannelId, pid : str, body : bytes = b'x') -> PublicationOutcome:
        raise NotImplementedError

    def cancel_publication(self, channel : ChannelId, pid : str) -> bool:
        '''A definite cancellation of a pending send, or False when the transport cannot promise one.'''
        return False

    def stalled_acceptance(self, channel : ChannelId) -> Any:
        '''A context in which the broker accepts nothing until it ends (then everything held arrives at once).'''
        raise NotImplementedError

    def disconnected(self) -> Any:
        '''A context in which the broker is unreachable from every receiver and publisher of this driver.'''
        raise NotImplementedError

    def connected(self) -> bool:
        return True

    def drop_channel(self, channel : ChannelId) -> None:
        '''Remove the channel from under its publishers: the next publish is a definite refusal.'''
        raise NotImplementedError

    def last_retained(self, channel : ChannelId, kind : str = KIND_DATA) -> str | None:
        '''The publication id of the newest message of ``kind`` the channel holds.'''
        raise NotImplementedError

    # -- faults a driver can inject on the observation and resolution paths ---------------
    def deny_observation(self, bound : Bound, reason : str = 'timeout') -> None:
        '''Make the management read behind ``observe`` fail (a real client failure, not a barrier).'''
        raise NotImplementedError

    def restore_observation(self, bound : Bound) -> None:
        raise NotImplementedError

    def resolve_stranded(self, bound : Bound, delivery : Delivery) -> str:
        '''
        The operator's resolution of an exhausted, retained input: a durable
        terminal record (a dead letter under the delivery's identity) and the
        input's removal from the working channel. Returns the record reference.
        '''
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


# -- the model ------------------------------------------------------------------------------

class MemoryDriver(Driver):
    name = 'memory'

    def __init__(self, flow_id : str = 'f', run_id : str = 'r', clock : FakeClock | None = None,
                 messenger_poll : float = 0.05, **backend_kwargs : Any) -> None:
        self.flow_id, self.run_id = flow_id, run_id
        self.clock = clock or FakeClock()
        self.backend = MemoryMessagingBackend(self.clock, **backend_kwargs)
        self._receivers : set[str] = set()
        # What each named receiver holds unsettled (the model has no per-process state of its own).
        self._held : Dict[str, List[DeliveryToken]] = {}
        self._messengers : List[NATSMessenger] = []
        self._effective : Dict[SubscriptionId, Dict[str, Any]] = {}
        self._verified : Dict[ChannelId, Dict[str, Any]] = {}
        self._sequences : Dict[tuple[ChannelId, str], int] = {}
        self._next_sequence : Dict[ChannelId, int] = {}
        self._clock_step = 0.1
        # The messenger polls the backend in real time; the model's time only moves
        # when asked, so a receive with nothing ready advances the fake clock a step
        # instead of sleeping — leases, NAK delays and dedup windows run on model time.
        clock = self.clock
        step = self._clock_step

        def receive_any(self_ : MemoryMessagingBackend, subscriptions : Sequence[SubscriptionId], item_credit : int,
                        byte_credit : int, deadline : float) -> List[tuple[SubscriptionId, Delivery]]:
            while True:
                out : List[tuple[SubscriptionId, Delivery]] = []
                for subscription in subscriptions:
                    for delivery in self_.receive(subscription, item_credit, byte_credit, 0.0):
                        out.append((subscription, delivery))
                if out or time.monotonic() >= deadline:
                    return out
                clock.advance(step)
                time.sleep(0.001)
        self.backend.receive_any = types.MethodType(receive_any, self.backend)  # type: ignore[method-assign]
        self._messenger_poll = messenger_poll

    def capabilities(self) -> MessagingCapabilities:
        return self.backend.capabilities()

    def channel(self, node : str, profile : str, retention : str, max_msgs : int = 10_000,
                max_bytes : int | None = None, max_age : float | None = None, per_subject : bool = False,
                dedup_window : int = topology.DUPLICATE_WINDOW_SECONDS,
                required : Sequence[SubscriptionId] = ()) -> ChannelId:
        spec_ = self.channel_spec(node, profile, retention, max_msgs, max_bytes, max_age, per_subject, dedup_window, required)
        verified = self.backend.ensure_channel(spec_, f'op:{uuid.uuid4().hex[:6]}')
        self._verified[spec_.id] = dict(verified.effective)
        return spec_.id

    def channel_ids(self) -> List[ChannelId]:
        return self.backend.channel_ids()

    def channel_verified(self, node : str, profile : str, retention : str, max_msgs : int = 10_000,
                         max_bytes : int | None = None, per_subject : bool = False, persistent : bool = False) -> Any:
        spec_ = self.channel_spec(node, profile, retention, max_msgs, max_bytes, None, per_subject, persistent = persistent)
        return self.backend.ensure_channel(spec_, f'op:{uuid.uuid4().hex[:6]}')

    def provision_subscription(self, channel : ChannelId, consumer : str, ack_wait : float = 2.0,
                               max_deliver : int = 4, credit : int = 8, partition : int | None = None) -> SubscriptionId:
        sub = SubscriptionId(channel, consumer, partition, SUBSCRIPTION_DATA)
        verified = self.backend.ensure_subscription(
            SubscriptionSpec(sub, partition is None, ack_wait, max_deliver, credit, DEFAULT_BYTE_CREDIT), 'provision')
        self._effective[verified.spec.id] = dict(verified.effective)
        return verified.spec.id

    def subscription_effective(self, channel : ChannelId, consumer : str, partition : int | None = None) -> Dict[str, Any]:
        sub = SubscriptionId(channel, consumer, partition, SUBSCRIPTION_DATA)
        effective = self._effective.get(sub)
        assert effective is not None, f'{sub} was never provisioned through this driver'
        return {'max_ack_pending': effective['item_credit'], 'ack_wait': effective['ack_wait_seconds'],
                'max_deliver': effective['max_deliver'], 'competing': effective['competing']}

    def subscriptions(self, channel : ChannelId) -> List[str]:
        return sorted(f'{s.consumer_node}' + ('' if s.partition is None else f'--p{s.partition}')
                      + ('' if s.instance is None else f'@{s.instance}')
                      for s in self.backend.subscription_ids(channel) if s.kind == SUBSCRIPTION_DATA)

    def retained(self, channel : ChannelId) -> int:
        return sum(1 for e in self.backend.stored(channel) if e.kind != KIND_DLQ)

    def retained_floor(self, channel : ChannelId) -> int:
        held = [self._sequences[(channel, e.publication_id)] for e in self.backend.stored(channel)
                if (channel, e.publication_id) in self._sequences]
        return min(held) if held else self._next_sequence.get(channel, 1)

    def retained_ids(self, channel : ChannelId, kind : str = KIND_DATA) -> List[str]:
        return [e.publication_id for e in self.backend.stored(channel) if e.kind == kind]

    def receiver(self, name : str, prefetch : int = 1, keepalive : bool = False) -> MessagingBackend:
        self._receivers.add(name)
        return self.backend

    def kill(self, receiver : str) -> None:
        # A dead process settles nothing; its leases expire on the model's clock.
        self._receivers.discard(receiver)

    def retire(self, receiver : str) -> None:
        # A graceful scale-down: whatever the receiver still held goes back to the model at once.
        self._receivers.discard(receiver)
        for token in self._held.pop(receiver, []):
            self.backend.settle(token, Retry(0), f'retire:{token.message_id}:{token.attempt}')

    def publish(self, channel : ChannelId, pid : str, body : bytes = b'x', kind : str = KIND_DATA,
                partition_key : str | None = None, headers : Dict[str, str] | None = None,
                event_id : str | None = None, deadline : float | None = None,
                event_ts : float | None = None) -> PublicationOutcome:
        outcome = self.backend.publish(self.envelope(channel, pid, body, kind, partition_key, headers, event_id, event_ts),
                                       deadline if deadline is not None else 0.0)
        from videoflow.backends.outcomes import Accepted
        if isinstance(outcome, Accepted) and outcome.sequence is not None:
            self._sequences[(channel, pid)] = outcome.sequence
            self._next_sequence[channel] = outcome.sequence + 1
        return outcome

    def receive(self, bound : Bound, timeout : float = 1.0, item_credit : int = 1,
                byte_credit : int = DEFAULT_BYTE_CREDIT) -> List[Delivery]:
        got = bound.backend.receive(bound.subscription, item_credit, byte_credit, 0.0)
        self._held.setdefault(bound.receiver, []).extend(d.token for d in got)
        return got

    def settle(self, bound : Bound, token : DeliveryToken, outcome : Settlement,
               settlement_id : str | None = None) -> SettlementOutcome:
        held = self._held.get(bound.receiver)
        if held and token in held:
            held.remove(token)
        return super().settle(bound, token, outcome, settlement_id)

    def wait(self, seconds : float) -> None:
        self.clock.advance(seconds)

    def until(self, condition : Callable[[], bool], timeout : float, step : float = 0.1) -> bool:
        # Model time advances a step per poll; a little real time passes too, so
        # receiver threads driven from this loop get to run between polls.
        elapsed = 0.0
        while True:
            if condition():
                return True
            if elapsed >= timeout:
                return condition()
            self.clock.advance(step)
            elapsed += step
            time.sleep(0.005)

    def messenger(self, node : str, parents : List[str], flow_type : str, max_retries : int = 3,
                  ack_wait : int = 2, delivery : dict | None = None, blob_store : BlobStore | None = None,
                  replica_id : int = 0, nb_tasks : int = 1, partition_by : str | None = None) -> NATSMessenger:
        nats_messenger._FETCH_TIMEOUT_SECONDS = self._messenger_poll
        m = NATSMessenger(StubNode(node), list(parents), 'memory://model', self.flow_id, flow_type, self.run_id,
                          max_retries = max_retries, ack_wait = ack_wait, delivery_policy = delivery,
                          blob_store = blob_store, replica_id = replica_id, nb_tasks = nb_tasks,
                          partition_by = partition_by, backend = self.backend)
        self._messengers.append(m)
        return m

    def publish_bytes(self, parent : str, data : bytes, msg_id : str, headers : Dict[str, str] | None = None) -> None:
        channel = ChannelId(self.flow_id, self.run_id, parent)
        if channel not in self.backend.channel_ids():
            self.backend.ensure_channel(channel_spec_for(self.flow_id, self.run_id, parent, BATCH, RELIABLE_WORK), 'parent')
        outcome = self.backend.publish(self.envelope(channel, msg_id, data, KIND_DATA, headers = headers), 0.0)
        from videoflow.backends.outcomes import Accepted
        assert isinstance(outcome, Accepted), outcome

    def dlq(self, node : str | None = None) -> List[Dict[str, Any]]:
        out : List[Dict[str, Any]] = []
        for channel in self.backend.channel_ids():
            if channel.flow_id != self.flow_id or channel.run_id != self.run_id:
                continue
            if node is not None and channel.node != node:
                continue
            for e in self.backend.stored(channel):
                if e.kind == KIND_DLQ:
                    out.append({'headers': dict(e.headers), 'data': e.body, 'node': channel.node})
        return out

    def dlq_outage(self, node : str) -> None:
        self.backend.pause_acceptance(ChannelId(self.flow_id, self.run_id, node))

    def dlq_restore(self, node : str) -> None:
        self.backend.resume_acceptance(ChannelId(self.flow_id, self.run_id, node))

    def consumer_state(self, child : str, parent : str) -> Observation[tuple[int, int]]:
        sub = SubscriptionId(ChannelId(self.flow_id, self.run_id, parent), child, None, SUBSCRIPTION_DATA)
        observed = self.backend.observe_subscription(sub)
        if not isinstance(observed, known(0).__class__):
            return observed
        return known((observed.value.available, observed.value.leased), observed.generation)

    def provision(self, specs : List[NodeSpec], flow_type : str, max_retries : int = 3) -> None:
        from videoflow.messaging.jetstream_backend import subscription_spec_for
        by_name = {s.name: s for s in specs}
        for s in specs:
            self.backend.ensure_channel(channel_spec_for(self.flow_id, self.run_id, s.name, flow_type,
                                                         profile_of(flow_type)), 'provision')
        for s in specs:
            max_deliver = topology.max_deliver_for(flow_type, max_retries, s.delivery)
            partitioned = bool(s.partition_by and s.nb_tasks > 1)
            credit = topology.consumer_credit(s.nb_tasks, partitioned)
            for parent in s.parents:
                if parent not in by_name:
                    continue
                channel = ChannelId(self.flow_id, self.run_id, parent)
                replicas = range(s.nb_tasks) if partitioned else [None]
                for partition in replicas:
                    sub = SubscriptionId(channel, s.name, partition, SUBSCRIPTION_DATA)
                    self.backend.ensure_subscription(subscription_spec_for(sub, 60, max_deliver, credit), 'provision')

    def observe_publication(self, channel : ChannelId, pid : str, body : bytes = b'x') -> PublicationOutcome:
        return self.backend.observe_publication(self.envelope(channel, pid, body))

    def cancel_publication(self, channel : ChannelId, pid : str) -> bool:
        return self.backend.cancel_publication(channel, pid)

    @contextlib.contextmanager
    def stalled_acceptance(self, channel : ChannelId) -> Any:
        self.backend.pause_acceptance(channel)
        try:
            yield
        finally:
            self.backend.resume_acceptance(channel)

    @contextlib.contextmanager
    def disconnected(self) -> Any:
        channels = [c for c in self.backend.channel_ids() if c.flow_id == self.flow_id and c.run_id == self.run_id]
        for c in channels:
            self.backend.pause_acceptance(c)
        try:
            yield
        finally:
            for c in channels:
                self.backend.resume_acceptance(c)

    def drop_channel(self, channel : ChannelId) -> None:
        self.backend.close([channel], '')

    def last_retained(self, channel : ChannelId, kind : str = KIND_DATA) -> str | None:
        data = [e for e in self.backend.stored(channel) if e.kind == kind]
        return data[-1].publication_id if data else None

    def deny_observation(self, bound : Bound, reason : str = 'timeout') -> None:
        self.backend.fail_observation(bound.subscription, reason)

    def restore_observation(self, bound : Bound) -> None:
        self.backend.fail_observation(bound.subscription, None)

    def resolve_stranded(self, bound : Bound, delivery : Delivery) -> str:
        from videoflow.backends.messaging import Terminal
        from videoflow.backends.outcomes import SettleConfirmed
        record = f'dlq:{self.flow_id}:{self.run_id}:{bound.subscription.channel.node}:{delivery.token.stream_sequence}'
        channel = bound.subscription.channel
        outcome = self.backend.publish(self.envelope(channel, record, delivery.envelope_bytes, KIND_DLQ,
                                                     headers = {'VF-Code': 'VF_STRANDED'}), 0.0)
        from videoflow.backends.outcomes import Accepted
        assert isinstance(outcome, Accepted), outcome
        settled = self.backend.settle(delivery.token, Terminal(record), f'resolve:{record}')
        assert isinstance(settled, SettleConfirmed), settled
        return record

    def close(self) -> None:
        for m in self._messengers:
            with contextlib.suppress(Exception):
                m.quiesce()
                m.close()
        self.backend.close(self.backend.channel_ids(), '')


# -- the broker ----------------------------------------------------------------------------------

class JetStreamDriver(Driver):
    name = 'jetstream'

    def __init__(self, nats_url : str, flow_id : str, run_id : str, flow_type : str = BATCH,
                 client_url : str | None = None, toxiproxy : Any = None, proxy : str = 'nats') -> None:
        '''
        - Arguments:
            - nats_url: the broker, reached directly — what the raw helpers (seeding, \
                inventory, teardown) use.
            - client_url: what the adapters and messengers connect through; the \
                toxiproxy listener for the fault cases, else ``nats_url``.
            - toxiproxy / proxy: the control client and the proxy name behind \
                ``client_url``, for ``stalled_acceptance`` and ``disconnected``.
        '''
        self.nats_url = nats_url
        self.client_url = client_url or nats_url
        self.toxiproxy = toxiproxy
        self.proxy = proxy
        self.flow_id, self.run_id = flow_id, run_id
        self.flow_type = flow_type
        self.clock = None
        self._backends : Dict[str, JetStreamMessagingBackend] = {}
        self._messengers : List[NATSMessenger] = []
        self._channels : List[ChannelId] = []
        self._verified : Dict[ChannelId, Dict[str, Any]] = {}
        self._prov = self._start('_provision', prefetch = 1, keepalive = False)

    def _start(self, name : str, prefetch : int, keepalive : bool) -> JetStreamMessagingBackend:
        backend = JetStreamMessagingBackend(self.client_url, self.flow_id, self.run_id, self.flow_type,
                                            prefetch = prefetch, keepalive = keepalive)
        backend.start()
        self._backends[name] = backend
        return backend

    def capabilities(self) -> MessagingCapabilities:
        return self._prov.capabilities()

    def channel(self, node : str, profile : str, retention : str, max_msgs : int = 10_000,
                max_bytes : int | None = None, max_age : float | None = None, per_subject : bool = False,
                dedup_window : int = topology.DUPLICATE_WINDOW_SECONDS,
                required : Sequence[SubscriptionId] = ()) -> ChannelId:
        spec_ = self.channel_spec(node, profile, retention, max_msgs, max_bytes, max_age, per_subject, dedup_window, required)
        verified = self._prov.ensure_channel(spec_, f'op:{uuid.uuid4().hex[:6]}')
        self._verified[spec_.id] = dict(verified.effective)
        self._channels.append(spec_.id)
        return spec_.id

    def stream(self, channel : ChannelId) -> str:
        return topology.stream_name_for(channel.flow_id, channel.run_id, channel.node)

    def channel_ids(self) -> List[ChannelId]:
        import nats  # optional dep (distributed extras)
        prefix = topology.stream_label_selector(self.flow_id, self.run_id)

        async def _go() -> List[str]:
            nc = await nats.connect(self.nats_url)
            try:
                infos = await topology._list_streams(nc.jetstream())
                return [i.config.name for i in infos if (i.config.name or '').startswith(prefix)]
            finally:
                await nc.drain()
        names = run_async(_go)
        return [c for c in self._channels if self.stream(c) in names] + \
            [ChannelId(self.flow_id, self.run_id, n[len(prefix):]) for n in names
             if n not in {self.stream(c) for c in self._channels}]

    def channel_verified(self, node : str, profile : str, retention : str, max_msgs : int = 10_000,
                         max_bytes : int | None = None, per_subject : bool = False, persistent : bool = False) -> Any:
        spec_ = self.channel_spec(node, profile, retention, max_msgs, max_bytes, None, per_subject, persistent = persistent)
        verified = self._prov.ensure_channel(spec_, f'op:{uuid.uuid4().hex[:6]}')
        self._channels.append(spec_.id)
        return verified

    def provision_subscription(self, channel : ChannelId, consumer : str, ack_wait : float = 2.0,
                               max_deliver : int = 4, credit : int = 8, partition : int | None = None) -> SubscriptionId:
        import nats  # optional dep (distributed extras)
        config = topology.consumer_config_for(channel.flow_id, channel.run_id, consumer, channel.node,
                                              ack_wait = int(ack_wait), max_deliver = max_deliver, max_ack_pending = credit)
        if partition is not None:
            config.durable_name = topology.partitioned_durable_name_for(consumer, channel.node, partition)

        async def _go() -> None:
            nc = await nats.connect(self.nats_url)
            try:
                await topology._ensure_consumer(nc.jetstream(), self.stream(channel), config)
            finally:
                await nc.drain()
        run_async(_go)
        return SubscriptionId(channel, consumer, partition, SUBSCRIPTION_DATA)

    def subscription_effective(self, channel : ChannelId, consumer : str, partition : int | None = None) -> Dict[str, Any]:
        import nats  # optional dep (distributed extras)
        durable = (topology.partitioned_durable_name_for(consumer, channel.node, partition) if partition is not None
                   else topology.durable_name_for(consumer, channel.node))

        async def _go() -> Dict[str, Any]:
            nc = await nats.connect(self.nats_url)
            try:
                info = await nc.jetstream().consumer_info(self.stream(channel), durable)
                return {'max_ack_pending': info.config.max_ack_pending, 'ack_wait': info.config.ack_wait,
                        'max_deliver': info.config.max_deliver, 'ack_policy': str(info.config.ack_policy),
                        'filter_subject': info.config.filter_subject, 'metadata': dict(info.config.metadata or {})}
            finally:
                await nc.drain()
        return run_async(_go)

    def subscriptions(self, channel : ChannelId) -> List[str]:
        import nats  # optional dep (distributed extras)

        async def _go() -> List[str]:
            nc = await nats.connect(self.nats_url)
            try:
                infos = await nc.jetstream().consumers_info(self.stream(channel))
                return sorted(i.name for i in infos if '--eos--' not in i.name)
            finally:
                await nc.drain()
        return run_async(_go)

    def retained(self, channel : ChannelId) -> int:
        return int(stream_state(self.nats_url, self.stream(channel)).messages)

    def retained_floor(self, channel : ChannelId) -> int:
        state = stream_state(self.nats_url, self.stream(channel))
        return int(state.first_seq) if state.messages else int(state.last_seq) + 1

    def receiver(self, name : str, prefetch : int = 1, keepalive : bool = False) -> MessagingBackend:
        if name not in self._backends:
            self._start(name, prefetch, keepalive)
        return self._backends[name]

    def kill(self, receiver : str) -> None:
        # A dead process settles nothing and hands nothing back: its leases lapse on the broker's clock.
        backend = self._backends.pop(receiver, None)
        if backend is not None:
            backend.shutdown(hand_back = False)

    def retire(self, receiver : str) -> None:
        # A scaled-down replica: a graceful shutdown hands its unsettled deliveries back at once.
        backend = self._backends.pop(receiver, None)
        if backend is not None:
            backend.shutdown()

    def publish(self, channel : ChannelId, pid : str, body : bytes = b'x', kind : str = KIND_DATA,
                partition_key : str | None = None, headers : Dict[str, str] | None = None,
                event_id : str | None = None, deadline : float | None = None,
                event_ts : float | None = None) -> PublicationOutcome:
        return self._prov.publish(self.envelope(channel, pid, body, kind, partition_key, headers, event_id, event_ts),
                                  deadline if deadline is not None else time.monotonic() + 5.0)

    def receive(self, bound : Bound, timeout : float = 1.0, item_credit : int = 1,
                byte_credit : int = DEFAULT_BYTE_CREDIT) -> List[Delivery]:
        return bound.backend.receive(bound.subscription, item_credit, byte_credit, time.monotonic() + timeout)

    def wait(self, seconds : float) -> None:
        time.sleep(seconds)

    def until(self, condition : Callable[[], bool], timeout : float, step : float = 0.05) -> bool:
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if condition():
                return True
            time.sleep(step)
        return condition()

    def messenger(self, node : str, parents : List[str], flow_type : str, max_retries : int = 3,
                  ack_wait : int = 2, delivery : dict | None = None, blob_store : BlobStore | None = None,
                  replica_id : int = 0, nb_tasks : int = 1, partition_by : str | None = None) -> NATSMessenger:
        m = NATSMessenger(StubNode(node), list(parents), self.client_url, self.flow_id, flow_type, self.run_id,
                          max_retries = max_retries, ack_wait = ack_wait, delivery_policy = delivery,
                          blob_store = blob_store, replica_id = replica_id, nb_tasks = nb_tasks,
                          partition_by = partition_by)
        self._messengers.append(m)
        return m

    def provision(self, specs : List[NodeSpec], flow_type : str, max_retries : int = 3) -> None:
        topology.provision_flow_sync(self.nats_url, specs, self.flow_id, self.run_id, flow_type,
                                     max_retries = max_retries, timeout = 60)
        for s in specs:
            self._channels.append(ChannelId(self.flow_id, self.run_id, s.name))

    def publish_bytes(self, parent : str, data : bytes, msg_id : str, headers : Dict[str, str] | None = None) -> None:
        publish_raw(self.nats_url, topology.subject_for(self.flow_id, self.run_id, parent), data,
                    headers = {'Nats-Msg-Id': msg_id, **(headers or {})})

    def dlq(self, node : str | None = None) -> List[Dict[str, Any]]:
        import nats  # optional dep (distributed extras)
        stream = topology.dlq_stream_name(self.flow_id)
        subject = topology.dlq_subject_filter(self.flow_id, self.run_id, node)

        async def _go() -> List[Dict[str, Any]]:
            nc = await nats.connect(self.nats_url)
            try:
                js = nc.jetstream()
                try:
                    info = await js.stream_info(stream)
                except Exception:  # noqa: BLE001 — no DLQ stream: no dead letters
                    return []
                if not info.state.messages:
                    return []
                psub = await js.pull_subscribe(subject, stream = stream)      # ephemeral: reads never consume
                out : List[Dict[str, Any]] = []
                try:
                    msgs = await psub.fetch(info.state.messages, timeout = 2)
                except Exception:  # noqa: BLE001 — fewer than the stream total match the filter
                    msgs = []
                for m in msgs:
                    out.append({'headers': dict(m.headers or {}), 'data': m.data, 'subject': m.subject,
                                'node': m.subject.rsplit('.', 1)[-1]})
                await psub.unsubscribe()
                return out
            finally:
                await nc.drain()
        return run_async(_go)

    def dlq_outage(self, node : str) -> None:
        delete_dlq(self.nats_url, self.flow_id)

    def dlq_restore(self, node : str) -> None:
        import nats  # optional dep (distributed extras)

        async def _go() -> None:
            nc = await nats.connect(self.nats_url)
            try:
                await topology._ensure_stream(nc.jetstream(), topology.dlq_stream_config(self.flow_id))
            finally:
                await nc.drain()
        run_async(_go)

    def consumer_state(self, child : str, parent : str) -> Observation[tuple[int, int]]:
        import nats  # optional dep (distributed extras)
        durable = topology.durable_name_for(child, parent)
        stream = topology.stream_name_for(self.flow_id, self.run_id, parent)

        async def _go() -> Observation[tuple[int, int]]:
            nc = await nats.connect(self.nats_url)
            try:
                info = await nc.jetstream().consumer_info(stream, durable)
                return known((int(info.num_pending), int(info.num_ack_pending)))
            finally:
                await nc.drain()
        from videoflow.backends.outcomes import unknown
        try:
            return run_async(_go)
        except Exception as e:  # noqa: BLE001 — reported, never zero
            return unknown('api', f'{type(e).__name__}: {e}')

    def observe_publication(self, channel : ChannelId, pid : str, body : bytes = b'x') -> PublicationOutcome:
        return self._prov.observe_publication(self.envelope(channel, pid, body))

    def _require_toxiproxy(self) -> Any:
        assert self.toxiproxy is not None, 'this driver was built without a toxiproxy client'
        return self.toxiproxy

    @contextlib.contextmanager
    def stalled_acceptance(self, channel : ChannelId) -> Any:
        '''toxiproxy ``timeout`` with ``timeout = 0``: client bytes are held, and delivered when the toxic goes.'''
        toxiproxy = self._require_toxiproxy()
        name = toxiproxy.add_toxic(self.proxy, 'timeout', {'timeout': 0}, name = 'vf-stall', stream = 'upstream')
        try:
            yield
        finally:
            toxiproxy.remove_toxic(self.proxy, name)

    @contextlib.contextmanager
    def disconnected(self) -> Any:
        '''The proxy disabled: every live connection is cut and the listener refuses until re-enabled.'''
        toxiproxy = self._require_toxiproxy()
        toxiproxy.enable(self.proxy, False)
        try:
            yield
        finally:
            toxiproxy.enable(self.proxy, True)

    def connected(self) -> bool:
        return all(b.connected for b in self._backends.values())

    def drop_channel(self, channel : ChannelId) -> None:
        import nats  # optional dep (distributed extras)

        async def _go() -> None:
            nc = await nats.connect(self.nats_url)
            try:
                await nc.jetstream().delete_stream(self.stream(channel))
            finally:
                await nc.drain()
        run_async(_go)

    def last_retained(self, channel : ChannelId, kind : str = KIND_DATA) -> str | None:
        import nats  # optional dep (distributed extras)
        subject = (topology.eos_subject_for(channel.flow_id, channel.run_id, channel.node) if kind in ('eos', 'abort')
                   else topology.subject_for(channel.flow_id, channel.run_id, channel.node))

        async def _go() -> str | None:
            nc = await nats.connect(self.nats_url)
            try:
                try:
                    msg = await nc.jetstream().get_last_msg(self.stream(channel), subject)
                except Exception:  # noqa: BLE001 — nothing retained
                    return None
                return (msg.headers or {}).get('Nats-Msg-Id')
            finally:
                await nc.drain()
        return run_async(_go)

    def deny_observation(self, bound : Bound, reason : str = 'timeout') -> None:
        import nats.errors  # optional dep (distributed extras)
        backend = bound.backend
        assert isinstance(backend, JetStreamMessagingBackend)
        real = backend._js
        error = nats.errors.TimeoutError if reason == 'timeout' else PermissionError

        class _Denied:
            '''The management API refusing every read: ``consumer_info`` never answers.'''
            def __getattr__(self, name : str) -> Any:
                return getattr(real, name)

            async def consumer_info(self, *args : Any, **kwargs : Any) -> Any:
                raise error('injected: consumer info denied')

        backend._vf_real_js = real  # type: ignore[attr-defined]
        backend._js = _Denied()

    def restore_observation(self, bound : Bound) -> None:
        backend = bound.backend
        assert isinstance(backend, JetStreamMessagingBackend)
        real = getattr(backend, '_vf_real_js', None)
        if real is not None:
            backend._js = real

    def resolve_stranded(self, bound : Bound, delivery : Delivery) -> str:
        import nats  # optional dep (distributed extras)
        channel = bound.subscription.channel
        seq = int(delivery.token.stream_sequence or 0)
        record = f'dlq:{self.flow_id}:{self.run_id}:{channel.node}:{seq}'
        self.dlq_restore(channel.node)
        publish_raw(self.nats_url, topology.dlq_subject_for(self.flow_id, self.run_id, channel.node),
                    delivery.envelope_bytes, headers = {'Nats-Msg-Id': record, 'VF-Code': 'VF_STRANDED',
                                                        'VF-Origin-Node': channel.node, 'VF-Run-Id': self.run_id})

        async def _go() -> None:
            nc = await nats.connect(self.nats_url)
            try:
                await nc.jetstream().delete_msg(self.stream(channel), seq)
            finally:
                await nc.drain()
        run_async(_go)
        return record

    def close(self) -> None:
        for m in self._messengers:
            with contextlib.suppress(Exception):
                m.quiesce()
                m.close()
        for backend in list(self._backends.values()):
            with contextlib.suppress(Exception):
                backend.shutdown()
        self._backends.clear()
        delete_run(self.nats_url, self.flow_id, self.run_id)
        delete_dlq(self.nats_url, self.flow_id)


def profile_of(flow_type : str) -> str:
    return LIVE_LATEST if flow_type == REALTIME else RELIABLE_WORK


def retention_of(flow_type : str) -> str:
    return RETENTION_LIMITS if flow_type == REALTIME else RETENTION_INTEREST
