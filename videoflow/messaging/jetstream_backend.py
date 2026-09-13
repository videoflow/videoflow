'''
NATS JetStream as a ``MessagingBackend`` — the transport half of what
``NATSMessenger`` used to be all of.

The split follows the contract in ``videoflow.backends.messaging``: this adapter
owns the connection, its asyncio loop thread, the streams and durables it
provisions, the pull loops that prefetch deliveries into bounded per-subscription
queues, the leases it keeps alive, and the settlement of every delivery token it
hands out. It knows nothing about joining inputs, dead-letter policy, payload
bytes or end-of-stream drains — those stay in the messenger, which composes this
with a payload store.

What the contract buys, concretely:

- A **delivery token** names the subscription, the message, the broker's attempt
  count and the subscription generation. A redelivery of the same message makes
  the earlier token *stale*: settling it returns ``SettleStale`` without a broker
  call, so a superseded handle can never TERM the delivery that replaced it
  (MSG-011).
- **Settlement is confirmed or unknown, never assumed.** ``Completed`` uses
  ``ack_sync`` and reports ``SettleUnknown`` when the acknowledgement is not
  confirmed within the timeout — the caller must not release a payload
  obligation on that (PAY-004). ``Terminal`` requires a durable record reference
  (DELIV-15).
- **Publication outcomes are typed.** A deadline that expires after the send is
  ``PublicationUnknown``; a broker refusal is ``Rejected`` with ``retryable``
  saying whether waiting helps (a full BATCH stream) or not (no stream captures
  the subject); a lost receipt is never reported as rejection (MSG-013, MSG-014).
  ``observe_publication`` resolves an unknown send by an idempotent re-publish
  inside the stream's duplicate window and is ``Unresolvable`` beyond it.
- **Observations distinguish zero from unknown.** ``observe_subscription`` is
  ``Unknown`` when the consumer could not be read, and its ``unresolved`` count
  is fed by the server's own ``MAX_DELIVERIES`` advisories, so work the broker
  retains but will never redeliver is not reported as "nothing pending" (MSG-012).

Everything a NATS API behaviour relies on is cited against nats-py 2.15.0
(``.venv/lib/python3.12/site-packages/nats``): ``Msg.ack_sync`` waits for the
server's reply and marks the message acked only then (``nats/aio/msg.py``);
``nak``/``term``/``in_progress`` are fire-and-forget (same file); ``JetStreamContext.publish``
raises ``nats.js.errors.APIError`` for a server-side refusal and
``nats.errors.TimeoutError`` when no PubAck arrives (``nats/js/client.py``);
``pull_subscribe`` creates the durable from ``config`` when ``consumer_info`` says it
does not exist (``nats/js/client.py``); a JetStream ``Msg.metadata`` carries
``num_delivered`` and ``sequence.stream`` (``nats/aio/msg.py``).
'''
from __future__ import absolute_import, division, print_function

import asyncio
import concurrent.futures
import contextlib
import json
import logging
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Coroutine, Dict, Optional, Sequence, TypeVar

import nats
import nats.errors
import nats.js.errors
from nats.aio.msg import Msg
from nats.js.api import ConsumerConfig, RetentionPolicy, StorageType, StreamConfig

from ..backends import faults
from ..backends.capabilities import LEDGER_WINDOW, RETENTION_INTEREST, RETENTION_LIMITS, MessagingCapabilities
from ..backends.messaging import (
    KIND_ABORT,
    KIND_DATA,
    KIND_DLQ,
    KIND_EOS,
    OVERFLOW_EVICT_OLDEST,
    OVERFLOW_REJECT,
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
from ..backends.outcomes import (
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
from ..core.constants import BATCH, REALTIME
from ..core.errors import BrokerUnavailable
from . import topology
from .topology import DEFAULT_PREFETCH, LEGACY_BIND_CREDIT, consumer_credit  # noqa: F401 — re-exported for callers

logger = logging.getLogger(__package__)

_T = TypeVar('_T')

#: How long one pull request waits for a message before the loop re-checks
#: whether it should stop. Also the unit ``receive_any`` waits in.
FETCH_TIMEOUT_SECONDS = 1.0
#: How long a ``Completed`` settlement waits for the server's ack reply before it
#: is reported ``SettleUnknown`` (``Msg.ack_sync(timeout)``, nats-py 2.15.0).
ACK_CONFIRM_SECONDS = 2.0
#: Bound on a synchronous call bridged onto the loop thread; a broker that does
#: not answer within it yields an Unknown outcome rather than a hang.
BRIDGE_TIMEOUT_SECONDS = 15.0
#: Prefetch depth per subscription: un-acked deliveries parked locally age
def channel_spec_for(flow_id : str, run_id : str, node : str, flow_type : str, profile : str,
                     required : Sequence[SubscriptionId] = (),
                     realtime_buffer : int = topology.DEFAULT_REALTIME_BUFFER,
                     batch_max_msgs : int = topology.DEFAULT_BATCH_MAX_MSGS, replicas : int = 1,
                     max_bytes : int | None = None) -> ChannelSpec:
    '''
    The channel a node's output stream must provide, from the flow type — today's
    stream settings, as a spec. ``replicas`` is the copies a replicated broker
    keeps (``STREAM-14``); ``max_bytes`` caps the stream's storage (a capacity
    fixture); both default to today's unset values.
    '''
    realtime = flow_type == REALTIME
    return ChannelSpec(
        id = ChannelId(flow_id, run_id, node), profile = profile,
        retention = RETENTION_LIMITS if realtime else RETENTION_INTEREST,
        max_msgs = max(1, realtime_buffer) if realtime else batch_max_msgs,
        max_bytes = max_bytes, max_age_seconds = None,
        overflow = OVERFLOW_EVICT_OLDEST if realtime else OVERFLOW_REJECT,
        dedup_window_seconds = topology.DUPLICATE_WINDOW_SECONDS, replicas = replicas, persistence = replicas > 1,
        required_subscriptions = tuple(required),
        owner_labels = topology.owner_labels(flow_id, run_id, node = node, kind = 'stream'))


def stream_config_for_spec(spec : ChannelSpec, generation : str | None = None) -> StreamConfig:
    '''The JetStream ``StreamConfig`` a channel spec means — byte-identical to today's for a default spec.'''
    flow_type = BATCH if spec.retention == RETENTION_INTEREST else REALTIME
    config = topology.stream_config_for(
        spec.id.flow_id, spec.id.run_id, spec.id.node, flow_type,
        realtime_buffer = spec.max_msgs if flow_type == REALTIME else topology.DEFAULT_REALTIME_BUFFER,
        batch_max_msgs = spec.max_msgs if flow_type == BATCH else topology.DEFAULT_BATCH_MAX_MSGS,
        generation = generation, replicas = spec.replicas)
    if spec.max_bytes is not None:
        config.max_bytes = spec.max_bytes
    if spec.persistence:
        # A persistent channel asks for file storage explicitly, so a stream that
        # exists on memory storage — which an update cannot change (err_code
        # 10052) — reads back as a mismatch instead of passing on its name.
        config.storage = StorageType.FILE
    if spec.dedup_window_seconds != topology.DUPLICATE_WINDOW_SECONDS:
        config.duplicate_window = spec.dedup_window_seconds
    if spec.per_subject_limits:
        # One slot per subject — data and EOS each keep ``max_msgs`` — instead of
        # one slot shared by both: ``max_msgs_per_subject`` (nats-server 2.10;
        # nats-py 2.15.0 ``StreamConfig.max_msgs_per_subject``, read back by
        # ``stream_info``). ``max_msgs`` goes back to the dataclass default
        # (``None`` — omitted from the request, so the server's unlimited applies).
        config.max_msgs_per_subject = max(1, spec.max_msgs)
        config.max_msgs = StreamConfig().max_msgs
    return config


def subscription_spec_for(subscription : SubscriptionId, ack_wait_seconds : float, max_deliver : int,
                          credit : int, byte_credit : int = 0) -> SubscriptionSpec:
    '''A subscription spec: ``credit`` is the broker-side ``max_ack_pending`` this subscription is bound with.'''
    return SubscriptionSpec(id = subscription, competing = subscription.partition is None,
                            ack_wait_seconds = ack_wait_seconds, max_deliver = max_deliver,
                            item_credit = credit, byte_credit = byte_credit)


def _key(token : DeliveryToken) -> tuple[SubscriptionId, int]:
    return token.subscription, int(token.stream_sequence or 0)


@dataclass
class _Live:
    '''A delivery this receiver holds: its token and the broker message behind it.'''
    token : DeliveryToken
    msg : Msg


@dataclass
class _Bound:
    '''One bound subscription: its pull subscription, prefetch queue and identity on the broker.'''
    spec : SubscriptionSpec
    stream : str
    durable : str
    generation : str
    queue : 'asyncio.Queue[Delivery]'
    psub : Any
    task : Optional['asyncio.Task[None]'] = None
    #: The consumer's admission filter (``set_admission``): refused deliveries are
    #: acked on the loop and never parked; ``skipped`` counts them.
    admit : Optional[Callable[[Delivery], bool]] = None
    on_skip : Optional[Callable[[Delivery], None]] = None
    skipped : int = 0


class JetStreamMessagingBackend(MessagingBackend):
    '''
    - Arguments:
        - nats_url: the server, e.g. ``nats://localhost:4222``.
        - flow_id / run_id / flow_type: the run this backend serves; every channel \
            and subscription it touches belongs to it.
        - prefetch: per-subscription local prefetch depth (``DEFAULT_PREFETCH``).
        - fetch_timeout: seconds one pull request waits (``FETCH_TIMEOUT_SECONDS``).
        - ack_confirm_seconds: how long ``Completed`` waits for the broker's reply.
        - keepalive: whether to extend the lease of every unsettled delivery \
            periodically (``ack_wait / 3``), as the messenger always has.
    '''
    def __init__(self, nats_url : str, flow_id : str, run_id : str, flow_type : str,
                 prefetch : int = DEFAULT_PREFETCH, fetch_timeout : float = FETCH_TIMEOUT_SECONDS,
                 ack_confirm_seconds : float = ACK_CONFIRM_SECONDS, keepalive : bool = True,
                 connect_timeout : float = 30.0) -> None:
        self._nats_url = nats_url
        self._flow_id = flow_id
        self._run_id = run_id
        self._flow_type = flow_type
        self._prefetch = max(1, prefetch)
        self._fetch_timeout = fetch_timeout
        self._ack_confirm = ack_confirm_seconds
        self._keepalive = keepalive
        self._connect_timeout = connect_timeout
        self._loop = asyncio.new_event_loop()
        # Where on_skip callbacks run: never on the loop (they may touch a payload store).
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers = 2, thread_name_prefix = 'vf-js-skip')
        self._thread : Optional[threading.Thread] = None
        self._nc : Any = None
        self._js : Any = None
        self._closing = threading.Event()
        self._lock = threading.Lock()
        self._bound : Dict[SubscriptionId, _Bound] = {}
        self._live : Dict[tuple[SubscriptionId, int], _Live] = {}
        self._exhausted : Dict[SubscriptionId, set[int]] = {}
        self._streams : Dict[ChannelId, StreamConfig] = {}
        self._first_sent : Dict[str, float] = {}
        self._rejected : Dict[ChannelId, int] = {}
        self._stream_state_cache : Dict[str, tuple[float, Any]] = {}
        self._tasks : list['asyncio.Task[None]'] = []
        self._ack_wait_max = 0.0

    # -- lifecycle ----------------------------------------------------------------

    def start(self) -> None:
        '''Connect on a fresh loop thread. Raises ``BrokerUnavailable`` when the server cannot be reached.'''
        if self._thread is not None:
            return
        self._thread = threading.Thread(target = self._run_loop, daemon = True, name = 'vf-jetstream')
        self._thread.start()

        async def _connect() -> None:
            self._nc = await nats.connect(self._nats_url)
            self._js = self._nc.jetstream()

        try:
            self._run(_connect(), timeout = self._connect_timeout)
        except Exception as e:  # noqa: BLE001 — every connect failure has one meaning here
            self.shutdown()
            raise BrokerUnavailable(
                f'could not connect to NATS at {self._nats_url}: {type(e).__name__}: {e}',
                remedy = 'Check the broker URL and that the server is reachable from this worker.') from e

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def _run(self, coro : Coroutine[Any, Any, _T], timeout : float = BRIDGE_TIMEOUT_SECONDS) -> _T:
        '''Run ``coro`` on the loop thread and wait for it (bounded): the bridge every sync method crosses.'''
        fut = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return fut.result(timeout = timeout)

    @property
    def loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    @property
    def connected(self) -> bool:
        return self._nc is not None and bool(self._nc.is_connected)

    def shutdown(self, hand_back : bool = True) -> None:
        '''
        Stop the pull loops, hand back every delivery this receiver still holds,
        flush pending publishes, close the connection and the loop thread.

        The hand-back is what makes a scale-down or a rollout cheap: a delivery
        parked in the prefetch queue, or received and never settled, would
        otherwise sit leased until ``ack_wait`` lapsed — on a replica that no
        longer exists. A NAK returns it to the broker at once for the survivors.
        A delivery already settled is left alone (``MsgAlreadyAckdError``).

        - Arguments:
            - hand_back: False closes without the NAKs — what a crashed process \
                looks like to the broker (its leases lapse on their own). Tests \
                simulating a crash pass it; a worker never does.
        '''
        self._closing.set()
        if self._thread is None:
            return

        async def _close() -> None:
            if hand_back and self._tasks:
                # Let each pull loop finish the fetch it has in flight (bounded by
                # the fetch timeout) so a message the server already dispatched to
                # it is handed back by the loop itself, not dropped by a cancel.
                done, _pending = await asyncio.wait(self._tasks, timeout = self._fetch_timeout + 2.0)
            for task in self._tasks:
                task.cancel()
            for task in self._tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:  # noqa: BLE001
                    logger.debug('pull task raised during shutdown', exc_info = True)
            with self._lock:
                held = list(self._live.values()) if hand_back else []
                self._live.clear()
            for entry in held:
                try:
                    await entry.msg.nak()
                except nats.errors.MsgAlreadyAckdError:
                    pass
                except Exception:  # noqa: BLE001 — the lease lapses on its own; this only shortens the wait
                    logger.debug(f'hand-back of seq {entry.token.stream_sequence} failed at shutdown', exc_info = True)
            if held:
                logger.info(f'handed back {len(held)} unsettled delivery(ies) at shutdown')
            if self._nc is not None:
                if self._nc.is_connected:
                    try:
                        await asyncio.wait_for(self._nc.flush(), timeout = 5)
                    except Exception:  # noqa: BLE001
                        logger.debug('NATS flush incomplete during shutdown', exc_info = True)
                try:
                    await self._nc.close()
                except Exception:  # noqa: BLE001
                    logger.debug('NATS close raised during shutdown', exc_info = True)

        try:
            self._run(_close(), timeout = 10)
        except Exception:  # noqa: BLE001
            logger.debug('NATS connection teardown incomplete at shutdown', exc_info = True)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout = 10)
        self._thread = None
        self._executor.shutdown(wait = False)

    # -- capabilities --------------------------------------------------------------

    def capabilities(self) -> MessagingCapabilities:
        '''
        What this broker offers, with the parts that need a read-back reported as
        observed: ``max_payload`` from the connection, replication and storage
        from the first stream this backend provisioned (``ensure_channel``), and
        ``Unknown('unread')`` for either until then.
        '''
        if self._nc is not None and self._nc.is_connected:
            max_payload : Observation[int] = known(int(self._nc.max_payload))
        else:
            max_payload = unknown('unread', 'not connected')
        replication : Observation[int] = unknown('unread', 'no stream read back yet')
        persistence : Observation[bool] = unknown('unread', 'no stream read back yet')
        with self._lock:
            effective = next(iter(self._streams.values()), None)
        if effective is not None:
            replication = known(int(effective.num_replicas or 1))
            if effective.storage == StorageType.MEMORY:
                persistence = known(False)
            else:
                # A file store survives a *process* restart; whether its volume
                # survives a pod loss (emptyDir vs a claim) is a deployment fact
                # the broker cannot report. Left unread rather than overclaimed
                # (MSG-021): the deployment's inspection fills it in.
                persistence = unknown('unread', 'file store; whether its volume outlives the pod is not '
                                      'observable from the broker')
        return MessagingCapabilities(
            adapter = 'jetstream', version = self._server_version(), retained_backlog = True,
            recoverable_delivery = True, latest_per_key = False,
            dedup_window_seconds = topology.DUPLICATE_WINDOW_SECONDS, publication_ledger = LEDGER_WINDOW,
            replication_factor = replication, persistent_storage = persistence, max_payload_bytes = max_payload,
            credit_resizable = True, control_shares_data_slot = True)

    def _server_version(self) -> str:
        if self._nc is None or not self._nc.is_connected:
            return 'unknown'
        try:
            return str(self._nc.connected_server_version)
        except Exception:  # noqa: BLE001
            return 'unknown'

    # -- provisioning --------------------------------------------------------------

    def ensure_channel(self, spec : ChannelSpec, operation_id : str) -> VerifiedChannel:
        faults.barrier('provision.channel.before', channel = spec.id, op_id = operation_id)
        config = stream_config_for_spec(spec)
        verified = self._run(topology._ensure_stream(self._js, config))
        if verified.effective is not None:
            with self._lock:
                self._streams[spec.id] = verified.effective
        faults.barrier('provision.channel.after', channel = spec.id, op_id = operation_id)
        effective = _stream_effective(verified.effective) if verified.effective is not None else {}
        return VerifiedChannel(spec, effective, verified.mismatches)

    def ensure_subscription(self, spec : SubscriptionSpec, operation_id : str) -> VerifiedSubscription:
        '''
        Bind the subscription's durable (creating it when absent, from a config
        that reproduces today's bytes), read it back, and start prefetching into
        its queue. A data subscription also subscribes to the broker's
        ``MAX_DELIVERIES`` advisory for its durable, which feeds ``unresolved``.
        '''
        sub = spec.id
        faults.barrier('provision.subscription.before', subscription = sub, op_id = operation_id)
        channel = sub.channel
        stream = topology.stream_name_for(channel.flow_id, channel.run_id, channel.node)
        if sub.kind == SUBSCRIPTION_EOS:
            config = topology.eos_consumer_config(channel.flow_id, channel.run_id, sub.consumer_node, channel.node,
                                                  sub.instance or 'r0')
            subject = topology.eos_subject_for(channel.flow_id, channel.run_id, channel.node)
        else:
            config = topology.consumer_config_for(channel.flow_id, channel.run_id, sub.consumer_node, channel.node,
                                                  ack_wait = int(spec.ack_wait_seconds), max_deliver = spec.max_deliver,
                                                  max_ack_pending = spec.item_credit)
            if sub.partition is not None:
                config.durable_name = topology.partitioned_durable_name_for(sub.consumer_node, channel.node,
                                                                            sub.partition)
            subject = topology.subject_for(channel.flow_id, channel.run_id, channel.node)
        durable = config.durable_name or ''
        generation = f'{durable}@{uuid.uuid4().hex[:8]}'
        self._ack_wait_max = max(self._ack_wait_max, float(config.ack_wait or 30))

        async def _bind() -> tuple[Any, Any]:
            psub = await self._js.pull_subscribe(subject, durable = durable, config = config)
            info = await psub.consumer_info()
            queue : asyncio.Queue[Delivery] = asyncio.Queue(maxsize = self._prefetch)
            bound = _Bound(spec, stream, durable, generation, queue, psub)
            with self._lock:
                self._bound[sub] = bound
                self._exhausted.setdefault(sub, set())
            bound.task = asyncio.ensure_future(self._pull_loop(sub, bound), loop = self._loop)
            self._tasks.append(bound.task)
            if sub.kind == SUBSCRIPTION_DATA:
                await self._nc.subscribe(f'$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.{stream}.{durable}',
                                         cb = self._on_max_deliveries(sub))
            if self._keepalive and not any(getattr(t, '_vf_keepalive', False) for t in self._tasks):
                task = asyncio.ensure_future(self._keepalive_loop(), loop = self._loop)
                task._vf_keepalive = True  # type: ignore[attr-defined]
                self._tasks.append(task)
            return psub, info

        _psub, info = self._run(_bind(), timeout = self._connect_timeout)
        faults.barrier('provision.subscription.after', subscription = sub, op_id = operation_id)
        effective = _consumer_effective(info.config) if info is not None else {}
        mismatches = topology._mismatches(config, info.config, topology._CONSUMER_FIELDS) if info is not None else ()
        return VerifiedSubscription(spec, effective, mismatches)

    def _on_max_deliveries(self, sub : SubscriptionId) -> Callable[[Msg], Coroutine[Any, Any, None]]:
        async def _cb(msg : Msg) -> None:
            try:
                advisory = json.loads(msg.data)
                seq = int(advisory.get('stream_seq'))
            except (ValueError, TypeError):
                return
            with self._lock:
                self._exhausted.setdefault(sub, set()).add(seq)
        return _cb

    # -- prefetch ------------------------------------------------------------------

    async def _pull_loop(self, sub : SubscriptionId, bound : _Bound) -> None:
        while not self._closing.is_set():
            try:
                msgs = await bound.psub.fetch(batch = 1, timeout = self._fetch_timeout)
            except (nats.errors.TimeoutError, TimeoutError):
                continue
            except Exception as e:  # noqa: BLE001
                if self._closing.is_set():
                    return
                logger.warning(f'pull fetch on {bound.durable} failed: {e}')
                await asyncio.sleep(0.5)
                continue
            for msg in msgs:
                if self._closing.is_set():
                    # Fetched while shutting down: nobody will receive it here. It
                    # goes straight back rather than into a queue that is about to
                    # be abandoned (the message would otherwise sit leased).
                    with contextlib.suppress(Exception):
                        await msg.nak()
                    continue
                delivery = self._admit(sub, bound, msg)
                if bound.admit is not None and not self._admits(bound, delivery):
                    await self._skip(bound, delivery, msg)
                    continue
                await bound.queue.put(delivery)

    def _admits(self, bound : _Bound, delivery : Delivery) -> bool:
        assert bound.admit is not None
        try:
            return bool(bound.admit(delivery))
        except Exception:  # noqa: BLE001 — a filter that fails admits: the receiving side classifies the bytes
            logger.debug(f'{bound.durable}: admission filter raised; delivery admitted', exc_info = True)
            return True

    async def _skip(self, bound : _Bound, delivery : Delivery, msg : Msg) -> None:
        '''
        Settle a refused delivery ``Completed`` right here on the loop, so a message
        this consumer will never process leaves its ack window at once; only a
        *confirmed* ack hands the delivery to ``on_skip`` (on the executor, never on
        the loop). An unconfirmed ack leaves the token live: the redelivery repeats
        the decision, and nothing is released for it (PAY-004).
        '''
        key = _key(delivery.token)
        try:
            await msg.ack_sync(timeout = self._ack_confirm)
        except nats.errors.MsgAlreadyAckdError:
            pass
        except Exception as e:  # noqa: BLE001 — every unconfirmed ack is the same case
            logger.debug(f'{bound.durable}: skip of seq {delivery.token.stream_sequence} not confirmed '
                         f'({type(e).__name__}: {e})')
            return
        with self._lock:
            live = self._live.get(key)
            if live is not None and live.token == delivery.token:
                del self._live[key]
            bound.skipped += 1
        if bound.on_skip is not None:
            self._loop.run_in_executor(self._executor, bound.on_skip, delivery)

    def _admit(self, sub : SubscriptionId, bound : _Bound, msg : Msg) -> Delivery:
        '''Mint the token for a fetched message; a redelivery retires the token of the attempt before it.'''
        try:
            meta = msg.metadata
            seq = int(meta.sequence.stream)
            attempt = int(meta.num_delivered)
        except Exception:  # noqa: BLE001 — a message without JetStream metadata is not a delivery we can settle
            seq, attempt = 0, 1
        headers = dict(msg.headers or {})
        message_id = headers.get('Nats-Msg-Id') or f'{bound.stream}:{seq}'
        token = DeliveryToken(sub, message_id, seq, attempt, bound.generation)
        with self._lock:
            previous = self._live.get((sub, seq))
            if previous is not None:
                logger.debug(f'{bound.durable}: attempt {attempt} of seq {seq} supersedes attempt '
                             f'{previous.token.attempt}')
            self._live[(sub, seq)] = _Live(token, msg)
        return Delivery(token, msg.data, len(msg.data), time.monotonic(), headers)

    async def _keepalive_loop(self) -> None:
        interval = max(1.0, self._ack_wait_max / 3.0)
        while not self._closing.is_set():
            await asyncio.sleep(interval)
            with self._lock:
                live = list(self._live.values())
            for entry in live:
                try:
                    await entry.msg.in_progress()
                except Exception:  # noqa: BLE001
                    pass

    # -- receiving ------------------------------------------------------------------

    def receive(self, subscription : SubscriptionId, item_credit : int, byte_credit : int,
                deadline : float) -> list[Delivery]:
        return [d for _s, d in self.receive_any([subscription], item_credit, byte_credit, deadline)]

    def receive_any(self, subscriptions : Sequence[SubscriptionId], item_credit : int, byte_credit : int,
                    deadline : float) -> list[tuple[SubscriptionId, Delivery]]:
        '''
        Waits until ``deadline`` (monotonic) for any of the subscriptions' queues to
        hold a delivery and returns everything that became ready in that wait —
        every item ``asyncio.wait`` reports done has already been dequeued, so all
        of them are returned (discarding all but one lost messages once).
        '''
        with self._lock:
            queues = {sub: self._bound[sub].queue for sub in subscriptions if sub in self._bound}
        if not queues:
            return []
        wait_for = max(0.0, deadline - time.monotonic())

        async def _wait() -> list[tuple[SubscriptionId, Delivery]]:
            getters = {asyncio.ensure_future(queue.get()): sub for sub, queue in queues.items()}
            done, pending = await asyncio.wait(getters.keys(), timeout = wait_for,
                                               return_when = asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            out : list[tuple[SubscriptionId, Delivery]] = []
            for task in done:
                out.append((getters[task], task.result()))
            return out

        return self._run(_wait(), timeout = wait_for + BRIDGE_TIMEOUT_SECONDS)

    def prefetched(self, subscription : SubscriptionId) -> int:
        with self._lock:
            bound = self._bound.get(subscription)
        return bound.queue.qsize() if bound is not None else 0

    def set_admission(self, subscription : SubscriptionId, admit : Callable[[Delivery], bool],
                      on_skip : Optional[Callable[[Delivery], None]] = None) -> bool:
        with self._lock:
            bound = self._bound.get(subscription)
            if bound is None:
                return False
            bound.admit = admit
            bound.on_skip = on_skip
        return True

    def skipped(self, subscription : SubscriptionId) -> int:
        '''Deliveries the admission filter refused and this backend acked on the consumer's behalf.'''
        with self._lock:
            bound = self._bound.get(subscription)
        return bound.skipped if bound is not None else 0

    def supersede(self, token : DeliveryToken) -> bool:
        key = _key(token)
        with self._lock:
            live = self._live.get(key)
            if live is None or live.token != token:
                return False
            del self._live[key]
        return True

    # -- leases and settlement --------------------------------------------------------

    def renew(self, token : DeliveryToken) -> LeaseObservation:
        with self._lock:
            live = self._live.get(_key(token))
        if live is None or live.token != token:
            return LeaseObservation(token, False, None, 'stale or unknown delivery')
        try:
            self._run(live.msg.in_progress())
        except Exception as e:  # noqa: BLE001
            return LeaseObservation(token, False, None, f'{type(e).__name__}: {e}')
        return LeaseObservation(token, True, None)

    def settle(self, token : DeliveryToken, outcome : Settlement, settlement_id : str) -> SettlementOutcome:
        if isinstance(outcome, Terminal) and not outcome.record_ref:
            raise ValueError('Terminal settlement requires a durable record reference')
        key = _key(token)
        faults.barrier('settle.before', op_id = token.message_id, consumer = token.subscription.consumer_node,
                       attempt = token.attempt, outcome = type(outcome).__name__)
        with self._lock:
            live = self._live.get(key)
            if live is None:
                return SettleUnknown(token, 'delivery not held by this receiver (settled, superseded or never delivered here)')
            if live.token != token:
                return SettleStale(token, live.token.attempt, live.token.generation)
            msg = live.msg

        async def _go() -> None:
            if isinstance(outcome, Completed):
                await msg.ack_sync(timeout = self._ack_confirm)
            elif isinstance(outcome, Retry):
                await msg.nak(delay = outcome.delay_seconds)
            else:
                await msg.term()

        try:
            self._run(_go(), timeout = self._ack_confirm + BRIDGE_TIMEOUT_SECONDS)
        except nats.errors.MsgAlreadyAckdError:
            pass                                    # our own earlier ack landed: confirmed
        except (nats.errors.TimeoutError, TimeoutError, asyncio.TimeoutError) as e:
            # The ack may have reached the server; the lease is still ours as far as
            # this receiver knows, so the token stays live for a retry.
            return SettleUnknown(token, f'settlement not confirmed within {self._ack_confirm:.1f}s ({type(e).__name__})')
        except Exception as e:  # noqa: BLE001
            return SettleUnknown(token, f'{type(e).__name__}: {e}')
        with self._lock:
            self._live.pop(key, None)
            self._exhausted.get(token.subscription, set()).discard(key[1])
        hit = faults.barrier('settle.after', op_id = token.message_id, consumer = token.subscription.consumer_node)
        if hit.drop_response:
            return SettleUnknown(token, 'settlement response lost')
        return SettleConfirmed(token, settlement_id)

    # -- publication ----------------------------------------------------------------

    def subject_for(self, channel : ChannelId, kind : str) -> str:
        if kind in (KIND_EOS, KIND_ABORT):
            return topology.eos_subject_for(channel.flow_id, channel.run_id, channel.node)
        if kind == KIND_DLQ:
            return topology.dlq_subject_for(channel.flow_id, channel.run_id, channel.node)
        return topology.subject_for(channel.flow_id, channel.run_id, channel.node)

    def publish(self, envelope : Envelope, deadline : float) -> PublicationOutcome:
        pid = envelope.publication_id
        subject = self.subject_for(envelope.channel, envelope.kind)
        headers = dict(envelope.headers)
        headers.setdefault('Nats-Msg-Id', pid)
        timeout = max(0.05, deadline - time.monotonic())
        faults.barrier('publish.send.before', op_id = pid, channel = envelope.channel)

        async def _go() -> Any:
            return await self._js.publish(subject, envelope.body, headers = headers, timeout = timeout)

        try:
            ack = self._run(_go(), timeout = timeout + BRIDGE_TIMEOUT_SECONDS)
        except (nats.errors.TimeoutError, TimeoutError, asyncio.TimeoutError) as e:
            # Sent, not acknowledged in time: the broker may well hold it.
            self._first_sent.setdefault(pid, time.monotonic())
            return PublicationUnknown(pid, f'no acknowledgement within {timeout:.1f}s ({type(e).__name__})')
        except nats.js.errors.NoStreamResponseError:
            # 503 "no responders": no JetStream leader answered for the subject —
            # the stream is missing, or it has lost its quorum. Nothing was stored
            # (definite), and the second cause clears (retryable) — MSG-023.
            return Rejected(pid, f'no stream answered for {subject}: the channel is not provisioned or '
                            'has no leader', True)
        except nats.js.errors.APIError as e:
            text = str(e.description or e).lower()
            # A refusal that clears on its own is retryable: a full stream drains,
            # storage is freed, a quorum re-forms (503 ``ServiceUnavailableError``,
            # nats-py 2.15.0 ``nats/js/errors.py:99``, e.g. err_code 10008
            # "JetStream system temporarily unavailable"). Anything else is final.
            retryable = isinstance(e, nats.js.errors.ServiceUnavailableError) or any(
                marker in text for marker in ('maximum messages', 'maximum bytes', 'insufficient storage',
                                              'wrong last sequence', 'resource limits', 'temporarily unavailable',
                                              'no leader', 'not current'))
            with self._lock:
                self._rejected[envelope.channel] = self._rejected.get(envelope.channel, 0) + 1
            return Rejected(pid, e.description or str(e), retryable)
        except nats.errors.ConnectionClosedError as e:
            return Rejected(pid, f'connection closed before the send: {e}', True)
        except Exception as e:  # noqa: BLE001 — anything after the send is ambiguous
            self._first_sent.setdefault(pid, time.monotonic())
            return PublicationUnknown(pid, f'{type(e).__name__}: {e}')
        faults.barrier('publish.send.after', op_id = pid, channel = envelope.channel)
        self._first_sent.setdefault(pid, time.monotonic())
        hit = faults.barrier('publish.receipt.before', op_id = pid, channel = envelope.channel)
        if hit.drop_response:
            return PublicationUnknown(pid, 'publication receipt lost')
        faults.barrier('publish.receipt.after', op_id = pid, channel = envelope.channel)
        return Accepted(pid, int(ack.seq), bool(ack.duplicate), DURABILITY_STREAM)

    def observe_publication(self, envelope : Envelope) -> PublicationOutcome:
        '''
        Resolve an earlier ``PublicationUnknown`` by an idempotent re-publish: inside
        the stream's duplicate window the broker answers ``duplicate`` when the
        first send was stored and stores it now when it was not — either way one
        accepted copy. Outside the window nothing can be known: ``Unresolvable``.
        '''
        pid = envelope.publication_id
        first = self._first_sent.get(pid)
        window = self._dedup_window(envelope.channel)
        if first is None:
            return PublicationUnresolvable(pid, 'never sent from this process; nothing to reconcile against')
        if time.monotonic() - first >= window:
            return PublicationUnresolvable(pid, f'outside the {window:.0f}s deduplication window')
        return self.publish(envelope, time.monotonic() + self._ack_confirm + 1.0)

    def _dedup_window(self, channel : ChannelId) -> float:
        with self._lock:
            effective = self._streams.get(channel)
        if effective is not None and effective.duplicate_window:
            return float(effective.duplicate_window)
        return float(topology.DUPLICATE_WINDOW_SECONDS)

    # -- observation ----------------------------------------------------------------

    def observe_subscription(self, subscription : SubscriptionId) -> Observation[SubscriptionObservation]:
        try:
            faults.barrier('observe.subscription.before', subscription = subscription)
        except Exception as e:  # noqa: BLE001 — an injected failure is an unobservable state
            return unknown('api', f'{type(e).__name__}: {e}')
        with self._lock:
            bound = self._bound.get(subscription)
            exhausted_seqs = sorted(self._exhausted.get(subscription, ()))
            rejected = self._rejected.get(subscription.channel, 0)
        if bound is None:
            return unknown('unbound', 'the subscription is not bound by this receiver')

        async def _go() -> Any:
            return await self._js.consumer_info(bound.stream, bound.durable)

        try:
            info = self._run(_go(), timeout = 5)
        except Exception as e:  # noqa: BLE001 — every failure is reported, never coerced to zero
            return unknown('api', f'{type(e).__name__}: {e}')
        if exhausted_seqs:
            # An exhausted message an operator resolved out of band (dead-lettered by
            # hand and deleted, or replayed) is no longer stranded: keep only the
            # sequences the stream still holds, and keep one whose lookup failed.
            exhausted_seqs = self._still_retained(bound.stream, exhausted_seqs)
            with self._lock:
                self._exhausted[subscription] = set(exhausted_seqs)
        exhausted = len(exhausted_seqs)
        dropped = self._dropped_before(bound, info)
        generation = str(info.delivered.stream_seq) if info.delivered is not None else None
        return known(SubscriptionObservation(
            available = int(info.num_pending), leased = int(info.num_ack_pending), unresolved = exhausted,
            dropped = dropped, rejected_publications = rejected, observed_at = time.monotonic(),
            generation = generation), generation)

    def _still_retained(self, stream : str, seqs : Sequence[int]) -> list[int]:
        '''
        The subset of ``seqs`` the stream still holds. ``get_msg`` answers
        ``NotFoundError`` (err_code 10037, nats-py 2.15.0 ``nats/js/manager.py``)
        for a deleted sequence; any other failure keeps the sequence — an
        unobservable message is still unresolved, never resolved by assumption.
        '''
        async def _lookup(seq : int) -> bool:
            try:
                await self._js.get_msg(stream, seq)
            except nats.js.errors.NotFoundError:
                return False
            except Exception:  # noqa: BLE001 — unobservable: assume still stranded
                return True
            return True

        kept : list[int] = []
        for seq in seqs:
            try:
                retained = self._run(_lookup(seq), timeout = 5)
            except Exception:  # noqa: BLE001 — the bridge timed out: still unresolved
                retained = True
            if retained:
                kept.append(seq)
        return kept

    def _dropped_before(self, bound : _Bound, info : Any) -> int:
        '''
        Messages a LIMITS stream evicted before this subscription reached them: the
        stream's first retained sequence beyond the subscription's ack floor. Read
        from ``stream_info`` at most once a second per stream so the drain loop's
        polling does not double its broker traffic.
        '''
        with self._lock:
            effective = self._streams.get(bound.spec.id.channel)
        if effective is not None and effective.retention != RetentionPolicy.LIMITS:
            return 0
        now = time.monotonic()
        cached = self._stream_state_cache.get(bound.stream)
        if cached is None or now - cached[0] > 1.0:
            async def _go() -> Any:
                return await self._js.stream_info(bound.stream)
            try:
                state = self._run(_go(), timeout = 5).state
            except Exception:  # noqa: BLE001
                return 0
            self._stream_state_cache[bound.stream] = (now, state)
        else:
            state = cached[1]
        floor = int(info.ack_floor.stream_seq) if info.ack_floor is not None else 0
        first = int(state.first_seq)
        return max(0, first - floor - 1) if state.messages else 0

    # -- control channel ----------------------------------------------------------------

    def subscribe_control(self, callback : Callable[[], None]) -> None:
        '''Invoke ``callback`` (on the loop thread) when the run's flow-wide stop is published.'''
        subject = topology.control_subject_for(self._flow_id, self._run_id)

        async def _cb(_msg : Msg) -> None:
            callback()

        async def _go() -> None:
            await self._nc.subscribe(subject, cb = _cb)

        self._run(_go())

    # -- teardown -----------------------------------------------------------------------

    def close(self, owned : Sequence[ChannelId], expected_generation : str) -> CleanupObservation:
        '''Delete exactly the owned channels' streams by ownership (``topology.delete_run_streams``).'''
        faults.barrier('delete.before', owned = tuple(c.node for c in owned), generation = expected_generation)
        node_names = [c.node for c in owned]
        try:
            observation = self._run(topology.delete_run_streams(
                self._nc, self._flow_id, self._run_id, node_names = node_names,
                generation = expected_generation or None), timeout = 30)
        except Exception as e:  # noqa: BLE001
            return CleanupObservation(False, (), tuple(node_names), f'{type(e).__name__}: {e}')
        faults.barrier('delete.after', owned = tuple(node_names))
        return observation


def _stream_effective(config : StreamConfig) -> Dict[str, Any]:
    return {
        'name': config.name, 'subjects': list(config.subjects or []),
        'retention': str(config.retention), 'discard': str(config.discard),
        'max_msgs': config.max_msgs, 'max_bytes': config.max_bytes, 'max_age': config.max_age,
        'duplicate_window': config.duplicate_window, 'storage': str(config.storage),
        'num_replicas': config.num_replicas, 'metadata': dict(config.metadata or {}),
    }


def _consumer_effective(config : ConsumerConfig) -> Dict[str, Any]:
    return {
        'durable_name': config.durable_name, 'filter_subject': config.filter_subject,
        'ack_wait': config.ack_wait, 'max_deliver': config.max_deliver, 'max_ack_pending': config.max_ack_pending,
        'deliver_policy': str(config.deliver_policy), 'metadata': dict(config.metadata or {}),
    }


__all__ = [
    'ACK_CONFIRM_SECONDS', 'DEFAULT_PREFETCH', 'FETCH_TIMEOUT_SECONDS', 'LEGACY_BIND_CREDIT',
    'JetStreamMessagingBackend', 'channel_spec_for', 'consumer_credit', 'stream_config_for_spec',
    'subscription_spec_for', 'KIND_DATA', 'KIND_EOS', 'KIND_ABORT', 'KIND_DLQ',
]
