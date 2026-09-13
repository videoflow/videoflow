'''
NATS JetStream-backed implementation of ``videoflow.core.engine.Messenger``.

One JetStream stream per node (subject ``vf.{flow_id}.{node.name}``); a node's
messenger publishes only its own output there. Each real parent gets its own
durable pull consumer, named after the *consuming* node so that replicas of the
same consuming node (``nb_tasks > 1``) share one durable name (competing
consumers / load balancing), while distinct children of the same parent get
distinct durable names (each gets its own full copy — broadcast fan-out).

The transport itself — connection, loop thread, streams, durables, prefetch,
leases, settlement, publication outcomes — lives in
``videoflow.messaging.jetstream_backend.JetStreamMessagingBackend`` behind the
``MessagingBackend`` contract; this class composes it with a payload store and
keeps what is the *messenger's* to decide: trace and sequence bookkeeping, join
assembly, the end-of-stream drain, the delivery-policy ladder, dead-lettering,
and when a payload obligation is released (only after a *confirmed* settlement).
Every delivery is a ``DeliveryToken`` from the backend, so a superseded handle
can never terminate the attempt that replaced it, and every payload is fetched
on the receiving thread *after* ownership is decided, never on the broker loop.
'''

from __future__ import absolute_import, division, print_function

import hashlib
import logging
import random
import threading
import time
import uuid
from typing import Any, Optional

from ..backends import faults
from ..backends.capabilities import LIVE_LATEST, RELIABLE_WORK
from ..backends.messaging import (
    KIND_ABORT,
    KIND_DATA,
    KIND_DLQ,
    KIND_EOS,
    SUBSCRIPTION_DATA,
    SUBSCRIPTION_EOS,
    ChannelId,
    Completed,
    Delivery,
    DeliveryToken,
    Envelope,
    MessagingBackend,
    Retry,
    SubscriptionId,
    Terminal,
)
from ..backends.outcomes import (
    Accepted,
    Known,
    Observation,
    PublicationOutcome,
    PublicationUnknown,
    PublicationUnresolvable,
    Rejected,
    SettleConfirmed,
    SettleStale,
    SettleUnknown,
    Unknown,
    known,
    unknown,
)
from ..backends.payload import PayloadStore
from ..backends.payload_bridge import PayloadStoreBlobBridge
from ..core import constants
from ..core.constants import REALTIME
from ..core.engine import Messenger
from ..core.errors import (
    DEFAULT_DISPOSITION,
    POISON,
    TRANSIENT,
    BrokerUnavailable,
    ConfigError,
    DecodeError,
    TransientFailure,
    classify,
    error_to_dict,
)
from ..core.node import Node
from ..core.policies import (
    ACTION_DLQ_SAMPLED,
    ACTION_NAK,
    ACTION_TERM,
    BEST_EFFORT,
    JOIN_TIME,
    DeliveryPolicy,
    JoinPolicy,
)
from ..wire.serialization import (
    DEFAULT_ENVELOPE_VERSION,
    ENVELOPE_OVERHEAD_BYTES,
    MAX_INLINE_PAYLOAD_BYTES,
    MSG_TYPE_ABORT,
    MSG_TYPE_DATA,
    MSG_TYPE_EOS,
    BlobStore,
    decode_envelope,
    derive_message_id,
    encode_envelope,
    hydrate_message,
    peek_envelope,
    safe_inline_threshold,
)
from .grouping import EnvelopeEntry, make_assembler
from .jetstream_backend import (
    FETCH_TIMEOUT_SECONDS,
    JetStreamMessagingBackend,
    channel_spec_for,
    subscription_spec_for,
)
from .topology import (
    DLQ_RETENTION_SECONDS,
    LEGACY_BIND_CREDIT,
    consumer_credit,
    durable_name_for,
    partitioned_durable_name_for,
)

logger = logging.getLogger(__package__)

_FETCH_TIMEOUT_SECONDS = FETCH_TIMEOUT_SECONDS
# Flow-type blob TTL defaults (BLOB-7). REALTIME delivery is near-immediate (the
# stream holds one message and never redelivers), so the TTL bounds only leaked
# blobs. A BATCH Interest-retention backlog can legitimately delay a blob's *first*
# read past an hour, so a short TTL there is silent data loss; refcounted
# reclamation (BLOB-5/6) is what makes the long TTL affordable.
DEFAULT_BLOB_TTL_REALTIME_SECONDS = 3600
DEFAULT_BLOB_TTL_BATCH_SECONDS = 86400
# How many times a BATCH publish retries when the stream is full (backpressure)
# before giving up. Each retry rechecks the termination flag so a stopping flow
# doesn't wedge here forever.
_PUBLISH_RETRY_BACKOFF = [0.05, 0.1, 0.2, 0.5, 1.0]
# Hard bound on getting an ABORT marker out. A dying worker should spend seconds,
# not minutes, trying to tell its children — the supervisor's control-abort and
# the receiver's progress deadline cover the case where it never manages.
_ABORT_PUBLISH_TIMEOUT = 10
# Overall bound on a data/EOS publish, retries included — the same 120 s the
# messenger has always waited on the broker before giving the failure back to
# the task, which naks the input for a redelivery that re-publishes under the
# same dedup id.
_PUBLISH_TIMEOUT = 120
#: Bounded backoff between attempts of a dead-letter publish.
_DLQ_PUBLISH_BACKOFF = [0.1, 0.2, 0.3]
#: Delay of the NAK that keeps a message alive when its dead-letter publish failed.
_DLQ_RETRY_DELAY = 5

class _DlqSampler:
    '''
    Rate-limits dead-lettering to a bounded number of specimens per
    ``(code, node)`` per minute.

    This is what makes a best-effort node's failures visible without making its
    dead-letter queue unbounded. Dropping a message under load shedding is a
    policy; dropping the *evidence* of an exception is just losing the bug report.
    A handful of specimens per distinct failure per minute is enough to diagnose
    any of them, and the suppressed remainder is still counted in
    ``videoflow_errors_total``.

    - Arguments:
        - per_minute: specimens admitted per key per window. 0 admits nothing.
        - clock: monotonic time source; injected for tests.
    '''
    def __init__(self, per_minute : int, clock : Any = time.monotonic) -> None:
        self._per_minute = per_minute
        self._clock = clock
        self._windows : dict[str, tuple[float, int]] = {}

    def admit(self, code : str, node : str) -> bool:
        '''Whether this failure should be dead-lettered rather than only counted.'''
        if self._per_minute <= 0:
            return False
        key = f'{code}\x00{node}'
        now = self._clock()
        started, count = self._windows.get(key, (now, 0))
        if now - started >= 60.0:
            started, count = now, 0
        if count >= self._per_minute:
            self._windows[key] = (started, count)
            return False
        self._windows[key] = (started, count + 1)
        return True

class _AckHandle:
    '''
    A delivery the task loop can acknowledge, negatively-acknowledge, terminate,
    or retire, from the main thread, over a ``DeliveryToken`` the backend
    settles. Ack-after-process is the whole point of at-least-once delivery:
    these are called only *after* the node has processed the message (and
    published its output), never before.

    A settlement is *confirmed* or it is not: the payload obligation behind a
    delivery is released only on ``SettleConfirmed`` (PAY-004), and a token the
    backend reports as stale — a redelivery replaced it — settles nothing
    (MSG-011). ``supersede`` retires the handle locally without touching the
    broker, which is what a join assembler does with the older of two attempts.
    '''
    def __init__(self, token : DeliveryToken, messenger : 'NATSMessenger', blob_ref : str | None = None,
                 raw : bytes = b'') -> None:
        self._token = token
        self._m = messenger
        # Wire key of the payload the message's bytes were resolved from, released
        # after a confirmed ack (BLOB-6 / BLOB-14); None for inline payloads and EOS.
        self._blob_ref = blob_ref
        # The raw envelope bytes, for a dead-letter publish that must carry them verbatim.
        self._raw = raw
        self._resolved = False

    @property
    def token(self) -> DeliveryToken:
        return self._token

    @property
    def raw(self) -> bytes:
        return self._raw

    @property
    def blob_ref(self) -> str | None:
        return self._blob_ref

    @property
    def num_delivered(self) -> int:
        return self._token.attempt

    @property
    def stream_seq(self) -> Optional[int]:
        return self._token.stream_sequence

    def ack(self) -> None:
        if self._resolved:
            return
        self._resolved = True
        self._m._forget_handle(self)
        outcome = self._m._backend.settle(self._token, Completed(), f'ack:{self._token.message_id}:{self._token.attempt}')
        if isinstance(outcome, SettleConfirmed):
            # Release only on *confirmed* settlement: an unconfirmed ack may mean
            # the broker redelivers, and a redelivery re-reads the payload.
            self._m._release_after_settlement(self._blob_ref, outcome.settlement_id)
        elif isinstance(outcome, SettleStale):
            logger.debug(f'ack of a superseded attempt ignored (current attempt {outcome.current_attempt})')
        else:
            logger.debug(f'ack not confirmed ({outcome.reason}); payload obligation kept')

    def nak(self, delay : float | None = None) -> None:
        if self._resolved:
            return
        self._resolved = True
        self._m._forget_handle(self)
        outcome = self._m._backend.settle(self._token, Retry(delay), f'nak:{self._token.message_id}:{self._token.attempt}')
        if isinstance(outcome, SettleUnknown):
            logger.debug(f'nak not confirmed: {outcome.reason}')

    def term(self, record_ref : str) -> None:
        '''Terminate the delivery; ``record_ref`` names the durable record that outlives it (DELIV-15).'''
        if self._resolved:
            return
        self._resolved = True
        self._m._forget_handle(self)
        outcome = self._m._backend.settle(self._token, Terminal(record_ref), f'term:{self._token.message_id}:{self._token.attempt}')
        if isinstance(outcome, SettleConfirmed):
            self._m._release_after_terminal(self._blob_ref, outcome.settlement_id)
        elif isinstance(outcome, SettleUnknown):
            logger.debug(f'term not confirmed: {outcome.reason}')

    def supersede(self) -> None:
        '''Retire this handle locally — a newer attempt of the same message is now held. No broker call.'''
        if self._resolved:
            return
        self._resolved = True
        self._m._forget_handle(self)
        self._m._backend.supersede(self._token)

class NATSMessenger(Messenger):
    """
    - Arguments:
        - node: the ``videoflow.core.node.Node`` this messenger is bound to.
        - parent_names ([str]): the real parents of ``node``, by ``.name``.
        - nats_url (str): e.g. ``nats://localhost:4222``.
        - flow_id (str): shared across every node in the flow.
        - flow_type (str): ``videoflow.core.constants.REALTIME`` or ``BATCH`` — \
            controls the stream retention/discard policy used for ``node``'s own \
            output stream.
        - blob_store: optional ``videoflow.wire.serialization.BlobStore`` for payloads \
            over the inline size threshold (the RFC 0002 counter store).
        - blob_readers (int): how many downstream reads each message this node \
            publishes receives (Σ over children of ``nb_tasks`` if partitioned else 1, \
            computed by the compiler); enables refcounted blob reclamation (BLOB-5). \
            ``None`` disables it (blobs are TTL-only).
        - blob_ttl_seconds (int): TTL for offloaded payloads; ``None`` picks the \
            flow-type default (3600s realtime / 86400s batch, BLOB-7).
        - join_policy (dict): serialized ``videoflow.core.policies.JoinPolicy`` \
            controlling how multi-parent input groups are formed (by trace id or \
            by event time) and expired; defaults per flow type when unset. The \
            policy's ``max_pending`` bounds how many not-yet-complete groups are \
            held in memory before the oldest is evicted.
        - backend: the ``MessagingBackend`` to compose; a \
            ``JetStreamMessagingBackend`` on ``nats_url`` when None.
        - payload_store: an obligation-keeping ``PayloadStore`` (RFC 0006). Used \
            instead of ``blob_store`` when the ``VF_RFC0006`` switch is on: puts \
            acquire the reader obligations in ``blob_reader_ids`` and the \
            publisher's intent, releases happen by reader id after a confirmed \
            settlement (BLOB-14). Ignored with the switch off.
        - blob_reader_ids ([str]): the reader obligations every put acquires \
            (``VF_BLOB_READER_IDS``): ``<child>`` per competing child, \
            ``<child>/p<i>`` per partitioned replica.
    """
    def __init__(self, node : Node, parent_names : list[str], nats_url : str, flow_id : str,
                flow_type : str, run_id : str, blob_store : BlobStore | None = None,
                replica_id : int = 0, ack_wait : int = 60, max_retries : int = 3,
                eos_quiescence_ms : int = 500, nb_tasks : int = 1,
                partition_by : str | None = None, join_policy : dict | None = None,
                envelope_version : int | None = None, blob_readers : int | None = None,
                blob_ttl_seconds : int | None = None,
                delivery_policy : dict | None = None,
                backend : MessagingBackend | None = None,
                payload_store : PayloadStore | None = None,
                blob_reader_ids : list[str] | None = None) -> None:
        self._node = node
        # Wire version this node emits (the protobuf v4 envelope; §4 of PROTOCOL.md).
        self._envelope_version = DEFAULT_ENVELOPE_VERSION if envelope_version is None else envelope_version
        self._parent_names = list(parent_names)
        self._nats_url = nats_url
        self._flow_id = flow_id
        self._flow_type = flow_type
        self._run_id = run_id
        # Downstream read count for refcounted blob reclamation (BLOB-5): None means
        # the deployment didn't supply one, so blobs stay TTL-only.
        self._blob_readers = blob_readers
        # Publisher-chosen blob TTL (BLOB-7): explicit override, else flow-type default.
        self._blob_ttl_seconds = (blob_ttl_seconds if blob_ttl_seconds is not None
                            else (DEFAULT_BLOB_TTL_REALTIME_SECONDS if flow_type == REALTIME
                                else DEFAULT_BLOB_TTL_BATCH_SECONDS))
        self._replica_id = replica_id
        self._ack_wait = ack_wait
        # What a failure costs here. Resolved once: the flow-type preset, this
        # node's own delivery/on_error override, then the deployment's retry count.
        self._delivery_policy = DeliveryPolicy.resolve(flow_type, delivery_policy, max_retries)
        self._max_deliver = self._delivery_policy.max_deliver
        self._dlq_sampler = _DlqSampler(self._delivery_policy.sample_per_minute)
        self._eos_quiescence_s = max(0.0, eos_quiescence_ms / 1000.0)
        self._nb_tasks = nb_tasks
        # Partitioned iff a key is set and there's more than one replica.
        self._partition_by = partition_by if (partition_by and nb_tasks > 1) else None
        self._join_policy: JoinPolicy = (JoinPolicy.from_dict(join_policy)
                            if join_policy else None) or JoinPolicy.default_for(flow_type)
        if (self._join_policy.mode == JOIN_TIME and len(self._parent_names) > 1
                and nb_tasks > 1):
            # Replicas (competing or partitioned) would each see only some halves
            # of a time window, so no replica could ever complete a group.
            raise ValueError(f'{node.name}: a time-aligned join (join_policy '
                            f"mode='time') requires nb_tasks == 1, got {nb_tasks}")
        # Assembles multi-parent input groups (by trace id or by event time) and
        # owns all pending-group buffering/expiry — see videoflow.messaging.grouping.
        self._assembler = make_assembler(node.name, self._parent_names, self._join_policy)
        # Unique per replica: names this replica's EOS consumers so every replica
        # observes end-of-stream (the shared data durable would deliver EOS to only
        # one of them).
        self._instance_id = f'r{replica_id}-{uuid.uuid4().hex[:8]}'
        # This node's own reader obligation id (BLOB-13): one per competing node,
        # one per replica of a partitioned node.
        self._reader_id = f'{node.name}/p{replica_id}' if self._partition_by else node.name
        self._payload_store = payload_store if constants.RFC0006 else None
        self._blob_reader_ids = list(blob_reader_ids or [])
        # (publication id, content id) of the message being encoded, so the payload
        # bridge can name the publisher's intent and the content at put time.
        self._current_publication : Optional[tuple[str, str]] = None
        self._bridge : Optional[PayloadStoreBlobBridge] = None
        if self._payload_store is not None:
            self._bridge = PayloadStoreBlobBridge(
                self._payload_store, self._blob_reader_ids, horizon_seconds = self._blob_ttl_seconds,
                durable_required = flow_type != REALTIME,
                content_id = lambda: self._current_publication[1] if self._current_publication else '',
                intent = lambda: f'intent/{self._current_publication[0]}' if self._current_publication else None)
            self._blob_store : BlobStore | None = self._bridge
        else:
            self._blob_store = blob_store

        self._trace_counter = 0
        self._last_trace_id: Optional[str] = None
        # seq/event_ts are carried forward from the input group so a re-run of the
        # same logical output derives the same message_id (dedup) and event time
        # survives the whole pipeline. Producers use the local counter (and stamp
        # event time themselves); downstream nodes inherit the input group's values.
        self._last_seq = 0
        self._last_event_ts: Optional[float] = None
        self._last_input_info: Optional[dict[str, Optional[dict]]] = None
        # Optional partition key / event timestamp set by the node (via
        # ctx.set_partition_key / ctx.set_event_timestamp) and attached to the
        # next published message.
        self._output_partition_key = None
        self._output_event_ts: Optional[float] = None

        self._stopped_parents: set[str] = set()
        # EOS drain state: a parent is fully stopped only once its EOS has been
        # observed AND its data durable is quiescent (all data drained) — see
        # _is_parent_stopped. _eos_handles holds the EOS ack until drain completes.
        self._eos_seen: set[str] = set()
        self._eos_handles: dict[str, _AckHandle] = {}
        self._quiescent_since: dict[str, float] = {}
        # Parents whose terminator was an ABORT rather than a clean EOS, with the
        # error each carried. Surfaced through receive_message so the task can
        # report the real cause and relay it, instead of reporting a clean finish.
        self._aborted_parents: dict[str, dict] = {}
        # Consecutive empty receive polls — drives the periodic EOS-drain stall log.
        self._idle_polls = 0
        # Ack handles of the group last returned by receive_message, resolved by
        # the task via ack_inputs()/fail_inputs() (handles of still-pending groups
        # live inside the assembler).
        self._inflight_handles: list[_AckHandle] = []
        # Local record of every delivery this node terminated without a
        # dead-letter entry (sampled out, dlq off, undecodable with the switch
        # off): the durable record a Terminal settlement must name (DELIV-15).
        # In-memory until the runtime store (RFC 0006 CTRL-4) carries it.
        self._terminal_log: list[dict] = []
        #: Publication outcomes by kind: accepted, duplicate, unknown, dropped, rejected.
        self.publication_stats: dict[str, int] = {}
        # Drops decided below the Messenger seam, handed to the health seam by take_drops().
        self._drops: dict[str, int] = {}
        self._evictions_seen = 0
        # Payloads over this many encoded bytes are offloaded; lowered from the
        # module default when the broker's max_payload cannot carry it (PAY-001).
        self._inline_threshold = MAX_INLINE_PAYLOAD_BYTES

        # _termination_event: control-channel "stop the whole flow" signal, read by
        #   producers (to stop early) and by receive_message (to stop waiting).
        # _closing: set only by close().
        self._termination_event = threading.Event()
        self._closing = threading.Event()

        self._backend : MessagingBackend = backend if backend is not None else JetStreamMessagingBackend(
            nats_url, flow_id, run_id, flow_type)
        self._data_subs: dict[str, SubscriptionId] = {}
        self._eos_subs: dict[str, SubscriptionId] = {}
        self._setup()

    # -- lifecycle -----------------------------------------------------

    def _setup(self) -> None:
        backend = self._backend
        if isinstance(backend, JetStreamMessagingBackend):
            backend.start()
        self._negotiate_inline_threshold(backend.capabilities().max_payload_bytes)
        operation = f'{self._node.name}:r{self._replica_id}:{uuid.uuid4().hex[:6]}'
        profile = LIVE_LATEST if self._flow_type == REALTIME else RELIABLE_WORK
        # Provisioning (topology.provision_flow) normally creates streams up front;
        # ensuring them here is the idempotent fallback it always was.
        backend.ensure_channel(channel_spec_for(self._flow_id, self._run_id, self._node.name, self._flow_type,
                                                profile), operation)
        backend.subscribe_control(self._termination_event.set)
        for parent_name in self._parent_names:
            channel = ChannelId(self._flow_id, self._run_id, parent_name)
            backend.ensure_channel(channel_spec_for(self._flow_id, self._run_id, parent_name, self._flow_type,
                                                    profile), operation)
            # Data consumer: shared durable (competing consumers), or a per-replica
            # durable for a partitioned node (broadcast + client-side ownership).
            # The broker-side credit is the pre-RFC value until RFC 0006 derives it
            # from the replica count (STREAM-15), the same way provisioning does.
            credit = (consumer_credit(self._nb_tasks, self._partition_by is not None) if constants.RFC0006
                      else LEGACY_BIND_CREDIT)
            data = SubscriptionId(channel, self._node.name,
                                  self._replica_id if self._partition_by else None, SUBSCRIPTION_DATA)
            backend.ensure_subscription(subscription_spec_for(data, self._ack_wait, self._max_deliver, credit),
                                        operation)
            if self._partition_by or constants.RFC0006:
                # Decide ownership and replay scope where the delivery arrives, so a
                # message this replica will never process is acked out of its ack
                # window at once instead of waiting behind the one being processed;
                # a backend without the filter gets the same decision in _admit_data.
                backend.set_admission(data, self._admit_on_loop, self._on_skipped)
            # EOS consumer: per-replica durable so every replica observes EOS.
            eos = SubscriptionId(channel, self._node.name, None, SUBSCRIPTION_EOS, instance = self._instance_id)
            backend.ensure_subscription(subscription_spec_for(eos, 30, 1, 1), operation)
            self._data_subs[parent_name] = data
            self._eos_subs[parent_name] = eos

    def _negotiate_inline_threshold(self, max_payload : Observation[int]) -> None:
        """
        PAY-001: the offload threshold is measured on the encoded payload and must \
            leave room for the envelope inside the broker's ``max_payload``. An unsafe \
            ``VIDEOFLOW_MAX_INLINE_PAYLOAD_BYTES`` is lowered to what the broker can \
            carry rather than letting a publish be refused after the fact; with no \
            store to offload to, that is a configuration error under RFC 0006 (the \
            payloads it would reject cannot be published at all) and a warning \
            otherwise. An unread limit changes nothing.
        """
        if not isinstance(max_payload, Known):
            return
        safe = safe_inline_threshold(max_payload.value, self._inline_threshold)
        if safe >= self._inline_threshold:
            return
        if self._blob_store is None and constants.RFC0006:
            raise ConfigError(
                f'{self._node.name}: the inline payload threshold ({self._inline_threshold} bytes) exceeds what '
                f'the broker can carry (max_payload {max_payload.value} bytes, {ENVELOPE_OVERHEAD_BYTES} reserved '
                'for the envelope) and no blob store is configured to offload the difference to.',
                remedy = f'Set VIDEOFLOW_MAX_INLINE_PAYLOAD_BYTES to at most {safe}, or configure '
                         'VF_BLOB_REDIS_URL / --blob-redis-url so larger payloads are offloaded.')
        logger.warning(f'{self._node.name}: inline payload threshold lowered from {self._inline_threshold} to {safe} '
                       f'bytes: the broker max_payload is {max_payload.value} bytes')
        self._inline_threshold = safe

    def _data_durable_name(self, parent_name : str) -> str:
        if self._partition_by:
            return partitioned_durable_name_for(self._node.name, parent_name, self._replica_id)
        return durable_name_for(self._node.name, parent_name)

    def _owns(self, entry : EnvelopeEntry) -> bool:
        """For a partitioned node, whether this replica owns the message (hash of the partition key modulo replica count)."""
        return self._owns_key(entry.trace_id, entry.metadata)

    def _owns_key(self, trace_id : str | None, metadata : dict | None) -> bool:
        if not self._partition_by:
            return True
        key : Any
        if self._partition_by == 'trace_id':
            key = trace_id
        else:
            key = (metadata or {}).get(self._partition_by)
        digest = hashlib.sha256(str(key).encode('utf-8')).hexdigest()
        return (int(digest[:8], 16) % self._nb_tasks) == self._replica_id

    def _replay_addressed_elsewhere(self, headers : Any) -> bool:
        """A replayed dead letter names its target (DELIV-16): honoured under RFC 0006, every child processes it otherwise."""
        target = headers.get('VF-Replay-Target') if headers else None
        return bool(constants.RFC0006 and target and target != self._node.name)

    def _admit_on_loop(self, delivery : Delivery) -> bool:
        """
        The backend's admission filter, on its thread: replay scope from the \
            headers and ownership from a payload-free peek at the envelope, so a \
            delivery this replica will never process is acked away before it is \
            parked (PART-4, DELIV-16, PAY-018). Undecodable bytes are admitted: the \
            receiving side records them as poison.
        """
        if self._replay_addressed_elsewhere(delivery.headers):
            return False
        if not self._partition_by:
            return True
        try:
            peeked = peek_envelope(delivery.envelope_bytes)
        except Exception:  # noqa: BLE001 — classified on the receiving side
            return True
        if peeked.get('is_stop_signal'):
            return True
        return self._owns_key(peeked.get('trace_id'), peeked.get('metadata'))

    def _on_skipped(self, delivery : Delivery) -> None:
        """
        After a *confirmed* ack-and-skip (on the backend's executor): a non-owner \
            releases its own share of the payload — reader obligation or refcount — \
            exactly as its ack would, without having fetched it. A replay skipped \
            for scope releases nothing: this node never acquired an obligation on it.
        """
        if self._replay_addressed_elsewhere(delivery.headers):
            return
        try:
            ref = peek_envelope(delivery.envelope_bytes).get('blob_ref')
        except Exception:  # noqa: BLE001 — nothing to release for bytes that do not decode
            return
        self._release_after_settlement(ref, f'skip:{delivery.token.message_id}:{delivery.token.attempt}')

    def _release_after_settlement(self, blob_ref : str | None, settlement_id : str) -> None:
        """
        Release this node's claim on a payload after a *confirmed* settlement of the \
            message that carried it: the reader obligation by id under RFC 0006 \
            (BLOB-14), the refcount decrement otherwise (BLOB-6). Failure is logged \
            and swallowed: the TTL backstop (BLOB-7) reclaims anything a failed \
            release leaks.
        """
        if blob_ref is None:
            return
        try:
            if self._payload_store is not None:
                faults.barrier('obligation.release.before', ref = blob_ref, reader = self._reader_id)
                receipt = self._payload_store.release_obligation(self._payload_store.ref_for_key(blob_ref),
                                                                 self._reader_id, settlement_id)
                faults.barrier('obligation.release.after', ref = blob_ref, reader = self._reader_id)
                if receipt.unknown:
                    # The store may or may not have applied it; a repeat is idempotent
                    # by reader id, and reconciliation settles what a lost reply left.
                    logger.warning(f'release of {self._reader_id} on {blob_ref} not confirmed ({settlement_id})')
            elif self._blob_store is not None:
                self._blob_store.release(blob_ref)
        except Exception:  # noqa: BLE001 — the TTL is the backstop; a release must never kill the node
            logger.debug(f'payload release failed for {blob_ref}', exc_info = True)

    def _release_after_terminal(self, blob_ref : str | None, settlement_id : str) -> None:
        """
        A *terminated* delivery releases this node's reader obligation only under \
            the obligation model, where the dead letter pinned the payload for the \
            DLQ retention first (BLOB-14 step 3). Under the legacy refcount the dead \
            letter re-reads the blob, so a termination leaves it to its TTL (BLOB-6).
        """
        if self._payload_store is None:
            return
        self._release_after_settlement(blob_ref, settlement_id)

    def _forget_handle(self, handle : _AckHandle) -> None:
        return None

    def _count_drop(self, reason : str) -> None:
        self._drops[reason] = self._drops.get(reason, 0) + 1

    def take_drops(self) -> dict[str, int]:
        """
        Inputs this messenger gave up on since the last call, by reason, that no \
            caller above the ``Messenger`` seam could have seen: a retry budget \
            exhausted (``exhausted``), bytes that could not be decoded \
            (``undecodable``), a join group evicted (``join_evicted``), a live \
            publication the broker refused (``publish_discarded``).
        """
        evicted = self._assembler.evictions - self._evictions_seen
        if evicted:
            self._evictions_seen = self._assembler.evictions
            self._drops['join_evicted'] = self._drops.get('join_evicted', 0) + evicted
        drops, self._drops = self._drops, {}
        return drops

    def close(self) -> None:
        self._closing.set()
        self._backend.shutdown()

    # -- Messenger interface ---------------------------------------------

    def check_for_termination(self) -> bool:
        return self._termination_event.is_set()

    def quiesce(self) -> None:
        """Stop pulling new input on SIGTERM; what is already held still settles normally."""
        self._termination_event.set()

    def set_output_partition_key(self, value : Any) -> None:
        self._output_partition_key = value

    def set_output_event_timestamp(self, value : float) -> None:
        self._output_event_ts = value

    def last_input_info(self) -> Optional[dict[str, Optional[dict]]]:
        """
        Per-parent envelope info (``event_ts``, ``metadata``, ``trace_id``, ``seq``)
        for the input group last returned by ``receive_message``; ``None`` entries
        for parents missing from a quorum emission. ``None`` for producers.
        """
        return self._last_input_info

    def last_input_key(self) -> Optional[str]:
        """
        A stable identity for the input group last returned by ``receive_message``,
        derived from its trace_id + seq — used as an idempotency key by a sink. The
        same logical event yields the same key across redelivery/restart.
        """
        if self._last_trace_id is None:
            return None
        return derive_message_id(self._flow_id, self._run_id, self._node.name,
                                self._last_trace_id, self._last_seq, MSG_TYPE_DATA)

    def publish_message(self, message : Any, metadata : Optional[dict] = None) -> None:
        trace_id = self._last_trace_id
        seq = self._last_seq
        if trace_id is None:
            # Only a producer (no parents) mints fresh trace ids; everything
            # downstream carries forward the trace id + seq of the input group it
            # was derived from, so a re-run derives the same message_id (dedup).
            self._trace_counter += 1
            trace_id = f'{self._node.name}:{self._trace_counter}'
            seq = self._trace_counter
        # Event time: an explicit stamp from the node (ctx.set_event_timestamp)
        # wins; otherwise it is inherited from the input group; a producer with
        # neither gets publish wall-clock as a last resort.
        if self._output_event_ts is not None:
            event_ts = self._output_event_ts
            self._output_event_ts = None
        elif self._last_event_ts is not None:
            event_ts = self._last_event_ts
        else:
            event_ts = time.time()
        if self._output_partition_key is not None:
            metadata = dict(metadata or {})
            metadata['_partition_key'] = self._output_partition_key
            self._output_partition_key = None
        self._publish(message, metadata, trace_id, seq, MSG_TYPE_DATA, event_ts = event_ts)

    def publish_stop_signal(self) -> None:
        # EOS goes on this node's dedicated _eos subject (not the data subject), so
        # every downstream replica observes it via its own EOS consumer. The dedup
        # id includes replica_id so EOS markers from different replicas of one node
        # don't collapse into a single one.
        eos_trace = f'eos-r{self._replica_id}'
        self._publish(None, None, eos_trace, self._last_seq, MSG_TYPE_EOS)

    def publish_abort(self, error : Any) -> None:
        """
        Publishes an abnormal end-of-stream carrying why this node died.

        Rides the same ``_eos`` subject as a clean EOS, which is the point: it
        reuses the per-replica EOS consumers and the provisioning interest anchor
        unchanged, so a marker published by a dying node is still retained and
        still reaches every downstream replica. Its dedup id is distinct from the
        clean marker's so a node that aborts is never mistaken for one that
        finished.
        """
        abort_trace = f'abort-r{self._replica_id}'
        self._publish(None, None, abort_trace, self._last_seq, MSG_TYPE_ABORT,
                    error = error_to_dict(error))

    def pending_count(self) -> int:
        """
        Messages waiting for this node across all its parents — locally prefetched,
        held in incomplete join groups, or still on the broker. Parents whose
        broker query failed contribute only their local queue; use
        ``pending_observation`` where "failed" must not read as "zero".
        """
        observed = self.pending_observation()
        return observed.value if isinstance(observed, Known) else self._local_pending()

    def _local_pending(self) -> int:
        return sum(self._backend.prefetched(sub) for sub in self._data_subs.values())

    def pending_observation(self) -> Observation[int]:
        """
        ``pending_count`` as an observation: ``Unknown`` as soon as any parent's
        broker query failed, because a total that silently omits one parent is
        exactly the "zero pending" that declared a stalled node idle. Feeds
        ``ProgressDeadline``, which neither resets nor trips on Unknown.
        """
        total = self._local_pending()
        for parent in self._parent_names:
            observed = self._consumer_pending(parent)
            if isinstance(observed, Unknown):
                return observed
            num_pending, num_ack_pending = observed.value
            total += num_pending + num_ack_pending
        return known(total)

    def subscription_status(self) -> dict[str, Observation[Any]]:
        """
        Per parent, the backend's observation of this node's data subscription —
        ``available``, ``leased``, ``unresolved`` (retained but exhausted: never
        "all done", MSG-012), ``dropped`` — or ``Unknown`` when it could not be read.
        """
        return {parent: self._backend.observe_subscription(sub) for parent, sub in self._data_subs.items()}

    # -- publishing ----------------------------------------------------------

    def _count(self, kind : str) -> None:
        self.publication_stats[kind] = self.publication_stats.get(kind, 0) + 1

    def _publish(self, message : Any, metadata : Optional[dict], trace_id : str, seq : int,
                msg_type : str, event_ts : float | None = None,
                error : Optional[dict] = None) -> None:
        node_name = self._node.name
        # Content-derived dedup id: a re-published retry of the same logical message
        # is dropped by JetStream within the stream's duplicate_window. It is also
        # the publication's operation identity for the payload intent (BLOB-14).
        publication_id = derive_message_id(self._flow_id, self._run_id, node_name, trace_id, seq, msg_type)
        self._current_publication = (publication_id, f'{node_name}:{trace_id}:{seq}')
        if self._bridge is not None:
            self._bridge.last_ref = None
        try:
            buf = encode_envelope(
                node_name, self._flow_id, self._run_id, trace_id, seq, msg_type,
                metadata, message, replica_id = self._replica_id, event_ts = event_ts,
                blob_store = self._blob_store, version = self._envelope_version,
                blob_readers = self._blob_readers, blob_ttl_seconds = self._blob_ttl_seconds,
                error = error, inline_threshold = self._inline_threshold,
            )
        finally:
            self._current_publication = None
        put_ref = self._bridge.last_ref if self._bridge is not None else None
        kind = KIND_EOS if msg_type == MSG_TYPE_EOS else KIND_ABORT if msg_type == MSG_TYPE_ABORT else KIND_DATA
        # The VF-Env header lets tooling (and the DLQ inspector) identify the wire
        # version without decoding.
        headers = {'Nats-Msg-Id': publication_id, 'VF-Env': str(self._envelope_version)}
        partition_key = (metadata or {}).get('_partition_key') if metadata else None
        envelope = Envelope(
            channel = ChannelId(self._flow_id, self._run_id, node_name), publication_id = publication_id,
            headers = headers, body = buf, size = len(buf), event_id = publication_id,
            partition_key = None if partition_key is None else str(partition_key), event_ts = event_ts,
            source_epoch = None, source_offset = None, schema_version = self._envelope_version, kind = kind)
        outcome, failure, orphaned = self._publish_envelope(envelope, is_abort = msg_type == MSG_TYPE_ABORT)
        # Settled on the *final* outcome, error or not: a definite refusal frees the
        # object nobody will ever name, an unconfirmed send keeps its intent for
        # reconciliation to resolve (BLOB-14 step 1).
        self._resolve_intent(publication_id, put_ref, outcome, orphaned)
        if failure is not None:
            raise failure

    def _publish_envelope(self, envelope : Envelope,
                          is_abort : bool) -> tuple[PublicationOutcome, BrokerUnavailable | None, bool]:
        """
        Drive one publication to a truthful outcome. REALTIME (Discard=OLD) never
        blocks: a refusal is a counted drop, an unconfirmed send stays unknown.
        BATCH (Interest + Discard=NEW): a full stream *rejects* the publish, so a
        retryable refusal backs off and retries — real backpressure instead of
        lost data — and an unknown send is reconciled by an idempotent re-publish
        under the same dedup id (MSG-013/014). An ABORT is published by a worker
        that is already dying, so it gets a short bound rather than BATCH's patience.

        - Raises:
            - BrokerUnavailable: a BATCH publication could not be made definite \
                within ``_PUBLISH_TIMEOUT`` (or a definite refusal). The task naks \
                its inputs, and the redelivery re-publishes the same message id.
        """
        pid = envelope.publication_id
        is_realtime = self._flow_type == REALTIME
        max_attempts = 3 if is_abort else None
        overall = time.monotonic() + (_ABORT_PUBLISH_TIMEOUT if is_abort else _PUBLISH_TIMEOUT)
        attempt = 0
        while True:
            step = min(overall, time.monotonic() + (_ABORT_PUBLISH_TIMEOUT if is_abort else 30.0))
            outcome = self._backend.publish(envelope, step)
            if isinstance(outcome, Accepted):
                self._count('duplicate' if outcome.duplicate else 'accepted')
                return outcome, None, bool(outcome.duplicate)
            if isinstance(outcome, (PublicationUnknown, PublicationUnresolvable)):
                self._count('unknown')
                resolved = self._backend.observe_publication(envelope)
                if isinstance(resolved, Accepted):
                    self._count('duplicate' if resolved.duplicate else 'accepted')
                    return resolved, None, False           # our own send landed: same object
                if is_realtime:
                    return outcome, None, False             # freshest wins; the ambiguity is on record
                if time.monotonic() >= overall or self._termination_event.is_set():
                    return outcome, BrokerUnavailable(
                        f'{self._node.name}: publication {pid} could not be confirmed ({outcome.reason})',
                        remedy = 'The input is handed back for redelivery; the retry re-publishes the same '
                                 'message id, which the broker deduplicates.', node = self._node.name), False
                time.sleep(_PUBLISH_RETRY_BACKOFF[min(attempt, len(_PUBLISH_RETRY_BACKOFF) - 1)])
                attempt += 1
                continue
            assert isinstance(outcome, Rejected), outcome
            if is_realtime:
                self._count('dropped')
                self._count_drop('publish_discarded')
                logger.debug(f'{self._node.name}: live publication {pid} dropped: {outcome.reason}')
                return outcome, None, True
            self._count('rejected')
            if not outcome.retryable or self._termination_event.is_set():
                return outcome, BrokerUnavailable(
                    f'{self._node.name}: publication {pid} rejected by the broker: {outcome.reason}',
                    remedy = 'Check the stream exists and its limits; the input is handed back for redelivery.',
                    node = self._node.name), True
            if (max_attempts is not None and attempt >= max_attempts) or time.monotonic() >= overall:
                return outcome, BrokerUnavailable(
                    f'{self._node.name}: publication {pid} still refused after {attempt} attempts: {outcome.reason}',
                    remedy = 'The stream is full; slow the producer or raise the stream limits.',
                    node = self._node.name), True
            time.sleep(_PUBLISH_RETRY_BACKOFF[min(attempt, len(_PUBLISH_RETRY_BACKOFF) - 1)])
            attempt += 1

    def _resolve_intent(self, publication_id : str, ref : Any, outcome : PublicationOutcome,
                        orphaned : bool = False) -> None:
        """
        Release the publisher's ``intent/<id>`` obligation once the send is definite \
            (BLOB-14 step 1) — and, when ``orphaned``, the reader shares too: no \
            message names the object written for this attempt (the broker refused \
            it, or kept an earlier attempt's publication of the same id, whose \
            object the readers will fetch), so it is reclaimed at once.
        """
        if self._payload_store is None or ref is None:
            return
        if not isinstance(outcome, (Accepted, Rejected)):
            return
        obligations = [f'intent/{publication_id}']
        if orphaned:
            obligations += self._blob_reader_ids
        for obligation in obligations:
            try:
                receipt = self._payload_store.release_obligation(ref, obligation, f'publish:{publication_id}')
                if receipt.unknown:
                    logger.warning(f'release of {obligation} on {ref.key} not confirmed; reconciliation will settle it')
            except Exception:  # noqa: BLE001 — reconciliation reclaims what a failed release leaves
                logger.debug(f'release of {obligation} on {ref.key} failed', exc_info = True)

    # -- ack / fail (called by the task after process()/consume()) --------

    def ack_inputs(self) -> None:
        """Acknowledge the input group last returned by ``receive_message`` — the node processed it successfully (and, for a processor, already published its output)."""
        for handle in self._inflight_handles:
            handle.ack()
        self._inflight_handles = []

    def fail_inputs(self, exc : BaseException) -> None:
        """
        The node raised while processing the last input group. The action is
        decided by ``DeliveryPolicy.action_for`` from *how the error classified*
        and how many times the broker has delivered it — not by the flow type
        alone. This messenger only executes the verdict.

        The difference that matters: a poison message is dead-lettered on its
        first failure rather than burning four attempts on its way to the same
        place, and a worker-fatal error naks without dead-lettering, because the
        message is fine and this worker is not. A delivery is terminated only
        against a durable record: the dead letter's id when one was accepted, an
        entry in this node's terminal log otherwise (DELIV-15); a dead-letter
        publish that failed keeps the delivery alive for a later attempt.
        """
        disposition = classify(exc, self._delivery_policy.on_error or DEFAULT_DISPOSITION)
        error = dict(error_to_dict(exc))
        # Stamp the disposition that was actually *used*, not the one the exception
        # happened to carry: a bare ValueError has none, and that is precisely the
        # case where the classifier did the work and the record must say so.
        error['disposition'] = disposition
        for handle in self._inflight_handles:
            self._settle_failure(handle, error, disposition)
        self._inflight_handles = []

    def _settle_failure(self, handle : _AckHandle, error : dict, disposition : str) -> None:
        '''Execute the ladder's verdict for one delivery of a failed input (``DeliveryPolicy.action_for``).'''
        action = self._delivery_policy.action_for(disposition, handle.num_delivered)
        if action != ACTION_NAK and disposition == TRANSIENT and self._delivery_policy.delivery != BEST_EFFORT:
            # A retry budget running out is a drop only this seam can see: the
            # health seam counts the verdicts it can tell from the disposition
            # alone (poison, best-effort), and this one depends on the attempt.
            self._count_drop('exhausted')
        if action == ACTION_TERM:
            handle.term(self._terminal_record(handle, error, 'terminated'))
        elif action == ACTION_NAK:
            delay = self._delivery_policy.retry_delay(handle.num_delivered, jitter = random.uniform(0.5, 1.5))
            handle.nak(delay = delay)
        elif action == ACTION_DLQ_SAMPLED:
            # Bounded specimens per distinct failure: enough to diagnose, never
            # enough to fill a stream.
            code = str(error.get('code', 'VF_UNKNOWN'))
            record = None
            admitted = self._dlq_sampler.admit(code, self._node.name)
            if admitted:
                record = self._dlq_publish(handle, error) or None
                if record is None and constants.RFC0006:
                    # The specimen was owed and could not be recorded: keep the
                    # delivery for a later attempt rather than lose the evidence.
                    handle.nak(delay = _DLQ_RETRY_DELAY)
                    return
            handle.term(record or self._terminal_record(handle, error,
                                                        'dlq-publish-failed' if admitted else 'sampled-out'))
        else:                                   # ACTION_DLQ
            record = self._dlq_publish(handle, error) or None
            if record is not None:
                handle.term(record)
            else:
                # Never silently drop: if the DLQ publish itself failed, keep the
                # message alive (nak) so a later attempt can dead-letter it.
                handle.nak(delay = _DLQ_RETRY_DELAY)

    def _terminal_record(self, handle : _AckHandle, error : dict, reason : str) -> str:
        """Append a terminal entry for a delivery ending without a dead letter and return its record reference."""
        self._terminal_log.append({
            'message_id': handle.token.message_id, 'stream_seq': handle.stream_seq,
            'attempt': handle.num_delivered, 'code': str(error.get('code', 'VF_UNKNOWN')),
            'reason': reason, 'at': time.time(),
        })
        return f'ledger:{self._node.name}/terminal:{len(self._terminal_log)}'

    def _dlq_publish(self, handle : _AckHandle, error : dict) -> str | None:
        """
        Dead-letter the raw bytes of a delivery. Returns the dead letter's message id
        (the durable record a Terminal settlement names) once the broker accepted it
        — a ``duplicate`` acceptance counts — or None when it could not be recorded.
        Under RFC 0006 with a payload store the dead letter pins the payload for the
        DLQ retention first (BLOB-14 step 3), so the bytes outlive the run.
        """
        seq = handle.stream_seq
        record_id = f'dlq:{self._flow_id}:{self._run_id}:{self._node.name}:{seq}'
        headers = {
            'VF-Origin-Node': self._node.name,
            'VF-Run-Id': self._run_id,
            # Structured, so dead letters can be grouped and alerted on. The old
            # repr(exc) was free text: unaggregatable, and never the same twice.
            'VF-Code': str(error.get('code', 'VF_UNKNOWN')),
            'VF-Disposition': str(error.get('disposition', '')),
            'VF-Error': str(error.get('message', ''))[:256],
            'VF-Remedy': str(error.get('remedy') or '')[:256],
            'VF-Num-Delivered': str(handle.num_delivered),
            # Idempotent DLQ id (stream seq is unique per original message), so a
            # re-attempt of the same dead-letter doesn't duplicate it.
            'Nats-Msg-Id': record_id,
        }
        if self._payload_store is not None and handle.blob_ref is not None:
            try:
                ref = self._payload_store.ref_for_key(handle.blob_ref)
                self._payload_store.acquire_obligation(ref, f'dlq/{self._flow_id}',
                                                       time.time() + DLQ_RETENTION_SECONDS)
            except LookupError:
                # The object is already gone (expired, evicted, or the very reason
                # this delivery is dead-lettered): there is nothing to pin, and the
                # dead letter records that — the envelope still names the ref.
                logger.debug(f'dead-letter payload {handle.blob_ref} is not stored; nothing to pin')
            except Exception:  # noqa: BLE001 — the store did not answer: the bytes may exist and are not pinned
                # A dead letter whose payload may vanish before anyone inspects it is
                # not a record (BLOB-14 step 3): the delivery is kept for a later attempt.
                logger.warning(f'dead-letter payload pin failed for {handle.blob_ref}; keeping the delivery',
                               exc_info = True)
                return None
        envelope = Envelope(
            channel = ChannelId(self._flow_id, self._run_id, self._node.name), publication_id = record_id,
            headers = headers, body = handle.raw, size = len(handle.raw), event_id = record_id,
            partition_key = None, event_ts = None, source_epoch = None, source_offset = None,
            schema_version = self._envelope_version, kind = KIND_DLQ)
        for backoff in _DLQ_PUBLISH_BACKOFF:
            outcome = self._backend.publish(envelope, time.monotonic() + 5.0)
            if isinstance(outcome, Accepted):
                return record_id
            if isinstance(outcome, (PublicationUnknown, PublicationUnresolvable)):
                resolved = self._backend.observe_publication(envelope)
                if isinstance(resolved, Accepted):
                    return record_id
            elif isinstance(outcome, Rejected) and not outcome.retryable:
                logger.error(f'dead-letter publish rejected for {record_id}: {outcome.reason}')
                return None
            time.sleep(backoff)
        logger.error(f'dead-letter publish could not be confirmed for {record_id}')
        return None

    # -- receiving ------------------------------------------------------------

    def receive_message(self) -> dict:
        while True:
            # A control-channel stop ends the flow immediately, even mid-stream —
            # surface it to the task loop as an all-parents-stopped result so
            # ConsumerTask/ProcessorTask break out and run close(). Otherwise a
            # parent is "stopped" only once its EOS is seen and its data is drained.
            if (self._termination_event.is_set() or self._all_parents_stopped()
                    or self._any_parent_aborted_and_drained()):
                self._last_trace_id = None
                self._last_input_info = None
                return self._terminal_result()

            self._assembler.sweep()

            ready = self._assembler.pop_ready()
            if ready is not None:
                self._last_trace_id = ready.trace_id
                # Deterministic representative seq/event_ts carried forward so this
                # node's output derives a stable message_id across retries and
                # keeps its event time.
                self._last_seq = ready.seq
                self._last_event_ts = ready.event_ts
                # Hold this group's handles for the task to ack/fail after process().
                self._inflight_handles = list(ready.handles)
                out: dict[str, dict[str, Any]] = {}
                info: dict = {}
                for name in self._parent_names:
                    entry = ready.entries.get(name)
                    if entry is None:
                        # Parent missing from a quorum emission: the node sees None.
                        out[name] = {'message': None, 'metadata': None,
                                    'is_stop_signal': False, 'event_ts': None}
                        info[name] = None
                        continue
                    # A CollectEntry carries lists and has no lineage of its own,
                    # so it reports trace_id/seq as None — as it always has.
                    out[name] = {
                        'message': entry.message,
                        'metadata': entry.metadata,
                        'is_stop_signal': False,
                        'event_ts': entry.event_ts,
                    }
                    info[name] = {
                        'event_ts': entry.event_ts,
                        'metadata': entry.metadata,
                        'trace_id': entry.trace_id if isinstance(entry, EnvelopeEntry) else None,
                        'seq': entry.seq if isinstance(entry, EnvelopeEntry) else None,
                    }
                self._last_input_info = info
                return out

            # Everything the backend reported ready in one wait is folded into the
            # assembler here — every one of those items has already left its
            # queue, so discarding any of them would lose a message.
            ready_items = self._recv_ready()
            if ready_items:
                self._idle_polls = 0
            else:
                # Nothing arriving. If we're in the EOS-drain phase (some parent
                # already ended) and still not stopped after ~15s of idle polls,
                # say why — a stall here otherwise looks like a silent hang.
                self._idle_polls += 1
                if self._eos_seen and self._idle_polls % 15 == 0:
                    self._log_drain_stall()
            for parent_name, entry, handle in ready_items:
                if entry.is_stop_signal:
                    # Data consumers filter to the data subject, so EOS is handled by
                    # the EOS subscriptions, not here. Ack defensively if one slips through.
                    handle.ack()
                    continue
                self._assembler.add(parent_name, entry, handle)

    def _recv_ready(self) -> list[tuple[str, EnvelopeEntry, _AckHandle]]:
        """
        Waits up to a short timeout for any parent subscription to hold a delivery, \
            then returns every *data* item that became ready in that wait as \
            ``[(parent_name, entry, handle), ...]`` — possibly empty on timeout, \
            which lets ``receive_message`` loop back and re-check the termination \
            event instead of blocking forever when the flow is being torn down. \
            Terminators are recorded here (``_on_terminator``); deliveries this \
            replica does not own, or that a replay addressed elsewhere, are \
            acked-and-skipped before any payload is fetched; the rest are hydrated.
        """
        subscriptions = list(self._data_subs.values()) + list(self._eos_subs.values())
        if not subscriptions:
            time.sleep(_FETCH_TIMEOUT_SECONDS)
            return []
        deadline = time.monotonic() + _FETCH_TIMEOUT_SECONDS
        results : list[tuple[str, EnvelopeEntry, _AckHandle]] = []
        for subscription, delivery in self._backend.receive_any(subscriptions, 1, 1 << 30, deadline):
            parent_name = subscription.channel.node
            if subscription.kind == SUBSCRIPTION_EOS:
                self._on_terminator(parent_name, delivery)
                continue
            admitted = self._admit_data(parent_name, delivery)
            if admitted is not None:
                results.append(admitted)
        return results

    def _on_terminator(self, parent_name : str, delivery : Delivery) -> None:
        # A parent's end-of-stream — clean (EOS) or abnormal (ABORT); both ride
        # this subject. The marker is *not* acked here: it's held (in
        # _eos_handles) and acked only once the parent's data is fully drained
        # (see _is_parent_stopped), so a crash mid-drain leaves it un-acked and
        # re-observable on restart.
        aborted = False
        try:
            decoded = decode_envelope(delivery.envelope_bytes, resolve_blobs = False)
            aborted = bool(decoded.get('is_abort'))
        except Exception:  # noqa: BLE001
            # An undecodable terminator still means "this parent ended".
            # Refusing to stop because the *reason* was unreadable would
            # trade a diagnosable failure for a hang.
            logger.debug(f'could not decode terminator from {parent_name}', exc_info = True)
            decoded = {}
        if aborted:
            # An abort outranks a clean EOS from the same parent: one
            # replica finishing normally does not undo another one dying.
            self._aborted_parents[parent_name] = decoded.get('error') or {}
        handle = _AckHandle(delivery.token, self, raw = delivery.envelope_bytes)
        if parent_name in self._eos_seen:
            # Already saw a terminator from this parent (another replica's
            # marker): ack the extra and move on.
            handle.ack()
            return
        self._eos_seen.add(parent_name)
        self._eos_handles[parent_name] = handle

    def _admit_data(self, parent_name : str, delivery : Delivery) -> tuple[str, EnvelopeEntry, _AckHandle] | None:
        try:
            # The one place a decoded envelope crosses into messaging: adapt the
            # wire dict to the typed record here so nothing downstream (join, EOS
            # drain, ownership) reads it by key. Metadata only, for now.
            decoded = decode_envelope(delivery.envelope_bytes, resolve_blobs = False)
            entry = EnvelopeEntry.from_decoded(decoded)
        except Exception as e:  # noqa: BLE001 — anything undecodable is poison at the transport
            self._discard_undecodable(parent_name, delivery, e)
            return None
        handle = _AckHandle(delivery.token, self, blob_ref = entry.blob_ref, raw = delivery.envelope_bytes)
        # A replayed dead letter names its target (DELIV-16): any other child of
        # the parent acks and skips it without fetching its payload, and without
        # releasing an obligation it never acquired. (Already decided on the
        # backend's thread where it runs the admission filter; this is the path
        # for a backend without one.)
        if self._replay_addressed_elsewhere(delivery.headers):
            self._backend.settle(delivery.token, Completed(), f'skip:{delivery.token.message_id}')
            return None
        # Partitioned node: this replica keeps only the messages it owns and
        # acks-and-skips the rest (every replica sees every message on its own
        # durable). Ownership is stable across replicas via hashing, and decided
        # before any payload is fetched (PAY-018). The non-owner's confirmed ack
        # releases its own reader share (BLOB-6 / its ``<node>/p<i>`` obligation).
        if not self._owns(entry):
            handle.ack()
            return None
        if not decoded.get('hydrated', True):
            try:
                entry = self._hydrate(decoded, entry)
            except Exception as e:  # noqa: BLE001 — classified below
                if _is_transient_store_failure(e):
                    # The bytes may well exist; the store was unreachable or slow.
                    # Retry the delivery, never dead-letter it for that (PAY-002).
                    logger.warning(f'{self._node.name}: payload fetch failed transiently for '
                                   f'{entry.blob_ref}: {e}')
                    handle.nak(delay = self._delivery_policy.retry_delay(
                        handle.num_delivered, jitter = random.uniform(0.5, 1.5)))
                    return None
                self._discard_undecodable(parent_name, delivery, e, handle)
                return None
        return parent_name, entry, handle

    def _hydrate(self, decoded : dict, entry : EnvelopeEntry) -> EnvelopeEntry:
        if self._blob_store is None:
            raise DecodeError(f'payload {entry.blob_ref} is an offloaded reference but no blob store is configured',
                              remedy = 'Configure VF_BLOB_REDIS_URL / --blob-redis-url for this worker.')
        faults.barrier('payload.read.before', ref = entry.blob_ref, node = self._node.name)
        decoded['message'] = hydrate_message(decoded, self._blob_store)
        decoded['hydrated'] = True
        faults.barrier('payload.read.after', ref = entry.blob_ref, node = self._node.name)
        return EnvelopeEntry.from_decoded(decoded)

    def _discard_undecodable(self, parent_name : str, delivery : Delivery, error : BaseException,
                             handle : _AckHandle | None = None) -> None:
        """
        Poison at the transport: bytes that cannot be decoded, or a payload that
        is definitely missing or corrupt. Under RFC 0006 the raw bytes are
        dead-lettered first and the delivery terminated against that record
        (DELIV-15); a failed dead-letter publish keeps the delivery. With the
        switch off the delivery is terminated against this node's terminal log,
        as it always was.
        """
        handle = handle or _AckHandle(delivery.token, self, raw = delivery.envelope_bytes)
        logger.exception(f'{self._node.name}: undecodable message from {parent_name}; terminating it')
        self._count_drop('undecodable')
        poison = error if isinstance(error, DecodeError) else DecodeError(
            f'{type(error).__name__}: {error}'[:512],
            remedy = 'The wire bytes could not be decoded; inspect the dead letter with videoflow dlq show.')
        if constants.RFC0006:
            # The poison ladder, exactly as for a node that raised: dead-lettered
            # (or sampled) before the delivery is terminated; a failed dead-letter
            # publish keeps the delivery.
            self._settle_failure(handle, dict(error_to_dict(poison), disposition = POISON), POISON)
            return
        handle.term(self._terminal_record(handle, {'code': poison.code, 'message': str(error)}, 'undecodable'))

    # -- EOS drain -------------------------------------------------------

    def _terminal_result(self) -> dict:
        """
        The all-parents-stopped shape ``receive_message`` returns when this node
        should end. An aborted parent is reported as such so the task raises the
        real cause and relays it downstream, instead of treating a crashed
        upstream as a clean end of stream.
        """
        return {
            name: {
                'message': None, 'metadata': None, 'is_stop_signal': True,
                'is_abort': name in self._aborted_parents,
                'abort_origin': name if name in self._aborted_parents else None,
                'abort_error': self._aborted_parents.get(name),
            }
            for name in self._parent_names
        }

    def _any_parent_aborted_and_drained(self) -> bool:
        """
        Whether some parent aborted and its data is fully drained.

        This is what keeps a *join* from hanging on a half-dead graph: if one
        parent died, no further input group involving it can ever complete, so
        waiting for the surviving parents' end-of-stream would be waiting for
        nothing. Draining first is deliberate — the work that was already
        published still gets done before the node stops.
        """
        return any(self._is_parent_stopped(p) for p in self._aborted_parents)

    def _all_parents_stopped(self) -> bool:
        if not self._parent_names:
            return False
        return all(self._is_parent_stopped(p) for p in self._parent_names)

    def _is_parent_stopped(self, parent : str) -> bool:
        """
        A parent is stopped once (a) its EOS has been observed and (b) its data is
        fully drained. Drain = no data prefetched locally for it, no pending join
        group holding its half, and its data durable reports no pending and no
        un-acked messages — confirmed on two checks ``eos_quiescence`` apart, which
        tolerates a replicated parent whose sibling replica is momentarily between
        finishing and publishing. For a shared durable (nb_tasks>1) these counts
        span all replicas, so replicas naturally stop together only once the whole
        durable is drained.
        """
        if parent in self._stopped_parents:
            return True
        if parent not in self._eos_seen:
            return False
        if self._backend.prefetched(self._data_subs[parent]) > 0:
            self._quiescent_since.pop(parent, None)
            return False
        if self._has_pending_from(parent):
            self._quiescent_since.pop(parent, None)
            return False
        observed = self._consumer_pending(parent)
        if isinstance(observed, Unknown):
            # A failed query is not an empty durable: the drain cannot complete on
            # evidence that was never obtained. The stall log names the reason.
            self._quiescent_since.pop(parent, None)
            return False
        num_pending, num_ack_pending = observed.value
        if num_pending == 0 and num_ack_pending == 0:
            now = time.monotonic()
            since = self._quiescent_since.get(parent)
            if since is None:
                self._quiescent_since[parent] = now
                return False
            if now - since >= self._eos_quiescence_s:
                self._stopped_parents.add(parent)
                self._ack_eos(parent)
                return True
            return False
        self._quiescent_since.pop(parent, None)
        return False

    def _has_pending_from(self, parent : str) -> bool:
        return self._assembler.has_pending_from(parent)

    def _log_drain_stall(self) -> None:
        """
        Periodic (once per ~15 idle seconds) explanation of why the EOS drain has
        not completed, per unstopped parent — turns a would-be silent termination
        hang into a directly diagnosable log line (e.g. a parent whose EOS was
        never observed, a join group still holding a half, or unacked deliveries).
        """
        parts = []
        for parent in self._parent_names:
            if parent in self._stopped_parents:
                continue
            observed = self._consumer_pending(parent)
            if isinstance(observed, Unknown):
                broker = f'broker_state=unknown({observed.reason}: {observed.detail})'
            else:
                broker = f'broker_pending={observed.value[0]}, unacked={observed.value[1]}'
            parts.append(
                f'{parent}(eos_seen={parent in self._eos_seen}, '
                f'prefetched={self._backend.prefetched(self._data_subs[parent])}, '
                f'in_groups={self._assembler.has_pending_from(parent)}, {broker})'
            )
        if parts:
            logger.info(f'{self._node.name}: EOS drain waiting on ' + '; '.join(parts))

    def _consumer_pending(self, parent : str) -> Observation[tuple[int, int]]:
        """
        ``(num_pending, num_ack_pending)`` of a parent's data durable — or
        ``Unknown`` with the reason when the query failed or timed out. Never
        ``(0, 0)`` for a failure: that reading declared drains complete and
        nodes idle on evidence that did not exist.
        """
        subscription = self._data_subs.get(parent)
        if subscription is None:
            return unknown('unbound', f'no data subscription for parent {parent!r}')
        observed = self._backend.observe_subscription(subscription)
        if isinstance(observed, Unknown):
            return observed
        return known((observed.value.available, observed.value.leased), observed.generation)

    def _ack_eos(self, parent : str) -> None:
        handle = self._eos_handles.pop(parent, None)
        if handle is not None:
            handle.ack()


def _is_transient_store_failure(error : BaseException) -> bool:
    """
    Whether a payload fetch failed because the *store* did (unreachable, slow,
    loading) rather than because the bytes are gone: the core ``TransientFailure``,
    or a client library's connection/timeout errors by name (``redis.exceptions``
    and friends are optional dependencies this module must not import).
    """
    if isinstance(error, TransientFailure):
        return True
    name = type(error).__name__
    return any(marker in name for marker in ('ConnectionError', 'TimeoutError', 'BusyLoading', 'ConnectionRefused'))
