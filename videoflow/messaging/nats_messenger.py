'''
NATS JetStream-backed implementation of ``videoflow.core.engine.Messenger``.

One JetStream stream per node (subject ``vf.{flow_id}.{node.name}``); a node's
messenger publishes only its own output there. Each real parent gets its own
durable pull consumer, named after the *consuming* node so that replicas of the
same consuming node (``nb_tasks > 1``) share one durable name (competing
consumers / load balancing), while distinct children of the same parent get
distinct durable names (each gets its own full copy — broadcast fan-out).

Runs its own asyncio event loop on a background thread so the rest of the
framework (``videoflow.core.task``) can stay synchronous, matching the blocking
``Queue.get()``-style calls the local engine used to make.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import hashlib
import logging
import random
import threading
import time
import uuid
from typing import Any, Coroutine, Optional, TypeVar

import nats
from nats.aio.msg import Msg
from nats.js import JetStreamContext

from ..core.constants import REALTIME
from ..core.engine import Messenger
from ..core.errors import DEFAULT_DISPOSITION, classify, error_to_dict
from ..core.node import Node
from ..core.policies import (
    ACTION_DLQ_SAMPLED,
    ACTION_NAK,
    ACTION_TERM,
    JOIN_TIME,
    DeliveryPolicy,
    JoinPolicy,
)
from ..wire.serialization import (
    DEFAULT_ENVELOPE_VERSION,
    MSG_TYPE_ABORT,
    MSG_TYPE_DATA,
    MSG_TYPE_EOS,
    BlobStore,
    decode_envelope,
    derive_message_id,
    encode_envelope,
)
from .grouping import EnvelopeEntry, make_assembler
from .topology import (
    consumer_config_for,
    control_subject_for,
    dlq_subject_for,
    durable_name_for,
    eos_consumer_config,
    eos_subject_for,
    partitioned_durable_name_for,
    stream_config_for,
    stream_name_for,
    subject_for,
)

logger = logging.getLogger(__package__)

_FETCH_TIMEOUT_SECONDS = 1.0
# Flow-type blob TTL defaults (BLOB-7). REALTIME delivery is near-immediate (the
# stream holds one message and never redelivers), so the TTL bounds only leaked
# blobs. A BATCH Interest-retention backlog can legitimately delay a blob's *first*
# read past an hour, so a short TTL there is silent data loss; refcounted
# reclamation (BLOB-5/6) is what makes the long TTL affordable.
DEFAULT_BLOB_TTL_REALTIME_SECONDS = 3600
DEFAULT_BLOB_TTL_BATCH_SECONDS = 86400
# Small prefetch: un-acked messages parked here age against ack_wait, so we keep
# few in flight and let the server-side max_ack_pending bound the rest.
_QUEUE_MAXSIZE = 4
# How many times a BATCH publish retries when the stream is full (backpressure)
# before giving up. Each retry rechecks the termination flag so a stopping flow
# doesn't wedge here forever.
_PUBLISH_RETRY_BACKOFF = [0.05, 0.1, 0.2, 0.5, 1.0]
# Hard bound on getting an ABORT marker out. A dying worker should spend seconds,
# not minutes, trying to tell its children — the supervisor's control-abort and
# the receiver's progress deadline cover the case where it never manages.
_ABORT_PUBLISH_TIMEOUT = 10

#: Result type of a coroutine handed to ``_AckHandle._run`` — ties the value the
#: caller gets back to the coroutine it passed in.
_T = TypeVar('_T')

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
    Thread-safe wrapper over a JetStream ``Msg`` so the synchronous task loop (on
    the main thread) can acknowledge, negatively-acknowledge, terminate, or extend
    a message whose I/O lives on the messenger's background event loop. The whole
    point of Phase-2 at-least-once delivery is that these are called only *after*
    the node has processed the message (and published its output), never before.
    '''
    def __init__(self, msg : Msg, messenger : 'NATSMessenger', blob_ref : str | None = None) -> None:
        self._msg = msg
        self._m = messenger
        # Blob store reference the message's payload was resolved from, released
        # after a successful ack (BLOB-6); None for inline payloads and EOS.
        self._blob_ref = blob_ref
        self._resolved = False

    @property
    def num_delivered(self) -> int:
        try:
            return self._msg.metadata.num_delivered
        except Exception:
            return 1

    @property
    def stream_seq(self) -> Optional[int]:
        try:
            return self._msg.metadata.sequence.stream
        except Exception:
            return None

    def _run(self, coro : Coroutine[Any, Any, _T]) -> _T:
        fut = asyncio.run_coroutine_threadsafe(coro, self._m._loop)
        return fut.result(timeout = 10)

    def ack(self) -> None:
        if self._resolved:
            return
        self._resolved = True
        self._m._forget_handle(self)
        try:
            self._run(self._msg.ack())
        except Exception:
            logger.debug('ack failed (message may have been redelivered/evicted)', exc_info = True)
        else:
            # Release only on ack *success*: a failed ack can mean the broker
            # redelivers, and a redelivery re-reads the blob (BLOB-6). nak/term
            # never release for the same reason.
            self._m._release_blob(self._blob_ref)

    def nak(self, delay : float | None = None) -> None:
        if self._resolved:
            return
        self._resolved = True
        self._m._forget_handle(self)
        try:
            self._run(self._msg.nak(delay = delay))
        except Exception:
            logger.debug('nak failed', exc_info = True)

    def term(self) -> None:
        if self._resolved:
            return
        self._resolved = True
        self._m._forget_handle(self)
        try:
            self._run(self._msg.term())
        except Exception:
            logger.debug('term failed', exc_info = True)

class NATSMessenger(Messenger):
    '''
    - Arguments:
        - node: the ``videoflow.core.node.Node`` this messenger is bound to.
        - parent_names ([str]): the real parents of ``node``, by ``.name``.
        - nats_url (str): e.g. ``nats://localhost:4222``.
        - flow_id (str): shared across every node in the flow.
        - flow_type (str): ``videoflow.core.constants.REALTIME`` or ``BATCH`` — \
            controls the stream retention/discard policy used for ``node``'s own \
            output stream.
        - blob_store: optional ``videoflow.wire.serialization.BlobStore`` for payloads \
            over the inline size threshold.
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
    '''
    def __init__(self, node : Node, parent_names : list[str], nats_url : str, flow_id : str,
                flow_type : str, run_id : str, blob_store : BlobStore | None = None,
                replica_id : int = 0, ack_wait : int = 60, max_retries : int = 3,
                eos_quiescence_ms : int = 500, nb_tasks : int = 1,
                partition_by : str | None = None, join_policy : dict | None = None,
                envelope_version : int | None = None, blob_readers : int | None = None,
                blob_ttl_seconds : int | None = None,
                delivery_policy : dict | None = None) -> None:
        self._node = node
        # Wire version this node emits (the protobuf v4 envelope; §4 of PROTOCOL.md).
        self._envelope_version = DEFAULT_ENVELOPE_VERSION if envelope_version is None else envelope_version
        self._parent_names = list(parent_names)
        self._nats_url = nats_url
        self._flow_id = flow_id
        self._flow_type = flow_type
        self._run_id = run_id
        self._blob_store = blob_store
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
        # Ack handles: _inflight_handles are the handles of the group last returned
        # by receive_message, resolved by the task via ack_inputs()/fail_inputs()
        # (handles of still-pending groups live inside the assembler).
        # _live_handles is every unresolved handle (for the keepalive extender),
        # guarded by _live_lock.
        self._inflight_handles: list[_AckHandle] = []
        self._live_lock = threading.Lock()
        self._live_handles: set[_AckHandle] = set()

        # _termination_event: control-channel "stop the whole flow" signal, read by
        #   producers (to stop early) and by receive_message (to stop waiting).
        # _closing: set only by close(); the pull loops keep draining the broker
        #   into local queues until then, independent of termination, so a
        #   control-stop doesn't strand messages already in flight.
        self._termination_event = threading.Event()
        self._closing = threading.Event()

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target = self._run_loop, daemon = True)
        self._thread.start()

        self._parent_queues: dict[str, asyncio.Queue] = {}

        fut = asyncio.run_coroutine_threadsafe(self._setup(), self._loop)
        fut.result(timeout = 30)

    # -- lifecycle -----------------------------------------------------

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _setup(self) -> None:
        self._nc = await nats.connect(self._nats_url)
        self._js = self._nc.jetstream()
        self._pull_tasks = []

        await self._ensure_stream(self._node.name)
        await self._nc.subscribe(control_subject_for(self._flow_id, self._run_id), cb = self._on_control_message)

        for parent_name in self._parent_names:
            await self._ensure_stream(parent_name)
            self._parent_queues[parent_name] = asyncio.Queue(maxsize = _QUEUE_MAXSIZE)

            # Data consumer: shared durable (competing consumers), or a per-replica
            # durable for a partitioned node (broadcast + client-side ownership).
            data_durable = self._data_durable_name(parent_name)
            base_cfg = consumer_config_for(
                self._flow_id, self._run_id, self._node.name, parent_name,
                ack_wait = self._ack_wait, max_deliver = self._max_deliver,
                max_ack_pending = _QUEUE_MAXSIZE + 2)
            base_cfg.durable_name = data_durable
            data_sub = await self._js.pull_subscribe(
                subject_for(self._flow_id, self._run_id, parent_name),
                durable = data_durable, config = base_cfg,
            )
            self._pull_tasks.append(
                asyncio.ensure_future(self._pull_loop(parent_name, data_sub), loop = self._loop)
            )

            # EOS consumer: per-replica durable so every replica observes EOS.
            eos_cfg = eos_consumer_config(self._flow_id, self._run_id, self._node.name,
                                        parent_name, self._instance_id)
            eos_sub = await self._js.pull_subscribe(
                eos_subject_for(self._flow_id, self._run_id, parent_name),
                durable = eos_cfg.durable_name, config = eos_cfg,
            )
            self._pull_tasks.append(
                asyncio.ensure_future(self._eos_pull_loop(parent_name, eos_sub), loop = self._loop)
            )

        # Periodically extend the ack deadline of unresolved messages so a slow
        # process() call doesn't trigger spurious redelivery of parked/in-flight
        # messages while they wait their turn.
        self._pull_tasks.append(asyncio.ensure_future(self._keepalive_loop(), loop = self._loop))

    def _data_durable_name(self, parent_name : str) -> str:
        if self._partition_by:
            return partitioned_durable_name_for(self._node.name, parent_name, self._replica_id)
        return durable_name_for(self._node.name, parent_name)

    def _owns(self, entry : EnvelopeEntry) -> bool:
        '''For a partitioned node, whether this replica owns the message (hash of the partition key modulo replica count).'''
        if not self._partition_by:
            return True
        key : Any
        if self._partition_by == 'trace_id':
            key = entry.trace_id
        else:
            key = (entry.metadata or {}).get(self._partition_by)
        digest = hashlib.sha256(str(key).encode('utf-8')).hexdigest()
        return (int(digest[:8], 16) % self._nb_tasks) == self._replica_id

    async def _ensure_stream(self, node_name : str) -> None:
        # Provisioning (topology.provision_flow) normally creates streams up front;
        # this lazy create is a fallback. It is idempotent — a sibling child or a
        # replica of this node may have created the same stream already.
        config = stream_config_for(self._flow_id, self._run_id, node_name, self._flow_type)
        try:
            await self._js.add_stream(config)
        except Exception as e:
            if 'already in use' not in str(e) and 'name already in use' not in str(e):
                logger.debug(f'add_stream({config.name}) raised (likely already exists): {e}')

    async def _on_control_message(self, msg : Msg) -> None:
        self._termination_event.set()

    async def _pull_loop(self, parent_name : str, sub : JetStreamContext.PullSubscription) -> None:
        while not self._closing.is_set():
            try:
                msgs = await sub.fetch(batch = 1, timeout = _FETCH_TIMEOUT_SECONDS)
            except (nats.errors.TimeoutError, TimeoutError):
                continue
            except Exception as e:
                if self._closing.is_set():
                    return
                logger.warning(f'pull fetch on {parent_name} failed: {e}')
                await asyncio.sleep(0.5)
                continue
            for msg in msgs:
                try:
                    # The one place a decoded envelope crosses into messaging:
                    # adapt the wire dict to the typed record here so nothing
                    # downstream (join, EOS drain, ownership) reads it by key.
                    entry = EnvelopeEntry.from_decoded(
                        decode_envelope(msg.data, blob_store = self._blob_store))
                except Exception:
                    # Undecodable message: terminate it so it is not redelivered
                    # forever (a genuinely poisoned wire payload).
                    logger.exception(f'Failed to decode message from {parent_name}; terminating it')
                    try:
                        await msg.term()
                    except Exception:
                        pass
                    continue
                # Partitioned node: this replica keeps only the messages it owns and
                # acks-and-skips the rest (every replica sees every message on its own
                # durable). Ownership is stable across replicas via hashing.
                if not self._owns(entry):
                    try:
                        await msg.ack()
                    except Exception:
                        pass
                    else:
                        # This replica read the blob during decode even though it
                        # skips processing, so its ack counts as one release
                        # (BLOB-6). Called directly, not via a handle: _AckHandle
                        # bridges *from another thread* into this loop and would
                        # deadlock called from the loop itself.
                        self._release_blob(entry.blob_ref)
                    continue
                # Ack-after-process: the handle is queued *unacked*. It is resolved
                # only once the task calls ack_inputs()/fail_inputs() (data), or
                # immediately in receive_message() for stop markers.
                handle = _AckHandle(msg, self, blob_ref = entry.blob_ref)
                self._register_handle(handle)
                await self._parent_queues[parent_name].put((entry, handle))

    async def _eos_pull_loop(self, parent_name : str, sub : JetStreamContext.PullSubscription) -> None:
        # Observes end-of-stream for one parent — clean (EOS) or abnormal (ABORT);
        # both ride this subject. The marker is *not* acked here: it's held (in
        # _eos_handles) and acked only once the parent's data is fully drained
        # (see _is_parent_stopped), so a crash mid-drain leaves it un-acked and
        # re-observable on restart.
        while not self._closing.is_set():
            try:
                msgs = await sub.fetch(batch = 1, timeout = _FETCH_TIMEOUT_SECONDS)
            except (nats.errors.TimeoutError, TimeoutError):
                continue
            except Exception as e:
                if self._closing.is_set():
                    return
                logger.debug(f'eos fetch on {parent_name} failed: {e}')
                await asyncio.sleep(0.5)
                continue
            for msg in msgs:
                aborted = False
                try:
                    decoded = decode_envelope(msg.data)
                    aborted = bool(decoded.get('is_abort'))
                except Exception:
                    # An undecodable terminator still means "this parent ended".
                    # Refusing to stop because the *reason* was unreadable would
                    # trade a diagnosable failure for a hang.
                    logger.debug(f'could not decode terminator from {parent_name}',
                                exc_info = True)
                    decoded = {}
                if aborted:
                    # An abort outranks a clean EOS from the same parent: one
                    # replica finishing normally does not undo another one dying.
                    self._aborted_parents[parent_name] = decoded.get('error') or {}
                if parent_name in self._eos_seen:
                    # Already saw a terminator from this parent (another replica's
                    # marker): ack the extra and move on.
                    try:
                        await msg.ack()
                    except Exception:
                        pass
                    continue
                self._eos_seen.add(parent_name)
                self._eos_handles[parent_name] = _AckHandle(msg, self)
                self._register_handle(self._eos_handles[parent_name])

    async def _keepalive_loop(self) -> None:
        interval = max(1.0, self._ack_wait / 3.0)
        while not self._closing.is_set():
            await asyncio.sleep(interval)
            with self._live_lock:
                handles = list(self._live_handles)
            for h in handles:
                if not h._resolved:
                    try:
                        await h._msg.in_progress()
                    except Exception:
                        pass

    def _release_blob(self, blob_ref : str | None) -> None:
        '''
        Decrement-and-maybe-delete a blob after its message was successfully acked \
            (BLOB-6). Failure is logged and swallowed: the TTL backstop (BLOB-7) \
            reclaims anything a failed release leaks.
        '''
        if blob_ref is None or self._blob_store is None:
            return
        try:
            self._blob_store.release(blob_ref)
        except Exception:
            logger.debug(f'blob release failed for {blob_ref}', exc_info = True)

    def _register_handle(self, handle : _AckHandle) -> None:
        with self._live_lock:
            self._live_handles.add(handle)

    def _forget_handle(self, handle : _AckHandle) -> None:
        with self._live_lock:
            self._live_handles.discard(handle)

    def close(self) -> None:
        self._closing.set()
        async def _close() -> None:
            for task in self._pull_tasks:
                task.cancel()
            for task in self._pull_tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.debug('pull task raised during shutdown', exc_info = True)
            # We've stopped consuming (pull tasks cancelled) and all data is already
            # acked by the time a node closes (EOS-drain, see _is_parent_stopped), so a
            # graceful ``drain()`` — which also drains the still-registered JetStream
            # pull *subscriptions* — would just block until timeout on cancelled pull
            # consumers. Flush pending publishes, then close: ``close()`` cancels the
            # client's internal read/ping/flush tasks cleanly (no "task destroyed" noise).
            if self._nc is not None:
                if self._nc.is_connected:
                    try:
                        await asyncio.wait_for(self._nc.flush(), timeout = 5)
                    except Exception:
                        logger.debug('NATS flush incomplete during shutdown', exc_info = True)
                try:
                    await self._nc.close()
                except Exception:
                    logger.debug('NATS close raised during shutdown', exc_info = True)
        try:
            fut = asyncio.run_coroutine_threadsafe(_close(), self._loop)
            fut.result(timeout = 10)
        except Exception:
            logger.debug('NATS connection teardown incomplete at shutdown', exc_info = True)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout = 10)

    # -- Messenger interface ---------------------------------------------

    def check_for_termination(self) -> bool:
        return self._termination_event.is_set()

    def set_output_partition_key(self, value : Any) -> None:
        self._output_partition_key = value

    def set_output_event_timestamp(self, value : float) -> None:
        self._output_event_ts = value

    def last_input_info(self) -> Optional[dict[str, Optional[dict]]]:
        '''
        Per-parent envelope info (``event_ts``, ``metadata``, ``trace_id``, ``seq``)
        for the input group last returned by ``receive_message``; ``None`` entries
        for parents missing from a quorum emission. ``None`` for producers.
        '''
        return self._last_input_info

    def last_input_key(self) -> Optional[str]:
        '''
        A stable identity for the input group last returned by ``receive_message``,
        derived from its trace_id + seq — used as an idempotency key by a sink. The
        same logical event yields the same key across redelivery/restart.
        '''
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
        '''
        Publishes an abnormal end-of-stream carrying why this node died.

        Rides the same ``_eos`` subject as a clean EOS, which is the point: it
        reuses the per-replica EOS consumers and the provisioning interest anchor
        unchanged, so a marker published by a dying node is still retained and
        still reaches every downstream replica. Its dedup id is distinct from the
        clean marker's so a node that aborts is never mistaken for one that
        finished.
        '''
        abort_trace = f'abort-r{self._replica_id}'
        self._publish(None, None, abort_trace, self._last_seq, MSG_TYPE_ABORT,
                    error = error_to_dict(error))

    def pending_count(self) -> int:
        '''
        Messages waiting for this node across all its parents — locally queued,
        held in incomplete join groups, or still on the broker. Feeds
        ``ProgressDeadline``, which needs "is there work to do" to tell a stalled
        node from an idle one.
        '''
        total = 0
        for parent in self._parent_names:
            queue = self._parent_queues.get(parent)
            if queue is not None:
                total += queue.qsize()
            num_pending, num_ack_pending = self._consumer_pending(parent)
            total += num_pending + num_ack_pending
        return total

    def _publish(self, message : Any, metadata : Optional[dict], trace_id : str, seq : int,
                msg_type : str, event_ts : float | None = None,
                error : Optional[dict] = None) -> None:
        node_name = self._node.name
        buf = encode_envelope(
            node_name, self._flow_id, self._run_id, trace_id, seq, msg_type,
            metadata, message, replica_id = self._replica_id, event_ts = event_ts,
            blob_store = self._blob_store, version = self._envelope_version,
            blob_readers = self._blob_readers, blob_ttl_seconds = self._blob_ttl_seconds,
            error = error,
        )
        if msg_type in (MSG_TYPE_EOS, MSG_TYPE_ABORT):
            subject = eos_subject_for(self._flow_id, self._run_id, node_name)
        else:
            subject = subject_for(self._flow_id, self._run_id, node_name)

        # Content-derived dedup id: a re-published retry of the same logical message
        # is dropped by JetStream within the stream's duplicate_window. Safe for EOS
        # now that the re-injection hack is gone. The VF-Env header lets tooling (and
        # the DLQ inspector) identify the wire version without decoding.
        headers = {
            'Nats-Msg-Id': derive_message_id(
                self._flow_id, self._run_id, node_name, trace_id, seq, msg_type),
            'VF-Env': str(self._envelope_version),
        }

        is_realtime = self._flow_type == REALTIME
        # An ABORT is published by a worker that is already dying, so it must not
        # inherit BATCH's block-until-there-is-room backpressure: the whole point
        # of the marker is to reach children *quickly*. If it cannot get out, the
        # supervisor's control-abort and the receiver-side progress deadline are
        # the layers behind it.
        is_abort = msg_type == MSG_TYPE_ABORT
        max_attempts = 3 if is_abort else None

        async def _do_publish() -> None:
            # REALTIME (Discard=OLD): a full stream evicts the oldest message, so a
            # publish never blocks — freshest wins. BATCH (Interest + Discard=NEW):
            # a full stream *rejects* the publish; retry with backoff so a slow
            # consumer applies real backpressure instead of losing data.
            attempt = 0
            while True:
                try:
                    await self._js.publish(subject, buf, headers = headers)
                    return
                except Exception as e:  # noqa: BLE001
                    if is_realtime or self._termination_event.is_set():
                        # REALTIME never blocks; a stopping flow abandons the publish.
                        if is_realtime:
                            return
                        raise
                    if 'maximum messages' not in str(e).lower() and 'wrong last sequence' not in str(e).lower():
                        raise
                    if max_attempts is not None and attempt >= max_attempts:
                        raise
                    delay = _PUBLISH_RETRY_BACKOFF[min(attempt, len(_PUBLISH_RETRY_BACKOFF) - 1)]
                    attempt += 1
                    await asyncio.sleep(delay)

        fut = asyncio.run_coroutine_threadsafe(_do_publish(), self._loop)
        fut.result(timeout = _ABORT_PUBLISH_TIMEOUT if is_abort else 120)

    # -- ack / fail (called by the task after process()/consume()) --------

    def ack_inputs(self) -> None:
        '''Acknowledge the input group last returned by ``receive_message`` — the node processed it successfully (and, for a processor, already published its output).'''
        for handle in self._inflight_handles:
            handle.ack()
        self._inflight_handles = []

    def fail_inputs(self, exc : BaseException) -> None:
        '''
        The node raised while processing the last input group. The action is
        decided by ``DeliveryPolicy.action_for`` from *how the error classified*
        and how many times the broker has delivered it — not by the flow type
        alone. This messenger only executes the verdict.

        The difference that matters: a poison message is dead-lettered on its
        first failure rather than burning four attempts on its way to the same
        place, and a worker-fatal error naks without dead-lettering, because the
        message is fine and this worker is not.
        '''
        disposition = classify(exc, self._delivery_policy.on_error or DEFAULT_DISPOSITION)
        error = dict(error_to_dict(exc))
        # Stamp the disposition that was actually *used*, not the one the exception
        # happened to carry: a bare ValueError has none, and that is precisely the
        # case where the classifier did the work and the record must say so.
        error['disposition'] = disposition
        for handle in self._inflight_handles:
            action = self._delivery_policy.action_for(disposition, handle.num_delivered)
            if action == ACTION_TERM:
                handle.term()
            elif action == ACTION_NAK:
                delay = self._delivery_policy.retry_delay(
                    handle.num_delivered, jitter = random.uniform(0.5, 1.5))
                handle.nak(delay = delay)
            elif action == ACTION_DLQ_SAMPLED:
                # Bounded specimens per distinct failure: enough to diagnose,
                # never enough to fill a stream.
                code = str(error.get('code', 'VF_UNKNOWN'))
                if self._dlq_sampler.admit(code, self._node.name):
                    self._dlq_publish(handle, error)
                handle.term()
            else:                                   # ACTION_DLQ
                if self._dlq_publish(handle, error):
                    handle.term()
                else:
                    # Never silently drop: if the DLQ publish itself failed, keep
                    # the message alive (nak) so a later attempt can dead-letter it.
                    handle.nak(delay = 5)
        self._inflight_handles = []

    def _dlq_publish(self, handle : _AckHandle, error : dict) -> bool:
        subject = dlq_subject_for(self._flow_id, self._run_id, self._node.name)
        seq = handle.stream_seq
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
            'Nats-Msg-Id': f'dlq:{self._flow_id}:{self._run_id}:{self._node.name}:{seq}',
        }
        data = handle._msg.data

        async def _go() -> bool:
            for attempt in range(3):
                try:
                    await self._js.publish(subject, data, headers = headers)
                    return True
                except Exception:
                    await asyncio.sleep(0.1 * (attempt + 1))
            return False

        try:
            fut = asyncio.run_coroutine_threadsafe(_go(), self._loop)
            return fut.result(timeout = 15)
        except Exception:
            logger.exception('DLQ publish raised')
            return False

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

            # asyncio.wait(FIRST_COMPLETED) can legitimately return more than one
            # completed get() if two parent queues both had data ready — every one
            # of those items has already been dequeued from its asyncio.Queue, so
            # all of them must be folded into the assembler here. Discarding
            # all-but-one (an earlier version of this method did) silently lost
            # messages whenever two parents produced close together in time.
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
                    # the EOS loop, not here. Ack defensively if one slips through.
                    handle.ack()
                    continue
                self._assembler.add(parent_name, entry, handle)

    # -- EOS drain -------------------------------------------------------

    def _terminal_result(self) -> dict:
        '''
        The all-parents-stopped shape ``receive_message`` returns when this node
        should end. An aborted parent is reported as such so the task raises the
        real cause and relays it downstream, instead of treating a crashed
        upstream as a clean end of stream.
        '''
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
        '''
        Whether some parent aborted and its data is fully drained.

        This is what keeps a *join* from hanging on a half-dead graph: if one
        parent died, no further input group involving it can ever complete, so
        waiting for the surviving parents' end-of-stream would be waiting for
        nothing. Draining first is deliberate — the work that was already
        published still gets done before the node stops.
        '''
        return any(self._is_parent_stopped(p) for p in self._aborted_parents)

    def _all_parents_stopped(self) -> bool:
        if not self._parent_names:
            return False
        return all(self._is_parent_stopped(p) for p in self._parent_names)

    def _is_parent_stopped(self, parent : str) -> bool:
        '''
        A parent is stopped once (a) its EOS has been observed and (b) its data is
        fully drained. Drain = no data buffered locally for it, no pending join
        group holding its half, and its data durable reports no pending and no
        un-acked messages — confirmed on two checks ``eos_quiescence`` apart, which
        tolerates a replicated parent whose sibling replica is momentarily between
        finishing and publishing. For a shared durable (nb_tasks>1) these counts
        span all replicas, so replicas naturally stop together only once the whole
        durable is drained.
        '''
        if parent in self._stopped_parents:
            return True
        if parent not in self._eos_seen:
            return False
        q = self._parent_queues.get(parent)
        if q is not None and q.qsize() > 0:
            self._quiescent_since.pop(parent, None)
            return False
        if self._has_pending_from(parent):
            self._quiescent_since.pop(parent, None)
            return False
        num_pending, num_ack_pending = self._consumer_pending(parent)
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
        '''
        Periodic (once per ~15 idle seconds) explanation of why the EOS drain has
        not completed, per unstopped parent — turns a would-be silent termination
        hang into a directly diagnosable log line (e.g. a parent whose EOS was
        never observed, a join group still holding a half, or unacked deliveries).
        '''
        parts = []
        for parent in self._parent_names:
            if parent in self._stopped_parents:
                continue
            q = self._parent_queues.get(parent)
            num_pending, num_ack_pending = self._consumer_pending(parent)
            parts.append(
                f'{parent}(eos_seen={parent in self._eos_seen}, '
                f'queued={q.qsize() if q is not None else 0}, '
                f'in_groups={self._assembler.has_pending_from(parent)}, '
                f'broker_pending={num_pending}, unacked={num_ack_pending})'
            )
        if parts:
            logger.info(f'{self._node.name}: EOS drain waiting on ' + '; '.join(parts))

    def _consumer_pending(self, parent : str) -> tuple[int, int]:
        durable = self._data_durable_name(parent)
        stream = stream_name_for(self._flow_id, self._run_id, parent)

        async def _go() -> tuple[int, int]:
            try:
                info = await self._js.consumer_info(stream, durable)
                return info.num_pending, info.num_ack_pending
            except Exception:
                return 0, 0

        try:
            fut = asyncio.run_coroutine_threadsafe(_go(), self._loop)
            return fut.result(timeout = 5)
        except Exception:
            return 0, 0

    def _ack_eos(self, parent : str) -> None:
        handle = self._eos_handles.pop(parent, None)
        if handle is not None:
            handle.ack()

    def _recv_ready(self) -> list[tuple[str, EnvelopeEntry, _AckHandle]]:
        '''
        Waits up to a short timeout for at least one parent queue to have an item, \
            then returns every parent item that became ready within that wait as \
            ``[(parent_name, entry, handle), ...]`` — possibly empty on timeout, \
            which lets ``receive_message`` loop back and re-check the termination \
            event instead of blocking forever when the flow is being torn down.
        '''
        async def _wait_for_any_parent() -> list[tuple[str, EnvelopeEntry, _AckHandle]]:
            get_tasks = {
                asyncio.ensure_future(self._parent_queues[name].get()): name
                for name in self._parent_names
            }
            done, pending = await asyncio.wait(
                get_tasks.keys(),
                timeout = _FETCH_TIMEOUT_SECONDS,
                return_when = asyncio.FIRST_COMPLETED,
            )
            for p in pending:
                p.cancel()
            # Cancelling a pending asyncio.Queue.get() is safe: if an item had
            # become available it would be in `done`, not `pending`; a genuinely
            # pending getter has no item and leaves the queue untouched.
            results = []
            for task in done:
                entry, handle = task.result()
                results.append((get_tasks[task], entry, handle))
            return results

        fut = asyncio.run_coroutine_threadsafe(_wait_for_any_parent(), self._loop)
        return fut.result()
