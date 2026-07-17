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
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import asyncio
import hashlib
import logging
import threading
import time
import uuid

import nats

from ..core.constants import REALTIME
from ..core.engine import Messenger
from ..serialization import (
    MSG_TYPE_DATA, MSG_TYPE_EOS, decode_envelope, derive_message_id, encode_envelope,
)
from ..core.policies import JoinPolicy, MISSING_DROP, MISSING_ERROR, MISSING_WAIT
from .topology import (
    consumer_config_for, control_subject_for, dlq_subject_for, durable_name_for,
    eos_consumer_config, eos_subject_for, max_deliver_for, partitioned_durable_name_for,
    stream_config_for, stream_name_for, subject_for,
)

logger = logging.getLogger(__package__)

_FETCH_TIMEOUT_SECONDS = 1.0
# Small prefetch: un-acked messages parked here age against ack_wait, so we keep
# few in flight and let the server-side max_ack_pending bound the rest.
_QUEUE_MAXSIZE = 4
# How many times a BATCH publish retries when the stream is full (backpressure)
# before giving up. Each retry rechecks the termination flag so a stopping flow
# doesn't wedge here forever.
_PUBLISH_RETRY_BACKOFF = [0.05, 0.1, 0.2, 0.5, 1.0]

class _AckHandle:
    '''
    Thread-safe wrapper over a JetStream ``Msg`` so the synchronous task loop (on
    the main thread) can acknowledge, negatively-acknowledge, terminate, or extend
    a message whose I/O lives on the messenger's background event loop. The whole
    point of Phase-2 at-least-once delivery is that these are called only *after*
    the node has processed the message (and published its output), never before.
    '''
    def __init__(self, msg, messenger):
        self._msg = msg
        self._m = messenger
        self._resolved = False

    @property
    def num_delivered(self) -> int:
        try:
            return self._msg.metadata.num_delivered
        except Exception:
            return 1

    @property
    def stream_seq(self):
        try:
            return self._msg.metadata.sequence.stream
        except Exception:
            return None

    def _run(self, coro):
        fut = asyncio.run_coroutine_threadsafe(coro, self._m._loop)
        return fut.result(timeout = 10)

    def ack(self):
        if self._resolved:
            return
        self._resolved = True
        self._m._forget_handle(self)
        try:
            self._run(self._msg.ack())
        except Exception:
            logger.debug('ack failed (message may have been redelivered/evicted)', exc_info = True)

    def nak(self, delay = None):
        if self._resolved:
            return
        self._resolved = True
        self._m._forget_handle(self)
        try:
            self._run(self._msg.nak(delay = delay))
        except Exception:
            logger.debug('nak failed', exc_info = True)

    def term(self):
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
        - blob_store: optional ``videoflow.serialization.BlobStore`` for payloads \
            over the inline size threshold.
        - join_buffer_size (int): max number of not-yet-complete multi-parent \
            join groups to hold in memory before evicting the oldest (protects \
            against unbounded growth if a sibling branch stalls or drops a message).
    '''
    def __init__(self, node, parent_names, nats_url : str, flow_id : str, flow_type : str,
                run_id : str, blob_store = None, join_buffer_size : int = 256,
                replica_id : int = 0, ack_wait : int = 60, max_retries : int = 3,
                eos_quiescence_ms : int = 500, nb_tasks : int = 1, partition_by = None,
                join_policy = None):
        self._node = node
        self._parent_names = list(parent_names)
        self._nats_url = nats_url
        self._flow_id = flow_id
        self._flow_type = flow_type
        self._run_id = run_id
        self._blob_store = blob_store
        self._replica_id = replica_id
        self._ack_wait = ack_wait
        self._max_deliver = max_deliver_for(flow_type, max_retries)
        self._eos_quiescence_s = max(0.0, eos_quiescence_ms / 1000.0)
        self._nb_tasks = nb_tasks
        # Partitioned iff a key is set and there's more than one replica.
        self._partition_by = partition_by if (partition_by and nb_tasks > 1) else None
        self._join_policy = (JoinPolicy.from_dict(join_policy)
                            if join_policy else JoinPolicy.default_for(flow_type))
        self._join_buffer_size = self._join_policy.max_pending
        # first-seen wall-clock per pending group, for join timeout expiry.
        self._group_first_seen = {}
        # Unique per replica: names this replica's EOS consumers so every replica
        # observes end-of-stream (the shared data durable would deliver EOS to only
        # one of them).
        self._instance_id = f'r{replica_id}-{uuid.uuid4().hex[:8]}'

        self._trace_counter = 0
        self._last_trace_id = None
        # seq is carried forward from the input group so a re-run of the same
        # logical output derives the same message_id (dedup). Producers use the
        # local counter; downstream nodes inherit the input group's seq.
        self._last_seq = 0
        # Optional partition key set by the node (via ctx.set_partition_key) and
        # attached to the next published message's metadata.
        self._output_partition_key = None

        self._stopped_parents = set()
        # EOS drain state: a parent is fully stopped only once its EOS has been
        # observed AND its data durable is quiescent (all data drained) — see
        # _is_parent_stopped. _eos_handles holds the EOS ack until drain completes.
        self._eos_seen = set()
        self._eos_handles = {}
        self._quiescent_since = {}
        self._pending_groups = {}
        self._pending_order = []
        # Ack handles for buffered/in-flight messages: _group_handles mirrors
        # _pending_groups (per trace_id, per parent); _inflight_handles are the
        # handles of the group last returned by receive_message, resolved by the
        # task via ack_inputs()/fail_inputs(). _live_handles is every unresolved
        # handle (for the keepalive extender), guarded by _live_lock.
        self._group_handles = {}
        self._inflight_handles = []
        self._live_lock = threading.Lock()
        self._live_handles = set()

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

        self._parent_queues = {}

        fut = asyncio.run_coroutine_threadsafe(self._setup(), self._loop)
        fut.result(timeout = 30)

    # -- lifecycle -----------------------------------------------------

    def _run_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _setup(self):
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

    def _owns(self, entry) -> bool:
        '''For a partitioned node, whether this replica owns the message (hash of the partition key modulo replica count).'''
        if not self._partition_by:
            return True
        if self._partition_by == 'trace_id':
            key = entry.get('trace_id')
        else:
            key = (entry.get('metadata') or {}).get(self._partition_by)
        digest = hashlib.sha256(str(key).encode('utf-8')).hexdigest()
        return (int(digest[:8], 16) % self._nb_tasks) == self._replica_id

    async def _ensure_stream(self, node_name : str):
        # Provisioning (topology.provision_flow) normally creates streams up front;
        # this lazy create is a fallback. It is idempotent — a sibling child or a
        # replica of this node may have created the same stream already.
        config = stream_config_for(self._flow_id, self._run_id, node_name, self._flow_type)
        try:
            await self._js.add_stream(config)
        except Exception as e:
            if 'already in use' not in str(e) and 'name already in use' not in str(e):
                logger.debug(f'add_stream({config.name}) raised (likely already exists): {e}')

    async def _on_control_message(self, msg):
        self._termination_event.set()

    async def _pull_loop(self, parent_name : str, sub):
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
                    entry = decode_envelope(msg.data, blob_store = self._blob_store)
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
                    continue
                # Ack-after-process: the handle is queued *unacked*. It is resolved
                # only once the task calls ack_inputs()/fail_inputs() (data), or
                # immediately in receive_message() for stop markers.
                handle = _AckHandle(msg, self)
                self._register_handle(handle)
                await self._parent_queues[parent_name].put((entry, handle))

    async def _eos_pull_loop(self, parent_name : str, sub):
        # Observes end-of-stream for one parent. The EOS message is *not* acked
        # here — it's held (in _eos_handles) and acked only once the parent's data
        # is fully drained (see _is_parent_stopped), so a crash mid-drain leaves EOS
        # un-acked and re-observable on restart.
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
                if parent_name in self._eos_seen:
                    # Already saw EOS from this parent (another replica's marker):
                    # ack the extra and move on.
                    try:
                        await msg.ack()
                    except Exception:
                        pass
                    continue
                self._eos_seen.add(parent_name)
                self._eos_handles[parent_name] = _AckHandle(msg, self)
                self._register_handle(self._eos_handles[parent_name])

    async def _keepalive_loop(self):
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

    def _register_handle(self, handle):
        with self._live_lock:
            self._live_handles.add(handle)

    def _forget_handle(self, handle):
        with self._live_lock:
            self._live_handles.discard(handle)

    def close(self):
        self._closing.set()
        async def _close():
            for task in self._pull_tasks:
                task.cancel()
            for task in self._pull_tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    logger.debug('pull task raised during shutdown', exc_info = True)
            await self._nc.drain()
        try:
            fut = asyncio.run_coroutine_threadsafe(_close(), self._loop)
            fut.result(timeout = 10)
        except Exception:
            logger.exception('Error draining NATS connection')
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout = 10)

    # -- Messenger interface ---------------------------------------------

    def check_for_termination(self) -> bool:
        return self._termination_event.is_set()

    def set_output_partition_key(self, value):
        self._output_partition_key = value

    def last_input_key(self):
        '''
        A stable identity for the input group last returned by ``receive_message``,
        derived from its trace_id + seq — used as an idempotency key by a sink. The
        same logical event yields the same key across redelivery/restart.
        '''
        if self._last_trace_id is None:
            return None
        return derive_message_id(self._flow_id, self._run_id, self._node.name,
                                self._last_trace_id, self._last_seq, MSG_TYPE_DATA)

    def publish_message(self, message, metadata = None):
        trace_id = self._last_trace_id
        seq = self._last_seq
        if trace_id is None:
            # Only a producer (no parents) mints fresh trace ids; everything
            # downstream carries forward the trace id + seq of the input group it
            # was derived from, so a re-run derives the same message_id (dedup).
            self._trace_counter += 1
            trace_id = f'{self._node.name}:{self._trace_counter}'
            seq = self._trace_counter
        if self._output_partition_key is not None:
            metadata = dict(metadata or {})
            metadata['_partition_key'] = self._output_partition_key
            self._output_partition_key = None
        self._publish(message, metadata, trace_id, seq, MSG_TYPE_DATA)

    def publish_stop_signal(self):
        # EOS goes on this node's dedicated _eos subject (not the data subject), so
        # every downstream replica observes it via its own EOS consumer. The dedup
        # id includes replica_id so EOS markers from different replicas of one node
        # don't collapse into a single one.
        eos_trace = f'eos-r{self._replica_id}'
        self._publish(None, None, eos_trace, self._last_seq, MSG_TYPE_EOS)

    def _publish(self, message, metadata, trace_id, seq, msg_type):
        node_name = self._node.name
        buf = encode_envelope(
            node_name, self._flow_id, self._run_id, trace_id, seq, msg_type,
            metadata, message, replica_id = self._replica_id, blob_store = self._blob_store,
        )
        if msg_type == MSG_TYPE_EOS:
            subject = eos_subject_for(self._flow_id, self._run_id, node_name)
        else:
            subject = subject_for(self._flow_id, self._run_id, node_name)

        # Content-derived dedup id: a re-published retry of the same logical message
        # is dropped by JetStream within the stream's duplicate_window. Safe for EOS
        # now that the re-injection hack is gone.
        headers = {'Nats-Msg-Id': derive_message_id(
            self._flow_id, self._run_id, node_name, trace_id, seq, msg_type)}

        is_realtime = self._flow_type == REALTIME

        async def _do_publish():
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
                    delay = _PUBLISH_RETRY_BACKOFF[min(attempt, len(_PUBLISH_RETRY_BACKOFF) - 1)]
                    attempt += 1
                    await asyncio.sleep(delay)

        fut = asyncio.run_coroutine_threadsafe(_do_publish(), self._loop)
        fut.result(timeout = 120)

    # -- ack / fail (called by the task after process()/consume()) --------

    def ack_inputs(self):
        '''Acknowledge the input group last returned by ``receive_message`` — the node processed it successfully (and, for a processor, already published its output).'''
        for handle in self._inflight_handles:
            handle.ack()
        self._inflight_handles = []

    def fail_inputs(self, exc):
        '''
        The node raised while processing the last input group. REALTIME drops it
        (no redelivery — freshest wins). BATCH nak's it for redelivery until it
        exhausts ``max_deliver``, then dead-letters it and terminates it so it
        stops being redelivered.
        '''
        for handle in self._inflight_handles:
            if self._flow_type == REALTIME:
                handle.term()
            elif handle.num_delivered >= self._max_deliver:
                if self._dlq_publish(handle, exc):
                    handle.term()
                else:
                    # Never silently drop: if the DLQ publish itself failed, keep
                    # the message alive (nak) so a later attempt can dead-letter it.
                    handle.nak(delay = 5)
            else:
                handle.nak(delay = min(2 ** handle.num_delivered, 30))
        self._inflight_handles = []

    def _dlq_publish(self, handle, exc) -> bool:
        subject = dlq_subject_for(self._flow_id, self._run_id, self._node.name)
        seq = handle.stream_seq
        headers = {
            'VF-Origin-Node': self._node.name,
            'VF-Error': repr(exc)[:256],
            'VF-Num-Delivered': str(handle.num_delivered),
            # Idempotent DLQ id (stream seq is unique per original message), so a
            # re-attempt of the same dead-letter doesn't duplicate it.
            'Nats-Msg-Id': f'dlq:{self._flow_id}:{self._run_id}:{self._node.name}:{seq}',
        }
        data = handle._msg.data

        async def _go():
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
            if self._termination_event.is_set() or self._all_parents_stopped():
                self._last_trace_id = None
                return {
                    name: {'message': None, 'metadata': None, 'is_stop_signal': True}
                    for name in self._parent_names
                }

            self._sweep_join_timeouts()

            complete_trace_id = self._find_complete_group()
            if complete_trace_id is not None:
                group = self._pending_groups.pop(complete_trace_id)
                handles = self._group_handles.pop(complete_trace_id)
                self._pending_order.remove(complete_trace_id)
                self._group_first_seen.pop(complete_trace_id, None)
                self._last_trace_id = complete_trace_id
                # Carry a deterministic representative seq forward so this node's
                # output derives a stable message_id across retries (min over the
                # group is stable because the same messages reassemble on retry).
                self._last_seq = min(group[name]['seq'] for name in self._parent_names)
                # Hold this group's handles for the task to ack/fail after process().
                self._inflight_handles = [handles[name] for name in self._parent_names]
                return {
                    name: {
                        'message': group[name]['message'],
                        'metadata': group[name]['metadata'],
                        'is_stop_signal': False,
                    }
                    for name in self._parent_names
                }

            # asyncio.wait(FIRST_COMPLETED) can legitimately return more than one
            # completed get() if two parent queues both had data ready — every one
            # of those items has already been dequeued from its asyncio.Queue, so
            # all of them must be folded into pending groups here. Discarding
            # all-but-one (an earlier version of this method did) silently lost
            # messages whenever two parents produced close together in time.
            for parent_name, entry, handle in self._recv_ready():
                if entry['is_stop_signal']:
                    # Data consumers filter to the data subject, so EOS is handled by
                    # the EOS loop, not here. Ack defensively if one slips through.
                    handle.ack()
                    continue
                trace_id = entry['trace_id']
                group = self._pending_groups.setdefault(trace_id, {})
                handles = self._group_handles.setdefault(trace_id, {})
                if parent_name in handles:
                    # Redelivery of a half we already buffered (the group hadn't
                    # completed yet). Supersede: terminate the stale handle and keep
                    # the fresh delivery so its ack deadline restarts.
                    handles[parent_name].term()
                elif trace_id not in self._pending_order:
                    self._pending_order.append(trace_id)
                    self._group_first_seen[trace_id] = time.monotonic()
                group[parent_name] = entry
                handles[parent_name] = handle

            # Hard cap on buffered groups (last-resort memory guard): drop the oldest.
            while len(self._pending_order) > self._join_buffer_size:
                self._evict_group(self._pending_order[0], missing = MISSING_DROP, reason = 'max_pending exceeded')

    def _sweep_join_timeouts(self):
        '''Expire incomplete join groups older than the join policy's timeout, applying the missing policy (drop/error). WAIT policies have no timeout.'''
        timeout = self._join_policy.timeout_seconds
        if timeout is None or len(self._parent_names) < 2:
            return
        now = time.monotonic()
        for trace_id in list(self._pending_order):
            first = self._group_first_seen.get(trace_id, now)
            if now - first >= timeout:
                self._evict_group(trace_id, missing = self._join_policy.missing,
                                reason = f'join timeout ({timeout}s)')

    def _evict_group(self, trace_id, missing, reason):
        if trace_id not in self._pending_groups:
            return
        group = self._pending_groups.pop(trace_id)
        self._pending_order.remove(trace_id)
        self._group_first_seen.pop(trace_id, None)
        handles = self._group_handles.pop(trace_id, {})
        for handle in handles.values():
            if missing == MISSING_ERROR:
                handle.nak()   # redeliver — the missing half may still arrive
            else:
                handle.ack()   # DROP: give up on this partial group
        logger.warning(
            f'{self._node.name}: evicting incomplete join group {trace_id} '
            f'(had {list(group.keys())}, needed {self._parent_names}) — {reason}, '
            f'missing policy={missing}.'
        )

    def _find_complete_group(self):
        for trace_id in self._pending_order:
            if len(self._pending_groups[trace_id]) == len(self._parent_names):
                return trace_id
        return None

    # -- EOS drain -------------------------------------------------------

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
        return any(parent in group for group in self._pending_groups.values())

    def _consumer_pending(self, parent : str):
        durable = self._data_durable_name(parent)
        stream = stream_name_for(self._flow_id, self._run_id, parent)

        async def _go():
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

    def _ack_eos(self, parent : str):
        handle = self._eos_handles.pop(parent, None)
        if handle is not None:
            handle.ack()

    def _recv_ready(self):
        '''
        Waits up to a short timeout for at least one parent queue to have an item, \
            then returns every parent item that became ready within that wait as \
            ``[(parent_name, entry, handle), ...]`` — possibly empty on timeout, \
            which lets ``receive_message`` loop back and re-check the termination \
            event instead of blocking forever when the flow is being torn down.
        '''
        async def _wait_for_any_parent():
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
