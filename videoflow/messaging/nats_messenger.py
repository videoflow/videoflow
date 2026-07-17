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
import logging
import queue
import re
import threading
import time
import uuid

import nats
from nats.js.api import ConsumerConfig, DiscardPolicy, RetentionPolicy, StreamConfig

from ..core.constants import REALTIME
from ..core.engine import Messenger
from ..serialization import decode_envelope, encode_envelope

logger = logging.getLogger(__package__)

_SANITIZE_RE = re.compile(r'[^A-Za-z0-9_-]+')

def _sanitize(value : str) -> str:
    return _SANITIZE_RE.sub('_', value)

def subject_for(flow_id : str, node_name : str) -> str:
    return f'vf.{flow_id}.{_sanitize(node_name)}'

def stream_name_for(flow_id : str, node_name : str) -> str:
    return f'vf-{_sanitize(flow_id)}-{_sanitize(node_name)}'

def control_subject_for(flow_id : str) -> str:
    return f'vf.{flow_id}._control.stop'

def durable_name_for(consumer_node_name : str, parent_node_name : str) -> str:
    return f'{_sanitize(consumer_node_name)}--from--{_sanitize(parent_node_name)}'

_FETCH_TIMEOUT_SECONDS = 1.0
_QUEUE_MAXSIZE = 64

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
                blob_store = None, join_buffer_size : int = 256):
        self._node = node
        self._parent_names = list(parent_names)
        self._nats_url = nats_url
        self._flow_id = flow_id
        self._flow_type = flow_type
        self._blob_store = blob_store
        self._join_buffer_size = join_buffer_size

        self._seq = 0
        self._trace_counter = 0
        self._last_trace_id = None

        self._stopped_parents = set()
        self._pending_groups = {}
        self._pending_order = []

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
        await self._nc.subscribe(control_subject_for(self._flow_id), cb = self._on_control_message)

        for parent_name in self._parent_names:
            await self._ensure_stream(parent_name)
            self._parent_queues[parent_name] = asyncio.Queue(maxsize = _QUEUE_MAXSIZE)
            durable = durable_name_for(self._node.name, parent_name)
            sub = await self._js.pull_subscribe(
                subject_for(self._flow_id, parent_name),
                durable = durable,
                config = ConsumerConfig(durable_name = durable, ack_wait = 60),
            )
            self._pull_tasks.append(
                asyncio.ensure_future(self._pull_loop(parent_name, sub), loop = self._loop)
            )

    async def _ensure_stream(self, node_name : str):
        stream_name = stream_name_for(self._flow_id, node_name)
        subject = subject_for(self._flow_id, node_name)
        if self._flow_type == REALTIME:
            # "Freshest wins": keep only the most recent message. Discard=OLD means
            # a new publish evicts the stale un-consumed message rather than being
            # rejected, so the producer never blocks and a slow consumer always
            # sees the latest frame. (Discard=NEW would be wrong here: under LIMITS
            # retention an ack does not free space, so the stream would pin the
            # first message forever and reject everything after it, stop signal
            # included.)
            config = StreamConfig(
                name = stream_name, subjects = [subject],
                retention = RetentionPolicy.LIMITS, max_msgs = 1,
                discard = DiscardPolicy.OLD,
            )
        else:
            config = StreamConfig(
                name = stream_name, subjects = [subject],
                retention = RetentionPolicy.LIMITS, max_msgs = 1_000_000,
                discard = DiscardPolicy.OLD,
            )
        try:
            await self._js.add_stream(config)
        except Exception as e:
            # Idempotent create: another node (a sibling child, or the node
            # itself being started twice for nb_tasks>1) may have already
            # created this exact stream.
            if 'already in use' not in str(e) and 'name already in use' not in str(e):
                logger.debug(f'add_stream({stream_name}) raised (likely already exists): {e}')

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
                    await msg.ack()
                except Exception:
                    logger.exception(f'Failed to decode/ack message from {parent_name}')
                    continue
                await self._parent_queues[parent_name].put(entry)

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

    def publish_message(self, message, metadata = None):
        self._seq += 1
        trace_id = self._last_trace_id
        if trace_id is None:
            # Only a producer (no parents) mints fresh trace ids; everything
            # downstream carries forward the trace id of the input group it
            # was derived from.
            self._trace_counter += 1
            trace_id = f'{self._node.name}:{self._trace_counter}'
        self._publish(message, metadata, trace_id, is_stop_signal = False)

    def publish_stop_signal(self):
        self._publish(None, None, '', is_stop_signal = True)

    def _publish(self, message, metadata, trace_id, is_stop_signal, as_node_name = None):
        node_name = as_node_name or self._node.name
        buf = encode_envelope(
            node_name, self._flow_id, trace_id, self._seq, is_stop_signal,
            metadata, message, blob_store = self._blob_store,
        )
        subject = subject_for(self._flow_id, node_name)

        # Both flow types use Discard=OLD, so a publish to a full stream evicts the
        # oldest message instead of failing — the producer never blocks. REALTIME
        # (MaxMsgs=1) therefore drops all but the freshest; BATCH (large MaxMsgs)
        # keeps a deep buffer and only drops if a consumer falls millions of
        # messages behind.
        async def _do_publish():
            await self._js.publish(subject, buf)

        fut = asyncio.run_coroutine_threadsafe(_do_publish(), self._loop)
        fut.result(timeout = 60)

    def _reinject_stop_signal(self, parent_name : str):
        '''
        Replicas of a node (``nb_tasks > 1``) share one durable pull consumer, so a \
            parent's single stop signal is delivered to only one of them. Whoever \
            receives it puts it back on the parent's stream (impersonating the \
            parent as the envelope's producer) so the remaining replicas each get \
            one too. Guarded to run only on the first stop seen from a given parent, \
            which keeps total pills bounded (one re-injection per messenger instance \
            per parent) instead of ping-ponging between still-running children.
        '''
        self._publish(None, None, '', is_stop_signal = True, as_node_name = parent_name)

    def receive_message(self) -> dict:
        while True:
            all_parents_stopped = (
                bool(self._parent_names) and self._stopped_parents == set(self._parent_names)
            )
            # A control-channel stop ends the flow immediately, even mid-stream —
            # surface it to the task loop as an all-parents-stopped result so
            # ConsumerTask/ProcessorTask break out and run close().
            if all_parents_stopped or self._termination_event.is_set():
                self._last_trace_id = None
                return {
                    name: {'message': None, 'metadata': None, 'is_stop_signal': True}
                    for name in self._parent_names
                }

            complete_trace_id = self._find_complete_group()
            if complete_trace_id is not None:
                group = self._pending_groups.pop(complete_trace_id)
                self._pending_order.remove(complete_trace_id)
                self._last_trace_id = complete_trace_id
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
            for parent_name, entry in self._recv_ready():
                if entry['is_stop_signal']:
                    if parent_name not in self._stopped_parents:
                        self._stopped_parents.add(parent_name)
                        self._reinject_stop_signal(parent_name)
                    continue
                trace_id = entry['trace_id']
                group = self._pending_groups.setdefault(trace_id, {})
                if trace_id not in self._pending_order:
                    self._pending_order.append(trace_id)
                group[parent_name] = entry

            while len(self._pending_order) > self._join_buffer_size:
                stale_trace_id = self._pending_order.pop(0)
                stale_group = self._pending_groups.pop(stale_trace_id)
                logger.warning(
                    f'{self._node.name}: evicting incomplete join group for trace_id '
                    f'{stale_trace_id} (had {list(stale_group.keys())}, needed '
                    f'{self._parent_names}) — a sibling branch likely dropped a '
                    'message under REALTIME flow control.'
                )

    def _find_complete_group(self):
        for trace_id in self._pending_order:
            if len(self._pending_groups[trace_id]) == len(self._parent_names):
                return trace_id
        return None

    def _recv_ready(self):
        '''
        Waits up to a short timeout for at least one parent queue to have an item, \
            then returns every parent item that became ready within that wait as \
            ``[(parent_name, entry), ...]`` — possibly empty on timeout, which lets \
            ``receive_message`` loop back and re-check the termination event instead \
            of blocking forever when the flow is being torn down.
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
            return [(get_tasks[task], task.result()) for task in done]

        fut = asyncio.run_coroutine_threadsafe(_wait_for_any_parent(), self._loop)
        return fut.result()
