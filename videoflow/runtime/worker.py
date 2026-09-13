'''
The single process entrypoint that runs exactly one graph node — used identically
whether the node is launched as a local subprocess (``LocalProcessEngine``) or as a
Kubernetes pod. It's fully driven by environment variables so it needs no access to
the original graph-building script::

    VF_NODE_CLASS       fully-qualified class, e.g. videoflow.processors.basic.IdentityProcessor
    VF_NODE_PARAMS_JSON JSON dict of constructor kwargs (from NodeSpec.params)
    VF_NODE_KIND        producer | processor | consumer. Must match the family of
                        VF_NODE_CLASS; the compiler writes both together, and the
                        worker rejects a disagreement up front (see require_node_kind).
    VF_NODE_NAME        this node's stable name
    VF_PARENT_NAMES     comma-separated parent node names ('' if none)
    VF_HAS_CHILDREN     '1' or '0'
    VF_NATS_URL         nats://host:port
    VF_FLOW_ID          shared flow identifier (stable across runs)
    VF_FLOW_TYPE        realtime | batch
    VF_RUN_ID           per-run identifier that scopes this run's broker streams
    VF_REPLICA_ID       index of this replica (0 for single-task nodes)
    VF_ACK_WAIT_SECONDS optional; per-message ack deadline (default 60)
    VF_MAX_RETRIES      optional; BATCH redelivery attempts before dead-letter (default 3)
    VF_EOS_QUIESCENCE_MS optional; drain quiescence window before honoring EOS (default 500)
    VF_NB_TASKS         optional; replica count of this node (for partition ownership)
    VF_PARTITION_BY     optional; partition key ('trace_id' or a metadata field)
    VF_JOIN_POLICY_JSON optional; JSON JoinPolicy for a multi-parent node
    VF_BLOB_REDIS_URL   optional; enables the external blob store for large payloads.
                        The store is chosen by the URL's scheme (redis:// and
                        rediss:// built in; others via register_blob_store), so the
                        name is historical rather than a restriction to Redis.
    VF_FAULT_SCHEDULE_JSON / VF_FAULT_MARKER_DIR
                        optional; a conformance test's fault schedule (RFC 0006 ENV-16/17),
                        installed before the node is built. Never set by a control plane.
    VF_BLOB_READER_IDS  optional; the reader obligations each payload this node
                        publishes is held for, comma-separated (RFC 0006 BLOB-13);
                        honoured only under VF_RFC0006 with a redis:// blob store.
    VF_BLOB_READERS     optional; how many downstream reads each message this node
                        publishes receives — enables refcounted blob reclamation
                        (PROTOCOL.md BLOB-5). Unset ⇒ blobs are TTL-only.
    VF_BLOB_TTL_SECONDS optional; TTL for offloaded payloads (PROTOCOL.md BLOB-7).
                        Unset ⇒ flow-type default (3600 realtime / 86400 batch).
    VF_GPU_COUNT        optional; whole GPUs granted to this worker (GPU nodes
                        only, RFC 0003). Visible devices are exactly 0..count-1.
                        Informational — a Python node's own gpu_count param is
                        authoritative; native components read this.
    VF_GPU_RESOURCE_NAME optional; extended-resource name the GPUs were requested
                        as, e.g. a MIG profile (RFC 0003).
    VF_ENVELOPE_VERSION optional; wire envelope version to emit (only 4, protobuf)
    VF_DELIVERY         optional; 'at-least-once' | 'best-effort', overriding the
                        flow type's preset for this node (PROTOCOL.md §7)
    VF_ON_ERROR         optional; disposition for exceptions nothing classifies —
                        'poison' | 'transient' | 'worker_fatal' (default transient)
    VF_BREAKER_THRESHOLD optional; consecutive failures before the worker declares
                        itself unhealthy and exits so its inputs go to another
                        replica (default 10; 0 disables)
    VF_PROGRESS_TIMEOUT_SECONDS optional; seconds this node may ack nothing while
                        work is pending before it is declared stalled (default 300;
                        0 disables)
    VF_PROFILE_REQUESTS_JSON  optional; the operator's explicit channel profiles
                        (deploy --require-profile), as JSON. Enforced at bind
                        (RFC 0006 ENV-13): before the node is built or opened,
                        the streams of this node's own channel and its parents'
                        are read back and a stream that does not carry its
                        requested profile ends the worker with
                        IncompatibleProfile (exit 2); one that could not be read
                        back, with UnobservableState (exit 3). Absent => the
                        flow-type presets, i.e. today's behaviour — nothing is
                        read back.
    VF_ADMISSION_TIMEOUT_SECONDS optional; how long that read-back may take,
                        connect included (default 60)
    VF_WATCHDOG_INTERVAL_SECONDS optional; how often a watchdog thread re-checks
                        that same progress deadline while the node is *inside*
                        process()/consume(), so a callback that never returns is
                        still caught — the run loop only checks between messages
                        (default 5; 0 disables the thread, leaving the loop's own
                        check). A stall found this way is written to the
                        termination log and ends the process with the error's
                        exit code (ProgressStalled: 5)
    VF_TERMINATION_LOG  optional; path the structured termination reason is written
                        to (default /dev/termination-log, which Kubernetes surfaces
                        in the pod's containerStatuses)

SIGTERM — a pod being deleted, a rollout, a local ``kill`` — first calls
``messenger.quiesce()`` (stop admitting input; what is already held keeps being
settled) and then takes the signal's default action, so a worker still dies of
SIGTERM exactly as it always has: nothing is acked that was not processed, and the
un-acked inputs are redelivered to its replacement.
'''
from __future__ import absolute_import, division, print_function

import importlib
import json
import logging
import os
import signal
import threading
from types import FrameType
from typing import Any, Callable, Optional, Sequence, Type, TypeVar
from urllib.parse import urlparse

from ..backends import faults
from ..backends.capabilities import (
    ADMISSION_TIMEOUT_ENV,
    PROFILE_REQUESTS_ENV,
    ProfileRequest,
    admission_timeout_from_env,
    requests_from_env,
)
from ..core import constants
from ..core.compiler import NODE_KIND_CONSUMER, NODE_KIND_PROCESSOR, NODE_KIND_PRODUCER
from ..core.context import RuntimeContext
from ..core.engine import Messenger
from ..core.errors import (
    DEFAULT_DISPOSITION,
    DISPOSITIONS,
    EXIT_FLOW_STALLED,
    ConfigError,
    NodeContractError,
    VideoflowError,
    error_to_dict,
)
from ..core.node import ConsumerNode, Node, ProcessorNode, ProducerNode
from ..core.policies import DeliveryPolicy
from ..core.supervision import (
    DEFAULT_BREAKER_THRESHOLD,
    DEFAULT_PROGRESS_TIMEOUT_SECONDS,
    ConsecutiveFailureBreaker,
    ProgressDeadline,
)
from ..core.task import ConsumerTask, ProcessorTask, ProducerTask, Task
from ..wire.redis_payload_store import RedisPayloadStore
from .health import HealthServer, HealthState, InstrumentedMessenger
from .idempotency import RedisIdempotencyStore
from .logging_config import configure_logging
from .watchdog import DEFAULT_WATCHDOG_INTERVAL_SECONDS, ProgressWatchdog

logger = logging.getLogger('videoflow.worker')

#: Where the structured termination reason is written. Kubernetes reads this path
#: by default and surfaces its contents in
#: ``pod.status.containerStatuses[].state.terminated.message``, which is how the
#: deploy watchdog learns *why* a pod died without scraping its logs.
DEFAULT_TERMINATION_LOG = '/dev/termination-log'

def write_termination_reason(error : BaseException,
                            path : str | None = None) -> None:
    '''
    Records why this worker is exiting, in machine-readable form.

    Deliberately best-effort and completely silent on failure: the file may not
    exist outside Kubernetes, and a worker that cannot explain its death must
    still die of the original cause rather than of a logging problem.

    - Arguments:
        - error: the exception that ended the worker.
        - path: override for the termination-log path (``VF_TERMINATION_LOG``).
    '''
    target = path or os.environ.get('VF_TERMINATION_LOG') or DEFAULT_TERMINATION_LOG
    try:
        with open(target, 'w') as f:
            f.write(json.dumps(error_to_dict(error), sort_keys = True))
    except Exception:
        logger.debug(f'could not write termination reason to {target}', exc_info = True)

def exit_on_stall(error : BaseException,
                exit_process : Callable[[int], Any] = os._exit) -> None:
    '''
    What the progress watchdog does with a stall: record the reason, then end the
    process. ``os._exit`` rather than an exception, because the watchdog runs on
    its own thread and the frame that is wedged — a ``process()`` that never
    returned — belongs to the main thread, which nothing can unwind. The un-acked
    inputs go back to the broker for the replacement, exactly as they would after
    any other death.

    - Arguments:
        - error: the ``ProgressStalled`` (or ``BrokerUnavailable``) the deadline raised.
        - exit_process: the process-ending call; injected so a test can observe \
            the exit status instead of losing the interpreter.
    '''
    code = error.exit_code if isinstance(error, VideoflowError) else EXIT_FLOW_STALLED
    logger.error(f'progress watchdog: {error}')
    write_termination_reason(error)
    exit_process(code)

def watchdog_interval_from_env() -> float:
    '''
    ``VF_WATCHDOG_INTERVAL_SECONDS`` as a number: the default when unset, 0 for
    "no watchdog thread".

    - Raises:
        - ConfigError: if the value is not a number, or is negative.
    '''
    raw = os.environ.get('VF_WATCHDOG_INTERVAL_SECONDS')
    if raw in (None, ''):
        return DEFAULT_WATCHDOG_INTERVAL_SECONDS
    try:
        interval = float(raw)
    except ValueError as e:
        raise ConfigError(
            f'VF_WATCHDOG_INTERVAL_SECONDS={raw!r} is not a number.',
            remedy = 'Set it to the seconds between progress checks, or 0 to disable '
                    'the watchdog thread.') from e
    if interval < 0:
        raise ConfigError(
            f'VF_WATCHDOG_INTERVAL_SECONDS={raw!r} is negative.',
            remedy = 'Set it to the seconds between progress checks, or 0 to disable '
                    'the watchdog thread.')
    return interval

def build_watchdog(deadline : ProgressDeadline, interval_seconds : float,
                progress_timeout : float, node_name : str,
                on_stall : Callable[[BaseException], None] = exit_on_stall,
                ) -> Optional[ProgressWatchdog]:
    '''
    The watchdog for a non-producer node, or ``None`` when either knob disables
    it: an interval of 0 means no thread, and a progress timeout of 0 means the
    deadline itself never trips, so a thread would only be re-checking nothing.

    - Arguments:
        - deadline: the node's ``ProgressDeadline`` — the same instance the task \
            loop is given, which is what makes the watchdog's reading of \
            "silence" the loop's own.
        - interval_seconds: ``VF_WATCHDOG_INTERVAL_SECONDS``.
        - progress_timeout: ``VF_PROGRESS_TIMEOUT_SECONDS``.
        - node_name: names the thread.
        - on_stall: what to do with the stall; the process-ending default is \
            replaced in tests.
    '''
    if interval_seconds <= 0 or progress_timeout <= 0:
        return None
    return ProgressWatchdog(deadline, interval_seconds, on_stall, name = node_name)

def install_sigterm_quiesce(messenger : Messenger,
                            then : Optional[Callable[[int], None]] = None,
                            ) -> Callable[[], None]:
    '''
    SIGTERM → ``messenger.quiesce()``, then the signal's default action.

    Quiescing first is what lets the messenger stop admitting input (and hand
    back what it prefetched but never delivered) *before* the process is gone,
    instead of leaving those messages to time out on the broker; the default
    action afterwards keeps every existing contract about how a SIGTERMed worker
    dies — exit status, no clean end-of-stream, un-acked inputs redelivered.
    Finishing the in-flight message before exiting is the rollout drain's job
    (a later phase), not this hook's.

    Only the main thread may install a signal handler, so from any other thread
    (a test driving ``run_from_env`` in-process) this installs nothing.

    - Arguments:
        - messenger: whose ``quiesce()`` runs on SIGTERM.
        - then: what follows the quiesce, given the signal number. Default: \
            restore ``SIG_DFL`` and re-raise the signal to this process. \
            Injected so a test can deliver a real SIGTERM without dying of it.

    - Returns:
        - a callable that restores the previous SIGTERM disposition (a no-op \
            when nothing was installed).
    '''
    if threading.current_thread() is not threading.main_thread():
        logger.debug('not on the main thread: SIGTERM quiesce hook not installed')
        return lambda: None

    def _resignal_default(signum : int) -> None:
        signal.signal(signum, signal.SIG_DFL)
        os.kill(os.getpid(), signum)

    follow_up = then if then is not None else _resignal_default

    def _handler(signum : int, frame : Optional[FrameType]) -> None:
        logger.info('SIGTERM received: quiescing input before exiting')
        try:
            messenger.quiesce()
        except Exception:
            # A quiesce that fails must not keep a doomed worker alive.
            logger.debug('quiesce on SIGTERM failed', exc_info = True)
        finally:
            follow_up(signum)

    previous = signal.signal(signal.SIGTERM, _handler)

    def _restore() -> None:
        # Only if ours is still installed: the handler itself may have swapped
        # in SIG_DFL, and a re-installed default must stay that way.
        if signal.getsignal(signal.SIGTERM) is _handler:
            signal.signal(signal.SIGTERM, previous)
    return _restore

def _import_class(fq_class : str) -> type:
    module_path, class_name = fq_class.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)

def _resolve_replica_id() -> int:
    '''
    Replica id from VF_REPLICA_ID (local engine sets it), else parsed from the
    trailing ordinal of POD_NAME/HOSTNAME (a StatefulSet pod is ``<name>-<n>``),
    else 0.
    '''
    explicit = os.environ.get('VF_REPLICA_ID')
    if explicit not in (None, ''):
        try:
            return int(explicit)
        except ValueError:
            pass
    for var in ('POD_NAME', 'HOSTNAME'):
        val = os.environ.get(var, '')
        if '-' in val:
            tail = val.rsplit('-', 1)[1]
            if tail.isdigit():
                return int(tail)
    return 0

_N = TypeVar('_N', bound = Node)

def require_node_kind(node : Node, expected : Type[_N], kind : str) -> _N:
    '''
    Check that the node built from ``VF_NODE_CLASS`` matches the node family declared
    by ``VF_NODE_KIND``, and return it narrowed to that family.

    The two env vars are written together by the compiler, so a mismatch means the
    ConfigMap was hand-edited or the image is stale. Without this check the wrong
    Task type is constructed and the disagreement only surfaces later, deep in the
    run loop, as an opaque ``AttributeError`` (e.g. calling ``next()`` on a consumer).

    - Raises:
        - ValueError: if ``node`` is not an instance of ``expected``.
    '''
    if not isinstance(node, expected):
        actual = f'{type(node).__module__}.{type(node).__name__}'
        raise NodeContractError(
            f'VF_NODE_KIND={kind!r} requires a {expected.__name__}, but VF_NODE_CLASS '
            f'({actual}) is not one.',
            remedy = ('These two are set together by the compiler — redeploy the flow '
                    'rather than editing the ConfigMap by hand.'),
            expected = expected.__name__, actual = actual)
    return node

def build_node_from_env() -> Node:
    fq_class = os.environ.get('VF_NODE_CLASS')
    if not fq_class:
        # A remote (language-agnostic) component must run its own image's entrypoint,
        # not the Python worker. Reaching here means a remote node was scheduled onto
        # a Python worker image — a deploy/image mismatch.
        ref = os.environ.get('VF_COMPONENT_REF', '<unknown>')
        raise ConfigError(
            f'VF_NODE_CLASS is not set (component_ref={ref!r}). The Python worker only '
            'runs native videoflow nodes; a remote component must run its own image.',
            remedy = "Check that the node's image and descriptor command are set correctly.",
            component_ref = ref)
    node_class = _import_class(fq_class)
    params = json.loads(os.environ.get('VF_NODE_PARAMS_JSON', '{}'))
    return node_class(**params)

def verify_explicit_profiles(nats_url : str, flow_id : str, run_id : str, flow_type : str, node_name : str,
                             parent_names : Sequence[str], has_children : bool,
                             requests : Sequence[ProfileRequest], timeout : float) -> None:
    '''
    Bind the operator's explicit channel profiles (``VF_PROFILE_REQUESTS_JSON``)
    to the streams this node touches — its own output channel and its parents'
    — before it opens. The streams are read back over a connection of their own
    (``topology.read_back_streams``; nothing reaches into the messenger), and a
    stream whose effective configuration contradicts the profile it was
    requested to carry is refused: ``IncompatibleProfile``, which ``main`` turns
    into exit 2 with the reason in the termination log. A stream that could not
    be read back is refused too (``UnobservableState``, exit 3): an explicit
    request does not pass on an unread guarantee. Requests for channels this
    node neither publishes nor consumes are other workers' to judge; no
    requests at all means nothing is read and nothing changes.
    '''
    channels = ([node_name] if has_children else []) + list(parent_names)
    mine = [r for r in requests if r.channel in channels]
    if not mine:
        return
    # Deferred: topology imports the optional `nats` client at module scope.
    from ..messaging import topology
    read_back = topology.read_back_streams(nats_url, flow_id, run_id, [r.channel for r in mine], flow_type,
                                           timeout = timeout)
    topology.verify_channel_profiles(read_back, mine, unknown_is_fatal = True, where = f'worker {node_name}')
    logger.info('explicit channel profiles verified at bind: '
                + ', '.join(f'{r.channel}={r.profile}' for r in mine))

def run_from_env() -> None:
    # Deferred: nats_messenger imports the optional `nats` client at module scope.
    from ..messaging.nats_messenger import NATSMessenger

    node_name = os.environ['VF_NODE_NAME']
    kind = os.environ['VF_NODE_KIND']
    parent_names = [p for p in os.environ.get('VF_PARENT_NAMES', '').split(',') if p]
    has_children = os.environ.get('VF_HAS_CHILDREN', '1') == '1'
    nats_url = os.environ['VF_NATS_URL']
    flow_id = os.environ['VF_FLOW_ID']
    flow_type = os.environ.get('VF_FLOW_TYPE', 'realtime')
    run_id = os.environ['VF_RUN_ID']
    replica_id = _resolve_replica_id()
    ack_wait = int(os.environ.get('VF_ACK_WAIT_SECONDS', '60'))
    max_retries = int(os.environ.get('VF_MAX_RETRIES', '3'))
    eos_quiescence_ms = int(os.environ.get('VF_EOS_QUIESCENCE_MS', '500'))
    nb_tasks = int(os.environ.get('VF_NB_TASKS', '1'))
    partition_by = os.environ.get('VF_PARTITION_BY') or None
    join_policy_json = os.environ.get('VF_JOIN_POLICY_JSON')
    join_policy = json.loads(join_policy_json) if join_policy_json else None

    # Per-node failure handling. Absent ⇒ the flow type's preset, which is what a
    # flow that never touches these gets.
    delivery = os.environ.get('VF_DELIVERY') or None
    on_error = os.environ.get('VF_ON_ERROR') or None
    if on_error is not None and on_error not in DISPOSITIONS:
        raise ConfigError(
            f'VF_ON_ERROR={on_error!r} is not a known disposition.',
            remedy = f'Use one of: {", ".join(DISPOSITIONS)}.')
    delivery_policy = {'delivery': delivery, 'on_error': on_error} if (delivery or on_error) else None
    breaker_threshold = int(os.environ.get('VF_BREAKER_THRESHOLD',
                                        str(DEFAULT_BREAKER_THRESHOLD)))
    progress_timeout = float(os.environ.get('VF_PROGRESS_TIMEOUT_SECONDS',
                                        str(DEFAULT_PROGRESS_TIMEOUT_SECONDS)))
    watchdog_interval = watchdog_interval_from_env()

    # Deferred: serialization imports the optional `msgpack`/`protobuf` deps at module scope.
    from ..wire.serialization import DEFAULT_ENVELOPE_VERSION, EMITTABLE_ENVELOPE_VERSIONS
    envelope_version = int(os.environ.get('VF_ENVELOPE_VERSION', str(DEFAULT_ENVELOPE_VERSION)))
    if envelope_version not in EMITTABLE_ENVELOPE_VERSIONS:
        raise ConfigError(
            f'VF_ENVELOPE_VERSION={envelope_version} is not emittable by this build.',
            remedy = f'Supported versions: {EMITTABLE_ENVELOPE_VERSIONS}.')

    # The operator's explicit channel profiles bind here, before anything else: no
    # node is built, nothing has opened, a producer has published nothing when a
    # request is refused. Absent ⇒ nothing is read back (today's behaviour).
    explicit_profiles = requests_from_env(os.environ.get(PROFILE_REQUESTS_ENV))
    if explicit_profiles:
        logger.info('explicit channel profiles requested: ' +
                    ', '.join(f'{r.channel}={r.profile}' for r in explicit_profiles))
        verify_explicit_profiles(nats_url, flow_id, run_id, flow_type, node_name, parent_names, has_children,
                                 explicit_profiles,
                                 timeout = admission_timeout_from_env(os.environ.get(ADMISSION_TIMEOUT_ENV)))

    blob_store = None
    payload_store = None
    # Env var name is historical: any registered URL scheme works, not just Redis.
    blob_redis_url = os.environ.get('VF_BLOB_REDIS_URL')
    if blob_redis_url:
        if constants.RFC0006 and urlparse(blob_redis_url).scheme in ('redis', 'rediss'):
            # Obligation-keeping store (RFC 0006 BLOB-13..15) on the same keys the
            # counter store used, so a blob written either way is readable both ways.
            payload_store = RedisPayloadStore(blob_redis_url)
        else:
            # Deferred: serialization imports the optional `msgpack`/`protobuf` deps at module scope.
            from ..wire.serialization import make_blob_store
            blob_store = make_blob_store(blob_redis_url)
    # The reader obligations this node's payloads are held for (BLOB-13); absent
    # or under the switch off ⇒ no identity obligations (the count above applies).
    blob_reader_ids = [r for r in os.environ.get('VF_BLOB_READER_IDS', '').split(',') if r]
    # Absent ⇒ None ⇒ refcounted reclamation off — the safe default for a manifest
    # rendered by an older CLI (a default of 1 would delete fan-out blobs after the
    # first child's ack while siblings still need them).
    blob_readers_env = os.environ.get('VF_BLOB_READERS')
    blob_readers = int(blob_readers_env) if blob_readers_env else None
    # Absent ⇒ None ⇒ the messenger picks the flow-type default (BLOB-7).
    blob_ttl_env = os.environ.get('VF_BLOB_TTL_SECONDS')
    blob_ttl_seconds = int(blob_ttl_env) if blob_ttl_env else None

    node = build_node_from_env()
    # The node's own name comes from the env, not from whatever get_params captured
    # (they should match, but the env is authoritative for routing).
    node._name = node_name

    messenger: Messenger = NATSMessenger(
        node, parent_names, nats_url, flow_id, flow_type, run_id,
        blob_store = blob_store, replica_id = replica_id,
        ack_wait = ack_wait, max_retries = max_retries,
        eos_quiescence_ms = eos_quiescence_ms, nb_tasks = nb_tasks,
        partition_by = partition_by, join_policy = join_policy,
        envelope_version = envelope_version, blob_readers = blob_readers,
        blob_ttl_seconds = blob_ttl_seconds, delivery_policy = delivery_policy,
        payload_store = payload_store, blob_reader_ids = blob_reader_ids,
    )

    # Health/metrics server: reads VF_HEALTH_PORT (0 disables, e.g. under the local
    # engine where several workers share a host and would collide on the port).
    health_port = int(os.environ.get('VF_HEALTH_PORT', '0'))
    health_server = None
    if health_port > 0:
        state = HealthState(node_name)
        health_server = HealthServer(state, port = health_port)
        health_server.start()
        # The resolved policy lets the instrumented messenger count drops by the
        # verdict it produces (best-effort exhaustion vs poison), not only poison.
        messenger = InstrumentedMessenger(messenger, state,
                                          delivery_policy = DeliveryPolicy.resolve(flow_type, delivery_policy,
                                                                                    max_retries))

    ctx = RuntimeContext(
        flow_id, run_id, node_name, replica_id,
        logging.getLogger(f'videoflow.node.{node_name}'), messenger = messenger,
    )

    # Self-protection, constructed here and handed down (the task lives in core,
    # which must not depend on runtime). A producer gets none of it: it has no
    # inputs to fail on and no durable to be pending against. The watchdog holds
    # the *same* deadline the task loop does — the loop resets it on every ack,
    # the watchdog re-reads it from a thread the wedged callback cannot block.
    breaker = None
    deadline = None
    watchdog = None
    if kind != NODE_KIND_PRODUCER:
        breaker = ConsecutiveFailureBreaker(breaker_threshold, node_name)
        deadline = ProgressDeadline(progress_timeout, messenger.pending_observation, node_name)
        watchdog = build_watchdog(deadline, watchdog_interval, progress_timeout, node_name)

    task: Task
    if kind == NODE_KIND_PRODUCER:
        task = ProducerTask(require_node_kind(node, ProducerNode, kind),
                            messenger, has_children, ctx = ctx,
                            on_error = on_error or DEFAULT_DISPOSITION)
    elif kind == NODE_KIND_PROCESSOR:
        task = ProcessorTask(require_node_kind(node, ProcessorNode, kind),
                            messenger, has_children, parent_names, ctx = ctx,
                            breaker = breaker, deadline = deadline,
                            on_error = on_error or DEFAULT_DISPOSITION,
                            watchdog = watchdog)
    elif kind == NODE_KIND_CONSUMER:
        consumer = require_node_kind(node, ConsumerNode, kind)
        idem_store = None
        if consumer.idempotent and blob_redis_url:
            idem_store = RedisIdempotencyStore(blob_redis_url)
        task = ConsumerTask(consumer, messenger, has_children, parent_names, ctx = ctx,
                            idempotency_store = idem_store,
                            breaker = breaker, deadline = deadline,
                            on_error = on_error or DEFAULT_DISPOSITION,
                            watchdog = watchdog)
    else:
        raise ConfigError(f'Unknown VF_NODE_KIND: {kind!r}.',
                        remedy = f'Expected one of: {NODE_KIND_PRODUCER}, '
                                f'{NODE_KIND_PROCESSOR}, {NODE_KIND_CONSUMER}.')

    logger.info(f'Worker starting: node={node_name} kind={kind} parents={parent_names}')
    restore_sigterm = install_sigterm_quiesce(messenger)
    try:
        task.run()
    except BaseException as e:
        # Record *why* before dying, so the deploy watchdog can report the cause
        # from the Kubernetes API instead of guessing from a crash-loop.
        if not isinstance(e, KeyboardInterrupt):
            write_termination_reason(e)
        raise
    finally:
        restore_sigterm()
        messenger.close()
        if health_server is not None:
            health_server.stop()
    logger.info(f'Worker finished: node={node_name}')

def main() -> int:
    '''
    Entrypoint. Returns the process exit status rather than raising, so a typed
    failure exits with the code its class carries (see
    ``videoflow.core.errors``) and an operator can tell a bad node from a bad
    cluster without reading the log.
    '''
    configure_logging()
    try:
        # A test's fault schedule (ENV-16/ENV-17) is installed before anything the
        # barriers guard can run; absent, a barrier is one attribute read.
        schedule = faults.FaultSchedule.from_env()
        if schedule is not None:
            schedule.install()
        run_from_env()
    except VideoflowError as e:
        logger.error(f'{e.code}: {e.message}' + (f' {e.remedy}' if e.remedy else ''))
        write_termination_reason(e)
        return e.exit_code
    except KeyboardInterrupt:
        return 130
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
