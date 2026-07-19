'''
The single process entrypoint that runs exactly one graph node — used identically
whether the node is launched as a local subprocess (``LocalProcessEngine``) or as a
Kubernetes pod. It's fully driven by environment variables so it needs no access to
the original graph-building script.

    VF_NODE_CLASS       fully-qualified class, e.g. videoflow.processors.basic.IdentityProcessor
    VF_NODE_PARAMS_JSON JSON dict of constructor kwargs (from NodeSpec.params)
    VF_NODE_KIND        producer | processor | consumer
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
    VF_ENVELOPE_VERSION optional; wire envelope version to emit (3 msgpack | 4 protobuf)
    VF_ALLOW_PICKLE     optional; '1' permits the legacy Python-only pickle payload codec
'''
from __future__ import absolute_import, division, print_function

import importlib
import json
import logging
import os
from typing import Any

logger = logging.getLogger('videoflow.worker')

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

def build_node_from_env() -> Any:
    fq_class = os.environ.get('VF_NODE_CLASS')
    if not fq_class:
        # A remote (language-agnostic) component must run its own image's entrypoint,
        # not the Python worker. Reaching here means a remote node was scheduled onto
        # a Python worker image — a deploy/image mismatch.
        ref = os.environ.get('VF_COMPONENT_REF', '<unknown>')
        raise RuntimeError(
            f'VF_NODE_CLASS is not set (component_ref={ref!r}). The Python worker only '
            'runs native videoflow nodes; a remote component must run its own image. '
            'Check that the node\'s image and descriptor command are set correctly.')
    node_class = _import_class(fq_class)
    params = json.loads(os.environ.get('VF_NODE_PARAMS_JSON', '{}'))
    return node_class(**params)

def run_from_env() -> None:
    from ..core.compiler import NODE_KIND_CONSUMER, NODE_KIND_PROCESSOR, NODE_KIND_PRODUCER
    from ..core.task import ConsumerTask, ProcessorTask, ProducerTask, Task
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

    from ..wire.serialization import DEFAULT_ENVELOPE_VERSION, EMITTABLE_ENVELOPE_VERSIONS
    envelope_version = int(os.environ.get('VF_ENVELOPE_VERSION', str(DEFAULT_ENVELOPE_VERSION)))
    if envelope_version not in EMITTABLE_ENVELOPE_VERSIONS:
        raise ValueError(f'VF_ENVELOPE_VERSION={envelope_version} is not emittable by this '
                        f'build (supported: {EMITTABLE_ENVELOPE_VERSIONS})')
    allow_pickle = os.environ.get('VF_ALLOW_PICKLE', '0') == '1'

    blob_store = None
    # Env var name is historical: any registered URL scheme works, not just Redis.
    blob_redis_url = os.environ.get('VF_BLOB_REDIS_URL')
    if blob_redis_url:
        from ..wire.serialization import make_blob_store
        blob_store = make_blob_store(blob_redis_url)

    node = build_node_from_env()
    # The node's own name comes from the env, not from whatever get_params captured
    # (they should match, but the env is authoritative for routing).
    node._name = node_name

    from ..core.engine import Messenger
    messenger: Messenger = NATSMessenger(
        node, parent_names, nats_url, flow_id, flow_type, run_id,
        blob_store = blob_store, replica_id = replica_id,
        ack_wait = ack_wait, max_retries = max_retries,
        eos_quiescence_ms = eos_quiescence_ms, nb_tasks = nb_tasks,
        partition_by = partition_by, join_policy = join_policy,
        envelope_version = envelope_version, allow_pickle = allow_pickle,
    )

    # Health/metrics server: reads VF_HEALTH_PORT (0 disables, e.g. under the local
    # engine where several workers share a host and would collide on the port).
    from .health import HealthServer, HealthState, InstrumentedMessenger
    health_port = int(os.environ.get('VF_HEALTH_PORT', '0'))
    health_server = None
    if health_port > 0:
        state = HealthState(node_name)
        health_server = HealthServer(state, port = health_port)
        health_server.start()
        messenger = InstrumentedMessenger(messenger, state)

    from ..core.context import RuntimeContext
    ctx = RuntimeContext(
        flow_id, run_id, node_name, replica_id,
        logging.getLogger(f'videoflow.node.{node_name}'), messenger = messenger,
    )

    task: Task
    if kind == NODE_KIND_PRODUCER:
        task = ProducerTask(node, messenger, has_children, ctx = ctx)
    elif kind == NODE_KIND_PROCESSOR:
        task = ProcessorTask(node, messenger, has_children, parent_names, ctx = ctx)
    elif kind == NODE_KIND_CONSUMER:
        idem_store = None
        if getattr(node, 'idempotent', False) and blob_redis_url:
            from .idempotency import RedisIdempotencyStore
            idem_store = RedisIdempotencyStore(blob_redis_url)
        task = ConsumerTask(node, messenger, has_children, parent_names, ctx = ctx,
                            idempotency_store = idem_store)
    else:
        raise ValueError(f'Unknown VF_NODE_KIND: {kind}')

    logger.info(f'Worker starting: node={node_name} kind={kind} parents={parent_names}')
    try:
        task.run()
    finally:
        messenger.close()
        if health_server is not None:
            health_server.stop()
    logger.info(f'Worker finished: node={node_name}')

def main() -> None:
    from .logging_config import configure_logging
    configure_logging()
    run_from_env()

if __name__ == '__main__':
    main()
