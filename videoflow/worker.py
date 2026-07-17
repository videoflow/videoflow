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
    VF_FLOW_ID          shared flow identifier
    VF_FLOW_TYPE        realtime | batch
    VF_BLOB_REDIS_URL   optional; enables the external blob store for large payloads
'''
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import importlib
import json
import logging
import os
import sys

logger = logging.getLogger('videoflow.worker')

def _import_class(fq_class : str):
    module_path, class_name = fq_class.rsplit('.', 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)

def build_node_from_env():
    node_class = _import_class(os.environ['VF_NODE_CLASS'])
    params = json.loads(os.environ.get('VF_NODE_PARAMS_JSON', '{}'))
    return node_class(**params)

def run_from_env():
    from .core.task import ProducerTask, ProcessorTask, ConsumerTask
    from .messaging.nats_messenger import NATSMessenger
    from .compiler import NODE_KIND_PRODUCER, NODE_KIND_PROCESSOR, NODE_KIND_CONSUMER

    node_name = os.environ['VF_NODE_NAME']
    kind = os.environ['VF_NODE_KIND']
    parent_names = [p for p in os.environ.get('VF_PARENT_NAMES', '').split(',') if p]
    has_children = os.environ.get('VF_HAS_CHILDREN', '1') == '1'
    nats_url = os.environ['VF_NATS_URL']
    flow_id = os.environ['VF_FLOW_ID']
    flow_type = os.environ.get('VF_FLOW_TYPE', 'realtime')

    blob_store = None
    blob_redis_url = os.environ.get('VF_BLOB_REDIS_URL')
    if blob_redis_url:
        from .serialization import RedisBlobStore
        blob_store = RedisBlobStore(blob_redis_url)

    node = build_node_from_env()
    # The node's own name comes from the env, not from whatever get_params captured
    # (they should match, but the env is authoritative for routing).
    node._name = node_name

    messenger = NATSMessenger(
        node, parent_names, nats_url, flow_id, flow_type, blob_store = blob_store,
    )

    # Health/metrics server: reads VF_HEALTH_PORT (0 disables, e.g. under the local
    # engine where several workers share a host and would collide on the port).
    from .health import HealthState, HealthServer, InstrumentedMessenger
    health_port = int(os.environ.get('VF_HEALTH_PORT', '0'))
    health_server = None
    if health_port > 0:
        state = HealthState(node_name)
        health_server = HealthServer(state, port = health_port)
        health_server.start()
        messenger = InstrumentedMessenger(messenger, state)

    if kind == NODE_KIND_PRODUCER:
        task = ProducerTask(node, messenger, has_children)
    elif kind == NODE_KIND_PROCESSOR:
        task = ProcessorTask(node, messenger, has_children, parent_names)
    elif kind == NODE_KIND_CONSUMER:
        task = ConsumerTask(node, messenger, has_children, parent_names)
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

def main():
    logging.basicConfig(
        level = logging.INFO,
        format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    )
    run_from_env()

if __name__ == '__main__':
    main()
