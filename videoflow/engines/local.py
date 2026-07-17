'''
Execution engine that runs a distributed flow entirely on the local machine, one
OS subprocess per node (per replica, for nb_tasks > 1), all talking to a local
NATS server. Same ``videoflow.worker`` code path Kubernetes uses — only the way
processes are started differs — so it's the primary way to develop and test a
flow without a cluster.

Prerequisite: a running NATS JetStream server, e.g. ``nats-server -js`` or
``docker run -p 4222:4222 nats -js``.
'''
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import json
import logging
import os
import subprocess
import sys

import nats  # noqa: F401  (import guard: fail fast if the broker client is missing)

from ..core.engine import ExecutionEngine
from ..compiler import NodeSpec, specs_from_tasks_data

logger = logging.getLogger(__package__)

DEFAULT_NATS_URL = 'nats://localhost:4222'

class LocalProcessEngine(ExecutionEngine):
    '''
    - Arguments:
        - nats_url: URL of the NATS server every worker connects to.
        - blob_redis_url: optional Redis URL for the large-payload blob store.
        - specs: optional precompiled list of ``NodeSpec``. If not given, they are \
            compiled from the flow's ``tasks_data`` at ``allocate_and_run_tasks`` time.
    '''
    def __init__(self, nats_url : str = DEFAULT_NATS_URL, blob_redis_url : str = None, specs = None):
        self._nats_url = nats_url
        self._blob_redis_url = blob_redis_url
        self._specs = specs
        self._procs = []
        self._flow_id = None
        super(LocalProcessEngine, self).__init__()

    def _al_create_and_start_processes(self, tasks_data, flow_id : str, flow_type : str):
        self._flow_id = flow_id
        specs = self._specs if self._specs is not None else specs_from_tasks_data(tasks_data)

        for spec in specs:
            for replica_idx in range(spec.nb_tasks):
                env = _worker_env(spec, self._nats_url, flow_id, flow_type, self._blob_redis_url)
                proc = subprocess.Popen([sys.executable, '-m', 'videoflow.worker'], env = env)
                self._procs.append(proc)
                logger.info(
                    f'Started worker pid={proc.pid} node={spec.name} replica={replica_idx}'
                )

    def signal_flow_termination(self):
        _publish_stop(self._nats_url, self._flow_id)

    def join_task_processes(self):
        for proc in self._procs:
            try:
                proc.wait()
            except KeyboardInterrupt:
                proc.wait()

def _worker_env(spec : NodeSpec, nats_url, flow_id, flow_type, blob_redis_url):
    env = dict(os.environ)
    env.update({
        'VF_NODE_CLASS': spec.node_class,
        'VF_NODE_PARAMS_JSON': json.dumps(spec.params),
        'VF_NODE_KIND': spec.kind,
        'VF_NODE_NAME': spec.name,
        'VF_PARENT_NAMES': ','.join(spec.parents),
        'VF_HAS_CHILDREN': '1' if spec.has_children else '0',
        'VF_NATS_URL': nats_url,
        'VF_FLOW_ID': flow_id,
        'VF_FLOW_TYPE': flow_type,
    })
    if blob_redis_url:
        env['VF_BLOB_REDIS_URL'] = blob_redis_url
    return env

def _publish_stop(nats_url, flow_id):
    import asyncio
    from ..messaging.nats_messenger import control_subject_for

    async def _go():
        nc = await nats.connect(nats_url)
        await nc.publish(control_subject_for(flow_id), b'stop')
        await nc.flush()
        await nc.drain()

    asyncio.run(_go())
