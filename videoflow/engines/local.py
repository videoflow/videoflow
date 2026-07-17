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
        self._run_id = None
        super(LocalProcessEngine, self).__init__()

    def _al_create_and_start_processes(self, tasks_data, flow_id : str, flow_type : str, run_id : str):
        self._flow_id = flow_id
        self._run_id = run_id
        specs = self._specs if self._specs is not None else specs_from_tasks_data(tasks_data)

        # Provision streams + durable consumers up front. Required for BATCH: under
        # interest retention, a message published before its consumer exists is lost.
        from ..messaging.topology import provision_flow_sync
        provision_flow_sync(self._nats_url, specs, flow_id, run_id, flow_type)

        for spec in specs:
            for replica_idx in range(spec.nb_tasks):
                env = _worker_env(spec, self._nats_url, flow_id, flow_type, run_id,
                                self._blob_redis_url, replica_idx)
                proc = subprocess.Popen([sys.executable, '-m', 'videoflow.worker'], env = env)
                self._procs.append(proc)
                logger.info(
                    f'Started worker pid={proc.pid} node={spec.name} replica={replica_idx}'
                )

    def signal_flow_termination(self):
        _publish_stop(self._nats_url, self._flow_id, self._run_id)

    def join_task_processes(self):
        try:
            for proc in self._procs:
                try:
                    proc.wait()
                except KeyboardInterrupt:
                    proc.wait()
        finally:
            self._teardown_streams()

    def _teardown_streams(self):
        if self._flow_id is None or self._run_id is None:
            return
        import asyncio
        from ..messaging.topology import delete_run_streams

        async def _go():
            nc = await nats.connect(self._nats_url)
            try:
                await delete_run_streams(nc, self._flow_id, self._run_id)
            finally:
                await nc.drain()

        try:
            asyncio.run(_go())
        except Exception:
            logger.debug('stream teardown failed', exc_info = True)

def _worker_env(spec : NodeSpec, nats_url, flow_id, flow_type, run_id, blob_redis_url, replica_id):
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
        'VF_RUN_ID': run_id,
        'VF_REPLICA_ID': str(replica_id),
        'VF_NB_TASKS': str(spec.nb_tasks),
    })
    if spec.partition_by:
        env['VF_PARTITION_BY'] = spec.partition_by
    if spec.join_policy:
        env['VF_JOIN_POLICY_JSON'] = json.dumps(spec.join_policy)
    if blob_redis_url:
        env['VF_BLOB_REDIS_URL'] = blob_redis_url
    return env

def _publish_stop(nats_url, flow_id, run_id):
    import asyncio
    from ..messaging.topology import control_subject_for

    async def _go():
        nc = await nats.connect(nats_url)
        await nc.publish(control_subject_for(flow_id, run_id), b'stop')
        await nc.flush()
        await nc.drain()

    asyncio.run(_go())
