'''
Execution engine that runs a distributed flow on a Kubernetes cluster: it renders
one Deployment/Job (+ ConfigMap) per node and applies them with ``kubectl``. The
worker container image is the same ``videoflow.worker`` entrypoint the local engine
uses — only the launch mechanism (pods vs. subprocesses) differs.

Requires ``kubectl`` on PATH, configured against the target cluster, and the
per-component images already built and pushed (see ``docker/``).
'''
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import logging
import subprocess

from ..core.engine import ExecutionEngine
from ..compiler import specs_from_tasks_data
from ..manifests import (
    render_manifests, dump_manifests, LABEL_FLOW_ID, k8s_name,
)

logger = logging.getLogger(__package__)

class KubernetesExecutionEngine(ExecutionEngine):
    '''
    - Arguments:
        - nats_url: URL workers use to reach NATS from inside the cluster, \
            e.g. ``nats://nats.videoflow.svc:4222``.
        - namespace: target namespace (must already exist).
        - registry: image registry prefix, e.g. ``ghcr.io/acme``. Empty for local \
            images (kind/minikube).
        - image_tag: tag applied to every ``videoflow-<family>`` image.
        - blob_redis_url: optional Redis URL for the large-payload blob store.
        - specs: optional precompiled ``NodeSpec`` list; compiled from tasks_data if omitted.
        - kubectl: kubectl binary name/path.
    '''
    def __init__(self, nats_url : str, namespace : str = 'default', registry : str = '',
                image_tag : str = 'latest', blob_redis_url : str = None, specs = None,
                kubectl : str = 'kubectl'):
        self._nats_url = nats_url
        self._namespace = namespace
        self._registry = registry
        self._image_tag = image_tag
        self._blob_redis_url = blob_redis_url
        self._specs = specs
        self._kubectl = kubectl
        self._flow_id = None
        self._run_id = None
        super(KubernetesExecutionEngine, self).__init__()

    def _al_create_and_start_processes(self, tasks_data, flow_id : str, flow_type : str, run_id : str):
        self._flow_id = flow_id
        self._run_id = run_id
        specs = self._specs if self._specs is not None else specs_from_tasks_data(tasks_data)
        manifests = render_manifests(
            specs, flow_id, flow_type, self._nats_url, run_id, namespace = self._namespace,
            registry = self._registry, image_tag = self._image_tag,
            blob_redis_url = self._blob_redis_url,
        )
        yaml_str = dump_manifests(manifests)
        self._kubectl_apply(yaml_str)
        logger.info(f'Applied {len(manifests)} manifests for flow {flow_id} to namespace {self._namespace}')

    def _kubectl_apply(self, yaml_str : str):
        proc = subprocess.run(
            [self._kubectl, 'apply', '-n', self._namespace, '-f', '-'],
            input = yaml_str.encode('utf-8'),
            capture_output = True,
        )
        if proc.returncode != 0:
            raise RuntimeError(f'kubectl apply failed: {proc.stderr.decode("utf-8")}')
        logger.info(proc.stdout.decode('utf-8').strip())

    def signal_flow_termination(self):
        '''
        Publishes the control-channel stop, then deletes the flow's workloads. The
        control message lets in-flight pods drain cleanly; the delete tears down the
        long-running Deployments that would otherwise never exit on their own.
        '''
        _publish_stop(self._nats_url, self._flow_id, self._run_id)
        selector = f'{LABEL_FLOW_ID}={k8s_name(self._flow_id)}'
        subprocess.run(
            [self._kubectl, 'delete', '-n', self._namespace,
             'deployment,job,configmap,networkpolicy', '-l', selector],
            check = False,
        )

    def join_task_processes(self):
        '''
        Blocks until every Job for this flow has completed. (Long-running
        Deployments never "complete" on their own — they're torn down by
        ``signal_flow_termination``, so joining waits only on the finite Jobs.)
        '''
        selector = f'{LABEL_FLOW_ID}={k8s_name(self._flow_id)}'
        subprocess.run(
            [self._kubectl, 'wait', '--for=condition=complete', '-n', self._namespace,
             'job', '-l', selector, '--timeout=-1s'],
            check = False,
        )

def _publish_stop(nats_url, flow_id, run_id):
    import asyncio
    import nats
    from ..messaging.topology import control_subject_for, delete_run_streams

    async def _go():
        nc = await nats.connect(nats_url)
        try:
            await nc.publish(control_subject_for(flow_id, run_id), b'stop')
            await nc.flush()
            await delete_run_streams(nc, flow_id, run_id)
        finally:
            await nc.drain()

    asyncio.run(_go())
