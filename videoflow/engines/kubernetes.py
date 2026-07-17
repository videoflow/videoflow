'''
Execution engine that runs a distributed flow on a Kubernetes cluster: it renders
one Deployment/Job (+ ConfigMap) per node and applies them with ``kubectl``. The
worker container image is the same ``videoflow.worker`` entrypoint the local engine
uses — only the launch mechanism (pods vs. subprocesses) differs.

Requires ``kubectl`` on PATH, configured against the target cluster, and the
per-component images already built and pushed (see ``docker/``).
'''
from __future__ import absolute_import, division, print_function

import logging
import subprocess
from typing import Optional

from ..compiler import specs_from_tasks_data
from ..core.engine import ExecutionEngine
from ..manifests import (
    LABEL_FLOW_ID,
    dump_manifests,
    k8s_name,
    render_manifests,
)

logger = logging.getLogger(__package__)

class KubernetesExecutionEngine(ExecutionEngine):
    '''
    - Arguments:
        - nats_url: URL workers use to reach NATS from inside the cluster, \
            e.g. ``nats://nats.videoflow.svc:4222``.
        - namespace: target namespace (must already exist).
        - default_image: image ref for nodes that don't declare their own ``image=`` \
            (e.g. ``ghcr.io/acme/app:v1``, built FROM ``videoflow-base`` with your code).
        - image_overrides: optional mapping of node name to image ref (wins over both).
        - blob_redis_url: optional Redis URL for the large-payload blob store.
        - specs: optional precompiled ``NodeSpec`` list; compiled from tasks_data if omitted.
        - kubectl: kubectl binary name/path.
    '''
    def __init__(self, nats_url : str, namespace : str = 'default', default_image : str = None,
                image_overrides : dict = None, blob_redis_url : str = None, specs = None,
                kubectl : str = 'kubectl') -> None:
        self._nats_url = nats_url
        self._namespace = namespace
        self._default_image = default_image
        self._image_overrides = image_overrides
        self._blob_redis_url = blob_redis_url
        self._specs = specs
        self._kubectl = kubectl
        self._flow_id: Optional[str] = None
        self._run_id: Optional[str] = None
        super(KubernetesExecutionEngine, self).__init__()

    def _al_create_and_start_processes(self, tasks_data, flow_id : str, flow_type : str, run_id : str) -> None:
        self._flow_id = flow_id
        self._run_id = run_id
        specs = self._specs if self._specs is not None else specs_from_tasks_data(tasks_data)
        manifests = render_manifests(
            specs, flow_id, flow_type, self._nats_url, run_id, namespace = self._namespace,
            default_image = self._default_image, image_overrides = self._image_overrides,
            blob_redis_url = self._blob_redis_url,
        )
        yaml_str = dump_manifests(manifests)
        self._kubectl_apply(yaml_str)
        logger.info(f'Applied {len(manifests)} manifests for flow {flow_id} to namespace {self._namespace}')

    def _kubectl_apply(self, yaml_str : str) -> None:
        proc = subprocess.run(
            [self._kubectl, 'apply', '-n', self._namespace, '-f', '-'],
            input = yaml_str.encode('utf-8'),
            capture_output = True,
        )
        if proc.returncode != 0:
            raise RuntimeError(f'kubectl apply failed: {proc.stderr.decode("utf-8")}')
        logger.info(proc.stdout.decode('utf-8').strip())

    def signal_flow_termination(self) -> None:
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

    def join_task_processes(self) -> None:
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

def _publish_stop(nats_url, flow_id, run_id) -> None:
    import asyncio

    import nats

    from ..messaging.topology import control_subject_for, delete_run_streams

    async def _go() -> None:
        nc = await nats.connect(nats_url)
        try:
            await nc.publish(control_subject_for(flow_id, run_id), b'stop')
            await nc.flush()
            await delete_run_streams(nc, flow_id, run_id)
        finally:
            await nc.drain()

    asyncio.run(_go())
