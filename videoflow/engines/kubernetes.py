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
import sys
import time
from typing import List, Optional

from ..compiler import specs_from_tasks_data
from ..core.engine import ExecutionEngine
from ..manifests import (
    LABEL_NODE,
    LABEL_RUN_ID,
    delete_resources,
    dump_manifests,
    k8s_name,
    render_manifests,
    split_provision_manifests,
)

logger = logging.getLogger(__package__)

# Upper bound on the best-effort broker stop/stream-delete during teardown.
_PUBLISH_STOP_TIMEOUT = 8

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
                kubectl : str = 'kubectl', envelope_version : int = None, allow_pickle : bool = False,
                provision_image : str = None, autoscaling : bool = False, max_replicas : int = 10,
                nats_monitoring_endpoint : str = None) -> None:
        self._nats_url = nats_url
        self._namespace = namespace
        self._default_image = default_image
        self._image_overrides = image_overrides
        self._blob_redis_url = blob_redis_url
        self._specs = specs
        self._kubectl = kubectl
        self._envelope_version = envelope_version
        self._allow_pickle = allow_pickle
        self._provision_image = provision_image
        self._autoscaling = autoscaling
        self._max_replicas = max_replicas
        self._nats_monitoring_endpoint = nats_monitoring_endpoint
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
            blob_redis_url = self._blob_redis_url, autoscaling = self._autoscaling,
            max_replicas = self._max_replicas, nats_monitoring_endpoint = self._nats_monitoring_endpoint,
            envelope_version = self._envelope_version, allow_pickle = self._allow_pickle,
            provision_image = self._provision_image,
        )
        # Two-phase apply: provision the broker (streams, durables, EOS anchors) and
        # wait for it to finish before starting workers, so a fast finite producer
        # can't publish end-of-stream before its consumers' interest exists.
        phase1, phase2 = split_provision_manifests(manifests, flow_id)
        self._kubectl_apply(dump_manifests(phase1))
        self._wait_provision(flow_id)
        self._kubectl_apply(dump_manifests(phase2))
        logger.info(f'Applied {len(manifests)} manifests for flow {flow_id} to namespace {self._namespace}')

    def _kubectl_apply(self, yaml_str : str) -> None:
        try:
            proc = subprocess.run(
                [self._kubectl, 'apply', '-n', self._namespace, '-f', '-'],
                input = yaml_str.encode('utf-8'),
                capture_output = True,
            )
        except FileNotFoundError as e:
            raise RuntimeError(f'{self._kubectl!r} not found on PATH — install kubectl and '
                               'point it at your cluster.') from e
        if proc.returncode != 0:
            raise RuntimeError(f'kubectl apply failed: {proc.stderr.decode("utf-8")}')
        logger.info(proc.stdout.decode('utf-8').strip())

    def _job_states(self, selector : str) -> List[tuple]:
        '''Returns ``(job_name, node_name, succeeded, failed)`` for each Job matching selector.'''
        jsonpath = ('{range .items[*]}{.metadata.name}{"|"}{.status.succeeded}{"|"}'
                    '{.status.failed}{"|"}{.metadata.labels.videoflow\\.io/node}{"\\n"}{end}')
        proc = subprocess.run(
            [self._kubectl, 'get', 'jobs', '-n', self._namespace, '-l', selector, '-o', f'jsonpath={jsonpath}'],
            capture_output = True, text = True, check = False,
        )
        states = []
        for line in proc.stdout.splitlines():
            if not line:
                continue
            name, succ, fail, node = (line.split('|') + ['', '', '', ''])[:4]
            states.append((name, node, bool(succ and int(succ) >= 1), bool(fail and int(fail) >= 1)))
        return states

    def _run_selector(self) -> str:
        return f'{LABEL_RUN_ID}={k8s_name(self._run_id)}'

    def _wait_provision(self, flow_id : str, timeout_secs : int = 180) -> None:
        '''Blocks until the provision Job completes; raises if it fails or times out.'''
        name = k8s_name('vf', flow_id, 'provision')
        deadline = time.time() + timeout_secs
        while time.time() < deadline:
            for job_name, _node, ok, failed in self._job_states(self._run_selector()):
                if job_name != name:
                    continue
                if ok:
                    return
                if failed:
                    self.dump_failed_logs(['provision'], node_label = name)
                    raise RuntimeError('provision Job failed — broker streams were not created.')
            time.sleep(2)
        raise RuntimeError(f'provision Job did not complete within {timeout_secs}s.')

    def wait_for_completion(self, poll_secs : int = 3) -> List[str]:
        '''
        Blocks until every node Job for this run has succeeded or failed. Returns the
        list of failed node names (empty when the whole flow completed cleanly). Fails
        fast: returns the instant any node Job exhausts its ``backoffLimit`` rather
        than hanging on the others.
        '''
        provision = k8s_name('vf', self._flow_id, 'provision')
        while True:
            pending, failed = [], []
            for job_name, node, ok, job_failed in self._job_states(self._run_selector()):
                if job_name == provision:
                    continue
                if job_failed:
                    failed.append(node or job_name)
                elif not ok:
                    pending.append(job_name)
            if failed:
                return failed
            if not pending:
                return []
            time.sleep(poll_secs)

    def dump_failed_logs(self, nodes : List[str], node_label : str = None) -> None:
        '''Prints the recent pod logs of each failed node (before teardown removes them).'''
        for node in nodes:
            if node_label is not None:
                sel = f'job-name={node_label}'
            else:
                sel = f'{LABEL_NODE}={k8s_name(node)},{self._run_selector()}'
            print(f'--- logs: node {node} ---', file = sys.stderr)
            subprocess.run(
                [self._kubectl, 'logs', '-n', self._namespace, '-l', sel,
                 '--tail=80', '--all-containers', '--prefix'],
                check = False,
            )

    def teardown(self) -> None:
        '''
        Tears down this run: publishes the control stop + deletes the run's broker
        streams (best-effort — the deploying host may not be able to reach the NATS
        URL the in-cluster workers use), then always deletes every Kubernetes
        resource for this run by label. The k8s cleanup is the guarantee; the broker
        step is skipped with a warning if NATS is unreachable.
        '''
        try:
            _publish_stop(self._nats_url, self._flow_id, self._run_id)
        except Exception as e:
            logger.warning(f'Broker teardown skipped (could not reach NATS at '
                           f'{self._nats_url}): {e}. Delete the run streams later with '
                           f'`videoflow teardown --flow-id {self._flow_id} --run-id '
                           f'{self._run_id} --nats <reachable-url>`.')
        delete_resources(self._kubectl, self._namespace, self._flow_id, self._run_id)

    def signal_flow_termination(self) -> None:
        '''Stops the flow and removes its resources (used by ``Flow.stop``).'''
        self.teardown()

    def join_task_processes(self) -> None:
        '''Blocks until every node Job for this run reaches a terminal state.'''
        self.wait_for_completion()

def _publish_stop(nats_url, flow_id, run_id) -> None:
    import asyncio

    import nats

    from ..messaging.topology import control_subject_for, delete_run_streams

    async def _quiet(_e) -> None:
        pass  # swallow the client's connect-retry error logging (we handle failure)

    async def _go() -> None:
        nc = await nats.connect(nats_url, allow_reconnect = False, connect_timeout = 3,
                                max_reconnect_attempts = 0, error_cb = _quiet)
        try:
            await nc.publish(control_subject_for(flow_id, run_id), b'stop')
            await nc.flush()
            await delete_run_streams(nc, flow_id, run_id)
        finally:
            await nc.drain()

    # Hard overall bound: an unresolvable in-cluster NATS name raises gaierror that
    # the client can retry in a tight loop, so cap the whole attempt. The caller
    # treats a failure/timeout as best-effort and proceeds to the k8s teardown.
    async def _bounded() -> None:
        await asyncio.wait_for(_go(), timeout = _PUBLISH_STOP_TIMEOUT)

    asyncio.run(_bounded())
