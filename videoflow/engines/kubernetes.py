'''
Execution engine that runs a distributed flow on a Kubernetes cluster: it renders
one Deployment/Job (+ ConfigMap) per node and applies them with ``kubectl``. The
worker container image is the same ``videoflow.worker`` entrypoint the local engine
uses — only the launch mechanism (pods vs. subprocesses) differs.

Requires ``kubectl`` on PATH, configured against the target cluster, and the
per-component images already built and pushed (see ``docker/``).
'''
from __future__ import absolute_import, division, print_function

import asyncio
import logging
import subprocess
import sys
import time
from dataclasses import dataclass
from typing import List, Optional

from ..core.compiler import specs_from_tasks_data
from ..core.engine import ExecutionEngine
from ..deploy.images import DEFAULT_IMAGE_PULL_POLICY
from ..deploy.manifests import (
    LABEL_NODE,
    LABEL_RUN_ID,
    STARTUP_PROBE_FAILURE_THRESHOLD,
    STARTUP_PROBE_PERIOD_SECONDS,
    Mount,
    delete_resources,
    dump_manifests,
    k8s_name,
    render_manifests,
    split_provision_manifests,
)

logger = logging.getLogger(__package__)

# Upper bound on the best-effort broker stop/stream-delete during teardown.
_PUBLISH_STOP_TIMEOUT = 8

# Container-waiting reasons that are kubelet steady states reached only after
# failure. Transient reasons (ErrImagePull, ContainerCreating, PodInitializing)
# are deliberately absent: they either resolve on their own or harden into one
# of these.
_FATAL_WAITING_REASONS = frozenset({
    'CrashLoopBackOff', 'ImagePullBackOff', 'InvalidImageName',
    'CreateContainerConfigError',
})

_STARTUP_WINDOW_SECS = STARTUP_PROBE_PERIOD_SECONDS * STARTUP_PROBE_FAILURE_THRESHOLD
# The rollout watchdog must outlive the startup-probe window: a worker whose
# open() exceeds it is killed at ~window seconds, so its first restart cannot be
# observed any sooner. The margin covers scheduling, image pull and the kubelet
# stamping CrashLoopBackOff.
_ROLLOUT_DEADLINE_SECS = _STARTUP_WINDOW_SECS + 30

@dataclass(frozen = True)
class _ContainerState:
    '''
    One pod's worker-container status, as read by ``_container_states`` (the pod
    renders exactly one container, so a single record per pod is faithful).
    ``node_label`` is the pod's ``videoflow.io/node`` label — the k8s_name-mangled
    node name, which ``dump_failed_logs`` accepts as-is (k8s_name is idempotent).
    '''
    pod : str
    node_label : str
    phase : str
    ready : bool
    restart_count : int
    waiting_reason : str
    last_terminated_reason : str

@dataclass(frozen = True)
class RolloutReport:
    '''
    Outcome of ``rollout_report``. ``failing`` are confirmed failures the deploy
    should abort on — ``(node label, human detail)`` pairs; ``warnings`` are
    ambiguous findings (slow startup, no pods yet, autoscaler scale-up in flight)
    that deserve stderr but not a non-zero exit.
    '''
    failing : list[tuple[str, str]]
    warnings : list[str]

def _is_failing(state : _ContainerState) -> bool:
    '''
    Whether a pod is confirmed broken: a fatal waiting reason, or repeated
    restarts while still not Ready — the latter catches the window between
    restarts where the container is briefly Running before the kubelet stamps
    CrashLoopBackOff. One restart is tolerated (a rocky startup that recovered).
    '''
    if state.waiting_reason in _FATAL_WAITING_REASONS:
        return True
    return state.restart_count >= 2 and not state.ready

def _failure_detail(state : _ContainerState) -> str:
    '''Human problem statement for a confirmed-failing pod, naming the likely fix.'''
    if state.waiting_reason in ('ImagePullBackOff', 'InvalidImageName'):
        return (f'pod {state.pod} cannot pull its image ({state.waiting_reason}) — check '
                'the image name/tag, that the cluster can reach the registry, and '
                '--image-pull-policy (a locally loaded image needs IfNotPresent).')
    if state.waiting_reason == 'CreateContainerConfigError':
        return (f'pod {state.pod} cannot start ({state.waiting_reason}) — a ConfigMap '
                'or Secret it references is missing or invalid; check the pod events.')
    if state.last_terminated_reason == 'OOMKilled':
        return (f'pod {state.pod} was OOM-killed ({state.restart_count} restarts) — the '
                'container exceeded its memory limit; raise the limit, or if a model '
                'load is the culprit, use a smaller model or grant more memory/VRAM.')
    return (f'pod {state.pod} is crash-looping '
            f'({state.waiting_reason or "repeated restarts"}, '
            f'{state.restart_count} restarts) — see the logs above; if the model load '
            f'is just slow, the {_STARTUP_WINDOW_SECS}s startup-probe window may be '
            f'killing open() before it finishes.')

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
        - blob_ttl_seconds: TTL override for offloaded payloads (PROTOCOL.md \
            BLOB-7); ``None`` lets workers pick the flow-type default.
        - specs: optional precompiled ``NodeSpec`` list; compiled from tasks_data if omitted.
        - kubectl: kubectl binary name/path.
        - mounts: optional ``Mount`` records (see ``manifests.parse_mounts``) — hostPath \
            volumes added to every node workload.
        - gpu_runtime_class: ``runtimeClassName`` for GPU pods (``nvidia`` on k3s and \
            other distros where the NVIDIA runtime is opt-in rather than the node \
            default). Without it a GPU pod schedules but sees no device.
        - gpu_mode: GPU strategy name (``'exclusive'``, the default: whole-device \
            claims via the extended resource; see ``deploy.gpu``).
        - gpu_resource_name: deploy-level extended-resource name for GPU claims \
            (clusters advertising whole devices under a non-default name).
        - gpu_autoscaling: include GPU nodes in KEDA autoscaling (off by default — \
            each extra replica claims whole GPUs).
        - image_pull_policy: ``imagePullPolicy`` for every rendered container. \
            Defaults to ``IfNotPresent``, which is what lets a locally built image \
            loaded into the cluster actually run (see ``manifests.render_manifests``).
    '''
    def __init__(self, nats_url : str, namespace : str = 'default', default_image : str | None = None,
                image_overrides : dict | None = None, blob_redis_url : str | None = None,
                blob_ttl_seconds : int | None = None,
                specs : list | None = None,
                kubectl : str = 'kubectl', envelope_version : int | None = None,
                provision_image : str | None = None, autoscaling : bool = False, max_replicas : int = 10,
                nats_monitoring_endpoint : str | None = None, mounts : list[Mount] | None = None,
                gpu_runtime_class : str | None = None, gpu_mode : str = 'exclusive',
                gpu_resource_name : str | None = None, gpu_autoscaling : bool = False,
                image_pull_policy : str = DEFAULT_IMAGE_PULL_POLICY) -> None:
        self._nats_url = nats_url
        self._namespace = namespace
        self._default_image = default_image
        self._image_overrides = image_overrides
        self._blob_redis_url = blob_redis_url
        self._blob_ttl_seconds = blob_ttl_seconds
        self._specs = specs
        self._kubectl = kubectl
        self._envelope_version = envelope_version
        self._provision_image = provision_image
        self._autoscaling = autoscaling
        self._max_replicas = max_replicas
        self._nats_monitoring_endpoint = nats_monitoring_endpoint
        self._mounts = mounts
        self._gpu_runtime_class = gpu_runtime_class
        self._gpu_mode = gpu_mode
        self._gpu_resource_name = gpu_resource_name
        self._gpu_autoscaling = gpu_autoscaling
        self._image_pull_policy = image_pull_policy
        self._flow_id: Optional[str] = None
        self._run_id: Optional[str] = None
        super(KubernetesExecutionEngine, self).__init__()

    def _al_create_and_start_processes(self, tasks_data : Optional[list], flow_id : str,
                                       flow_type : str, run_id : str) -> None:
        self._flow_id = flow_id
        self._run_id = run_id
        # Exactly one source of specs: pre-compiled (the deploy CLI path, which has no
        # in-process graph) or derived from the live tasks_data. Neither is a caller bug
        # worth a traceback, so name the fix.
        if self._specs is not None:
            specs = self._specs
        elif tasks_data is None:
            raise ValueError('no specs to deploy: construct the engine with specs = ... or '
                            'pass tasks_data from Flow.build_tasks_data()')
        else:
            specs = specs_from_tasks_data(tasks_data)
        manifests = render_manifests(
            specs, flow_id, flow_type, self._nats_url, run_id, namespace = self._namespace,
            default_image = self._default_image, image_overrides = self._image_overrides,
            blob_redis_url = self._blob_redis_url, blob_ttl_seconds = self._blob_ttl_seconds,
            autoscaling = self._autoscaling,
            max_replicas = self._max_replicas, nats_monitoring_endpoint = self._nats_monitoring_endpoint,
            envelope_version = self._envelope_version,
            provision_image = self._provision_image, mounts = self._mounts,
            gpu_runtime_class = self._gpu_runtime_class, gpu_mode = self._gpu_mode,
            gpu_resource_name = self._gpu_resource_name,
            gpu_autoscaling = self._gpu_autoscaling,
            image_pull_policy = self._image_pull_policy,
        )
        # Two-phase apply: provision the broker (streams, durables, EOS anchors) and
        # wait for it to finish before starting workers, so a fast finite producer
        # can't publish end-of-stream before its consumers' interest exists.
        phases = split_provision_manifests(manifests, flow_id)
        self._kubectl_apply(dump_manifests(phases.provision))
        self._wait_provision(flow_id)
        self._kubectl_apply(dump_manifests(phases.worker))
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

    def _pod_states(self, selector : str) -> List[tuple]:
        '''
        ``(pod_name, phase, scheduled_reason, scheduled_message)`` for each pod
        matching selector. ``scheduled_reason`` is the PodScheduled condition's
        reason — ``'Unschedulable'`` is the scheduler saying the pod fits nowhere
        (its message names the cause, e.g. ``Insufficient nvidia.com/gpu``).
        '''
        jsonpath = ('{range .items[*]}{.metadata.name}{"|"}{.status.phase}{"|"}'
                    '{.status.conditions[?(@.type=="PodScheduled")].reason}{"|"}'
                    '{.status.conditions[?(@.type=="PodScheduled")].message}{"\\n"}{end}')
        proc = subprocess.run(
            [self._kubectl, 'get', 'pods', '-n', self._namespace, '-l', selector,
             '-o', f'jsonpath={jsonpath}'],
            capture_output = True, text = True, check = False,
        )
        states = []
        for line in proc.stdout.splitlines():
            if not line:
                continue
            name, phase, reason, message = (line.split('|', 3) + ['', '', '', ''])[:4]
            states.append((name, phase, reason, message))
        return states

    def _unschedulable_pods(self, selector : str) -> List[tuple]:
        '''``(pod_name, message)`` for pods the scheduler has declared Unschedulable.'''
        return [(name, message) for name, phase, reason, message in self._pod_states(selector)
                if phase == 'Pending' and reason == 'Unschedulable']

    def _container_states(self, selector : str) -> list[_ContainerState]:
        '''
        Worker-container status for each pod matching selector — what
        ``_pod_states`` cannot see: readiness, restart count and the waiting /
        last-terminated reasons that reveal a crash-loop. Kept a sibling rather
        than an extension because ``_pod_states``'s trailing field is free text
        (parsed with maxsplit); every field here is enum-ish or numeric, so a
        plain split is safe. Index ``[0]`` is correct: the pod spec renders
        exactly one container and no init containers. A not-yet-scheduled pod has
        no containerStatuses — its fields come back empty and parse to defaults.
        '''
        jsonpath = ('{range .items[*]}{.metadata.name}{"|"}'
                    '{.metadata.labels.videoflow\\.io/node}{"|"}{.status.phase}{"|"}'
                    '{.status.containerStatuses[0].ready}{"|"}'
                    '{.status.containerStatuses[0].restartCount}{"|"}'
                    '{.status.containerStatuses[0].state.waiting.reason}{"|"}'
                    '{.status.containerStatuses[0].lastState.terminated.reason}{"\\n"}{end}')
        proc = subprocess.run(
            [self._kubectl, 'get', 'pods', '-n', self._namespace, '-l', selector,
             '-o', f'jsonpath={jsonpath}'],
            capture_output = True, text = True, check = False,
        )
        states : list[_ContainerState] = []
        for line in proc.stdout.splitlines():
            if not line:
                continue
            pod, node, phase, ready, restarts, waiting, last_term = (line.split('|') + [''] * 7)[:7]
            states.append(_ContainerState(
                pod = pod, node_label = node, phase = phase,
                ready = ready == 'true',
                restart_count = int(restarts or 0),
                waiting_reason = waiting,
                last_terminated_reason = last_term,
            ))
        return states

    def _scaleup_in_flight(self, pod_names : List[str]) -> bool:
        '''
        Whether a cluster-autoscaler scale-up is in progress *for these pods* (they
        are then expected to schedule once the new node joins, so the unschedulable
        watchdog must not abort). Scoped per pod via ``involvedObject.name`` — an
        unscoped namespace query would let a stale TriggeredScaleUp event from any
        prior workload (Events persist ~1h) mute the watchdog indefinitely.
        '''
        for name in pod_names[:8]:      # bounded: one kubectl call per stuck pod
            proc = subprocess.run(
                [self._kubectl, 'get', 'events', '-n', self._namespace,
                 '--field-selector', f'reason=TriggeredScaleUp,involvedObject.name={name}',
                 '-o', 'name'],
                capture_output = True, text = True, check = False,
            )
            if proc.stdout.strip():
                return True
        return False

    def _wait_provision(self, flow_id : str, timeout_secs : int = 180) -> None:
        '''Blocks until the provision Job completes; raises if it fails, cannot be
        scheduled, or times out.'''
        name = k8s_name('vf', flow_id, 'provision')
        deadline = time.time() + timeout_secs
        unschedulable_since = None
        while time.time() < deadline:
            for job_name, _node, ok, failed in self._job_states(self._run_selector()):
                if job_name != name:
                    continue
                if ok:
                    return
                if failed:
                    self.dump_failed_logs(['provision'], node_label = name)
                    raise RuntimeError('provision Job failed — broker streams were not created.')
            # An unschedulable provision pod would otherwise burn the whole timeout.
            stuck = self._unschedulable_pods(f'job-name={name}')
            if stuck:
                unschedulable_since = unschedulable_since or time.time()
                if (time.time() - unschedulable_since >= 30
                        and not self._scaleup_in_flight([n for n, _ in stuck])):
                    raise RuntimeError(f'provision pod cannot be scheduled '
                                       f'({stuck[0][1] or "Unschedulable"}) — broker streams '
                                       f'were not created.')
            else:
                unschedulable_since = None
            time.sleep(2)
        raise RuntimeError(f'provision Job did not complete within {timeout_secs}s.')

    def wait_for_completion(self, poll_secs : int = 3,
                            unschedulable_grace_secs : int = 60) -> List[str]:
        '''
        Blocks until every node Job for this run has succeeded or failed. Returns the
        list of failed node names (empty when the whole flow completed cleanly). Fails
        fast two ways: returns the instant any node Job exhausts its ``backoffLimit``,
        and raises ``RuntimeError`` when any pod has sat scheduler-Unschedulable
        (e.g. ``Insufficient nvidia.com/gpu``) for ``unschedulable_grace_secs`` —
        an unschedulable pod never runs, never consumes its backoffLimit, and would
        otherwise leave this loop (and the whole flow, via backpressure) hanging
        forever. The grace period tolerates scheduling churn, and the abort is
        skipped while a cluster-autoscaler scale-up is in flight.
        '''
        provision = k8s_name('vf', self._flow_id, 'provision')
        unschedulable_since : dict = {}
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
            now = time.time()
            stuck = self._unschedulable_pods(self._run_selector())
            names = {name for name, _ in stuck}
            unschedulable_since = {n: t for n, t in unschedulable_since.items() if n in names}
            for name, _message in stuck:
                unschedulable_since.setdefault(name, now)
            overdue = [(name, message) for name, message in stuck
                       if now - unschedulable_since[name] >= unschedulable_grace_secs]
            if overdue and not self._scaleup_in_flight([name for name, _ in overdue]):
                detail = '; '.join(f'{name}: {message or "Unschedulable"}' for name, message in overdue)
                error = (f'{len(overdue)} pod(s) cannot be scheduled and the flow would stall '
                         f'({detail}).')
                # GPU-specific remedies only when the scheduler actually named a GPU
                # resource — for a CPU/memory/affinity stall they would mislead.
                if any('gpu' in (message or '') for _, message in overdue):
                    error += (' The flow demands more GPUs than the cluster has allocatable — '
                              'reduce GPU nodes/replicas, or enable device-plugin time-slicing '
                              '(dev clusters).')
                raise RuntimeError(error)
            time.sleep(poll_secs)

    def rollout_report(self, deadline_secs : int = _ROLLOUT_DEADLINE_SECS, poll_secs : int = 3,
                       unschedulable_grace_secs : int = 30) -> RolloutReport:
        '''
        Bounded post-apply health check for a REALTIME run — the counterpart of
        the ``wait_for_completion`` watchdog (a REALTIME deploy otherwise returns
        immediately, and a broken node fails silently: producers keep publishing,
        frames are evicted, downstream output never appears). Success means every
        pod is Ready (the readiness probe passes only after ``node.open()``
        completes) or Succeeded (a finite-producer Job); merely being *scheduled*
        is not enough — a pod that finds a node and then dies in ``open()`` (CUDA
        OOM, a bad image, the startup-probe window killing a slow model load)
        crash-loops with no Unschedulable condition to see.

        Exits early both ways: as soon as every pod is Ready on two consecutive
        polls (seconds, in the healthy case), and as soon as any pod is confirmed
        failing — a fatal waiting reason or repeated restarts on two consecutive
        polls, or scheduler-Unschedulable past ``unschedulable_grace_secs`` with
        no autoscaler scale-up in flight. The default deadline exceeds the
        startup-probe window so a probe kill of a slow ``open()`` is observable
        at all.

        - Arguments:
            - deadline_secs: give up after this long without a verdict; pods \
                still not Ready are then reported as warnings, not failures \
                (a slow model load is slow, not proven dead).
            - poll_secs: seconds between kubectl polls.
            - unschedulable_grace_secs: how long a pod may sit Unschedulable \
                before it is a confirmed failure (tolerates scheduling churn).
        - Returns:
            - A ``RolloutReport`` — ``failing`` non-empty means the deploy \
                should dump logs and exit non-zero.
        '''
        deadline = time.time() + deadline_secs
        clean_polls = 0
        saw_pods = False
        failing_streak : dict[str, int] = {}
        unschedulable_since : dict[str, float] = {}
        states : list[_ContainerState] = []
        scaleup_muted = False
        while time.time() < deadline:
            now = time.time()
            states = self._container_states(self._run_selector())
            saw_pods = saw_pods or bool(states)
            node_of = {s.pod: s.node_label or s.pod for s in states}

            # Confirmed container failures, debounced over two consecutive polls
            # (symmetric with the two-clean-polls success guard below) so a
            # momentary blip or a just-recovered pod doesn't abort the deploy.
            failing_now = {s.pod: s for s in states if _is_failing(s)}
            failing_streak = {pod: failing_streak.get(pod, 0) + 1 for pod in failing_now}
            confirmed = [failing_now[pod] for pod, streak in sorted(failing_streak.items())
                         if streak >= 2]
            if confirmed:
                return RolloutReport(
                    failing = [(node_of[s.pod], _failure_detail(s)) for s in confirmed],
                    warnings = [],
                )

            # Scheduler-Unschedulable past grace is equally fatal — such a pod
            # never starts, so it can never crash-loop. Same per-pod grace
            # bookkeeping as wait_for_completion; the abort is skipped while a
            # cluster-autoscaler scale-up is in flight for these pods.
            stuck = self._unschedulable_pods(self._run_selector())
            names = {name for name, _ in stuck}
            unschedulable_since = {n: t for n, t in unschedulable_since.items() if n in names}
            for name, _message in stuck:
                unschedulable_since.setdefault(name, now)
            overdue = [(name, message) for name, message in stuck
                       if now - unschedulable_since[name] >= unschedulable_grace_secs]
            scaleup_muted = bool(overdue) and self._scaleup_in_flight([n for n, _ in overdue])
            if overdue and not scaleup_muted:
                return RolloutReport(
                    failing = [(node_of.get(name, name),
                                f'pod {name} cannot be scheduled: {message or "Unschedulable"}')
                               for name, message in overdue],
                    warnings = [],
                )

            # Success needs pods to exist AND two consecutive all-ready polls:
            # right after apply the controllers may not have created the pods
            # yet, and a single empty snapshot would pass the check vacuously.
            if states and all(s.ready or s.phase == 'Succeeded' for s in states):
                clean_polls += 1
                if clean_polls >= 2:
                    return RolloutReport(failing = [], warnings = [])
            else:
                clean_polls = 0
            time.sleep(poll_secs)
        if not saw_pods:
            return RolloutReport(failing = [], warnings = [
                f'no pods appeared for this run within {deadline_secs}s — check the '
                f'workloads with kubectl get pods -n {self._namespace}'])
        warnings : list[str] = []
        if scaleup_muted:
            warnings.append('pod(s) are Pending, but a cluster-autoscaler scale-up is '
                            'in flight — they should schedule once the new node joins.')
        for s in states:
            if s.ready or s.phase == 'Succeeded':
                continue
            warnings.append(f'pod {s.pod} is not Ready after {deadline_secs}s '
                            f'({s.waiting_reason or s.phase}, {s.restart_count} restarts) '
                            f'— a slow model load? watch it with '
                            f'kubectl get pods -n {self._namespace}')
        return RolloutReport(failing = [], warnings = warnings)

    def dump_failed_logs(self, nodes : List[str], node_label : str | None = None) -> None:
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
        flow_id, run_id = self._flow_id, self._run_id
        if flow_id is None or run_id is None:      # nothing was ever applied
            return
        try:
            _publish_stop(self._nats_url, flow_id, run_id)
        except Exception as e:
            logger.warning(f'Broker teardown skipped (could not reach NATS at '
                           f'{self._nats_url}): {e}. Delete the run streams later with '
                           f'`videoflow teardown --flow-id {self._flow_id} --run-id '
                           f'{self._run_id} --nats <reachable-url>`.')
        delete_resources(self._kubectl, self._namespace, flow_id, run_id)

    def signal_flow_termination(self) -> None:
        '''Stops the flow and removes its resources (used by ``Flow.stop``).'''
        self.teardown()

    def join_task_processes(self) -> None:
        '''
        Blocks until every node Job for this run reaches a terminal state. Keeps
        ``Flow.join()``'s documented block-then-return contract: a watchdog abort
        (unschedulable pods — the flow can never finish) is logged, not raised, so
        programmatic callers are not crashed mid-join; the CLI's deploy path calls
        ``wait_for_completion`` directly and does get the exception.
        '''
        try:
            self.wait_for_completion()
        except RuntimeError as e:
            logger.error(f'flow will never complete: {e}')

def _publish_stop(nats_url : str, flow_id : str, run_id : str) -> None:
    # Deferred (both): the optional `nats` client, which topology also imports at module
    # scope. Rendering/applying manifests must work without the broker client installed.
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
