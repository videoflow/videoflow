'''
Execution engine that runs a distributed flow entirely on the local machine, one
OS subprocess per node (per replica, for nb_tasks > 1), all talking to a local
NATS server. Same ``videoflow.worker`` code path Kubernetes uses — only the way
processes are started differs — so it's the primary way to develop and test a
flow without a cluster.

Prerequisite: a running NATS JetStream server, e.g. ``nats-server -js`` or
``docker run -p 4222:4222 nats -js``.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import json
import logging
import os
import signal
import site
import subprocess
import sys
import sysconfig
import tempfile
import time
from typing import List, Optional

import nats  # noqa: F401  (import guard: fail fast if the broker client is missing)

from ..core.compiler import (
    NodeSpec,
    specs_from_tasks_data,
    validate_wire_compatibility,
)
from ..core.engine import ExecutionEngine
from ..core.errors import BrokerUnavailable, ConfigError
from ..core.supervision import (
    EventLog,
    NodeExited,
    NodeGaveUp,
    NodeRestarted,
    NodeStarted,
    SupervisionPolicy,
    render_event,
)

# The module, not the symbols: tests monkeypatch ``topology.provision_flow_sync``,
# which only works while the name resolves at call time (CLAUDE.md's plugin-registry
# rule — the same trap, for the same reason).
from ..messaging import topology
from ..utils.system import visible_physical_gpus

logger = logging.getLogger(__package__)

DEFAULT_NATS_URL = 'nats://localhost:4222'

# Bound on the up-front stream provisioning. Locally an unreachable broker is a
# setup mistake worth reporting, not a transient worth blocking on indefinitely.
PROVISION_TIMEOUT_SECONDS = 15

async def _quiet_error_cb(_e : BaseException) -> None:
    '''Swallows the NATS client's per-retry error logging; we report the failure ourselves.'''
    pass

def inherited_python_path() -> list:
    '''
    The ``sys.path`` entries this process added beyond the interpreter's own
    defaults — typically the graph/solution directory (inserted by
    ``videoflow.deploy.compile.load_flow``), an editable checkout, or a test support dir.

    Worker subprocesses inherit the environment but *not* ``sys.path``, so without
    re-exporting these as ``PYTHONPATH`` every node class living next to the graph
    fails to import in its worker.
    '''
    builtin = set()
    for path in sysconfig.get_paths().values():
        if path:
            builtin.add(os.path.abspath(path))
    for getter in ('getsitepackages', 'getusersitepackages'):
        try:
            value = getattr(site, getter)()
        except Exception:                       # pragma: no cover - venv built without site
            continue
        for path in ([value] if isinstance(value, str) else value):
            builtin.add(os.path.abspath(path))

    entries, seen = [], set()
    for entry in sys.path:
        if not entry:                           # '' means cwd; already implicit for the child
            continue
        resolved = os.path.abspath(entry)
        if resolved in builtin or resolved in seen or not os.path.isdir(resolved):
            continue
        seen.add(resolved)
        entries.append(resolved)
    return entries

def needs_container_image(spec : NodeSpec) -> bool:
    '''
    Whether running ``spec`` locally requires a container image to exist.

    Only a native component with no ``runtime.localCommand`` does: a Python node runs
    as a host subprocess in the current interpreter, and a native component with a
    ``localCommand`` runs that binary directly. This is the predicate ``run-local``
    uses to decide whether to auto-build at all — most flows are pure Python, and
    building a (possibly CUDA) solution image to launch a few subprocesses would be a
    large and pointless cost.
    '''
    if spec.node_class:
        return False
    runtime = (spec.descriptor or {}).get('spec', {}).get('runtime', {})
    return not runtime.get('localCommand')

class LocalProcessEngine(ExecutionEngine):
    '''
    - Arguments:
        - nats_url: URL of the NATS server every worker connects to.
        - blob_redis_url: optional Redis URL for the large-payload blob store.
        - specs: optional precompiled list of ``NodeSpec``. If not given, they are \
            compiled from the flow's ``tasks_data`` at ``allocate_and_run_tasks`` time.
        - python_path: extra directories prepended to each worker's ``PYTHONPATH``.
        - inherit_python_path: also re-export this process's own ``sys.path`` \
            additions (default True) — what makes node classes defined next to the \
            graph importable in the workers. Set False for a hermetic child env.
        - default_image: image used for a native component that declares no \
            ``image=`` — the solution image ``run-local`` auto-builds. A node's own \
            ``image=`` still wins.
        - blob_ttl_seconds: TTL override for offloaded payloads (PROTOCOL.md \
            BLOB-7); ``None`` lets workers pick the flow-type default \
            (3600s realtime / 86400s batch).
        - supervision: how a dead worker is restarted. Defaults to \
            ``SupervisionPolicy.local()`` — the same restart count Kubernetes \
            uses, with a compressed backoff. This is the point of the parameter: \
            a crash used to recover in the cluster and hang here, which made \
            local development the one place the recovery path was never \
            exercised. Pass ``SupervisionPolicy.disabled()`` (``--no-restart``) \
            for a tight debug loop.
    '''
    def __init__(self, nats_url : str = DEFAULT_NATS_URL, blob_redis_url : str | None = None,
                specs : List[NodeSpec] | None = None,
                local_docker_nats_url : str | None = None,
                python_path : list | None = None, inherit_python_path : bool = True,
                default_image : str | None = None,
                blob_ttl_seconds : int | None = None,
                supervision : SupervisionPolicy | None = None) -> None:
        self._supervision = supervision or SupervisionPolicy.local()
        self._events = EventLog()
        self._nats_url = nats_url
        self._blob_redis_url = blob_redis_url
        # Blob TTL override (BLOB-7); None ⇒ workers use the flow-type default.
        self._blob_ttl_seconds = blob_ttl_seconds
        self._specs = specs
        # Fallback image for a native component that declares none — the solution image
        # run-local auto-builds. A node's own image= still wins.
        self._default_image = default_image
        # NATS URL a docker-run remote component connects to (containers can't reach a
        # host 'localhost'); on macOS/Windows this is typically host.docker.internal.
        self._local_docker_nats_url = local_docker_nats_url
        extra = list(python_path or [])
        if inherit_python_path:
            extra += [p for p in inherited_python_path() if p not in extra]
        self._python_path = extra
        self._procs: list = []
        self._failures: list = []
        # (node, replica) -> (spec, env), so a restart relaunches the identical worker.
        self._launchers: dict = {}
        # (node, replica) -> restarts already spent.
        self._attempts: dict = {}
        # (node, replica) -> file the worker writes its structured death reason to.
        # The local stand-in for Kubernetes' termination-message path, and the only
        # way the supervisor learns *why* a worker died rather than just that it did.
        self._termination_logs: dict = {}
        self._termination_dir: Optional[str] = None
        self._flow_id: Optional[str] = None
        self._run_id: Optional[str] = None
        super(LocalProcessEngine, self).__init__()

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
            raise ConfigError('No specs to run.',
                            remedy = 'Construct the engine with specs = ..., or pass '
                                    'tasks_data from Flow.build_tasks_data().')
        else:
            specs = specs_from_tasks_data(tasks_data)

        # The whole-run wire version: the single language-neutral protobuf envelope (v4).
        # Deferred: serialization imports the optional `protobuf` deps at module scope.
        from ..wire.serialization import DEFAULT_ENVELOPE_VERSION
        envelope_version = DEFAULT_ENVELOPE_VERSION
        validate_wire_compatibility(specs, envelope_version)

        # Provision streams + durable consumers up front. Required for BATCH: under
        # interest retention, a message published before its consumer exists is lost.
        # Fail fast rather than retrying forever: locally, an unreachable broker is a
        # setup mistake to report, not a transient the run should wait out.
        try:
            topology.provision_flow_sync(self._nats_url, specs, flow_id, run_id, flow_type,
                                connect_options = {'allow_reconnect': False,
                                                   'connect_timeout': 5,
                                                   'max_reconnect_attempts': 0,
                                                   'error_cb': _quiet_error_cb},
                                timeout = PROVISION_TIMEOUT_SECONDS)
        except Exception as e:
            raise BrokerUnavailable(
                f'Could not reach NATS at {self._nats_url} ({type(e).__name__}: {e}).',
                remedy = ('Start one with `videoflow run-local` (it provisions a dev '
                        'broker), `docker compose up -d`, or `nats-server -js` — or '
                        'point --nats at a running server.'),
                nats_url = self._nats_url) from e

        # Only probe the host's GPUs (nvidia-smi) when the flow actually has GPU
        # nodes — a CPU-only flow must not depend on the probe in any way.
        gpu_assignment = (assign_local_gpus(specs, visible_physical_gpus())
                        if any(s.device_type == 'gpu' for s in specs) else {})
        for spec in specs:
            for replica_idx in range(spec.nb_tasks):
                env = _worker_env(spec, self._nats_url, flow_id, flow_type, run_id,
                                self._blob_redis_url, replica_idx, envelope_version,
                                self._python_path, blob_ttl_seconds = self._blob_ttl_seconds,
                                gpu_devices = gpu_assignment.get((spec.name, replica_idx)))
                env['VF_TERMINATION_LOG'] = self._termination_log_path(spec.name, replica_idx)
                # Kept so a restart relaunches the identical worker, and so the
                # supervisor never has to re-derive an environment.
                self._launchers[(spec.name, replica_idx)] = (spec, env)
                self._start_worker(spec, replica_idx, attempt = 0)

    def _termination_log_path(self, node : str, replica_idx : int) -> str:
        if self._termination_dir is None:
            self._termination_dir = tempfile.mkdtemp(prefix = 'videoflow-term-')
        path = os.path.join(self._termination_dir, f'{node}-{replica_idx}.json')
        self._termination_logs[(node, replica_idx)] = path
        return path

    def _start_worker(self, spec : NodeSpec, replica_idx : int, attempt : int) -> None:
        _spec, env = self._launchers[(spec.name, replica_idx)]
        cmd, run_env = self._launch_command(spec, env)
        proc = subprocess.Popen(cmd, env = run_env)
        self._procs.append((spec.name, replica_idx, proc))
        self._attempts[(spec.name, replica_idx)] = attempt
        self._events.emit(NodeStarted(spec.name, replica_idx, attempt))
        logger.info(
            f'Started worker pid={proc.pid} node={spec.name} replica={replica_idx} '
            f'({"remote" if spec.is_remote else "python"})'
            + (f' [restart {attempt}]' if attempt else '')
        )

    def _launch_command(self, spec : NodeSpec, env : dict) -> tuple:
        '''
        The command + environment to start one worker for ``spec``:

        - native Python node: ``python -m videoflow.worker`` in the current env;
        - remote component with a ``localCommand``: run that binary directly (env carries VF_*);
        - remote component otherwise: ``docker run`` its image with VF_* passed via -e.
        '''
        # A Python node/component (node_class set) runs the Python worker, whether it
        # came from a graph class or a descriptor's pythonClass. Only a native
        # component (no node_class) uses localCommand / docker.
        if spec.node_class:
            return [sys.executable, '-m', 'videoflow.worker'], env
        runtime = (spec.descriptor or {}).get('spec', {}).get('runtime', {})
        local_command = runtime.get('localCommand')
        if local_command:
            return list(local_command), env
        # docker run: pass only the VF_* vars, and rewrite a localhost NATS URL to one
        # the container can reach (host.docker.internal on macOS/Windows).
        vf_env = {k: v for k, v in env.items() if k.startswith('VF_') or k == 'VIDEOFLOW_BLOB_REDIS_URL'}
        docker_nats = self._local_docker_nats_url or self._nats_url.replace('localhost', 'host.docker.internal')
        vf_env['VF_NATS_URL'] = docker_nats
        docker = ['docker', 'run', '--rm', '--network', 'host']
        for k, v in vf_env.items():
            docker += ['-e', f'{k}={v}']
        image = spec.image or self._default_image
        if not image:
            raise ConfigError(
                f'Remote component node {spec.name!r} has no image to run locally.',
                remedy = 'Give it an `image=`, or a `runtime.localCommand` in its '
                        'component descriptor.',
                node = spec.name)
        docker.append(image)
        if spec.command:
            docker += list(spec.command)
        return docker, dict(os.environ)

    def signal_flow_termination(self) -> None:
        flow_id, run_id = self._flow_id, self._run_id
        if flow_id is None or run_id is None:      # nothing was ever started
            return
        _publish_stop(self._nats_url, flow_id, run_id)

    def wait_for_completion(self) -> List[str]:
        '''
        Blocks until every worker process exits, **restarting failed ones** per the
        supervision policy. Returns the names of nodes that ran out of restarts
        (empty when the flow ran cleanly) — the same contract as
        ``KubernetesExecutionEngine.wait_for_completion``, and now the same
        behaviour: a crash that the cluster would recover from recovers here too,
        because the restarted worker rebinds the same durable and its un-acked
        messages are redelivered.

        A worker killed by SIGINT/SIGTERM is not counted and not restarted: that
        is Ctrl-C or ``flow.stop()`` propagating, not a failure.
        '''
        stopped = {-signal.SIGINT, -signal.SIGTERM}
        self._failures = []
        pending = list(self._procs)
        self._procs = []
        while pending:
            name, replica_idx, proc = pending.pop(0)
            while True:
                try:
                    proc.wait()
                    break
                except KeyboardInterrupt:
                    # The children got the same SIGINT; keep reaping rather than
                    # abandoning them (a second Ctrl-C used to escape here).
                    continue
            code = proc.returncode or 0
            if code == 0 or code in stopped:
                continue
            reason = self._termination_reason(name, replica_idx)
            self._events.emit(NodeExited(name, replica_idx, code, reason))
            restarted = self._maybe_restart(name, replica_idx, reason)
            if restarted is not None:
                pending.append(restarted)
                continue
            self._failures.append((name, replica_idx, code))
        failed, seen = [], set()
        for name, _replica, _code in self._failures:
            if name not in seen:
                seen.add(name)
                failed.append(name)
        return failed

    def _termination_reason(self, name : str, replica_idx : int) -> dict | None:
        '''
        The structured cause a worker wrote before dying, when it managed to.

        Local workers inherit stdout/stderr, so the traceback is already on the
        terminal; this is for the *supervisor*, which needs the disposition to
        decide whether restarting is worth anything.
        '''
        path = self._termination_logs.get((name, replica_idx))
        if not path or not os.path.isfile(path):
            return None
        try:
            with open(path) as f:
                return json.loads(f.read() or '{}')
        except Exception:
            return None

    def _maybe_restart(self, name : str, replica_idx : int,
                    reason : dict | None) -> tuple | None:
        '''
        Restarts a dead worker if the policy says it is worth it, returning the
        new ``(name, replica, proc)`` to keep waiting on, else ``None``.
        '''
        attempt = self._attempts.get((name, replica_idx), 0)
        disposition = reason.get('disposition') if reason else None
        if not self._supervision.should_restart(attempt, disposition):
            self._events.emit(NodeGaveUp(name, replica_idx, attempt + 1, reason))
            # Immediately, not once every worker has been reaped: this node will
            # never publish its end-of-stream, so its children are already waiting
            # for one that is not coming — and this loop is waiting for *them*.
            # Deferring the signal to the end of the loop deadlocks against
            # exactly the situation it is meant to resolve.
            self._abort_flow(name)
            return None
        delay = self._supervision.delay_for(attempt)
        self._events.emit(NodeRestarted(name, replica_idx, attempt, delay))
        logger.warning(f'node {name} replica {replica_idx} failed; restarting in {delay:g}s '
                    f'(attempt {attempt + 1}/{self._supervision.max_restarts})')
        time.sleep(delay)
        spec, _env = self._launchers[(name, replica_idx)]
        before = len(self._procs)
        self._start_worker(spec, replica_idx, attempt = attempt + 1)
        return self._procs[before]

    def _abort_flow(self, node : str) -> None:
        '''
        Publishes the flow-wide stop so surviving workers end instead of waiting
        for an end-of-stream that a dead node will never send.

        This is the supervisor layer of abort propagation (``ABORT-6``): it covers
        the worker that died too abruptly to publish an ABORT marker of its own —
        an ``os._exit``, an OOM kill, a SIGKILL.
        '''
        logger.error(f'node {node} gave up; stopping the flow so its children do not '
                    f'wait for an end-of-stream that is not coming')
        try:
            self.signal_flow_termination()
        except Exception:
            logger.debug('could not signal termination after a node gave up', exc_info = True)

    def events(self) -> EventLog:
        '''Lifecycle events for this run — the same records the Kubernetes engine emits.'''
        return self._events

    def failures(self) -> List[tuple]:
        '''``(node_name, replica_idx, returncode)`` for each worker that gave up.'''
        return list(self._failures)

    def report_failures(self) -> None:
        '''
        Prints one line per lifecycle event worth reading — restarts included, so a
        run that recovered says so. Local workers inherit stdout/stderr, so their
        tracebacks are already on the terminal; this is the index, not a dump.
        '''
        for event in self._events.events:
            line = render_event(event)
            if line:
                print(f'--- {line}', file = sys.stderr)

    def join_task_processes(self) -> None:
        try:
            self.wait_for_completion()
        finally:
            self._teardown_streams()

    def _teardown_streams(self) -> None:
        flow_id, run_id = self._flow_id, self._run_id
        if flow_id is None or run_id is None:
            return

        async def _go() -> None:
            nc = await nats.connect(self._nats_url)
            try:
                await topology.delete_run_streams(nc, flow_id, run_id)
            finally:
                await nc.drain()

        try:
            asyncio.run(_go())
        except Exception:
            logger.debug('stream teardown failed', exc_info = True)

def _runs_via_docker(spec : NodeSpec) -> bool:
    '''
    Whether ``_launch_command`` will run this spec with ``docker run``: a native
    component with no ``localCommand``. Such a worker cannot receive a GPU grant
    locally — the env filter passes only VF_* variables and no ``--gpus`` flag is
    injected (a documented non-goal) — so the assignment and env code below must
    treat it as ungrantable rather than hand it devices it can never see.
    '''
    if spec.node_class:
        return False
    runtime = (spec.descriptor or {}).get('spec', {}).get('runtime', {})
    return not runtime.get('localCommand')

def assign_local_gpus(specs : List[NodeSpec],
                    host_gpus : list[int]) -> dict[tuple[str, int], list[int]]:
    '''
    Deterministic device assignment for a local run: walking specs in order, each
    replica of each GPU node takes the next ``gpu_count`` ordinals from
    ``host_gpus`` — the local twin of the exclusive Kubernetes grant (RFC 0003),
    so a worker's ``CUDA_VISIBLE_DEVICES`` shows exactly its granted devices.
    Docker-run native components are skipped: they cannot receive the mask (see
    ``_runs_via_docker``), so granting them ordinals would only starve the
    workers that can.

    When demand exceeds ``len(host_gpus)`` the walk wraps around (duplicates
    within one replica are collapsed, with a per-replica warning naming the
    short grant) and a single aggregate warning is logged: sharing devices is
    fine for dev, but the same flow will not schedule that way on Kubernetes.
    An empty ``host_gpus`` returns an empty mapping — no env gets set, so
    CPU-fallback GPU nodes on a GPU-less machine behave exactly as before.
    '''
    if not host_gpus:
        return {}
    assignment : dict[tuple[str, int], list[int]] = {}
    cursor = 0
    demand = 0
    for spec in specs:
        if spec.device_type != 'gpu' or _runs_via_docker(spec):
            continue
        for replica_idx in range(spec.nb_tasks):
            devices = [host_gpus[(cursor + i) % len(host_gpus)] for i in range(spec.gpu_count)]
            cursor += spec.gpu_count
            demand += spec.gpu_count
            # dict.fromkeys collapses wrap-around duplicates while keeping order.
            granted = list(dict.fromkeys(devices))
            if len(granted) < spec.gpu_count:
                logger.warning(
                    f'node {spec.name} replica {replica_idx} asked for gpu_count='
                    f'{spec.gpu_count} but only {len(granted)} distinct device(s) are '
                    f'visible — VF_GPU_COUNT will report {len(granted)}, the delivered '
                    f'grant.')
            assignment[(spec.name, replica_idx)] = granted
    if demand > len(host_gpus):
        logger.warning(
            f'local GPU demand ({demand} device claims) exceeds the {len(host_gpus)} visible '
            f'device(s) — workers will share devices. Fine for dev; the same flow will not '
            f'schedule this way on Kubernetes.')
    return assignment

def _worker_env(spec : NodeSpec, nats_url : str, flow_id : str, flow_type : str, run_id : str,
                blob_redis_url : str | None, replica_id : int, envelope_version : int,
                python_path : list | None = None,
                blob_ttl_seconds : int | None = None,
                gpu_devices : list[int] | None = None) -> dict:
    env = dict(os.environ)
    if python_path:
        # Prepend, so a caller-supplied path wins over an inherited PYTHONPATH the
        # same way sys.path order works in the parent.
        existing = env.get('PYTHONPATH')
        env['PYTHONPATH'] = os.pathsep.join(list(python_path) + ([existing] if existing else []))
    env.update({
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
        'VF_ENVELOPE_VERSION': str(envelope_version),
    })
    if spec.node_class:
        env['VF_NODE_CLASS'] = spec.node_class
    if spec.component_ref:
        env['VF_COMPONENT_REF'] = spec.component_ref
        if spec.protocol_version is not None:
            env['VF_PROTOCOL_VERSION'] = str(spec.protocol_version)
    if spec.partition_by:
        env['VF_PARTITION_BY'] = spec.partition_by
    if spec.join_policy:
        env['VF_JOIN_POLICY_JSON'] = json.dumps(spec.join_policy)
    # Per-node failure handling, exactly as the manifests render it. A node that
    # overrides nothing ships nothing, so an unchanged flow gets the environment
    # it always did — and one that does override something behaves the same way
    # here as it will in the cluster, which is the entire point.
    if spec.delivery:
        if spec.delivery.get('delivery'):
            env['VF_DELIVERY'] = spec.delivery['delivery']
        if spec.delivery.get('on_error'):
            env['VF_ON_ERROR'] = spec.delivery['on_error']
    if blob_redis_url:
        env['VF_BLOB_REDIS_URL'] = blob_redis_url
    if spec.blob_readers is not None:
        # Enables refcounted blob reclamation (PROTOCOL.md BLOB-5); absent ⇒ TTL-only.
        env['VF_BLOB_READERS'] = str(spec.blob_readers)
    if blob_ttl_seconds is not None:
        env['VF_BLOB_TTL_SECONDS'] = str(blob_ttl_seconds)
    if spec.device_type == 'gpu' and not _runs_via_docker(spec):
        # The worker's GPU grant (RFC 0003): informational for native components,
        # which never see the Python node's reconstruction params. The count is the
        # *delivered* grant — when an oversubscribed host shrank the device list,
        # reporting spec.gpu_count would promise devices that don't exist. A
        # docker-run native gets neither variable: it receives no devices at all
        # (see _runs_via_docker), so a count would be a lie.
        env['VF_GPU_COUNT'] = str(len(gpu_devices) if gpu_devices is not None
                                  else spec.gpu_count)
        if spec.gpu_resource_name:
            env['VF_GPU_RESOURCE_NAME'] = spec.gpu_resource_name
    if gpu_devices:
        # Cooperative masking: the worker sees exactly its granted devices, so the
        # visibility contract holds locally too (see assign_local_gpus).
        env['CUDA_VISIBLE_DEVICES'] = ','.join(str(d) for d in gpu_devices)
    return env

def _publish_stop(nats_url : str, flow_id : str, run_id : str) -> None:
    async def _go() -> None:
        nc = await nats.connect(nats_url)
        await nc.publish(topology.control_subject_for(flow_id, run_id), b'stop')
        await nc.flush()
        await nc.drain()

    asyncio.run(_go())
