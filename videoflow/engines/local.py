'''
Execution engine that runs a distributed flow entirely on the local machine, one
OS subprocess per node (per replica, for nb_tasks > 1), all talking to a local
NATS server. Same ``videoflow.worker`` code path Kubernetes uses — only the way
processes are started differs — so it's the primary way to develop and test a
flow without a cluster.

A worker is the current interpreter by default. With ``worker_image`` set (what
``run-local`` does when the graph's dependencies are not installed on this host)
every Python worker is instead one ``docker run`` of the solution image — the
same image a cluster deploy uses — on the host network, with the solution's
mounts and, when the daemon has the NVIDIA runtime, its granted devices.

Prerequisite: a running NATS JetStream server, e.g. ``nats-server -js`` or
``docker run -p 4222:4222 nats -js``.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import json
import logging
import os
import queue
import re
import signal
import site
import subprocess
import sys
import sysconfig
import tempfile
import threading
import time
from typing import TYPE_CHECKING, List, Mapping, Optional

import nats  # also an import guard: fail fast if the broker client is missing

# The module, not the symbols: tests monkeypatch ``topology.provision_flow_sync``,
# which only works while the name resolves at call time (CLAUDE.md's plugin-registry
# rule — the same trap, for the same reason).
from ..backends.allocation import SHARING_COOPERATIVE, SHARING_EXCLUSIVE, DeliveredGrant, Infeasible, WorkloadRequest
from ..backends.outcomes import Unknown
from ..core.compiler import (
    NodeSpec,
    blob_reader_ids,
    parent_replicas,
    specs_from_tasks_data,
    store_admission,
    validate_wire_compatibility,
)
from ..core.engine import ExecutionEngine
from ..core.errors import BrokerUnavailable, ConfigError, ResourceUnavailable
from ..core.supervision import (
    EventLog,
    NodeExited,
    NodeGaveUp,
    NodeRestarted,
    NodeStarted,
    SupervisionPolicy,
    render_event,
)
from ..deploy.allocation_local import GRANT_ENV, POLICY_SHARED, LocalAllocationBackend
from ..deploy.build import docker_run_extra_args
from ..messaging import topology

if TYPE_CHECKING:
    # Type-only: manifests imports the optional `yaml` extra at module scope.
    from ..deploy.manifests import Mount

logger = logging.getLogger(__package__)

DEFAULT_NATS_URL = 'nats://localhost:4222'

# Docker container names: [a-zA-Z0-9][a-zA-Z0-9_.-]+; everything else becomes a dash.
_CONTAINER_NAME_RE = re.compile(r'[^a-z0-9.-]+')

def _container_name(env : Mapping[str, str], attempt : int) -> str:
    '''
    ``vf-<flow>-<run>-<node>-<replica>[-r<attempt>]`` for a worker container:
    unique per launch, so a restart never collides with a container that
    ``--rm`` has not finished removing, and recognisable in ``docker ps``.
    '''
    parts = [env.get('VF_FLOW_ID', 'flow'), env.get('VF_RUN_ID', 'run'),
             env.get('VF_NODE_NAME', 'node'), env.get('VF_REPLICA_ID', '0')]
    if attempt:
        parts.append(f'r{attempt}')
    return 'vf-' + '-'.join(_CONTAINER_NAME_RE.sub('-', str(p).lower()).strip('-') or 'x' for p in parts)

# Bound on the up-front stream provisioning. Locally an unreachable broker is a
# setup mistake worth reporting, not a transient worth blocking on indefinitely.
PROVISION_TIMEOUT_SECONDS = 15

# How often the supervisor repeats its control-abort while workers are still
# alive. The control stop is a plain (non-JetStream) publish, so it reaches only
# the workers subscribed at that instant — and a node can die before its siblings
# have finished connecting. Repeating is what makes the announcement reliable:
# a worker that was still starting up hears the next one instead of waiting
# forever on an end-of-stream that is not coming.
ABORT_REANNOUNCE_SECONDS = 2.0

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
    uses to decide whether to auto-build at all for a graph that imports on the host
    — most flows are pure Python, and building a (possibly CUDA) solution image to
    launch a few subprocesses would be a large and pointless cost. (A graph whose
    dependencies are *not* installed here is a different case: then the image is the
    only place the workers can run, and ``run-local`` builds it regardless.)
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
        - worker_image: when set, every Python worker runs as ``docker run`` of \
            this image (the solution image) instead of the current interpreter — \
            for a graph whose dependencies live only in its image. Natives keep \
            using their own ``image=`` / ``default_image``.
        - worker_mounts: the bind mounts those containers get (the solution's \
            ``x-mounts`` and ``--mount``; claim mounts are skipped — a claim \
            exists only in a cluster).
        - docker_gpus: whether the docker daemon has the NVIDIA runtime, so a \
            worker's granted devices can be handed to its container (``--gpus``). \
            Without it GPU workers in containers run device-less (a warning under \
            the shared policy, a refusal under strict).
    '''
    def __init__(self, nats_url : str = DEFAULT_NATS_URL, blob_redis_url : str | None = None,
                specs : List[NodeSpec] | None = None,
                local_docker_nats_url : str | None = None,
                python_path : list | None = None, inherit_python_path : bool = True,
                default_image : str | None = None,
                blob_ttl_seconds : int | None = None,
                supervision : SupervisionPolicy | None = None,
                profile_requests : dict[str, str] | None = None,
                gpu_policy : str = POLICY_SHARED,
                worker_image : str | None = None,
                worker_mounts : 'list[Mount] | None' = None,
                docker_gpus : bool = False) -> None:
        self._supervision = supervision or SupervisionPolicy.local()
        # In-image workers (see the module docstring): the image, its mounts and
        # whether the daemon can hand a container its GPU grant.
        self._worker_image = worker_image
        self._worker_mounts : list = list(worker_mounts or [])
        self._docker_gpus = docker_gpus
        # Names of the containers this engine started, for cleanup_containers().
        self._containers : list[str] = []
        # How the host's GPUs are partitioned across workers (``--gpu-policy``):
        # ``shared`` is today's wrap-around walk, ``strict`` refuses short grants
        # before launch. See deploy.allocation_local.
        self._gpu_policy = gpu_policy
        # Explicit channel-profile requests for the workers' env (deploy.admission); empty by default.
        self._profile_requests = dict(profile_requests or {})
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
        # Abort announcer: repeats the control stop until every worker has been
        # reaped (see _abort_flow). _abort_done ends it.
        self._abort_thread: Optional[threading.Thread] = None
        self._abort_done = threading.Event()
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
        # nodes — a CPU-only flow must not depend on the probe in any way. Every
        # policy goes through the allocation backend, which grants UUIDs (the
        # ``shared`` policy reproduces the ordinal walk of ``assign_local_gpus``)
        # and tells every worker what it really received (VF_GPU_GRANT_JSON).
        gpu_env : dict[tuple[str, int], Mapping[str, str]] = {}
        if any(s.device_type == 'gpu' for s in specs):
            gpu_env = allocate_local_gpus(specs, flow_id, run_id, LocalAllocationBackend(self._gpu_policy))
        if self._worker_image is not None and not self._docker_gpus:
            # A grant a container cannot receive: the daemon has no NVIDIA runtime,
            # so there is no --gpus to hand the devices over with. Strict refuses
            # (the whole point of strict is never launching on a short grant);
            # shared launches device-less and says so — the same stance as an
            # unobservable host.
            granted = sorted({name for (name, _replica), env in gpu_env.items() if env.get('CUDA_VISIBLE_DEVICES')})
            if granted and self._gpu_policy != POLICY_SHARED:
                raise ResourceUnavailable(
                    f'GPU node(s) {", ".join(granted)} would run inside {self._worker_image}, but the docker '
                    f'daemon has no nvidia runtime to hand them their devices; strict GPU policy refuses to '
                    f'launch them without.',
                    remedy = 'Install the NVIDIA container toolkit (`docker info` must list the nvidia runtime), '
                             'or run with --gpu-policy shared to launch them without a device.')
            if granted:
                logger.warning(f'docker daemon has no nvidia runtime: GPU node(s) {", ".join(granted)} run '
                               f'inside {self._worker_image} without a device.')
        for spec in specs:
            for replica_idx in range(spec.nb_tasks):
                env = _worker_env(spec, self._nats_url, flow_id, flow_type, run_id,
                                self._blob_redis_url, replica_idx, envelope_version,
                                self._python_path, blob_ttl_seconds = self._blob_ttl_seconds,
                                gpu_env = gpu_env.get((spec.name, replica_idx)),
                                profile_requests = self._profile_requests,
                                blob_reader_ids = blob_reader_ids(spec, specs),
                                parent_replicas = parent_replicas(spec, specs),
                                runtime_store_url = self._runtime_store_url(),
                                store_admission = store_admission(spec, specs))
                env['VF_TERMINATION_LOG'] = self._termination_log_path(spec.name, replica_idx)
                # Kept so a restart relaunches the identical worker, and so the
                # supervisor never has to re-derive an environment.
                self._launchers[(spec.name, replica_idx)] = (spec, env)
                self._start_worker(spec, replica_idx, attempt = 0)

    def _runtime_store_url(self) -> str:
        '''The run ledger of a local run (RFC 0006 ENV-10): a file store beside the termination logs, shared by every worker.'''
        if self._termination_dir is None:
            self._termination_dir = tempfile.mkdtemp(prefix = 'videoflow-term-')
        return 'file://' + os.path.join(self._termination_dir, 'ledger')

    def _termination_log_path(self, node : str, replica_idx : int) -> str:
        if self._termination_dir is None:
            self._termination_dir = tempfile.mkdtemp(prefix = 'videoflow-term-')
        path = os.path.join(self._termination_dir, f'{node}-{replica_idx}.json')
        self._termination_logs[(node, replica_idx)] = path
        return path

    def _start_worker(self, spec : NodeSpec, replica_idx : int, attempt : int) -> None:
        _spec, env = self._launchers[(spec.name, replica_idx)]
        cmd, run_env = self._launch_command(spec, env, attempt)
        proc = subprocess.Popen(cmd, env = run_env)
        self._procs.append((spec.name, replica_idx, proc))
        self._attempts[(spec.name, replica_idx)] = attempt
        self._events.emit(NodeStarted(spec.name, replica_idx, attempt))
        logger.info(
            f'Started worker pid={proc.pid} node={spec.name} replica={replica_idx} '
            f'({"remote" if spec.is_remote else "python"})'
            + (f' [restart {attempt}]' if attempt else '')
        )

    def _launch_command(self, spec : NodeSpec, env : dict, attempt : int = 0) -> tuple:
        '''
        The command + environment to start one worker for ``spec``:

        - native Python node: ``python -m videoflow.worker`` in the current env, \
            or ``docker run`` of ``worker_image`` (its entrypoint is that worker) \
            when the engine was given one;
        - remote component with a ``localCommand``: run that binary directly (env carries VF_*);
        - remote component otherwise: ``docker run`` its image with VF_* passed via -e.
        '''
        # A Python node/component (node_class set) runs the Python worker, whether it
        # came from a graph class or a descriptor's pythonClass. Only a native
        # component (no node_class) uses localCommand / docker.
        if spec.node_class:
            if self._worker_image is None:
                return [sys.executable, '-m', 'videoflow.worker'], env
            return self._docker_command(env, self._worker_image, None, attempt)
        runtime = (spec.descriptor or {}).get('spec', {}).get('runtime', {})
        local_command = runtime.get('localCommand')
        if local_command:
            return list(local_command), env
        image = spec.image or self._default_image
        if not image:
            raise ConfigError(
                f'Remote component node {spec.name!r} has no image to run locally.',
                remedy = 'Give it an `image=`, or a `runtime.localCommand` in its '
                        'component descriptor.',
                node = spec.name)
        return self._docker_command(env, image, spec.command, attempt)

    def _docker_url(self, url : str) -> str:
        '''
        A localhost URL as a worker container reaches it. On Linux ``--network
        host`` puts the container in the host's network namespace, so ``localhost``
        is the host; Docker Desktop (macOS/Windows) routes to the host through
        ``host.docker.internal`` instead.
        '''
        if sys.platform.startswith('linux'):
            return url
        return url.replace('localhost', 'host.docker.internal').replace('127.0.0.1', 'host.docker.internal')

    def _docker_command(self, env : dict, image : str, command : Optional[list], attempt : int) -> tuple:
        '''
        ``docker run`` of ``image`` as one worker: the host network (the broker is
        on this machine), only the VF_* environment (and the blob store URL and
        device mask) passed through with localhost URLs rewritten for the
        container, the worker mounts, the termination-log/ledger directory the
        supervisor reads, and — for a Python worker holding a grant on a daemon
        with the NVIDIA runtime — ``--gpus`` naming exactly its devices.
        '''
        passthrough = {k: v for k, v in env.items()
                       if k.startswith('VF_') or k in ('VIDEOFLOW_BLOB_REDIS_URL', 'CUDA_VISIBLE_DEVICES')}
        if 'VF_NATS_URL' in passthrough:
            passthrough['VF_NATS_URL'] = self._local_docker_nats_url or self._docker_url(passthrough['VF_NATS_URL'])
        for key in ('VF_BLOB_REDIS_URL', 'VIDEOFLOW_BLOB_REDIS_URL'):
            if key in passthrough:
                passthrough[key] = self._docker_url(passthrough[key])
        name = _container_name(env, attempt)
        docker = ['docker', 'run', '--rm', '--network', 'host', '--name', name, *docker_run_extra_args()]
        devices = env.get('CUDA_VISIBLE_DEVICES')
        if devices and self._docker_gpus:
            # docker parses --gpus as CSV: a comma-separated device list needs the quoted form.
            docker += ['--gpus', f'"device={devices}"']
        for k, v in passthrough.items():
            docker += ['-e', f'{k}={v}']
        for m in self._worker_mounts:
            if m.claim is not None:                 # a claim exists only in a cluster
                continue
            docker += ['-v', f'{m.host_path}:{m.container_path}' + (':ro' if m.read_only else '')]
        if self._termination_dir is not None:
            # VF_TERMINATION_LOG and the file:// run ledger live there; the
            # supervisor reads the death note back from the same path.
            docker += ['-v', f'{self._termination_dir}:{self._termination_dir}']
        docker.append(image)
        if command:
            docker += list(command)
        self._containers.append(name)
        return docker, dict(os.environ)

    def cleanup_containers(self) -> None:
        '''
        Removes whatever worker containers are still around — a client killed before
        it attached, or a ``--rm`` that never got to run. Best effort: nothing here
        can fail a run that already finished, and containers that are gone already
        are the normal case.
        '''
        names, self._containers = self._containers, []
        if not names:
            return
        try:
            subprocess.run(['docker', 'rm', '-f', *names], capture_output = True, check = False)
        except OSError:
            logger.debug('could not remove worker containers', exc_info = True)

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
        is Ctrl-C or ``flow.stop()`` propagating, not a failure. The same signal
        reported as an exit status (130/143 — what the worker's own handlers
        return, and what a ``docker run`` client relays for its container) counts
        the same way.

        Every worker is watched **concurrently**: one waiter thread per child
        reports its exit on a queue, and this loop drains the queue. Waiting on
        the children one after another — the previous shape — meant a processor
        that died while the source ahead of it in the list was still running was
        not restarted (or the flow not failed) until that source exited, which
        for an unbounded source is never: a healthy producer hid a dead
        downstream worker indefinitely.
        '''
        stopped = {-signal.SIGINT, -signal.SIGTERM, 128 + signal.SIGINT, 128 + signal.SIGTERM}
        self._failures = []
        pending = list(self._procs)
        self._procs = []
        exits : queue.Queue[tuple] = queue.Queue()
        outstanding = 0
        for entry in pending:
            _watch_exit(entry, exits)
            outstanding += 1
        try:
            while outstanding:
                try:
                    name, replica_idx, proc = exits.get()
                except KeyboardInterrupt:
                    # The children got the same SIGINT; keep reaping rather than
                    # abandoning them (a second Ctrl-C used to escape here).
                    continue
                outstanding -= 1
                code = proc.returncode or 0
                if code == 0 or code in stopped:
                    continue
                reason = self._termination_reason(name, replica_idx)
                self._events.emit(NodeExited(name, replica_idx, code, reason))
                restarted = self._maybe_restart(name, replica_idx, reason)
                if restarted is not None:
                    _watch_exit(restarted, exits)
                    outstanding += 1
                    continue
                self._failures.append((name, replica_idx, code))
        finally:
            # Every worker is accounted for, so there is nobody left to tell.
            self._stop_abort_announcer()
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

        The stop is **repeated** until every worker has been reaped, in a
        background thread so the reaping loop keeps running. One publish is not
        enough: the control subject is core NATS, delivered only to whoever is
        subscribed at that instant, and a node that dies on its first message
        typically dies while the workers further down the graph are still
        connecting. Those are exactly the workers that need to hear it, and a
        missed announcement hangs the run until something else times out.
        '''
        logger.error(f'node {node} gave up; stopping the flow so its children do not '
                    f'wait for an end-of-stream that is not coming')
        if self._abort_thread is not None:          # already announcing this run's death
            return
        if self._flow_id is None or self._run_id is None:
            return
        self._abort_done.clear()
        self._abort_thread = threading.Thread(
            target = self._announce_abort, name = 'vf-abort-announcer', daemon = True)
        self._abort_thread.start()

    def _announce_abort(self) -> None:
        '''Republishes the control stop every ``ABORT_REANNOUNCE_SECONDS`` until reaping ends.'''
        flow_id, run_id = self._flow_id, self._run_id
        assert flow_id is not None and run_id is not None   # checked by _abort_flow

        async def _go() -> None:
            nc = await nats.connect(self._nats_url, error_cb = _quiet_error_cb)
            subject = topology.control_subject_for(flow_id, run_id)
            try:
                while not self._abort_done.is_set():
                    await nc.publish(subject, b'stop')
                    await nc.flush()
                    await asyncio.sleep(ABORT_REANNOUNCE_SECONDS)
            finally:
                await nc.drain()

        try:
            asyncio.run(_go())
        except Exception:
            logger.debug('could not signal termination after a node gave up', exc_info = True)

    def _stop_abort_announcer(self) -> None:
        '''Ends the repeated stop announcement (no-op when nothing aborted).'''
        self._abort_done.set()
        thread, self._abort_thread = self._abort_thread, None
        if thread is not None:
            thread.join(timeout = ABORT_REANNOUNCE_SECONDS + 5)

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
                observation = await topology.delete_run_streams(nc, flow_id, run_id)
                if not observation.complete:
                    # Incomplete is not failed-silently: name what is left so a
                    # retried teardown knows there is something to retry.
                    logger.warning(f'run {run_id} of flow {flow_id}: broker cleanup incomplete — '
                                   f'{observation.reason or "some streams remain"}; remaining: '
                                   f'{", ".join(observation.remaining) or "unknown"}')
            finally:
                await nc.drain()

        try:
            asyncio.run(_go())
        except Exception:
            logger.debug('stream teardown failed', exc_info = True)

def _watch_exit(entry : tuple, exits : 'queue.Queue[tuple]') -> threading.Thread:
    '''
    Waits for one worker on its own daemon thread and reports ``entry`` — the
    supervisor's ``(name, replica, proc)`` — on ``exits`` once it has ended.

    The thread reports unconditionally: a waiter that failed to report would
    leave the supervisor loop counting a worker that nothing will ever deliver.
    The exit status is read off ``proc.returncode`` by the loop, as before, so a
    ``wait()`` that raised (it should not; ``Popen.wait`` retries EINTR) is
    logged and the process is judged by whatever status it recorded.

    Daemon, so a supervisor interrupted out of its loop does not hang the
    interpreter on a worker it has already given up waiting for.
    '''
    name, replica_idx, proc = entry

    def _wait() -> None:
        try:
            proc.wait()
        except Exception:
            logger.warning(f'waiting on worker node={name} replica={replica_idx} raised',
                        exc_info = True)
        finally:
            exits.put(entry)

    thread = threading.Thread(target = _wait, daemon = True,
                              name = f'vf-wait-{name}-{replica_idx}')
    thread.start()
    return thread

def _runs_via_docker(spec : NodeSpec) -> bool:
    '''
    Whether this spec is a *native* component that ``_launch_command`` runs with
    ``docker run`` (no ``localCommand``). Such a worker receives no GPU grant
    locally (a documented non-goal), so the assignment and env code below must
    treat it as ungrantable rather than hand it devices it can never see. A
    Python worker keeps its grant wherever it runs: on the host the mask is its
    environment, in the solution image the engine hands the same devices to the
    container with ``--gpus``.
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

def local_workload_requests(specs : List[NodeSpec], flow_id : str, run_id : str) -> list[WorkloadRequest]:
    '''
    One ``WorkloadRequest`` per replica of every GPU node the local engine can
    grant devices to (docker-run natives excluded, see ``_runs_via_docker``),
    in launch order: whole-device requests are exclusive, a node declaring
    ``gpu_memory_gib`` is a cooperative sharer whose declared peak is that demand.
    '''
    requests : list[WorkloadRequest] = []
    for spec in specs:
        if spec.device_type != 'gpu' or _runs_via_docker(spec):
            continue
        peak = int(spec.gpu_memory_gib * (1 << 30)) if spec.gpu_memory_gib is not None else None
        for replica_idx in range(spec.nb_tasks):
            requests.append(WorkloadRequest(
                flow_id = flow_id, run_id = run_id, workload_id = f'{spec.name}/{replica_idx}',
                device_count = spec.gpu_count,
                sharing = SHARING_COOPERATIVE if peak is not None else SHARING_EXCLUSIVE,
                declared_peak_memory_bytes = peak, provenance = {'device_count': 'spec.gpu_count'}))
    return requests

def allocate_local_gpus(specs : List[NodeSpec], flow_id : str, run_id : str,
                        backend : LocalAllocationBackend) -> dict[tuple[str, int], Mapping[str, str]]:
    '''
    The GPU environment of every worker, decided before any worker is launched:
    ``CUDA_VISIBLE_DEVICES`` (UUIDs), ``VF_GPU_COUNT`` (the delivered count) and
    ``VF_GPU_GRANT_JSON``. Strict policy raises ``ResourceUnavailable`` naming
    every reason the flow does not fit, and refuses an unobservable host; the
    shared policy launches on an unobservable host without a mask but marks
    every grant ``host='unobserved'`` so nobody reads it as a zero-GPU machine.
    '''
    requests = local_workload_requests(specs, flow_id, run_id)
    if not requests:
        return {}
    keyed = {r.workload_id: (r.workload_id.rsplit('/', 1)[0], int(r.workload_id.rsplit('/', 1)[1])) for r in requests}
    observed = backend.inventory({})
    if isinstance(observed, Unknown):
        if backend.policy != POLICY_SHARED:
            raise ResourceUnavailable(
                f'Cannot observe this host\'s GPUs ({observed.reason}: {observed.detail}); strict GPU policy '
                f'refuses to launch against an unobserved host.',
                remedy = 'Fix nvidia-smi (driver, PATH) and rerun, or run with --gpu-policy shared to launch '
                         'without a grant.')
        logger.warning(f'GPU discovery failed ({observed.reason}: {observed.detail}); launching GPU workers '
                       f'without a device grant. This is an unobserved host, not a zero-GPU one.')
        return {key: {GRANT_ENV: json.dumps(DeliveredGrant(w, (), False, r.device_count, backend.policy,
                                                               host = 'unobserved').to_dict(),
                                            separators = (',', ':'))}
                for w, key in keyed.items() for r in requests if r.workload_id == w}
    outcome = backend.plan(requests, observed.value)
    if isinstance(outcome, Infeasible):
        if backend.policy != POLICY_SHARED:
            raise ResourceUnavailable(
                'This flow does not fit the GPUs visible to run-local under --gpu-policy strict:\n  - '
                + '\n  - '.join(outcome.reasons),
                remedy = 'Reduce replicas or gpu_count, declare gpu_memory_gib on sharers, free the devices, '
                         'or opt into device sharing with --gpu-policy shared.')
        # Shared policy on an empty pool: today's behaviour — no mask, CPU fallback —
        # but the grant says so.
        logger.warning('no GPU visible to run-local: ' + '; '.join(outcome.reasons))
        return {key: {'VF_GPU_COUNT': '0',
                      GRANT_ENV: json.dumps(DeliveredGrant(w, (), False, r.device_count, backend.policy).to_dict(),
                                            separators = (',', ':'))}
                for w, key in keyed.items() for r in requests if r.workload_id == w}
    for note in outcome.notes:
        logger.warning(f'local GPU plan: {note}')
    claim = backend.reserve(outcome, f'{flow_id}:{run_id}', outcome.snapshot_generation)
    if claim.grant is None:
        raise ResourceUnavailable(f'Could not reserve the planned GPUs: {claim.evidence.get("reason", claim.status)}.',
                                  remedy = 'Rerun; the host changed between planning and launch.')
    return {keyed[w]: backend.bindings(claim.claim_id, w).env for w in outcome.assignments}

def _worker_env(spec : NodeSpec, nats_url : str, flow_id : str, flow_type : str, run_id : str,
                blob_redis_url : str | None, replica_id : int, envelope_version : int,
                python_path : list | None = None,
                blob_ttl_seconds : int | None = None,
                gpu_devices : list[int] | None = None,
                gpu_env : Mapping[str, str] | None = None,
                profile_requests : dict[str, str] | None = None,
                blob_reader_ids : list[str] | None = None,
                parent_replicas : list[int] | None = None,
                runtime_store_url : str | None = None,
                store_admission : float | None = None) -> dict:
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
    if blob_reader_ids:
        # Reader obligations by identity (RFC 0006 BLOB-13); only rendered under the switch.
        env['VF_BLOB_READER_IDS'] = ','.join(blob_reader_ids)
    if store_admission is not None and store_admission < 1.0:
        # This publisher's share of the payload store before it holds (BLOB-16); the
        # deepest publisher's full share is the worker's default.
        env['VF_STORE_ADMISSION'] = f'{store_admission:g}'
    if parent_replicas:
        # The EOS-7 barrier's expectation per parent (RFC 0006 ENV-11); under the switch only.
        env['VF_PARENT_REPLICAS'] = ','.join(str(n) for n in parent_replicas)
    if runtime_store_url:
        # The run ledger (RFC 0006 ENV-10); under the switch only.
        env['VF_RUNTIME_STORE_URL'] = runtime_store_url
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
    if gpu_env:
        # The allocation backend's bindings (strict policy or RFC 0006): a UUID
        # mask, the delivered count and the grant record itself (ENV-14).
        env.update(gpu_env)
    if profile_requests:
        env.update(profile_requests)
    return env

def _publish_stop(nats_url : str, flow_id : str, run_id : str) -> None:
    async def _go() -> None:
        nc = await nats.connect(nats_url)
        await nc.publish(topology.control_subject_for(flow_id, run_id), b'stop')
        await nc.flush()
        await nc.drain()

    asyncio.run(_go())
