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
from typing import List, Optional

import nats  # noqa: F401  (import guard: fail fast if the broker client is missing)

from ..core.compiler import (
    NodeSpec,
    specs_from_tasks_data,
    validate_wire_compatibility,
)
from ..core.engine import ExecutionEngine
from ..messaging.topology import control_subject_for, delete_run_streams, provision_flow_sync

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
    '''
    def __init__(self, nats_url : str = DEFAULT_NATS_URL, blob_redis_url : str | None = None,
                specs : List[NodeSpec] | None = None,
                local_docker_nats_url : str | None = None,
                python_path : list | None = None, inherit_python_path : bool = True,
                default_image : str | None = None) -> None:
        self._nats_url = nats_url
        self._blob_redis_url = blob_redis_url
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
            raise ValueError('no specs to run: construct the engine with specs = ... or '
                            'pass tasks_data from Flow.build_tasks_data()')
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
            provision_flow_sync(self._nats_url, specs, flow_id, run_id, flow_type,
                                connect_options = {'allow_reconnect': False,
                                                   'connect_timeout': 5,
                                                   'max_reconnect_attempts': 0,
                                                   'error_cb': _quiet_error_cb},
                                timeout = PROVISION_TIMEOUT_SECONDS)
        except Exception as e:
            raise RuntimeError(
                f'could not reach NATS at {self._nats_url} ({type(e).__name__}: {e}). '
                f'Start one with `videoflow run-local` (it provisions a dev broker), '
                f'`docker compose up -d`, or `nats-server -js` — or point --nats at a '
                f'running server.') from e

        for spec in specs:
            for replica_idx in range(spec.nb_tasks):
                env = _worker_env(spec, self._nats_url, flow_id, flow_type, run_id,
                                self._blob_redis_url, replica_idx, envelope_version,
                                self._python_path)
                cmd, run_env = self._launch_command(spec, env)
                proc = subprocess.Popen(cmd, env = run_env)
                self._procs.append((spec.name, replica_idx, proc))
                logger.info(
                    f'Started worker pid={proc.pid} node={spec.name} replica={replica_idx} '
                    f'({"remote" if spec.is_remote else "python"})'
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
            raise ValueError(
                f'remote component node {spec.name!r} has no image to run locally — give it an '
                f'`image=` or a `runtime.localCommand` in its component descriptor.')
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
        Blocks until every worker process exits. Returns the names of nodes that had
        at least one replica exit non-zero (empty when the flow ran cleanly) — the
        same contract as ``KubernetesExecutionEngine.wait_for_completion``.

        A worker killed by SIGINT/SIGTERM is not counted: that is Ctrl-C or
        ``flow.stop()`` propagating, not a failure.
        '''
        stopped = {-signal.SIGINT, -signal.SIGTERM}
        self._failures = []
        for name, replica_idx, proc in self._procs:
            while True:
                try:
                    proc.wait()
                    break
                except KeyboardInterrupt:
                    # The children got the same SIGINT; keep reaping rather than
                    # abandoning them (a second Ctrl-C used to escape here).
                    continue
            if proc.returncode and proc.returncode not in stopped:
                self._failures.append((name, replica_idx, proc.returncode))
        failed, seen = [], set()
        for name, _replica, _code in self._failures:
            if name not in seen:
                seen.add(name)
                failed.append(name)
        return failed

    def failures(self) -> List[tuple]:
        '''``(node_name, replica_idx, returncode)`` for each worker that failed.'''
        return list(self._failures)

    def report_failures(self) -> None:
        '''
        Prints one line per failed worker. Local workers inherit stdout/stderr, so
        their tracebacks are already on the terminal — this is the index, not a dump.
        '''
        for name, replica_idx, code in self._failures:
            print(f'--- node {name} replica {replica_idx} exited with code {code}',
                  file = sys.stderr)

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
                await delete_run_streams(nc, flow_id, run_id)
            finally:
                await nc.drain()

        try:
            asyncio.run(_go())
        except Exception:
            logger.debug('stream teardown failed', exc_info = True)

def _worker_env(spec : NodeSpec, nats_url : str, flow_id : str, flow_type : str, run_id : str,
                blob_redis_url : str | None, replica_id : int, envelope_version : int,
                python_path : list | None = None) -> dict:
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
    if blob_redis_url:
        env['VF_BLOB_REDIS_URL'] = blob_redis_url
    return env

def _publish_stop(nats_url : str, flow_id : str, run_id : str) -> None:
    async def _go() -> None:
        nc = await nats.connect(nats_url)
        await nc.publish(control_subject_for(flow_id, run_id), b'stop')
        await nc.flush()
        await nc.drain()

    asyncio.run(_go())
