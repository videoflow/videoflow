'''
Command-line entrypoint for deploying a videoflow graph to Kubernetes.

    videoflow deploy path/to/graph.py

``deploy`` is one command that does everything: it generates the solution config
(asking the template's questions when none exists), runs the solution's prepare
hook, builds and loads the node image into the detected local cluster, provisions
the broker (an in-cluster dev NATS + Redis when ``--nats`` is omitted), applies
the flow, and — for a BATCH flow — waits for it to run to completion and then
tears down every resource (Kubernetes workloads + broker streams + owned infra).
A REALTIME flow is applied and left running; stop it later with ``videoflow
teardown``. Each automatic step has an explicit override (``--config``,
``--image``, ``--nats``, ``--mount``, ``--no-prepare``, ``--no-build``, ...).
Nothing is written to disk (beyond a generated ``config.yaml``) unless
``--render-only`` (write manifest files + kustomization) or ``--dry-run`` (print
YAML to stdout) is given.

The graph module must expose a factory (default name ``build_flow``) that returns a
built ``videoflow.core.flow.Flow`` without calling ``.run()`` on it — the CLI needs
the graph, not a running flow. When the graph's dependencies are not importable on
this machine, deploy compiles it inside the solution image instead.
'''
from __future__ import absolute_import, division, print_function

import argparse
import asyncio
import os
import signal
import subprocess
import sys
import traceback
import uuid
from typing import Any

import numpy as np
import yaml

from .. import __version__
from ..backends.capabilities import kubernetes_execution_capabilities, local_execution_capabilities
from ..components.descriptor import load_descriptor
from ..components.oci import inspect_component, pull_component, push_component
from ..core.compiler import compile_flow, specs_from_tasks_data
from ..core.constants import BATCH
from ..core.errors import (
    EXIT_INTERRUPTED,
    BrokerUnavailable,
    ClusterError,
    ConfigError,
    FlowFailed,
    FlowStalled,
    GraphError,
    ResourceUnavailable,
    VideoflowError,
)
from ..core.flow import Flow
from ..core.supervision import SupervisionPolicy
from ..utils.plugins import load_plugin_group
from .admission import (
    admit,
    enforce_admission,
    free_gpu_devices_observed,
    jetstream_capabilities,
    jetstream_capabilities_observed,
    local_dev_capabilities,
    parse_profile_requests,
    redis_payload_capabilities,
    redis_payload_capabilities_observed,
    requests_env,
    requirements_for,
    rollout_problems,
    run_stream_names,
    unknown_admission,
    verify_graph_size,
    verify_topology_shape,
)
from .broker_profiles import BROKER_PROFILE_NAMES, BrokerProfile, RedisProfile, broker_profiles
from .build import (
    PUSH_TOOLS,
    autobuild,
    docker_gpus_available,
    find_dockerfile,
    image_exists,
    push_image,
    registry_ref,
    resolve_needs_gpu,
    run_in_image,
)
from .cluster import (
    current_context,
    detect_cluster,
    gpu_preflight,
    hostpath_warning,
    is_registry_qualified,
    load_images,
    nvidia_runtimeclass,
)
from .compile import declared_requirements, load_flow, requirements_from_document, specs_from_document
from .gpu import (
    GPU_STRATEGY_ENTRY_POINT_GROUP,
    IMPOSSIBLE_GPU_REQUEST,
    UNOBSERVABLE_GPU_STATE,
    GpuStrategy,
    get_gpu_mode,
    registered_gpu_modes,
    resolve_gpu_resource,
)
from .images import (
    DEFAULT_IMAGE_PULL_POLICY,
    IMAGE_PULL_POLICIES,
    parse_override,
    resolve_image,
)
from .profiles import PROFILES_FILE_ENV, apply_docker_env, command_defaults, load_profiles, select_profile

#: The commands a cluster profile feeds (see ``deploy.profiles``).
PROFILE_COMMANDS = ('deploy', 'run-local', 'teardown')

GRAPH_HELP = ('path/to/graph.py[:build_flow], or <repo>://<name> for a solution shipped in a '
              'videoflow repository (e.g. videoflow-contrib://human_tracking), fetched at this '
              'version into ~/.videoflow/solutions')

#: `pip install videoflow` is the core; the broker client, wire format, OpenCV, Redis
#: and oras are extras (pyproject.toml). Top-level module -> (distribution, the
#: smallest extra that provides it), for turning a bare ModuleNotFoundError into
#: the install command the operator needs.
_OPTIONAL_MODULES = {
    'nats': ('nats-py', 'distributed'),
    'msgpack': ('msgpack', 'distributed'),
    'google': ('protobuf', 'distributed'),          # google.protobuf
    'redis': ('redis', 'blob'),
    'oras': ('oras', 'deploy'),
    'cv2': ('opencv-python-headless', 'vision'),
}

def _missing_extra(error : ModuleNotFoundError, command : str) -> ResourceUnavailable | None:
    '''
    The operator-facing error for a ``ModuleNotFoundError`` on one of videoflow's
    own optional dependencies, or None when the module is not one of them (a real
    bug, whose traceback must stay).
    '''
    entry = _OPTIONAL_MODULES.get((error.name or '').split('.')[0])
    if entry is None:
        return None
    distribution, extra = entry
    return ResourceUnavailable(
        f'`videoflow {command}` needs the {distribution} package, which this install does not include.',
        remedy = f"Install the CLI extras: pip install 'videoflow[all]'  (videoflow[{extra}] is the "
                 f'minimum for this one).')


def _load_flow(target : str) -> Flow:
    '''
    - Arguments:
        - target: ``path/to/graph.py`` or ``path/to/graph.py:factory_name`` \
            (factory defaults to ``build_flow``).

    - Returns:
        - a built ``Flow`` produced by calling the factory.
    '''
    return load_flow(target)

def _graph_location(graph_arg : str) -> tuple[str, str, str, str | None]:
    '''
    Where the graph lives, for a ``path/to/graph.py[:factory]`` argument or a
    ``<repo>://<name>[:factory]`` solution reference (fetched at this version
    into the solutions cache, see ``solution_refs``).

    - Returns:
        - ``(graph_path, graph_dir, graph_target, build_context)``: the module \
            path, its directory, the ``path[:factory]`` target ``load_flow`` takes, \
            and the docker build context a reference implies (its checkout root; \
            None for a plain path, which keeps the enclosing-git-root default).

    - Raises:
        - ``ConfigError``: a malformed reference (anything with ``://`` that is \
            not ``<repo>://<name>[:factory]``), or a path that is not a file.
    '''
    from .solution_refs import resolve_solution_ref
    build_context = None
    # Anything with '://' is meant as a reference — a path never has one — so a
    # malformed ref is reported as such, not as a missing file called 'videoflow'.
    if '://' in graph_arg:
        resolved = resolve_solution_ref(graph_arg)
        graph_path, factory, build_context = resolved.graph_path, resolved.factory, resolved.build_context
    else:
        graph_path, factory = (graph_arg.rsplit(':', 1) if ':' in graph_arg else (graph_arg, None))
    if not os.path.isfile(graph_path):
        raise ConfigError(f'Graph module not found: {graph_path}')
    graph_dir = os.path.dirname(os.path.abspath(graph_path))
    graph_target = os.path.abspath(graph_path) + (f':{factory}' if factory else '')
    return graph_path, graph_dir, graph_target, build_context

def _probe_host_import(graph_target : str) -> ImportError | None:
    '''
    Whether the graph's dependencies import on this machine: the ``ImportError``
    when they do not (the signal that the workers must run inside the solution
    image), else None. Any other failure of the factory — a config that is not
    there yet, an output the prepare hook has not produced — is not this probe's
    to report: the deps import, so the real load after prepare will raise it with
    its own message. ``SystemExit`` (no such module or factory) propagates.
    '''
    try:
        _load_flow(graph_target)
    except ImportError as e:
        return e
    except Exception:                                 # noqa: BLE001 — see docstring
        return None
    return None

def _probe_host_flow(graph_target : str) -> Flow | None:
    '''The graph when it builds on this machine, else None (see ``_probe_host_import``).'''
    try:
        return _load_flow(graph_target)
    except Exception:                                 # noqa: BLE001 — a host that cannot build it
        return None

def _export_solution_config(config_path : str | None) -> None:
    '''
    Publishes the resolved config path to every ``build_flow`` this process (or a
    worker it spawns) runs, so a solution reading ``VF_SOLUTION_CONFIG`` honours
    ``--config`` instead of always opening the ``config.yaml`` beside its module.
    '''
    from .solution import CONFIG_ENV
    if config_path:
        os.environ[CONFIG_ENV] = config_path

def _solution_env(config_path : str | None) -> dict[str, str]:
    '''The same variable for a container (``run_in_image(env=...)``).'''
    from .solution import CONFIG_ENV
    return {CONFIG_ENV: config_path} if config_path else {}

def _solution_template(graph_dir : str, config_path : str | None) -> tuple[dict | None, dict]:
    '''The solution's ``config.template.yaml`` (None without one) and the resolved config (``{}`` without one).'''
    from .solution import find_template, load_template
    template_path = find_template(graph_dir)
    template = load_template(template_path) if template_path else None
    config : dict = {}
    if config_path:
        with open(config_path) as f:
            config = yaml.safe_load(f) or {}
    return template, config

def _config_mounts(graph_dir : str, config_path : str | None) -> list:
    '''
    A ``--config`` that lives outside the solution directory, mounted read-only
    at its own path so the prepare/compile containers (which see the solution
    directory only) can read it.
    '''
    from .manifests import parse_mounts
    if not config_path:
        return []
    real = os.path.realpath(config_path)
    if real.startswith(os.path.realpath(graph_dir).rstrip('/') + '/'):
        return []
    return parse_mounts([f'{real}:{os.path.abspath(config_path)}:ro'])

def _needs_gpu_for_build(graph_dir : str, graph_target : str, declared : bool | None) -> tuple[bool, str | None]:
    '''
    Which of the solution's Dockerfiles to build (``build.resolve_needs_gpu``):
    the template's ``x-gpu`` when declared; otherwise, when both files exist and
    the graph builds on this machine, its compiled device placement.
    '''
    specs = None
    if declared is None and find_dockerfile(graph_dir, True) != find_dockerfile(graph_dir, False):
        flow = _probe_host_flow(graph_target)
        if flow is not None:
            try:
                specs = compile_flow(flow)
            except ValueError:
                specs = None
    return resolve_needs_gpu(graph_dir, declared, specs)

#: ``--gpu-runtime-class none``: deliberately no runtimeClassName on the GPU pods.
GPU_RUNTIME_CLASS_NONE = 'none'

def _normalize_runtime_class(requested : str | None) -> str | None:
    '''``none`` (or empty) means no runtimeClassName, not a class called that.'''
    if requested is None or requested.strip().lower() in ('', GPU_RUNTIME_CLASS_NONE):
        return None
    return requested

def _resolve_gpu_runtime_class(requested : str | None, gpu_specs : list, kubectl : str) -> str | None:
    '''
    The ``runtimeClassName`` GPU pods get: the one asked for; none for ``none``;
    otherwise, for a flow with GPU nodes, the NVIDIA RuntimeClass the cluster
    registers, when it does. k3s ships an opt-in ``nvidia`` handler and leaves
    runc the default, so a GPU pod without it schedules and then starts with no
    device — the easiest mistake to make, and one that does not look like a flag
    problem; a cluster whose default runtime already injects devices is not
    harmed by naming the class it also registers. The choice is announced.
    '''
    if not gpu_specs or requested is not None:
        return _normalize_runtime_class(requested)
    found = nvidia_runtimeclass(kubectl)
    if found:
        print(f'NOTE: GPU pods will use RuntimeClass {found!r} (registered in the cluster); '
              f'pass --gpu-runtime-class {GPU_RUNTIME_CLASS_NONE} to opt out.', file = sys.stderr)
    return found

class _SpecsFlow:
    '''
    The engine-facing half of a ``Flow`` for a graph compiled inside its image:
    ``run-local`` then holds the specs but no in-process graph. ``run``/``join``/
    ``stop`` mirror ``core.flow.Flow`` without ``tasks_data`` — the engine was
    constructed with the specs — so the run loop treats both the same way.
    '''
    def __init__(self, flow_id : str, flow_type : str) -> None:
        self.flow_id = flow_id
        self.flow_type = flow_type
        self.run_id : str | None = None
        self._engine : Any = None

    def run(self, engine : Any, run_id : str | None = None) -> None:
        self._engine = engine
        self.run_id = run_id or uuid.uuid4().hex[:12]
        engine.allocate_and_run_tasks(None, self.flow_id, self.flow_type, self.run_id)

    def join(self) -> None:
        self._engine.join_task_processes()

    def stop(self) -> None:
        self._engine.signal_flow_termination()
        self.join()

def _gpu_cleanup(gpu_strategy : GpuStrategy | None, kubectl : str,
                 flow_id : str | None = None) -> None:
    '''
    Runs a GPU strategy's ``cleanup()``, best-effort. Called from error paths and
    teardown, so a failure to undo the reconfiguration must be reported but must
    not mask the original error or abort the rest of teardown. A ``None`` strategy
    (no GPU nodes, or an unknown mode) is a no-op. ``flow_id`` scopes the cleanup
    to this flow's cluster state — other flows may be running against the same pool.
    '''
    if gpu_strategy is None:
        return
    try:
        gpu_strategy.cleanup(kubectl = kubectl, flow_id = flow_id)
    except Exception as e:                            # noqa: BLE001 — see docstring
        print(f'WARNING: GPU mode {gpu_strategy.name!r} cleanup failed: {e}', file = sys.stderr)

def _gpu_prepare(gpu_strategy : GpuStrategy, demand : dict, kubectl : str,
                 flow_id : str | None = None) -> None:
    '''
    Runs a GPU strategy's ``prepare()``, rolling back if it does not complete.

    A prepare that fails partway may already have changed the cluster, and
    ``cleanup()`` is documented to tolerate exactly that. The rollback covers
    ``BaseException`` rather than the usual pair, so a Ctrl-C mid-prepare is
    undone too.

    - Raises:
        - ClusterError: prepare failed with a RuntimeError/ValueError (the message \
            names the mode). Any other exception propagates unchanged, after \
            rollback.
    '''
    try:
        gpu_strategy.prepare(demand = demand, kubectl = kubectl, flow_id = flow_id)
    except BaseException as e:
        _gpu_cleanup(gpu_strategy, kubectl, flow_id)
        if isinstance(e, (RuntimeError, ValueError)):
            raise ClusterError(f'GPU mode {gpu_strategy.name!r} could not prepare '
                             f'the cluster: {e}') from e
        raise

def parse_resources(entries : list[str] | None) -> dict[str, dict[str, str]]:
    '''
    ``--resources`` values as ``{node or '*': {cpu, memory, cpu_limit, memory_limit}}``:
    each entry is ``NODE=key:quantity[,key:quantity...]`` (``*`` for every node).

    - Raises:
        - ConfigError: an entry is malformed or names an unknown key.
    '''
    keys = ('cpu', 'memory', 'cpu_limit', 'memory_limit')
    out : dict[str, dict[str, str]] = {}
    for entry in entries or []:
        node, sep, rest = entry.partition('=')
        if not sep or not node.strip() or not rest.strip():
            raise ConfigError(f'--resources entry {entry!r} is not NODE=key:quantity[,key:quantity].',
                              remedy = "Example: --resources '*=cpu:500m,memory:1Gi' --resources detector=memory_limit:4Gi")
        for pair in rest.split(','):
            key, colon, quantity = pair.partition(':')
            key, quantity = key.strip(), quantity.strip()
            if not colon or key not in keys or not quantity:
                raise ConfigError(f'--resources entry {entry!r}: {pair.strip()!r} is not one of {keys} with a quantity.',
                                  remedy = "Example: --resources '*=cpu:500m,memory:1Gi'")
            out.setdefault(node.strip(), {})[key] = quantity
    return out

def _placement_options(args : argparse.Namespace) -> dict[str, Any]:
    '''
    The opt-in placement keywords for ``render_manifests`` (plan Phase 4):
    ``rollout_policy``, ``gpu_nodes`` and ``resources`` — each passed only when
    the operator asked, so a deploy that never did renders byte-for-byte as before.
    '''
    options : dict[str, Any] = {}
    if args.rollout_policy:
        options['rollout_policy'] = args.rollout_policy
    if args.gpu_nodes:
        options['gpu_nodes'] = [n.strip() for n in args.gpu_nodes.split(',') if n.strip()]
    resources = parse_resources(args.resources)
    if resources:
        options['resources'] = resources
    return options

def _cmd_deploy(args : argparse.Namespace) -> None:
    from .manifests import parse_mounts, parse_pvc_mounts

    overrides = {}
    for override in args.image_override or []:
        try:
            name, ref = parse_override(override)
        except ValueError as e:
            raise ConfigError(str(e)) from e
        overrides[name] = ref

    graph_path, graph_dir, graph_target, ref_context = _graph_location(args.graph)

    # 0. Solution conventions: ensure a config (interactive Q&A over the solution's
    # config.template.yaml when none exists), collect its x-mounts, and read what
    # it says about the image to build (x-gpu).
    from .solution import ensure_config, resolve_gpu, resolve_mounts, split_mount_specs
    interactive = not args.non_interactive and sys.stdin.isatty()
    config_path = ensure_config(graph_dir, args.config, interactive)
    _export_solution_config(config_path)
    template, config = _solution_template(graph_dir, config_path)
    try:
        template_mounts = (resolve_mounts(template, config, graph_dir, home = args.mount_home)
                           if template and config_path else [])
        host_specs, claim_specs = split_mount_specs(template_mounts)
        # Host paths and claims in one list: the two parsers number their volume
        # names from disjoint prefixes (vf-mount-*, vf-pvc-*), so the pods take the
        # concatenation as-is. A host path under a claim's path is served by the
        # claim in the pods (manifests.pod_mounts) and kept for the prep/compile
        # containers.
        mounts = parse_mounts((args.mount or []) + host_specs) \
            + parse_pvc_mounts((args.mount_pvc or []) + claim_specs)
        # Prep/compile containers additionally see the solution directory itself
        # (config, prepare.py, work dir) at its host path — and a --config kept
        # elsewhere. run_in_image skips the claim mounts — a claim exists only
        # inside the cluster.
        container_mounts = parse_mounts([graph_dir]) + mounts + _config_mounts(graph_dir, config_path)
        declared_gpu = resolve_gpu(template, config)
    except ValueError as e:
        raise ConfigError(str(e)) from e
    # Resolved before any step that costs time: a bad --broker-replicas should not
    # wait for an image build to be reported. ``args.broker_profile`` is None
    # when the operator left the choice to the deploy: then a broker the
    # namespace already runs is adopted as it is; a named profile that
    # contradicts one is refused (adopt_profiles, below).
    broker_profile, redis_profile = broker_profiles(
        args.broker_profile or 'dev', replicas = args.broker_replicas,
        storage_class = args.broker_storage_class, priority_class = args.priority_class)

    # 1. Image: --image wins; else build from the solution's [gpu.]Dockerfile
    # (base image built from a source checkout when missing, or pulled from
    # ghcr.io on a wheel install). Which of the
    # two is decided by the flow (x-gpu / device placement), never by whether
    # this machine's docker daemon happens to have the NVIDIA runtime — that
    # only decides whether the prepare/compile containers get --gpus.
    gpus = docker_gpus_available()
    image = args.image
    if image is None and not args.no_build:
        needs_gpu, note = _needs_gpu_for_build(graph_dir, graph_target, declared_gpu)
        if note:
            print(f'NOTE: {note}', file = sys.stderr)
        try:
            image = autobuild(graph_dir, needs_gpu = needs_gpu,
                              context_override = args.build_context or ref_context)
        except RuntimeError as e:
            raise ResourceUnavailable(str(e)) from e
    # 2. Prepare hook: runs inside the image, before compiling (its outputs get
    # baked into the compiled specs).
    from .solution import find_prepare, prepare_command, run_prepare_local
    if find_prepare(graph_dir) is not None and not args.no_prepare:
        try:
            if image is not None:
                run_in_image(image, prepare_command(config_path), mounts = container_mounts,
                             workdir = graph_dir, gpus = gpus, interactive = interactive,
                             env = _solution_env(config_path))
            else:
                run_prepare_local(graph_dir, config_path)
        except (RuntimeError, subprocess.CalledProcessError) as e:
            raise ClusterError(str(e)) from e

    # 3. Compile: locally when the graph's deps import on the host, else inside
    # the image (specs round-trip as JSON — same format as the specs ConfigMap).
    flow_id, flow_type, specs, declared = _compile_graph(args, graph_target, graph_dir, image,
                                                         container_mounts, gpus, config_path)
    if args.flow_id:
        flow_id = args.flow_id
    run_id = args.run_id or uuid.uuid4().hex[:12]

    # 3b. A registry (--registry, typically from the cluster profile): the local
    # image — now known to compile the graph — is pushed and the pods pull the
    # registry-qualified ref, the way onto a multi-node cluster where side-loading
    # reaches one node only. An image that already names a registry was pushed by
    # whoever built it.
    if args.registry and image and not is_registry_qualified(image) and image_exists(image):
        if args.dry_run:
            # No side effects on a dry run: render the ref the real deploy would push.
            print(f'NOTE: --dry-run: {image} is not pushed to {args.registry}; the manifests name the ref '
                  f'a deploy would.', file = sys.stderr)
            image = registry_ref(args.registry, image)
        else:
            try:
                image = push_image(image, args.registry, args.push_tool)
            except (RuntimeError, ValueError) as e:
                raise ClusterError(str(e)) from e

    # --dry-run / --render-only never touch the cluster (they include the dev-infra
    # manifests whenever the broker would have been auto-provisioned).
    if args.dry_run or args.render_only:
        _render_manifests_to_disk(args, image, flow_id, flow_type, specs, run_id,
                                  overrides, mounts)
        return

    # 4. Cluster mechanics: detect the flavor, load locally built images into it,
    # and warn (with copy-pasteable fixes) on hostPath/GPU mismatches.
    flavor = detect_cluster(args.kubectl)
    try:
        images = sorted({resolve_image(s.name, s.image, image, overrides) for s in specs})
    except ValueError as e:
        raise ConfigError(str(e)) from e
    local_images = [ref for ref in images if not is_registry_qualified(ref) and image_exists(ref)]
    if local_images:
        try:
            load_images(flavor, local_images, kubectl = args.kubectl)
        except RuntimeError as e:
            raise ClusterError(str(e)) from e
    if any(m.claim is None for m in mounts):
        # Only host paths are affected — a claim resolves inside the cluster on
        # every flavor.
        warning = hostpath_warning(flavor)
        if warning:
            print(f'WARNING: {warning}', file = sys.stderr)
    gpu_specs = [s for s in specs if s.device_type == 'gpu']
    # Decided here, before preflight, and used by everything downstream: the
    # preflight's advice, the manifests and the teardown hint all see one value.
    gpu_runtime_class = _resolve_gpu_runtime_class(args.gpu_runtime_class, gpu_specs, args.kubectl)
    if gpu_specs:
        from .manifests import _is_partitioned, gpu_demand, gpu_max_per_pod, gpu_pod_claims
        # The strategy decides names and geometry first (mix resolves each
        # sharer's MIG profile into its spec) so that everything downstream —
        # demand math, preflight, manifests, env — consumes the resolved specs.
        try:
            specs = get_gpu_mode(args.gpu_mode).resolve_specs(
                specs, kubectl = args.kubectl, default_resource = args.gpu_resource_name,
                flow_id = flow_id)
        except ValueError as e:
            raise ConfigError(str(e)) from e
        gpu_specs = [s for s in specs if s.device_type == 'gpu']
        # Whole-flow demand per extended resource: every replica of a GPU node claims
        # its own gpu_count devices, so a partially-schedulable flow deadlocks. The
        # per-pod maximum bounds single-node schedulability for multi-GPU nodes.
        demand = gpu_demand(specs, default_resource = args.gpu_resource_name)
        problems = gpu_preflight(args.kubectl, gpu_runtime_class = gpu_runtime_class,
                                 demand = demand, gpu_mode = args.gpu_mode,
                                 max_per_pod = gpu_max_per_pod(specs, default_resource = args.gpu_resource_name),
                                 pod_claims = gpu_pod_claims(specs, default_resource = args.gpu_resource_name))
        # An impossible request (multi-unit claim against a MIG/time-sliced
        # resource) can only end in an admission error or a silently broken
        # visibility contract — fatal regardless of --strict-preflight.
        # An unobservable occupancy is fatal wherever a mode mutates the cluster on
        # the strength of it (mix repartitions cards); for exclusive claims it is a
        # capacity warning the strict gate can promote.
        fatal = [p for p in problems if p.startswith(IMPOSSIBLE_GPU_REQUEST)
                 or (args.gpu_mode == 'mix' and p.startswith(UNOBSERVABLE_GPU_STATE))]
        if fatal:
            for p in fatal:
                print(f'ERROR: {p}', file = sys.stderr)
            raise ResourceUnavailable('ERROR: the GPU preflight found impossible requests '
                             '(above); nothing was applied.')
        if args.autoscaling and args.gpu_autoscaling:
            # Partitioned nodes never autoscale (fixed scale), so they contribute
            # their fixed replica count to the ceiling, not max_replicas.
            ceiling = sum((s.nb_tasks if _is_partitioned(s) else max(s.nb_tasks, args.max_replicas))
                          * s.gpu_count for s in gpu_specs)
            print(f'NOTE: --gpu-autoscaling can scale GPU demand up to {ceiling} device(s); '
                  f'replicas beyond allocatable capacity will wait Pending.', file = sys.stderr)
        elif args.autoscaling:
            print(f'NOTE: {len(gpu_specs)} GPU node(s) excluded from --autoscaling (each '
                  f'extra replica claims whole GPUs); pass --gpu-autoscaling to include them.',
                  file = sys.stderr)
        for problem in problems:
            print(f'WARNING: {problem}', file = sys.stderr)
        if problems and args.strict_preflight:
            raise ResourceUnavailable('ERROR: --strict-preflight set and the GPU preflight found '
                             'problems (above); nothing was applied.')
        # A declared rollout policy is admitted against the pool's spare capacity
        # (ALLOC-029/RUN-029): surge with nothing free would wait forever behind
        # the old replica, so it is refused here rather than applied.
        rollout = rollout_problems(args.rollout_policy, specs, flow_type, free_gpu_devices_observed(args.kubectl))
        if rollout and args.rollout_policy == 'surge':
            raise ResourceUnavailable('ERROR: ' + rollout[0] + '; nothing was applied.')
        for problem in rollout:
            print(f'WARNING: {problem}', file = sys.stderr)
        # gpu_memory_gib drives the mix strategy's MIG slice choice; every other
        # mode grants whole devices, so a declared demand deserves a heads-up
        # rather than silence (a mix-authored flow must still deploy anywhere).
        memory_nodes = [s.name for s in gpu_specs if s.gpu_memory_gib is not None]
        if memory_nodes and args.gpu_mode != 'mix':
            print(f'NOTE: gpu_memory_gib on {", ".join(sorted(memory_nodes))} is unused under '
                  f'--gpu-mode {args.gpu_mode} — each replica gets a whole device. Deploy with '
                  f'--gpu-mode mix to pack these nodes onto MIG slices.', file = sys.stderr)

    # 4b. Composition admission: does the broker and store this flow will run on
    # provide what every channel asks for? Binding with an explicit
    # --require-profile (or the RFC 0006 switch), advisory otherwise — see
    # deploy.admission. An auto-provisioned broker or store is judged by its
    # declared profile; a bring-your-own one is read back live here, before any
    # infrastructure exists (a short probe — what it cannot read stays Unknown,
    # never assumed). An explicit request the flow type's own streams cannot
    # carry is refused first, with nothing to tear down.
    explicit_profiles = parse_profile_requests(args.require_profile, specs)
    verify_topology_shape(flow_type, flow_id, run_id, explicit_profiles)
    store_configured = args.blob_redis_url is not None or args.nats is None
    from .infra import adopt_profiles, ensure_infra, ensure_namespace, reused_infra, teardown_infra, wait_infra_ready
    judged_broker : BrokerProfile | None = broker_profile
    judged_redis : RedisProfile | None = redis_profile
    reused : list[str] = []
    if args.nats is None:
        # The namespace may already run a broker and store (an earlier deploy
        # with --keep-infra, scripts/k3s-test-up.sh); ensure_infra reuses them,
        # so admission must judge what is there — the profile their creator
        # recorded on the Service — not what this deploy would have rendered. A
        # component with no record is unread here (the provision Job reads it
        # back in-cluster before it creates a stream).
        reuse = reused_infra(args.kubectl, args.namespace, need_redis = args.blob_redis_url is None)
        judged_broker, judged_redis = adopt_profiles(reuse, args.broker_profile, broker_profile,
                                                     redis_profile, args.namespace)
        reused = reuse.components()
        if judged_broker is not None and reuse.nats is not None:
            broker_profile = judged_broker      # stream replicas and the teardown hint follow the real broker
    if args.nats is not None:
        messaging_caps = jetstream_capabilities_observed(
            args.nats, stream_names = run_stream_names(flow_id, run_id, specs))
    else:
        messaging_caps = jetstream_capabilities(
            judged_broker, unread = f'the nats Service in namespace {args.namespace} records no profile '
                                    f'(created by hand or by an older videoflow); the provision Job reads '
                                    f'it back in-cluster')
    payload_caps = (None if not store_configured
                    else redis_payload_capabilities_observed(args.blob_redis_url)
                    if args.blob_redis_url is not None
                    else redis_payload_capabilities(
                        judged_redis, unread = f'the redis Service in namespace {args.namespace} records no '
                                               f'profile (created by hand or by an older videoflow); the '
                                               f'provision Job reads it back in-cluster'))
    verify_graph_size(specs, flow_id, run_id, messaging_caps)
    admit(requirements_for(flow_type, specs, explicit_profiles, declared), messaging_caps, payload_caps,
          payload_refs_in_use = store_configured, enforce = enforce_admission(explicit_profiles),
          unknown_is_fatal = unknown_admission(explicit_profiles), where = 'deploy',
          execution = kubernetes_execution_capabilities(bool(args.autoscaling)))
    profile_requests = requests_env(explicit_profiles)

    # 5. Broker infra: bring-your-own via --nats, else auto-provision dev NATS
    # (+ Redis for the blob store) in the namespace, owning only what we created.
    nats_url = args.nats
    blob_redis_url = args.blob_redis_url
    created = []
    if nats_url is None:
        try:
            ensure_namespace(args.kubectl, args.namespace)
            urls, created = ensure_infra(args.kubectl, args.namespace,
                                         need_redis = blob_redis_url is None,
                                         profile = broker_profile, redis_profile = redis_profile)
            wait_infra_ready(args.kubectl, args.namespace, created, profile = broker_profile)
        except RuntimeError as e:
            raise ClusterError(str(e)) from e
        nats_url = urls['nats']
        blob_redis_url = blob_redis_url or urls['redis']
        if created:
            print(f'Provisioned {broker_profile.name} {" + ".join(created)} in namespace {args.namespace}.')
        if reused:
            print(f'Reusing the {" + ".join(reused)} namespace {args.namespace} already runs'
                  + (f' ({judged_broker.name} profile)' if judged_broker is not None and 'nats' in reused else '')
                  + '; not owned, not torn down.')

    keep_infra = args.keep or args.keep_infra
    teardown_cmd = (f'  videoflow teardown --flow-id {flow_id} --run-id {run_id} '
                    f'--nats {nats_url} --namespace {args.namespace}'
                    + (' --infra' if created and not keep_infra else '')
                    # teardown must know the infra is a StatefulSet to delete it; the
                    # Service records the profile, and the printed flag makes the
                    # command self-contained for a cluster where that record is gone.
                    + (f' --broker-profile {broker_profile.name}'
                       if created and not keep_infra and broker_profile.stateful else '')
                    # The run's GPU mode is not recoverable from the cluster, so the
                    # printed command carries it: teardown needs it to call the
                    # strategy's cleanup() and undo whatever prepare() set up.
                    + (f' --gpu-mode {args.gpu_mode}' if gpu_specs else ''))

    from ..engines.kubernetes import KubernetesExecutionEngine
    engine_options : dict[str, Any] = {}
    if args.priority_class:
        # Passed only when set: the engine forwards it to render_manifests as
        # `priority_class`, and a deploy that never asked for a priority must not
        # depend on the keyword at all.
        engine_options['priority_class'] = args.priority_class
    engine_options.update(_placement_options(args))
    if args.single_run:
        engine_options['single_run'] = True
    engine = KubernetesExecutionEngine(
        nats_url = nats_url, namespace = args.namespace, default_image = image,
        image_overrides = overrides, blob_redis_url = blob_redis_url,
        blob_ttl_seconds = args.blob_ttl_seconds, specs = specs,
        kubectl = args.kubectl, envelope_version = args.envelope_version,
        provision_image = args.provision_image,
        autoscaling = args.autoscaling, max_replicas = args.max_replicas,
        mounts = mounts, gpu_runtime_class = gpu_runtime_class,
        gpu_mode = args.gpu_mode, gpu_resource_name = args.gpu_resource_name,
        gpu_autoscaling = args.gpu_autoscaling,
        image_pull_policy = args.image_pull_policy,
        profile_requests = profile_requests,
        # Only an auto-provisioned broker has a profile to size streams by; a
        # bring-your-own broker keeps the server default.
        stream_replicas = broker_profile.jetstream_replicas if args.nats is None else 1,
        **engine_options,
    )
    # Cluster setup the GPU mode needs for this run (no-op for the built-in modes;
    # the hook exists for strategies that retune the device plugin per run).
    gpu_strategy = None
    if gpu_specs:
        gpu_strategy = get_gpu_mode(args.gpu_mode)
        _gpu_prepare(gpu_strategy, demand, args.kubectl, flow_id)
    try:
        engine.allocate_and_run_tasks(None, flow_id, flow_type, run_id)
    except BaseException as e:
        # BaseException, not (RuntimeError, ValueError): the flow never got
        # established, so prepare()'s reconfiguration has no user and must be
        # undone — including on a Ctrl-C during the provision wait, which can
        # block for minutes and is the likeliest interruption point.
        _gpu_cleanup(gpu_strategy, args.kubectl, flow_id)
        if isinstance(e, (RuntimeError, ValueError)):
            raise ClusterError(str(e)) from e
        raise
    print(f'Flow {flow_id} run {run_id} applied to namespace {args.namespace}.')

    if flow_type != BATCH:
        # A REALTIME deploy returns immediately, so a broken node would fail
        # silently (producers keep publishing, frames evicted, downstream output
        # never appears). One bounded rollout check turns an unschedulable or
        # crash-looping pod into a log dump and a non-zero exit.
        report = engine.rollout_report()
        for problem in report.warnings:
            print(f'WARNING: {problem}', file = sys.stderr)
        # Deliberately no gpu_strategy.cleanup() here (on either path): the flow
        # keeps running after this returns — including on failure, where the
        # crash-looping pod is the debugging evidence. Any cluster state
        # prepare() set up must persist for the flow's lifetime. Teardown is the
        # end of a REALTIME run, so the mode is carried in the printed command
        # for `teardown` to undo it there.
        if report.failing:
            engine.dump_failed_logs(sorted({node for node, _ in report.failing}))
            print('Failing pods were left running for inspection. Tear the flow down with:')
            print(teardown_cmd)
            raise FlowFailed('Flow applied but not healthy: '
                             + '; '.join(detail for _, detail in report.failing))
        print('REALTIME flow is running. Tear it down with:')
        print(teardown_cmd)
        return

    # BATCH: run to completion, then clean everything up — on success, failure,
    # stall, or Ctrl-C (the finally block always runs unless --keep is set).
    failed = []
    stall = None
    try:
        failed = engine.wait_for_completion()
    except KeyboardInterrupt:
        print('\nInterrupted; cleaning up...', file = sys.stderr)
    except RuntimeError as e:
        # The unschedulable-pod watchdog: the flow can never finish, so clean up
        # instead of hanging (the historical behavior) — see wait_for_completion.
        stall = str(e)
    finally:
        if failed:
            engine.dump_failed_logs(failed)
        if args.keep:
            print('--keep set: leaving resources up. Tear them down with:')
            print(teardown_cmd)
        else:
            engine.teardown()
            if created and not keep_infra:
                teardown_infra(args.kubectl, args.namespace, created, profile = broker_profile)
            print('Cleaned up all resources.')
        # Restore whatever the GPU mode changed once the run reaches here —
        # succeeded, failed, stalled or interrupted. Runs even under --keep: the
        # workloads may be worth keeping, a retuned cluster is not. Failures
        # *before* this try block are covered by the rollbacks around
        # _gpu_prepare and allocate_and_run_tasks above.
        _gpu_cleanup(gpu_strategy, args.kubectl, flow_id)
    if stall:
        raise FlowStalled(f'Flow aborted: {stall}')
    if failed:
        raise FlowFailed(f'Flow failed: {", ".join(failed)}')
    print(f'Flow {flow_id} completed.')

def _compile_in_image(args : argparse.Namespace, graph_target : str, graph_dir : str,
                      image : str, container_mounts : list, gpus : bool,
                      config_path : str | None = None) -> tuple:
    '''
    ``(flow_id, flow_type, specs, declared requirements)`` compiled inside the
    solution image: the graph dir is mounted at the same absolute path, so config
    paths resolve identically, and the specs round-trip as JSON — the same format
    as the specs ConfigMap.
    '''
    compile_cmd = ['python', '-m', 'videoflow.compile', graph_target]
    # run-local's parser has no --envelope-version; the build default applies.
    envelope_version = getattr(args, 'envelope_version', None)
    if envelope_version is not None:
        compile_cmd += ['--envelope-version', str(envelope_version)]
    try:
        out = run_in_image(image, compile_cmd, mounts = container_mounts,
                           workdir = graph_dir, gpus = gpus, capture = True,
                           env = _solution_env(config_path))
    except RuntimeError as e:
        raise ResourceUnavailable(str(e)) from e
    if out is None:
        # capture = True above, so stdout is always captured; a None here would
        # mean run_in_image stopped capturing, and json.loads would fail with a
        # traceback instead of a message the operator can act on.
        raise ResourceUnavailable(f'Compiling the graph in {image} produced no output. '
                                  'Re-run with --no-build and an importable graph, or rebuild the image.')
    return specs_from_document(out) + (requirements_from_document(out),)

def _compile_graph(args : argparse.Namespace, graph_target : str, graph_dir : str,
                   image : str | None, container_mounts : list, gpus : bool,
                   config_path : str | None = None) -> tuple:
    '''
    ``(flow_id, flow_type, specs, declared requirements)`` — via a local import when
    the graph's deps are installed on the host (cheap), else compiled inside the
    solution image (``_compile_in_image``).
    '''
    try:
        flow = _load_flow(graph_target)
    except ImportError as e:
        if image is None:
            raise ConfigError(
                f'Cannot import the graph on this machine ({e}) and there is no '
                f'solution image to compile it in — pass --image or drop --no-build.') from e
        return _compile_in_image(args, graph_target, graph_dir, image, container_mounts, gpus, config_path)

    # Building the Flow already ran GraphEngine's cycle/uniqueness validation.
    try:
        specs = compile_flow(flow, envelope_version = args.envelope_version)
    except ValueError as e:
        raise ConfigError(str(e)) from e
    return flow.flow_id, flow.flow_type, specs, declared_requirements(flow)

def _render_manifests_to_disk(args : argparse.Namespace, image : str | None, flow_id : str,
                              flow_type : str, specs : list, run_id : str, overrides : dict,
                              mounts : list) -> None:
    '''
    --dry-run (stdout) / --render-only (files) — the manifest-generation escape hatch.

    ``image`` is the *resolved* default image — ``--image`` when given, otherwise
    whatever ``autobuild`` produced — and not ``args.image``. Reading the flag here
    instead made these two paths disagree: a plain
    ``videoflow deploy graph.py --render-only`` built the image, then rendered
    manifests that referenced none and died with "node 'x' has no container image",
    while the same command without ``--render-only`` deployed fine.
    '''
    from .infra import infra_urls, nats_manifests, redis_manifests
    from .manifests import dump_manifests, render_manifests

    # When the broker would be auto-provisioned, emit its manifests too so the
    # rendered output is a complete, appliable deployment.
    infra_manifests = []
    nats_url = args.nats
    blob_redis_url = args.blob_redis_url
    stream_replicas = 1
    if nats_url is None:
        broker_profile, redis_profile = broker_profiles(
            args.broker_profile or 'dev', replicas = args.broker_replicas,
            storage_class = args.broker_storage_class, priority_class = args.priority_class)
        stream_replicas = broker_profile.jetstream_replicas
        urls = infra_urls(args.namespace)
        nats_url = urls['nats']
        infra_manifests += nats_manifests(args.namespace, broker_profile)
        if blob_redis_url is None:
            blob_redis_url = urls['redis']
            infra_manifests += redis_manifests(args.namespace, redis_profile)

    try:
        manifests = render_manifests(
            specs, flow_id, flow_type, nats_url, run_id,
            namespace = args.namespace, default_image = image,
            image_overrides = overrides, blob_redis_url = blob_redis_url,
            blob_ttl_seconds = args.blob_ttl_seconds,
            autoscaling = args.autoscaling, max_replicas = args.max_replicas,
            envelope_version = args.envelope_version,
            provision_image = args.provision_image, mounts = mounts,
            # A render never touches the cluster, so no RuntimeClass is detected
            # here; `none` still means none.
            gpu_runtime_class = _normalize_runtime_class(args.gpu_runtime_class), gpu_mode = args.gpu_mode,
            gpu_resource_name = args.gpu_resource_name,
            gpu_autoscaling = args.gpu_autoscaling,
            image_pull_policy = args.image_pull_policy,
            priority_class = args.priority_class,
            stream_replicas = stream_replicas,
            **_placement_options(args),
        )
    except ValueError as e:
        raise ConfigError(str(e)) from e
    manifests = infra_manifests + manifests

    if args.dry_run:
        sys.stdout.write(dump_manifests(manifests))
        return

    os.makedirs(args.output, exist_ok = True)
    for m in manifests:
        fname = f"{m['kind'].lower()}-{m['metadata']['name']}.yaml"
        with open(os.path.join(args.output, fname), 'w') as f:
            f.write(dump_manifests([m]))

    kustomization = {
        'apiVersion': 'kustomize.config.k8s.io/v1beta1',
        'kind': 'Kustomization',
        'namespace': args.namespace,
        'resources': [
            f"{m['kind'].lower()}-{m['metadata']['name']}.yaml" for m in manifests
        ],
    }
    with open(os.path.join(args.output, 'kustomization.yaml'), 'w') as f:
        f.write(yaml.dump(kustomization, default_flow_style = False, sort_keys = False))

    print(f'Wrote {len(manifests)} manifests + kustomization.yaml to {args.output}')
    print(f'Apply with:  kubectl apply -k {args.output}')
    print(f'Flow id: {flow_id}   Run id: {run_id}')

def _cmd_component_validate(args : argparse.Namespace) -> None:
    ok = True
    for path in args.path:
        try:
            desc = load_descriptor(path)
        except Exception as e:
            print(f'INVALID  {path}: {e}')
            ok = False
            continue
        images = ', '.join(f'{k}={v}' for k, v in sorted(desc.images.items()))
        print(f'OK       {desc.name} v{desc.version}  role={desc.role}  protocol={desc.protocol}  '
            f'device={desc.device}  images[{images}]')
    if not ok:
        raise ConfigError('One or more component descriptors are invalid (see above).',
                        remedy = 'Fix the reported problems and re-run `videoflow component validate`.')

def _cmd_component_push(args : argparse.Namespace) -> None:
    try:
        target = push_component(args.path, args.ref)
    except Exception as e:
        raise ClusterError(f'push failed: {e}') from e
    print(f'Pushed component descriptor {args.path} -> oci://{target}')

def _cmd_component_pull(args : argparse.Namespace) -> None:
    try:
        path = pull_component(args.ref, force = args.force, verify = args.verify,
                            cosign_args = args.cosign_arg or None)
    except Exception as e:
        raise ClusterError(f'pull failed: {e}') from e
    print(f'Resolved {args.ref} -> {path}')

def _cmd_component_inspect(args : argparse.Namespace) -> None:
    try:
        d = inspect_component(args.ref, verify = args.verify)
    except Exception as e:
        raise ClusterError(f'inspect failed: {e}') from e
    images = ', '.join(f'{k}={v}' for k, v in sorted(d.images.items()))
    kind = 'python' if not d.is_native else 'native'
    print(f'{d.name} v{d.version}  ({kind}, role={d.role}, protocol={d.protocol})')
    print(f'  device: {d.device}')
    print(f'  images: {images}')
    if d.python_class:
        print(f'  pythonClass: {d.python_class}')
    if d.params_schema.get('properties'):
        print(f'  params: {", ".join(sorted(d.params_schema["properties"]))}')

def _needs_local_build(flow : Flow) -> bool:
    '''
    Whether ``run-local`` must build the solution image for this flow.

    True only when some node is a native component that declares no ``image=`` and no
    ``runtime.localCommand`` — the one case ``LocalProcessEngine`` runs via
    ``docker run``. A pure-Python flow spawns host subprocesses and needs no image, so
    building one (potentially a multi-GB CUDA image) would cost minutes and buy
    nothing.

    '''
    # optional dep: the local engine imports nats at module scope
    from ..engines.local import needs_container_image
    specs = specs_from_tasks_data(flow.tasks_data())
    return any(needs_container_image(s) and not s.image for s in specs)

def _raise_interrupt(signum : int, frame : Any) -> None:
    raise KeyboardInterrupt


def _install_sigterm_as_interrupt() -> Any:
    '''Route SIGTERM through the Ctrl-C path; returns the previous handler, or None off the main thread.'''
    try:
        return signal.signal(signal.SIGTERM, _raise_interrupt)
    except ValueError:
        return None


def _cmd_run_local(args : argparse.Namespace) -> None:
    graph_path, graph_dir, graph_target, ref_context = _graph_location(args.graph)

    # 0. Solution conventions: ensure a config (interactive Q&A over the solution's
    # config.template.yaml when none exists) and collect its x-mounts — locally
    # they matter only for workers that run inside the solution image.
    from .manifests import parse_mounts
    from .solution import (
        ensure_config,
        find_prepare,
        prepare_command,
        resolve_gpu,
        resolve_mounts,
        run_prepare_local,
        split_mount_specs,
    )
    interactive = not args.non_interactive and sys.stdin.isatty()
    config_path = ensure_config(graph_dir, args.config, interactive)
    _export_solution_config(config_path)
    template, config = _solution_template(graph_dir, config_path)
    try:
        host_specs, claim_specs = split_mount_specs(
            resolve_mounts(template, config, graph_dir, home = args.mount_home)
            if template and config_path else [])
        worker_mounts = parse_mounts((args.mount or []) + host_specs)
        declared_gpu = resolve_gpu(template, config)
    except ValueError as e:
        raise ConfigError(str(e)) from e

    # 1. Where the workers run: on this host when the graph's dependencies import
    # here (the common case), inside the solution image — the same one deploy
    # builds — when they do not, or on --in-image. The probe only runs when an
    # image could exist at all, so a plain graph without a Dockerfile keeps the
    # host path and is loaded exactly once, after prepare.
    could_run_in_image = (args.in_image or args.image is not None
                          or find_dockerfile(graph_dir, needs_gpu = False) is not None)
    import_error = None if args.in_image or not could_run_in_image else _probe_host_import(graph_target)
    in_image = args.in_image or import_error is not None
    gpus = docker_gpus_available()
    image = args.image
    flow : Flow | None = None
    if in_image:
        if image is None and not args.no_build:
            needs_gpu, note = resolve_needs_gpu(graph_dir, declared_gpu, None)
            if note:
                print(f'NOTE: {note}', file = sys.stderr)
            try:
                image = autobuild(graph_dir, needs_gpu = needs_gpu,
                              context_override = args.build_context or ref_context)
            except RuntimeError as e:
                raise ResourceUnavailable(str(e)) from e
        if image is None:
            why = f' ({import_error})' if import_error is not None else ''
            raise ConfigError(
                f'Cannot import the graph on this machine{why} and there is no solution image to run it in.',
                remedy = 'Pass --image <ref>, or drop --no-build so the solution\'s Dockerfile is built.')
        # The prepare/compile containers see the solution directory itself (config,
        # prepare.py, work dir) at its host path, plus whatever the workers mount.
        container_mounts = parse_mounts([graph_dir]) + worker_mounts + _config_mounts(graph_dir, config_path)
        if claim_specs:
            print(f'NOTE: x-mounts claim entries ({", ".join(claim_specs)}) name data inside a cluster; '
                  f'a local run cannot mount them.', file = sys.stderr)
        # 2. Prepare hook, inside the image: its dependencies live there, not here.
        if find_prepare(graph_dir) is not None and not args.no_prepare:
            try:
                run_in_image(image, prepare_command(config_path), mounts = container_mounts,
                             workdir = graph_dir, gpus = gpus, interactive = interactive,
                             env = _solution_env(config_path))
            except RuntimeError as e:
                raise ResourceUnavailable(f'prepare.py failed: {e}') from e
        # 3. Compile inside the image; the specs are all the engine needs.
        flow_id, flow_type, local_specs, declared = _compile_in_image(
            args, graph_target, graph_dir, image, container_mounts, gpus, config_path)
    elif not args.no_prepare:
        # 2. Prepare hook, on this host (the workers are local processes too). Runs
        # before the graph is loaded, because the factory reads the hook's outputs.
        try:
            run_prepare_local(graph_dir, config_path)
        except subprocess.CalledProcessError as e:
            raise ResourceUnavailable(f'prepare.py failed: {e}') from e

    # Warn about solution inputs that don't exist yet (a bad path is worth
    # catching before spawning N processes).
    _warn_missing_solution_inputs(graph_dir, config_path)

    # 3. Broker: bring-your-own via --nats, else reuse whatever already listens on
    # localhost and start dev containers for whatever doesn't.
    from .localinfra import DEFAULT_NATS_URL, ensure_local_infra, teardown_local_infra, wait_local_infra_ready
    nats_url = args.nats
    blob_redis_url = args.blob_redis_url or os.environ.get('VIDEOFLOW_BLOB_REDIS_URL')
    # A store the operator brought is read back for admission; the dev one is
    # judged by the shape localinfra starts it with.
    byo_redis = blob_redis_url is not None
    created: list[str] = []
    if nats_url is None:
        if args.no_infra:
            nats_url = DEFAULT_NATS_URL
        else:
            try:
                urls, created = ensure_local_infra(
                    need_redis = blob_redis_url is None and not args.no_redis)
                wait_local_infra_ready(created, urls)
            except RuntimeError as e:
                raise ClusterError(str(e)) from e
            nats_url = urls['nats']
            blob_redis_url = blob_redis_url or urls['redis']
            if created:
                print(f'Started dev {" + ".join(created)} (docker).')

    # 4. Load + run. load_flow puts the graph dir on sys.path, which the engine
    # re-exports as PYTHONPATH so the workers can import its sibling modules. A
    # graph compiled inside its image was loaded there instead; its specs stand
    # in for the Flow from here on.
    if not in_image:
        flow = _load_flow(graph_target)
        local_specs = compile_flow(flow)
        flow_id, flow_type, declared = flow.flow_id, flow.flow_type, declared_requirements(flow)
    from ..engines.local import LocalProcessEngine  # optional dep: the local engine imports nats
    # Composition admission: a container this run just started is judged by the
    # shape localinfra starts it with; whatever already answered on the dev port
    # (a compose server, a leftover --keep-infra container), a bring-your-own
    # broker (--nats, or whatever listens under --no-infra) and a bring-your-own
    # store are read back live — see deploy.admission. Binding for a definite
    # incompatibility; an unobserved guarantee binds only under --require-profile.
    explicit_profiles = parse_profile_requests(args.require_profile, local_specs)
    verify_topology_shape(flow_type, flow_id, args.run_id or 'run', explicit_profiles)
    dev_messaging_caps, dev_payload_caps = local_dev_capabilities()
    if args.nats is None and not args.no_infra and 'nats' in created:
        messaging_caps = dev_messaging_caps
    else:
        messaging_caps = jetstream_capabilities_observed(nats_url)
    payload_caps = (None if blob_redis_url is None
                    else dev_payload_caps if not byo_redis and 'redis' in created
                    else redis_payload_capabilities_observed(blob_redis_url))
    try:
        verify_graph_size(local_specs, flow_id, args.run_id or 'run', messaging_caps)
        admit(requirements_for(flow_type, local_specs, explicit_profiles, declared),
              messaging_caps, payload_caps,
              payload_refs_in_use = blob_redis_url is not None, enforce = enforce_admission(explicit_profiles),
              unknown_is_fatal = unknown_admission(explicit_profiles), where = 'run-local',
              execution = local_execution_capabilities())
    except VideoflowError:
        # A refused flow leaves nothing behind: the containers this run started
        # for it go too (--keep-infra keeps them, as it would after a run).
        if created and not args.keep_infra:
            teardown_local_infra(created)
        raise
    # 4a. Image, but only if some node actually needs one. A pure-Python flow on
    # this host runs as subprocesses, so building the solution image would cost
    # minutes and buy nothing; a native component without a localCommand is
    # docker-run and can't start without it. --image wins over building,
    # mirroring deploy. (In-image runs built or were given theirs above.)
    if image is None and not args.no_build and flow is not None and _needs_local_build(flow):
        needs_gpu, note = resolve_needs_gpu(graph_dir, declared_gpu, local_specs)
        if note:
            print(f'NOTE: {note}', file = sys.stderr)
        try:
            image = autobuild(graph_dir, needs_gpu = needs_gpu,
                              context_override = args.build_context or ref_context)
        except RuntimeError as e:
            raise ResourceUnavailable(str(e)) from e
    # Restart failed workers by default, exactly as the cluster would — with a
    # compressed backoff so a genuinely broken node still surfaces in seconds.
    supervision = (SupervisionPolicy.disabled() if args.no_restart
                else SupervisionPolicy.local())
    engine = LocalProcessEngine(nats_url = nats_url, blob_redis_url = blob_redis_url,
                                local_docker_nats_url = args.local_docker_nats_url,
                                default_image = image,
                                blob_ttl_seconds = args.blob_ttl_seconds,
                                supervision = supervision,
                                profile_requests = requests_env(explicit_profiles),
                                gpu_policy = args.gpu_policy,
                                specs = local_specs if in_image else None,
                                worker_image = image if in_image else None,
                                worker_mounts = worker_mounts if in_image else None,
                                docker_gpus = gpus)
    runner : Any = _SpecsFlow(flow_id, flow_type) if in_image else flow
    # A SIGTERM — `kill`, a CI cancel, a closing multiplexer — stops the flow the
    # way Ctrl-C does: the workers are quiesced and the containers removed,
    # instead of the process dying and leaving them orphaned.
    previous_term = _install_sigterm_as_interrupt()
    try:
        try:
            runner.run(engine, run_id = args.run_id)
        except (RuntimeError, ValueError) as e:
            # Broker unreachable / incompatible wire settings: report the message,
            # not a traceback through the engine internals.
            raise BrokerUnavailable(str(e)) from e
        where = f'in {image}' if in_image else 'locally'
        print(f'Flow {runner.flow_id} run {runner.run_id} running {where} against {nats_url}. '
              f'Ctrl-C to stop.')
        try:
            runner.join()
        except KeyboardInterrupt:
            print('\nInterrupted; stopping...', file = sys.stderr)
            runner.stop()
    finally:
        if previous_term is not None:
            signal.signal(signal.SIGTERM, previous_term)
        if in_image:
            engine.cleanup_containers()
        if created and not args.keep_infra:
            teardown_local_infra(created)
        elif created:
            names = ' '.join(f'videoflow-{c}' for c in created)
            print(f'--keep-infra set: left {" + ".join(created)} running (reused next run; '
                  f'remove with `docker rm -f {names}`).')

    failed = sorted({name for name, _replica, _code in engine.failures()})
    if failed:
        engine.report_failures()
        raise FlowFailed(f'Flow failed: {", ".join(failed)}')
    print(f'Flow {runner.flow_id} completed.')

def _warn_missing_solution_inputs(graph_dir : str, config_path : str | None) -> None:
    '''
    Warns when a path the solution declares in ``x-mounts`` doesn't exist. Locally
    nothing is mounted, so this is purely an early check — and only a warning,
    since an output directory legitimately may not exist yet.
    '''
    from .solution import find_template, load_template, resolve_mounts

    template_path = find_template(graph_dir)
    if not (template_path and config_path):
        return
    with open(config_path) as f:
        config = yaml.safe_load(f)
    from .solution import split_mount_specs
    # Only host paths can be checked here; a claim mount names data inside the
    # cluster, which a local run neither needs nor can see.
    host_specs, _claim_specs = split_mount_specs(
        resolve_mounts(load_template(template_path), config, graph_dir))
    for spec in host_specs:
        read_only = spec.endswith(':ro')
        path = (spec[:-3] if read_only else spec).split(':', 1)[0]
        if read_only and not os.path.exists(path):
            print(f'WARNING: solution input {path} does not exist.', file = sys.stderr)

def _load_solution_flow(args : argparse.Namespace) -> Flow:
    '''
    The flow for a command that only inspects the graph (``explain``,
    ``provision``): the solution config convention applies exactly as in
    ``deploy`` — ``--config``, else ``config.yaml`` next to the graph, else the
    template's Q&A (or, non-interactively, a message listing the inputs) — so a
    solution whose ``build_flow`` reads its config never fails with a bare
    ``FileNotFoundError`` before the first deploy has written one.
    '''
    from .solution import ensure_config
    graph_path, graph_dir, graph_target, _ = _graph_location(args.graph)
    interactive = not args.non_interactive and sys.stdin.isatty()
    config_path = ensure_config(graph_dir, args.config, interactive)
    _export_solution_config(config_path)
    return _load_flow(graph_target)

def _cmd_explain(args : argparse.Namespace) -> None:
    # optional dep: topology imports nats at module scope
    from ..messaging.topology import dlq_stream_name, subject_for

    flow = _load_solution_flow(args)
    run_id = args.run_id or '<run-id>'
    # Describe the graph without enforcing wire compatibility (that is a deploy-time
    # concern; explain must work for any flow, including remote-on-default-wire).
    specs = specs_from_tasks_data(flow.tasks_data())
    lines = [f'Flow: {flow.flow_id}   type={flow.flow_type}   run={run_id}',
            f'Nodes ({len(specs)}):']
    for s in specs:
        bits = [f'replicas={s.nb_tasks}']
        if s.device_type == 'gpu':
            bits.append('device=gpu')
            if s.gpu_count != 1:
                bits.append(f'gpu_count={s.gpu_count}')
            if s.gpu_memory_gib is not None:
                bits.append(f'gpu_memory_gib={s.gpu_memory_gib}')
            if s.gpu_resource_name:
                bits.append(f'gpu_resource={s.gpu_resource_name}')
        if s.partition_by:
            bits.append(f'partition_by={s.partition_by}')
        if s.join_policy:
            bits.append(f"join={s.join_policy.get('missing')}")
        image = s.image or '«--image default»'
        kind = f'{s.kind}, remote' if s.is_remote else s.kind
        lines.append(f'  {s.name}  [{kind}]  image={image}  ' + '  '.join(bits))
        if s.is_remote:
            lines.append(f'      component: {s.component_ref}  (protocol v{s.protocol_version})')
        lines.append(f'      subject: {subject_for(flow.flow_id, run_id, s.name)}')
        if s.parents:
            lines.append(f'      from: {", ".join(s.parents)}')
    # GPU demand summary: in the default exclusive mode every replica claims its own
    # whole devices, so this is the allocatable capacity the cluster must have.
    # Same helper as deploy's preflight (and the same --gpu-resource-name flag), so
    # what explain prints is exactly what deploy will request.
    gpu_specs = [s for s in specs if s.device_type == 'gpu']
    if gpu_specs:
        from .manifests import gpu_demand, gpu_max_per_pod
        default_resource = args.gpu_resource_name
        demand = gpu_demand(specs, default_resource = default_resource)
        lines.append('GPU demand (exclusive mode — whole devices per replica):')
        if any(s.gpu_memory_gib is not None for s in gpu_specs):
            lines.append('  note: nodes with gpu_memory_gib become solver-chosen MIG slices '
                         'under --gpu-mode mix (resolved against the cluster at deploy time)')
        for s in gpu_specs:
            resource = resolve_gpu_resource(s, default_resource)
            lines.append(f'  {s.name}: {s.nb_tasks} x {s.gpu_count} {resource}')
        for resource, units in sorted(demand.items()):
            lines.append(f'  total: {units} x {resource} — the cluster needs at least this '
                         f'allocatable (or enable device-plugin time-slicing on a dev cluster)')
        # One-node bound (RFC 0003): all of one replica's gpu_count devices must sit
        # on a single host, so the totals above can be satisfiable while the biggest
        # pod never schedules. Noise at 1 device — only printed for multi-GPU pods.
        for resource, per_pod in sorted(gpu_max_per_pod(specs, default_resource = default_resource).items()):
            if per_pod > 1:
                lines.append(f'  largest single pod: {per_pod} x {resource} — all {per_pod} devices '
                             f'must sit on one cluster node; deploy\'s preflight checks the largest '
                             f'node\'s allocatable, not just the total')
    # Flow-scoped, so it outlives any single run's teardown.
    lines.append(f'DLQ stream: {dlq_stream_name(flow.flow_id)} '
                f'(read it with `videoflow dlq ls --flow-id {flow.flow_id}`)')
    print('\n'.join(lines))

def _cmd_provision(args : argparse.Namespace) -> None:
    # optional dep: topology imports nats at module scope
    from ..messaging.topology import provision_flow_sync

    flow = _load_solution_flow(args)
    run_id = args.run_id or uuid.uuid4().hex[:12]
    # Provisioning only needs stream/durable names (language-neutral); no wire check.
    specs = specs_from_tasks_data(flow.tasks_data())
    provision_flow_sync(args.nats, specs, flow.flow_id, run_id, flow.flow_type)
    print(f'Provisioned {len(specs)} node streams for flow {flow.flow_id} run {run_id}')

def _cmd_teardown(args : argparse.Namespace) -> None:
    import nats  # optional dep (distributed/deploy extras)

    if not args.nats:
        raise ConfigError('teardown needs the broker URL the run used.',
                          remedy = 'Pass --nats <url> (deploy prints it in its teardown hint), or name '
                                   '`nats:` in the cluster profile.')

    # optional dep: topology imports nats at module scope
    from ..messaging.topology import control_subject_for, delete_run_streams

    async def _quiet(_e : Exception) -> None:
        pass  # swallow the client's connect-retry error logging (we handle failure)

    incomplete : list[str] = []

    async def _go() -> None:
        nc = await nats.connect(args.nats, allow_reconnect = False, connect_timeout = 3,
                                max_reconnect_attempts = 0, error_cb = _quiet)
        try:
            await nc.publish(control_subject_for(args.flow_id, args.run_id), b'stop')
            await nc.flush()
            observation = await delete_run_streams(nc, args.flow_id, args.run_id)
            if not observation.complete:
                # A listing that failed or a delete that did not land is not a
                # finished cleanup: say what remains, finish the other steps, and
                # exit non-zero so a retry is the obvious next move.
                incomplete.append(f'{observation.reason or "some streams remain"}; removed: '
                                  f'{", ".join(observation.removed) or "none"}; remaining: '
                                  f'{", ".join(observation.remaining) or "unknown"}')
        finally:
            await nc.drain()

    async def _bounded() -> None:
        await asyncio.wait_for(_go(), timeout = 8)

    # Broker cleanup is best-effort: if the host can't reach --nats (e.g. an
    # in-cluster-only URL), still delete the k8s workloads below.
    try:
        asyncio.run(_bounded())
        if incomplete:
            print(f'WARNING: broker cleanup incomplete for flow {args.flow_id} run {args.run_id}: '
                  f'{incomplete[0]}', file = sys.stderr)
        else:
            print(f'Sent stop + deleted run streams for flow {args.flow_id} run {args.run_id}')
    except Exception as e:
        print(f'Broker teardown skipped (could not reach NATS at {args.nats}): {e}', file = sys.stderr)
    if args.namespace:
        from .manifests import delete_resources
        # Scope to this run (matches the run-id label the deploy stamps on every
        # resource); the broker phase above is already run-scoped.
        delete_resources(args.kubectl, args.namespace, args.flow_id, args.run_id)
        print(f'Deleted workloads in namespace {args.namespace} for flow {args.flow_id} run {args.run_id}')
    if args.infra:
        if not args.namespace:
            raise ConfigError('--infra requires --namespace.')
        from .infra import LABEL_PROFILE, NATS_SERVICE, service_labels, teardown_infra
        # The profile decides the kinds deleted (a durable NATS is a StatefulSet);
        # the persistent claims of a durable profile are kept, see deploy.infra.
        # The Service records the profile that rendered it; an explicit flag wins.
        name = args.broker_profile
        if name is None:
            recorded = (service_labels(args.kubectl, args.namespace, NATS_SERVICE) or {}).get(LABEL_PROFILE)
            name = recorded if recorded in BROKER_PROFILE_NAMES else 'dev'
        profile, _redis_profile = broker_profiles(name)
        teardown_infra(args.kubectl, args.namespace, ['nats', 'redis'], profile = profile)
        print(f'Deleted auto-provisioned {name} infra in namespace {args.namespace}.')
    # Undo any per-run cluster reconfiguration the GPU mode applied at deploy time.
    # This is the terminal hook for a REALTIME run, which stays up past deploy and
    # so is never cleaned up there. Built-in modes have a no-op cleanup().
    if args.gpu_mode:
        strategy : GpuStrategy | None
        try:
            strategy = get_gpu_mode(args.gpu_mode)
        except ValueError as e:
            # An unresolvable mode must not fail a teardown whose real work
            # already succeeded — the operator would read the traceback as
            # "teardown failed" and run it again. Typically the plugin that
            # registered the mode is not installed in *this* shell.
            print(f'WARNING: skipping GPU cleanup: {e}', file = sys.stderr)
            strategy = None
        _gpu_cleanup(strategy, args.kubectl, args.flow_id)
    if incomplete:
        # Every other step ran (they are the guarantee); the broker half is what a
        # retry must finish, so say so with the exit code and not only on stderr.
        raise BrokerUnavailable(
            f'Broker cleanup incomplete for flow {args.flow_id} run {args.run_id}: {incomplete[0]}',
            remedy = 'Re-run the same `videoflow teardown` command once the broker is reachable; it is idempotent.')

def _format_payload(message : Any) -> str:
    '''One-line human summary of a decoded payload for the debug inspector.'''
    if message is None:
        return 'None (EOS or empty)'
    if isinstance(message, np.ndarray):
        return f'ndarray shape={tuple(message.shape)} dtype={message.dtype}'
    # optional dep: serialization imports protobuf at module scope
    from ..wire.serialization import RawPayload
    if isinstance(message, RawPayload):
        return f'RawPayload type={message.payload_type} ({len(message.data)} bytes, opaque)'
    # optional dep: protobuf ships with the same extras as the serialization module above
    from google.protobuf.message import Message
    if isinstance(message, Message):
        return f'{message.DESCRIPTOR.full_name}: ' + str(message).replace('\n', ' ').strip()[:200]
    return repr(message)[:200]

def _print_decoded(buf : bytes, headers : dict | None = None) -> None:
    # optional dep: serialization imports protobuf at module scope
    from ..wire.serialization import decode_envelope
    d = decode_envelope(buf)
    if headers:
        interesting = {k: v for k, v in headers.items() if k.startswith('VF-') or k == 'Nats-Msg-Id'}
        if interesting:
            print('  headers: ' + '  '.join(f'{k}={v}' for k, v in interesting.items()))
    print(f"  {d['type']}  producer={d['producer_name']}  trace={d['trace_id']}  seq={d['seq']}  "
        f"replica={d['replica_id']}  event_ts={d['event_ts']}")
    if d['metadata']:
        print(f"  metadata: {d['metadata']}")
    print(f"  payload: {_format_payload(d['message'])}")

def _cmd_debug_decode(args : argparse.Namespace) -> None:
    if args.file:
        with open(args.file, 'rb') as f:
            buf = f.read()
        print(f'Envelope from {args.file} ({len(buf)} bytes):')
        _print_decoded(buf)
        return
    if not args.dlq:
        raise ConfigError('Provide a FILE of raw envelope bytes, or --dlq with --flow-id.',
                        remedy = 'For richer dead-letter inspection, use `videoflow dlq ls`.')
    if not args.flow_id:
        raise ConfigError('--dlq requires --flow-id.')
    _dlq_scan(args.nats, args.flow_id, args.run_id, None, args.limit, _print_dlq_entry)

# -- dead-letter queue -------------------------------------------------------
#
# The DLQ is only half useful if it can be read but not drained: a queue you
# cannot replay from is a graveyard. These four commands close the loop —
# find the failures, look at one, fix the bug, put the messages back.

def _dlq_fetch(nats_url : str, flow_id : str, run_id : str | None, node : str | None,
            limit : int) -> list:
    '''
    Reads up to ``limit`` dead letters without consuming them (an ephemeral,
    no-ack pull), newest-first order not guaranteed — JetStream replays a stream
    in publish order.

    - Returns:
        - a list of ``(subject, headers, payload bytes)``.

    - Raises:
        - BrokerUnavailable: the broker is unreachable or has no DLQ for this flow.
    '''
    import nats  # optional dep (distributed/deploy extras)

    # optional dep: topology imports nats at module scope
    from ..messaging.topology import dlq_stream_name, dlq_subject_filter

    stream = dlq_stream_name(flow_id)
    subject = dlq_subject_filter(flow_id, run_id, node)

    async def _go() -> list:
        nc = await nats.connect(nats_url)
        try:
            js = nc.jetstream()
            try:
                sub = await js.pull_subscribe(subject, stream = stream)
            except Exception as e:
                raise BrokerUnavailable(
                    f'No dead-letter stream {stream} on {nats_url} ({e}).',
                    remedy = ('Check --flow-id and --nats. A flow that has never '
                            'dead-lettered anything has no DLQ stream yet.')) from e
            out : list = []
            while len(out) < limit:
                try:
                    msgs = await sub.fetch(batch = min(20, limit - len(out)), timeout = 2.0)
                except (nats.errors.TimeoutError, TimeoutError):
                    break
                for msg in msgs:
                    out.append((msg.subject, dict(msg.headers or {}), msg.data))
            return out
        finally:
            # close(), not drain(): a graceful drain also drains the still-registered
            # JetStream pull subscription above and blocks until its timeout, printing
            # a DrainTimeoutError for a read that already succeeded. Same trap the
            # messenger's own close() documents.
            await nc.close()

    return asyncio.run(_go())

def _dlq_scan(nats_url : str, flow_id : str, run_id : str | None, node : str | None,
            limit : int, render : Any) -> None:
    entries = _dlq_fetch(nats_url, flow_id, run_id, node, limit)
    if not entries:
        print(f'No dead-lettered messages for flow {flow_id}'
            + (f' run {run_id}' if run_id else '') + '.')
        return
    for index, (subject, headers, data) in enumerate(entries, start = 1):
        render(index, subject, headers, data)
    print(f'{len(entries)} dead-lettered message(s) (left in place).')

def _print_dlq_entry(index : int, subject : str, headers : dict, data : bytes) -> None:
    print(f'--- DLQ message {index} (subject {subject}) ---')
    _print_decoded(data, headers = headers)

def _cmd_dlq_ls(args : argparse.Namespace) -> None:
    '''One line per dead letter: what failed, where, and why — the triage view.'''
    entries = _dlq_fetch(args.nats, args.flow_id, args.run_id, args.node, args.limit)
    entries = [e for e in entries if not args.code or e[1].get('VF-Code') == args.code]
    if not entries:
        print(f'No dead-lettered messages for flow {args.flow_id}.')
        return
    print(f'{"#":>3}  {"NODE":<20} {"CODE":<24} {"DELIV":>5}  RUN / ERROR')
    for index, (_subject, headers, _data) in enumerate(entries, start = 1):
        print(f'{index:>3}  {headers.get("VF-Origin-Node", "?"):<20} '
            f'{headers.get("VF-Code", "?"):<24} '
            f'{headers.get("VF-Num-Delivered", "?"):>5}  '
            f'{headers.get("VF-Run-Id", "?")} / {headers.get("VF-Error", "")}')
    by_code : dict = {}
    for _subject, headers, _data in entries:
        code = headers.get('VF-Code', '?')
        by_code[code] = by_code.get(code, 0) + 1
    print('\nby code: ' + '  '.join(f'{c}={n}' for c, n in sorted(by_code.items())))

def _cmd_dlq_show(args : argparse.Namespace) -> None:
    '''Full decode of one dead letter, payload included.'''
    entries = _dlq_fetch(args.nats, args.flow_id, args.run_id, args.node, args.id)
    if len(entries) < args.id:
        raise ConfigError(f'No dead-lettered message #{args.id} (found {len(entries)}).',
                        remedy = 'Run `videoflow dlq ls` to see what is there.')
    subject, headers, data = entries[args.id - 1]
    _print_dlq_entry(args.id, subject, headers, data)

def _cmd_dlq_replay(args : argparse.Namespace) -> None:
    '''
    Re-publishes dead letters onto the subject they originally failed on, so a
    fixed flow can finish the work it dropped.

    Two details make this actually work rather than merely look like it does. The
    replayed copy gets a **fresh** ``Nats-Msg-Id``: reusing the original would put
    it inside the stream's de-duplication window, and JetStream would silently
    discard the very message being replayed. And it carries ``VF-Replay`` naming
    the run it came from, so a replayed message is never mistaken for a first
    delivery when the numbers are audited.
    '''
    import nats  # optional dep (distributed/deploy extras)

    # optional dep: topology imports nats at module scope
    from ..messaging.topology import subject_for

    entries = _dlq_fetch(args.nats, args.flow_id, args.run_id, args.node, args.limit)
    entries = [e for e in entries if not args.code or e[1].get('VF-Code') == args.code]
    if not entries:
        print('Nothing to replay.')
        return
    target_run = args.to_run or args.run_id
    if not target_run:
        raise ConfigError('Replay needs a target run: pass --to-run (the run that should '
                        'process these messages) or --run-id.',
                        remedy = 'A dead letter records which run produced it, but not '
                                'which run should retry it.')
    # optional dep: serialization imports protobuf at module scope
    from ..wire.serialization import decode_envelope

    # Routing reads envelope metadata only (``resolve_blobs = False``): the target
    # of a dead letter is its parent's subject, which never needs the payload
    # bytes, so an offloaded entry routes exactly like an inline one even while
    # the payload store is unreachable (PAY-015).
    plan : list[tuple[str, dict, bytes, str | None, str | None]] = []
    undecodable = 0
    for _subject, headers, data in entries:
        # A dead letter holds the *input* the failing node was given, so it has
        # to go back onto the subject that node **reads from** — which is its
        # parent's, not its own. The envelope names that parent in
        # producer_name; VF-Origin-Node is the node that failed, and publishing
        # there would put the message somewhere nothing reads.
        try:
            decoded = decode_envelope(data, resolve_blobs = False)
        except Exception:  # noqa: BLE001 — anything undecodable cannot be routed, and is reported
            undecodable += 1
            continue
        plan.append((subject_for(args.flow_id, target_run, decoded['producer_name']), headers, data,
                     decoded.get('blob_ref'), headers.get('VF-Origin-Node') or None))
    if args.dry_run:
        for target, headers, _data, blob_ref, origin in plan:
            print(f'would replay {headers.get("VF-Code")} from {origin} into run {target_run} '
                  f'on {target}' + (f' (payload {blob_ref} offloaded)' if blob_ref else ''))
        print(f'{len(plan)} message(s) would be replayed (--dry-run).')
        if undecodable:
            print(f'{undecodable} entry(ies) could not be decoded and cannot be routed.', file = sys.stderr)
        return

    # The bytes a replayed entry needs are verified separately from routing, and
    # only when a store was named: a missing payload is a typed error, not a
    # message that was "replayed" and then dead-lettered again for its payload.
    unavailable : list[str] = []
    if args.blob_redis_url:
        # optional dep: serialization imports protobuf at module scope
        from ..wire.serialization import make_blob_store
        store = make_blob_store(args.blob_redis_url)
        for _target, _headers, _data, blob_ref, _origin in plan:
            if blob_ref is None:
                continue
            try:
                store.get(blob_ref)
            except Exception as e:  # noqa: BLE001 — every store failure is reported the same way
                unavailable.append(f'{blob_ref}: {e}')
        if unavailable:
            raise ResourceUnavailable(
                f'{len(unavailable)} dead letter(s) reference a payload the store could not return: '
                + '; '.join(unavailable[:5]),
                remedy = 'Restore the payload store (or its backup) before replaying; the entries were '
                         'left in place.')

    async def _go() -> int:
        nc = await nats.connect(args.nats)
        try:
            js = nc.jetstream()
            replayed = 0
            for target, headers, data, _blob_ref, origin in plan:
                replay_headers = {
                    # A fresh id, deliberately: reusing the original would land
                    # inside the stream's de-duplication window and JetStream
                    # would silently discard the very message being replayed.
                    'Nats-Msg-Id': f'replay:{uuid.uuid4().hex}',
                    'VF-Replay': headers.get('VF-Run-Id', ''),
                }
                if origin:
                    # Only the node that failed reprocesses it (DELIV-16): a parent's
                    # other children ack and skip a replay addressed elsewhere.
                    replay_headers['VF-Replay-Target'] = origin
                await js.publish(target, data, headers = replay_headers)
                replayed += 1
            await nc.flush()
            return replayed
        finally:
            await nc.close()

    count = asyncio.run(_go())
    print(f'Replayed {count} message(s) into run {target_run}.')
    if undecodable:
        # Never a silent partial: an entry that cannot be decoded cannot be routed.
        print(f'{undecodable} entry(ies) could not be decoded and were left in place.',
            file = sys.stderr)

def _cmd_dlq_purge(args : argparse.Namespace) -> None:
    '''Deletes the flow's dead letters — the only thing that ever should.'''
    import nats  # optional dep (distributed/deploy extras)

    # optional dep: topology imports nats at module scope
    from ..messaging.topology import dlq_stream_name

    stream = dlq_stream_name(args.flow_id)

    async def _go() -> None:
        nc = await nats.connect(args.nats)
        try:
            js = nc.jetstream()
            try:
                await js.delete_stream(stream)
            except Exception as e:
                raise BrokerUnavailable(f'Could not purge {stream}: {e}.',
                                    remedy = 'Check --flow-id and --nats.') from e
        finally:
            await nc.drain()

    asyncio.run(_go())
    print(f'Purged dead-letter stream {stream}.')

def _add_profile_flags(command : argparse.ArgumentParser) -> None:
    '''``--cluster`` / ``--clusters-file`` on every command a cluster profile feeds.'''
    command.add_argument('--cluster', default = None, metavar = 'NAME',
                         help = 'Cluster profile to take defaults from (a named entry of the clusters file). '
                                'Without it, the profile whose `context` is the current kubectl context '
                                'applies, if any. Flags given explicitly always win.')
    command.add_argument('--clusters-file', default = None, metavar = 'PATH',
                         help = f'The clusters file (default: ${PROFILES_FILE_ENV}, else '
                                '~/.config/videoflow/clusters.yaml). See the README, "Multi-node clusters".')

def _add_config_args(command : argparse.ArgumentParser) -> None:
    '''The solution config convention, for a command that loads the graph without running it.'''
    command.add_argument('--config', default = None,
                         help = 'Solution config file (default: config.yaml next to the graph; when '
                                'absent and the solution ships config.template.yaml, its questions '
                                'are asked and config.yaml written, as deploy does).')
    command.add_argument('--non-interactive', action = 'store_true',
                         help = 'Never prompt; fail with the list of missing config inputs instead.')

def build_parser(profile_defaults : dict[str, dict] | None = None) -> argparse.ArgumentParser:
    '''
    - Arguments:
        - profile_defaults: per-command argparse defaults from the selected \
            cluster profile (``main`` resolves them before parsing), applied with \
            ``set_defaults`` so an explicit flag still overrides them.
    '''
    parser = argparse.ArgumentParser(prog = 'videoflow', description = 'Deploy videoflow graphs.')
    parser.add_argument('--version', action = 'version', version = f'videoflow {__version__}')
    sub = parser.add_subparsers(dest = 'command', required = True)
    profile_defaults = profile_defaults or {}

    deploy = sub.add_parser(
        'deploy',
        help = 'Deploy a graph to Kubernetes: config Q&A, prepare, image build+load, broker '
               'provisioning, apply — and for a BATCH flow run to completion and tear down.',
        description = 'One-stop deploy: generates the solution config (interactive Q&A over its '
                      'template), runs its prepare.py hook, builds the node image from its '
                      '[gpu.]Dockerfile and loads it into the detected cluster, provisions the '
                      'broker (dev NATS/Redis when --nats is omitted), applies the flow, and — '
                      'for a BATCH flow — waits for it to finish and then deletes every resource '
                      '(k8s + broker streams + owned infra). A REALTIME flow is left running '
                      '(tear it down with `videoflow teardown`). Every automatic step has an '
                      'explicit override flag.')
    deploy.add_argument('graph', help = GRAPH_HELP)
    deploy.add_argument('--nats', default = None,
                        help = 'NATS URL reachable from inside the cluster. Omit to auto-provision '
                               'a dev NATS (and Redis for the blob store) in --namespace; a BATCH '
                               'run tears them down again unless --keep-infra is set.')
    deploy.add_argument('--namespace', default = 'default')
    deploy.add_argument('--kubectl', default = 'kubectl', help = 'kubectl binary name/path.')
    deploy.add_argument('--flow-id', default = None,
                        help = 'Stable flow id for naming resources (overrides the graph module\'s). '
                               'Use the same value to redeploy/update an existing flow.')
    deploy.add_argument('--run-id', default = None,
                        help = 'Per-run id that scopes this run\'s broker streams (auto-generated if omitted). '
                               'A new run id gives fresh streams; reuse it to target the same run.')
    deploy.add_argument('--render-only', action = 'store_true',
                        help = 'Write manifests to --output (+ a kustomization.yaml) and print the '
                               'kubectl-apply command instead of applying. The old deploy behavior.')
    deploy.add_argument('--keep', '--no-cleanup', dest = 'keep', action = 'store_true',
                        help = 'For a BATCH flow, leave all resources up after the run finishes or '
                               'fails (for debugging) instead of tearing them down. Implies --keep-infra.')
    deploy.add_argument('--keep-infra', action = 'store_true',
                        help = 'Leave auto-provisioned NATS/Redis up after a BATCH run (faster '
                               'redeploys; they are reused when present).')
    deploy.add_argument('--config', default = None,
                        help = 'Solution config file (default: config.yaml next to the graph; when '
                               'absent and the solution ships config.template.yaml, deploy asks its '
                               'questions and writes config.yaml).')
    deploy.add_argument('--non-interactive', action = 'store_true',
                        help = 'Never prompt; fail with the list of missing config inputs instead.')
    deploy.add_argument('--no-prepare', action = 'store_true',
                        help = 'Skip the solution\'s prepare.py hook.')
    deploy.add_argument('--no-build', action = 'store_true',
                        help = 'Never auto-build images from the solution\'s Dockerfile.')
    deploy.add_argument('--build-context', default = None,
                        help = 'Docker build context for the auto-built solution image '
                               '(default: the git root enclosing the graph).')
    deploy.add_argument('--mount', action = 'append', metavar = 'HOST[:CONTAINER][:ro]',
                        help = 'hostPath volume mounted into every node workload (and prep/compile '
                               'containers), e.g. --mount /data/videos:ro. Absolute paths; the '
                               'single-path form mounts the same path on both sides. Repeatable. '
                               'Solution x-mounts are added automatically.')
    deploy.add_argument('--mount-pvc', action = 'append', metavar = 'CLAIM:PATH[:ro]',
                        help = 'Existing PersistentVolumeClaim mounted at PATH in every node '
                               'workload, e.g. --mount-pvc work-share:/shared/videoflow. '
                               'The claim must live in --namespace. A --mount or x-mounts host '
                               'path at or under PATH is served by the claim in the pods (and '
                               'still by the host in the prep/compile containers) — the way a '
                               'multi-node cluster shares inputs, work dirs and caches. '
                               'Repeatable; solution x-mounts of the form pvc:CLAIM:PATH are '
                               'added automatically.')
    deploy.add_argument('--mount-home', default = None, metavar = 'DIR',
                        help = 'What `~` in the solution\'s x-mounts resolves to on the host side '
                               '(default: your home directory). On a multi-node cluster point it '
                               'inside the directory passed to --mount-pvc, so the model caches '
                               'the pods mount are the ones the prepare hook filled.')
    deploy.add_argument('--priority-class', default = None, metavar = 'NAME',
                        help = 'priorityClassName for every pod this deploy creates — workers, '
                               'the provision Job and any broker it provisions. The PriorityClass '
                               'must exist in the cluster; on a shared cluster it is how the flow '
                               'yields to (or preempts) other tenants\' work.')
    deploy.add_argument('--single-run', action = 'store_true',
                        help = 'Refuse to start this run while another run of the same flow holds workloads '
                               'in the namespace (checked before anything is created). Default: runs of one '
                               'flow coexist under run-scoped names.')
    deploy.add_argument('--rollout-policy', choices = ['drain', 'surge'], default = None,
                        help = 'How a node\'s Deployment replaces its pods on an update: drain '
                               '(Recreate — old replicas stop before new ones start; what a GPU '
                               'node needs when its devices cannot be held twice) or surge (one '
                               'extra replica at a time, none unavailable; needs the spare '
                               'capacity). Default: the Kubernetes default rolling update.')
    deploy.add_argument('--gpu-nodes', default = None, metavar = 'HOST[,HOST...]',
                        help = 'Pin every GPU pod to these hostnames (a required nodeAffinity on '
                               'kubernetes.io/hostname), on top of the pool label. For a shared '
                               'cluster where only some GPU nodes are yours to use.')
    deploy.add_argument('--resources', action = 'append', metavar = 'NODE=key:quantity[,...]', default = None,
                        help = 'Host requests/limits for a node\'s worker container: keys cpu, '
                               'memory (requests), cpu_limit, memory_limit (limits); NODE=* for '
                               'every node. Repeatable; a node entry overrides the * entry, '
                               'both override a component descriptor\'s spec.resources.')
    deploy.add_argument('--broker-profile', choices = list(BROKER_PROFILE_NAMES), default = None,
                        help = 'Shape of the auto-provisioned NATS/Redis when --nats is omitted. '
                               'dev (default): one emptyDir server each — an append-only, '
                               'never-evicting Redis, so BATCH flows are admitted — torn down '
                               'with a BATCH run. durable: a NATS StatefulSet with cluster routes '
                               'and a PersistentVolumeClaim per pod, plus the same Redis on a '
                               'claim, so streams and blobs survive a pod or a node. A broker the '
                               'namespace already runs is reused as it is; naming a profile that '
                               'contradicts its record is refused.')
    deploy.add_argument('--broker-replicas', type = int, default = None, metavar = 'N',
                        help = 'NATS servers for --broker-profile durable (default 3; odd). '
                               'Streams keep min(N, 3) copies.')
    deploy.add_argument('--broker-storage-class', default = None, metavar = 'NAME',
                        help = 'StorageClass of the durable profile\'s claims (default '
                               f'{BrokerProfile.durable().storage_class}, what k3s and kind ship).')
    deploy.add_argument('--gpu-runtime-class', default = None, metavar = 'NAME',
                        help = 'runtimeClassName for GPU pods. Default: the NVIDIA RuntimeClass the '
                               'cluster registers (`nvidia` on k3s, where the NVIDIA container '
                               'runtime is opt-in and a GPU pod without it starts with no device '
                               'visible), detected at deploy time and announced. Pass a name to '
                               'choose one, or `none` for no runtimeClassName.')
    # Choices come from the strategy registry, so a registered third mode is
    # selectable without touching this file. The entry-point group is loaded
    # first: argparse fixes `choices` at parser-construction time, so a plugin
    # mode would otherwise be rejected here before get_gpu_mode's lazy load ever
    # got the chance to register it. load_plugin_group is idempotent and logs
    # (rather than raises) for a broken plugin.
    load_plugin_group(GPU_STRATEGY_ENTRY_POINT_GROUP)
    deploy.add_argument('--gpu-mode', choices = registered_gpu_modes(), default = 'exclusive',
                        help = 'exclusive (default): each GPU replica claims whole physical devices '
                               'via the extended resource — a flow needs as many allocatable units '
                               'as it has GPU replicas, and gpu_count > 1 spans devices on one host. '
                               'mix: nodes declaring gpu_memory_gib share cards via solver-chosen '
                               'exclusive MIG slices; everything else still gets whole devices '
                               '(needs MIG-capable GPUs + GPU Feature Discovery labels).')
    deploy.add_argument('--gpu-resource-name', default = None, metavar = 'RESOURCE',
                        help = 'Extended-resource name GPU nodes request (default nvidia.com/gpu). '
                               'For clusters whose whole devices are advertised under another name, '
                               'e.g. amd.com/gpu. Not for MIG profiles or sliced resources — a unit '
                               'of this resource must be one whole physical device.')
    deploy.add_argument('--gpu-autoscaling', action = 'store_true',
                        help = 'Include GPU nodes in --autoscaling (off by default: every autoscaled '
                               'replica claims its own GPUs, so lag-driven scaling can demand more '
                               'devices than the cluster has and strand pods Pending).')
    deploy.add_argument('--require-profile', action = 'append', metavar = 'CHANNEL=PROFILE', default = None,
                        help = 'Require a messaging profile (live_latest, reliable_work, durable_control, '
                               'replay_archive) on the named channel — the output of that node. Makes the '
                               'composition check binding: a broker/store that cannot provide it is rejected '
                               'before anything is applied (a bring-your-own --nats/--blob-redis-url is read '
                               'back live), the provision Job verifies the streams it creates, and each worker '
                               'verifies its channels before it opens. Repeatable.')
    deploy.add_argument('--strict-preflight', action = 'store_true',
                        help = 'Exit non-zero (before applying anything) when the GPU preflight '
                               'finds problems, instead of proceeding with warnings.')
    deploy.add_argument('--output', default = './manifests',
                        help = 'Directory for --render-only manifest files (default ./manifests).')
    deploy.add_argument('--image', default = None,
                        help = 'Default container image ref for nodes that do not declare their own '
                               '(e.g. ghcr.io/acme/app:v1). Build it FROM videoflow-base with your code + deps.')
    deploy.add_argument('--registry', default = None, metavar = 'HOST[:PORT][/PREFIX]',
                        help = 'Push the built (or local --image) image here and have the pods pull '
                               'it from there, e.g. --registry 10.0.0.1:5000 or ghcr.io/acme. Needed '
                               'on a multi-node cluster, where side-loading reaches one node only; '
                               'usually set once in the cluster profile (--cluster).')
    deploy.add_argument('--push-tool', choices = list(PUSH_TOOLS), default = 'docker',
                        help = 'How --registry pushes: docker (default; the daemon must trust the '
                               'registry) or crane (user space, `--insecure`, for a plain-HTTP '
                               'registry the daemon does not trust — needs crane on PATH).')
    deploy.add_argument('--blob-redis-url', default = None, help = 'Redis URL for the large-payload blob store.')
    deploy.add_argument('--blob-ttl-seconds', type = int, default = None,
                        help = 'TTL for offloaded payloads in the blob store. Default: flow-type '
                               'default (3600 realtime / 86400 batch). Must exceed the worst-case '
                               'publish-to-ack latency of the flow (PROTOCOL.md BLOB-7).')
    deploy.add_argument('--image-override', action = 'append', metavar = 'NAME=IMAGE',
                        help = 'Override the container image for one node (wins over --image and the node\'s '
                               'own image=). Repeatable.')
    deploy.add_argument('--image-pull-policy', default = DEFAULT_IMAGE_PULL_POLICY,
                        choices = list(IMAGE_PULL_POLICIES),
                        help = f'imagePullPolicy for every container (default {DEFAULT_IMAGE_PULL_POLICY}). '
                               'The default is what lets a locally built image run: deploy loads it into '
                               'the cluster itself, so there is nothing to pull — and an auto-built image '
                               'is deployed under a content-addressed tag, so it stays right on a registry '
                               'too (a changed image gets a new tag). Use Always for a registry image under '
                               'a mutable tag you re-push.')
    deploy.add_argument('--autoscaling', action = 'store_true',
                        help = 'Emit a KEDA ScaledObject per processor node (requires KEDA in-cluster).')
    deploy.add_argument('--max-replicas', type = int, default = 10,
                        help = 'Upper bound for autoscaled processors (default 10).')
    deploy.add_argument('--envelope-version', type = int, default = None,
                        help = 'Wire envelope version. The only supported version is 4 (protobuf); '
                               'defaults to the build default.')
    deploy.add_argument('--provision-image', default = None,
                        help = 'Image the provision init Job runs on (needs videoflow + broker client). '
                               'Set this when --image is a non-Python vendor image.')
    deploy.add_argument('--dry-run', action = 'store_true', help = 'Print manifests to stdout, write nothing.')
    _add_profile_flags(deploy)
    deploy.set_defaults(func = _cmd_deploy, **profile_defaults.get('deploy', {}))

    run = sub.add_parser(
        'run-local',
        help = 'Run a graph as local subprocesses: config Q&A, prepare, broker provisioning, '
               'run to completion — the local twin of `deploy`.',
        description = 'One-stop local run: generates the solution config (interactive Q&A over '
                      'its config.template.yaml when none exists), runs its prepare.py hook, '
                      'starts a dev NATS (and Redis for the blob store) in docker when '
                      '--nats is omitted and nothing is already listening, spawns one worker '
                      'subprocess per node replica, waits for the flow to finish, reports any node '
                      'that exited non-zero, and stops only the containers it started. When the '
                      'graph\'s dependencies are not installed on this host (or with --in-image), '
                      'the prepare hook and every worker run inside the solution image instead — '
                      'the same image `deploy` builds from its [gpu.]Dockerfile. Every '
                      'automatic step has an explicit override flag.')
    run.add_argument('graph', help = GRAPH_HELP)
    run.add_argument('--nats', default = None,
                    help = 'NATS URL to use. Omit to reuse a broker already listening on '
                           'localhost:4222, or else start a dev NATS (and Redis) in docker and '
                           'stop it again when the flow ends.')
    run.add_argument('--blob-redis-url', default = None,
                    help = 'Redis URL for the large-payload blob store (default: '
                           '$VIDEOFLOW_BLOB_REDIS_URL, else auto-provisioned alongside NATS).')
    run.add_argument('--blob-ttl-seconds', type = int, default = None,
                    help = 'TTL for offloaded payloads in the blob store. Default: flow-type '
                           'default (3600 realtime / 86400 batch).')
    run.add_argument('--run-id', default = None,
                    help = 'Per-run id that scopes this run\'s broker streams (auto-generated if omitted).')
    run.add_argument('--config', default = None,
                    help = 'Solution config file (default: config.yaml next to the graph; when '
                           'absent and the solution ships config.template.yaml, run-local asks its '
                           'questions and writes config.yaml).')
    run.add_argument('--non-interactive', action = 'store_true',
                    help = 'Never prompt; fail with the list of missing config inputs instead.')
    run.add_argument('--no-prepare', action = 'store_true',
                    help = 'Skip the solution\'s prepare.py hook.')
    run.add_argument('--no-infra', action = 'store_true',
                    help = 'Never start broker containers; assume a broker is already running.')
    run.add_argument('--no-redis', action = 'store_true',
                    help = 'Do not auto-provision Redis for the large-payload blob store.')
    run.add_argument('--keep-infra', action = 'store_true',
                    help = 'Leave auto-started NATS/Redis containers running afterwards (faster '
                           'reruns; they are reused when present).')
    run.add_argument('--local-docker-nats-url', default = None,
                    help = 'NATS URL a docker-run remote component connects to '
                           '(default rewrites localhost -> host.docker.internal).')
    run.add_argument('--image', default = None,
                    help = 'The solution image: what the workers run in when the graph\'s '
                           'dependencies are not installed on this host (or with --in-image), '
                           'and what native components that declare no image= of their own '
                           'run in. Suppresses the auto-build. A graph that imports here runs '
                           'its Python nodes as host subprocesses regardless.')
    run.add_argument('--no-build', action = 'store_true',
                    help = 'Never auto-build the solution image. A graph that does not import '
                           'here, or a native component with no image= and no '
                           'runtime.localCommand, then fails to start.')
    run.add_argument('--in-image', action = 'store_true',
                    help = 'Run the prepare hook and every worker inside the solution image even '
                           'when the graph imports on this host. The default does so only when '
                           'it does not.')
    run.add_argument('--build-context', default = None,
                    help = 'docker build context for the auto-build (default: the git root '
                           'enclosing the graph).')
    run.add_argument('--mount', action = 'append', metavar = 'HOST[:CONTAINER][:ro]',
                    help = 'Extra bind mount for workers that run inside the solution image (and '
                           'its prepare/compile containers), e.g. --mount /data/videos:ro. '
                           'Absolute paths; the single-path form mounts the same path on both '
                           'sides. Repeatable; solution x-mounts are added automatically. '
                           'Workers on this host need none.')
    run.add_argument('--mount-home', default = None, metavar = 'DIR',
                    help = 'What `~` in the solution\'s x-mounts resolves to on the host side '
                           '(default: your home directory); see deploy --mount-home.')
    run.add_argument('--require-profile', action = 'append', metavar = 'CHANNEL=PROFILE', default = None,
                    help = 'Require a messaging profile on the named channel; makes the composition check '
                           'against the dev broker/store binding (see deploy --require-profile).')
    run.add_argument('--no-restart', action = 'store_true',
                    help = 'Do not restart a worker that crashes. By default run-local '
                           'restarts up to 3 times (backoff 1/2/4s), matching the '
                           'Kubernetes Job semantics so the recovery path is exercised '
                           'locally; pass this for a tight debug loop.')
    run.add_argument('--gpu-policy', choices = ['shared', 'strict'], default = 'shared',
                    help = 'How this host\'s GPUs are shared out to GPU workers. shared (default): '
                           'when demand exceeds the visible devices, workers share them — each '
                           'worker is told the grant it really got. strict: refuse to start when '
                           'the exclusive grants do not fit, or a sharer\'s declared gpu_memory_gib '
                           'does not fit its device (VF_GPU_HEADROOM_BYTES of headroom each); '
                           'a host nvidia-smi cannot read is refused too.')
    _add_profile_flags(run)
    run.set_defaults(func = _cmd_run_local, **profile_defaults.get('run-local', {}))

    comp = sub.add_parser('component', help = 'Work with component descriptors.')
    comp_sub = comp.add_subparsers(dest = 'component_command', required = True)
    validate = comp_sub.add_parser('validate', help = 'Validate one or more component.yaml descriptors.')
    validate.add_argument('path', nargs = '+', help = 'Path(s) to a component.yaml or its directory.')
    validate.set_defaults(func = _cmd_component_validate)

    push = comp_sub.add_parser('push', help = 'Push a component descriptor to an OCI registry as an artifact.')
    push.add_argument('path', help = 'Path to a component.yaml or its directory.')
    push.add_argument('ref', help = 'Target ref, e.g. oci://ghcr.io/vendor/name:1.2.0')
    push.set_defaults(func = _cmd_component_push)

    pull = comp_sub.add_parser('pull', help = 'Pull + cache a component descriptor from an OCI registry.')
    pull.add_argument('ref', help = 'Component ref, e.g. oci://ghcr.io/vendor/name:1.2.0')
    pull.add_argument('--force', action = 'store_true', help = 'Re-pull even if cached.')
    pull.add_argument('--verify', action = 'store_true', help = 'Verify the artifact signature with cosign before trusting it.')
    pull.add_argument('--cosign-arg', action = 'append', metavar = 'ARG',
                    help = 'Extra arg passed to `cosign verify` (e.g. --key=cosign.pub). Repeatable.')
    pull.set_defaults(func = _cmd_component_pull)

    inspect = comp_sub.add_parser('inspect', help = 'Show a remote component descriptor (pulls + caches it).')
    inspect.add_argument('ref', help = 'Component ref, e.g. oci://ghcr.io/vendor/name:1.2.0')
    inspect.add_argument('--verify', action = 'store_true', help = 'Verify the signature with cosign first.')
    inspect.set_defaults(func = _cmd_component_inspect)

    explain = sub.add_parser('explain', help = 'Print a human-readable summary of a compiled graph.')
    explain.add_argument('graph', help = GRAPH_HELP)
    explain.add_argument('--run-id', default = None)
    _add_config_args(explain)
    explain.add_argument('--gpu-resource-name', default = None, metavar = 'RESOURCE',
                         help = 'Default GPU extended-resource name, as on deploy — pass the same '
                                'value so the printed GPU demand matches what deploy will request.')
    explain.set_defaults(func = _cmd_explain)

    prov = sub.add_parser('provision', help = 'Create a flow\'s streams/durables on the broker (usually run automatically).')
    prov.add_argument('graph', help = GRAPH_HELP)
    prov.add_argument('--nats', required = True)
    prov.add_argument('--run-id', default = None)
    _add_config_args(prov)
    prov.set_defaults(func = _cmd_provision)

    teardown = sub.add_parser('teardown', help = 'Stop a run and delete its broker streams (and, with --namespace, its K8s workloads).')
    teardown.add_argument('--flow-id', required = True)
    teardown.add_argument('--run-id', required = True)
    teardown.add_argument('--nats', default = None,
                          help = 'The broker the run used (deploy prints it in its teardown hint). Required '
                                 'unless the cluster profile names one.')
    teardown.add_argument('--namespace', default = None, help = 'If set, also kubectl-delete the run\'s workloads.')
    teardown.add_argument('--kubectl', default = 'kubectl', help = 'kubectl binary name/path.')
    teardown.add_argument('--infra', action = 'store_true',
                          help = 'Also delete auto-provisioned dev NATS/Redis in --namespace '
                                 '(only resources labeled videoflow.io/infra).')
    teardown.add_argument('--broker-profile', choices = list(BROKER_PROFILE_NAMES), default = None,
                          help = 'The broker profile the run was deployed with; with --infra, '
                                 'durable also deletes the NATS StatefulSet (its claims are kept). '
                                 'Read from the record on the nats Service when omitted; deploy '
                                 'prints this flag in the teardown command when it applies.')
    teardown.add_argument('--gpu-mode', default = None,
                          help = 'The GPU mode the run was deployed with. Only needed when that '
                                 'mode reconfigured the cluster in prepare() and must be undone; '
                                 'deploy prints this flag in the teardown command when it applies.')
    _add_profile_flags(teardown)
    teardown.set_defaults(func = _cmd_teardown, **profile_defaults.get('teardown', {}))

    debug = sub.add_parser('debug', help = 'Inspect wire messages (envelopes, DLQ).')
    debug_sub = debug.add_subparsers(dest = 'debug_command', required = True)
    decode = debug_sub.add_parser('decode', help = 'Decode and print videoflow envelope(s) from a file or a run\'s DLQ.')
    decode.add_argument('file', nargs = '?', help = 'Path to a file of raw envelope bytes.')
    decode.add_argument('--dlq', action = 'store_true', help = 'Read from a run\'s DLQ stream instead of a file.')
    decode.add_argument('--nats', default = 'nats://localhost:4222')
    decode.add_argument('--flow-id', default = None)
    decode.add_argument('--run-id', default = None)
    decode.add_argument('--limit', type = int, default = 20, help = 'Max DLQ messages to decode (default 20).')
    decode.set_defaults(func = _cmd_debug_decode)

    dlq = sub.add_parser('dlq', help = "Inspect and replay a flow's dead-lettered messages.")
    dlq_sub = dlq.add_subparsers(dest = 'dlq_command', required = True)

    def _dlq_common(p : argparse.ArgumentParser) -> argparse.ArgumentParser:
        p.add_argument('--flow-id', required = True, help = 'Flow whose DLQ to read (it is flow-scoped, not per run).')
        p.add_argument('--run-id', help = 'Only entries produced by this run.')
        p.add_argument('--node', help = 'Only entries from this node.')
        p.add_argument('--nats', default = 'nats://localhost:4222')
        return p

    dlq_ls = _dlq_common(dlq_sub.add_parser('ls', help = 'List dead-lettered messages with their error codes.'))
    dlq_ls.add_argument('--code', help = 'Only this error code, e.g. VF_POISON_DECODE.')
    dlq_ls.add_argument('--limit', type = int, default = 50)
    dlq_ls.set_defaults(func = _cmd_dlq_ls)

    dlq_show = _dlq_common(dlq_sub.add_parser('show', help = 'Fully decode one dead-lettered message.'))
    dlq_show.add_argument('--id', type = int, required = True, help = 'Index from `dlq ls`.')
    dlq_show.set_defaults(func = _cmd_dlq_show)

    dlq_replay = _dlq_common(dlq_sub.add_parser(
        'replay', help = 'Re-publish dead-lettered messages so a fixed flow can process them.'))
    dlq_replay.add_argument('--code', help = 'Only replay this error code.')
    dlq_replay.add_argument('--to-run', help = 'Run that should process them (defaults to --run-id).')
    dlq_replay.add_argument('--limit', type = int, default = 1000)
    dlq_replay.add_argument('--dry-run', action = 'store_true', help = 'Print what would be replayed.')
    dlq_replay.add_argument('--blob-redis-url', default = None,
                            help = 'Payload store URL; when given, every offloaded payload is verified to be '
                                   'readable before anything is replayed.')
    dlq_replay.set_defaults(func = _cmd_dlq_replay)

    dlq_purge = dlq_sub.add_parser('purge', help = "Delete the flow's dead-letter stream.")
    dlq_purge.add_argument('--flow-id', required = True)
    dlq_purge.add_argument('--nats', default = 'nats://localhost:4222')
    dlq_purge.set_defaults(func = _cmd_dlq_purge)

    return parser

def resolve_profile_defaults(argv : list[str]) -> dict[str, dict]:
    '''
    The cluster profile's contribution to the command about to be parsed (see
    ``deploy.profiles``): ``--cluster`` / ``--clusters-file`` are read ahead of
    the real parse, the file is loaded, a profile is selected (by name, else by
    the current kubectl context), its ``docker`` section becomes the docker
    environment for variables not already set, and its cluster keys become the
    command's argparse defaults — so a flag typed on the command line still wins.
    Commands that take no profile get nothing and read no file.

    - Raises:
        - ConfigError: the file is malformed, or ``--cluster`` names no profile.
    '''
    command = argv[0] if argv and argv[0] in PROFILE_COMMANDS else None
    if command is None:
        return {}
    pre = argparse.ArgumentParser(add_help = False, allow_abbrev = False)
    pre.add_argument('--cluster', default = None)
    pre.add_argument('--clusters-file', default = None)
    pre.add_argument('--kubectl', default = 'kubectl')
    known, _rest = pre.parse_known_args(argv[1:])
    profiles = load_profiles(known.clusters_file)
    apply_docker_env(profiles)
    if command == 'run-local':
        return {}                                   # a local run has no cluster; the docker section applied
    selected = select_profile(profiles, known.cluster, lambda: current_context(known.kubectl))
    if selected is None:
        return {}
    name, profile = selected
    defaults = command_defaults(profile, command)
    context = f" (context {profile['context']!r})" if profile.get('context') else ''
    print(f'Using cluster profile {name!r}{context} from {profiles.path}: '
          + ', '.join(f'{k}={v!r}' for k, v in defaults.items()), file = sys.stderr)
    return {command: defaults}

def render_error(error : VideoflowError) -> None:
    '''
    Prints a failure the way an operator needs to read it: what broke, then what
    to do about it, then the structured detail — never a traceback, which is a
    stack of framework internals the reader did not write and cannot act on. Set
    ``VF_DEBUG=1`` when the traceback *is* the thing you want.
    '''
    print(error.render(), file = sys.stderr)
    if isinstance(error, GraphError) and len(error.diagnostics) > 1:
        # Already listed inside the message; the warnings are the extra value.
        for diagnostic in error.diagnostics:
            if diagnostic.severity != 'error':
                print(f'  {diagnostic.render()}', file = sys.stderr)

def main(argv : list[str] | None = None) -> int:
    '''
    The one place a videoflow failure becomes an operator-facing message and a
    process exit status.

    The exit status carries the *class* of failure — 2 your flow, 3 your
    environment, 4 the flow ran and lost nodes, 5 it stalled — so CI and wrapper
    scripts can triage without parsing stderr. Everything used to exit 1.
    '''
    argv = list(sys.argv[1:] if argv is None else argv)
    try:
        defaults = resolve_profile_defaults(argv)
    except VideoflowError as e:
        render_error(e)
        return e.exit_code
    # Built without arguments when no profile contributes anything — the common
    # case, and the signature callers (tests included) may stand in for.
    parser = build_parser(defaults) if defaults else build_parser()
    args = parser.parse_args(argv)
    try:
        args.func(args)
    except VideoflowError as e:
        render_error(e)
        if os.environ.get('VF_DEBUG'):
            traceback.print_exc()
        return e.exit_code
    except ModuleNotFoundError as e:
        # A bare `pip install videoflow` has the console script but not the extras
        # most commands need; say which one to install instead of dumping a stack.
        error = _missing_extra(e, args.command)
        if error is None:
            raise
        render_error(error)
        if os.environ.get('VF_DEBUG'):
            traceback.print_exc()
        return error.exit_code
    except KeyboardInterrupt:
        print('\nInterrupted.', file = sys.stderr)
        return EXIT_INTERRUPTED
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
