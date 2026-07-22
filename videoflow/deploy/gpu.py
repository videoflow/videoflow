'''
GPU allocation strategies: how a GPU node's pods claim devices, what to preflight
before deploying them, and any cluster state a strategy needs to set up for a run
and restore afterwards.

Two strategies ship, matching the two-mode GPU design (RFC 0004):

- ``exclusive`` (default) claims whole physical devices through an integer
  extended resource, so the scheduler accounts for them and a flow that
  outgrows the cluster stays Pending rather than thrashing. Under it a GPU unit
  *is* one whole device — sharing a device between components is not a
  capability of the mode, and the node API offers no way to ask for it. (Dev
  clusters share devices by advertising time-sliced units and keeping
  ``gpu_count == 1``; the preflight below understands that arrangement.)
- ``mix`` (opt-in) serves declared demands: nodes with ``gpu_memory_gib`` get
  exclusive MIG slices chosen by the layout solver (``deploy/mig.py``), nodes
  with ``gpu_count`` — declared or defaulted — get whole physical cards. Its
  ``prepare``/``cleanup`` hooks apply and restore MIG geometry for the run.

The mode used to be a bare string branched on in four places (pod resources,
manifest validation, preflight, the CLI's choices), which is why adding a third
meant finding all four. A strategy is now one class registered here.
'''
from __future__ import absolute_import, division, print_function

import dataclasses
import json
import logging
import subprocess
import time
from typing import List, Optional

from ..core.compiler import NodeSpec
from ..utils import plugins
from .mig import GpuLayout, layout_to_mig_parted_config, solve_layout

logger = logging.getLogger(__package__)

#: Extended resource used when neither the node nor the deploy names one.
DEFAULT_GPU_RESOURCE = 'nvidia.com/gpu'

#: Node label a GPU pod selects on, and the taint key it tolerates.
GPU_POOL_LABEL = 'videoflow.io/gpu-pool'
GPU_TAINT_KEY = 'nvidia.com/gpu'

GPU_STRATEGY_ENTRY_POINT_GROUP = 'videoflow.gpu_strategies'

#: Marker prefix for a preflight problem that is fatal regardless of
#: ``--strict-preflight``: the request is impossible by construction (a multi-unit
#: claim against a MIG or time-sliced resource), so deploying anyway can only end
#: in an admission error or a silently broken visibility contract. A constant so
#: the CLI's check never depends on message prose.
IMPOSSIBLE_GPU_REQUEST = 'impossible GPU request'

def resolve_gpu_resource(spec : NodeSpec, default : Optional[str] = None) -> str:
    '''The extended-resource name a GPU spec requests: the spec's resolved name
    (internal — only a GPU strategy sets it, e.g. ``mix`` assigning a MIG profile),
    else the deploy default (``--gpu-resource-name``), else ``nvidia.com/gpu``.'''
    return spec.gpu_resource_name or default or DEFAULT_GPU_RESOURCE

class GpuStrategy:
    '''
    One GPU allocation mode.

    A strategy owns three decisions that must agree with each other: what a GPU
    pod asks the scheduler for (``pod_resources``), what makes that request
    satisfiable and is therefore worth checking first (``preflight_problems``),
    and whether the cluster needs temporary reconfiguration to honour it
    (``prepare``/``cleanup``). Splitting them across modules is what made the
    old string-mode version easy to extend incorrectly.
    '''
    #: Mode name, as used by ``--gpu-mode``.
    name : str = ''

    def pod_resources(self, spec : NodeSpec, gpu_resource_name : Optional[str] = None) -> dict:
        '''
        The container ``resources`` fragment for one GPU pod — ``{}`` for a
        strategy that requests nothing.

        Stays a plain dict on purpose: it is spliced straight into a container
        spec, so its shape is Kubernetes' ``ResourceRequirements`` schema and not
        a record we own. A strategy may legitimately emit ``requests``, ``limits``
        or neither, with arbitrary extended-resource keys.

        - Arguments:
            - spec: the node's ``NodeSpec`` (``gpu_count``, ``gpu_resource_name``).
            - gpu_resource_name: deploy-level default extended-resource name.
        '''
        raise NotImplementedError('GpuStrategy subclass must implement pod_resources()')

    def preflight_problems(self, kubectl : str = 'kubectl', demand : Optional[dict[str, int]] = None,
                        gpu_runtime_class : Optional[str] = None,
                        max_per_pod : Optional[dict[str, int]] = None) -> List[str]:
        '''
        Strategy-specific preflight problems, each a string naming its fix. The
        flavor-independent checks (cluster reachable, a labeled GPU node) are run
        by ``cluster.gpu_preflight`` before this is called. A problem prefixed
        with ``IMPOSSIBLE_GPU_REQUEST`` is fatal regardless of
        ``--strict-preflight``.

        Third-party strategies should tolerate future keyword inputs (accept
        ``**kwargs``): new preflight inputs arrive as keywords with ``None``
        defaults, as ``max_per_pod`` did (RFC 0003).

        - Arguments:
            - demand: extended-resource name -> units the flow requests, or None \
                to skip capacity comparison.
            - gpu_runtime_class: the ``--gpu-runtime-class`` value, if given.
            - max_per_pod: extended-resource name -> the largest single-pod claim \
                (``manifests.gpu_max_per_pod``), or None to skip per-node checks.
        '''
        return []

    def resolve_specs(self, specs : List[NodeSpec], kubectl : str = 'kubectl',
                    default_resource : Optional[str] = None) -> List[NodeSpec]:
        '''
        The strategy's chance to decide names and geometry before anything is
        rendered or preflighted: called once per deploy, with the compiled specs,
        before ``gpu_demand``/``gpu_max_per_pod``/``render_manifests`` consume
        them. Default: identity. The ``mix`` strategy returns specs whose
        ``gpu_resource_name`` carries each sharer's solver-chosen MIG profile —
        after which the entire downstream pipeline runs unchanged.

        - Raises:
            - ValueError: the flow's demands cannot be laid out (``mix``'s \
                ``LayoutError`` is one) — the deploy should stop before rendering.
        '''
        return specs

    def prepare(self, demand : Optional[dict[str, int]] = None, kubectl : str = 'kubectl') -> None:
        '''
        Cluster setup this strategy needs before a run's manifests are applied.
        Default: nothing. A strategy that mutates cluster state here is
        responsible for restoring it in ``cleanup``.
        '''
        return None

    def cleanup(self, kubectl : str = 'kubectl') -> None:
        '''
        Undoes ``prepare``. Must be idempotent and tolerant: it is called after a
        ``prepare`` that only partly succeeded, and — for a REALTIME flow, whose
        lifetime outlives the deploy command — from a later ``videoflow teardown``
        that passes ``--gpu-mode`` but shares no state with the deploy that ran
        ``prepare``. So it cannot assume ``prepare`` completed, or ran at all.
        '''
        return None

class ExclusiveGpu(GpuStrategy):
    '''Whole-device claims through an integer extended resource (the default).'''
    name = 'exclusive'

    def pod_resources(self, spec : NodeSpec, gpu_resource_name : Optional[str] = None) -> dict:
        # nvidia.com/gpu (or a MIG profile / renamed time-sliced resource via
        # gpu_resource_name) is an integer extended resource the scheduler
        # allocates exclusively — N GPU replicas need N allocatable units, and
        # pods beyond capacity stay Pending.
        return {'limits': {resolve_gpu_resource(spec, gpu_resource_name): spec.gpu_count}}

    def preflight_problems(self, kubectl : str = 'kubectl', demand : Optional[dict[str, int]] = None,
                        gpu_runtime_class : Optional[str] = None,
                        max_per_pod : Optional[dict[str, int]] = None) -> List[str]:
        # Function-level: cluster.py imports this module at module scope (for
        # get_gpu_mode), so importing it back at module scope here would be a cycle.
        from .cluster import (
            allocatable_gpus,
            classify_gpu_resource,
            max_allocatable_gpus_per_node,
            nvidia_runtimeclass,
        )

        problems = []
        for resource in sorted(demand) if demand else [DEFAULT_GPU_RESOURCE]:
            capacity = allocatable_gpus(kubectl, resource)
            if capacity == 0:
                if resource == DEFAULT_GPU_RESOURCE:
                    problems.append('no node advertises nvidia.com/gpu — install the NVIDIA device '
                                    'plugin: kubectl apply -f https://raw.githubusercontent.com/NVIDIA/'
                                    'k8s-device-plugin/v0.16.2/deployments/static/nvidia-device-plugin.yml')
                else:
                    problems.append(f'no node advertises {resource} — the flow requests it '
                                    f'(--gpu-resource-name) but the cluster does not expose it')
            elif demand and demand[resource] > capacity:
                problems.append(
                    f'flow demands {demand[resource]} x {resource} but the cluster has only '
                    f'{capacity} allocatable — {demand[resource] - capacity} pod(s) will stay '
                    f'Pending and the flow will stall. Reduce GPU nodes/replicas, or enable '
                    f'device-plugin time-slicing (dev clusters; see the GPU sharing docs)')
        # Per-pod bound (RFC 0003): all of one replica's gpu_count devices must sit
        # on ONE host, so the cluster total above is necessary but not sufficient.
        # And they must be whole physical devices: a multi-unit claim against a MIG
        # or time-sliced resource can be granted by the scheduler and still hand the
        # pod devices a model cannot span (one CUDA process addresses one MIG
        # instance; time-sliced units may be shares of the same card) — so the
        # resource is classified before the arithmetic, and an impossible request
        # is fatal (IMPOSSIBLE_GPU_REQUEST), not a strict-mode warning.
        for resource, per_pod in sorted((max_per_pod or {}).items()):
            if per_pod <= 1:
                continue
            kind = classify_gpu_resource(kubectl, resource)
            if kind == 'mig':
                problems.append(
                    f'{IMPOSSIBLE_GPU_REQUEST}: a node requests {per_pod} x {resource} in one '
                    f'pod, but MIG slices are hardware-isolated partitions that a model cannot '
                    f'span — gpu_count > 1 against a MIG resource can never satisfy the '
                    f'visibility contract. Request whole GPUs from a non-MIG pool, or reduce '
                    f'gpu_count to 1.')
                continue
            if kind == 'time-sliced':
                problems.append(
                    f'{IMPOSSIBLE_GPU_REQUEST}: a node requests {per_pod} x {resource} in one '
                    f'pod, but {resource} is time-sliced on this cluster — its units are shares '
                    f'of a device, not devices, so the pod is either rejected at admission '
                    f'(failRequestsGreaterThanOne) or granted slices of the same physical GPU. '
                    f'Spanning needs whole physical GPUs: disable time-slicing for this pool, '
                    f'or reduce gpu_count to 1.')
                continue
            if kind == 'unknown':
                problems.append(
                    f'cannot classify {resource} (no GPU Feature Discovery labels on the '
                    f'advertising nodes) — assuming its units are whole physical devices. '
                    f'If this pool is MIG- or time-sliced, gpu_count = {per_pod} will not '
                    f'behave as a multi-GPU grant.')
            biggest = max_allocatable_gpus_per_node(kubectl, resource)
            # biggest == 0 means the resource is unadvertised, already reported above.
            if biggest and per_pod > biggest:
                problems.append(
                    f'a node requests {per_pod} x {resource} in a single pod but the largest '
                    f'cluster node has only {biggest} allocatable — all of one replica\'s GPUs '
                    f'must sit on one Kubernetes host, so the cluster total is irrelevant. '
                    f'Fix: add a node with >= {per_pod} GPUs, or reduce gpu_count.')
        if not gpu_runtime_class:
            nvidia_rc = nvidia_runtimeclass(kubectl)
            if nvidia_rc:
                problems.append(f'a {nvidia_rc!r} RuntimeClass exists but no --gpu-runtime-class was '
                                f'given — if the NVIDIA runtime is not this node\'s containerd default, '
                                f'GPU pods will start with no device. Fix: deploy with '
                                f'--gpu-runtime-class {nvidia_rc}')
        return problems

#: Node annotation where mix's prepare() records the node's previous
#: nvidia.com/mig.config label value ('' = the label was absent), so a later
#: cleanup() — possibly a teardown in a fresh shell — can restore it without
#: sharing any state with the deploy that ran prepare().
MIG_RESTORE_ANNOTATION = 'videoflow.io/mig-config-restore'

#: The node label the GPU Operator's MIG manager watches, and its status twin.
MIG_CONFIG_LABEL = 'nvidia.com/mig.config'
MIG_CONFIG_STATE_LABEL = 'nvidia.com/mig.config.state'

#: How long prepare() waits for the MIG manager to report success per apply.
MIG_APPLY_TIMEOUT_SECONDS = 600
MIG_APPLY_POLL_SECONDS = 5


def _kubectl_run(kubectl : str, *args : str) -> str:
    '''Runs kubectl for a mutation and returns stdout; raises RuntimeError with
    stderr on failure (unlike ``cluster._kubectl_out``, silence is not an option
    when we are changing cluster state).'''
    try:
        proc = subprocess.run([kubectl, *args], capture_output = True, text = True, check = False)
    except FileNotFoundError as e:
        raise RuntimeError(f'{kubectl} not found — is kubectl installed and on PATH?') from e
    if proc.returncode != 0:
        raise RuntimeError(f'{kubectl} {" ".join(args)} failed: {proc.stderr.strip()}')
    return proc.stdout.strip()


class MixGpu(ExclusiveGpu):
    '''
    Declared-demand MIG partitioning (RFC 0004): sharers (``gpu_memory_gib``) get
    exclusive MIG slices chosen by the layout solver, spanners and
    undeclared-demand nodes get whole physical cards. Pod claims are
    exclusive-style integer limits — only the *names* differ, and those are
    decided in ``resolve_specs``, which is why this subclasses ``ExclusiveGpu``.
    '''
    name = 'mix'

    def __init__(self) -> None:
        # The layout is computed once per deploy in resolve_specs and reused by
        # preflight/prepare in the same process. Solving is deterministic, so a
        # recompute would agree — the cache only saves kubectl round-trips.
        self._layout : Optional[GpuLayout] = None

    def resolve_specs(self, specs : List[NodeSpec], kubectl : str = 'kubectl',
                    default_resource : Optional[str] = None) -> List[NodeSpec]:
        # Function-level: same gpu <-> cluster cycle as in ExclusiveGpu.preflight_problems.
        from .cluster import gpu_inventory

        if not any(s.device_type == 'gpu' for s in specs):
            return specs
        layout = solve_layout(gpu_inventory(kubectl), specs)   # raises LayoutError (a ValueError)
        self._layout = layout
        resolved = []
        for spec in specs:
            profile_resource = layout.spec_resources.get(spec.name)
            if profile_resource is not None:
                # replace() rather than mutation: the caller may hold the originals.
                spec = dataclasses.replace(spec, gpu_resource_name = profile_resource)
            resolved.append(spec)
        return resolved

    def preflight_problems(self, kubectl : str = 'kubectl', demand : Optional[dict[str, int]] = None,
                        gpu_runtime_class : Optional[str] = None,
                        max_per_pod : Optional[dict[str, int]] = None) -> List[str]:
        # Function-level: same cycle as above.
        from .cluster import allocatable_gpus, nvidia_runtimeclass

        problems = []
        layout = self._layout
        if layout is None:
            problems.append('mix preflight ran without a resolved layout — deploy through the '
                            'videoflow CLI, which calls resolve_specs first.')
            return problems
        # The numeric exclusive checks compare against *current* allocatable,
        # which prepare() is about to change (MIG'ing a card retires its
        # nvidia.com/gpu units and advertises slices instead) — so mix does its
        # own check: is the geometry this layout needs already advertised?
        for resource, needed in sorted(layout.slice_demand.items()):
            advertised = allocatable_gpus(kubectl, resource)
            if advertised < needed:
                problems.append(
                    f'the layout needs {needed} x {resource} but the cluster currently '
                    f'advertises {advertised} — geometry is not applied yet. prepare() will '
                    f'apply it via the GPU Operator MIG manager if present; otherwise apply '
                    f'this nvidia-mig-parted config and retry:\n'
                    + layout_to_mig_parted_config(layout))
        if not gpu_runtime_class:
            nvidia_rc = nvidia_runtimeclass(kubectl)
            if nvidia_rc:
                problems.append(f'a {nvidia_rc!r} RuntimeClass exists but no --gpu-runtime-class '
                                f'was given — if the NVIDIA runtime is not this node\'s containerd '
                                f'default, GPU pods will start with no device. Fix: deploy with '
                                f'--gpu-runtime-class {nvidia_rc}')
        return problems

    def _mig_manager_pods(self, kubectl : str) -> List[tuple]:
        '''``(namespace, pod)`` of the GPU Operator's MIG manager, or [] if absent.'''
        # Silence-tolerant read: cluster._kubectl_out semantics, done inline to
        # keep the gpu <-> cluster import one-directional at module scope.
        try:
            proc = subprocess.run([kubectl, 'get', 'pods', '-A', '-l', 'app=nvidia-mig-manager',
                                   '-o', 'jsonpath={range .items[*]}{.metadata.namespace} {.metadata.name}{"\\n"}{end}'],
                                  capture_output = True, text = True, check = False)
        except FileNotFoundError:
            return []
        if proc.returncode != 0:
            return []
        return [tuple(line.split()) for line in proc.stdout.strip().splitlines() if line.split()]

    def prepare(self, demand : Optional[dict[str, int]] = None, kubectl : str = 'kubectl') -> None:
        '''
        Applies the layout's MIG geometry through the GPU Operator's MIG manager:
        publish the generated mig-parted config as a ConfigMap, record each MIG'd
        node's previous ``nvidia.com/mig.config`` label in
        ``MIG_RESTORE_ANNOTATION``, set the label, and wait for
        ``nvidia.com/mig.config.state=success``. Without a MIG manager it fails
        actionably, with the config to apply by hand.
        '''
        layout = self._layout
        if layout is None:
            raise RuntimeError('mix prepare() called before resolve_specs() — deploy through the '
                               'videoflow CLI, which resolves the layout first.')
        nodes = layout.mig_nodes()
        if not nodes:
            return
        config = layout_to_mig_parted_config(layout)
        managers = self._mig_manager_pods(kubectl)
        if not managers:
            raise RuntimeError(
                'no GPU Operator MIG manager found (pods labeled app=nvidia-mig-manager) — '
                'videoflow cannot apply MIG geometry itself. Apply this nvidia-mig-parted '
                'config to the nodes below and redeploy:\n' + config)
        namespace = managers[0][0]
        configmap = {
            'apiVersion': 'v1', 'kind': 'ConfigMap',
            'metadata': {'name': 'videoflow-mig-parted-config', 'namespace': namespace},
            'data': {'config.yaml': config},
        }
        proc = subprocess.run([kubectl, 'apply', '-f', '-'], input = json.dumps(configmap),
                              capture_output = True, text = True, check = False)
        if proc.returncode != 0:
            raise RuntimeError(f'could not publish the mig-parted ConfigMap: {proc.stderr.strip()}')
        for node in nodes:
            self._label_node_for_mig(kubectl, node)
        deadline = time.monotonic() + MIG_APPLY_TIMEOUT_SECONDS
        pending = list(nodes)
        while pending:
            if time.monotonic() > deadline:
                raise RuntimeError(
                    f'MIG geometry did not reach state=success on {", ".join(pending)} within '
                    f'{MIG_APPLY_TIMEOUT_SECONDS}s — check the MIG manager logs '
                    f'(kubectl logs -n {namespace} -l app=nvidia-mig-manager).')
            still = []
            for node in pending:
                state = _kubectl_run(kubectl, 'get', 'node', node, '-o',
                                     'jsonpath={.metadata.labels.nvidia\\.com/mig\\.config\\.state}')
                if state == 'failed':
                    raise RuntimeError(
                        f'the MIG manager reported state=failed on node {node} — the generated '
                        f'geometry may need adjusting; config applied:\n' + config)
                if state != 'success':
                    still.append(node)
            pending = still
            if pending:
                time.sleep(MIG_APPLY_POLL_SECONDS)

    def _label_node_for_mig(self, kubectl : str, node : str) -> None:
        previous = _kubectl_run(kubectl, 'get', 'node', node, '-o',
                                'jsonpath={.metadata.labels.nvidia\\.com/mig\\.config}')
        recorded = _kubectl_run(kubectl, 'get', 'node', node, '-o',
                                'jsonpath={.metadata.annotations.videoflow\\.io/mig-config-restore}')
        if not recorded:
            # Idempotent across a retried prepare: only the first attempt records
            # the pre-videoflow value ('' = the label was absent).
            _kubectl_run(kubectl, 'annotate', 'node', node, '--overwrite',
                         f'{MIG_RESTORE_ANNOTATION}={previous}')
        _kubectl_run(kubectl, 'label', 'node', node, '--overwrite',
                     f'{MIG_CONFIG_LABEL}=videoflow-{node}')

    def cleanup(self, kubectl : str = 'kubectl') -> None:
        '''
        Restores every node carrying ``MIG_RESTORE_ANNOTATION`` to its recorded
        pre-videoflow ``nvidia.com/mig.config`` value (removing the label where
        it was absent), then drops the annotation and the published ConfigMap.
        State lives entirely in the cluster, so this works from a teardown that
        shares nothing with the deploy that ran prepare — and is a no-op when
        prepare never ran.
        '''
        try:
            proc = subprocess.run([kubectl, 'get', 'nodes', '-o', 'json'],
                                  capture_output = True, text = True, check = False)
            nodes = json.loads(proc.stdout).get('items', []) if proc.returncode == 0 else []
        except (FileNotFoundError, ValueError):
            nodes = []
        for node in nodes:
            meta = node.get('metadata') or {}
            annotations = meta.get('annotations') or {}
            if MIG_RESTORE_ANNOTATION not in annotations:
                continue
            name = meta.get('name', '')
            previous = annotations[MIG_RESTORE_ANNOTATION]
            try:
                if previous:
                    _kubectl_run(kubectl, 'label', 'node', name, '--overwrite',
                                 f'{MIG_CONFIG_LABEL}={previous}')
                else:
                    _kubectl_run(kubectl, 'label', 'node', name, f'{MIG_CONFIG_LABEL}-')
                _kubectl_run(kubectl, 'annotate', 'node', name, f'{MIG_RESTORE_ANNOTATION}-')
            except RuntimeError as e:
                # Best-effort per node: one stuck node must not abort the rest.
                logger.warning(f'mix cleanup could not restore node {name}: {e}')
        for namespace, _pod in self._mig_manager_pods(kubectl):
            try:
                _kubectl_run(kubectl, 'delete', 'configmap', 'videoflow-mig-parted-config',
                             '-n', namespace, '--ignore-not-found')
            except RuntimeError as e:
                logger.warning(f'mix cleanup could not delete the mig-parted ConfigMap: {e}')
            break


# -- registry --------------------------------------------------------------

_GPU_STRATEGIES : dict[str, GpuStrategy] = {}

def register_gpu_mode(strategy : GpuStrategy) -> None:
    '''
    Registers a GPU allocation strategy under its ``name``. Registering makes the
    mode selectable via ``--gpu-mode`` — the CLI builds its choices from here — so
    a new strategy needs no CLI edit.

    - Arguments:
        - strategy: the strategy instance.

    - Raises:
        - ValueError: the strategy has no ``name``.
    '''
    if not strategy.name:
        raise ValueError(f'{type(strategy).__name__} must set a non-empty name')
    _GPU_STRATEGIES[strategy.name] = strategy

def registered_gpu_modes() -> list[str]:
    '''Registered GPU mode names, sorted. The CLI's ``--gpu-mode`` choices.'''
    return sorted(_GPU_STRATEGIES)

def get_gpu_mode(name : str) -> GpuStrategy:
    '''
    The strategy registered under ``name``.

    - Raises:
        - ValueError: no strategy is registered under that name; the message \
            names the known modes and ``register_gpu_mode``.
    '''
    strategy = _GPU_STRATEGIES.get(name)
    if strategy is None:
        # May belong to an installed-but-unimported package. Called through the
        # module object so the entry-point scan stays a patchable seam.
        plugins.load_plugin_group(GPU_STRATEGY_ENTRY_POINT_GROUP)
        strategy = _GPU_STRATEGIES.get(name)
    if strategy is None:
        raise ValueError(
            f'gpu_mode must be one of {tuple(registered_gpu_modes())}, got {name!r}. '
            f'Register another with videoflow.deploy.gpu.register_gpu_mode.')
    return strategy

register_gpu_mode(ExclusiveGpu())
register_gpu_mode(MixGpu())
