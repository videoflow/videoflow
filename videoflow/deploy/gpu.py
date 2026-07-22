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
from typing import Any, List, Optional

from ..core.compiler import NodeSpec
from ..utils import plugins
from .mig import GpuLayout, layout_to_mig_parted_config, solve_layout

logger = logging.getLogger(__package__)

#: Extended resource used when neither the node nor the deploy names one.
DEFAULT_GPU_RESOURCE = 'nvidia.com/gpu'

#: Node label a GPU pod selects on, and the taint key it tolerates.
GPU_POOL_LABEL = 'videoflow.io/gpu-pool'
GPU_TAINT_KEY = 'nvidia.com/gpu'

#: Node label recording which flow owns a node's MIG geometry. The cluster is
#: multi-tenant: mix's prepare() stamps it (compare-and-swap, no --overwrite)
#: before partitioning, other flows exclude stamped nodes from planning and
#: scheduling, and cleanup() restores only the nodes its flow stamped. The value
#: is ``manifests.k8s_name(flow_id)`` — identical to the pods' flow-id label.
GPU_OWNER_LABEL = 'videoflow.io/gpu-owner'

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

#: ConfigMap videoflow publishes in the operator namespace: the operator's
#: current mig-parted config with the generated videoflow-<node> entries merged
#: in. The MIG manager only reads the ConfigMap named in ClusterPolicy
#: migManager.config.name, so prepare() points that field here for the run.
MIG_CONFIGMAP_NAME = 'videoflow-mig-parted-config'

#: The GPU Operator's stock mig-parted ConfigMap — the merge base when
#: ClusterPolicy names none, and what cleanup() restores when the
#: migManager.config.name field was absent before videoflow touched it.
MIG_OPERATOR_DEFAULT_CONFIGMAP = 'default-mig-parted-config'

#: ClusterPolicy annotation recording the pre-videoflow migManager.config.name,
#: so a teardown in a fresh shell can restore it (the ClusterPolicy twin of
#: MIG_RESTORE_ANNOTATION on nodes).
MIG_CONFIG_NAME_RESTORE_ANNOTATION = 'videoflow.io/mig-config-name-restore'

#: Sentinel recorded in MIG_CONFIG_NAME_RESTORE_ANNOTATION when the field was
#: absent — never '', which a failed read could be mistaken for.
MIG_CONFIG_NAME_ABSENT = '__absent__'

#: Config entry always injected into the merged file so cleanup() can un-MIG a
#: node whose pre-videoflow mig.config label was absent: removing the label
#: triggers no reconfiguration, so such nodes are pointed here first.
MIG_DISABLED_CONFIG = 'videoflow-all-disabled'

#: How long prepare() waits for the mig-manager DaemonSet to remount the
#: videoflow ConfigMap after the ClusterPolicy patch.
MIG_MANAGER_ROLLOUT_TIMEOUT_SECONDS = 300


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


def _kubectl_json(kubectl : str, *args : str) -> Optional[dict]:
    '''Silence-tolerant kubectl read: ``kubectl *args -o json`` parsed, or None
    on any failure (kubectl missing, non-zero exit, unparseable output) — the
    read-side twin of ``_kubectl_run``, for callers that degrade gracefully.'''
    try:
        proc = subprocess.run([kubectl, *args, '-o', 'json'],
                              capture_output = True, text = True, check = False)
    except FileNotFoundError:
        return None
    if proc.returncode != 0:
        return None
    try:
        return json.loads(proc.stdout)
    except ValueError:
        return None


def _cluster_policy(kubectl : str) -> Optional[dict]:
    '''
    The GPU Operator's ClusterPolicy object (``clusterpolicies.nvidia.com``,
    cluster-scoped), or None when the CRD or the object is absent — mix then
    degrades to an actionable error rather than a crash. The operator enforces
    a single instance; should several exist anyway, the first by name is used
    with a warning.
    '''
    listing = _kubectl_json(kubectl, 'get', 'clusterpolicies.nvidia.com')
    items = (listing or {}).get('items') or []
    if not items:
        return None
    items.sort(key = lambda item: (item.get('metadata') or {}).get('name', ''))
    if len(items) > 1:
        names = ', '.join((i.get('metadata') or {}).get('name', '?') for i in items)
        logger.warning(f'multiple ClusterPolicy objects found ({names}) — using the first')
    return items[0]


def _operator_configmap_yaml(kubectl : str, namespace : str, name : str) -> str:
    '''The ``config.yaml`` payload of a mig-parted ConfigMap, or '' when the map
    or the key is absent. JSON + dict access rather than jsonpath: the dotted
    key needs no escaping and absent-vs-empty stays unambiguous.'''
    configmap = _kubectl_json(kubectl, 'get', 'configmap', name, '-n', namespace)
    return ((configmap or {}).get('data') or {}).get('config.yaml', '')


def _merge_mig_configs(base_config : str, videoflow_config : str) -> str:
    '''
    The operator's current mig-parted file with videoflow's generated configs
    merged in, plus a ``MIG_DISABLED_CONFIG`` entry cleanup() can point a node
    at to un-partition it.

    Merging rather than replacing is a correctness requirement: once
    ClusterPolicy points at the merged copy, the operator's
    ``migManager.config.default`` and any pre-existing node ``mig.config``
    labels must keep resolving in the mounted file. Videoflow wins name
    collisions — its entries are ``videoflow-``-prefixed, so there should be
    none.

    - Arguments:
        - base_config: the operator's current ``config.yaml`` text ('' or \
            unparseable starts the merge from videoflow's entries only).
        - videoflow_config: ``layout_to_mig_parted_config`` output.
    '''
    # Function-level: PyYAML ships via the deploy extras; core must import this
    # module without it. (Text-level merging was rejected — the operator's file
    # is arbitrary YAML, with indentation and anchors we cannot safely splice.)
    import yaml

    try:
        base = yaml.safe_load(base_config)
    except yaml.YAMLError:
        base = None
    if not isinstance(base, dict):
        if base_config.strip():
            logger.warning('could not parse the operator\'s mig-parted config.yaml — '
                           'the merged config starts from videoflow\'s entries only')
        base = {}
    merged : dict[str, Any] = {'version': base.get('version') or 'v1',
                               'mig-configs': dict(base.get('mig-configs') or {})}
    ours = yaml.safe_load(videoflow_config)        # generated by us: always valid
    merged['mig-configs'].update(ours.get('mig-configs') or {})
    merged['mig-configs'][MIG_DISABLED_CONFIG] = [{'devices': 'all', 'mig-enabled': False}]
    return yaml.safe_dump(merged, sort_keys = False, default_flow_style = False)


def _point_cluster_policy_at(kubectl : str, policy_name : str, configmap_name : str) -> None:
    '''Patches ClusterPolicy ``migManager.config.name`` to ``configmap_name``
    (merge patch, so a missing path is created rather than rejected).

    - Raises:
        - RuntimeError: the patch failed — this mutates cluster state, so \
            silence is not an option.
    '''
    patch = json.dumps({'spec': {'migManager': {'config': {'name': configmap_name}}}})
    _kubectl_run(kubectl, 'patch', 'clusterpolicies.nvidia.com', policy_name,
                 '--type', 'merge', '-p', patch)


def _wait_for_mig_manager_rollout(kubectl : str, namespace : str, configmap_name : str,
                                  timeout_seconds : Optional[int] = None) -> None:
    '''
    Blocks until the mig-manager DaemonSet mounts ``configmap_name`` and its
    pods have rolled to that spec. Polls the DaemonSet object rather than using
    ``kubectl rollout status``: the operator propagates ClusterPolicy into the
    DaemonSet asynchronously (an immediate status check happily passes on the
    old generation), and some operator versions recreate the DaemonSet instead
    of updating it.

    - Arguments:
        - timeout_seconds: None means MIG_MANAGER_ROLLOUT_TIMEOUT_SECONDS, \
            resolved at call time so tests can shrink the module constant.
    - Raises:
        - RuntimeError: the rollout did not complete within the timeout.
    '''
    if timeout_seconds is None:
        timeout_seconds = MIG_MANAGER_ROLLOUT_TIMEOUT_SECONDS
    deadline = time.monotonic() + timeout_seconds
    while True:
        listing = _kubectl_json(kubectl, 'get', 'daemonsets', '-n', namespace,
                                '-l', 'app=nvidia-mig-manager')
        for ds in (listing or {}).get('items') or []:
            template_spec = (((ds.get('spec') or {}).get('template') or {}).get('spec') or {})
            mounts_config = any((volume.get('configMap') or {}).get('name') == configmap_name
                                for volume in template_spec.get('volumes') or [])
            status = ds.get('status') or {}
            desired = status.get('desiredNumberScheduled', -1)
            rolled = (status.get('observedGeneration', 0) >= (ds.get('metadata') or {}).get('generation', 1)
                      and desired >= 0
                      and status.get('updatedNumberScheduled') == desired
                      and status.get('numberReady') == desired)
            if mounts_config and rolled:
                return
        if time.monotonic() > deadline:
            raise RuntimeError(
                f'the MIG manager DaemonSet did not remount {configmap_name} within '
                f'{timeout_seconds}s of the ClusterPolicy patch — check the GPU Operator '
                f'(kubectl get ds -n {namespace} -l app=nvidia-mig-manager; '
                f'kubectl logs -n {namespace} -l app=gpu-operator).')
        time.sleep(MIG_APPLY_POLL_SECONDS)


def _wait_for_mig_state(kubectl : str, nodes : List[str],
                        timeout_seconds : Optional[int] = None) -> dict[str, str]:
    '''
    Polls each node's ``nvidia.com/mig.config.state`` label until it is
    terminal and returns ``{node: 'success' | 'failed' | 'timeout'}``. Never
    raises: prepare treats failed/timeout as fatal while cleanup only warns, so
    severity is the caller's decision. A transient read error counts as
    still-pending rather than aborting a long wait.

    - Arguments:
        - timeout_seconds: None means MIG_APPLY_TIMEOUT_SECONDS, resolved at \
            call time so tests can shrink the module constant.
    '''
    if timeout_seconds is None:
        timeout_seconds = MIG_APPLY_TIMEOUT_SECONDS
    states = dict.fromkeys(nodes, 'timeout')
    deadline = time.monotonic() + timeout_seconds
    pending = list(nodes)
    while pending:
        still = []
        for node in pending:
            try:
                state = _kubectl_run(kubectl, 'get', 'node', node, '-o',
                                     'jsonpath={.metadata.labels.nvidia\\.com/mig\\.config\\.state}')
            except RuntimeError:
                still.append(node)
                continue
            if state in ('success', 'failed'):
                states[node] = state
            else:
                still.append(node)
        pending = still
        if not pending or time.monotonic() > deadline:
            break
        time.sleep(MIG_APPLY_POLL_SECONDS)
    return states


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
        Applies the layout's MIG geometry through the GPU Operator: merge the
        generated mig-parted config into the operator's current one, publish
        the result as ``MIG_CONFIGMAP_NAME``, point ClusterPolicy
        ``migManager.config.name`` at it (recording the original name in
        ``MIG_CONFIG_NAME_RESTORE_ANNOTATION``), wait for the mig-manager
        DaemonSet to remount, then label each MIG'd node ``videoflow-<node>``
        (recording its previous label in ``MIG_RESTORE_ANNOTATION``) and wait
        for ``nvidia.com/mig.config.state=success``. The manager only reads the
        ConfigMap that ClusterPolicy names — a side ConfigMap it never mounts
        cannot carry the config. Without a MIG manager or a ClusterPolicy it
        fails actionably, with the config to apply by hand.
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
        policy = _cluster_policy(kubectl)
        if policy is None:
            raise RuntimeError(
                'a MIG manager is running but no ClusterPolicy (clusterpolicies.nvidia.com) '
                'was found — videoflow wires its config through ClusterPolicy '
                'migManager.config.name and cannot reach a standalone mig-manager. Mount and '
                'select this nvidia-mig-parted config yourself and redeploy:\n' + config)
        policy_name = (policy.get('metadata') or {}).get('name', '')
        original = (((policy.get('spec') or {}).get('migManager') or {}).get('config') or {}).get('name')
        base_name = original or MIG_OPERATOR_DEFAULT_CONFIGMAP
        base = _operator_configmap_yaml(kubectl, namespace, base_name)
        if not base and base_name != MIG_OPERATOR_DEFAULT_CONFIGMAP:
            # The named ConfigMap is gone (e.g. a prior unclean run left the
            # policy pointing at ours and something deleted it) — fall back to
            # the stock file so the merge keeps the operator's entries.
            base = _operator_configmap_yaml(kubectl, namespace, MIG_OPERATOR_DEFAULT_CONFIGMAP)
        if not base:
            logger.warning(f'no readable mig-parted config in ConfigMap {base_name!r} — '
                           f'the merged config will carry only videoflow\'s entries')
        merged = _merge_mig_configs(base, config)
        configmap = {
            'apiVersion': 'v1', 'kind': 'ConfigMap',
            'metadata': {'name': MIG_CONFIGMAP_NAME, 'namespace': namespace},
            'data': {'config.yaml': merged},
        }
        proc = subprocess.run([kubectl, 'apply', '-f', '-'], input = json.dumps(configmap),
                              capture_output = True, text = True, check = False)
        if proc.returncode != 0:
            raise RuntimeError(f'could not publish the mig-parted ConfigMap: {proc.stderr.strip()}')
        annotations = (policy.get('metadata') or {}).get('annotations') or {}
        if MIG_CONFIG_NAME_RESTORE_ANNOTATION not in annotations:
            # Record before patching, so the restore record exists in-cluster
            # before the mutation it undoes. Never record our own name as the
            # "original" — a prior run that left the patch but lost its record
            # would make cleanup a no-op forever.
            if original == MIG_CONFIGMAP_NAME:
                logger.warning(f'ClusterPolicy already points at {MIG_CONFIGMAP_NAME} with no '
                               f'restore record — cleanup will restore the operator default')
                record = MIG_CONFIG_NAME_ABSENT
            else:
                record = original or MIG_CONFIG_NAME_ABSENT
            _kubectl_run(kubectl, 'annotate', 'clusterpolicies.nvidia.com', policy_name,
                         f'{MIG_CONFIG_NAME_RESTORE_ANNOTATION}={record}')
        if original != MIG_CONFIGMAP_NAME:
            _point_cluster_policy_at(kubectl, policy_name, MIG_CONFIGMAP_NAME)
        # The ClusterPolicy change swaps the DaemonSet's config volume; a node
        # labeled before the new pods mount it is judged against the old file
        # and stamped state=failed, which the wait below treats as fatal.
        _wait_for_mig_manager_rollout(kubectl, namespace, MIG_CONFIGMAP_NAME)
        for node in nodes:
            self._label_node_for_mig(kubectl, node)
        states = _wait_for_mig_state(kubectl, nodes)
        failed = sorted(node for node, state in states.items() if state == 'failed')
        if failed:
            raise RuntimeError(
                f'the MIG manager reported state=failed on {", ".join(failed)} — the generated '
                f'geometry may need adjusting; config applied:\n' + config)
        timed_out = sorted(node for node, state in states.items() if state == 'timeout')
        if timed_out:
            raise RuntimeError(
                f'MIG geometry did not reach state=success on {", ".join(timed_out)} within '
                f'{MIG_APPLY_TIMEOUT_SECONDS}s — check the MIG manager logs '
                f'(kubectl logs -n {namespace} -l app=nvidia-mig-manager).')

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
        pre-videoflow ``nvidia.com/mig.config`` value — a node whose label was
        absent is first pointed at ``MIG_DISABLED_CONFIG`` so the manager
        actually un-partitions the cards (removing the label triggers nothing),
        then unlabeled — and waits for ``mig.config.state=success`` before
        declaring a node done. Only when every node reverted does it restore
        ClusterPolicy ``migManager.config.name`` from
        ``MIG_CONFIG_NAME_RESTORE_ANNOTATION`` and delete the published
        ConfigMap; a node that failed keeps its annotation, and the policy
        patch and ConfigMap stay wired, so a retried teardown can resume.
        State lives entirely in the cluster, so this works from a teardown that
        shares nothing with the deploy that ran prepare — and is a no-op when
        prepare never ran.
        '''
        listing = _kubectl_json(kubectl, 'get', 'nodes')
        nodes = (listing or {}).get('items') or []
        policy = _cluster_policy(kubectl)
        policy_name = ((policy or {}).get('metadata') or {}).get('name', '')
        current_name = ((((policy or {}).get('spec') or {}).get('migManager') or {})
                        .get('config') or {}).get('name')
        # The disabled config must exist in the file the manager currently
        # mounts: ours while the policy still points at the merged copy, the
        # stock all-disabled once an earlier partial cleanup restored it.
        disabled = MIG_DISABLED_CONFIG if current_name == MIG_CONFIGMAP_NAME else 'all-disabled'
        restored : dict[str, str] = {}     # node -> recorded previous label value
        unrestored = False
        for node in nodes:
            meta = node.get('metadata') or {}
            annotations = meta.get('annotations') or {}
            if MIG_RESTORE_ANNOTATION not in annotations:
                continue
            name = meta.get('name', '')
            previous = annotations[MIG_RESTORE_ANNOTATION]
            try:
                _kubectl_run(kubectl, 'label', 'node', name, '--overwrite',
                             f'{MIG_CONFIG_LABEL}={previous or disabled}')
                restored[name] = previous
            except RuntimeError as e:
                # Best-effort per node: one stuck node must not abort the rest.
                unrestored = True
                logger.warning(f'mix cleanup could not restore node {name}: {e}')
        states = _wait_for_mig_state(kubectl, sorted(restored)) if restored else {}
        for name in sorted(restored):
            if states.get(name) != 'success':
                unrestored = True
                logger.warning(
                    f'node {name} did not reach mig.config.state=success while reverting '
                    f'(state: {states.get(name)}) — its restore annotation is kept; check the '
                    f'MIG manager logs and re-run videoflow teardown --gpu-mode mix to retry.')
                continue
            try:
                if not restored[name]:
                    # The label was absent before videoflow; the disabled config
                    # has done its job, so the label itself can go now.
                    _kubectl_run(kubectl, 'label', 'node', name, f'{MIG_CONFIG_LABEL}-')
                _kubectl_run(kubectl, 'annotate', 'node', name, f'{MIG_RESTORE_ANNOTATION}-')
            except RuntimeError as e:
                unrestored = True
                logger.warning(f'mix cleanup could not finish restoring node {name}: {e}')
        if unrestored:
            logger.warning(
                f'mix cleanup left the ClusterPolicy patch and the {MIG_CONFIGMAP_NAME} '
                f'ConfigMap in place — a retried teardown needs them to finish reverting '
                f'the nodes above.')
            return
        if policy is not None:
            recorded = ((policy.get('metadata') or {}).get('annotations') or {}) \
                .get(MIG_CONFIG_NAME_RESTORE_ANNOTATION)
            if recorded:
                # Restoring "absent" by setting the operator default explicitly
                # is semantically identical and avoids a JSON-patch remove op
                # racing CRD defaulting.
                target = MIG_OPERATOR_DEFAULT_CONFIGMAP if recorded == MIG_CONFIG_NAME_ABSENT else recorded
                try:
                    _point_cluster_policy_at(kubectl, policy_name, target)
                    _kubectl_run(kubectl, 'annotate', 'clusterpolicies.nvidia.com', policy_name,
                                 f'{MIG_CONFIG_NAME_RESTORE_ANNOTATION}-')
                except RuntimeError as e:
                    logger.warning(f'mix cleanup could not restore ClusterPolicy '
                                   f'migManager.config.name: {e}')
        for namespace, _pod in self._mig_manager_pods(kubectl):
            try:
                _kubectl_run(kubectl, 'delete', 'configmap', MIG_CONFIGMAP_NAME,
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
