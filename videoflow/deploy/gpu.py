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

import collections
import dataclasses
import hashlib
import json
import logging
import subprocess
import time
import uuid
from typing import AbstractSet, Any, Callable, Dict, List, Mapping, Optional, Sequence

from ..backends.outcomes import Observation, Unknown, known, unknown, value_or
from ..core.compiler import NodeSpec
from ..core.errors import OwnershipConflict, UnobservableState
from ..core.provenance import (
    FIELD_GPU_RESOURCE_NAME,
    SOURCE_CLI_DEFAULT,
    SOURCE_DEFAULT,
    SOURCE_STRATEGY,
    Declaration,
    resolve_gpu_requirements,
)
from ..utils import plugins
from .mig import GpuLayout, LayoutError, NodeInventory, layout_to_mig_parted_config, solve_layout

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
#: Companion of ``GPU_OWNER_LABEL``, stamped in the same write with a fresh
#: per-claim value: a release can then tell the claim it is undoing from a
#: later re-claim by the same flow (a crashed deploy's teardown racing a
#: redeploy), and leave the newer claim standing.
GPU_OWNER_EPOCH_LABEL = 'videoflow.io/gpu-owner-epoch'

GPU_STRATEGY_ENTRY_POINT_GROUP = 'videoflow.gpu_strategies'

#: Marker prefix for a preflight problem that is fatal regardless of
#: ``--strict-preflight``: the request is impossible by construction (a multi-unit
#: claim against a MIG or time-sliced resource), so deploying anyway can only end
#: in an admission error or a silently broken visibility contract. A constant so
#: the CLI's check never depends on message prose.
IMPOSSIBLE_GPU_REQUEST = 'impossible GPU request'

#: Prefix of a preflight problem saying that an occupancy or inventory read the
#: capacity math depends on could not be made. The numbers that follow assume
#: an idle pool — an upper bound, not a fact. Fatal for a mode that mutates the
#: cluster on the strength of it (mix repartitions cards); for exclusive claims
#: it is a warning ``--strict-preflight`` promotes.
UNOBSERVABLE_GPU_STATE = 'unobservable GPU state'

def resolve_gpu_resource(spec : NodeSpec, default : Optional[str] = None) -> str:
    '''The extended-resource name a GPU spec requests: the spec's resolved name
    (internal — only a GPU strategy sets it, e.g. ``mix`` assigning a MIG profile),
    else the deploy default (``--gpu-resource-name``), else ``nvidia.com/gpu``.'''
    return gpu_resource_provenance(spec, default)[0]

def gpu_resource_provenance(spec : NodeSpec, default : Optional[str] = None) -> tuple[str, str]:
    '''
    ``resolve_gpu_resource`` with its answer's source: ``(name, source)`` where
    the source is ``'strategy'`` (the spec's resolved name), ``'cli-default'``
    (``--gpu-resource-name``) or ``'default'`` (``nvidia.com/gpu``). The one
    merge point for the resource name, expressed as declarations to the shared
    resolver in ``videoflow.core.provenance`` so precedence and provenance are
    stated once: a strategy's resolution is a hard decision, the two defaults
    are soft.
    '''
    declarations = []
    if spec.gpu_resource_name:
        declarations.append(Declaration(FIELD_GPU_RESOURCE_NAME, spec.gpu_resource_name, SOURCE_STRATEGY))
    if default:
        declarations.append(Declaration(FIELD_GPU_RESOURCE_NAME, default, SOURCE_CLI_DEFAULT, hard = False))
    declarations.append(Declaration(FIELD_GPU_RESOURCE_NAME, DEFAULT_GPU_RESOURCE, SOURCE_DEFAULT, hard = False))
    resolution = resolve_gpu_requirements(declarations, subject = 'gpu resource')
    return resolution.values[FIELD_GPU_RESOURCE_NAME], resolution.provenance[FIELD_GPU_RESOURCE_NAME]

def _is_gpu_resource(name : str) -> bool:
    '''
    Whether an extended resource claims accelerator units — ``nvidia.com/gpu``
    (whole cards, or the time-sliced/MPS replicas advertised under that name),
    ``nvidia.com/mig-*`` slices, or another vendor's ``<domain>/gpu``. Anything
    else a pod holds (hugepages, SR-IOV VFs, a custom device plugin) occupies
    no card and must not count against a node's GPUs.
    '''
    domain, sep, unit = name.partition('/')
    if not sep:
        return False
    return unit == 'gpu' or unit.startswith('gpu.') or (domain == 'nvidia.com' and unit.startswith('mig-'))

def _occupancy_unknown_problem(observed : Unknown) -> str:
    return (f'{UNOBSERVABLE_GPU_STATE}: could not list running pods ({observed.reason}: '
            f'{observed.detail}) — free GPU capacity is unknown, not zero, and the counts '
            f'below assume an idle pool. Fix: restore `kubectl get pods -A` (RBAC: list pods '
            f'in all namespaces) before trusting the capacity math')

def _capacity_problems(kubectl : str, demand : Optional[dict[str, int]],
                       exclude_nodes : AbstractSet[str] = frozenset(),
                       in_use : Optional[dict[str, dict[str, int]]] = None) -> List[str]:
    '''
    Demand-vs-free-capacity problems for whole-unit extended-resource claims,
    shared by the exclusive preflight and mix's spanner check. Free capacity is
    pool allocatable minus what running pods already hold — raw allocatable lies
    in a multi-tenant cluster, where the scheduler will not grant units another
    workload owns. ``exclude_nodes`` drops nodes the caller has ruled out of
    planning (mix: nodes owned by other flows) so the arithmetic matches what
    will actually be deployed; ``in_use`` takes a pre-fetched occupancy map so
    multi-resource callers list pods once. When the pod listing cannot be read
    the problems open with ``UNOBSERVABLE_GPU_STATE`` and the arithmetic runs
    against an assumed-idle pool: unknown is reported as unknown, never as zero.
    '''
    # Function-level: cluster.py imports this module at module scope (for
    # get_gpu_mode), so importing it back at module scope here would be a cycle.
    from .cluster import gpu_availability, gpu_units_in_use_observed

    problems : List[str] = []
    if in_use is None:
        observed = gpu_units_in_use_observed(kubectl)
        if isinstance(observed, Unknown):
            problems.append(_occupancy_unknown_problem(observed))
        in_use = value_or(observed, {})
    for resource in sorted(demand) if demand else [DEFAULT_GPU_RESOURCE]:
        availability = gpu_availability(kubectl, resource, in_use = in_use,
                                        exclude_nodes = exclude_nodes)
        if availability.allocatable == 0:
            if resource == DEFAULT_GPU_RESOURCE:
                problems.append('no node advertises nvidia.com/gpu — install the NVIDIA device '
                                'plugin: kubectl apply -f https://raw.githubusercontent.com/NVIDIA/'
                                'k8s-device-plugin/v0.16.2/deployments/static/nvidia-device-plugin.yml')
            else:
                problems.append(f'no node advertises {resource} — the flow requests it '
                                f'(--gpu-resource-name) but the cluster does not expose it')
        elif demand and demand[resource] > availability.free:
            needed = demand[resource]
            if availability.in_use:
                problems.append(
                    f'flow demands {needed} x {resource} but the pool has only '
                    f'{availability.free} free ({availability.in_use} of '
                    f'{availability.allocatable} allocatable in use by running pods) — '
                    f'{needed - availability.free} pod(s) will stay Pending and the flow will '
                    f'stall. Reduce GPU replicas, wait for the competing workloads, or add capacity')
            else:
                problems.append(
                    f'flow demands {needed} x {resource} but the cluster has only '
                    f'{availability.allocatable} allocatable — {needed - availability.allocatable} '
                    f'pod(s) will stay Pending and the flow will stall. Reduce GPU nodes/replicas, '
                    f'or enable device-plugin time-slicing (dev clusters; see the GPU sharing docs)')
    return problems

#: Bounds of the exhaustive placement search in ``pack_pod_claims``: within
#: them a negative answer is a proof; beyond them it is first-fit-decreasing's
#: opinion, and the problem string says so.
PACKING_EXACT_MAX_PODS = 12
PACKING_EXACT_MAX_NODES = 8

@dataclasses.dataclass(frozen = True)
class PodPacking:
    '''
    Whether a list of per-pod whole-unit claims fits a pool's per-node free
    units with every pod's whole claim on one node.

    - Attributes:
        - feasible: a placement was found.
        - proven: the answer is exact — a placement is always a proof, and an \
            infeasible answer is one when an exhaustive search found nothing. \
            False only for an infeasible answer on an instance too large to \
            search, where the scheduler might still succeed.
        - placement: claim index -> node, when feasible.
        - unplaced: the claim indices first-fit-decreasing could not place — \
            the evidence the report quotes when nothing fits.
    '''
    feasible : bool
    proven : bool
    placement : Dict[int, str]
    unplaced : tuple[int, ...]

def _first_fit_decreasing(claims : Sequence[int],
                          free : Mapping[str, int]) -> tuple[Dict[int, str], List[int]]:
    '''Largest claims first, each onto the first node (most free first, then by name) with room.'''
    remaining = dict(free)
    order = sorted(remaining, key = lambda node: (-remaining[node], node))
    placement : Dict[int, str] = {}
    unplaced : List[int] = []
    for index in sorted(range(len(claims)), key = lambda i: (-claims[i], i)):
        for node in order:
            if remaining[node] >= claims[index]:
                remaining[node] -= claims[index]
                placement[index] = node
                break
        else:
            unplaced.append(index)
    return placement, unplaced

def _exact_packing(claims : Sequence[int], free : Mapping[str, int]) -> Optional[Dict[int, str]]:
    '''
    Exhaustive search: claims largest first, backtracking over nodes. Two nodes
    with equal remaining capacity are interchangeable, so only one is tried per
    claim, and a (claim position, sorted remaining capacities) state that already
    failed is never searched twice — the pruning that keeps
    ``PACKING_EXACT_MAX_PODS`` x ``PACKING_EXACT_MAX_NODES`` instant.
    '''
    names = sorted(free)
    remaining = [free[name] for name in names]
    order = sorted(range(len(claims)), key = lambda i: (-claims[i], i))
    placement : Dict[int, str] = {}
    failed : set[tuple[int, tuple[int, ...]]] = set()

    def place(position : int) -> bool:
        if position == len(order):
            return True
        state = (position, tuple(sorted(remaining)))
        if state in failed:
            return False
        index = order[position]
        size = claims[index]
        tried : set[int] = set()
        for slot, name in enumerate(names):
            capacity = remaining[slot]
            if capacity < size or capacity in tried:
                continue
            tried.add(capacity)
            remaining[slot] -= size
            placement[index] = name
            if place(position + 1):
                return True
            remaining[slot] += size
            del placement[index]
        failed.add(state)
        return False

    return dict(placement) if place(0) else None

def pack_pod_claims(claims : Sequence[int], free : Mapping[str, int]) -> PodPacking:
    '''
    Decide whether every pod's whole-unit claim can be placed on one node of a
    pool with ``free`` units per node — the per-host check the aggregate
    arithmetic cannot make: per-node free ``[3, 3]`` against pods ``[2, 2, 2]``
    passes both the total (6 <= 6) and the largest-pod bound (2 <= 3), yet only
    two pods fit.

    First-fit-decreasing answers first, and a placement it finds is a proof.
    When it finds none and the instance is small (at most
    ``PACKING_EXACT_MAX_PODS`` pods over ``PACKING_EXACT_MAX_NODES`` nodes) an
    exhaustive search settles the question either way; a larger instance is
    reported infeasible but unproven.

    - Arguments:
        - claims: one entry per pod replica (``manifests.gpu_pod_claims``).
        - free: node -> free units of the resource being claimed.

    - Returns:
        - a ``PodPacking``.
    '''
    sizes = [int(c) for c in claims]
    capacity = {node: max(0, int(units)) for node, units in free.items()}
    placement, unplaced = _first_fit_decreasing(sizes, capacity)
    if not unplaced:
        return PodPacking(True, True, placement, ())
    if sum(sizes) > sum(capacity.values()) or max(sizes) > max(capacity.values(), default = 0):
        return PodPacking(False, True, {}, tuple(unplaced))
    if len(sizes) <= PACKING_EXACT_MAX_PODS and len(capacity) <= PACKING_EXACT_MAX_NODES:
        exact = _exact_packing(sizes, capacity)
        if exact is not None:
            return PodPacking(True, True, exact, ())
        return PodPacking(False, True, {}, tuple(unplaced))
    return PodPacking(False, False, {}, tuple(unplaced))

def _packing_problem(resource : str, claims : Sequence[int], free : Mapping[str, int],
                     packing : PodPacking) -> str:
    '''The preflight problem for a ``pack_pod_claims`` result that found no placement.'''
    by_size = collections.Counter(claims)
    sizes = ', '.join(f'{count} pod(s) x {size}' for size, count in sorted(by_size.items(), reverse = True))
    per_node = ', '.join(f'{node}={units}' for node, units in sorted(free.items()))
    left = sorted((claims[i] for i in packing.unplaced), reverse = True)
    if packing.proven:
        verdict = 'an exhaustive search proves that no host assignment fits every pod'
    else:
        verdict = (f'first-fit-decreasing found no host assignment, and {len(claims)} pods over '
                   f'{len(free)} nodes is too large for an exhaustive proof, so the scheduler may '
                   f'still find one')
    return (f'{len(claims)} GPU pod(s) ({sizes} x {resource}) cannot all be placed on the pool\'s '
            f'hosts (free per node: {per_node} — {sum(free.values())} free for {sum(claims)} demanded): '
            f'each replica\'s devices must sit on one host, and {verdict}; first-fit-decreasing left '
            f'{len(left)} pod(s) needing {left} unplaced, which would stay Pending and stall the flow. '
            f'Fix: free {max(left)} unit(s) on one host, add a node with >= {max(left)} free '
            f'{resource}, or reduce gpu_count/replicas to fit the per-host free counts.')

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
                        max_per_pod : Optional[dict[str, int]] = None,
                        pod_claims : Optional[dict[str, list[int]]] = None) -> List[str]:
        '''
        Strategy-specific preflight problems, each a string naming its fix. The
        flavor-independent checks (cluster reachable, a labeled GPU node) are run
        by ``cluster.gpu_preflight`` before this is called. A problem prefixed
        with ``IMPOSSIBLE_GPU_REQUEST`` is fatal regardless of
        ``--strict-preflight``.

        Third-party strategies should tolerate future keyword inputs (accept
        ``**kwargs``): new preflight inputs arrive as keywords with ``None``
        defaults, as ``max_per_pod`` (RFC 0003) and ``pod_claims`` did.

        - Arguments:
            - demand: extended-resource name -> units the flow requests, or None \
                to skip capacity comparison.
            - gpu_runtime_class: the ``--gpu-runtime-class`` value, if given.
            - max_per_pod: extended-resource name -> the largest single-pod claim \
                (``manifests.gpu_max_per_pod``), or None to skip per-node checks.
            - pod_claims: extended-resource name -> one claim per pod replica \
                (``manifests.gpu_pod_claims``), or None to skip the per-host \
                packing check (``pack_pod_claims``).
        '''
        return []

    def resolve_specs(self, specs : List[NodeSpec], kubectl : str = 'kubectl',
                    default_resource : Optional[str] = None,
                    flow_id : Optional[str] = None) -> List[NodeSpec]:
        '''
        The strategy's chance to decide names and geometry before anything is
        rendered or preflighted: called once per deploy, with the compiled specs,
        before ``gpu_demand``/``gpu_max_per_pod``/``render_manifests`` consume
        them. Default: identity. The ``mix`` strategy returns specs whose
        ``gpu_resource_name`` carries each sharer's solver-chosen MIG profile —
        after which the entire downstream pipeline runs unchanged.

        Like ``preflight_problems``, new lifecycle inputs arrive as keywords with
        ``None`` defaults (``flow_id`` did): third-party strategies should accept
        ``**kwargs``. ``flow_id`` identifies the deploying flow so a multi-tenant
        strategy can tell its own cluster state from another flow's.

        - Raises:
            - ValueError: the flow's demands cannot be laid out (``mix``'s \
                ``LayoutError`` is one) — the deploy should stop before rendering.
        '''
        return specs

    def prepare(self, demand : Optional[dict[str, int]] = None, kubectl : str = 'kubectl',
                flow_id : Optional[str] = None) -> None:
        '''
        Cluster setup this strategy needs before a run's manifests are applied.
        Default: nothing. A strategy that mutates cluster state here is
        responsible for restoring it in ``cleanup`` — and, in a multi-tenant
        cluster, for marking that state with ``flow_id`` so concurrent flows
        keep out of each other's way.
        '''
        return None

    def cleanup(self, kubectl : str = 'kubectl', flow_id : Optional[str] = None) -> None:
        '''
        Undoes ``prepare``. Must be idempotent and tolerant: it is called after a
        ``prepare`` that only partly succeeded, and — for a REALTIME flow, whose
        lifetime outlives the deploy command — from a later ``videoflow teardown``
        that passes ``--gpu-mode`` but shares no state with the deploy that ran
        ``prepare``. So it cannot assume ``prepare`` completed, or ran at all.
        With a ``flow_id`` it must restore only that flow's state; without one it
        may sweep everything videoflow owns (single-operator escape hatch).
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
                        max_per_pod : Optional[dict[str, int]] = None,
                        pod_claims : Optional[dict[str, list[int]]] = None) -> List[str]:
        # Function-level: cluster.py imports this module at module scope (for
        # get_gpu_mode), so importing it back at module scope here would be a cycle.
        from .cluster import classify_gpu_resource, gpu_availability, gpu_units_in_use_observed, nvidia_runtimeclass

        observed = gpu_units_in_use_observed(kubectl)
        in_use = value_or(observed, {})
        problems = _capacity_problems(kubectl, demand, in_use = in_use)
        if isinstance(observed, Unknown):
            problems.insert(0, _occupancy_unknown_problem(observed))
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
            if kind == 'mps':
                problems.append(
                    f'{IMPOSSIBLE_GPU_REQUEST}: a node requests {per_pod} x {resource} in one '
                    f'pod, but {resource} is shared through MPS on this cluster — its units are '
                    f'client slots on a device with a per-client memory cap, not devices, so '
                    f'gpu_count > 1 lands the pod on shares of the same physical GPU. Spanning '
                    f'needs whole physical GPUs: disable MPS for this pool, or reduce gpu_count to 1.')
                continue
            if kind == 'unknown':
                problems.append(
                    f'cannot classify {resource} (no GPU Feature Discovery labels on the '
                    f'advertising nodes) — assuming its units are whole physical devices. '
                    f'If this pool is MIG- or time-sliced, gpu_count = {per_pod} will not '
                    f'behave as a multi-GPU grant.')
            availability = gpu_availability(kubectl, resource, in_use = in_use)
            # allocatable == 0 means the resource is unadvertised, already reported above.
            if availability.allocatable and per_pod > availability.max_free_on_node:
                held = ' (running pods hold the rest)' if availability.in_use else ''
                problems.append(
                    f'a node requests {per_pod} x {resource} in a single pod but the largest '
                    f'cluster node has only {availability.max_free_on_node} free{held} — all of '
                    f'one replica\'s GPUs must sit on one Kubernetes host, so the cluster total '
                    f'is irrelevant. Fix: add a node with >= {per_pod} GPUs, or reduce gpu_count.')
        # Per-host packing: the total and the largest-node bound above are
        # necessary, not sufficient — free [3, 3] against pods [2, 2, 2] passes
        # both and places only two pods. Every pod's claim is packed onto the
        # per-node free counts (allocatable minus what running pods hold); a
        # shortfall the aggregate checks already reported is not reported twice.
        for resource, claims in sorted((pod_claims or {}).items()):
            sizes = [int(c) for c in claims if int(c) > 0]
            if not sizes:
                continue
            availability = gpu_availability(kubectl, resource, in_use = in_use)
            if not availability.allocatable:
                continue                      # unadvertised: reported by the capacity check
            demanded = (demand or {}).get(resource)
            largest = (max_per_pod or {}).get(resource)
            if (demanded is not None and demanded > availability.free) or \
                    (largest is not None and largest > 1 and largest > availability.max_free_on_node):
                continue                      # already explained above
            free = {node: max(0, count - availability.per_node_in_use.get(node, 0))
                    for node, count in availability.per_node_allocatable.items()}
            packing = pack_pod_claims(sizes, free)
            if not packing.feasible:
                problems.append(_packing_problem(resource, sizes, free, packing))
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

#: Node annotation recording the node's current mig-parted entry name. Entry
#: names carry a per-run nonce (see ``_mig_config_name``), so unlike the old
#: fixed ``videoflow-<node>`` scheme they cannot be reconstructed — cleanup()
#: reads this annotation (and the mig.config label) to know which entries in
#: the shared ConfigMap are this flow's to strip. Stamped before the ConfigMap
#: publish, so even a prepare that crashes between publishing and labeling
#: leaves the record.
MIG_ENTRY_ANNOTATION = 'videoflow.io/mig-entry'

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

#: ConfigMap annotation the last flow out stamps on ``MIG_CONFIGMAP_NAME``
#: instead of deleting it (value: UTC time of retirement). kubectl cannot
#: express delete-with-precondition, and a map nothing references is harmless
#: where a wrong delete pulls the file out from under a manager still mounting
#: it — so retirement is strip + tombstone, and removal is the operator's:
#: kubectl delete configmap videoflow-mig-parted-config -n <gpu-operator namespace>
MIG_TOMBSTONE_ANNOTATION = 'videoflow.io/mig-config-tombstone'

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


def _mig_config_name(node : str, nonce : str) -> str:
    '''
    The mig-parted entry name (and ``nvidia.com/mig.config`` label value) for
    one node in one prepare() run: ``videoflow-<node>-<nonce>``. The nonce makes
    every fresh apply a label *change*: the MIG manager reacts only to changes,
    so re-applying a previous run's exact value against a leftover
    ``state=failed`` would read back ``failed`` forever (the retry deadlock).

    Label values cap at 63 characters while node names run to 253 (EC2 FQDNs),
    so an oversized name keeps a readable node prefix and swaps the rest for a
    short stable hash — two long names sharing a prefix still get distinct
    values, and the same node truncates the same way every run.
    '''
    value = f'videoflow-{node}-{nonce}'
    if len(value) > 63:
        digest = hashlib.sha256(node.encode()).hexdigest()[:8]
        keep = 63 - len(f'videoflow--{digest}-{nonce}')
        # A truncated prefix may end in '-' or '.', which a label value (and
        # the character before the digest separator) must not.
        value = f'videoflow-{node[:keep].rstrip("-._")}-{digest}-{nonce}'
    return value


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


def _rename_mig_config_entries(videoflow_config : str, names : dict[str, str]) -> str:
    '''
    The generated mig-parted config with each node's entry re-keyed from the
    readable ``videoflow-<node>`` that ``layout_to_mig_parted_config`` emits to
    that node's per-run name (``names``: node -> ``_mig_config_name`` value).
    Only the applied copy is renamed — the by-hand config printed when no MIG
    manager can apply it keeps the readable names.
    '''
    # Function-level: PyYAML via the deploy extras, as in _merge_mig_configs.
    import yaml

    parsed = yaml.safe_load(videoflow_config)      # generated by us: always valid
    parsed['mig-configs'] = {
        names.get(key.removeprefix('videoflow-'), key): entry
        for key, entry in (parsed.get('mig-configs') or {}).items()}
    return yaml.safe_dump(parsed, sort_keys = False, default_flow_style = False)


def _add_disabled_alias_entry(live_config : str, alias : str) -> str:
    '''
    The published mig-parted file with ``alias`` added as a copy of the
    ``MIG_DISABLED_CONFIG`` entry. cleanup() bounces a deadlocked node — one
    already labeled with its restore target while ``state=failed`` — through
    such an alias, because only a label *change* makes the manager re-apply.
    '''
    # Function-level: PyYAML via the deploy extras, as in _merge_mig_configs.
    import yaml

    try:
        live = yaml.safe_load(live_config) if live_config else None
    except yaml.YAMLError:
        live = None
    if not isinstance(live, dict):
        live = {'version': 'v1', 'mig-configs': {}}
    configs = dict(live.get('mig-configs') or {})
    configs[alias] = [{'devices': 'all', 'mig-enabled': False}]
    live['mig-configs'] = configs
    return yaml.safe_dump(live, sort_keys = False, default_flow_style = False)


def _merge_mig_configs(base_config : str, videoflow_config : str,
                       live_config : str = '',
                       drop : AbstractSet[str] = frozenset()) -> str:
    '''
    The operator's mig-parted file with videoflow's generated configs merged
    in, plus a ``MIG_DISABLED_CONFIG`` entry cleanup() can point a node at to
    un-partition it.

    Merging rather than replacing is a correctness requirement twice over: once
    ClusterPolicy points at the merged copy, the operator's
    ``migManager.config.default`` and any pre-existing node ``mig.config``
    labels must keep resolving in the mounted file — and in a multi-tenant
    cluster the currently *published* copy carries other flows'
    ``videoflow-<node>`` entries, which must survive this flow's publish or
    their nodes would go state=failed on the next manager pass. Node ownership
    keeps per-node config names disjoint across flows, so this flow wins name
    collisions only for its own nodes.

    - Arguments:
        - base_config: the operator's own ``config.yaml`` text ('' or \
            unparseable starts the merge from videoflow's entries only).
        - videoflow_config: ``layout_to_mig_parted_config`` output.
        - live_config: the currently published ``MIG_CONFIGMAP_NAME`` payload \
            ('' when absent) — only its ``videoflow-*`` entries are taken, the \
            rest comes fresh from ``base_config``.
        - drop: live entry names NOT to carry over — the previous per-run names \
            of this flow's own nodes, read off their labels/annotations before \
            relabeling. Without this a retried prepare would leak its failed \
            attempt's nonce'd entry into the shared map forever, and the stale \
            entry would block the last-one-out ConfigMap deletion at teardown.
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
    try:
        live = yaml.safe_load(live_config) if live_config else None
    except yaml.YAMLError:
        live = None
    if isinstance(live, dict):
        for name, entry in (live.get('mig-configs') or {}).items():
            if name.startswith('videoflow-') and name not in drop:
                merged['mig-configs'][name] = entry
    ours = yaml.safe_load(videoflow_config)        # generated by us: always valid
    merged['mig-configs'].update(ours.get('mig-configs') or {})
    merged['mig-configs'][MIG_DISABLED_CONFIG] = [{'devices': 'all', 'mig-enabled': False}]
    return yaml.safe_dump(merged, sort_keys = False, default_flow_style = False)


def _remove_mig_config_entries(live_config : str, mine : List[str]) -> tuple[str, List[str]]:
    '''
    The published mig-parted file with this flow's named configs stripped, and
    the ``videoflow-*`` entries other flows still hold (``MIG_DISABLED_CONFIG``
    and its nonce'd bounce aliases excluded — they are shared plumbing, not a
    flow's geometry, and an alias orphaned by a failed bounce must not block
    the last flow out). An empty ``remaining`` is the last-one-out signal:
    cleanup may then restore the ClusterPolicy pointer and delete the map.
    '''
    # Function-level: PyYAML via the deploy extras, as in _merge_mig_configs.
    import yaml

    try:
        live = yaml.safe_load(live_config) if live_config else None
    except yaml.YAMLError:
        live = None
    if not isinstance(live, dict):
        return '', []
    configs = {name: entry for name, entry in (live.get('mig-configs') or {}).items()
               if name not in mine}
    remaining = sorted(name for name in configs
                       if name.startswith('videoflow-')
                       and not name.startswith(MIG_DISABLED_CONFIG))
    stripped = dict(live)
    stripped['mig-configs'] = configs
    return yaml.safe_dump(stripped, sort_keys = False, default_flow_style = False), remaining


def _publish_mig_configmap(kubectl : str, namespace : str,
                           build : Callable[[str], str]) -> None:
    '''
    Publishes ``MIG_CONFIGMAP_NAME`` with optimistic concurrency. ``build``
    turns the live ``config.yaml`` payload ('' when absent) into the desired
    one, and is re-run on every attempt: the map is created when absent, else
    replaced carrying the read ``resourceVersion``, so a concurrent flow's
    write turns into a conflict and a re-read/re-merge here instead of a
    last-write-wins that would silently drop that flow's entries.

    - Raises:
        - RuntimeError: a non-conflict failure, or the conflict retries ran out.
    '''
    attempts = 5
    for _attempt in range(attempts):
        current = _kubectl_json(kubectl, 'get', 'configmap', MIG_CONFIGMAP_NAME, '-n', namespace)
        live_yaml = ((current or {}).get('data') or {}).get('config.yaml', '')
        payload : dict[str, Any] = {
            'apiVersion': 'v1', 'kind': 'ConfigMap',
            'metadata': {'name': MIG_CONFIGMAP_NAME, 'namespace': namespace},
            'data': {'config.yaml': build(live_yaml)},
        }
        if current is None:
            verb = 'create'
        else:
            verb = 'replace'
            payload['metadata']['resourceVersion'] = ((current.get('metadata') or {})
                                                      .get('resourceVersion', ''))
        proc = subprocess.run([kubectl, verb, '-f', '-'], input = json.dumps(payload),
                              capture_output = True, text = True, check = False)
        if proc.returncode == 0:
            return
        stderr = proc.stderr.strip()
        if any(marker in stderr for marker in ('Conflict', 'conflict', 'AlreadyExists',
                                               'already exists')):
            continue                       # raced another flow: re-read and re-merge
        raise RuntimeError(f'could not publish the mig-parted ConfigMap: {stderr}')
    raise RuntimeError(f'could not publish the mig-parted ConfigMap after {attempts} '
                       f'attempts — another deploy keeps writing it; retry this deploy.')


def _read_node_metadata(kubectl : str, node : str) -> dict:
    '''
    ``metadata`` of a node as a strict read: a failed or unparseable ``kubectl
    get node`` raises rather than reading as "no labels", and the result always
    carries the ``resourceVersion`` the compare-and-swap writes need.

    - Raises:
        - RuntimeError: kubectl failed, printed no JSON, or the node has no \
            resourceVersion (an API object always does; its absence means the \
            output is not a node).
    '''
    try:
        info = json.loads(_kubectl_run(kubectl, 'get', 'node', node, '-o', 'json'))
    except ValueError as e:
        raise RuntimeError(f'could not read node {node}: {kubectl} printed no JSON') from e
    meta = (info or {}).get('metadata') or {}
    if not meta.get('resourceVersion'):
        raise RuntimeError(f'could not read node {node}: the object carries no resourceVersion')
    return meta


def _read_mig_configmap_yaml(kubectl : str, namespace : str) -> str:
    '''
    The published ``MIG_CONFIGMAP_NAME``'s ``config.yaml`` — '' when the map
    does not exist, which is distinct from a read that failed: that raises,
    because cleanup decides on the map's *other* entries and an unreadable map
    must not pass for an empty one (``_operator_configmap_yaml`` is the
    silence-tolerant twin, for the merge base at prepare time).

    - Raises:
        - RuntimeError: kubectl failed or printed something that is not a ConfigMap.
    '''
    out = _kubectl_run(kubectl, 'get', 'configmap', MIG_CONFIGMAP_NAME, '-n', namespace,
                       '-o', 'json', '--ignore-not-found')
    if not out.strip():
        return ''
    try:
        current = json.loads(out)
    except ValueError as e:
        raise RuntimeError(f'{MIG_CONFIGMAP_NAME} read printed no JSON') from e
    return ((current or {}).get('data') or {}).get('config.yaml', '')


def _tombstone_mig_configmap(kubectl : str, namespace : str, mine : List[str]) -> bool:
    '''
    Last-one-out retirement of ``MIG_CONFIGMAP_NAME``: this flow's entries are
    stripped (a compare-and-swap publish, so a deploy racing in keeps its own
    entries) and the map is annotated ``MIG_TOMBSTONE_ANNOTATION``. The map is
    never deleted here — see the annotation's note. Returns False when the map
    does not exist (nothing to retire).

    - Raises:
        - RuntimeError: a read or write failed; the caller keeps its retry state.
    '''
    if not _mig_configmap_exists(kubectl, namespace):
        return False
    _publish_mig_configmap(kubectl, namespace,
                           lambda live: _remove_mig_config_entries(live, mine)[0])
    stamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    _kubectl_run(kubectl, 'annotate', 'configmap', MIG_CONFIGMAP_NAME, '-n', namespace,
                 '--overwrite', f'{MIG_TOMBSTONE_ANNOTATION}={stamp}')
    return True


def _mig_configmap_exists(kubectl : str, namespace : str) -> bool:
    return bool(_kubectl_run(kubectl, 'get', 'configmap', MIG_CONFIGMAP_NAME, '-n', namespace,
                             '-o', 'name', '--ignore-not-found').strip())


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


def flow_owner_value(flow_id : str) -> str:
    '''The ``GPU_OWNER_LABEL`` value for a flow: ``manifests.k8s_name(flow_id)``,
    so an arbitrary ``--flow-id`` charset becomes a legal <= 63-char label value —
    the same value the flow's pods already carry in their flow-id label.'''
    # Function-level: manifests imports yaml at module scope (optional dep).
    from .manifests import k8s_name
    return k8s_name(flow_id)


def _partition_inventory(inventory : List[NodeInventory],
                         flow_id : Optional[str]) -> tuple[List[NodeInventory], dict[str, str]]:
    '''
    Splits the pool into nodes mix may plan on and nodes it must not touch, with
    a reason per excluded node. The cluster is multi-tenant, so exclusion is the
    normal case, not an error: other flows own nodes, foreign workloads hold
    devices. Per node, first matching rule wins:

    1. Stamped with THIS flow's owner label — leftover geometry from a run that
       was never cleaned up. Hard ``ValueError``: on a partially-MIG'd node
       GFD's ``gpu.count`` no longer maps ``card_index`` to physical positions,
       so replanning could repartition the wrong cards. The fix is a teardown.
    2. Stamped by another flow (or by anyone, when this deploy has no flow id) —
       excluded quietly; that flow's teardown will free it.
    3. Time-sliced — its units are shares, not addressable cards; a misconfigured
       pool member, excluded with a warning.
    4. Already carrying MIG geometry videoflow does not own — same card-position
       blindness as rule 1, but the fix is the operator's, not a teardown.
    5. Busy (running pods hold devices) — repartitioning would destroy them, so
       MIG is disallowed, but whole-card spanner claims are scheduler-accounted
       and stay safe: the node survives with only its free cards.
    '''
    owner = flow_owner_value(flow_id) if flow_id else None
    usable : List[NodeInventory] = []
    excluded : dict[str, str] = {}
    stale : List[NodeInventory] = []
    for node in inventory:
        if node.owner is not None and owner is not None and node.owner == owner:
            stale.append(node)
            continue
        if node.owner is not None:
            excluded[node.name] = f'owned by another videoflow flow ({GPU_OWNER_LABEL}={node.owner})'
            logger.info('mix: skipping node %s — %s', node.name, excluded[node.name])
            continue
        if node.time_sliced:
            excluded[node.name] = (f'time-sliced (product {node.product}) — mix partitions '
                                   f'physical cards and cannot address shared units. Disable '
                                   f'time-slicing on it, or remove it from the pool: '
                                   f'kubectl label node {node.name} {GPU_POOL_LABEL}-')
            logger.warning('mix: skipping node %s — %s', node.name, excluded[node.name])
            continue
        if node.mig_partitioned or node.mig_config not in (None, '', 'all-disabled',
                                                           MIG_DISABLED_CONFIG):
            evidence = (f'nvidia.com/mig.config={node.mig_config}' if node.mig_config
                        else 'it advertises nvidia.com/mig-* resources')
            excluded[node.name] = (f'already MIG-partitioned outside videoflow ({evidence}) — '
                                   f'its gpu.count no longer maps card positions, so a generated '
                                   f'layout could repartition the wrong cards. Clear its MIG '
                                   f'geometry, or remove it from the pool: '
                                   f'kubectl label node {node.name} {GPU_POOL_LABEL}-')
            logger.warning('mix: skipping node %s — %s', node.name, excluded[node.name])
            continue
        if not node.occupancy_known:
            # The pod listing failed, so which of this node's cards are held is
            # not known — and repartitioning a card another tenant holds
            # destroys their workload. Unknown is not idle; refuse to plan.
            raise UnobservableState(
                f'{UNOBSERVABLE_GPU_STATE}: the pod listing that says which of {node.name}\'s '
                f'cards are in use could not be read, so mix cannot tell a free card from one '
                f'another tenant holds and will not plan MIG geometry on a guess.',
                remedy = f'Restore `kubectl get pods -A` (RBAC: list pods in all namespaces) and '
                         f'redeploy, or remove the node from the pool: kubectl label node '
                         f'{node.name} {GPU_POOL_LABEL}-',
                node = node.name)
        busy_units = sum(units for resource, units in node.used_units.items()
                         if _is_gpu_resource(resource))
        if busy_units >= node.card_count:
            excluded[node.name] = (f'{busy_units} unit(s) in use by running pods across its '
                                   f'{node.card_count} card(s) — no free cards to plan')
            logger.info('mix: skipping node %s — %s', node.name, excluded[node.name])
            continue
        if busy_units:
            # Repartitioning a busy node would destroy the running workloads, and
            # which physical card a pod holds is not knowable from the API — so no
            # MIG here, and only the free cards count for spanners.
            usable.append(dataclasses.replace(node, card_count = node.card_count - busy_units,
                                              mig_allowed = False))
            continue
        usable.append(node)
    if stale:
        names = ', '.join(sorted(n.name for n in stale))
        raise ValueError(
            f'node(s) {names} still carry this flow\'s MIG geometry '
            f'({GPU_OWNER_LABEL}={owner}) from a previous run that was not cleaned up — '
            f'their gpu.count no longer maps card positions, so replanning could repartition '
            f'the wrong cards. Fix: videoflow teardown --flow-id {flow_id} --run-id <run-id> '
            f'--nats <url> --namespace <ns> --gpu-mode mix, then redeploy.')
    return usable, excluded


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
        # The layout and the exclusion set are computed once per deploy in
        # resolve_specs and reused by preflight/prepare in the same process.
        # Solving is deterministic, so a recompute would agree — the cache only
        # saves kubectl round-trips. flow_id is deliberately NOT cached: it is a
        # per-call parameter, so a stale value cannot leak between deploys
        # sharing this registered singleton.
        self._layout : Optional[GpuLayout] = None
        self._excluded_nodes : dict[str, str] = {}

    def resolve_specs(self, specs : List[NodeSpec], kubectl : str = 'kubectl',
                    default_resource : Optional[str] = None,
                    flow_id : Optional[str] = None) -> List[NodeSpec]:
        # Function-level: same gpu <-> cluster cycle as in ExclusiveGpu.preflight_problems.
        from .cluster import gpu_inventory_observed

        if not any(s.device_type == 'gpu' for s in specs):
            return specs
        observed = gpu_inventory_observed(kubectl)
        if isinstance(observed, Unknown):
            # An unreadable pool is not an empty pool: planning against [] would
            # report "no MIG-capable node" and send the operator after labels
            # that are there.
            raise UnobservableState(
                f'{UNOBSERVABLE_GPU_STATE}: the GPU pool could not be listed ({observed.reason}: '
                f'{observed.detail}) — mix plans MIG geometry from node labels and cannot '
                f'proceed on an unreadable pool.',
                remedy = f'Check kubectl access to nodes (kubectl get nodes -l {GPU_POOL_LABEL}=true) '
                         f'and redeploy.')
        usable, excluded = _partition_inventory(observed.value, flow_id)
        self._excluded_nodes = excluded
        try:
            layout = solve_layout(usable, specs)   # raises LayoutError (a ValueError)
        except LayoutError as e:
            if excluded:
                # Shrunken capacity must never fail silently: the operator sees
                # which pool nodes were ruled out and why, at the failure point.
                detail = '\n'.join(f'  - {node}: {reason}'
                                   for node, reason in sorted(excluded.items()))
                raise LayoutError(f'{e}\nNodes excluded from mix planning:\n{detail}') from e
            raise
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
                        max_per_pod : Optional[dict[str, int]] = None,
                        pod_claims : Optional[dict[str, list[int]]] = None) -> List[str]:
        # ``pod_claims`` is accepted for the shared call shape and unused: mix
        # places every spanner replica on a concrete host in resolve_specs
        # (solve_layout raises LayoutError when no host has the cards), so the
        # per-host packing check is already made before preflight runs.
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
        # "Not applied yet" is the expected state on a fresh cluster, not a
        # problem: prepare() applies the geometry through the GPU Operator MIG
        # manager immediately after preflight. So a shortfall only blocks when
        # NO manager is present to apply it (prepare() would then fail with the
        # config to apply by hand); with a manager it is informational and must
        # not trip --strict-preflight on a first deploy. Manager presence is read
        # at most once, and only when a shortfall actually shows up.
        manager_present : Optional[bool] = None
        for resource, needed in sorted(layout.slice_demand.items()):
            advertised = allocatable_gpus(kubectl, resource)
            if advertised >= needed:
                continue
            if manager_present is None:
                manager_present = bool(self._mig_manager_pods(kubectl))
            if manager_present:
                logger.info('mix preflight: the layout needs %d x %s; the cluster advertises '
                            '%d today. prepare() will apply the MIG geometry via the GPU '
                            'Operator MIG manager before the flow is scheduled.',
                            needed, resource, advertised)
            else:
                problems.append(
                    f'the layout needs {needed} x {resource} but the cluster currently '
                    f'advertises {advertised} and no GPU Operator MIG manager (pods labeled '
                    f'app=nvidia-mig-manager) is present to apply it — apply this '
                    f'nvidia-mig-parted config to the pool nodes and retry:\n'
                    + layout_to_mig_parted_config(layout))
        # Spanner demand is claimed exclusive-style, so the whole-device check
        # applies before prepare() too: applying geometry only *shrinks* whole-card
        # capacity (a MIG'd card retires its units), so demand exceeding today's
        # free pool can never be satisfied afterwards — and 0 allocatable is the
        # broken/absent device-plugin case the slice check cannot see.
        whole = {resource: units for resource, units in sorted((demand or {}).items())
                 if resource not in layout.slice_demand}
        if whole:
            problems.extend(_capacity_problems(kubectl, whole,
                                               exclude_nodes = frozenset(self._excluded_nodes)))
        if not gpu_runtime_class:
            nvidia_rc = nvidia_runtimeclass(kubectl)
            if nvidia_rc:
                problems.append(f'a {nvidia_rc!r} RuntimeClass exists but no --gpu-runtime-class '
                                f'was given — if the NVIDIA runtime is not this node\'s containerd '
                                f'default, GPU pods will start with no device. Fix: deploy with '
                                f'--gpu-runtime-class {nvidia_rc}')
        return problems

    def _mig_manager_pods_observed(self, kubectl : str) -> Observation[List[tuple]]:
        '''
        ``(namespace, pod)`` of the GPU Operator's MIG manager pods: ``Known([])``
        when none exist, ``Unknown`` when the listing could not be read. Done
        inline (not via ``cluster``) to keep the gpu <-> cluster import
        one-directional at module scope.
        '''
        try:
            proc = subprocess.run([kubectl, 'get', 'pods', '-A', '-l', 'app=nvidia-mig-manager',
                                   '-o', 'jsonpath={range .items[*]}{.metadata.namespace} {.metadata.name}{"\\n"}{end}'],
                                  capture_output = True, text = True, check = False)
        except FileNotFoundError:
            return unknown('missing', f'{kubectl!r} is not on PATH')
        if proc.returncode != 0:
            lines = (proc.stderr or proc.stdout).strip().splitlines()
            return unknown('failed', (lines[-1] if lines else f'{kubectl} exited {proc.returncode}')[:300])
        return known([tuple(line.split()) for line in proc.stdout.strip().splitlines() if line.split()])

    def _mig_manager_pods(self, kubectl : str) -> List[tuple]:
        '''
        ``_mig_manager_pods_observed`` for callers where "absent" is already the
        actionable outcome (preflight and prepare fail with the config to apply
        by hand): [] when absent or unreadable. Cleanup uses the observed form —
        it must not mistake an unreadable listing for "no manager, nothing to
        strip" and retire shared state on that basis.
        '''
        return value_or(self._mig_manager_pods_observed(kubectl), [])

    def prepare(self, demand : Optional[dict[str, int]] = None, kubectl : str = 'kubectl',
                flow_id : Optional[str] = None) -> None:
        '''
        Applies the layout's MIG geometry through the GPU Operator: claim the
        target nodes for this flow, merge the generated mig-parted config into
        the operator's file (preserving other flows' published entries),
        publish the result as ``MIG_CONFIGMAP_NAME``, point ClusterPolicy
        ``migManager.config.name`` at it (recording the original name in
        ``MIG_CONFIG_NAME_RESTORE_ANNOTATION``), wait for the mig-manager
        DaemonSet to remount, then label each MIG'd node with its per-run
        entry name — ``videoflow-<node>-<nonce>``, see ``_mig_config_name`` —
        (recording its previous label in ``MIG_RESTORE_ANNOTATION`` and the
        entry name in ``MIG_ENTRY_ANNOTATION``) and wait for
        ``nvidia.com/mig.config.state=success``. The per-run nonce guarantees
        the label value *changes*: the manager reacts only to changes, so a
        leftover ``state=failed`` from a previous attempt cannot deadlock the
        retry. The manager only reads the
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
        if flow_id is None:
            raise RuntimeError('mix prepare() needs a flow_id to stamp node ownership — the '
                               'cluster may host other flows. Deploy through the videoflow '
                               'CLI, which passes the flow id.')
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
        # Claim the nodes before any geometry work: a competing deploy planning
        # against the same pool must see them as taken from here on, and losing
        # the claim race must abort before anything was mutated.
        self._stamp_node_owners(kubectl, nodes, flow_owner_value(flow_id))
        nonce = uuid.uuid4().hex[:6]
        names = {node: _mig_config_name(node, nonce) for node in nodes}
        stale_names = self._record_entry_names(kubectl, names)
        applied_config = _rename_mig_config_entries(config, names)
        original = (((policy.get('spec') or {}).get('migManager') or {}).get('config') or {}).get('name')
        annotations = (policy.get('metadata') or {}).get('annotations') or {}
        recorded = annotations.get(MIG_CONFIG_NAME_RESTORE_ANNOTATION)
        # The merge base must be the operator's own file, never our published
        # map — its videoflow entries ride in via the live-config merge instead.
        # The recorded pre-videoflow name wins (another flow may have already
        # repointed the policy at ours), else the current pointer unless it is
        # ours, else the stock default.
        if recorded and recorded != MIG_CONFIG_NAME_ABSENT:
            base_name = recorded
        elif original and original != MIG_CONFIGMAP_NAME:
            base_name = original
        else:
            base_name = MIG_OPERATOR_DEFAULT_CONFIGMAP
        base = _operator_configmap_yaml(kubectl, namespace, base_name)
        if not base and base_name != MIG_OPERATOR_DEFAULT_CONFIGMAP:
            # The named ConfigMap is gone (e.g. a prior unclean run left the
            # policy pointing at ours and something deleted it) — fall back to
            # the stock file so the merge keeps the operator's entries.
            base = _operator_configmap_yaml(kubectl, namespace, MIG_OPERATOR_DEFAULT_CONFIGMAP)
        if not base:
            logger.warning(f'no readable mig-parted config in ConfigMap {base_name!r} — '
                           f'the merged config will carry only videoflow\'s entries')
        _publish_mig_configmap(kubectl, namespace,
                               lambda live: _merge_mig_configs(base, applied_config,
                                                               live_config = live,
                                                               drop = stale_names))
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
            self._label_node_for_mig(kubectl, node, names[node])
        states = _wait_for_mig_state(kubectl, nodes)
        failed = sorted(node for node, state in states.items() if state == 'failed')
        if failed:
            raise RuntimeError(
                f'the MIG manager reported state=failed on {", ".join(failed)} — the generated '
                f'geometry may need adjusting; config applied:\n' + applied_config)
        timed_out = sorted(node for node, state in states.items() if state == 'timeout')
        if timed_out:
            raise RuntimeError(
                f'MIG geometry did not reach state=success on {", ".join(timed_out)} within '
                f'{MIG_APPLY_TIMEOUT_SECONDS}s — check the MIG manager logs '
                f'(kubectl logs -n {namespace} -l app=nvidia-mig-manager).')

    def _stamp_node_owners(self, kubectl : str, nodes : List[str], owner : str) -> str:
        '''
        Claims each node for this flow via ``GPU_OWNER_LABEL``, compare-and-swap
        on the node's ``resourceVersion``: the label write carries
        ``--resource-version`` from the read that found the node unowned, so
        the API server rejects it (409 Conflict) if anything touched the node in
        between — losing a race to a concurrent deploy fails the write instead
        of silently stealing the node. (``kubectl label`` without ``--overwrite``
        only checks client-side, which two deploys can both pass.)
        ``GPU_OWNER_EPOCH_LABEL`` rides in the same write with a fresh per-claim
        value, so a later release can tell this claim from a re-claim. On any
        loss the nodes this call stamped are released and prepare aborts with
        ``OwnershipConflict`` — partial ownership would strand capacity another
        flow already planned against. Returns the epoch stamped.
        '''
        epoch = uuid.uuid4().hex[:8]
        stamped : List[str] = []
        for node in nodes:
            meta = _read_node_metadata(kubectl, node)
            current = (meta.get('labels') or {}).get(GPU_OWNER_LABEL)
            if current == owner:
                continue                              # retried prepare: already ours
            error : Optional[str] = None
            if current:
                error = f'it is owned by {current!r}'
            else:
                try:
                    _kubectl_run(kubectl, 'label', 'node', node,
                                 f'{GPU_OWNER_LABEL}={owner}', f'{GPU_OWNER_EPOCH_LABEL}={epoch}',
                                 f'--resource-version={meta["resourceVersion"]}')
                    stamped.append(node)
                except RuntimeError as e:             # lost the compare-and-swap
                    error = str(e)
            if error:
                for claimed in stamped:
                    try:
                        self._release_owner(kubectl, claimed, owner, epoch)
                    except RuntimeError as undo_error:
                        logger.warning(f'could not release the owner stamp on {claimed}: '
                                       f'{undo_error}')
                raise OwnershipConflict(
                    f'could not claim node {node} for this flow: {error} — another deploy '
                    f'took it between planning and prepare.',
                    remedy = 'Re-run the deploy to plan against the remaining pool.',
                    node = node)
        return epoch

    def _release_owner(self, kubectl : str, node : str, owner : str,
                       epoch : Optional[str]) -> bool:
        '''
        Removes this flow's claim from ``node`` — only while the node still
        carries exactly the claim being released (``owner`` and, when the claim
        recorded one, ``epoch``), and only through a compare-and-swap on the
        node's ``resourceVersion``. A claim that changed hands in between (a
        redeploy of this flow re-claimed the node after a crashed run, say) is
        left standing: releasing it would return cards to the pool while the
        newer claim's geometry is in use. Returns True when the claim was
        removed, False when it was not this claim to remove.

        - Raises:
            - RuntimeError: the node could not be read, or the CAS kept losing.
        '''
        for _attempt in range(3):
            meta = _read_node_metadata(kubectl, node)
            labels = meta.get('labels') or {}
            if labels.get(GPU_OWNER_LABEL) != owner:
                logger.info(f'not releasing {node}: it is now owned by '
                            f'{labels.get(GPU_OWNER_LABEL)!r}, not {owner!r}')
                return False
            if epoch is not None and labels.get(GPU_OWNER_EPOCH_LABEL) not in (None, epoch):
                logger.info(f'not releasing {node}: {owner!r} re-claimed it under epoch '
                            f'{labels.get(GPU_OWNER_EPOCH_LABEL)!r} (this release is for {epoch!r})')
                return False
            try:
                _kubectl_run(kubectl, 'label', 'node', node,
                             f'{GPU_OWNER_LABEL}-', f'{GPU_OWNER_EPOCH_LABEL}-',
                             f'--resource-version={meta["resourceVersion"]}')
                return True
            except RuntimeError as e:
                if 'onflict' not in str(e):
                    raise
        raise RuntimeError(f'could not release the owner stamp on {node}: the node kept '
                           f'changing under the compare-and-swap')

    def _record_entry_names(self, kubectl : str, names : dict[str, str]) -> set[str]:
        '''
        Stamps each claimed node's per-run entry name in ``MIG_ENTRY_ANNOTATION``
        — before the ConfigMap publish, so even a prepare that crashes between
        publishing and labeling leaves a cluster-side record cleanup can strip
        the entry by (nonce'd names cannot be reconstructed) — and returns the
        names a previous attempt left on these nodes (label or annotation),
        which the merge must drop so a retried prepare does not leak its failed
        attempt's entry into the shared map. The nodes are owner-stamped ours by
        the time this runs, so every ``videoflow-*`` value found is this flow's.
        '''
        stale : set[str] = set()
        for node in sorted(names):
            # A strict read, as in _label_node_for_mig: a failed read mistaken
            # for "nothing recorded" would leak the previous entry.
            info = json.loads(_kubectl_run(kubectl, 'get', 'node', node, '-o', 'json'))
            meta = info.get('metadata') or {}
            label = (meta.get('labels') or {}).get(MIG_CONFIG_LABEL, '')
            entry = (meta.get('annotations') or {}).get(MIG_ENTRY_ANNOTATION, '')
            for value in (label, entry):
                if value.startswith('videoflow-') and value != MIG_DISABLED_CONFIG:
                    stale.add(value)
            _kubectl_run(kubectl, 'annotate', 'node', node, '--overwrite',
                         f'{MIG_ENTRY_ANNOTATION}={names[node]}')
        return stale

    def _label_node_for_mig(self, kubectl : str, node : str, entry_name : str) -> None:
        # A strict read: a failed one mistaken for "no annotation" would
        # re-record and corrupt the restore value, so it must raise instead
        # (which _kubectl_json's silence-tolerance would not).
        info = json.loads(_kubectl_run(kubectl, 'get', 'node', node, '-o', 'json'))
        meta = info.get('metadata') or {}
        previous = (meta.get('labels') or {}).get(MIG_CONFIG_LABEL, '')
        annotations = meta.get('annotations') or {}
        if MIG_RESTORE_ANNOTATION not in annotations:
            # Idempotent across a retried prepare: only the first attempt records
            # the pre-videoflow value ('' = the label was absent). Key presence,
            # not value truthiness — the recorded value is legitimately '', and a
            # jsonpath read cannot tell that apart from "no annotation".
            if previous.startswith('videoflow-'):
                # A videoflow label with no record (a prior run lost it, under
                # this or the pre-nonce naming): recording it as "previous"
                # would make the geometry permanent — record "absent", like the
                # ClusterPolicy twin in prepare(). MIG_DISABLED_CONFIG prefix-
                # matches too, deliberately: "disabled" and "absent" both mean
                # no geometry, and cleanup restoring "absent" (unlabel via the
                # disabled entry) is equivalent to restoring "disabled".
                logger.warning(f'node {node} already carries {MIG_CONFIG_LABEL}='
                               f'{previous} with no restore record — cleanup '
                               f'will remove the label and un-partition the cards')
                previous = ''
            _kubectl_run(kubectl, 'annotate', 'node', node,
                         f'{MIG_RESTORE_ANNOTATION}={previous}')
        _kubectl_run(kubectl, 'label', 'node', node, '--overwrite',
                     f'{MIG_CONFIG_LABEL}={entry_name}')

    def cleanup(self, kubectl : str = 'kubectl', flow_id : Optional[str] = None) -> None:
        '''
        Restores every node carrying ``MIG_RESTORE_ANNOTATION`` to its recorded
        pre-videoflow ``nvidia.com/mig.config`` value — a node whose label was
        absent is first pointed at ``MIG_DISABLED_CONFIG`` so the manager
        actually un-partitions the cards (removing the label triggers nothing),
        then unlabeled — and waits for ``mig.config.state=success`` before
        declaring a node done. Only when every node reverted AND no other
        flow's entries remain in the published map does it restore
        ClusterPolicy ``migManager.config.name`` from
        ``MIG_CONFIG_NAME_RESTORE_ANNOTATION`` and delete the published
        ConfigMap (last one out); otherwise it strips only this flow's entries.
        Entry names carry a per-run nonce, so which entries are this flow's is
        read off its nodes' ``mig.config`` labels and ``MIG_ENTRY_ANNOTATION``
        stamps, never reconstructed. A node that failed keeps its annotation,
        and the policy patch and ConfigMap stay wired, so a retried teardown
        can resume — and a node already sitting at its restore target with
        ``state=failed`` is first bounced through a nonce'd alias of the
        disabled entry, because rewriting the identical label value is a no-op
        for the manager and would deadlock every retry.
        State lives entirely in the cluster, so this works from a teardown that
        shares nothing with the deploy that ran prepare — and is a no-op when
        prepare never ran.

        With a ``flow_id``, only nodes stamped ``GPU_OWNER_LABEL=<this flow>``
        are touched — other flows' geometry stays up. Without one, every node
        videoflow owns is swept (the single-operator escape hatch, and the
        pre-ownership behaviour). A node stamped but never restore-annotated —
        prepare crashed between claiming and labeling — has no geometry to
        revert, so its claim is simply released.
        '''
        try:
            listing = json.loads(_kubectl_run(kubectl, 'get', 'nodes', '-o', 'json'))
        except (RuntimeError, ValueError) as e:
            # Unknown owners are not "no owners": a cleanup that cannot see which
            # nodes carry this flow's claims must change nothing, not sweep the
            # shared state as if it were the last flow out.
            logger.warning(f'mix cleanup could not list nodes ({e}) — nothing was changed; re-run '
                           f'videoflow teardown --gpu-mode mix once kubectl can list nodes.')
            return
        nodes = (listing or {}).get('items') or []
        owner = flow_owner_value(flow_id) if flow_id else None
        policy = _cluster_policy(kubectl)
        policy_name = ((policy or {}).get('metadata') or {}).get('name', '')
        current_name = ((((policy or {}).get('spec') or {}).get('migManager') or {})
                        .get('config') or {}).get('name')
        # The disabled config must exist in the file the manager currently
        # mounts: ours while the policy still points at the merged copy, the
        # stock all-disabled once an earlier partial cleanup restored it.
        disabled = MIG_DISABLED_CONFIG if current_name == MIG_CONFIGMAP_NAME else 'all-disabled'
        entry_names : set[str] = set()     # this flow's entries, read off the cluster
        targets : dict[str, str] = {}      # node -> label value the restore writes
        previous_of : dict[str, str] = {}  # node -> recorded previous label value
        stamped : dict[str, bool] = {}     # node -> node carries our owner stamp
        claim_of : dict[str, tuple[str, Optional[str]]] = {}   # node -> (owner, epoch) read above
        annotated : dict[str, bool] = {}   # node -> carries MIG_ENTRY_ANNOTATION
        stuck : List[str] = []             # already at target with state=failed
        unrestored = False
        for node in nodes:
            meta = node.get('metadata') or {}
            labels = meta.get('labels') or {}
            annotations = meta.get('annotations') or {}
            name = meta.get('name', '')
            node_owner = labels.get(GPU_OWNER_LABEL)
            node_epoch = labels.get(GPU_OWNER_EPOCH_LABEL)
            if owner is not None:
                ours = node_owner == owner
            else:
                ours = node_owner is not None or MIG_RESTORE_ANNOTATION in annotations
            if not ours:
                continue
            for value in (labels.get(MIG_CONFIG_LABEL, ''),
                          annotations.get(MIG_ENTRY_ANNOTATION, '')):
                if value.startswith('videoflow-') and value != MIG_DISABLED_CONFIG:
                    entry_names.add(value)
            if MIG_RESTORE_ANNOTATION not in annotations:
                # Orphan claim: prepare crashed between stamping and labeling, so
                # no geometry was applied — just release the node. Its ConfigMap
                # entry may exist (publish precedes labeling); the entry
                # annotation recorded its name above, so it still gets stripped.
                try:
                    if node_owner is not None:
                        self._release_owner(kubectl, name, node_owner, node_epoch)
                    if MIG_ENTRY_ANNOTATION in annotations:
                        _kubectl_run(kubectl, 'annotate', 'node', name,
                                     f'{MIG_ENTRY_ANNOTATION}-')
                except RuntimeError as e:
                    logger.warning(f'mix cleanup could not release the owner stamp on {name}: {e}')
                continue
            previous = annotations[MIG_RESTORE_ANNOTATION]
            previous_of[name] = previous
            targets[name] = previous or disabled
            stamped[name] = node_owner is not None
            claim_of[name] = (node_owner or '', node_epoch)
            annotated[name] = MIG_ENTRY_ANNOTATION in annotations
            if (labels.get(MIG_CONFIG_LABEL) == targets[name]
                    and labels.get(MIG_CONFIG_STATE_LABEL) == 'failed'):
                stuck.append(name)
        skip : set[str] = set()
        if stuck:
            skip = self._bounce_stuck_nodes(kubectl, stuck, current_name, entry_names)
            unrestored = unrestored or bool(skip)
        restored : dict[str, str] = {}     # node -> recorded previous label value
        for name in sorted(targets):
            if name in skip:
                continue
            try:
                _kubectl_run(kubectl, 'label', 'node', name, '--overwrite',
                             f'{MIG_CONFIG_LABEL}={targets[name]}')
                restored[name] = previous_of[name]
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
                if annotated[name]:
                    _kubectl_run(kubectl, 'annotate', 'node', name, f'{MIG_ENTRY_ANNOTATION}-')
                if stamped[name]:
                    # Reverted and unrecorded: the node returns to the pool —
                    # unless the claim changed hands meanwhile (a redeploy
                    # re-claimed it), which the CAS release detects and leaves.
                    self._release_owner(kubectl, name, *claim_of[name])
            except RuntimeError as e:
                unrestored = True
                logger.warning(f'mix cleanup could not finish restoring node {name}: {e}')
        if unrestored:
            logger.warning(
                f'mix cleanup left the ClusterPolicy patch and the {MIG_CONFIGMAP_NAME} '
                f'ConfigMap in place — a retried teardown needs them to finish reverting '
                f'the nodes above.')
            return
        # Strip this flow's entries from the shared map; only the last flow out
        # may restore the ClusterPolicy pointer and delete it — earlier flows'
        # nodes still resolve their per-run entry names in the mounted file.
        # The names were read off this flow's node labels and entry annotations
        # above (nonce'd names cannot be reconstructed from node names).
        mine = sorted(entry_names)
        managers = self._mig_manager_pods_observed(kubectl)
        if isinstance(managers, Unknown):
            # Retiring the shared map decides on what OTHER flows still hold in
            # it; with the manager listing unreadable that cannot be checked, so
            # everything stays wired for a retried teardown.
            logger.warning(
                f'mix cleanup could not list the MIG manager pods ({managers.reason}: '
                f'{managers.detail}) — leaving the ClusterPolicy pointer and the '
                f'{MIG_CONFIGMAP_NAME} ConfigMap in place; re-run videoflow teardown '
                f'--gpu-mode mix once kubectl can list pods.')
            return
        strip_namespace = next((ns for ns, _pod in managers.value), None)
        if strip_namespace is not None:
            try:
                live = _read_mig_configmap_yaml(kubectl, strip_namespace)
            except RuntimeError as e:
                # Same reasoning: an unreadable map is not an empty one.
                logger.warning(f'mix cleanup could not read the {MIG_CONFIGMAP_NAME} ConfigMap '
                               f'({e}) — leaving the ClusterPolicy pointer and the map in '
                               f'place; re-run videoflow teardown --gpu-mode mix.')
                return
            remaining = _remove_mig_config_entries(live, mine)[1]
            if remaining:
                try:
                    _publish_mig_configmap(
                        kubectl, strip_namespace,
                        lambda live_now: _remove_mig_config_entries(live_now, mine)[0])
                except RuntimeError as e:
                    logger.warning(f'mix cleanup could not strip this flow\'s entries from '
                                   f'the mig-parted ConfigMap: {e}')
                logger.info(f'other videoflow flows still hold MIG geometry '
                            f'({", ".join(remaining)}) — leaving the ClusterPolicy pointer '
                            f'and the {MIG_CONFIGMAP_NAME} ConfigMap for the last flow out')
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
        if strip_namespace is not None:
            # Last one out retires the map without deleting it (see
            # _tombstone_mig_configmap): our entries are stripped so a later
            # deploy's live-config merge cannot resurrect them, and the map is
            # marked for the operator to remove.
            try:
                _tombstone_mig_configmap(kubectl, strip_namespace, mine)
            except RuntimeError as e:
                logger.warning(f'mix cleanup could not retire the mig-parted ConfigMap: {e}')

    def _bounce_stuck_nodes(self, kubectl : str, stuck : List[str],
                            current_name : Optional[str],
                            entry_names : set[str]) -> set[str]:
        '''
        Un-deadlocks nodes already labeled with their restore target while
        ``mig.config.state=failed``: rewriting the identical value is a no-op
        for the MIG manager (it reacts to label *changes*), so a retried
        cleanup would read back ``failed`` forever — the cleanup twin of the
        prepare-side retry deadlock the per-run nonce fixes. Each node is
        pointed at a fresh nonce'd alias of the disabled entry (a guaranteed
        change) and must reach success there before the caller's restore write
        — itself a change again — can converge. The alias name is added to
        ``entry_names`` so the caller strips it with the rest of this flow's
        entries. Returns the nodes that could not be bounced; they keep their
        retry state.
        '''
        if current_name != MIG_CONFIGMAP_NAME:
            # The manager is not mounting our map, so no alias we publish could
            # be resolved — leave the retry state rather than label into a void.
            logger.warning(
                f'node(s) {", ".join(sorted(stuck))} sit at state=failed on their restore '
                f'target, but the ClusterPolicy no longer names {MIG_CONFIGMAP_NAME}, so '
                f'videoflow cannot publish the alias entry needed to bounce them — check '
                f'the MIG manager logs and re-run videoflow teardown --gpu-mode mix.')
            return set(stuck)
        namespace = next((ns for ns, _pod in self._mig_manager_pods(kubectl)), None)
        if namespace is None:
            logger.warning(
                f'node(s) {", ".join(sorted(stuck))} sit at state=failed on their restore '
                f'target, but no MIG manager is running to react to a bounce — restore the '
                f'GPU Operator MIG manager and re-run videoflow teardown --gpu-mode mix.')
            return set(stuck)
        alias = f'{MIG_DISABLED_CONFIG}-{uuid.uuid4().hex[:6]}'
        try:
            _publish_mig_configmap(kubectl, namespace,
                                   lambda live: _add_disabled_alias_entry(live, alias))
        except RuntimeError as e:
            logger.warning(f'mix cleanup could not publish the bounce alias entry: {e}')
            return set(stuck)
        entry_names.add(alias)
        failed : set[str] = set()
        bounced : List[str] = []
        for name in stuck:
            try:
                _kubectl_run(kubectl, 'label', 'node', name, '--overwrite',
                             f'{MIG_CONFIG_LABEL}={alias}')
                bounced.append(name)
            except RuntimeError as e:
                failed.add(name)
                logger.warning(f'mix cleanup could not bounce node {name}: {e}')
        states = _wait_for_mig_state(kubectl, sorted(bounced)) if bounced else {}
        for name in bounced:
            if states.get(name) != 'success':
                failed.add(name)
                logger.warning(
                    f'node {name} did not reach mig.config.state=success on the bounce '
                    f'alias (state: {states.get(name)}) — its restore annotation is kept; '
                    f'check the MIG manager logs and re-run videoflow teardown '
                    f'--gpu-mode mix to retry.')
        return failed


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
