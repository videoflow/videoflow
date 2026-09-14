'''
The Kubernetes ``AcceleratorAllocationBackend``: the videoflow GPU pool as the
contract sees it, over the two registered strategies.

- ``exclusive`` — the device plugin is the authority. Videoflow reads the pool
  (GFD labels, allocatable, running pods) and plans a per-host packing, but it
  writes nothing: the scheduler accounts whole-device claims, so ``reserve`` is
  a plan admitted for rendering and readiness is the pod's, not the claim's.
- ``mix`` — videoflow itself becomes an authority over geometry (RFC 0004): it
  claims nodes by a server-enforced compare-and-swap on their
  ``resourceVersion``, re-reads the occupancy after claiming and before any
  geometry write, publishes the ``nvidia-mig-parted`` entries under CAS, and
  judges readiness by evidence correlated with *this* operation — the
  per-run entry name and owner epoch it wrote, the manager's ``success`` **and**
  the requested slices in ``status.allocatable`` (ALLOC-003/011/033).

What the reads could not see stays unknown: a pod listing the API refused
makes the inventory ``partial`` and no plan may admit on it; an unreadable node
makes ``observe`` ``Unknown``, never "ready". Release is fenced on the claim's
generation (the owner epoch) and ``keep_workloads=True`` retains ownership and
geometry for a later explicit release (ALLOC-013). The shared ConfigMap is
never deleted: the last flow out restores the operator's pointer and leaves a
tombstone (decision D3).

The strategies keep their registry API for the CLI (``resolve_specs`` →
``preflight_problems`` → ``prepare`` → ``cleanup``); this module is the same
machinery behind the contract, with the plan explicit (``gpu.AllocationPlan``).
'''
from __future__ import absolute_import, division, print_function

import dataclasses
import hashlib
import time
from typing import Any, Callable, Mapping, Sequence

from ..backends.allocation import (
    CLAIM_ALLOCATED,
    CLAIM_FAILED,
    CLAIM_PENDING,
    CLAIM_READY,
    COMPLETENESS_COMPLETE,
    COMPLETENESS_PARTIAL,
    RELEASE_PENDING_RECOVERY,
    RELEASE_RELEASED,
    RELEASE_STALE,
    SHARING_ISOLATED_MIG,
    AcceleratorAllocationBackend,
    ClaimObservation,
    Constraint,
    DeviceIdentity,
    FeasiblePlan,
    Infeasible,
    InventorySnapshot,
    PlanOutcome,
    ReleaseObservation,
    WorkloadBindings,
    WorkloadRequest,
)
from ..backends.capabilities import ENFORCEMENT_HARDWARE, ENFORCEMENT_NONE, AllocationCapabilities
from ..backends.memory.allocation import pack_whole_devices
from ..backends.outcomes import Observation, Unknown, known, unknown
from ..core.compiler import NodeSpec
from ..core.errors import ClusterError, OwnershipConflict, UnobservableState
from .gpu import (
    GPU_OWNER_LABEL,
    AllocationPlan,
    AppliedGeometry,
    MixGpu,
    _is_gpu_resource,
    _partition_inventory,
    flow_owner_value,
    get_gpu_mode,
)
from .mig import LayoutError, NodeInventory, mig_table_for_product, solve_layout

ADAPTER_DEVICE_PLUGIN = 'kubernetes-device-plugin'
ADAPTER_MANAGED_MIG = 'kubernetes-managed-mig'
AUTHORITY_DEVICE_PLUGIN = 'device-plugin'
AUTHORITY_MANAGED_MIG = 'managed-mig'

CLASS_PHYSICAL = 'physical'
CLASS_MIG = 'mig'
CLASS_TIME_SLICED = 'time-sliced'

DEFAULT_GPU_RESOURCE = 'nvidia.com/gpu'
GIB = 1 << 30


def snapshot_from_inventory(inventory : Sequence[NodeInventory], observed_at : float,
                            generation : str | None) -> InventorySnapshot:
    '''
    The contract's view of ``cluster.gpu_inventory_observed``: one identity per
    card (host ordinal, no UUID — the API does not expose one), occupancy as
    GPU units held by running pods, the node's sharing classification and
    owner stamp, and ``partial`` completeness whenever any node's pod listing
    failed.
    '''
    devices : list[DeviceIdentity] = []
    occupancy : dict[str, int] = {}
    sharing : dict[str, str] = {}
    owners : dict[str, str] = {}
    complete = True
    for node in inventory:
        memory = int(node.memory_gib_per_card * GIB) if node.memory_gib_per_card else None
        for i in range(node.card_count):
            devices.append(DeviceIdentity(node.name, i, None, None, node.product, memory))
        occupancy[node.name] = sum(units for resource, units in node.used_units.items() if _is_gpu_resource(resource))
        sharing[node.name] = (CLASS_TIME_SLICED if node.time_sliced else
                              CLASS_MIG if node.mig_partitioned else CLASS_PHYSICAL)
        if node.owner:
            owners[node.name] = node.owner
        complete = complete and node.occupancy_known
    if generation is None:
        material = repr([(n.name, n.card_count, n.owner, sorted(n.used_units.items()), n.mig_config)
                         for n in inventory]).encode('utf-8')
        generation = hashlib.sha256(material).hexdigest()[:16]
    return InventorySnapshot(tuple(devices), occupancy, sharing, owners,
                             COMPLETENESS_COMPLETE if complete else COMPLETENESS_PARTIAL, observed_at, generation)


def _place_static_slice(request : WorkloadRequest, inventory : Sequence[NodeInventory],
                        owners : Mapping[str, str], taken : dict[tuple[str, str], int]) -> DeviceIdentity | None:
    '''The first free advertised ``nvidia.com/mig-<profile>`` slice holding the request's minimum memory.'''
    minimum = (request.minimum_usable_memory_bytes or 0) / GIB
    for node in sorted(inventory, key = lambda n: n.name):
        if node.dra_owned or (owners.get(node.name) and owners.get(node.name) != flow_owner_value(request.flow_id)):
            continue
        table = mig_table_for_product(node.product)
        profiles = {p.resource: p for p in (table.profiles if table else ())}
        for resource, count in sorted(node.allocatable.items()):
            if not resource.startswith('nvidia.com/mig-'):
                continue
            profile = profiles.get(resource)
            memory = profile.memory_gib if profile else None
            if memory is None:
                # An unknown profile family: trust the name's GiB suffix (``1g.24gb``).
                digits = ''.join(ch for ch in resource.rsplit('.', 1)[-1] if ch.isdigit())
                memory = float(digits) if digits else 0.0
            if memory < minimum:
                continue
            free = count - node.used_units.get(resource, 0) - taken.get((node.name, resource), 0)
            if free <= 0:
                continue
            taken[(node.name, resource)] = taken.get((node.name, resource), 0) + 1
            memory_bytes = int(memory * GIB)
            return DeviceIdentity(node.name, None, None, f'{node.name}/{resource}/{taken[(node.name, resource)]}',
                                  node.product, memory_bytes, resource.split('nvidia.com/mig-', 1)[1])
    return None


#: GFD attributes a hard constraint may name, and how the inventory answers them.
CONSTRAINT_KEYS = {
    'nvidia.com/gpu.product': lambda node: node.product,
    'nvidia.com/gpu.memory': lambda node: int(node.memory_gib_per_card * 1024),      # MiB, as GFD labels it
    'kubernetes.io/hostname': lambda node: node.name,
}


def constraint_holds(constraint : Constraint, node : NodeInventory) -> bool | None:
    '''Whether ``node`` satisfies a constraint on a verifiable attribute; None when the key is not one.'''
    reader = CONSTRAINT_KEYS.get(constraint.key)
    if reader is None:
        return None
    actual = reader(node)
    op = constraint.operator
    if op in ('In', 'in'):
        return str(actual) in constraint.values
    if op in ('NotIn', 'not-in'):
        return str(actual) not in constraint.values
    if op in ('Gt', 'gt', '>=', 'gte'):
        try:
            return float(actual) >= float(constraint.values[0])
        except (TypeError, ValueError):
            return False
    if op in ('Lt', 'lt', '<=', 'lte'):
        try:
            return float(actual) <= float(constraint.values[0])
        except (TypeError, ValueError):
            return False
    return None


def constraint_expressions(constraints : Sequence[Constraint]) -> list[dict]:
    '''The hard constraints as node-affinity ``matchExpressions`` (soft ones are preferences, not terms).'''
    out : list[dict] = []
    for constraint in constraints:
        if not constraint.hard:
            continue
        op = {'in': 'In', 'In': 'In', 'not-in': 'NotIn', 'NotIn': 'NotIn', 'Gt': 'Gt', 'gt': 'Gt', '>=': 'Gt',
              'gte': 'Gt', 'Lt': 'Lt', 'lt': 'Lt', '<=': 'Lt', 'lte': 'Lt'}.get(constraint.operator)
        if op is None:
            continue
        out.append({'key': constraint.key, 'operator': op, 'values': list(constraint.values)})
    return out


def requests_as_specs(requests : Sequence[WorkloadRequest]) -> list[NodeSpec]:
    '''
    The layout solver speaks ``NodeSpec``: one single-replica spec per request,
    a MIG sharer carrying its minimum usable memory as ``gpu_memory_gib``.
    '''
    specs : list[NodeSpec] = []
    for request in requests:
        memory = (request.minimum_usable_memory_bytes / GIB
                  if request.sharing == SHARING_ISOLATED_MIG and request.minimum_usable_memory_bytes else None)
        specs.append(NodeSpec(request.workload_id, 'videoflow.processors.basic.IdentityProcessor', {}, [],
                              'processor', True, 1, 'gpu', True, gpu_count = request.device_count,
                              gpu_memory_gib = memory))
    return specs


@dataclasses.dataclass
class _Claim:
    plan : FeasiblePlan
    owner : str
    generation : str
    nodes : tuple[str, ...]
    applied : AppliedGeometry | None
    status : str
    requests : Mapping[str, tuple[Constraint, ...]] = dataclasses.field(default_factory = dict)


class KubernetesAllocationBackend(AcceleratorAllocationBackend):
    '''
    - Arguments:
        - strategy_name: a registered ``--gpu-mode`` (``exclusive`` or ``mix``).
        - kubectl: the kubectl binary every read and write goes through.
    '''
    def __init__(self, strategy_name : str = 'exclusive', kubectl : str = 'kubectl',
                 clock : Callable[[], float] = time.time) -> None:
        self._strategy = get_gpu_mode(strategy_name)
        self._name = strategy_name
        self._kubectl = kubectl
        self._clock = clock
        self._claims : dict[str, _Claim] = {}
        self._plans : dict[str, AllocationPlan | None] = {}
        self._constraints : dict[str, dict[str, tuple[Constraint, ...]]] = {}
        self._facts : dict[str, list[NodeInventory]] = {}
        self._counter = 0

    @property
    def managed_mig(self) -> bool:
        return isinstance(self._strategy, MixGpu)

    def capabilities(self, environment : Mapping[str, Any]) -> AllocationCapabilities:
        static_mig = bool(environment.get('static_mig', False))
        if self.managed_mig:
            return AllocationCapabilities(
                adapter = ADAPTER_MANAGED_MIG, authority = AUTHORITY_MANAGED_MIG,
                exclusive_device = True, isolated_mig = True, cooperative_sharing = False,
                memory_enforcement = ENFORCEMENT_HARDWARE, multi_device = True,
                topology_verification = False, elastic = False, admission_boundary = True,
                version_matrix = {k: str(v) for k, v in environment.items()})
        return AllocationCapabilities(
            adapter = ADAPTER_DEVICE_PLUGIN, authority = AUTHORITY_DEVICE_PLUGIN,
            exclusive_device = True, isolated_mig = static_mig, cooperative_sharing = False,
            memory_enforcement = ENFORCEMENT_HARDWARE if static_mig else ENFORCEMENT_NONE,
            multi_device = True, topology_verification = False, elastic = True, admission_boundary = True,
            version_matrix = {k: str(v) for k, v in environment.items()})

    # -- reads ------------------------------------------------------------------------------

    def inventory(self, scope : Mapping[str, Any]) -> Observation[InventorySnapshot]:
        # Function-level: cluster imports gpu (get_gpu_mode) at module scope — the same cycle gpu.py defers.
        from .cluster import gpu_inventory_observed

        observed = gpu_inventory_observed(self._kubectl)
        if isinstance(observed, Unknown):
            return observed
        snapshot = snapshot_from_inventory(observed.value, self._clock(), observed.generation)
        self._facts[snapshot.generation] = list(observed.value)
        return known(snapshot, snapshot.generation)

    def _inventory_for(self, snapshot : InventorySnapshot) -> list[NodeInventory]:
        '''The node records behind a snapshot — this backend's when it made it, else rebuilt from the snapshot.'''
        facts = self._facts.get(snapshot.generation)
        if facts is not None:
            return facts
        by_node : dict[str, list[DeviceIdentity]] = {}
        for device in snapshot.devices:
            if device.node:
                by_node.setdefault(device.node, []).append(device)
        return [NodeInventory(node, devices[0].product, len(devices),
                              (devices[0].memory_bytes or 0) / GIB,
                              time_sliced = snapshot.sharing.get(node) == CLASS_TIME_SLICED,
                              mig_partitioned = snapshot.sharing.get(node) == CLASS_MIG,
                              owner = snapshot.owners.get(node),
                              used_units = {DEFAULT_GPU_RESOURCE: snapshot.occupancy.get(node, 0)},
                              occupancy_known = snapshot.completeness == COMPLETENESS_COMPLETE)
                for node, devices in sorted(by_node.items())]

    # -- planning -------------------------------------------------------------------------

    def plan(self, requests : Sequence[WorkloadRequest], snapshot : InventorySnapshot) -> PlanOutcome:
        '''
        Per-host packing for whole-device requests (a replica's devices sit on
        one node) and, under ``mix``, the layout solver for MIG sharers. A
        partial snapshot plans nothing: occupancy that could not be read is not
        idle capacity.
        '''
        if snapshot.completeness != COMPLETENESS_COMPLETE:
            return Infeasible(('occupancy unknown: the pod listing failed, so no device can be assumed idle',))
        flow_id = requests[0].flow_id if requests else None
        inventory = self._inventory_for(snapshot)
        try:
            usable, excluded = _partition_inventory(inventory, flow_id)
        except UnobservableState as e:
            return Infeasible((str(e),))
        except ValueError as e:
            return Infeasible((str(e),))
        sharers = [r for r in requests if r.sharing == SHARING_ISOLATED_MIG]
        exclusive = [r for r in requests if r.sharing != SHARING_ISOLATED_MIG]
        reasons : list[str] = []
        assignments : dict[str, tuple[DeviceIdentity, ...]] = {}
        geometry : dict[str, str] = {}
        layout_plan : AllocationPlan | None = None
        mig_cards : dict[str, set[int]] = {}
        if sharers and not self.managed_mig:
            # Static MIG (ALLOC-017): slices an administrator carved and the device
            # plugin advertises as nvidia.com/mig-<profile>. Consumed as they are —
            # never repartitioned, never restored — from any pool node that has a
            # free matching slice, foreign geometry and all.
            static_used : dict[tuple[str, str], int] = {}
            for request in sharers:
                placed = _place_static_slice(request, inventory, snapshot.owners, static_used)
                if placed is None:
                    reasons.append(f'{request.workload_id}: no pool node advertises a free MIG slice of '
                                   f'>= {(request.minimum_usable_memory_bytes or 0) / GIB:g} GiB '
                                   f'(static slices are consumed as advertised; --gpu-mode mix would carve them)')
                else:
                    assignments[request.workload_id] = (placed,)
        elif sharers:
            try:
                layout = solve_layout(usable, requests_as_specs(sharers))
            except LayoutError as e:
                reasons.append(str(e))
            else:
                layout_plan = AllocationPlan(layout, excluded, flow_id, snapshot.generation)
                slots : dict[str, list[DeviceIdentity]] = {}
                for card in layout.cards:
                    if not card.profiles:
                        continue
                    mig_cards.setdefault(card.node, set()).add(card.card_index)
                    product = next(n.product for n in inventory if n.name == card.node)
                    for profile, count in card.profiles.items():
                        for i in range(count):
                            slots.setdefault(f'nvidia.com/mig-{profile}', []).append(DeviceIdentity(
                                card.node, card.card_index, None, f'{card.node}/{card.card_index}/{profile}/{i}',
                                product, None, profile))
                for request in sharers:
                    resource = layout.spec_resources.get(request.workload_id)
                    pool = slots.get(resource or '', [])
                    if not pool:
                        reasons.append(f'{request.workload_id}: the layout placed no slice for it')
                        continue
                    assignments[request.workload_id] = (pool.pop(0),)
                for node in layout.mig_nodes():
                    counts : dict[str, int] = {}
                    for card in layout.cards:
                        if card.node == node and card.profiles:
                            for profile, count in card.profiles.items():
                                counts[profile] = counts.get(profile, 0) + count
                    geometry[node] = ','.join(f'{k}:{v}' for k, v in sorted(counts.items()))
        if exclusive:
            free : dict[str, int] = {}
            for record in usable:
                whole = record.card_count - len(mig_cards.get(record.name, ()))
                if whole > 0:
                    free[record.name] = whole
            # Hard constraints narrow the nodes a request may land on (ALLOC-031):
            # by the validated GFD attributes the inventory carries, never by a key
            # nobody here can check — that one is an explicit rejection.
            eligible : dict[str, set[str]] = {}
            for request in exclusive:
                nodes = set(free)
                for constraint in request.constraints:
                    if not constraint.hard:
                        continue
                    verdicts = {n.name: constraint_holds(constraint, n) for n in usable if n.name in nodes}
                    if any(v is None for v in verdicts.values()):
                        reasons.append(f'{request.workload_id}: hard constraint {constraint.key} {constraint.operator} '
                                       f'{list(constraint.values)} names an attribute this backend cannot verify '
                                       f'({", ".join(sorted(CONSTRAINT_KEYS))} are); refusing rather than dropping it')
                        nodes = set()
                        break
                    nodes = {name for name, ok in verdicts.items() if ok}
                eligible[request.workload_id] = nodes
                if not nodes and not any(r.startswith(request.workload_id + ':') for r in reasons):
                    reasons.append(f'{request.workload_id}: no pool node satisfies its hard constraints '
                                   f'({", ".join(f"{c.key} {c.operator} {list(c.values)}" for c in request.constraints if c.hard)})')
            packed = pack_whole_devices([(r.workload_id, r.device_count) for r in exclusive], free,
                                        eligible = eligible) if not reasons else None
            if packed is None and not reasons:
                reasons.append('no per-host packing places every whole-device request on one node '
                               f'(free whole cards per node: {free}; excluded: {excluded or "none"})')
            elif packed is not None:
                taken : dict[str, int] = {}
                for request in exclusive:
                    node_name = packed[request.workload_id]
                    record = next(n for n in usable if n.name == node_name)
                    ordinals = [i for i in range(record.card_count) if i not in mig_cards.get(node_name, ())]
                    start = taken.get(node_name, 0)
                    chosen = ordinals[start:start + request.device_count]
                    taken[node_name] = start + request.device_count
                    memory = int(record.memory_gib_per_card * GIB) if record.memory_gib_per_card else None
                    assignments[request.workload_id] = tuple(
                        DeviceIdentity(node_name, i, None, None, record.product, memory) for i in chosen)
        if reasons:
            return Infeasible(tuple(reasons))
        plan_id = self._next_id('plan')
        self._plans[plan_id] = layout_plan
        self._constraints[plan_id] = {r.workload_id: r.constraints for r in requests}
        notes = tuple(f'{node}: {reason}' for node, reason in sorted(excluded.items()))
        return FeasiblePlan(assignments, geometry, snapshot.generation, plan_id, notes)

    # -- reservation, bindings, readiness --------------------------------------------------

    def reserve(self, plan : FeasiblePlan, operation_id : str,
                expected_generation : str | None) -> ClaimObservation:
        '''
        ``exclusive``: nothing to write — the plan is admitted and the scheduler
        accounts the claims. ``mix``: the node claims (CAS), the occupancy
        re-read, the ConfigMap/ClusterPolicy publication and the geometry
        labels, through ``MixGpu.apply_plan``; the claim's generation is the
        owner epoch stamped on its nodes.

        - Raises:
            - OwnershipConflict: the plan was made on another snapshot, a node \
                was taken, or occupancy changed after the claim.
            - UnobservableState: occupancy could not be re-read after the claim.
            - ClusterError: the manager refused or never applied the geometry.
        '''
        flow_id = operation_id.split(':')[0]
        owner = flow_owner_value(flow_id)
        if expected_generation is not None and expected_generation != plan.snapshot_generation:
            raise OwnershipConflict(
                f'the plan was made on snapshot {plan.snapshot_generation}, not {expected_generation}; '
                f'nothing was reserved', remedy = 'Re-plan against the current inventory.')
        claim_id = self._next_id('claim')
        nodes = tuple(sorted({d.node for devices in plan.assignments.values() for d in devices if d.node}))
        granted = tuple(d for devices in plan.assignments.values() for d in devices)
        layout_plan = self._plans.get(plan.plan_id)
        if layout_plan is None or not isinstance(self._strategy, MixGpu) or not layout_plan.mig_nodes():
            generation = f'advisory-{claim_id}'
            self._claims[claim_id] = _Claim(plan, owner, generation, nodes, None, CLAIM_ALLOCATED,
                                            self._constraints.get(plan.plan_id, {}))
            return ClaimObservation(claim_id, owner, generation, generation, CLAIM_ALLOCATED, granted,
                                    {'authority': AUTHORITY_DEVICE_PLUGIN if not self.managed_mig else AUTHORITY_MANAGED_MIG,
                                     'written': False, 'note': 'scheduler-accounted; readiness is the pod\'s'})
        try:
            applied = self._strategy.apply_plan(layout_plan, kubectl = self._kubectl, flow_id = flow_id)
        except (RuntimeError, ValueError) as e:
            raise ClusterError(f'GPU mode {self._name!r} could not apply the planned geometry: {e}',
                               remedy = 'Check the MIG manager logs and redeploy; the claims were released.') from e
        assert applied is not None
        self._claims[claim_id] = _Claim(plan, owner, applied.epoch, applied.nodes, applied, CLAIM_ALLOCATED,
                                        self._constraints.get(plan.plan_id, {}))
        observed = self.observe(claim_id)
        if isinstance(observed, Unknown):
            return ClaimObservation(claim_id, owner, applied.epoch, None, CLAIM_ALLOCATED, granted,
                                    {'authority': AUTHORITY_MANAGED_MIG, 'written': True, 'readiness': observed.reason})
        return observed.value

    def bindings(self, claim_id : str, workload_id : str) -> WorkloadBindings:
        '''
        What a pod of ``workload_id`` carries: the extended-resource limit the
        strategy renders (a MIG profile name for a sharer), a hostname affinity
        to the planned node, and — under ``mix`` — the owner label the pool
        selector keys on.
        '''
        claim = self._claims[claim_id]
        devices = claim.plan.assignments.get(workload_id, ())
        nodes = sorted({d.node for d in devices if d.node})
        profile = next((d.mig_profile for d in devices if d.mig_profile), None)
        resource = f'nvidia.com/mig-{profile}' if profile else DEFAULT_GPU_RESOURCE
        container = {'resources': {'limits': {resource: len(devices)}}} if devices else {}
        node_constraints : dict[str, Any] = {'nodes': nodes}
        if claim.applied is not None:
            node_constraints['owner_label'] = {GPU_OWNER_LABEL: claim.owner}
        pod : dict[str, Any] = {}
        expressions = constraint_expressions(claim.requests.get(workload_id, ()))
        if nodes:
            expressions.append({'key': 'kubernetes.io/hostname', 'operator': 'In', 'values': nodes})
        if expressions:
            pod = {'affinity': {'nodeAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': {
                'nodeSelectorTerms': [{'matchExpressions': expressions}]}}}}
            node_constraints['expressions'] = expressions
        return WorkloadBindings({'VF_GPU_COUNT': str(len(devices)), 'VF_GPU_RESOURCE_NAME': resource} if devices else {},
                                pod, container, [], node_constraints)

    def observe(self, claim_id : str) -> Observation[ClaimObservation]:
        claim = self._claims.get(claim_id)
        if claim is None:
            return unknown('missing', f'no claim {claim_id}')
        granted = tuple(d for devices in claim.plan.assignments.values() for d in devices)
        if claim.applied is None:
            return known(ClaimObservation(claim_id, claim.owner, claim.generation, claim.generation, claim.status,
                                          granted, {'written': False}))
        states = self._strategy.observe_geometry(self._kubectl, claim.applied) \
            if isinstance(self._strategy, MixGpu) else known({})
        if isinstance(states, Unknown):
            return states
        if any(s == 'failed' for s in states.value.values()):
            status = CLAIM_FAILED
        elif any(s == 'lost' for s in states.value.values()):
            status = CLAIM_FAILED
        elif all(s == 'ready' for s in states.value.values()):
            status = CLAIM_READY
        else:
            status = CLAIM_PENDING if claim.status == CLAIM_PENDING else CLAIM_ALLOCATED
        claim.status = status
        return known(ClaimObservation(claim_id, claim.owner, claim.generation, claim.generation, status, granted,
                                      {'nodes': dict(states.value), 'entries': dict(claim.applied.entries),
                                       'written': True}))

    def reconcile(self, claim_id : str, desired : str, expected_generation : str) -> ClaimObservation:
        claim = self._claims[claim_id]
        if claim.generation != expected_generation:
            return ClaimObservation(claim_id, claim.owner, expected_generation, claim.generation, CLAIM_FAILED, None,
                                    {'reason': 'stale generation'})
        observed = self.observe(claim_id)
        if isinstance(observed, Unknown):
            return ClaimObservation(claim_id, claim.owner, expected_generation, None, claim.status, None,
                                    {'reason': observed.reason})
        return observed.value

    def release(self, claim_id : str, operation_id : str, expected_generation : str,
                keep_workloads : bool = False) -> ReleaseObservation:
        '''
        ``mix``: this flow's geometry restored and its claims released through
        ``MixGpu.cleanup`` (idempotent; a node that did not revert keeps its
        restore record for a retry, reported as ``pending_recovery``). With
        ``keep_workloads`` nothing is touched: ownership and geometry stay for a
        later explicit release. A stale generation releases nothing.
        '''
        claim = self._claims.get(claim_id)
        if claim is None:
            return ReleaseObservation(claim_id, RELEASE_RELEASED, (), 'already released')
        if claim.generation != expected_generation:
            return ReleaseObservation(claim_id, RELEASE_STALE, claim.nodes, f'claim generation is {claim.generation}')
        if keep_workloads:
            return ReleaseObservation(claim_id, RELEASE_PENDING_RECOVERY, claim.nodes, 'workloads retained')
        if claim.applied is None:
            del self._claims[claim_id]
            return ReleaseObservation(claim_id, RELEASE_RELEASED, ())
        flow_id = operation_id.split(':')[0]
        self._strategy.cleanup(kubectl = self._kubectl, flow_id = flow_id)
        remaining = self._still_owned(claim)
        if remaining:
            return ReleaseObservation(claim_id, RELEASE_PENDING_RECOVERY, remaining,
                                      'nodes still carry this claim; retry the release')
        del self._claims[claim_id]
        return ReleaseObservation(claim_id, RELEASE_RELEASED, ())

    def _still_owned(self, claim : _Claim) -> tuple[str, ...]:
        # Function-level: same gpu <-> cluster cycle as above.
        from .cluster import gpu_inventory_observed

        observed = gpu_inventory_observed(self._kubectl)
        if isinstance(observed, Unknown):
            return claim.nodes
        owners = {n.name: n.owner for n in observed.value}
        return tuple(n for n in claim.nodes if owners.get(n) == claim.owner)

    def _next_id(self, kind : str) -> str:
        self._counter += 1
        return f'{kind}-{self._counter}'
