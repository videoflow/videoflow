'''
The in-memory ``AcceleratorAllocationBackend``: a cluster of GPU nodes as data,
with the concurrency protocol the real adapters must honour.

Modelled deliberately:

- **Reads can fail.** ``fail_reads('pods')`` makes the pod listing unavailable:
  the inventory then reports ``completeness='partial'`` with no occupancy, and a
  plan that needs occupancy is ``Infeasible('occupancy unknown')`` — never "idle".
  ``fail_reads('nodes')`` makes ``inventory()`` itself ``Unknown``.
- **Ownership is a server-side compare-and-swap.** Every node carries a
  ``resource_version``; a write names the version it read, and loses when a
  concurrent writer bumped it. Two reservers paused between their read and their
  write (``owner.read.after`` / ``owner.update.before`` barriers) race exactly as
  two ``kubectl`` clients would.
- **Readiness is correlated with the operation.** A node's ``mig.config.state``
  carries the generation of the write it answers; a stale ``success`` for an
  earlier geometry does not complete a new one.
- **Foreign workloads and retained workloads hold devices.** Geometry is never
  destroyed while any workload the model knows about still uses the node.
- **Shared configuration is never deleted.** The last owner out restores the
  pointer and leaves a tombstoned entry-less map, under CAS.
- **Every mutation is audited**, so a zero-mutation assertion is a list check.
'''
from __future__ import absolute_import, division, print_function

import itertools
import threading
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from ...core.errors import OwnershipConflict
from ...deploy.cluster import classify_gfd_labels
from .. import faults
from ..allocation import (
    CLAIM_ALLOCATED,
    CLAIM_FAILED,
    CLAIM_PENDING,
    CLAIM_PREPARED,
    CLAIM_READY,
    COMPLETENESS_COMPLETE,
    COMPLETENESS_PARTIAL,
    RELEASE_PENDING_RECOVERY,
    RELEASE_RELEASED,
    RELEASE_STALE,
    SHARING_ACCOUNTING_ONLY,
    SHARING_COOPERATIVE,
    SHARING_EXCLUSIVE,
    SHARING_ISOLATED_MIG,
    AcceleratorAllocationBackend,
    ClaimObservation,
    DeviceIdentity,
    FeasiblePlan,
    Infeasible,
    InventorySnapshot,
    PlanOutcome,
    ReleaseObservation,
    WorkloadBindings,
    WorkloadRequest,
)
from ..capabilities import (
    ENFORCEMENT_ACCOUNTING,
    ENFORCEMENT_HARDWARE,
    ENFORCEMENT_NONE,
    AllocationCapabilities,
)
from ..observation import ObservationLog
from ..outcomes import Observation, known, unknown
from .clock import FakeClock
from .mig_geometry import check_layout, smallest_profile

AUTHORITY_DEVICE_PLUGIN = 'device-plugin'
AUTHORITY_MANAGED_MIG = 'managed-mig'
AUTHORITY_LOCAL = 'local'
AUTHORITY_DRA = 'dra'

CLASS_PHYSICAL = 'physical'
CLASS_MIG = 'mig'
CLASS_TIME_SLICED = 'time-sliced'
CLASS_MPS = 'mps'
CLASS_UNKNOWN = 'unknown'

DEFAULT_POINTER = 'default-mig-parted-config'

@dataclass
class NodeFixture:
    '''One GPU node. ``labels`` follow GPU Feature Discovery's vocabulary.'''
    name : str
    product : str
    gpu_count : int
    memory_gib : float
    labels : dict[str, str] = field(default_factory = dict)
    owner : str | None = None
    owner_epoch : str | None = None
    resource_version : int = 1
    mig_config : str = 'all-disabled'
    mig_state : str = 'success'
    mig_state_generation : int = 0
    mig_layout : dict[str, int] = field(default_factory = dict)
    workloads : dict[str, int] = field(default_factory = dict)      # workload id -> GPU units held
    restore_record : str | None = None

    def classification(self) -> str:
        '''
        The same per-node rule the cluster reader applies to GPU Feature Discovery
        labels (``deploy.cluster.classify_gfd_labels``), so a fixture and a real
        node with the same labels are judged identically — plus geometry this
        backend applied itself, which a real node would advertise as slices.
        '''
        if self.mig_layout:
            return CLASS_MIG
        return classify_gfd_labels(self.labels)

@dataclass
class _Claim:
    claim_id : str
    owner : str
    plan : FeasiblePlan
    generation : str
    nodes : list[str]
    status : str = CLAIM_PENDING
    workload_ready : bool = False
    entry_name : str = ''

class MemoryAllocationBackend(AcceleratorAllocationBackend):
    '''
    - Arguments:
        - nodes: the cluster.
        - authority: which real adapter this instance stands in for; decides the \
            capabilities advertised and whether geometry can be changed.
        - mig_apply_seconds: fake-clock delay before a geometry write reaches \
            ``state=success`` (0 = immediately on the next observe).
    '''
    def __init__(self, nodes : Sequence[NodeFixture], clock : FakeClock | None = None,
                 authority : str = AUTHORITY_DEVICE_PLUGIN, mig_apply_seconds : float = 0.0,
                 log : ObservationLog | None = None) -> None:
        self._nodes = {n.name: n for n in nodes}
        self._clock = clock or FakeClock()
        self._authority = authority
        self._apply_delay = mig_apply_seconds
        self._log = log
        self._claims : dict[str, _Claim] = {}
        self._failed_reads : dict[str, str] = {}
        self._shared_config : dict[str, dict[str, int]] = {}
        self._shared_config_version = 1
        self._shared_tombstone = False
        self._pointer = DEFAULT_POINTER
        self._pointer_version = 1
        self._pointer_restore : str | None = None
        self._pending_geometry : list[tuple[float, str, int]] = []
        self.audit : list[tuple[str, str, str, str]] = []
        self._lock = threading.RLock()
        self._ids = itertools.count(1)

    # -- capabilities --------------------------------------------------------------------

    def capabilities(self, environment : Mapping[str, Any]) -> AllocationCapabilities:
        managed = self._authority == AUTHORITY_MANAGED_MIG
        dra = self._authority == AUTHORITY_DRA
        return AllocationCapabilities(
            adapter = f'memory-{self._authority}', authority = self._authority,
            exclusive_device = True, isolated_mig = managed or dra,
            cooperative_sharing = self._authority == AUTHORITY_LOCAL,
            memory_enforcement = ENFORCEMENT_HARDWARE if (managed or dra) else
                                 (ENFORCEMENT_NONE if self._authority == AUTHORITY_LOCAL else ENFORCEMENT_ACCOUNTING),
            multi_device = True, topology_verification = False, elastic = not managed,
            admission_boundary = True, version_matrix = dict(environment.get('version_matrix', {})))

    # -- reads ------------------------------------------------------------------------------

    def fail_reads(self, kind : str, reason : str | None = 'timeout') -> None:
        '''Make ``nodes`` / ``pods`` / ``config`` reads fail (``None`` restores them).'''
        with self._lock:
            if reason is None:
                self._failed_reads.pop(kind, None)
            else:
                self._failed_reads[kind] = reason

    def inventory(self, scope : Mapping[str, Any]) -> Observation[InventorySnapshot]:
        with self._lock:
            if 'nodes' in self._failed_reads:
                return unknown(self._failed_reads['nodes'], 'node listing failed')
            pool = scope.get('nodes')
            nodes = [n for n in self._nodes.values() if pool is None or n.name in pool]
            devices = tuple(DeviceIdentity(n.name, i, f'GPU-{n.name}-{i}', None, n.product,
                                           int(n.memory_gib * (1 << 30)), None)
                            for n in nodes for i in range(n.gpu_count))
            partial = 'pods' in self._failed_reads
            occupancy = {} if partial else {n.name: sum(n.workloads.values()) for n in nodes}
            snapshot = InventorySnapshot(
                devices, occupancy, {n.name: n.classification() for n in nodes},
                {n.name: n.owner for n in nodes if n.owner}, COMPLETENESS_PARTIAL if partial else COMPLETENESS_COMPLETE,
                self._clock.now(), str(sum(n.resource_version for n in nodes)))
            return known(snapshot, snapshot.generation)

    # -- planning -----------------------------------------------------------------------------

    def plan(self, requests : Sequence[WorkloadRequest], snapshot : InventorySnapshot) -> PlanOutcome:
        reasons : list[str] = []
        if snapshot.completeness != COMPLETENESS_COMPLETE:
            return Infeasible(('occupancy unknown: the pod listing failed, so no device can be assumed idle',))
        by_node : dict[str, list[DeviceIdentity]] = {}
        for device in snapshot.devices:
            if device.node is not None:
                by_node.setdefault(device.node, []).append(device)
        free : dict[str, int] = {}
        excluded : list[str] = []
        for node, devices in by_node.items():
            owner = snapshot.owners.get(node)
            if owner and requests and owner != requests[0].flow_id:
                excluded.append(f'{node}: owned by {owner}')
                continue                                   # another flow's node is not ours to plan on
            if snapshot.sharing.get(node) in (CLASS_TIME_SLICED, CLASS_MPS, CLASS_UNKNOWN, CLASS_MIG):
                # Shares and slices are not whole physical devices: an exclusive or
                # multi-device claim cannot land here (a MIG'd node advertises its
                # slices under the plain resource name), and unknown evidence is
                # never upgraded to physical. Sharers reach MIG through geometry.
                excluded.append(f'{node}: {snapshot.sharing.get(node)} — its units are shares or slices, not whole devices')
                continue
            free[node] = len(devices) - snapshot.occupancy.get(node, 0)
        assignments : dict[str, tuple[DeviceIdentity, ...]] = {}
        geometry : dict[str, str] = {}
        exclusive = [r for r in requests if r.sharing in (SHARING_EXCLUSIVE, SHARING_COOPERATIVE, SHARING_ACCOUNTING_ONLY)]
        sharers = [r for r in requests if r.sharing == SHARING_ISOLATED_MIG]
        packed = pack_whole_devices([(r.workload_id, r.device_count) for r in exclusive], free)
        if packed is None:
            reasons.append('no per-host packing places every whole-device request on one node '
                           f'(free per node: {free}; excluded: {excluded or "none"})')
        else:
            for workload_id, node in packed.items():
                request = next(r for r in exclusive if r.workload_id == workload_id)
                taken = [d for d in by_node[node] if d not in {x for v in assignments.values() for x in v}]
                assignments[workload_id] = tuple(taken[:request.device_count])
        if sharers:
            if self._authority not in (AUTHORITY_MANAGED_MIG, AUTHORITY_DRA):
                reasons.append(f'{self._authority} cannot provide isolated MIG slices')
            else:
                layouts : dict[str, dict[str, int]] = {}
                for request in sharers:
                    mig_node = next((n for n in free if n not in packed.values() and free[n] > 0), None) \
                        if packed is not None else None
                    if mig_node is None:
                        reasons.append(f'no free card for MIG sharer {request.workload_id}')
                        continue
                    node = mig_node
                    product = self._nodes[node].product
                    gib = (request.minimum_usable_memory_bytes or 0) / (1 << 30)
                    profile = smallest_profile(product, gib)
                    if profile is None:
                        reasons.append(f'no MIG profile on {product} holds {gib:g} GiB')
                        continue
                    layouts.setdefault(node, {})
                    layouts[node][profile.name] = layouts[node].get(profile.name, 0) + 1
                    assignments[request.workload_id] = (DeviceIdentity(node, None, None, f'MIG-{node}-{profile.name}-{layouts[node][profile.name]}',
                                                                        product, int(profile.memory_gib * (1 << 30)), profile.name),)
                for node, layout in layouts.items():
                    problems = check_layout(self._nodes[node].product, layout)
                    if problems:
                        reasons.extend(f'{node}: {p}' for p in problems)
                    geometry[node] = ','.join(f'{k}:{v}' for k, v in sorted(layout.items()))
        if reasons:
            return Infeasible(tuple(reasons))
        return FeasiblePlan(assignments, geometry, snapshot.generation, f'plan-{next(self._ids)}', tuple(excluded))

    # -- reservation (CAS) -------------------------------------------------------------------------

    def reserve(self, plan : FeasiblePlan, operation_id : str,
                expected_generation : str | None) -> ClaimObservation:
        owner = operation_id.split(':')[0]
        nodes = sorted({d.node for devices in plan.assignments.values() for d in devices if d.node})
        with self._lock:
            current = str(sum(n.resource_version for n in self._nodes.values()))
        if expected_generation is not None and expected_generation != current:
            # The plan was made on an inventory that has since changed: revalidate
            # before writing anything, never apply a stale plan (ALLOC-023).
            raise OwnershipConflict(
                f'the inventory changed since the plan was made (generation {expected_generation} -> '
                f'{current}); nothing was reserved', remedy = 'Re-plan against the current inventory.')
        claim_id = f'claim-{next(self._ids)}'
        generation = f'gen-{next(self._ids)}'
        stamped : list[str] = []
        with self._lock:
            for node_name in nodes:
                node = self._nodes[node_name]
                seen_owner, seen_version = node.owner, node.resource_version
                faults.barrier('owner.read.after', node = node_name, op_id = operation_id, version = seen_version)
                if seen_owner is not None and seen_owner != owner:
                    self._rollback(stamped, generation, operation_id)
                    raise OwnershipConflict(f'node {node_name} is owned by {seen_owner!r}',
                                            remedy = 'Re-plan against the remaining pool.', node = node_name)
                faults.barrier('owner.update.before', node = node_name, op_id = operation_id)
                if node.resource_version != seen_version:
                    self._rollback(stamped, generation, operation_id)
                    raise OwnershipConflict(
                        f'node {node_name} changed (resourceVersion {seen_version} -> {node.resource_version}) '
                        f'between the read and the write', remedy = 'Re-plan against the current state.',
                        node = node_name)
                if node.owner != owner:
                    node.owner, node.owner_epoch = owner, generation
                    node.resource_version += 1
                    stamped.append(node_name)
                    self._record(operation_id, 'stamp-owner', node_name, owner)
                faults.barrier('owner.update.after', node = node_name, op_id = operation_id)
            claim = _Claim(claim_id, owner, plan, generation, nodes)
            self._claims[claim_id] = claim
            faults.barrier('claim.create.after', claim = claim_id, op_id = operation_id)
            if plan.geometry and self._authority == AUTHORITY_MANAGED_MIG:
                entry = f'videoflow-{owner}-{generation}'
                claim.entry_name = entry
                self._shared_config[entry] = dict.fromkeys(plan.geometry, 1)
                self._shared_config_version += 1
                self._shared_tombstone = False
                self._record(operation_id, 'publish-entry', entry, str(plan.geometry))
                if self._pointer != 'videoflow-map':
                    self._pointer_restore = self._pointer
                    self._pointer = 'videoflow-map'
                    self._pointer_version += 1
                    self._record(operation_id, 'point-policy', 'clusterpolicy', 'videoflow-map')
                for node_name in plan.geometry:
                    node = self._nodes[node_name]
                    if node.restore_record is None:
                        node.restore_record = node.mig_config
                        self._record(operation_id, 'record-restore', node_name, node.mig_config)
                    node.mig_config = entry
                    node.mig_state = 'pending'
                    node.resource_version += 1
                    self._record(operation_id, 'label-geometry', node_name, entry)
                    version = node.resource_version
                    if self._apply_delay > 0:
                        self._pending_geometry.append((self._clock.now() + self._apply_delay, node_name, version))
                    else:
                        self._apply_geometry(node_name, version)
                claim.status = CLAIM_ALLOCATED
            else:
                claim.status = CLAIM_ALLOCATED
            faults.barrier('claim.schedule.after', claim = claim_id, op_id = operation_id)
            return self._observe(claim)

    def _rollback(self, stamped : list[str], generation : str, operation_id : str) -> None:
        for node_name in stamped:
            node = self._nodes[node_name]
            if node.owner_epoch == generation:           # only what *we* stamped, never a newer owner
                node.owner, node.owner_epoch = None, None
                node.resource_version += 1
                self._record(operation_id, 'release-owner', node_name, 'rollback')

    def _apply_geometry(self, node_name : str, version : int) -> None:
        node = self._nodes[node_name]
        node.mig_state = 'success'
        node.mig_state_generation = version
        layout : dict[str, int] = {}
        entry = self._shared_config.get(node.mig_config)
        if entry is not None:
            for claim in self._claims.values():
                if claim.entry_name == node.mig_config:
                    for part in claim.plan.geometry.get(node_name, '').split(','):
                        if ':' in part:
                            profile, count = part.split(':')
                            layout[profile] = int(count)
        node.mig_layout = layout

    def apply_pending_geometry(self) -> None:
        '''Advance every geometry write whose fake-clock delay has elapsed (tests call this after ``clock.advance``).'''
        with self._lock:
            now = self._clock.now()
            due = [p for p in self._pending_geometry if p[0] <= now]
            self._pending_geometry = [p for p in self._pending_geometry if p[0] > now]
            for _, node_name, version in due:
                self._apply_geometry(node_name, version)

    # -- observation ---------------------------------------------------------------------------------

    def observe(self, claim_id : str) -> Observation[ClaimObservation]:
        with self._lock:
            if 'nodes' in self._failed_reads:
                return unknown(self._failed_reads['nodes'], 'node read failed')
            claim = self._claims.get(claim_id)
            if claim is None:
                return unknown('unreachable', f'no claim {claim_id}')
            return known(self._observe(claim), claim.generation)

    def _observe(self, claim : _Claim) -> ClaimObservation:
        evidence : dict[str, Any] = {}
        prepared = True
        for node_name in claim.nodes:
            node = self._nodes[node_name]
            evidence[node_name] = {'mig_config': node.mig_config, 'state': node.mig_state,
                                   'state_generation': node.mig_state_generation,
                                   'resource_version': node.resource_version, 'layout': dict(node.mig_layout)}
            if claim.entry_name:
                # Operation-correlated readiness: the state must answer *this* write.
                if not (node.mig_config == claim.entry_name and node.mig_state == 'success'
                        and node.mig_state_generation >= node.resource_version - 0):
                    prepared = False
        if claim.status == CLAIM_FAILED:
            status = CLAIM_FAILED
        elif claim.workload_ready:
            status = CLAIM_READY
        elif claim.entry_name and prepared:
            status = CLAIM_PREPARED
        else:
            status = claim.status
        grant = tuple(d for devices in claim.plan.assignments.values() for d in devices)
        return ClaimObservation(claim.claim_id, claim.owner, claim.generation, claim.generation, status, grant, evidence)

    def mark_workload_ready(self, claim_id : str) -> None:
        with self._lock:
            self._claims[claim_id].workload_ready = True
            faults.barrier('claim.ready.after', claim = claim_id)

    def bindings(self, claim_id : str, workload_id : str) -> WorkloadBindings:
        with self._lock:
            claim = self._claims[claim_id]
            devices = claim.plan.assignments.get(workload_id, ())
            env = {'CUDA_VISIBLE_DEVICES': ','.join(d.mig_uuid or d.uuid or str(d.ordinal) for d in devices),
                   'VF_GPU_COUNT': str(len(devices))}
            nodes = sorted({d.node for d in devices if d.node})
            return WorkloadBindings(env, {'nodeSelector': {'kubernetes.io/hostname': nodes[0]}} if nodes else {},
                                    {'resources': {'limits': {'nvidia.com/gpu': len(devices)}}}, [],
                                    {'nodes': nodes})

    def reconcile(self, claim_id : str, desired : str, expected_generation : str) -> ClaimObservation:
        with self._lock:
            claim = self._claims[claim_id]
            if claim.generation != expected_generation:
                raise OwnershipConflict(f'claim {claim_id} is at {claim.generation}, not {expected_generation}',
                                        remedy = 'Observe the claim and retry with its current generation.')
            if desired == CLAIM_FAILED:
                claim.status = CLAIM_FAILED
            return self._observe(claim)

    # -- release ------------------------------------------------------------------------------------------

    def release(self, claim_id : str, operation_id : str, expected_generation : str,
                keep_workloads : bool = False) -> ReleaseObservation:
        with self._lock:
            claim = self._claims.get(claim_id)
            if claim is None:
                return ReleaseObservation(claim_id, RELEASE_RELEASED, (), 'already released')
            if claim.generation != expected_generation:
                return ReleaseObservation(claim_id, RELEASE_STALE, tuple(claim.nodes),
                                          f'claim generation is {claim.generation}')
            busy = [n for n in claim.nodes if self._nodes[n].workloads]
            if keep_workloads or busy:
                return ReleaseObservation(claim_id, RELEASE_PENDING_RECOVERY, tuple(busy or claim.nodes),
                                          'workloads still hold the devices' if busy else 'workloads retained')
            faults.barrier('restore.before', claim = claim_id, op_id = operation_id)
            if 'config' in self._failed_reads:
                return ReleaseObservation(claim_id, RELEASE_PENDING_RECOVERY, tuple(claim.nodes),
                                          'shared configuration could not be read; ownership retained for retry')
            for node_name in claim.nodes:
                node = self._nodes[node_name]
                if claim.entry_name and node.restore_record is not None:
                    node.mig_config = node.restore_record
                    node.mig_layout = {}
                    node.mig_state = 'success'
                    node.resource_version += 1
                    node.mig_state_generation = node.resource_version
                    node.restore_record = None
                    self._record(operation_id, 'restore-geometry', node_name, node.mig_config)
                if node.owner == claim.owner and node.owner_epoch == claim.generation:
                    node.owner, node.owner_epoch = None, None
                    node.resource_version += 1
                    self._record(operation_id, 'release-owner', node_name, claim.owner)
            faults.barrier('restore.after', claim = claim_id, op_id = operation_id)
            if claim.entry_name:
                faults.barrier('delete.before', entry = claim.entry_name, op_id = operation_id)
                self._shared_config.pop(claim.entry_name, None)
                self._shared_config_version += 1
                self._record(operation_id, 'strip-entry', claim.entry_name, '')
                if not self._shared_config:
                    # Last one out: restore the pointer, keep the map (tombstoned), never delete it.
                    if self._pointer_restore is not None:
                        self._pointer = self._pointer_restore
                        self._pointer_restore = None
                        self._pointer_version += 1
                        self._record(operation_id, 'restore-policy', 'clusterpolicy', self._pointer)
                    self._shared_tombstone = True
                faults.barrier('delete.after', entry = claim.entry_name, op_id = operation_id)
            del self._claims[claim_id]
            return ReleaseObservation(claim_id, RELEASE_RELEASED, ())

    # -- test hooks ---------------------------------------------------------------------------------------

    def bind_workload(self, node : str, workload_id : str, units : int) -> None:
        '''A workload (ours or foreign) now holds ``units`` devices on ``node``.'''
        with self._lock:
            self._nodes[node].workloads[workload_id] = units
            self._nodes[node].resource_version += 1

    def unbind_workload(self, node : str, workload_id : str) -> None:
        with self._lock:
            self._nodes[node].workloads.pop(workload_id, None)

    def node(self, name : str) -> NodeFixture:
        return self._nodes[name]

    def shared_config(self) -> tuple[dict[str, dict[str, int]], int, bool]:
        with self._lock:
            return dict(self._shared_config), self._shared_config_version, self._shared_tombstone

    def pointer(self) -> str:
        return self._pointer

    def mutations(self, kinds : Sequence[str] | None = None) -> list[tuple[str, str, str, str]]:
        with self._lock:
            return [m for m in self.audit if kinds is None or m[1] in kinds]

    def _record(self, operation_id : str, kind : str, target : str, detail : str) -> None:
        self.audit.append((operation_id, kind, target, detail))
        if self._log is not None:
            self._log.emit('mutation', op_id = operation_id, mutation = kind, target = target, detail = detail)

def pack_whole_devices(requests : Sequence[tuple[str, int]], free : Mapping[str, int]) -> dict[str, str] | None:
    '''
    Exhaustive per-host packing for small inventories: each request needs
    ``count`` whole devices on *one* host. Returns workload -> host, or None when
    no assignment exists — the independent oracle for "aggregate capacity hides
    per-node fragmentation" (three hosts with two free each cannot host three
    requests of two? they can; two hosts with three free each cannot host three
    requests of two).
    '''
    remaining = dict(free)
    order = sorted(requests, key = lambda r: -r[1])
    assignment : dict[str, str] = {}

    def place(i : int) -> bool:
        if i == len(order):
            return True
        workload, count = order[i]
        for host in sorted(remaining):
            if remaining[host] >= count:
                remaining[host] -= count
                assignment[workload] = host
                if place(i + 1):
                    return True
                remaining[host] += count
                del assignment[workload]
        return False

    return assignment if place(0) else None
