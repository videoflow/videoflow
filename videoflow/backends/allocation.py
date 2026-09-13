'''
The ``AcceleratorAllocationBackend`` contract: which accelerator resources may a
workload use, and under what guarantees.

Named for what it decides, not for one mechanism: a Kubernetes DRA driver, the
device plugin, videoflow's managed MIG geometry and a local ``nvidia-smi`` walk
are all implementations. What every implementation must share:

- **Inventory and plans are advisory.** A feasible plan is a snapshot, never a
  reservation; authoritative allocation must survive concurrent demand.
- **Reservation is a compare-and-swap.** Ownership is written under a server
  enforced precondition (a Kubernetes ``resourceVersion`` or a JSON-patch ``test``
  op), and a stale rollback cannot remove a newer owner.
- **Readiness is observed, not inferred.** "Allocated", "prepared" and
  "application-ready" are different states; a historical ``success`` label is
  not evidence for a new operation.
- **Reads that failed are Unknown.** An occupancy listing the API refused does
  not prove a device idle.
- **Release is idempotent and generation-fenced**, and a retained workload keeps
  its allocation until an explicit later release.

Memory is expressed in bytes with separate meanings — usable minimum, reserved,
hard limit, declared peak — because one ``gpu_memory_gb`` with three meanings
was how a scheduler reservation got mistaken for isolation.
'''
from __future__ import absolute_import, division, print_function

import abc
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence, Union

from .capabilities import ENFORCEMENT_NONE, AllocationCapabilities
from .outcomes import Observation

SHARING_EXCLUSIVE = 'exclusive'
SHARING_ISOLATED_MIG = 'isolated_mig'
SHARING_COOPERATIVE = 'cooperative'
SHARING_ACCOUNTING_ONLY = 'accounting_only'
SHARING_MODES = (SHARING_EXCLUSIVE, SHARING_ISOLATED_MIG, SHARING_COOPERATIVE, SHARING_ACCOUNTING_ONLY)

ELASTICITY_FIXED = 'fixed'
ELASTICITY_ELASTIC = 'elastic'

CLAIM_PENDING = 'pending'
CLAIM_ALLOCATED = 'allocated'
CLAIM_PREPARED = 'prepared'
CLAIM_READY = 'ready'
CLAIM_FAILED = 'failed'
CLAIM_RELEASING = 'releasing'
CLAIM_STATES = (CLAIM_PENDING, CLAIM_ALLOCATED, CLAIM_PREPARED, CLAIM_READY, CLAIM_FAILED, CLAIM_RELEASING)

RELEASE_RELEASED = 'released'
RELEASE_PENDING_RECOVERY = 'pending_recovery'
RELEASE_STALE = 'stale'

COMPLETENESS_COMPLETE = 'complete'
COMPLETENESS_PARTIAL = 'partial'

PROVENANCE_EXPLICIT = 'explicit'
PROVENANCE_DESCRIPTOR = 'descriptor'
PROVENANCE_DEFAULT = 'default'

@dataclass(frozen = True)
class DeviceIdentity:
    '''
    One accelerator as three different identifier spaces: a host ordinal, a
    physical GPU UUID, and (for a slice) a MIG UUID. They are not interchangeable;
    a worker maps whatever it was granted onto CUDA-local ordinals itself.
    '''
    node : str | None
    ordinal : int | None
    uuid : str | None
    mig_uuid : str | None
    product : str
    memory_bytes : int | None
    mig_profile : str | None = None

@dataclass(frozen = True)
class InventorySnapshot:
    '''
    - Arguments:
        - occupancy: device key -> units in use, as far as the reads could see.
        - sharing: node -> classification (``physical``, ``mig``, ``time-sliced``, \
            ``mps``, ``unknown``).
        - owners: node -> owner recorded on it (videoflow's own stamp), if any.
        - completeness: ``complete`` when every read succeeded, ``partial`` when \
            some read failed — a partial snapshot may plan but may not admit.
    '''
    devices : tuple[DeviceIdentity, ...]
    occupancy : Mapping[str, int]
    sharing : Mapping[str, str]
    owners : Mapping[str, str]
    completeness : str
    observed_at : float
    generation : str

@dataclass(frozen = True)
class Constraint:
    '''A hard requirement or a soft preference on device or node attributes.'''
    key : str
    operator : str
    values : tuple[str, ...]
    hard : bool = True

@dataclass(frozen = True)
class WorkloadRequest:
    flow_id : str
    run_id : str
    workload_id : str
    device_count : int
    sharing : str
    minimum_usable_memory_bytes : int | None = None
    reserved_memory_bytes : int | None = None
    hard_memory_limit_bytes : int | None = None
    declared_peak_memory_bytes : int | None = None
    features : frozenset[str] = frozenset()
    constraints : tuple[Constraint, ...] = ()
    elasticity : str = ELASTICITY_FIXED
    host_cpu : str | None = None
    host_memory : str | None = None
    provenance : Mapping[str, str] = field(default_factory = dict)

@dataclass(frozen = True)
class FeasiblePlan:
    '''Advisory. ``plan_id`` and ``snapshot_generation`` tie a later reservation to the inventory it was planned on.'''
    assignments : Mapping[str, tuple[DeviceIdentity, ...]]
    geometry : Mapping[str, str]
    snapshot_generation : str
    plan_id : str
    notes : tuple[str, ...] = ()

@dataclass(frozen = True)
class Infeasible:
    reasons : tuple[str, ...]

PlanOutcome = Union[FeasiblePlan, Infeasible]

@dataclass(frozen = True)
class ClaimObservation:
    claim_id : str
    owner : str
    desired_generation : str
    observed_generation : str | None
    status : str
    grant : tuple[DeviceIdentity, ...] | None
    evidence : Mapping[str, Any] = field(default_factory = dict)

@dataclass(frozen = True)
class WorkloadBindings:
    '''
    How a granted claim reaches a workload: environment for a process, and
    Kubernetes fragments (plain dicts — external API schema) for a pod.
    '''
    env : Mapping[str, str]
    pod_fragment : dict
    container_fragment : dict
    claim_manifests : list[dict] = field(default_factory = list)
    node_constraints : dict = field(default_factory = dict)

@dataclass(frozen = True)
class ReleaseObservation:
    claim_id : str
    status : str
    remaining : tuple[str, ...] = ()
    reason : str = ''

@dataclass(frozen = True)
class DeliveredGrant:
    '''What a workload actually received, distinct from what it requested.'''
    workload_id : str
    devices : tuple[DeviceIdentity, ...]
    exclusive : bool
    requested : int
    policy : str

    def to_dict(self) -> dict[str, Any]:
        return {
            'workload_id': self.workload_id,
            'devices': [{'node': d.node, 'ordinal': d.ordinal, 'uuid': d.uuid, 'mig_uuid': d.mig_uuid,
                         'product': d.product, 'memory_bytes': d.memory_bytes, 'mig_profile': d.mig_profile}
                        for d in self.devices],
            'exclusive': self.exclusive,
            'requested': self.requested,
            'policy': self.policy,
        }

    @staticmethod
    def from_dict(d : Mapping[str, Any]) -> 'DeliveredGrant':
        return DeliveredGrant(
            workload_id = str(d['workload_id']),
            devices = tuple(DeviceIdentity(x.get('node'), x.get('ordinal'), x.get('uuid'), x.get('mig_uuid'),
                                           str(x.get('product', '')), x.get('memory_bytes'), x.get('mig_profile'))
                            for x in d.get('devices', ())),
            exclusive = bool(d.get('exclusive', False)),
            requested = int(d.get('requested', 0)),
            policy = str(d.get('policy', '')),
        )

class AcceleratorAllocationBackend(abc.ABC):
    @abc.abstractmethod
    def capabilities(self, environment : Mapping[str, Any]) -> AllocationCapabilities:
        ...

    @abc.abstractmethod
    def inventory(self, scope : Mapping[str, Any]) -> Observation[InventorySnapshot]:
        ...

    @abc.abstractmethod
    def plan(self, requests : Sequence[WorkloadRequest], snapshot : InventorySnapshot) -> PlanOutcome:
        ...

    @abc.abstractmethod
    def reserve(self, plan : FeasiblePlan, operation_id : str,
                expected_generation : str | None) -> ClaimObservation:
        ...

    @abc.abstractmethod
    def bindings(self, claim_id : str, workload_id : str) -> WorkloadBindings:
        ...

    @abc.abstractmethod
    def observe(self, claim_id : str) -> Observation[ClaimObservation]:
        ...

    @abc.abstractmethod
    def reconcile(self, claim_id : str, desired : str, expected_generation : str) -> ClaimObservation:
        ...

    @abc.abstractmethod
    def release(self, claim_id : str, operation_id : str, expected_generation : str,
                keep_workloads : bool = False) -> ReleaseObservation:
        ...


#: Feature vocabulary a request may name (``WorkloadRequest.features``); each is
#: admitted only when the backend's version matrix lists it as available — a
#: request for dynamic MIG on hardware or a driver that cannot partition is
#: rejected by name, never by a blanket "impossible" (ALLOC-023).
FEATURE_DYNAMIC_MIG = 'dynamic-mig'
FEATURE_MPS = 'mps'
FEATURE_CONSUMABLE_CAPACITY = 'consumable-capacity'
FEATURE_PEER_ACCESS = 'peer-access'
GATED_FEATURES = (FEATURE_DYNAMIC_MIG, FEATURE_MPS, FEATURE_CONSUMABLE_CAPACITY)
#: Combinations no backend can serve at once: MPS shares a device between clients
#: while dynamic MIG re-partitions it, and consumable shares already carve it.
EXCLUSIVE_FEATURE_PAIRS = ((FEATURE_MPS, FEATURE_DYNAMIC_MIG), (FEATURE_MPS, FEATURE_CONSUMABLE_CAPACITY))


def allocation_rejections(requests : Sequence[WorkloadRequest],
                          capabilities : AllocationCapabilities) -> list[str]:
    '''
    Why the backend cannot serve these requests, before anything is planned or
    written — the validation-before-write boundary. Empty means admitted.
    Each reason names the request, the feature/policy/sharing kind it needs
    and what the backend (``adapter``/``authority``) actually offers, so the
    operator can tell "this driver version lacks the gate" from "this hardware
    cannot do it".
    '''
    reasons : list[str] = []
    who = f'{capabilities.adapter} ({capabilities.authority})'
    available = set(capabilities.version_matrix.get('features', ()))
    for request in requests:
        w = request.workload_id
        if request.sharing == SHARING_ISOLATED_MIG and not capabilities.isolated_mig:
            reasons.append(f'{w}: needs hardware-isolated MIG slices, which {who} does not provide')
        if request.sharing == SHARING_COOPERATIVE and not capabilities.cooperative_sharing:
            reasons.append(f'{w}: needs cooperative device sharing, which {who} does not provide')
        if request.sharing == SHARING_ACCOUNTING_ONLY and capabilities.memory_enforcement == ENFORCEMENT_NONE:
            reasons.append(f'{w}: needs memory accounting, but {who} enforces nothing')
        if request.device_count > 1 and not capabilities.multi_device:
            reasons.append(f'{w}: needs {request.device_count} devices in one grant; {who} grants one')
        if request.elasticity != ELASTICITY_FIXED and not capabilities.elastic:
            reasons.append(f'{w}: elasticity {request.elasticity!r} is not supported by {who}')
        if FEATURE_PEER_ACCESS in request.features and not capabilities.topology_verification:
            reasons.append(f'{w}: needs verified peer access between devices, which {who} cannot verify')
        for feature in sorted(request.features):
            if feature in GATED_FEATURES and feature not in available:
                matrix = capabilities.version_matrix.get('version') or 'unversioned'
                reasons.append(f'{w}: feature {feature!r} is not available on {who} '
                               f'(capability snapshot {matrix}; available: {sorted(available) or "none"})')
        for a, b in EXCLUSIVE_FEATURE_PAIRS:
            if a in request.features and b in request.features:
                reasons.append(f'{w}: {a!r} and {b!r} cannot be combined on one device')
    return reasons
