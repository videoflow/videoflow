'''
Dynamic Resource Allocation (``resource.k8s.io/v1``) as an
``AcceleratorAllocationBackend`` — **render-only** in this release (plan
decision D9).

What it does: turn a ``WorkloadRequest`` into the manifests and pod fragments
a DRA-scheduled worker needs — a ``ResourceClaimTemplate`` for independent
replicas (one claim per pod) or a shared ``ResourceClaim`` for an explicit
execution group, the pod's ``spec.resourceClaims`` entry and the container's
``resources.claims`` reference, with the ``DeviceClass`` named, never created
(it is the driver's and the cluster administrator's object). And say, per
feature, whether the cluster and driver at hand can serve a request:
``DraEnvironment`` is the version matrix — Kubernetes ≥ 1.34 for the ``v1``
API, ``DRAPartitionableDevices`` for dynamic MIG (alpha and off by default on
1.34/1.35, beta and on from 1.36), ``DRAConsumableCapacity`` for shared
capacity (same schedule), MPS as a driver capability with no Kubernetes gate,
and the pairs no driver can honour at once (MPS with dynamic MIG, MPS with
consumable shares) — rejected by name before anything is rendered
(ALLOC-019/020/021/023/024).

What it does not do: read a ``ResourceSlice``, create or watch a claim, or
release one. Those need a DRA driver running in the cluster (the NVIDIA DRA
driver), which the cluster this was built against does not have; every
lifecycle call raises ``CapabilityError`` naming that follow-up rather than
pretending. ``gpu.DraGpu`` (``--gpu-mode dra``) is the same rendering behind the CLI.

Field names follow the ``resource.k8s.io/v1`` API reference (read at
implementation time, Kubernetes 1.36 docs): a request is ``exactly:
{deviceClassName, allocationMode: ExactCount | All, count, selectors[].cel,
capacity.requests}``; a pod lists ``resourceClaims[].resourceClaimTemplateName``
or ``resourceClaimName`` and a container ``resources.claims[].name``.
'''
from __future__ import absolute_import, division, print_function

import json
import re
import subprocess
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from ..backends.allocation import (
    FEATURE_CONSUMABLE_CAPACITY,
    FEATURE_DYNAMIC_MIG,
    FEATURE_MPS,
    AcceleratorAllocationBackend,
    ClaimObservation,
    FeasiblePlan,
    InventorySnapshot,
    PlanOutcome,
    ReleaseObservation,
    WorkloadBindings,
    WorkloadRequest,
)
from ..backends.capabilities import (
    ENFORCEMENT_ACCOUNTING,
    ENFORCEMENT_HARDWARE,
    ENFORCEMENT_NONE,
    AllocationCapabilities,
)
from ..backends.outcomes import Observation
from ..core.errors import CapabilityError

DRA_API_VERSION = 'resource.k8s.io/v1'
#: The first Kubernetes release serving ``resource.k8s.io/v1`` (DRA GA).
DRA_GA_VERSION = (1, 34)

GATE_PARTITIONABLE = 'DRAPartitionableDevices'
GATE_CONSUMABLE = 'DRAConsumableCapacity'
GATE_PRIORITIZED_LIST = 'DRAPrioritizedList'
GATE_ADMIN_ACCESS = 'DRAAdminAccess'
GATE_DEVICE_TAINTS = 'DRADeviceTaints'

#: Feature-gate schedule per the Kubernetes feature-gates reference (1.37 docs):
#: ``(first minor, stage, on by default)`` rows in ascending order; a gate is
#: absent before its first row. ``stable`` gates are locked on.
GATE_SCHEDULE : dict[str, tuple[tuple[int, str, bool], ...]] = {
    GATE_PARTITIONABLE: ((33, 'alpha', False), (36, 'beta', True)),
    GATE_CONSUMABLE: ((34, 'alpha', False), (36, 'beta', True)),
    GATE_PRIORITIZED_LIST: ((33, 'alpha', False), (34, 'beta', True), (36, 'stable', True)),
    GATE_ADMIN_ACCESS: ((32, 'alpha', False), (34, 'beta', True), (36, 'stable', True)),
    GATE_DEVICE_TAINTS: ((33, 'alpha', False), (36, 'beta', True), (37, 'stable', True)),
}

#: Which Kubernetes gate each gated feature needs (MPS is purely a driver matter).
FEATURE_GATES = {FEATURE_DYNAMIC_MIG: GATE_PARTITIONABLE, FEATURE_CONSUMABLE_CAPACITY: GATE_CONSUMABLE}

DRIVER_INSTALL_REMEDY = ('Install a DRA driver for the GPUs (the NVIDIA DRA driver publishes ResourceSlices and '
                         'DeviceClasses such as gpu.nvidia.com) and enable the gates the features need; until then '
                         '--gpu-mode dra renders manifests only.')


def parse_version(version : str) -> tuple[int, int, int]:
    '''``v1.36.3+k3s1`` → ``(1, 36, 3)``; anything unparseable is ``(0, 0, 0)``.'''
    m = re.match(r'v?(\d+)\.(\d+)(?:\.(\d+))?', version.strip())
    if not m:
        return (0, 0, 0)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3) or 0))


def gate_stage(gate : str, version : tuple[int, int, int]) -> tuple[str, bool] | None:
    '''``(stage, on by default)`` of a gate at a Kubernetes version, or None when the gate does not exist there.'''
    rows = GATE_SCHEDULE.get(gate, ())
    current : tuple[str, bool] | None = None
    for minor, stage, default in rows:
        if version[0] > 1 or (version[0] == 1 and version[1] >= minor):
            current = (stage, default)
    return current


@dataclass(frozen = True)
class DraEnvironment:
    '''
    The cluster and driver a DRA request is judged against.

    - Arguments:
        - kubernetes_version: the API server's version string.
        - feature_gates: gate states known for certain (read from the API server \
            flags, or declared by the operator); a gate not listed takes its \
            version default.
        - device_classes: the ``DeviceClass`` names the cluster serves.
        - driver: the DRA driver name (``gpu.nvidia.com``), None when no driver \
            publishes ResourceSlices.
        - driver_features: the gated features the driver itself implements \
            (``dynamic-mig``, ``mps``, ``consumable-capacity``).
    '''
    kubernetes_version : str
    feature_gates : Mapping[str, bool] = field(default_factory = dict)
    device_classes : tuple[str, ...] = ()
    driver : str | None = None
    driver_features : frozenset[str] = frozenset()

    @property
    def version(self) -> tuple[int, int, int]:
        return parse_version(self.kubernetes_version)

    @property
    def api_served(self) -> bool:
        return self.version[:2] >= DRA_GA_VERSION

    def gate_enabled(self, gate : str) -> bool | None:
        '''True/False when known (declared, or the version default); None when the gate does not exist at this version.'''
        if gate in self.feature_gates:
            return bool(self.feature_gates[gate])
        stage = gate_stage(gate, self.version)
        return None if stage is None else stage[1]

    def unavailable_reasons(self) -> dict[str, str]:
        '''Gated feature → why this environment cannot serve it; a feature absent here is available.'''
        reasons : dict[str, str] = {}
        if not self.api_served:
            for feature in (FEATURE_DYNAMIC_MIG, FEATURE_CONSUMABLE_CAPACITY, FEATURE_MPS):
                reasons[feature] = (f'{DRA_API_VERSION} needs Kubernetes >= 1.{DRA_GA_VERSION[1]}; '
                                    f'this cluster is {self.kubernetes_version}')
            return reasons
        if self.driver is None:
            for feature in (FEATURE_DYNAMIC_MIG, FEATURE_CONSUMABLE_CAPACITY, FEATURE_MPS):
                reasons[feature] = 'no DRA driver publishes GPU ResourceSlices in this cluster'
            return reasons
        for feature, gate in FEATURE_GATES.items():
            enabled = self.gate_enabled(gate)
            stage = gate_stage(gate, self.version)
            if enabled is None:
                reasons[feature] = f'feature gate {gate} does not exist on Kubernetes {self.kubernetes_version}'
            elif not enabled:
                how = (f'{stage[0]}, off by default on {self.kubernetes_version}' if stage else 'disabled')
                reasons[feature] = f'feature gate {gate} is not enabled ({how})'
            elif feature not in self.driver_features:
                reasons[feature] = f'driver {self.driver} does not implement {feature}'
        if FEATURE_MPS not in self.driver_features:
            reasons[FEATURE_MPS] = f'driver {self.driver} does not implement MPS sharing'
        return reasons

    def available_features(self) -> tuple[str, ...]:
        missing = self.unavailable_reasons()
        return tuple(f for f in (FEATURE_DYNAMIC_MIG, FEATURE_MPS, FEATURE_CONSUMABLE_CAPACITY) if f not in missing)

    def to_mapping(self) -> dict[str, Any]:
        return {'kubernetes_version': self.kubernetes_version, 'feature_gates': dict(self.feature_gates),
                'device_classes': list(self.device_classes), 'driver': self.driver,
                'driver_features': sorted(self.driver_features)}

    @staticmethod
    def from_mapping(environment : Mapping[str, Any]) -> 'DraEnvironment':
        return DraEnvironment(
            kubernetes_version = str(environment.get('kubernetes_version', '')),
            feature_gates = {str(k): bool(v) for k, v in (environment.get('feature_gates') or {}).items()},
            device_classes = tuple(str(c) for c in environment.get('device_classes') or ()),
            driver = environment.get('driver'),
            driver_features = frozenset(str(f) for f in environment.get('driver_features') or ()))


def observe_environment(kubectl : str = 'kubectl') -> DraEnvironment:
    '''
    The environment as the API server reports it, read-only: server version,
    the ``DeviceClass`` names served (empty when the API is absent), and the
    driver behind the first GPU ``ResourceSlice`` (None when none exists).
    Gate states are not readable through the API and stay at version defaults;
    driver features are unknown to a read and stay empty — an operator who
    knows the driver declares them.
    '''
    version = ''
    try:
        out = subprocess.run([kubectl, 'version', '-o', 'json'], capture_output = True, text = True, check = False)
        version = str((json.loads(out.stdout).get('serverVersion') or {}).get('gitVersion', '')) if out.returncode == 0 else ''
    except (OSError, ValueError):
        version = ''
    classes : list[str] = []
    driver : str | None = None
    for kind, collect in (('deviceclasses.resource.k8s.io', 'classes'), ('resourceslices.resource.k8s.io', 'driver')):
        try:
            out = subprocess.run([kubectl, 'get', kind, '-o', 'json'], capture_output = True, text = True, check = False)
            items = json.loads(out.stdout).get('items', []) if out.returncode == 0 else []
        except (OSError, ValueError):
            items = []
        for item in items:
            if collect == 'classes':
                classes.append(str((item.get('metadata') or {}).get('name', '')))
            elif driver is None:
                driver = (item.get('spec') or {}).get('driver')
    return DraEnvironment(version, {}, tuple(sorted(c for c in classes if c)), driver)


def claim_name_for(workload_id : str) -> str:
    '''The pod-local claim name (``spec.resourceClaims[].name``) for a workload.'''
    return 'vf-gpu-' + re.sub(r'[^a-z0-9-]+', '-', workload_id.lower()).strip('-')[:40]


def device_request(request : WorkloadRequest, device_class : str,
                   capacity : Mapping[str, str] | None = None) -> dict:
    '''
    One ``spec.devices.requests[]`` entry: an ``exactly`` request for
    ``device_count`` devices of the class, with the request's hard constraints
    as CEL selectors on device attributes and any capacity requests. Plain
    dicts throughout — the shape is the Kubernetes API's, not ours.
    '''
    exactly : dict[str, Any] = {'deviceClassName': device_class, 'allocationMode': 'ExactCount',
                                'count': request.device_count}
    selectors = []
    for constraint in request.constraints:
        if not constraint.hard:
            continue
        if constraint.operator in ('In', 'in'):
            alternatives = ' || '.join(f'device.attributes["{constraint.key}"] == {json.dumps(v)}'
                                       for v in constraint.values)
            selectors.append({'cel': {'expression': alternatives}})
        elif constraint.operator in ('Gt', '>=', 'gte'):
            selectors.append({'cel': {'expression': f'device.attributes["{constraint.key}"] >= {constraint.values[0]}'}})
    if selectors:
        exactly['selectors'] = selectors
    if capacity:
        exactly['capacity'] = {'requests': dict(capacity)}
    return {'name': 'gpu', 'exactly': exactly}


def render_bindings(request : WorkloadRequest, device_class : str, namespace : str,
                    shared_claim : str | None = None, capacity : Mapping[str, str] | None = None) -> WorkloadBindings:
    '''
    The DRA manifests and fragments for one workload: a ``ResourceClaimTemplate``
    (every pod gets its own claim — independent replicas) unless ``shared_claim``
    names a ``ResourceClaim`` all pods of an execution group share. The
    ``DeviceClass`` is referenced by name and never rendered.
    '''
    claim = claim_name_for(request.workload_id)
    devices = {'requests': [device_request(request, device_class, capacity)]}
    if shared_claim:
        manifest = {'apiVersion': DRA_API_VERSION, 'kind': 'ResourceClaim',
                    'metadata': {'name': shared_claim, 'namespace': namespace},
                    'spec': {'devices': devices}}
        pod_claim = {'name': claim, 'resourceClaimName': shared_claim}
    else:
        template = f'{claim}-template'
        manifest = {'apiVersion': DRA_API_VERSION, 'kind': 'ResourceClaimTemplate',
                    'metadata': {'name': template, 'namespace': namespace},
                    'spec': {'spec': {'devices': devices}}}
        pod_claim = {'name': claim, 'resourceClaimTemplateName': template}
    return WorkloadBindings(
        env = {'VF_GPU_COUNT': str(request.device_count)},
        pod_fragment = {'resourceClaims': [pod_claim]},
        container_fragment = {'resources': {'claims': [{'name': claim}]}},
        claim_manifests = [manifest],
        node_constraints = {'device_class': device_class, 'shared': bool(shared_claim)})


def _render_only(operation : str) -> CapabilityError:
    return CapabilityError(
        f'DRA allocation cannot {operation}: --gpu-mode dra renders ResourceClaimTemplates and pod fragments '
        f'only; the claim lifecycle (inventory, reservation, readiness, release) needs a DRA driver in the cluster.',
        remedy = DRIVER_INSTALL_REMEDY)


class DraAllocationBackend(AcceleratorAllocationBackend):
    '''
    - Arguments:
        - environment: the ``DraEnvironment`` capabilities are judged against; \
            ``capabilities(mapping)`` may also carry one (``DraEnvironment.from_mapping``).
        - device_class: the ``DeviceClass`` every rendered claim references.
    '''
    def __init__(self, environment : DraEnvironment | None = None, device_class : str = 'gpu.nvidia.com') -> None:
        self._environment = environment
        self._device_class = device_class

    def capabilities(self, environment : Mapping[str, Any]) -> AllocationCapabilities:
        env = (DraEnvironment.from_mapping(environment) if environment.get('kubernetes_version')
               else self._environment or DraEnvironment(''))
        available = env.available_features()
        isolated = FEATURE_DYNAMIC_MIG in available
        shares = FEATURE_MPS in available or FEATURE_CONSUMABLE_CAPACITY in available
        return AllocationCapabilities(
            adapter = 'kubernetes-dra', authority = 'dra',
            exclusive_device = env.api_served and env.driver is not None,
            isolated_mig = isolated, cooperative_sharing = shares,
            memory_enforcement = (ENFORCEMENT_HARDWARE if isolated else
                                  ENFORCEMENT_ACCOUNTING if FEATURE_CONSUMABLE_CAPACITY in available else
                                  ENFORCEMENT_NONE),
            multi_device = True, topology_verification = False, elastic = False,
            admission_boundary = True,
            version_matrix = {'version': f'kubernetes {env.kubernetes_version or "unknown"} / driver {env.driver or "none"}',
                              'api': DRA_API_VERSION, 'features': available,
                              'unavailable': env.unavailable_reasons(),
                              'gates': {gate: env.gate_enabled(gate) for gate in GATE_SCHEDULE},
                              'device_classes': list(env.device_classes), 'render_only': True})

    def render(self, request : WorkloadRequest, namespace : str, shared_claim : str | None = None,
               capacity : Mapping[str, str] | None = None) -> WorkloadBindings:
        return render_bindings(request, self._device_class, namespace, shared_claim, capacity)

    # -- the lifecycle needs a driver ------------------------------------------------------

    def inventory(self, scope : Mapping[str, Any]) -> Observation[InventorySnapshot]:
        raise _render_only('read an inventory')

    def plan(self, requests : Sequence[WorkloadRequest], snapshot : InventorySnapshot) -> PlanOutcome:
        raise _render_only('plan against ResourceSlices')

    def reserve(self, plan : FeasiblePlan, operation_id : str, expected_generation : str | None) -> ClaimObservation:
        raise _render_only('reserve a claim')

    def bindings(self, claim_id : str, workload_id : str) -> WorkloadBindings:
        raise _render_only('bind an allocated claim (use render() for the manifests)')

    def observe(self, claim_id : str) -> Observation[ClaimObservation]:
        raise _render_only('observe a claim')

    def reconcile(self, claim_id : str, desired : str, expected_generation : str) -> ClaimObservation:
        raise _render_only('reconcile a claim')

    def release(self, claim_id : str, operation_id : str, expected_generation : str,
                keep_workloads : bool = False) -> ReleaseObservation:
        raise _render_only('release a claim')
