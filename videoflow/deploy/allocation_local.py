'''
The local ``AcceleratorAllocationBackend``: this host's GPUs, read through
``nvidia-smi``, partitioned across the workers of a ``run-local`` flow.

Two policies, one contract (plan §C step 4, decision D4):

- ``shared`` (the default until RFC 0006 is accepted) reproduces today's
  wrap-around walk — when demand exceeds the visible devices, workers share
  them. What changes is the *reporting*: every worker receives a
  ``DeliveredGrant`` (``VF_GPU_GRANT_JSON``) that says how many devices it
  really got and that the grant is not exclusive, so a requested ``gpu_count``
  is never presented as delivered capacity (ALLOC-014, RUN-044).
- ``strict`` refuses, before any worker is launched, a flow whose exclusive
  requests do not fit the host's distinct devices, and admits cooperative
  sharers only within a declared peak-memory budget plus headroom
  (``VF_GPU_HEADROOM_BYTES``, ALLOC-016). A missing declaration is a rejection,
  never a guess.

What the host says, the backend repeats: a failed ``nvidia-smi`` read is an
``Unknown`` inventory ("could not observe"), never a confirmed zero-GPU
machine; an inherited ``CUDA_VISIBLE_DEVICES`` narrows the pool by ordinal,
card UUID or MIG UUID exactly as the CUDA runtime would (ALLOC-015). Grants
are written to the workers as UUIDs, the identity that survives renumbering.

There is no server-side compare-and-swap on a single host, so ``reserve``
fences on the inventory generation instead: a plan made on one snapshot is
refused when the host has changed since (a device disappeared, a foreign
process appeared), and ``release`` refuses a stale generation the same way.
'''
from __future__ import absolute_import, division, print_function

import hashlib
import json
import logging
import os
import subprocess
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping, Sequence

from ..backends.allocation import (
    CLAIM_ALLOCATED,
    CLAIM_FAILED,
    CLAIM_READY,
    COMPLETENESS_COMPLETE,
    COMPLETENESS_PARTIAL,
    RELEASE_RELEASED,
    RELEASE_STALE,
    SHARING_COOPERATIVE,
    AcceleratorAllocationBackend,
    ClaimObservation,
    DeliveredGrant,
    DeviceIdentity,
    FeasiblePlan,
    Infeasible,
    InventorySnapshot,
    PlanOutcome,
    ReleaseObservation,
    WorkloadBindings,
    WorkloadRequest,
)
from ..backends.capabilities import ENFORCEMENT_ACCOUNTING, ENFORCEMENT_NONE, AllocationCapabilities
from ..backends.outcomes import Observation, Unknown, known, unknown
from ..utils.system import (
    NVIDIA_SMI_TIMEOUT_SECONDS,
    apply_mask,
    host_devices_observed,
    mask_entries,
    parse_smi_used,
)

logger = logging.getLogger(__package__)

POLICY_SHARED = 'shared'
POLICY_STRICT = 'strict'
GPU_POLICIES = (POLICY_SHARED, POLICY_STRICT)

#: Env: bytes kept free on every shared device beyond the declared peaks
#: (allocator fragmentation, the CUDA context of each process). Strict policy only.
HEADROOM_ENV = 'VF_GPU_HEADROOM_BYTES'
DEFAULT_HEADROOM_BYTES = 1 << 30
#: Env: the worker-side copy of its ``DeliveredGrant`` (RFC 0006 ENV-14).
GRANT_ENV = 'VF_GPU_GRANT_JSON'

ADAPTER = 'local-nvidia-smi'
AUTHORITY_LOCAL = 'local'

HostReader = Callable[[], Observation[list[DeviceIdentity]]]
UsedReader = Callable[[], Observation[dict[str, int]]]


def device_key(device : DeviceIdentity) -> str:
    '''The mask entry a worker receives for a device: its MIG UUID, else its card UUID, else the ordinal.'''
    return device.mig_uuid or device.uuid or str(device.ordinal)


def host_memory_used_observed() -> Observation[dict[str, int]]:
    '''``card uuid -> bytes in use`` from ``nvidia-smi``, or ``Unknown`` when the read failed.'''
    try:
        text = subprocess.check_output(
            ['nvidia-smi', '--query-gpu=index,uuid,name,memory.total,memory.used', '--format=csv,noheader,nounits'],
            timeout = NVIDIA_SMI_TIMEOUT_SECONDS).decode('utf-8', errors = 'replace')
    except FileNotFoundError:
        return unknown('missing', 'nvidia-smi is not on PATH (no NVIDIA driver)')
    except subprocess.TimeoutExpired:
        return unknown('timeout', f'nvidia-smi did not answer within {NVIDIA_SMI_TIMEOUT_SECONDS}s')
    except (OSError, subprocess.SubprocessError) as e:
        return unknown('failed', f'{type(e).__name__}: {e}')
    return known(parse_smi_used(text))


def snapshot_generation(devices : Sequence[DeviceIdentity], used : Mapping[str, int]) -> str:
    '''A digest of what was observed: the fence ``reserve``/``release`` check the host against.'''
    material = json.dumps([[d.ordinal, d.uuid, d.mig_uuid, d.memory_bytes, used.get(d.uuid or '', 0)] for d in devices])
    return hashlib.sha256(material.encode('utf-8')).hexdigest()[:16]


@dataclass
class _Claim:
    plan : FeasiblePlan
    generation : str
    owner : str
    status : str
    grants : dict[str, DeliveredGrant]


class LocalAllocationBackend(AcceleratorAllocationBackend):
    '''
    - Arguments:
        - policy: ``shared`` (wrap-around, labelled) or ``strict`` (refuse short or \
            over-budget grants before launch).
        - headroom_bytes: strict-policy memory headroom per shared device; defaults \
            to ``VF_GPU_HEADROOM_BYTES`` or 1 GiB.
        - host_reader / used_reader: the ``nvidia-smi`` reads, injectable so a \
            process-level test can present one, zero or unobservable GPUs.
        - mask: a ``CUDA_VISIBLE_DEVICES`` value applied to the host pool before \
            planning; by default the one this process inherited from its environment \
            (``inherit_mask=False`` ignores the environment: the whole host).
    '''
    def __init__(self, policy : str = POLICY_SHARED, headroom_bytes : int | None = None,
                 host_reader : HostReader = host_devices_observed,
                 used_reader : UsedReader = host_memory_used_observed,
                 mask : str | None = None, inherit_mask : bool = True,
                 clock : Callable[[], float] = time.time) -> None:
        if policy not in GPU_POLICIES:
            raise ValueError(f'unknown local GPU policy {policy!r}; known: {", ".join(GPU_POLICIES)}')
        self._policy = policy
        env_headroom = os.environ.get(HEADROOM_ENV)
        self._headroom = (headroom_bytes if headroom_bytes is not None
                          else int(env_headroom) if env_headroom else DEFAULT_HEADROOM_BYTES)
        self._host_reader = host_reader
        self._used_reader = used_reader
        self._mask = (mask if mask is not None
                      else os.environ.get('CUDA_VISIBLE_DEVICES') if inherit_mask else None)
        self._clock = clock
        self._claims : dict[str, _Claim] = {}
        # plan id -> workload -> devices requested: what a grant is measured against.
        self._requested : dict[str, dict[str, int]] = {}
        self._counter = 0

    @property
    def policy(self) -> str:
        return self._policy

    def capabilities(self, environment : Mapping[str, Any]) -> AllocationCapabilities:
        strict = self._policy == POLICY_STRICT
        return AllocationCapabilities(
            adapter = ADAPTER, authority = AUTHORITY_LOCAL,
            exclusive_device = strict, isolated_mig = False, cooperative_sharing = True,
            # Strict policy accounts declared peaks against the device; nothing on a
            # single host enforces them, and the shared policy does not even account.
            memory_enforcement = ENFORCEMENT_ACCOUNTING if strict else ENFORCEMENT_NONE,
            multi_device = True, topology_verification = False, elastic = False,
            admission_boundary = strict,
            version_matrix = {'policy': self._policy, **{k: str(v) for k, v in environment.items()}})

    # -- reads ------------------------------------------------------------------------------

    def inventory(self, scope : Mapping[str, Any]) -> Observation[InventorySnapshot]:
        '''
        The devices this process may hand out: the host's, narrowed by the
        inherited mask. ``Unknown`` when the host could not be read — the caller
        decides what an unobservable host means, never "no GPUs".
        '''
        observed = self._host_reader()
        if isinstance(observed, Unknown):
            return observed
        pool = apply_mask(observed.value, mask_entries(self._mask))
        used_observed = self._used_reader()
        used = dict(used_observed.value) if not isinstance(used_observed, Unknown) else {}
        completeness = COMPLETENESS_COMPLETE if not isinstance(used_observed, Unknown) else COMPLETENESS_PARTIAL
        occupancy = {device_key(d): used.get(d.uuid or '', 0) for d in pool}
        return known(InventorySnapshot(
            devices = tuple(pool), occupancy = occupancy,
            sharing = {device_key(d): 'physical' if d.mig_uuid is None else 'mig' for d in pool},
            owners = {}, completeness = completeness, observed_at = self._clock(),
            generation = snapshot_generation(pool, used)))

    # -- planning -------------------------------------------------------------------------

    def plan(self, requests : Sequence[WorkloadRequest], snapshot : InventorySnapshot) -> PlanOutcome:
        outcome = (self._plan_strict(requests, snapshot) if self._policy == POLICY_STRICT
                   else self._plan_shared(requests, snapshot))
        if isinstance(outcome, FeasiblePlan):
            self._requested[outcome.plan_id] = {r.workload_id: r.device_count for r in requests}
        return outcome

    def _plan_shared(self, requests : Sequence[WorkloadRequest], snapshot : InventorySnapshot) -> PlanOutcome:
        '''
        Today's walk (``engines.local.assign_local_gpus``): each request takes
        the next ``device_count`` devices, wrapping around when the pool runs
        out. Every shortfall and every shared device is a note, and the grants
        the bindings carry say ``exclusive=False`` whenever a device is shared.
        '''
        pool = list(snapshot.devices)
        if not pool:
            return Infeasible(('no GPU is visible to this process (the host reports none, or '
                               'CUDA_VISIBLE_DEVICES hides them all)',))
        assignments : dict[str, tuple[DeviceIdentity, ...]] = {}
        notes : list[str] = []
        cursor = 0
        demand = 0
        for request in requests:
            devices = [pool[(cursor + i) % len(pool)] for i in range(request.device_count)]
            cursor += request.device_count
            demand += request.device_count
            granted = tuple(dict.fromkeys(devices))
            if len(granted) < request.device_count:
                notes.append(f'{request.workload_id}: asked for {request.device_count} devices, '
                             f'{len(granted)} distinct visible — the delivered grant is {len(granted)}')
            assignments[request.workload_id] = granted
        if demand > len(pool):
            notes.append(f'demand {demand} exceeds the {len(pool)} visible device(s): workers share devices '
                         f'(policy shared); the same flow will not schedule this way on Kubernetes')
        for request in requests:
            if request.declared_peak_memory_bytes is None and request.sharing == SHARING_COOPERATIVE:
                notes.append(f'{request.workload_id}: no declared peak memory; sharing is unaccounted (policy shared)')
        return FeasiblePlan(assignments, {}, snapshot.generation, self._next_id('plan'), tuple(notes))

    def _plan_strict(self, requests : Sequence[WorkloadRequest], snapshot : InventorySnapshot) -> PlanOutcome:
        '''
        Exclusive requests take distinct devices or the plan is infeasible;
        cooperative requests share devices within ``memory - used - headroom``
        of declared peaks. Nothing is admitted on a partial snapshot.
        '''
        reasons : list[str] = []
        pool = list(snapshot.devices)
        if snapshot.completeness != COMPLETENESS_COMPLETE:
            reasons.append('device memory in use could not be read; strict admission needs it '
                           '(rerun once nvidia-smi answers, or use --gpu-policy shared)')
        if not pool:
            reasons.append('no GPU is visible to this process (the host reports none, or '
                           'CUDA_VISIBLE_DEVICES hides them all)')
        exclusive = [r for r in requests if r.sharing != SHARING_COOPERATIVE]
        sharers = [r for r in requests if r.sharing == SHARING_COOPERATIVE]
        needed = sum(r.device_count for r in exclusive)
        assignments : dict[str, tuple[DeviceIdentity, ...]] = {}
        free = list(pool)
        if needed > len(pool):
            reasons.append(f'exclusive requests need {needed} distinct device(s); {len(pool)} visible '
                           f'({", ".join(device_key(d) for d in pool) or "none"}). Reduce replicas or gpu_count, '
                           f'or opt into sharing with --gpu-policy shared')
        else:
            for request in exclusive:
                assignments[request.workload_id] = tuple(free[:request.device_count])
                free = free[request.device_count:]
        if sharers:
            if not free:
                reasons.append(f'cooperative requests ({", ".join(r.workload_id for r in sharers)}) have no device '
                               f'left after the exclusive grants')
            else:
                budgets = {device_key(d): (d.memory_bytes or 0) - snapshot.occupancy.get(device_key(d), 0) - self._headroom
                           for d in free}
                for request in sharers:
                    peak = request.declared_peak_memory_bytes
                    if peak is None:
                        reasons.append(f'{request.workload_id}: cooperative sharing under strict policy needs a declared '
                                       f'peak memory (gpu_memory_gib on the node); none declared')
                        continue
                    if request.device_count != 1:
                        reasons.append(f'{request.workload_id}: a cooperative sharer takes one device, not '
                                       f'{request.device_count}')
                        continue
                    # First device whose remaining budget holds the peak, in pool order.
                    home = next((d for d in free if budgets[device_key(d)] >= peak), None)
                    if home is None:
                        reasons.append(f'{request.workload_id}: declared peak {peak} B does not fit any shared device '
                                       f'(remaining budgets after {self._headroom} B headroom: '
                                       + ', '.join(f'{k}={v}' for k, v in budgets.items()) + ')')
                        continue
                    budgets[device_key(home)] -= peak
                    assignments[request.workload_id] = (home,)
        if reasons:
            return Infeasible(tuple(reasons))
        return FeasiblePlan(assignments, {}, snapshot.generation, self._next_id('plan'),
                            (f'headroom {self._headroom} B per shared device',) if sharers else ())

    # -- reservation and bindings ----------------------------------------------------------

    def reserve(self, plan : FeasiblePlan, operation_id : str,
                expected_generation : str | None) -> ClaimObservation:
        '''
        Fences the plan on the host: re-reads the inventory and refuses when
        its generation differs from the one the plan was made on (a device or a
        foreign process came or went since), returning ``failed`` with the
        evidence rather than granting devices the plan never saw.
        '''
        claim_id = self._next_id('claim')
        owner = operation_id.split(':')[0]
        current = self.inventory({})
        if isinstance(current, Unknown):
            return ClaimObservation(claim_id, owner, plan.snapshot_generation, None, CLAIM_FAILED, None,
                                    {'reason': f'host unobservable at reservation: {current.reason}'})
        wanted = expected_generation or plan.snapshot_generation
        if current.value.generation != wanted:
            return ClaimObservation(claim_id, owner, wanted, current.value.generation, CLAIM_FAILED, None,
                                    {'reason': 'the host changed since the plan was made; plan again'})
        shared_devices = {device_key(d) for devices in plan.assignments.values() for d in devices
                          if sum(d in v for v in plan.assignments.values()) > 1}
        requested = self._requested.get(plan.plan_id, {})
        grants = {}
        for workload_id, devices in plan.assignments.items():
            exclusive = (self._policy == POLICY_STRICT and not any(device_key(d) in shared_devices for d in devices))
            grants[workload_id] = DeliveredGrant(workload_id, tuple(devices), exclusive,
                                                 requested.get(workload_id, len(devices)), self._policy)
        self._claims[claim_id] = _Claim(plan, wanted, owner, CLAIM_ALLOCATED, grants)
        return ClaimObservation(claim_id, owner, wanted, current.value.generation, CLAIM_ALLOCATED,
                                tuple(d for devices in plan.assignments.values() for d in devices),
                                {'policy': self._policy, 'notes': list(plan.notes)})

    def grant(self, claim_id : str, workload_id : str) -> DeliveredGrant:
        '''The grant a workload received under this claim (what ``bindings`` serialises).'''
        return self._claims[claim_id].grants[workload_id]

    def bindings(self, claim_id : str, workload_id : str) -> WorkloadBindings:
        grant = self.grant(claim_id, workload_id)
        env = {'CUDA_VISIBLE_DEVICES': ','.join(device_key(d) for d in grant.devices),
               'VF_GPU_COUNT': str(len(grant.devices)),
               GRANT_ENV: json.dumps(grant.to_dict(), separators = (',', ':'))}
        return WorkloadBindings(env, {}, {}, [], {})

    def observe(self, claim_id : str) -> Observation[ClaimObservation]:
        '''Ready when every granted device is still enumerated by the host; Unknown when the host cannot be read.'''
        claim = self._claims.get(claim_id)
        if claim is None:
            return unknown('missing', f'no claim {claim_id}')
        current = self._host_reader()
        if isinstance(current, Unknown):
            return current
        present = {device_key(d) for d in current.value}
        granted = tuple(d for g in claim.grants.values() for d in g.devices)
        missing = [device_key(d) for d in granted if device_key(d) not in present]
        status = CLAIM_FAILED if missing else CLAIM_READY
        claim.status = status
        return known(ClaimObservation(claim_id, claim.owner, claim.generation, claim.generation, status, granted,
                                      {'missing': missing}))

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
        claim = self._claims.get(claim_id)
        if claim is None:
            return ReleaseObservation(claim_id, RELEASE_RELEASED, (), 'already released')
        if claim.generation != expected_generation:
            return ReleaseObservation(claim_id, RELEASE_STALE, tuple(claim.grants), 'stale generation: nothing released')
        if keep_workloads:
            return ReleaseObservation(claim_id, RELEASE_RELEASED, tuple(claim.grants), 'workloads retained')
        del self._claims[claim_id]
        return ReleaseObservation(claim_id, RELEASE_RELEASED, ())

    def _next_id(self, kind : str) -> str:
        self._counter += 1
        return f'{kind}-{self._counter}'


def grant_from_env(environ : Mapping[str, str] = os.environ) -> DeliveredGrant | None:
    '''The ``DeliveredGrant`` a worker was launched with, or None outside a local grant.'''
    raw = environ.get(GRANT_ENV)
    if not raw:
        return None
    return DeliveredGrant.from_dict(json.loads(raw))
