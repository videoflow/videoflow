'''
Negative controls for the allocation and GPU-runtime cases of plan Phase 4
(ALLOC-003/005/006/011/014/015/016/017/018/019/021/028/029/030/031/033,
RUN-028/029/030/031/032/033/039/043/044): one monkeypatch per reviewed defect,
so the paired oracle can be shown to fail against it. See ``defects.py``.
'''
from __future__ import absolute_import, division, print_function

import dataclasses
from typing import Any

import pytest

from videoflow.backends import allocation as allocation_contract
from videoflow.backends.memory.allocation import MemoryAllocationBackend
from videoflow.backends.outcomes import Unknown, known
from videoflow.deploy import allocation_dra, allocation_kubernetes, allocation_local, gpu, manifests
from videoflow.runtime import assetcheck, gpucheck
from videoflow.utils import system

# -- ALLOC-014 / RUN-044: a launcher that shares silently and reports the request as delivered ----

def wraparound_strict(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Before the fix there was no strict policy: every launch wrapped around the
    visible devices, and ``VF_GPU_COUNT`` repeated the request.'''
    monkeypatch.setattr(allocation_local.LocalAllocationBackend, '_plan_strict',
                        allocation_local.LocalAllocationBackend._plan_shared)
    real = allocation_local.DeliveredGrant

    def requested_as_delivered(workload_id : str, devices : Any, exclusive : bool, requested : int, policy : str,
                               host : str = 'observed') -> Any:
        return real(workload_id, tuple(devices), True, requested, policy, host)
    monkeypatch.setattr(allocation_local, 'DeliveredGrant', requested_as_delivered)


def discovery_failure_is_zero_gpus(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``get_number_of_gpus`` swallowed every nvidia-smi failure into 0 (F-046 lineage).'''
    real = allocation_local.host_devices_observed

    def zero_on_failure() -> Any:
        observed = real()
        return known([]) if isinstance(observed, Unknown) else observed
    monkeypatch.setattr(allocation_local, 'host_devices_observed', zero_on_failure)


# -- ALLOC-015 / RUN-039: integer-only masks --------------------------------------------------------

def integer_only_masks(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``visible_physical_gpus`` dropped every non-integer entry, so a UUID grant read as "no GPUs".'''
    def integers_only(devices : Any, mask : Any) -> list:
        cards = [d for d in devices if d.mig_uuid is None]
        if mask is None:
            return list(cards)
        return [d for d in cards if str(d.ordinal) in [e for e in mask if e.isdigit()]]
    monkeypatch.setattr(system, 'apply_mask', integers_only)
    monkeypatch.setattr(gpucheck, 'apply_mask', integers_only)
    monkeypatch.setattr(allocation_local, 'apply_mask', integers_only)


# -- ALLOC-016: sharing without a memory budget -----------------------------------------------------

def budget_blind_sharing(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Sharers were placed by wrap-around: declared peaks and headroom never entered admission.'''
    monkeypatch.setattr(allocation_local.LocalAllocationBackend, '_plan_strict',
                        allocation_local.LocalAllocationBackend._plan_shared)


# -- RUN-043: a count-only readiness check ----------------------------------------------------------

def count_only_readiness(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The worker checked ``len(granted_gpus())`` and nothing about the devices' topology.'''
    monkeypatch.setattr(gpucheck, 'peer_access_observed', lambda devices: known(True))


# -- ALLOC-003: a wait that trusts the state label alone ---------------------------------------------

def stale_success_wait(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``_wait_for_mig_state`` completed on ``state=success`` whatever geometry the label described.'''
    def label_only(kubectl : str, expected : Any, timeout_seconds : Any = None) -> dict:
        return gpu._wait_for_mig_state(kubectl, sorted(expected), timeout_seconds)
    monkeypatch.setattr(gpu, '_wait_for_geometry', label_only)
    real_observe = gpu.MixGpu.observe_geometry

    def label_only_observe(self : Any, kubectl : str, applied : Any) -> Any:
        observed = real_observe(self, kubectl, applied)
        if isinstance(observed, Unknown):
            return observed
        return known({node: ('ready' if state == 'pending' else state) for node, state in observed.value.items()})
    monkeypatch.setattr(gpu.MixGpu, 'observe_geometry', label_only_observe)


# -- ALLOC-005: a last-one-out decided from the stale read ---------------------------------------------

def stale_last_out(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The memory backend's release deciding "nobody else holds geometry" from the map it
    read before the ``restore.before`` pause, not from the map as it is when it writes.'''
    real_barrier = MemoryAllocationBackend.release
    from videoflow.backends import faults as faults_module
    real_hit = faults_module.barrier

    def deciding_on_the_read(name : str, **context : Any) -> Any:
        hit = real_hit(name, **context)
        if name == 'delete.before':
            # "I was the last one when I looked": whoever published during the pause is ignored.
            backend = context.get('backend')
            if backend is not None and set(context.get('seen', ())) <= {context.get('entry')}:
                with backend._lock:
                    backend._shared_config.clear()
                    backend._shared_tombstone = True
                    if backend._pointer_restore is not None:
                        backend._pointer = backend._pointer_restore
                        backend._pointer_restore = None
        return hit

    def release(self : Any, claim_id : str, operation_id : str, expected_generation : str,
                keep_workloads : bool = False) -> Any:
        from videoflow.backends.memory import allocation as memory_allocation
        real_module_barrier = memory_allocation.faults.barrier
        memory_allocation.faults.barrier = lambda name, **ctx: deciding_on_the_read(name, backend = self, **ctx)
        try:
            return real_barrier(self, claim_id, operation_id, expected_generation, keep_workloads)
        finally:
            memory_allocation.faults.barrier = real_module_barrier
    monkeypatch.setattr(MemoryAllocationBackend, 'release', release)


# -- ALLOC-006: a cleanup that swallows a failed policy restore ------------------------------------------

def swallowed_policy_restore(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The ClusterPolicy repoint failing used to be logged and the retirement carried on.'''
    def quiet(kubectl : str, policy_name : str, configmap_name : str) -> None:
        try:
            gpu._kubectl_run(kubectl, 'patch', 'clusterpolicies.nvidia.com', policy_name, '--type=merge',
                             '-p', f'{{"spec":{{"migManager":{{"config":{{"name":"{configmap_name}"}}}}}}}}')
        except RuntimeError:
            return None
    monkeypatch.setattr(gpu, '_point_cluster_policy_at', quiet)
    monkeypatch.setattr(gpu, '_tombstone_mig_configmap', lambda kubectl, namespace, mine: (
        gpu._kubectl_run(kubectl, 'delete', 'configmap', '-n', namespace, gpu.MIG_CONFIGMAP_NAME) or True))


# -- ALLOC-011: the strategy singleton's cached layout ---------------------------------------------------

def cached_layout_strategy(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The registered singleton kept the last successful layout: a CPU-only or failed resolve left it
    in place, and ``prepare``/``apply_plan`` used it whatever plan the caller held.'''
    real = gpu.MixGpu.apply_plan

    def apply_cached(self : Any, plan : Any, kubectl : str = 'kubectl', flow_id : Any = None) -> Any:
        return real(self, self._plan if self._plan is not None else plan, kubectl = kubectl, flow_id = flow_id)
    monkeypatch.setattr(gpu.MixGpu, 'apply_plan', apply_cached)

    def resolve_keeping_stale(self : Any, specs : Any, kubectl : str = 'kubectl', default_resource : Any = None,
                              flow_id : Any = None) -> Any:
        if not any(s.device_type == 'gpu' for s in specs):
            return specs                                   # nothing cleared
        plan = self.plan_layout(specs, kubectl = kubectl, flow_id = flow_id)   # raises: the old plan stays
        self._plan = plan
        return self.apply_plan_to_specs(plan, specs)
    monkeypatch.setattr(gpu.MixGpu, 'resolve_specs', resolve_keeping_stale)


def no_occupancy_reread(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Prepare trusted the planning-time occupancy: nothing re-read it after the claim.'''
    monkeypatch.setattr(gpu, 'gpu_units_in_use_observed', lambda kubectl = 'kubectl': known({}), raising = False)
    from videoflow.deploy import cluster
    monkeypatch.setattr(cluster, 'gpu_units_in_use_observed', lambda kubectl = 'kubectl': known({}))


# -- ALLOC-033: records overwritten by generated values --------------------------------------------------

def overwriting_restore_record(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The restore annotation was re-recorded on every prepare, so a retry recorded videoflow's own label as the original.'''
    def relabel(self : Any, kubectl : str, node : str, entry_name : str) -> None:
        import json as _json
        info = _json.loads(gpu._kubectl_run(kubectl, 'get', 'node', node, '-o', 'json'))
        previous = ((info.get('metadata') or {}).get('labels') or {}).get(gpu.MIG_CONFIG_LABEL, '')
        gpu._kubectl_run(kubectl, 'annotate', 'node', node, '--overwrite', f'{gpu.MIG_RESTORE_ANNOTATION}={previous}')
        gpu._kubectl_run(kubectl, 'label', 'node', node, '--overwrite', f'{gpu.MIG_CONFIG_LABEL}={entry_name}')
    monkeypatch.setattr(gpu.MixGpu, '_label_node_for_mig', relabel)


# -- ALLOC-017: a static slice request that repartitions ---------------------------------------------------

def repartitioning_static(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The exclusive path handed MIG sharers to the managed-MIG solver, which would carve the node.'''
    monkeypatch.setattr(allocation_kubernetes.KubernetesAllocationBackend, 'managed_mig', property(lambda self: True))


# -- ALLOC-018: DRA-owned nodes planned by the device-plugin path --------------------------------------------

def dra_blind_inventory(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The inventory never asked which nodes a DRA driver publishes.'''
    from videoflow.deploy import cluster
    monkeypatch.setattr(cluster, 'dra_owned_nodes_observed', lambda kubectl = 'kubectl': known(set()))


# -- ALLOC-019 / ALLOC-021: DRA rendering and readiness ---------------------------------------------------------

def shared_claim_for_replicas(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Independent replicas rendered against one named ResourceClaim instead of a template.'''
    real = allocation_dra.render_bindings

    def always_shared(request : Any, device_class : str, namespace : str, shared_claim : Any = None,
                      capacity : Any = None) -> Any:
        return real(request, device_class, namespace, shared_claim or f'vf-shared-{request.flow_id}', capacity)
    monkeypatch.setattr(allocation_dra, 'render_bindings', always_shared)


def allocated_is_ready(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The memory DRA model reporting a claim ready as soon as it is allocated.'''
    real = MemoryAllocationBackend._observe

    def ready_early(self : Any, claim : Any) -> Any:
        observed = real(self, claim)
        if observed.status in (allocation_contract.CLAIM_ALLOCATED, allocation_contract.CLAIM_PREPARED):
            return dataclasses.replace(observed, status = allocation_contract.CLAIM_READY)
        return observed
    monkeypatch.setattr(MemoryAllocationBackend, '_observe', ready_early)


# -- ALLOC-024: every DRA feature assumed present once the API is GA ---------------------------------------------

def ga_means_everything(monkeypatch : pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(allocation_dra.DraEnvironment, 'unavailable_reasons',
                        lambda self: {} if self.api_served else {f: 'no v1 API' for f in
                                                                  (allocation_contract.FEATURE_DYNAMIC_MIG,
                                                                   allocation_contract.FEATURE_MPS,
                                                                   allocation_contract.FEATURE_CONSUMABLE_CAPACITY)})


# -- ALLOC-028: legacy geometry hooks on DRA-owned nodes ---------------------------------------------------------

def hooks_ignore_dra_ownership(monkeypatch : pytest.MonkeyPatch) -> None:
    real = gpu._partition_inventory

    def blind(inventory : Any, flow_id : Any) -> Any:
        return real([dataclasses.replace(n, dra_owned = False) for n in inventory], flow_id)
    monkeypatch.setattr(gpu, '_partition_inventory', blind)


# -- ALLOC-029 / RUN-029: the Deployment default under a zero-spare pool ------------------------------------------

def default_rolling_update(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Deployments rendered with no strategy at all: the API default (25% surge) on a full pool.'''
    monkeypatch.setattr(manifests, 'rollout_strategy', lambda policy: {})


def surge_admitted_without_capacity(monkeypatch : pytest.MonkeyPatch) -> None:
    from videoflow.deploy import admission
    monkeypatch.setattr(admission, 'rollout_problems', lambda *args, **kwargs: [])


# -- ALLOC-030 / RUN-028: desired replicas reported as capacity -------------------------------------------------------

def desired_as_ready(monkeypatch : pytest.MonkeyPatch) -> None:
    from videoflow.deploy import admission
    monkeypatch.setattr(admission, 'replica_admission',
                        lambda desired, *args, **kwargs: admission.ReplicaAdmission(desired, desired, desired, ()))


# -- ALLOC-031 / RUN-032: constraints dropped at render ---------------------------------------------------------------

def dropped_constraints(monkeypatch : pytest.MonkeyPatch) -> None:
    real = manifests._pod_spec

    def without_pins(*args : Any, **kwargs : Any) -> Any:
        kwargs.pop('gpu_nodes', None)
        return real(*args, **kwargs)
    monkeypatch.setattr(manifests, '_pod_spec', without_pins)
    monkeypatch.setattr(allocation_contract, 'allocation_rejections', lambda requests, capabilities: [])


# -- RUN-031: host requests dropped between contract and pod ---------------------------------------------------------------

def dropped_host_resources(monkeypatch : pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(manifests, 'host_resources_for', lambda spec, resources: {})


# -- RUN-033: a hostPath taken as evidence, no digest check -----------------------------------------------------------------

def trusting_assets(monkeypatch : pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(assetcheck, 'verify_assets', lambda requirements, node_name = 'node': [])


# -- ALLOC-004: a claim written without the server-side precondition -----------------------------------

def client_side_claim(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The claim used to be ``kubectl label`` without ``--resource-version``: the no-overwrite check
    is client-side, and two deploys that both read the node unowned both write.'''
    import uuid as uuid_module

    def stamp(self : Any, kubectl : str, nodes : Any, owner : str) -> str:
        from videoflow.backends import faults as faults_module
        epoch = uuid_module.uuid4().hex[:8]
        for node in nodes:
            meta = gpu._read_node_metadata(kubectl, node)
            faults_module.barrier('owner.read.after', node = node, version = meta.get('resourceVersion'), owner = owner)
            current = (meta.get('labels') or {}).get(gpu.GPU_OWNER_LABEL)
            if current == owner:
                continue
            gpu._kubectl_run(kubectl, 'label', 'node', node, '--overwrite',
                             f'{gpu.GPU_OWNER_LABEL}={owner}', f'{gpu.GPU_OWNER_EPOCH_LABEL}={epoch}')
        return epoch
    monkeypatch.setattr(gpu.MixGpu, '_stamp_node_owners', stamp)
