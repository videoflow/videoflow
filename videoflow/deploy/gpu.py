'''
GPU allocation strategies: how a GPU node's pods claim devices, what to preflight
before deploying them, and any cluster state a strategy needs to set up for a run
and restore afterwards.

Two strategies ship. ``exclusive`` claims whole devices through an integer
extended resource, so the scheduler accounts for them and a flow that outgrows
the cluster stays Pending rather than thrashing. ``shared`` emits no resource
limit at all: pods land on the pool by selector alone and share the physical
devices through the NVIDIA runtime — dev-box semantics, no accounting, no memory
isolation.

The mode used to be a bare string branched on in four places (pod resources,
manifest validation, preflight, the CLI's choices), which is why adding a third
meant finding all four. A strategy is now one class registered here. The two
lifecycle hooks, ``prepare`` and ``cleanup``, are no-ops for both built-ins but
exist because the strategies we expect next need them: setting device-plugin
time-slicing replicas for the duration of a run and reverting after, or
partitioning GPUs from measured per-component memory demand.
'''
from __future__ import absolute_import, division, print_function

import logging
from typing import List, Optional

logger = logging.getLogger(__package__)

#: Extended resource used when neither the node nor the deploy names one.
DEFAULT_GPU_RESOURCE = 'nvidia.com/gpu'

#: Node label a GPU pod selects on, and the taint key it tolerates.
GPU_POOL_LABEL = 'videoflow.io/gpu-pool'
GPU_TAINT_KEY = 'nvidia.com/gpu'

GPU_STRATEGY_ENTRY_POINT_GROUP = 'videoflow.gpu_strategies'

def resolve_gpu_resource(spec, default : Optional[str] = None) -> str:
    '''The extended-resource name a GPU spec requests: the node's own, else the
    deploy default (``--gpu-resource-name``), else ``nvidia.com/gpu``.'''
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

    def pod_resources(self, spec, gpu_resource_name : Optional[str] = None) -> dict:
        '''
        The container ``resources`` fragment for one GPU pod — ``{}`` for a
        strategy that requests nothing.

        - Arguments:
            - spec: the node's ``NodeSpec`` (``gpu_count``, ``gpu_resource_name``).
            - gpu_resource_name: deploy-level default extended-resource name.
        '''
        raise NotImplementedError('GpuStrategy subclass must implement pod_resources()')

    def preflight_problems(self, kubectl : str = 'kubectl', demand : Optional[dict] = None,
                        gpu_runtime_class : Optional[str] = None) -> List[str]:
        '''
        Strategy-specific preflight problems, each a string naming its fix. The
        flavor-independent checks (cluster reachable, a labeled GPU node) are run
        by ``cluster.gpu_preflight`` before this is called.

        - Arguments:
            - demand: extended-resource name -> units the flow requests, or None \
                to skip capacity comparison.
            - gpu_runtime_class: the ``--gpu-runtime-class`` value, if given.
        '''
        return []

    def prepare(self, demand : Optional[dict] = None, kubectl : str = 'kubectl') -> None:
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

    def pod_resources(self, spec, gpu_resource_name = None) -> dict:
        # nvidia.com/gpu (or a MIG profile / renamed time-sliced resource via
        # gpu_resource_name) is an integer extended resource the scheduler
        # allocates exclusively — N GPU replicas need N allocatable units, and
        # pods beyond capacity stay Pending.
        return {'limits': {resolve_gpu_resource(spec, gpu_resource_name): spec.gpu_count}}

    def preflight_problems(self, kubectl = 'kubectl', demand = None,
                        gpu_runtime_class = None) -> List[str]:
        # Function-level: cluster.py imports this module at module scope (for
        # SHARED_NEEDS_RUNTIME_CLASS and get_gpu_mode), so importing it back at
        # module scope here would be a cycle.
        from .cluster import allocatable_gpus, nvidia_runtimeclass

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
                                    f'(gpu_resource_name) but the cluster does not expose it')
            elif demand and demand[resource] > capacity:
                problems.append(
                    f'flow demands {demand[resource]} x {resource} but the cluster has only '
                    f'{capacity} allocatable — {demand[resource] - capacity} pod(s) will stay '
                    f'Pending and the flow will stall. Reduce GPU nodes/replicas, enable '
                    f'device-plugin time-slicing, or deploy with --gpu-mode shared '
                    f'(dev clusters; see the GPU sharing docs)')
        if not gpu_runtime_class:
            nvidia_rc = nvidia_runtimeclass(kubectl)
            if nvidia_rc:
                problems.append(f'a {nvidia_rc!r} RuntimeClass exists but no --gpu-runtime-class was '
                                f'given — if the NVIDIA runtime is not this node\'s containerd default, '
                                f'GPU pods will start with no device. Fix: deploy with '
                                f'--gpu-runtime-class {nvidia_rc}')
        return problems

#: The one preflight problem that is fatal in shared mode (cli hard-errors on it):
#: shared pods carry no GPU resource limit, so the RuntimeClass is their only path
#: to the device. A shared constant so the check never depends on message prose.
SHARED_NEEDS_RUNTIME_CLASS = '--gpu-mode shared without --gpu-runtime-class'

class SharedGpu(GpuStrategy):
    '''No resource limit: pods co-schedule on the pool and share devices via the NVIDIA runtime.'''
    name = 'shared'

    def pod_resources(self, spec, gpu_resource_name = None) -> dict:
        # No limit at all: every GPU pod is scheduled onto the pool by the selector
        # alone and shares the physical devices through the NVIDIA runtime (the CUDA
        # base images set NVIDIA_VISIBLE_DEVICES=all) — LocalProcessEngine semantics
        # on Kubernetes, bounded only by VRAM. Dev-box tool: no scheduler accounting,
        # no memory isolation, and on a multi-GPU node every shared pod sees every
        # device.
        return {}

    def preflight_problems(self, kubectl = 'kubectl', demand = None,
                        gpu_runtime_class = None) -> List[str]:
        # Function-level to break the same cycle as in ExclusiveGpu above.
        from .cluster import nvidia_runtimeclass

        # Capacity math and the device plugin are irrelevant without a resource
        # limit; the RuntimeClass is escalated instead, because in shared mode it is
        # the only thing granting device access at all.
        if gpu_runtime_class:
            return []
        nvidia_rc = nvidia_runtimeclass(kubectl)
        if not nvidia_rc:
            return []
        return [f'{SHARED_NEEDS_RUNTIME_CLASS}: shared pods carry no '
                f'GPU resource limit, so the RuntimeClass is the only thing that '
                f'injects the device — without it every GPU pod runs device-less. '
                f'Fix: deploy with --gpu-runtime-class {nvidia_rc}']

# -- registry --------------------------------------------------------------

_GPU_STRATEGIES : dict = {}

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

def registered_gpu_modes() -> list:
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
        # May belong to an installed-but-unimported package.
        from ..utils.plugins import load_plugin_group
        load_plugin_group(GPU_STRATEGY_ENTRY_POINT_GROUP)
        strategy = _GPU_STRATEGIES.get(name)
    if strategy is None:
        raise ValueError(
            f'gpu_mode must be one of {tuple(registered_gpu_modes())}, got {name!r}. '
            f'Register another with videoflow.deploy.gpu.register_gpu_mode.')
    return strategy

register_gpu_mode(ExclusiveGpu())
register_gpu_mode(SharedGpu())
