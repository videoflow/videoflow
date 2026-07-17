'''
Remote (language-agnostic) components in a Python-authored graph.

A ``component(ref, params=...)`` node stands in for a component that runs as its own
container image and is authored in any language. It behaves like a normal
Producer/Processor/Consumer node for graph-building (wiring, validation, the
compiler, manifests), but it carries no Python implementation — its
``next``/``process``/``consume`` run out-of-process in the vendor image, driven by
that image's own SDK speaking the wire protocol (spec/PROTOCOL.md). The Python
process only ever builds and compiles the graph; it never imports the component.

The three concrete kinds subclass the existing node base classes so every
``isinstance`` dispatch in ``graph.py``/``compiler.py``/``flow.py`` keeps working
unchanged; ``RemoteNodeMixin`` marks them for the compiler, which records a
``component_ref`` + descriptor instead of a Python ``node_class``.
'''
from __future__ import absolute_import, division, print_function

from typing import Optional

from ..component import ComponentDescriptor, load_descriptor
from .node import ConsumerNode, ProcessorNode, ProducerNode, _slugify

# Per-base counter so an unnamed remote node gets a readable, component-derived
# default name (e.g. 'sort-tracker-1') instead of the Python class name; final
# uniqueness within a flow is still enforced by GraphEngine at build time.
_remote_name_counters : dict = {}

def _default_remote_name(descriptor : ComponentDescriptor) -> str:
    base = _slugify(descriptor.name.split('/')[-1]) or 'component'
    _remote_name_counters[base] = _remote_name_counters.get(base, 0) + 1
    return f'{base}-{_remote_name_counters[base]}'

class RemoteNodeMixin:
    '''Shared state/identity for the three remote node kinds.'''
    _component_ref : str
    _descriptor : ComponentDescriptor
    _component_params : dict

    @property
    def component_ref(self) -> str:
        return self._component_ref

    @property
    def descriptor(self) -> ComponentDescriptor:
        return self._descriptor

    @property
    def component_command(self) -> Optional[list]:
        return self._descriptor.command

    @property
    def local_command(self) -> Optional[list]:
        return self._descriptor.local_command

    def get_params(self) -> dict:
        '''
        The component's own params, delivered via VF_NODE_PARAMS_JSON. For a native
        component these are the only params (routing settings reach it via env); for
        a Python component, the reconstructing worker calls ``pythonClass(**params)``,
        so node-level settings the class needs (nb_tasks/device_type) are merged in —
        matching what a normal node's ``get_params()`` would carry. Every videoflow
        node constructor accepts these via ``**kwargs``.
        '''
        params = dict(self._component_params)
        if not self._descriptor.is_native:
            params.update(self._python_node_params())
        return params

    def _python_node_params(self) -> dict:
        '''Node-level params to merge for a Python component; overridden per role.'''
        return {}

class RemoteProducer(RemoteNodeMixin, ProducerNode):
    def __init__(self, component_ref, descriptor, params, is_finite = True,
                name = None, image = None) -> None:
        self._component_ref = component_ref
        self._descriptor = descriptor
        self._component_params = params
        super().__init__(is_finite = is_finite, name = name, image = image)

    def next(self):
        raise NotImplementedError('RemoteProducer runs out-of-process in its own image; '
                                'next() is never called in the Python process.')

class RemoteProcessor(RemoteNodeMixin, ProcessorNode):
    def __init__(self, component_ref, descriptor, params, nb_tasks = 1, device_type = 'cpu',
                partition_by = None, join_policy = None, name = None, image = None) -> None:
        self._component_ref = component_ref
        self._descriptor = descriptor
        self._component_params = params
        super().__init__(nb_tasks = nb_tasks, device_type = device_type, name = name,
                        partition_by = partition_by, join_policy = join_policy, image = image)

    def _python_node_params(self) -> dict:
        # A Python processor is reconstructed as pythonClass(**params); it needs its
        # device/replica settings (e.g. to place a model on CPU vs GPU in open()).
        return {'nb_tasks': self._nb_tasks, 'device_type': self._device_type}

    def process(self, *inputs):
        raise NotImplementedError('RemoteProcessor runs out-of-process in its own image; '
                                'process() is never called in the Python process.')

class RemoteConsumer(RemoteNodeMixin, ConsumerNode):
    def __init__(self, component_ref, descriptor, params, metadata = False, idempotent = False,
                join_policy = None, name = None, image = None) -> None:
        self._component_ref = component_ref
        self._descriptor = descriptor
        self._component_params = params
        super().__init__(metadata = metadata, name = name, join_policy = join_policy,
                        idempotent = idempotent, image = image)

    def consume(self, *inputs):
        raise NotImplementedError('RemoteConsumer runs out-of-process in its own image; '
                                'consume() is never called in the Python process.')

def _resolve_join_policy(join_policy):
    from .policies import JoinPolicy
    if isinstance(join_policy, JoinPolicy):
        return join_policy
    if isinstance(join_policy, dict):
        return JoinPolicy.from_dict(join_policy)
    return None

def component(ref, params = None, name = None, nb_tasks = 1, device_type = 'cpu',
            partition_by = None, join_policy = None, image = None, is_finite = None,
            metadata = False, idempotent = False):
    '''
    Create a graph node backed by a language-agnostic component described by ``ref``
    (a path to a ``component.yaml`` or, later, an ``oci://`` ref). Returns a
    Producer/Processor/Consumer node per the descriptor's ``role``, so it wires into
    a Python graph exactly like a native node::

        from videoflow.core import Flow, component
        tracker = component('components/sort', params={'iou_threshold': 0.3})(detector)

    Validation happens here, at graph-build time, so a misconfiguration is a build
    error in the authoring script rather than a crash inside the vendor container:
    params are checked against the descriptor's JSON Schema, ``device_type`` against
    the declared devices, the image against what the descriptor provides, and the
    join policy against what the component says it accepts.

    - Arguments:
        - ref: descriptor reference (local path now; ``oci://`` in Phase 6), or an \
            already-loaded ``ComponentDescriptor``.
        - params: component params dict (validated + defaulted against the descriptor).
        - device_type: 'cpu' or 'gpu' (must be in the descriptor's ``device`` list).
        - image: explicit image ref override (else the descriptor's image for the device).
        - is_finite: producers only; defaults to the descriptor's ``finite``.
        - nb_tasks/partition_by/join_policy/metadata/idempotent: as on the native nodes.
    '''
    descriptor = ref if isinstance(ref, ComponentDescriptor) else load_descriptor(ref)
    component_ref = descriptor.source or (ref if isinstance(ref, str) else descriptor.name)

    if device_type not in descriptor.device:
        raise ValueError(f"component '{descriptor.name}': device_type={device_type!r} not supported; "
                        f'declared devices: {descriptor.device}')
    resolved_image = image or descriptor.image_for(device_type)
    if resolved_image is None:
        raise ValueError(f"component '{descriptor.name}': no image for device '{device_type}' "
                        f'(declared images: {sorted(descriptor.images)}). Pass image= or add it to the descriptor.')
    validated_params = descriptor.validate_params(params)
    policy = _resolve_join_policy(join_policy)

    if descriptor.singleton and nb_tasks != 1:
        raise ValueError(f"component '{descriptor.name}' is a singleton and cannot be replicated (nb_tasks={nb_tasks})")
    if partition_by and not descriptor.partitionable:
        raise ValueError(f"component '{descriptor.name}' is not partitionable but partition_by={partition_by!r} was set")
    # Parent-independent join capability check: a quorum policy delivers None for a
    # missing parent, so every input must tolerate it. (The parent-specific collect
    # check runs in the compiler, where wired parents are known.)
    if policy is not None and policy.quorum is not None and descriptor.inputs and not descriptor.all_inputs_accept('missing'):
        raise ValueError(f"component '{descriptor.name}' has inputs that do not accept missing values, "
                        "but the join policy uses quorum (which delivers None for absent parents). "
                        "Mark io.inputs[].accepts.missing on the component, or drop quorum.")

    if name is None:
        name = _default_remote_name(descriptor)

    role = descriptor.role
    if role == 'producer':
        finite = descriptor.finite if is_finite is None else is_finite
        return RemoteProducer(component_ref, descriptor, validated_params,
                            is_finite = finite, name = name, image = resolved_image)
    if role == 'processor':
        return RemoteProcessor(component_ref, descriptor, validated_params,
                            nb_tasks = nb_tasks, device_type = device_type,
                            partition_by = partition_by, join_policy = policy,
                            name = name, image = resolved_image)
    if role == 'consumer':
        return RemoteConsumer(component_ref, descriptor, validated_params,
                            metadata = metadata, idempotent = idempotent,
                            join_policy = policy, name = name, image = resolved_image)
    raise ValueError(f"component '{descriptor.name}': unknown role {role!r}")
