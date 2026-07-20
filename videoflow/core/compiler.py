'''
Turns a ``videoflow.core.flow.Flow`` (a built, validated graph) into a list of
per-node ``NodeSpec``s: everything an execution engine needs to launch one worker
per node, without any of the live ``Node`` objects. A ``NodeSpec`` is fully
JSON-serializable, which is what lets it cross into a separate process or a
Kubernetes pod as environment variables / a ConfigMap.
'''
from __future__ import absolute_import, division, print_function

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .flow import Flow
from .node import ConsumerNode, Node, ProcessorNode, ProducerNode
from .remote import RemoteNodeMixin

NODE_KIND_PRODUCER = 'producer'
NODE_KIND_PROCESSOR = 'processor'
NODE_KIND_CONSUMER = 'consumer'

def _node_kind(node : Node) -> str:
    if isinstance(node, ProducerNode):
        return NODE_KIND_PRODUCER
    if isinstance(node, ProcessorNode):
        return NODE_KIND_PROCESSOR
    if isinstance(node, ConsumerNode):
        return NODE_KIND_CONSUMER
    raise ValueError(f'{node} is not a Producer/Processor/Consumer node')

@dataclass
class NodeSpec:
    '''
    A flat, serializable description of one node's deployment.

    - Attributes:
        - name: node's stable name (unique in the flow).
        - node_class: fully-qualified import path, e.g. ``videoflow.processors.basic.IdentityProcessor``.
        - params: dict from ``node.get_params()`` — the kwargs to reconstruct it.
        - parents: list of parent node names, in ``process()`` positional order.
        - kind: one of producer/processor/consumer.
        - has_children: whether anything downstream consumes this node's output.
        - nb_tasks: desired replica count (processors only; 1 otherwise).
        - device_type: 'cpu' or 'gpu' (processors only).
        - gpu_count: GPUs each replica requests (GPU processors only; default 1).
        - gpu_resource_name: extended-resource name each replica requests, or None \
            to use the deploy-time default (``nvidia.com/gpu``).
        - is_finite: for producers, whether ``next()`` self-terminates.
        - image: the container image ref declared on the node, or None (the \
            deploy-time default/override supplies it — see ``videoflow.deploy.images``).

    The field order below *is* the constructor signature — callers pass these
    positionally (``NodeSpec('n', 'pkg.Cls', {}, [], 'processor', ...)``), so
    reordering or inserting a field is a breaking change. Not ``frozen``: a spec is
    a plain mutable record, and ``to_dict``/``from_dict`` stay explicit because
    they are the ``VF_FLOW_SPECS_JSON`` serialization boundary — ``asdict()``
    would deep-copy and rewrite the nested ``params``/``descriptor`` values.
    '''
    name : str
    # ``node_class`` is the Python import path for a native node, or None for a
    # remote (language-agnostic) component — those are identified by
    # ``component_ref`` and run their own image's entrypoint instead of the
    # Python worker. Exactly one of node_class / component_ref is set.
    node_class : Optional[str]
    params : Dict[str, Any]
    parents : List[str]
    kind : str
    has_children : bool
    nb_tasks : int
    device_type : str
    is_finite : bool
    image : Optional[str] = None
    # Routing-relevant fields lifted out of params so engines/manifests see
    # them without parsing constructor kwargs.
    partition_by : Optional[str] = None
    join_policy : Optional[Dict[str, Any]] = None   # dict or None
    # Remote-component fields (None for native Python nodes).
    component_ref : Optional[str] = None
    descriptor : Optional[Dict[str, Any]] = None    # the component descriptor as a dict
    command : Optional[List[str]] = None            # container command override, or None
    protocol_version : Optional[int] = None
    # GPU scheduling knobs, meaningful only when device_type == 'gpu'.
    gpu_count : int = 1
    gpu_resource_name : Optional[str] = None

    @property
    def is_remote(self) -> bool:
        '''Loaded from a component descriptor (Python or native), vs a native Python graph node.'''
        return self.component_ref is not None

    @property
    def is_native(self) -> bool:
        '''A non-Python component: runs its own image entrypoint (no node_class) and must speak the protobuf wire.'''
        return self.component_ref is not None and self.node_class is None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'node_class': self.node_class,
            'params': self.params,
            'parents': self.parents,
            'kind': self.kind,
            'has_children': self.has_children,
            'nb_tasks': self.nb_tasks,
            'device_type': self.device_type,
            'gpu_count': self.gpu_count,
            'gpu_resource_name': self.gpu_resource_name,
            'is_finite': self.is_finite,
            'image': self.image,
            'partition_by': self.partition_by,
            'join_policy': self.join_policy,
            'component_ref': self.component_ref,
            'descriptor': self.descriptor,
            'command': self.command,
            'protocol_version': self.protocol_version,
        }

    @classmethod
    def from_dict(cls, d : Dict[str, Any]) -> "NodeSpec":
        return cls(
            name = d['name'], node_class = d['node_class'], params = d['params'],
            parents = d['parents'], kind = d['kind'], has_children = d['has_children'],
            nb_tasks = d['nb_tasks'], device_type = d['device_type'],
            is_finite = d['is_finite'], image = d.get('image'),
            partition_by = d.get('partition_by'), join_policy = d.get('join_policy'),
            component_ref = d.get('component_ref'), descriptor = d.get('descriptor'),
            command = d.get('command'), protocol_version = d.get('protocol_version'),
            gpu_count = d.get('gpu_count', 1), gpu_resource_name = d.get('gpu_resource_name'),
        )

def specs_from_tasks_data(tasks_data : List[tuple]) -> List[NodeSpec]:
    '''
    Converts ``build_tasks_data`` output — tuples of
    ``(node, parent_names, is_last)`` — into a list of serializable ``NodeSpec``.
    '''
    specs : List[NodeSpec] = []
    for node, parent_names, is_last in tasks_data:
        kind = _node_kind(node)
        nb_tasks = node.nb_tasks if isinstance(node, ProcessorNode) else 1
        device_type = node.device_type if isinstance(node, ProcessorNode) else 'cpu'
        gpu_count = node.gpu_count if isinstance(node, ProcessorNode) else 1
        gpu_resource_name = node.gpu_resource_name if isinstance(node, ProcessorNode) else None
        is_finite = node.is_finite if isinstance(node, ProducerNode) else True
        # partition_by and _join_policy live on ProcessorNode/ConsumerNode; a producer
        # has neither. isinstance (not getattr) so the checker verifies the families.
        joinable = isinstance(node, (ProcessorNode, ConsumerNode))
        partition_by = node.partition_by if joinable else None
        join_policy = node._join_policy if joinable else None
        node_class: Optional[str]
        component_ref: Optional[str]
        descriptor: Optional[Dict[str, Any]]
        command: Optional[List[str]]
        protocol_version: Optional[int]
        if isinstance(node, RemoteNodeMixin):
            # A component loaded from a descriptor. A *Python* component still names a
            # class the worker imports (node_class = its pythonClass); a *native*
            # component has no class and runs its own image entrypoint/command.
            _validate_remote_node(node, parent_names)
            node_class = node.descriptor.python_class
            component_ref = node.component_ref
            descriptor = node.descriptor._raw
            command = node.component_command
            protocol_version = node.descriptor.protocol
        else:
            node_class = f'{type(node).__module__}.{type(node).__name__}'
            component_ref = descriptor = command = protocol_version = None
        specs.append(NodeSpec(
            name = node.name,
            node_class = node_class,
            params = node.get_params(),
            parents = parent_names,
            kind = kind,
            has_children = not is_last,
            nb_tasks = nb_tasks,
            device_type = device_type,
            gpu_count = gpu_count,
            gpu_resource_name = gpu_resource_name,
            is_finite = is_finite,
            image = node.image,
            partition_by = partition_by,
            join_policy = join_policy,
            component_ref = component_ref,
            descriptor = descriptor,
            command = command,
            protocol_version = protocol_version,
        ))
    return specs

def _validate_remote_node(node : RemoteNodeMixin, parent_names : List[str]) -> None:
    '''
    Parent-aware validation of a remote component now that its wired parents are
    known: a collect-join delivers a *list* on the collected parent's position, so
    that position's descriptor input must declare it accepts collected inputs
    (complements the parent-independent quorum check in ``core.remote.component``).
    '''
    desc = node.descriptor
    # Only ProcessorNode/ConsumerNode carry a join policy — a RemoteProducer has none.
    policy = node._join_policy if isinstance(node, (ProcessorNode, ConsumerNode)) else None
    if not policy or not desc.inputs:
        return
    collect = policy.get('collect') if isinstance(policy, dict) else None
    if not collect:
        return
    for parent_name in collect:
        if parent_name in parent_names:
            idx = parent_names.index(parent_name)
            if not desc.input_accepts(idx, 'collected'):
                raise ValueError(
                    f"component '{desc.name}': join policy collects parent '{parent_name}' "
                    f"(input #{idx}) as a list, but that input does not declare "
                    "accepts.collected. Mark it on the component, or drop it from collect.")

def has_remote_components(specs : List[NodeSpec]) -> bool:
    '''Whether any node came from a component descriptor (Python or native).'''
    return any(s.is_remote for s in specs)

def has_native_components(specs : List[NodeSpec]) -> bool:
    '''Whether any node is a native (non-Python) component — the ones that force the protobuf wire.'''
    return any(s.is_native for s in specs)

def validate_wire_compatibility(specs : List[NodeSpec], envelope_version : Optional[int]) -> None:
    '''
    The wire is the single language-neutral protobuf envelope (version 4) for every
    flow. Reject an explicit pin to any other version at compile/deploy time so the
    failure is actionable here rather than a worker refusing to start. (PROTOCOL.md §4.)
    '''
    if envelope_version is not None and envelope_version != 4:
        native = [s.name for s in specs if s.is_native]
        hint = f' Native components {native} require it.' if native else ''
        raise ValueError(
            f'Envelope version {envelope_version} is not supported; the only wire is the '
            f'protobuf v4 envelope.{hint} Remove the version pin or set VF_ENVELOPE_VERSION=4.')

def compile_flow(flow : Flow, envelope_version : Optional[int] = None) -> List[NodeSpec]:
    '''
    - Arguments:
        - flow: a built ``videoflow.core.flow.Flow`` (do NOT call ``.run()`` on it first).
        - envelope_version: the wire version this flow will deploy with. The only \
            supported version is ``4`` (protobuf); an explicit incompatible pin is \
            rejected. Defaults to the ambient ``DEFAULT_ENVELOPE_VERSION``.

    - Returns:
        - list of ``NodeSpec``, one per node in the flow's topological sort.
    '''
    specs = specs_from_tasks_data(flow.tasks_data())
    validate_wire_compatibility(specs, envelope_version)
    return specs
