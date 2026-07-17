'''
Turns a ``videoflow.core.flow.Flow`` (a built, validated graph) into a list of
per-node ``NodeSpec``s: everything an execution engine needs to launch one worker
per node, without any of the live ``Node`` objects. A ``NodeSpec`` is fully
JSON-serializable, which is what lets it cross into a separate process or a
Kubernetes pod as environment variables / a ConfigMap.
'''
from __future__ import absolute_import, division, print_function

from .core.node import ConsumerNode, ProcessorNode, ProducerNode

NODE_KIND_PRODUCER = 'producer'
NODE_KIND_PROCESSOR = 'processor'
NODE_KIND_CONSUMER = 'consumer'

def _node_kind(node) -> str:
    if isinstance(node, ProducerNode):
        return NODE_KIND_PRODUCER
    if isinstance(node, ProcessorNode):
        return NODE_KIND_PROCESSOR
    if isinstance(node, ConsumerNode):
        return NODE_KIND_CONSUMER
    raise ValueError(f'{node} is not a Producer/Processor/Consumer node')

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
        - is_finite: for producers, whether ``next()`` self-terminates.
        - image: the container image ref declared on the node, or None (the \
            deploy-time default/override supplies it — see ``videoflow.images``).
    '''
    def __init__(self, name, node_class, params, parents, kind, has_children,
                nb_tasks, device_type, is_finite, image = None,
                partition_by = None, join_policy = None) -> None:
        self.name = name
        self.node_class = node_class
        self.params = params
        self.parents = parents
        self.kind = kind
        self.has_children = has_children
        self.nb_tasks = nb_tasks
        self.device_type = device_type
        self.is_finite = is_finite
        self.image = image
        # Routing-relevant fields lifted out of params so engines/manifests see
        # them without parsing constructor kwargs.
        self.partition_by = partition_by
        self.join_policy = join_policy  # dict or None

    def to_dict(self) -> dict:
        return {
            'name': self.name,
            'node_class': self.node_class,
            'params': self.params,
            'parents': self.parents,
            'kind': self.kind,
            'has_children': self.has_children,
            'nb_tasks': self.nb_tasks,
            'device_type': self.device_type,
            'is_finite': self.is_finite,
            'image': self.image,
            'partition_by': self.partition_by,
            'join_policy': self.join_policy,
        }

    @classmethod
    def from_dict(cls, d) -> "NodeSpec":
        return cls(
            name = d['name'], node_class = d['node_class'], params = d['params'],
            parents = d['parents'], kind = d['kind'], has_children = d['has_children'],
            nb_tasks = d['nb_tasks'], device_type = d['device_type'],
            is_finite = d['is_finite'], image = d.get('image'),
            partition_by = d.get('partition_by'), join_policy = d.get('join_policy'),
        )

def specs_from_tasks_data(tasks_data) -> list:
    '''
    Converts ``build_tasks_data`` output — tuples of
    ``(node, parent_names, is_last)`` — into a list of serializable ``NodeSpec``.
    '''
    specs = []
    for node, parent_names, is_last in tasks_data:
        kind = _node_kind(node)
        node_class = f'{type(node).__module__}.{type(node).__name__}'
        nb_tasks = node.nb_tasks if isinstance(node, ProcessorNode) else 1
        device_type = node.device_type if isinstance(node, ProcessorNode) else 'cpu'
        is_finite = node.is_finite if isinstance(node, ProducerNode) else True
        partition_by = getattr(node, 'partition_by', None)
        join_policy = node._join_policy if hasattr(node, '_join_policy') else None
        specs.append(NodeSpec(
            name = node.name,
            node_class = node_class,
            params = node.get_params(),
            parents = parent_names,
            kind = kind,
            has_children = not is_last,
            nb_tasks = nb_tasks,
            device_type = device_type,
            is_finite = is_finite,
            image = getattr(node, 'image', None),
            partition_by = partition_by,
            join_policy = join_policy,
        ))
    return specs

def compile_flow(flow) -> list:
    '''
    - Arguments:
        - flow: a built ``videoflow.core.flow.Flow`` (do NOT call ``.run()`` on it first).

    - Returns:
        - list of ``NodeSpec``, one per node in the flow's topological sort.
    '''
    return specs_from_tasks_data(flow.tasks_data())
