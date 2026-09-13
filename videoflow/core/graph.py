'''
Validation and topological sorting of a computation graph.

Validation collects **every** problem in one pass and reports them together,
rather than raising on the first. The difference is not cosmetic: a graph is
built on one machine and run on many, so a mistake here is cheap to find now and
expensive to find later — and making the author fix one problem, re-run, and
discover the next is the slowest possible way to spend that cheapness. This is
the contract a compiler has and an interpreter does not.
'''
import logging
from typing import List, Optional

from ..utils.graph import has_cycle, topological_sort
from .errors import (
    SEVERITY_ERROR,
    SEVERITY_WARNING,
    Diagnostic,
    raise_for_diagnostics,
)
from .node import ConsumerNode, Node, ProcessorNode, ProducerNode

logger = logging.getLogger(__package__)

def _check_producers(producers : List[ProducerNode]) -> List[Diagnostic]:
    '''Every declared root really is a ``ProducerNode``.'''
    return [
        Diagnostic(SEVERITY_ERROR, 'VF_GRAPH_NOT_A_PRODUCER', getattr(p, 'name', None),
                f'{p} is a root of the graph but is not a ProducerNode.',
                'Every parentless node must be a ProducerNode; give it a parent, or '
                'make it one.')
        for p in producers if not isinstance(p, ProducerNode)
    ]

def _check_unique_names(nodes : List[Node]) -> List[Diagnostic]:
    '''
    Names are identity everywhere outside the building process — broker subjects,
    Kubernetes resource names, logs — so a duplicate is not a style problem, it is
    two nodes claiming one mailbox.
    '''
    seen, duplicates = set(), []
    for node in nodes:
        if node.name in seen and node.name not in duplicates:
            duplicates.append(node.name)
        seen.add(node.name)
    return [
        Diagnostic(SEVERITY_ERROR, 'VF_GRAPH_DUPLICATE_NAME', name,
                f'Two or more nodes are named {name!r}.',
                'Node names must be unique within a flow: pass an explicit, unique '
                'name= to each affected node.')
        for name in duplicates
    ]

def _check_name_collisions(nodes : List[Node]) -> List[Diagnostic]:
    '''
    Distinct names that become one physical name once encoded for the broker
    (``a.b`` and ``a_b`` both sanitize to ``a_b``) or for Kubernetes (``Node`` and
    ``node``; two long names truncated to 63 characters). The same failure as a
    duplicate name, one encoding later, so it is rejected here rather than
    discovered as cross-routed messages on a cluster.
    '''
    # Function-level: keeps core free of a module-scope dependency on the backends
    # package, which itself imports the compiler (layering, not a real cycle).
    from ..backends.identity import node_name_collisions
    names = [node.name for node in nodes]
    return [
        Diagnostic(SEVERITY_ERROR, 'VF_GRAPH_NAME_COLLISION', c.identities[0].parts[0],
                f'Node names {[i.parts[0] for i in c.identities]} all encode to the '
                f'{c.identities[0].kind} name {c.physical!r}.',
                'Rename one of them so the encoded names differ (avoid names that differ '
                'only by punctuation, case, or beyond 63 characters).')
        for c in node_name_collisions(names)
    ]

def _check_consumers_reachable(consumers : List[ConsumerNode],
                            tsort : List[Node]) -> List[Diagnostic]:
    reachable = set(tsort)
    return [
        Diagnostic(SEVERITY_ERROR, 'VF_GRAPH_UNREACHABLE_CONSUMER', c.name,
                f'{c} is not a descendant of any producer, so nothing would ever '
                f'reach it.',
                'Wire it to the graph with consumer(parent), or remove it.')
        for c in consumers if c not in reachable
    ]

def _check_replicated_joins(tsort : List[Node]) -> List[Diagnostic]:
    '''
    A replicated multi-parent (join) node must partition its input, otherwise the
    two halves of one logical event land on different replicas and neither can
    assemble the join. With ``partition_by='trace_id'`` both halves hash to the
    same replica, so joins can safely scale out.
    '''
    out = []
    for node in tsort:
        if not isinstance(node, ProcessorNode):
            continue
        parents = node.parents or []
        if len(parents) > 1 and node.nb_tasks > 1 and not node.partition_by:
            out.append(Diagnostic(
                SEVERITY_ERROR, 'VF_GRAPH_UNPARTITIONED_JOIN', node.name,
                f'{node.name} joins {len(parents)} parents with '
                f'nb_tasks={node.nb_tasks} but no partition_by, so replicas would '
                f'receive the two halves of a join on different workers.',
                "Set partition_by='trace_id' (recommended for joins), or nb_tasks=1."))
    return out

def _check_outputs_are_read(tsort : List[Node]) -> List[Diagnostic]:
    '''
    A node whose output nothing consumes is almost always a wiring mistake — the
    work still runs, and the result is discarded. A warning rather than an error:
    a processor used purely for its side effects is unusual but legitimate.
    '''
    return [
        Diagnostic(SEVERITY_WARNING, 'VF_GRAPH_OUTPUT_UNREAD', node.name,
                f'Nothing consumes the output of {node.name}, so its results are '
                f'computed and discarded.',
                'Wire a child to it, or drop it from the graph.')
        for node in tsort
        if not isinstance(node, ConsumerNode) and not node.children
    ]

def validate(producers : List[ProducerNode], consumers : List[ConsumerNode],
            tsort : Optional[List[Node]] = None) -> List[Diagnostic]:
    '''
    Every problem with a graph, in one pass.

    - Arguments:
        - producers: the roots.
        - consumers: the leaves.
        - tsort: the topological sort, when the caller already has one. Omit and \
            it is computed — but only if the graph is acyclic, since a cycle \
            makes the sort meaningless.

    - Returns:
        - diagnostics in a stable order: structural errors first (they make the \
            later checks unreliable), then per-node ones, then warnings. Empty \
            means the graph is sound.
    '''
    diagnostics = _check_producers(producers)
    if diagnostics:
        # Everything below walks the graph from these roots, so there is nothing
        # trustworthy to say until they are real producers.
        return diagnostics

    if has_cycle(producers):
        return [Diagnostic(
            SEVERITY_ERROR, 'VF_GRAPH_CYCLE', None,
            'The computation graph has a cycle.',
            'A flow must be a DAG: find the node that is (indirectly) its own '
            'parent and break the loop.')]

    if tsort is None:
        tsort = topological_sort(producers)

    diagnostics += _check_unique_names(tsort)
    diagnostics += _check_name_collisions(tsort)
    diagnostics += _check_consumers_reachable(consumers, tsort)
    diagnostics += _check_replicated_joins(tsort)
    diagnostics += _check_outputs_are_read(tsort)
    return diagnostics

class GraphEngine:
    '''
    Validates and topologically sorts a computation graph.

    - Arguments:
        - producers: list of ``ProducerNode`` instances that are the roots of the graph. \
            Any number of producers is supported (a flow may ingest from several \
            independent sources, e.g. multiple cameras, and fan them into shared \
            downstream processors).
        - consumers: list of ``ConsumerNode`` instances that are the leaves of the graph.

    - Raises:
        - ``videoflow.core.errors.GraphError`` listing **every** problem found: a \
            root that is not a ``ProducerNode``, a cycle, a consumer unreachable \
            from any producer, duplicate node names, or a replicated join with no \
            partition key. Warnings are logged rather than raised.
    '''
    def __init__(self, producers : List[ProducerNode], consumers : List[ConsumerNode]) -> None:
        self._producers = producers
        self._consumers = consumers

        diagnostics = validate(producers, consumers)
        for diagnostic in diagnostics:
            if diagnostic.severity == SEVERITY_WARNING:
                logger.warning(diagnostic.render())
        raise_for_diagnostics(diagnostics)

        self._tsort = topological_sort(self._producers)
        logger.debug("Topological sort: {}".format(self._tsort))

    def topological_sort(self) -> list:
        return list(self._tsort)
