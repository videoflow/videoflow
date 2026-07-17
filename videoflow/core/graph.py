from ..utils.graph import has_cycle, topological_sort
from .node import ProducerNode, ProcessorNode

import logging
logger = logging.getLogger(__package__)

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
        - ``AttributeError`` if any of ``producers`` is not a ``ProducerNode``.
        - ``ValueError`` if the graph has a cycle, if any consumer is unreachable \
            from the given producers, or if two or more nodes share the same ``name``.
    '''
    def __init__(self, producers, consumers):
        for producer in producers:
            if not isinstance(producer, ProducerNode):
                raise AttributeError('{} is not instance of ProducerNode'.format(producer))

        self._producers = producers
        self._consumers = consumers

        if has_cycle(self._producers):
            logger.error('Cycle detected in computation graph. Exiting now...')
            raise ValueError('Cycle found in graph')

        self._tsort = topological_sort(self._producers)
        logger.debug("Topological sort: {}".format(self._tsort))

        for consumer in consumers:
            if consumer not in self._tsort:
                logger.error(f'Consumer {consumer} is not descendant of any producer. Exiting now...')
                raise ValueError(f'{consumer} is not descendant of any producer')

        names = [node.name for node in self._tsort]
        seen = set()
        duplicates = set()
        for name in names:
            if name in seen:
                duplicates.add(name)
            seen.add(name)
        if duplicates:
            raise ValueError(
                'Node names must be unique within a flow. Duplicate name(s): '
                '{}. Pass an explicit, unique name= to each affected node.'.format(', '.join(sorted(duplicates)))
            )

        # A replicated multi-parent (join) node must partition its input, otherwise
        # the two halves of one logical event could land on different replicas and
        # neither could assemble the join. With partition_by='trace_id' both halves
        # hash to the same replica, so joins can safely scale out.
        for node in self._tsort:
            if (isinstance(node, ProcessorNode) and node.parents is not None
                    and len(node.parents) > 1 and node.nb_tasks > 1
                    and not node.partition_by):
                raise ValueError(
                    f'Node {node.name} joins {len(node.parents)} parents with '
                    f'nb_tasks={node.nb_tasks} but no partition_by. Replicas would '
                    'receive the two halves of a join on different workers. Set '
                    "partition_by='trace_id' (recommended for joins) or nb_tasks=1."
                )

        #3. TODO: Check that all producers' results are
        #being read by a consumer.

    def topological_sort(self):
        return list(self._tsort)
