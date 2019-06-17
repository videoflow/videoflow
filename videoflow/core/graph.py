from ..utils.graph import has_cycle, topological_sort
from .node import ProducerNode

import logging
logger = logging.getLogger(__package__)

class GraphEngine:
    def __init__(self, producers, consumers):
        if len(producers) != 1:
            raise AttributeError('Only support flows with 1 producer for now.')
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
        
        #3. TODO: Check that all producers' results are
        #being read by a consumer.
    
    def topological_sort(self):
        return list(self._tsort)
        