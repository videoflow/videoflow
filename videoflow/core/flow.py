from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import logging

from .graph import GraphEngine
from .constants import BATCH, REALTIME, FLOW_TYPES, STOP_SIGNAL
from .node import Node, ProducerNode, ConsumerNode, ProcessorNode
from .task import Task, ProducerTask, ProcessorTask, ConsumerTask
from .bottlenecks import MetadataConsumer
from ..engines.realtime import RealtimeExecutionEngine
from ..engines.batch import BatchExecutionEngine

logger = logging.getLogger(__package__)

def _task_data_from_node_tsort(tsort_l):
    tasks_data = []

    for i in range(len(tsort_l)):
        node = tsort_l[i]
        if isinstance(node, ProducerNode):
            task_data = (node, i, None, i >= (len(tsort_l) - 1))
        elif isinstance(node, ProcessorNode):
            task_data = (node, i, i - 1, i >= (len(tsort_l) - 1))
        elif isinstance(node, ConsumerNode):
            task_data = (node, i, i - 1, i >= (len(tsort_l) - 1))
        else:
            raise ValueError('node is not of one of the valid types')
        tasks_data.append(task_data)
        
    return tasks_data

class Flow:
    '''
    Represents a linear flow of data from one task to another.\
    Note that a flow is created from a **directed acyclic graph** of producer, processor \
    and consumer nodes, but the flow itself is **linear**, because it is an optimized \
    `topological sort` of the directed acyclic graph.

    - Arguments:
        - producers: a list of producer nodes of type ``videoflow.core.node.ProducerNode``.
        - consumers: a list of consumer nodes of type ``videoflow.core.node.ConsumerNode``.
        - flow_type: one of 'realtime' or 'batch'
    '''
    def __init__(self, producers, consumers, flow_type = REALTIME):
        self._graph_engine = GraphEngine(producers, consumers)
        if flow_type not in FLOW_TYPES:
            raise ValueError('flow_type must be one of {}'.format(','.join(FLOW_TYPES)))
        if flow_type == BATCH:
            self._execution_engine = BatchExecutionEngine()
        elif flow_type == REALTIME:
            self._execution_engine = RealtimeExecutionEngine()

    def run(self):
        '''
        Simple documentation: It starts the flow. 

        More complex documentation: 
        
        1. It creates a topological sort of the nodes in the \
            computation graph, and wraps each node around a ``videoflow.core.task.Task``
        2. It passes the tasks to the environment, which allocates them and creates the \
            channels that will be used for communication between tasks. Tasks themselves \
            do not know where this channels are, but the environment assigns a messenger \
            to each task that knows how to communicate in those channels.
        '''

        #1. Build a topological sort of the graph.
        tsort = self._graph_engine.topological_sort()
        metadata_consumer = MetadataConsumer()(*tsort)
        tsort.append(metadata_consumer)
        
        #2. TODO: OPtimize graph in the following ways:   
        # a) Tasks do not need to pass down to children
        # all of the outputs of parents.  Hence, at a given
        # level of the topological sort, have the list of 
        # inputs from parents that are not needed below that 
        # level

        # b) Not all the processors have to write to a pub/sub channel
        # If their output is only needed by the next preprocessor and non one
        # else below in the graph, then I can string subsequent preprocessors together
        # a big preprocessor
        
        #3. Create the tasks and the input/outputs
        # for them
        # task_data is a list of tuples (node, task_id, parent_task_id, has_chilren)
        tasks_data = _task_data_from_node_tsort(tsort)
        
        # 4. Put each task to run in the place where the processor it
        # contains inside runs.
        self._execution_engine.allocate_and_run_tasks(tasks_data)
        logger.info('Allocated processes for {} tasks'.format(len(tasks_data)))
        logger.info('Started running flow.')
    
    def join(self):
        '''
        Blocking method. Will make the process that calls this method block until the flow finishes
        running naturally.
        '''
        self._execution_engine.join_task_processes()
        logger.info('Flow has stopped.')

    def stop(self):
        '''
        Blocking method. Stops the flow.  Makes the execution environment send a flow termination signal.
        '''
        logger.info('Stop termination signal placed on flow.')
        self._execution_engine.signal_flow_termination()
        self.join()
