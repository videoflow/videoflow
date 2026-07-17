from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import logging
import time

from .node import Node, ProducerNode, ProcessorNode, ConsumerNode
from ..utils.generic_utils import DelayedKeyboardInterrupt

logger = logging.getLogger(__package__)

class Task:
    def run(self):
        '''
        Starts the task in an infinite loop.
        '''
        raise NotImplementedError('Subclass needs to implement run')

class NodeTask(Task):
    '''
    A ``NodeTask`` is a wrapper around a ``videoflow.core.node.Node`` that \
        is able to interact with the execution environment through a messenger. \
        Nodes receive input and/or produce output, but tasks are the ones \
        that run in infinite loops, receiving inputs from the environment and passing them to the \
        computation node, and taking outputs from the computation node and passing \
        them to the environment.

    - Arguments:
        - computation_node
        - messenger (Messenger): the messenger that will communicate between nodes.
        - has_children (bool): True if this node has at least one downstream child \
            in the graph — used to skip publishing when nothing would ever consume it.
    '''
    def __init__(self, computation_node : Node, messenger, has_children : bool):
        self._messenger = messenger
        self._computation_node = computation_node
        self._has_children = has_children

    @property
    def computation_node(self):
        '''
        Returns the current computation node
        '''
        return self._computation_node

    def _assert_messenger(self):
        assert self._messenger is not None, 'Task cannot run if messenger has not been set.'

    def _run(self):
        raise NotImplementedError('Sublcass needs to implement _run')

    def run(self):
        '''
        Starts the task in an infinite loop.  If this method is called and the \
            ``set_messenger()`` method has not been called yet, an assertion error \
            will happen.
        '''
        self._assert_messenger()
        self._computation_node.open()
        try:
            self._run()
        finally:
            self._computation_node.close()


class ProducerTask(NodeTask):
    '''
    It runs forever calling the ``next()`` method in the producer node. \
    At each iteration it checks for a termination signal, and if so it \
    sends a termination message to its child task and breaks the infinite loop.
    '''
    def __init__(self, producer : ProducerNode, messenger, has_children : bool):
        self._producer = producer
        super(ProducerTask, self).__init__(producer, messenger, has_children)

    def _run(self):
        previous_end_t = time.time()
        while True:
            try:
                with DelayedKeyboardInterrupt():
                    if self._messenger.check_for_termination():
                        break
                    start_t = time.time()
                    a = self._producer.next()
                    end_t = time.time()
                    proc_time = end_t - start_t
                    actual_proc_time = end_t - previous_end_t
                    previous_end_t = end_t
                    if self._has_children:
                        self._messenger.publish_message(
                            a,
                            {
                                'proctime': proc_time,
                                'actual_proctime': actual_proc_time
                            }
                        )
            except StopIteration:
                break
            except KeyboardInterrupt:
                logger.info('Interrupt signal received. Sending signal to stop flow.')
                break
        if self._has_children:
            self._messenger.publish_stop_signal()

class ProcessorTask(NodeTask):
    '''
    It runs forever, first blocking until it receives a message from every parent \
    node through the messenger. Then it passes the merged inputs to the processor \
    node and, when it gets back the output, uses the messenger to publish it down \
    the flow. If every parent has signaled termination, it passes termination \
    message down the flow and breaks from infinite loop.
    '''
    def __init__(self, processor : ProcessorNode, messenger, has_children : bool, parent_names):
        '''
        - Arguments:
            - parent_names ([str]): names of this node's real parents, in the exact \
                order ``process()`` expects its positional arguments. Passed \
                explicitly rather than read off ``processor.parents`` because a \
                worker process only reconstructs the one node it's responsible for \
                (via ``get_params()``) — it never has the live parent ``Node`` \
                objects the way the single-process local graph-building step does.
        '''
        self._processor = processor
        self._parent_names = list(parent_names)
        super(ProcessorTask, self).__init__(processor, messenger, has_children)

    @property
    def device_type(self):
        return self._processor.device_type

    def change_device(self, device_type : str):
        self._processor.change_device(device_type)

    def _run(self):
        previous_end_t = time.time()
        while True:
            try:
                with DelayedKeyboardInterrupt():
                    inputs_d = self._messenger.receive_message()
                    # Order matters: process(*inputs) is positional, so entries must
                    # follow the node's own declared parent order, not dict iteration
                    # order (the messenger may assemble the join in arrival order).
                    entries = [inputs_d[name] for name in self._parent_names]
                    if any(e['is_stop_signal'] for e in entries):
                        if self._has_children:
                            self._messenger.publish_stop_signal()
                        break

                    inputs = [e['message'] for e in entries]
                    if self._has_children:
                        start_2_t = time.time()
                        output = self._processor.process(*inputs)
                        end_t = time.time()
                        proc_time = end_t - start_2_t
                        actual_proc_time = end_t - previous_end_t
                        previous_end_t = end_t
                        self._messenger.publish_message(
                            output,
                            {
                                'proctime': proc_time,
                                'actual_proctime': actual_proc_time
                            }
                        )
                    else:
                        self._processor.process(*inputs)
            except KeyboardInterrupt:
                continue

class ConsumerTask(NodeTask):
    '''
    It runs forever, blocking until it receives a message from every parent node \
    through the messenger. It consumes the message and does not publish anything \
    back down the pipe — consumers are the leaves of the graph.
    '''
    def __init__(self, consumer : ConsumerNode, messenger, has_children : bool, parent_names):
        self._consumer = consumer
        self._parent_names = list(parent_names)
        super(ConsumerTask, self).__init__(consumer, messenger, has_children)

    def _run(self):
        while True:
            try:
                with DelayedKeyboardInterrupt():
                    inputs_d = self._messenger.receive_message()
                    entries = [inputs_d[name] for name in self._parent_names]
                    if any(e['is_stop_signal'] for e in entries):
                        break

                    if not self._consumer.metadata:
                        inputs = [e['message'] for e in entries]
                        self._consumer.consume(*inputs)
                    else:
                        metadatas = [e['metadata'] for e in entries]
                        self._consumer.consume(*metadatas)
            except KeyboardInterrupt:
                continue
