from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import logging
from multiprocessing import Queue, Lock
from threading import Thread

from .node import Node, ProducerNode, ProcessorNode, ConsumerNode
from .constants import STOP_SIGNAL, BATCH, REALTIME, FLOW_TYPES
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
        - tsort_id (int): the position of the node in the topological sort
        - parent_tsort_id: the position of the parent node in the topological sort.
    '''
    def __init__(self, computation_node : Node, messenger, tsort_id : int, parent_tsort_id : int = None):
        self._messenger = messenger
        self._computation_node = computation_node
        self._tsort_id = tsort_id
        self._parent_tsort_id = parent_tsort_id
    
    @property
    def id(self):
        '''
        Returns an integer as id.
        '''
        return self._tsort_id
    
    @property
    def parent_id(self):
        '''
        Returns the id of the parent task.  Id of parent task is lower than id of current task.
        '''
        return self._parent_tsort_id
    
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
        self._run()
        self._computation_node.close()


class ProducerTask(NodeTask):
    '''
    It runs forever calling the ``next()`` method in the producer node. \
    At each iteration it checks for a termination signal, and if so it \
    sends a termination message to its child task and breaks the infinite loop.
    '''
    def __init__(self, producer : ProducerNode, messenger, task_id : int):
        self._producer = producer
        super(ProducerTask, self).__init__(producer, messenger, task_id)
    
    def _run(self):
        while True:
            try:
                with DelayedKeyboardInterrupt():
                    a = self._producer.next()
                    self._messenger.publish_message(a)
            except StopIteration:
                break
            except KeyboardInterrupt:
                logger.info("Interrupt signal received. Sending signal to stop flow.")
                break
            if self._messenger.check_for_termination():
                break
        self._messenger.publish_termination_message(STOP_SIGNAL)

class ProcessorTask(NodeTask):
    '''
    It runs forever, first blocking until it receives a message from parent nodes through \
    the messenger.  Then it passes it to the processor node and when it gets back the output \
    it uses the messenger to publish it down the flow. If among the inputs it received from \
    a parent it receives a termination message, it passes termination message down the flow \
    and breaks from infinite loop.
    '''
    def __init__(self, processor : ProcessorNode, messenger, task_id : int, parent_task_id : int):
        self._processor = processor
        
        super(ProcessorTask, self).__init__(processor, messenger, task_id, parent_task_id)    
    
    @property
    def device_type(self):
        return self._processor.device_type
    
    def change_device(self, device_type : str):
        self._processor.change_device(device_type)
    
    def _run(self):
        while True:
            try:
                with DelayedKeyboardInterrupt():
                    inputs = self._messenger.receive_message()
                    stop_signal_received = any([isinstance(a, str) and a == STOP_SIGNAL for a in inputs])
                    if stop_signal_received:
                        self._messenger.publish_termination_message(STOP_SIGNAL)
                        break

                    #3. Pass inputs needed to processor
                    output = self._processor.process(*inputs)
                    self._messenger.publish_message(output)
            except KeyboardInterrupt:
                continue
        
class ConsumerTask(NodeTask):
    '''
    It runs forever, blocking until it receives a message from parent nodes through the messenger.
    It consumes the message and does not publish anything back down the pipe.

    If a consumer task has tasks after it in the topological sort, it does not mean that
    those tasks expect any input from the consumer task. It simply means that the consumer
    task is a passthrough of messages. 
    '''
    def __init__(self, consumer : ConsumerNode, messenger, task_id : int, parent_task_id : int,
                has_children_task : bool):
        self._consumer = consumer
        self._has_children_task = has_children_task
        super(ConsumerTask, self).__init__(consumer, messenger, task_id, parent_task_id)
    
    def _run(self):
        while True:
            try:
                with DelayedKeyboardInterrupt():
                    inputs = self._messenger.receive_message()
                    stop_signal_received = any([isinstance(a, str) and a == STOP_SIGNAL for a in inputs])
                    if stop_signal_received:
                        # No need to pass through stop signal to children.
                        # If children need to stop, they will receive it from
                        # someone else, so the message that I am passing through
                        # might be the one carrying it.
                        if self._has_children_task:
                            self._messenger.passthrough_termination_message()
                        break

                    if self._has_children_task:
                        self._messenger.passthrough_message()
                    self._consumer.consume(*inputs)
            except KeyboardInterrupt:
                continue

class MultiprocessingTask(Task):
    def __init__(self, processor : ProcessorNode):
        self._processor = processor
        self._parent_nodes_ids = [a.id for a in self._processor.parents]
    
    def _inputs_from_raw_inputs(self, raw_inputs):
        inputs = [raw_inputs[a] for a in self._parent_nodes_ids]
        return inputs

    def _has_stop_signal(self, raw_inputs):
        inputs = self._inputs_from_raw_inputs(raw_inputs)
        stop_signal_received = any([isinstance(a, str) and a == STOP_SIGNAL for a in inputs])
        return stop_signal_received

class MultiprocessingReceiveTask(MultiprocessingTask):
    def __init__(self, processor: ProcessorNode, parent_task_queue : Queue, receiveQueue : Queue, flow_type : str):
        self._parent_task_queue = parent_task_queue
        self._rq = receiveQueue
        if flow_type not in FLOW_TYPES:
            raise ValueError('flow_type must be one of {}'.format(','.join(FLOW_TYPES)))
        self._flow_type = flow_type
        super(MultiprocessingReceiveTask, self).__init__(processor)

    def run(self):
        while True:
            try:
                with DelayedKeyboardInterrupt():
                    raw_inputs = self._parent_task_queue.get()
                    if self._has_stop_signal(raw_inputs):
                        self._rq.put(raw_inputs, block = True)
                        break
                    if self._flow_type == BATCH:
                        self._rq.put(raw_inputs, block = True)
                    elif self._flow_type == REALTIME:
                        self._rq.put(raw_inputs, block = False)
            except KeyboardInterrupt:
                continue

class MultiprocessingProcessorTask(MultiprocessingTask):
    def __init__(self, idx : int, processor: ProcessorNode, lock : Lock,
                receiveQueue : Queue, accountingQueue : Queue, outputQueue : Queue):
        self._idx = idx
        self._lock = lock
        self._rq = receiveQueue
        self._aq = accountingQueue
        self._oq = outputQueue
        super(MultiprocessingProcessorTask, self).__init__(processor)
    
    @property
    def device_type(self):
        return self._processor.device_type
    
    def run(self):
        while True:
            try:
                with DelayedKeyboardInterrupt():
                    #1. Read from rq and update aq
                    with self._lock:
                        raw_inputs = self._rq.get(block = True)
                        self._aq.put(self._idx)

                    #2. If STOP_SIGNAL, place it in output, place it back in receiver, and break loop
                    if self._has_stop_signal(raw_inputs):
                        self._rq.put(raw_inputs, block = True)
                        raw_outputs = dict(raw_inputs)
                        raw_outputs[self._processor.id] = STOP_SIGNAL
                        self._oq.put(raw_outputs)
                        break
                    
                    #3. Else: process it, and place result in oq
                    inputs = self._inputs_from_raw_inputs(raw_inputs)
                    output = self._processor.process(*inputs)
                    raw_inputs[self._processor.id] = output
                    self._oq.put(raw_inputs)
            except KeyboardInterrupt:
                continue

class MultiprocessingOutputTask(MultiprocessingTask):
    def __init__(self, processor : ProcessorNode, task_queue : Queue, accountingQueue : Queue,
                output_queues : [Queue], flow_type : str):
        self._aq = accountingQueue
        self._task_queue = task_queue
        self._output_queues = output_queues
        self._finish_count = 0
        if flow_type not in FLOW_TYPES:
            raise ValueError('flow_type must be one of {}'.format(','.join(FLOW_TYPES)))
        self._flow_type = flow_type
        super(MultiprocessingOutputTask, self).__init__(processor)
    
    def run(self):
        while True:
            try:   
                with DelayedKeyboardInterrupt():
                    next_idx = self._aq.get(block = True)
                    raw_outputs = self._output_queues[next_idx].get(block = True)
                    if self._has_stop_signal(raw_outputs):
                        self._finish_count += 1
                    if self._flow_type == BATCH:
                        self._task_queue.put(raw_outputs, block = True)
                    elif self._flow_type == REALTIME:
                        self._task_queue.put(raw_outputs, block = False)
                    if self._finish_count == len(self._output_queues):
                        break
                    
            except KeyboardInterrupt:
                continue