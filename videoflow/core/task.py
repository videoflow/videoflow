from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from .node import Node, ProducerNode, ProcessorNode, ConsumerNode

STOP_SIGNAL = "alalsl;'sdlfj2389jdalskmghsaiaw98y8asdf;askljoa8y;dsf;lkasdb"

class Task:
    '''
    A ``Task`` is a wrapper around a ``videoflow.core.node.Node`` that \
        is able to interact with the execution environment through a messenger. \
        Nodes receive input and/or produce output, but tasks are the ones \
        that run in infinite loops, receiving inputs from the environment and passing them to the \
        computation node, and taking outputs from the computation node and passing \
        them to the environment.
    '''
    def __init__(self, computation_node : Node, task_id : int, parent_task_id : int = None):
        self._messenger = None
        self._computation_node = computation_node
        self._task_id = task_id
        self._parent_task_id = parent_task_id
    
    @property
    def id(self):
        '''
        Returns an integer as id.
        '''
        return self._task_id
    
    @property
    def parent_id(self):
        '''
        Returns the id of the parent task.  Id of parent task is lower than id of current task.
        '''
        return self._parent_task_id
    
    @property
    def computation_node(self):
        '''
        Returns the current computation node
        '''
        return self._computation_node

    def set_messenger(self, messenger):
        '''
        Used by environment to set the messenger that this task will use to interact with other 
        tasks
        '''
        self._messenger = messenger

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

class ProducerTask(Task):
    '''
    It runs forever calling the ``next()`` method in the producer node. \
    At each iteration it checks for a termination signal, and if so it \
    sends a termination message to its child task and breaks the infinite loop.
    '''
    def __init__(self, producer : ProducerNode, task_id : int):
        self._producer = producer
        super(ProducerTask, self).__init__(producer, task_id)
    
    def _run(self):
        while True:
            try:
                a = self._producer.next()
                self._messenger.publish_message(a)
            except StopIteration:
                break
            if self._messenger.check_for_termination():
                break
        self._messenger.publish_termination_message(STOP_SIGNAL)

class ProcessorTask(Task):
    '''
    It runs forever, first blocking until it receives a message from parent nodes through \
    the messenger.  Then it passes it to the processor node and when it gets back the output \
    it uses the messenger to publish it down the flow. If among the inputs it received from \
    a parent it receives a termination message, it passes termination message down the flow \
    and breaks from infinite loop.
    '''
    def __init__(self, processor : ProcessorNode, task_id : int, parent_task_id : int):
        self._processor = processor
        
        super(ProcessorTask, self).__init__(processor, task_id, parent_task_id)    
    
    @property
    def device_type(self):
        return self._processor.device_type
    
    def change_device(self, device_type : str):
        self._processor.change_device(device_type)
    
    def _run(self):
        while True:
            inputs = self._messenger.receive_message()
            stop_signal_received = any([isinstance(a, str) and a == STOP_SIGNAL for a in inputs])
            if stop_signal_received:
                self._messenger.publish_termination_message(STOP_SIGNAL)
                break

            #3. Pass inputs needed to processor
            output = self._processor.process(*inputs)
            self._messenger.publish_message(output)
        
class ConsumerTask(Task):
    '''
    It runs forever, blocking until it receives a message from parent nodes through the messenger.
    It consumes the message and does not publish anything back down the pipe.

    If a consumer task has tasks after it in the topological sort, it does not mean that
    those tasks expect any input from the consumer task. It simply means that the consumer
    task is a passthrough of messages. 
    '''
    def __init__(self, consumer : ConsumerNode, task_id : int, parent_task_id : int,
                has_children_task : bool):
        self._consumer = consumer
        self._has_children_task = has_children_task
        super(ConsumerTask, self).__init__(consumer, task_id, parent_task_id)
    
    def _run(self):
        while True:
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
    