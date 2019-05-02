from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from .task import Task
from .node import Node
from multiprocessing import Process

class Messenger:
    '''Not
    Utility class that tasks use to receive input and write output. \
    The ``videoflow.core.task.Task`` class knows what are the graph nodes from \
    where it receives computation outputs, and knows what are the graph nodes \
    that depend on its computation, but is oblivious of how to communicate with them. \
    The messenger, which is tightly coupled with the `execution environment` being used, \
    knows how to do this for the task.
    '''
    def publish_message(self, message):
        '''
        Publishes output message to a place where the child task will receive it.
        Depending on the kind of environment, this method might drop the message
        if the receiving container (usually a queue) is full.
        '''
        raise NotImplementedError('Messenger subclass must implement method')
    
    def passthrough_message(self):
        '''
        Used when the task has received a message that might be needed by another \
        task below it, but when the task itself does not produces output needed by \
        tasks below it. (i.e.: ``videoflow.core.task.ConsumerTask``)

        Depending on the kind of environment, this method might drop the message \
        if the receiving container (usually a queue) is full.
        '''
        raise NotImplementedError('Messenger subclass must implement method')
    
    def passthrough_termination_message(self):
        '''
        Same as ``passthrough_message``, but this method will never drop the message regardless \
        of environment, which means that sometimes this method might block until it can deliver \ 
        the message.
        '''
        raise NotImplementedError('Messenger subclass must implement method')

    def publish_termination_message(self, message):
        '''
        Similar to ``publish_message``, but this method will never drop the message regardless \
        of environment, which means that sometimes this method might block until it can deliver \ 
        the message.
        '''
        raise NotImplementedError('Messenger subclass must implement method')

    def check_for_termination(self) -> bool:
        '''
        Returns true if a flow termination signal has been received.  Used by ``videoflow.core.task.ProducerTask``.
        '''
        raise NotImplementedError('Messenger subclass must implement method')

    def receive_message(self):
        '''
        This method blocks. It waits until a message has been received.

        - Returns:
            - message: the message received from parent task in topological sort.
        '''
        raise NotImplementedError('Messenger subclass must implement method')


class ExecutionEnvironment:
    '''
    Defines the interface of the `execution environment`
    '''
    def __init__(self):
        pass
    
    def _al_create_communication_channels(self, tasks):
        raise NotImplementedError('Subclass of ExecutionEnvironment must implement')
    
    def _al_create_and_set_messengers(self, tasks):
        raise NotImplementedError('Subclass of ExecutionEnvironment must implement')
    
    def _al_create_and_start_processes(self, tasks):
        raise NotImplementedError('Subclass of ExecutionEnvironment must implement')
    
    def signal_flow_termination(self):
        '''
        Signals the execution environment that the flow needs to stop. \
        When this signal is received, all consumer tasks will pick it on \
        and pass it together with the flow until they reach every task in \
        the graph and everyone stops working.
        '''
        raise NotImplementedError('Subclass of ExecutionEnvironment must implement')
    
    def join_task_processes(self):
        '''
        Blocking method.  It is supposed to make the calling process sleep until all task \
        processes have finished processing.
        '''
        raise NotImplementedError('Subclass of ExecutionEnvironment must implement')

    def allocate_and_run_tasks(self, tasks):
        '''
        Defines a template with the order of methods that need to run in order to \
        allocate and run tasks.  How those methods are implemented corresponds to \
        subclasses of this class that implement different `execution environments`.

        '''
        self._al_create_communication_channels(tasks)
        self._al_create_and_set_messengers(tasks)
        self._al_create_and_start_processes(tasks)
    
    

    

    
