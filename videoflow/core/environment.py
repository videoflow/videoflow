from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from .task import Task
from .node import Node
from multiprocessing import Process

class Messenger:
    def publish_message(self, message):
        raise NotImplemented('Messenger subclass must implement method')
    
    def passthrough_message(self):
        raise NotImplemented('Messenger subclass must implement method')
    
    def passthrough_termination_message(self):
        raise NotImplemented('Messenger subclass must implement method')

    def publish_termination_message(self, message):
        raise NotImplemented('Messenger subclass must implement method')

    def check_for_termination(self):
        raise NotImplemented('Messenger subclass must implement method')

    def receive_message(self):
        '''
        Blocking method
        '''
        raise NotImplemented('Messenger subclass must implement method')


class ExecutionEnvironment:
    def __init__(self):
        pass
    
    def _al_create_communication_channels(self, tasks):
        raise NotImplemented('Subclass of ExecutionEnvironment must implement')
    
    def _al_create_and_set_messengers(self, tasks):
        raise NotImplemented('Subclass of ExecutionEnvironment must implement')
    
    def _al_create_and_start_processes(self, tasks):
        raise NotImplemented('Subclass of ExecutionEnvironment must implement')
    
    def signal_flow_termination(self):
        raise NotImplemented('Subclass of ExecutionEnvironment must implement')
    
    def join_task_processes(self):
        '''
        Blocking method
        '''
        raise NotImplemented('Subclass of ExecutionEnvironment must implement')

    def allocate_and_run_tasks(self, tasks):
        self._al_create_communication_channels(tasks)
        self._al_create_and_set_messengers(tasks)
        self._al_create_and_start_processes(tasks)
    
    

    

    
