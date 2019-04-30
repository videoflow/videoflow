from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

from .task import Task

GPU = 'gpu'
CPU = 'cpu'

class Allocator:
    '''
    The allocator object manages the allocation of tasks.
    It is responsible for deciding where to allocate the tasks, 
    taking into account the available resources in the system
    and the desire of the task allocation object.

    The idea is the following:
    1. Have the computation nodes implement different allocations
    or just one.
    2. If not is implemented, then we default to cpu. If gpu is
    implemented, then figure out how to do it.
    '''
    def allocate_task(task : Task):
        

class Allocation(object):
    '''
    An allocation is defined by the following parameters:
    - machine
    - nb_proc

    The default parameters are `localhost` for `machine`,
    and `1` for `nb_proc`
    '''
    def __init__(self, machine : str, nb_proc : int):
        self._machine = machine,
        self._nb_proc = nb_proc
    
    @property
    def machine(self):
        '''
        Returns the ip address of the machine
        where the task should be allocated.
        '''
        return self._machine
    
    @property
    def nb_proc(self):
        '''
        Returns the number of processors to allocate.
        '''
        return self._nb_proc
