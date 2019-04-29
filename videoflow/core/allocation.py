from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

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
    pass

class Allocation(object):
    '''
    An allocation is defined by the following parameters:
    - machine
    - device_type
    - device_count

    The default parameters are `localhost` for machine, `cpu` for device_type,
    and `1` for device_count
    '''
    def __init__(self, machine : str, device_type : str, device_count : int):
        self._machine = machine,
        self._device_type = device_type
        self._device_count = device_count
    
    @property
    def machine(self):
        '''
        Returns the ip address of the machine
        where the task should be allocated.
        '''
        return self._machine
    
    @property
    def device_type(self):
        '''
        Returns one of `gpu` or `cpu`.
        '''
        return self._device_type
    
    @property
    def device_count(self):
        '''
        Returns the number of processors to allocate
        '''
        return self._device_count