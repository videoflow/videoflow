'''
Tests multiple situations regarding allocation to gpu.
'''
import pytest
import time

from videoflow.core.flow import _task_data_from_node_tsort
from videoflow.engines.realtime import RealtimeExecutionEngine
from videoflow.utils.graph import topological_sort
from videoflow.core.node import TaskModuleNode, ProcessorNode
from videoflow.core.constants import CPU, GPU
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer

import videoflow.utils.system

def test_nb_tasks_created():
    #1. Test that the number of tasks created is equal to number of nodes
    A = IntProducer()
    B = IdentityProcessor()(A)
    C = IdentityProcessor()(B)
    D = JoinerProcessor()(C)
    E = JoinerProcessor()(B, C, D)

    tsort = topological_sort([A])
    tasks_data = _task_data_from_node_tsort(tsort)

    ee = RealtimeExecutionEngine()
    ee._al_create_processes(tasks_data)
    assert len(tsort) == len(ee._procs)

def test_nb_tasks_created_1():
    #2. Test that number of tasks created is different than number of
    # nodes, in the case of TaskModuleProcessor
    zero = IntProducer()
    a = IdentityProcessor()(zero)
    b = IdentityProcessor()(a)
    c = IdentityProcessor()(b)
    d = JoinerProcessor()(b, c)
    e = IdentityProcessor()(d)
    f = JoinerProcessor()(d, e, c, b)
    module = TaskModuleNode(a, f)
    out = CommandlineConsumer()(module)

    tsort = topological_sort([zero])
    tasks_data = _task_data_from_node_tsort(tsort)

    ee = RealtimeExecutionEngine()
    ee._al_create_processes(tasks_data)
    assert len(ee._procs) == 3

class IdentityProcessorGpuOnly(ProcessorNode):
    def __init__(self, fps = -1):
        super(IdentityProcessorGpuOnly, self).__init__(device_type = GPU)
        if fps > 0:
            self._wts = 1.0 / fps # wait time in seconds
        else:
            self._wts = 0

    def process(self, inp):
        if self._wts > 0:
            time.sleep(self._wts)
        return inp
    
    def change_device(self, device_type):
        if device_type == CPU:
            raise ValueError('Cannot allocate to CPU')

def test_gpu_nodes_accepted(monkeypatch):
    def gpus_mock():
        return [0, 1, 3]

    monkeypatch.setattr(videoflow.engines.realtime, 'get_gpus_available_to_process', gpus_mock)
    #1. Test that gpu nodes are accepted by having same number of gpu 
    # processes as gpus in the system
    A = IntProducer()
    B = IdentityProcessorGpuOnly()(A)
    C = IdentityProcessorGpuOnly()(B)
    D = IdentityProcessorGpuOnly()(C)
    E = JoinerProcessor()(D)
    F = JoinerProcessor()(B, C, E, D)

    tsort = topological_sort([A])
    tasks_data = _task_data_from_node_tsort(tsort)

    ee = RealtimeExecutionEngine()
    ee._al_create_processes(tasks_data)

    #2. Test that gpu nodes are accepted by having nodes not thrown an 
    #error if gpu is not available
    A1 = IntProducer()
    B1 = IdentityProcessor(device_type = GPU)(A1)
    C1 = IdentityProcessor(device_type = GPU)(B1)
    D1 = IdentityProcessor(device_type = GPU)(A1)
    D1 = JoinerProcessor(device_type = GPU)(C1)
    E1 = JoinerProcessor(device_type = GPU)(B1, C1, D1)

    tsort = topological_sort([A1])
    tasks_data = _task_data_from_node_tsort(tsort)

    ee1 = RealtimeExecutionEngine()
    ee1._al_create_processes(tasks_data)

def test_gpu_nodes_not_accepted(monkeypatch):
    #2. Test that gpu node rejects because already all gpus were allocated
    # to other nodes.
    def gpus_mock():
        return [0, 1]

    monkeypatch.setattr(videoflow.utils.system, 'get_gpus_available_to_process', gpus_mock)
    
    A = IntProducer()
    B = IdentityProcessorGpuOnly()(A)
    C = IdentityProcessorGpuOnly()(B)
    D = IdentityProcessorGpuOnly()(C)
    E = JoinerProcessor()(D)
    F = JoinerProcessor()(B, C, D, E)

    tsort = topological_sort([A])
    tasks_data = _task_data_from_node_tsort(tsort)

    ee = RealtimeExecutionEngine()
    with pytest.raises(RuntimeError):
        ee._al_create_processes(tasks_data)
    

if __name__ == "__main__":
    pytest.main([__file__])