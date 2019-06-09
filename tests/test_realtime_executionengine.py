import pytest

from videoflow.core.flow import _task_data_from_node_tsort
from videoflow.engines.realtime import RealtimeExecutionEngine
from videoflow.utils.graph import topological_sort
from videoflow.core.node import TaskModuleNode
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer

import videoflow.utils.system as vf_system

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

def test_gpu_nodes_accepted(monkeypatch):
    def gpus_mock():
        return [0, 1, 3]

    monkeypatch.setattr(vf_system, 'get_gpus_available_to_process', gpus_mock)
    #1. Test that gpu nodes are accepted by having same number of gpu 
    # processes as gpus in the system

    # TODO: Create my own gpu processor tester that raises an 
    # error if the device type is attempted to change.
    A = IntProducer()
    B = IdentityProcessor(device_type = 'gpu')(A)
    C = IdentityProcessor(device_type = 'gpu')(B)
    D = JoinerProcessor(device_type = 'gpu')(C)
    E = JoinerProcessor()(B, C, D)

    tsort = topological_sort([A])
    tasks_data = _task_data_from_node_tsort(tsort)

    ee = RealtimeExecutionEngine()
    ee._al_create_processes(tasks_data)

    #2. Test that gpu nodes are accepted by having nodes not thrown an 
    #error if gpu is not available
    pass

def test_gpu_nodes_not_accepted():
    #1. Test that gpu node rejects because it cannot find gpu in system

    #2. Test that gpu node rejects because already all gpus were allocated
    # to other nodes.
    pass

if __name__ == "__main__":
    pytest.main([__file__])