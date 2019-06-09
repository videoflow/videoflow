import pytest

from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor

def test_nb_tasks_created():
    #1. Test that the number of tasks created is equal to number of nodes

    #2. Test that number of tasks created is different than number of
    # nodes, in the case of TaskModuleProcessor
    pass

def test_gpu_nodes_accepted():
    #1. Test that gpu nodes are accepted by having same number of gpu 
    # processes as gpus in the system

    #2. Test that gpu nodes are accepted by having nodes not thrown an 
    #error if gpu is not available

def test_gpu_nodes_not_accepted():
    #1. Test that gpu node rejects because it cannot find gpu in system

    #2. Test that gpu node rejects because already all gpus were allocated
    # to other nodes.
    pass

if __name__ == "__main__":
    pytest.main([__file__])