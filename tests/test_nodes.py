import pytest

from videoflow.core.graph import GraphEngine
from videoflow.core.node import TaskModuleNode, ProcessorNode
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer

def test_taskmodule_node():
    '''
    Tests simple task module creation
    and tests that it can be part of a flow
    '''
    #1. Tests simple module first
    zero = IntProducer()
    a = IdentityProcessor()(zero)
    b = IdentityProcessor()(a)
    c = IdentityProcessor()(b)
    d = JoinerProcessor()(b, c)
    e = IdentityProcessor()(d)
    f = JoinerProcessor()(d, e, c, b)
    module = TaskModuleNode(a, f)
    out = CommandlineConsumer()(module)
    
    #2. Tests that you raise an exception as error here.
    with pytest.raises(RuntimeError):
        out1 = CommandlineConsumer()(f)
    graph_engine = GraphEngine([zero], [out])

    tsort = graph_engine.topological_sort()
    assert len(tsort) == 3

def test_taskmodule_node_1():
    '''
    Tests that task module can create its own parents without
    having to take them from the entry node.
    '''
    
    zero = IntProducer()
    a = IdentityProcessor()
    b = IdentityProcessor()(a)
    c = IdentityProcessor()(b)
    task_module = TaskModuleNode(a, c)(zero)
    out = CommandlineConsumer()(task_module)

    graph_engine = GraphEngine([zero], [out])
    
def test_taskmodule_node_2():
    '''
    Tests that task module can take the childs from its exit_entry
    '''
    zero = IntProducer()
    a = IdentityProcessor()(zero)
    b = IdentityProcessor()(a)
    c = IdentityProcessor()(b)
    out = CommandlineConsumer()(c)
    task_module = TaskModuleNode(a, c)

    graph_engine = GraphEngine([zero], [out])
    tsort = graph_engine.topological_sort()
    assert len(tsort) == 3
    assert task_module in tsort

def test_taskmodule_node_3():
    '''
    Test error when trying to put module inside of moduel
    '''

    #2. Tests module inside of module
    zero = IntProducer()
    a = IdentityProcessor()(zero)
    b = IdentityProcessor()(a)
    c = IdentityProcessor()(b)
    d = JoinerProcessor()(b, c)
    e = IdentityProcessor()(d)
    f = JoinerProcessor()(d, e, c, b)
    module = TaskModuleNode(a, f)

    g = IdentityProcessor()(module)
    h = IdentityProcessor()(g)
    i = IdentityProcessor()(h)
    with pytest.raises(ValueError):
        module1 = TaskModuleNode(module, i)

def test_taskmodule_node_4():
    '''
    Test the process algorithm of the taskmodule node
    '''

if __name__ == "__main__":
    pytest.main([__file__])

