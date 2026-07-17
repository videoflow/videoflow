'''
Tests for the distributed-execution foundations added to core.node/core.graph:
stable string node identity, get_params()-based reconstruction, and multi-producer
graph support.
'''
import pytest

from videoflow.core.graph import GraphEngine
from videoflow.core.node import Node
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer

def test_auto_generated_names_are_unique():
    a = IntProducer()
    b = IntProducer()
    c = IdentityProcessor()(a)
    assert a.name != b.name
    assert a.name != c.name

def test_explicit_name_is_used():
    a = IntProducer(name = 'camera-1')
    assert a.name == 'camera-1'

def test_duplicate_explicit_names_rejected_at_graph_build():
    a = IntProducer(name = 'dup')
    b = IdentityProcessor(name = 'dup')(a)
    c = CommandlineConsumer()(b)
    with pytest.raises(ValueError):
        GraphEngine([a], [c])

def test_get_params_round_trip():
    a = IntProducer(start_value = 1, end_value = 10, fps = 5, name = 'p1')
    params = a.get_params()
    b = IntProducer(**params)
    assert b.get_params() == params
    assert b.name == 'p1'

def test_get_params_missing_attribute_raises():
    class BadNode(Node):
        def __init__(self, foo, **kwargs):
            # deliberately does not store self._foo / self.foo
            super(BadNode, self).__init__(**kwargs)

    node = BadNode(foo = 1)
    with pytest.raises(AttributeError):
        node.get_params()

def test_multi_producer_graph():
    #1. Two independent producers feeding a shared joiner — this used to be
    # rejected outright by GraphEngine's single-producer restriction.
    a = IntProducer(name = 'cam-a')
    b = IntProducer(name = 'cam-b')
    joined = JoinerProcessor()(a, b)
    out = CommandlineConsumer()(joined)

    graph_engine = GraphEngine([a, b], [out])
    tsort = graph_engine.topological_sort()
    assert a in tsort and b in tsort and joined in tsort and out in tsort
    assert set(joined.parents) == {a, b}

def test_single_producer_still_works():
    a = IntProducer()
    b = IdentityProcessor()(a)
    c = CommandlineConsumer()(b)
    graph_engine = GraphEngine([a], [c])
    assert len(graph_engine.topological_sort()) == 3

def test_replicated_join_without_partition_by_is_rejected():
    a = IntProducer(name = 'a')
    b = IdentityProcessor(name = 'b')(a)
    # joiner with 2 parents and nb_tasks > 1 needs partition_by: otherwise replicas
    # would receive the two halves of a join on different workers.
    joined = JoinerProcessor(name = 'joined', nb_tasks = 3)(a, b)
    out = CommandlineConsumer(name = 'out')(joined)
    with pytest.raises(ValueError):
        GraphEngine([a], [out])

def test_replicated_join_with_partition_by_is_accepted():
    a = IntProducer(name = 'a')
    b = IdentityProcessor(name = 'b')(a)
    # partition_by='trace_id' co-locates both halves of a join on one replica.
    joined = JoinerProcessor(name = 'joined', nb_tasks = 3, partition_by = 'trace_id')(a, b)
    out = CommandlineConsumer(name = 'out')(joined)
    GraphEngine([a], [out])  # must not raise

def test_single_parent_processor_can_be_replicated():
    a = IntProducer(name = 'a')
    b = IdentityProcessor(name = 'b', nb_tasks = 5)(a)  # single parent, fine
    out = CommandlineConsumer(name = 'out')(b)
    GraphEngine([a], [out])  # must not raise

if __name__ == "__main__":
    pytest.main([__file__])
