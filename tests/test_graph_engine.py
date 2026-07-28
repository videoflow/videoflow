import pytest

from videoflow.consumers import CommandlineConsumer
from videoflow.core.errors import GraphError
from videoflow.core.graph import GraphEngine
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer


def test_no_raise_error():
    a = IntProducer()
    b = IdentityProcessor()(a)
    c = IdentityProcessor()(b)
    d = CommandlineConsumer()(c)

    graph_engine = GraphEngine([a], [d])

def test_raise_error_1():
    a = IntProducer()
    b = IdentityProcessor()(a)
    c = IdentityProcessor()(b)
    d = CommandlineConsumer()

    with pytest.raises(GraphError):
        graph_engine = GraphEngine([a], [d])

def test_raise_error_2():
    a = IntProducer()
    b = IdentityProcessor()(a)
    c = IdentityProcessor()(b)
    d = IdentityProcessor()
    e = CommandlineConsumer()(d)

    with pytest.raises(GraphError):
        graph_engine = GraphEngine([a], [e])

if __name__ == "__main__":
    pytest.main([__file__])
