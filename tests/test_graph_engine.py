import pytest

from videoflow.core.graph import GraphEngine
from videoflow.producers import IntProducer
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer

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

    with pytest.raises(ValueError):
        graph_engine = GraphEngine([a], [d])

def test_raise_error_2():
    a = IntProducer()
    b = IdentityProcessor()(a)
    c = IdentityProcessor()(b)
    d = IdentityProcessor()
    e = CommandlineConsumer()(d)

    with pytest.raises(ValueError):
        graph_engine = GraphEngine([a], [e])

if __name__ == "__main__":
    pytest.main([__file__])
