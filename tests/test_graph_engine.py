import pytest

from videoflow.consumers import CommandlineConsumer
from videoflow.core.errors import GraphError
from videoflow.core.graph import GraphEngine
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer


def _detached_consumer(producer):
    '''A consumer with no parents at all.'''
    return CommandlineConsumer()


def _detached_chain(producer):
    '''A consumer whose only ancestor is a processor nobody feeds.'''
    return CommandlineConsumer()(IdentityProcessor())


@pytest.mark.parametrize('build_consumer', [_detached_consumer, _detached_chain])
def test_a_consumer_the_producers_cannot_reach_is_rejected(build_consumer):
    a = IntProducer()
    IdentityProcessor()(IdentityProcessor()(a))
    with pytest.raises(GraphError):
        GraphEngine([a], [build_consumer(a)])


if __name__ == "__main__":
    pytest.main([__file__])
