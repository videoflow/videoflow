'''
Tests for the env-driven worker entrypoint (``videoflow.runtime.worker``), which
rebuilds one node inside its own container from environment variables alone.

The node's *class* comes from VF_NODE_CLASS and its *family* from VF_NODE_KIND.
The compiler writes both together, so they agree in practice — but nothing
downstream re-checked it, and a disagreement picked the wrong Task type and only
failed later inside the run loop.
'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.core.node import ConsumerNode, ProcessorNode, ProducerNode
from videoflow.runtime.worker import build_node_from_env, require_node_kind


class _Producer(ProducerNode):
    def next(self):
        raise StopIteration


class _Processor(ProcessorNode):
    def process(self, inp):
        return inp


class _Consumer(ConsumerNode):
    def consume(self, item):
        pass


def test_require_node_kind_returns_narrowed_node_on_match():
    node = _Producer()
    assert require_node_kind(node, ProducerNode, 'producer') is node


@pytest.mark.parametrize('node_factory, expected, kind', [
    (_Consumer, ProducerNode, 'producer'),
    (_Producer, ConsumerNode, 'consumer'),
    (_Producer, ProcessorNode, 'processor'),
])
def test_require_node_kind_rejects_mismatch(node_factory, expected, kind):
    '''
    A kind/class disagreement must fail immediately with a message naming the fix,
    not as an opaque AttributeError deep in the run loop.
    '''
    with pytest.raises(ValueError, match = r'VF_NODE_KIND') as exc:
        require_node_kind(node_factory(), expected, kind)
    message = str(exc.value)
    assert expected.__name__ in message
    assert node_factory.__name__ in message   # names the offending VF_NODE_CLASS
    assert 'redeploy' in message              # names the fix


def test_build_node_from_env_reconstructs_node(monkeypatch):
    '''The get_params() round trip: class path + JSON params rebuild the node.'''
    monkeypatch.setenv('VF_NODE_CLASS', 'videoflow.processors.basic.IdentityProcessor')
    monkeypatch.setenv('VF_NODE_PARAMS_JSON', '{"fps": 5}')
    node = build_node_from_env()
    assert isinstance(node, ProcessorNode)
    assert require_node_kind(node, ProcessorNode, 'processor') is node


def test_build_node_from_env_without_class_names_the_remote_case(monkeypatch):
    '''A remote component scheduled onto the Python worker image is a deploy error.'''
    monkeypatch.delenv('VF_NODE_CLASS', raising = False)
    monkeypatch.setenv('VF_COMPONENT_REF', 'acme/thing:1.0.0')
    with pytest.raises(RuntimeError, match = 'VF_NODE_CLASS'):
        build_node_from_env()


if __name__ == '__main__':
    pytest.main([__file__])
