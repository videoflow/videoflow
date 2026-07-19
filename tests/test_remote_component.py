'''
Tests for remote (language-agnostic) components in a Python graph:
the component() factory, compilation to NodeSpecs, the mixed-flow wire check,
and manifest rendering.
'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.components.descriptor import ComponentDescriptor
from videoflow.core import Flow, component
from videoflow.core.compiler import compile_flow, has_remote_components, validate_wire_compatibility
from videoflow.core.node import ConsumerNode, ProducerNode
from videoflow.core.policies import JoinPolicy
from videoflow.core.remote import RemoteConsumer, RemoteProcessor, RemoteProducer


def _descriptor(role = 'processor', **spec_overrides):
    spec = {
        'role': role,
        'protocol': 1,
        'runtime': {'images': {'cpu': 'ghcr.io/acme/thing:1.0.0'}},
        'device': ['cpu'],
        'params': {'schema': {
            'type': 'object', 'additionalProperties': False,
            'properties': {'threshold': {'type': 'number', 'default': 0.5}},
        }},
        'io': {'inputs': [{'name': 'in', 'type': 'any'}]},
    }
    spec.update(spec_overrides)
    return ComponentDescriptor.from_dict({
        'apiVersion': 'videoflow.io/v1', 'kind': 'Component',
        'metadata': {'name': 'acme/thing', 'version': '1.0.0'},
        'spec': spec,
    })


class _NativeProducer(ProducerNode):
    def next(self):
        raise StopIteration


class _NativeConsumer(ConsumerNode):
    def consume(self, item):
        pass


def test_factory_dispatches_on_role():
    assert isinstance(component(_descriptor('producer')), RemoteProducer)
    assert isinstance(component(_descriptor('processor')), RemoteProcessor)
    assert isinstance(component(_descriptor('consumer')), RemoteConsumer)


def test_params_validated_and_defaulted():
    node = component(_descriptor(), params = {})
    assert node.get_params() == {'threshold': 0.5}
    node2 = component(_descriptor(), params = {'threshold': 0.9})
    assert node2.get_params() == {'threshold': 0.9}


def test_device_not_supported_rejected():
    with pytest.raises(ValueError, match = 'device_type'):
        component(_descriptor(), device_type = 'gpu')


def test_no_image_for_device_rejected():
    desc = _descriptor(runtime = {'images': {'gpu': 'x:gpu'}}, device = ['cpu', 'gpu'])
    with pytest.raises(ValueError, match = 'no image'):
        component(desc, device_type = 'cpu')


def test_singleton_cannot_replicate():
    desc = _descriptor(constraints = {'singleton': True})
    with pytest.raises(ValueError, match = 'singleton'):
        component(desc, nb_tasks = 3)


def test_partition_requires_partitionable():
    with pytest.raises(ValueError, match = 'partitionable'):
        component(_descriptor(), partition_by = 'trace_id')
    # Allowed when the descriptor declares it.
    desc = _descriptor(constraints = {'partitionable': True})
    node = component(desc, nb_tasks = 2, partition_by = 'trace_id')
    assert node.partition_by == 'trace_id'


def test_quorum_requires_inputs_accept_missing():
    two_in = _descriptor(io = {'inputs': [{'name': 'a', 'type': 'any'}, {'name': 'b', 'type': 'any'}]})
    policy = JoinPolicy(mode = 'time', tolerance_ms = 10, timeout_seconds = 1, quorum = 1)
    with pytest.raises(ValueError, match = 'missing'):
        component(two_in, join_policy = policy)
    ok = _descriptor(io = {'inputs': [
        {'name': 'a', 'type': 'any', 'accepts': {'missing': True}},
        {'name': 'b', 'type': 'any', 'accepts': {'missing': True}}]})
    node = component(ok, join_policy = policy)
    assert node.join_policy.quorum == 1


def _py_descriptor(**spec_overrides):
    '''A Python component descriptor (has pythonClass), like the migrated contrib ones.'''
    spec = {
        'role': 'processor',
        'protocol': 1,
        'runtime': {
            'pythonClass': 'videoflow_contrib.tracker_sort.KalmanFilterBoundingBoxTracker',
            'images': {'cpu': 'ghcr.io/videoflow/contrib-tracker-sort:0.1'},
        },
        'device': ['cpu'],
        'params': {'schema': {'type': 'object', 'additionalProperties': False,
                            'properties': {'max_age': {'type': 'integer', 'default': 7}}}},
        'io': {'inputs': [{'name': 'detections', 'type': 'videoflow.v1.Tensor'}]},
    }
    spec.update(spec_overrides)
    return ComponentDescriptor.from_dict({
        'apiVersion': 'videoflow.io/v1', 'kind': 'Component',
        'metadata': {'name': 'videoflow/tracker-sort', 'version': '0.1'}, 'spec': spec})


def test_python_component_runs_worker_and_is_not_native():
    desc = _py_descriptor()
    assert desc.is_native is False
    node = component(desc)
    prod = _NativeProducer()
    specs = compile_flow(Flow([_NativeConsumer()(node(prod))]))  # default wire, no force
    spec = next(s for s in specs if s.is_remote)
    # Python component: keeps its class (runs the Python worker), carries its ref,
    # and is NOT native (so it does not force the protobuf wire).
    assert spec.node_class == 'videoflow_contrib.tracker_sort.KalmanFilterBoundingBoxTracker'
    assert spec.component_ref is not None and spec.is_native is False
    assert not has_remote_components([s for s in specs if not s.is_remote])


def test_python_component_get_params_merges_node_level():
    node = component(_py_descriptor(), params = {'max_age': 5}, device_type = 'cpu')
    params = node.get_params()
    # Vendor params plus the node-level settings the reconstructed class needs.
    assert params['max_age'] == 5
    assert params['device_type'] == 'cpu' and params['nb_tasks'] == 1


def test_python_component_singleton_and_env():
    from videoflow.manifests import render_manifests
    node = component(_py_descriptor(constraints = {'singleton': True}))
    with pytest.raises(ValueError, match = 'singleton'):
        component(_py_descriptor(constraints = {'singleton': True}), nb_tasks = 4)
    specs = compile_flow(Flow([_NativeConsumer()(node(_NativeProducer()))]))
    manifests = render_manifests(specs, 'flow', 'realtime', 'nats://n:4222', 'r',
                                default_image = 'ghcr.io/videoflow/contrib-tracker-sort:0.1')
    cm = next(m for m in manifests if m['kind'] == 'ConfigMap'
            and m['data'].get('VF_NODE_NAME', '').startswith('tracker-sort'))
    # Python component: both the class (for the worker) and the component ref; wire
    # stays the default (v3) because nothing native is present.
    assert cm['data']['VF_NODE_CLASS'].endswith('KalmanFilterBoundingBoxTracker')
    assert 'VF_COMPONENT_REF' in cm['data']
    assert cm['data']['VF_ENVELOPE_VERSION'] == '3'


def _remote_flow():
    prod = _NativeProducer()
    proc = component(_descriptor(), params = {'threshold': 0.7})(prod)
    cons = _NativeConsumer()(proc)
    return Flow([cons])


def test_remote_node_compiles_to_spec():
    specs = compile_flow(_remote_flow(), envelope_version = 4)
    by_name = {s.name: s for s in specs}
    remote = next(s for s in specs if s.is_remote)
    assert remote.node_class is None
    assert remote.component_ref is not None
    assert remote.descriptor['metadata']['name'] == 'acme/thing'
    assert remote.protocol_version == 1
    assert remote.params == {'threshold': 0.7}
    # Native nodes keep their Python class and are not remote.
    natives = [s for s in specs if not s.is_remote]
    assert all(s.node_class for s in natives)
    assert has_remote_components(specs)


def test_mixed_flow_rejects_pickle_and_old_wire():
    specs = compile_flow(_remote_flow(), envelope_version = 4)
    with pytest.raises(ValueError, match = 'pickle'):
        validate_wire_compatibility(specs, 4, allow_pickle = True)
    with pytest.raises(ValueError, match = 'protobuf|envelope'):
        validate_wire_compatibility(specs, 3, allow_pickle = False)
    # v4 + no pickle is fine.
    validate_wire_compatibility(specs, 4, allow_pickle = False)


def test_compile_flow_rejects_remote_on_v3():
    with pytest.raises(ValueError, match = 'protobuf|envelope'):
        compile_flow(_remote_flow(), envelope_version = 3)


def test_render_manifests_for_remote_flow():
    pytest.importorskip('yaml')
    from videoflow.manifests import render_manifests
    specs = compile_flow(_remote_flow(), envelope_version = 4)
    manifests = render_manifests(specs, 'flow', 'realtime', 'nats://n:4222', 'run-1',
                                default_image = 'ghcr.io/acme/python-worker:1',
                                provision_image = 'videoflow-base:latest')
    configmaps = {m['metadata']['name']: m for m in manifests if m['kind'] == 'ConfigMap'}
    remote_spec = next(s for s in specs if s.is_remote)
    cm = next(m for m in configmaps.values()
            if m['data'].get('VF_NODE_NAME') == remote_spec.name)
    assert cm['data']['VF_COMPONENT_REF']
    assert 'VF_NODE_CLASS' not in cm['data']
    assert cm['data']['VF_ENVELOPE_VERSION'] == '4'    # forced by the remote component
    assert cm['data']['VF_PROTOCOL_VERSION'] == '1'
    # Native node configmaps also get v4 (whole-run homogeneity).
    native_cm = next(m for m in configmaps.values()
                    if m['data'].get('VF_NODE_NAME') and 'VF_NODE_CLASS' in m['data'])
    assert native_cm['data']['VF_ENVELOPE_VERSION'] == '4'


def test_command_override_flows_to_pod():
    desc = _descriptor(runtime = {'images': {'cpu': 'x:1'}, 'command': ['/bin/thing', '--serve']})
    prod = _NativeProducer()
    proc = component(desc)(prod)
    cons = _NativeConsumer()(proc)
    specs = compile_flow(Flow([cons]), envelope_version = 4)
    remote = next(s for s in specs if s.is_remote)
    assert remote.command == ['/bin/thing', '--serve']
    from videoflow.manifests import workload
    wl = workload(remote, 'flow', 'realtime', 'x:1', 'nats-cm')
    container = wl['spec']['template']['spec']['containers'][0]
    assert container['command'] == ['/bin/thing', '--serve']


if __name__ == '__main__':
    pytest.main([__file__])
