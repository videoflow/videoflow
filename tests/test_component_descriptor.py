'''
Tests for component descriptor loading and validation (videoflow/component.py).
'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.components.descriptor import ComponentDescriptor, load_descriptor


def _desc(**overrides):
    d = {
        'apiVersion': 'videoflow.io/v1',
        'kind': 'Component',
        'metadata': {'name': 'acme/sort', 'version': '1.0.0', 'license': 'MIT'},
        'spec': {
            'role': 'processor',
            'protocol': 1,
            'runtime': {'images': {'cpu': 'ghcr.io/acme/sort:1.0.0', 'gpu': 'ghcr.io/acme/sort:1.0.0-cuda'}},
            'device': ['cpu', 'gpu'],
            'params': {'schema': {
                'type': 'object',
                'required': ['model'],
                'additionalProperties': False,
                'properties': {
                    'model': {'type': 'string', 'enum': ['a', 'b']},
                    'iou_threshold': {'type': 'number', 'default': 0.3, 'minimum': 0, 'maximum': 1},
                },
            }},
            'io': {'inputs': [{'name': 'detections', 'type': 'videoflow.v1.Tensor'}],
                'output': {'type': 'videoflow.v1.Tensor'}},
            'constraints': {'partitionable': True},
        },
    }
    d['spec'].update(overrides)
    return d


def test_descriptor_fields_parse():
    d = ComponentDescriptor.from_dict(_desc())
    assert d.name == 'acme/sort' and d.version == '1.0.0' and d.role == 'processor'
    assert d.protocol == 1 and d.device == ['cpu', 'gpu']
    assert d.image_for('cpu') == 'ghcr.io/acme/sort:1.0.0'
    assert d.image_for('gpu').endswith('-cuda') and d.image_for('tpu') is None
    assert d.partitionable is True and d.singleton is False


def test_validate_params_fills_defaults():
    d = ComponentDescriptor.from_dict(_desc())
    out = d.validate_params({'model': 'a'})
    assert out == {'model': 'a', 'iou_threshold': 0.3}


def test_validate_params_missing_required():
    d = ComponentDescriptor.from_dict(_desc())
    with pytest.raises(ValueError, match = 'model'):
        d.validate_params({'iou_threshold': 0.5})


def test_validate_params_enum_and_type():
    d = ComponentDescriptor.from_dict(_desc())
    with pytest.raises(ValueError):
        d.validate_params({'model': 'nope'})           # not in enum
    with pytest.raises(ValueError):
        d.validate_params({'model': 'a', 'iou_threshold': 'high'})  # wrong type
    with pytest.raises(ValueError):
        d.validate_params({'model': 'a', 'iou_threshold': 5})       # > maximum


def test_bad_descriptor_shapes_rejected():
    for mutate in [
        lambda d: d.update({'apiVersion': 'wrong'}),
        lambda d: d.update({'kind': 'Nope'}),
        lambda d: d['spec'].update({'role': 'sink'}),
        lambda d: d['spec'].update({'protocol': 0}),
        lambda d: d['spec']['runtime'].update({'images': {}}),
        lambda d: d['spec'].update({'device': ['tpu']}),
        lambda d: d['metadata'].pop('version'),
    ]:
        d = _desc()
        mutate(d)
        with pytest.raises(ValueError):
            ComponentDescriptor.from_dict(d)


def test_descriptor_resources_gpu_parses_count():
    d = ComponentDescriptor.from_dict(_desc(resources = {'gpu': {'count': 2}}))
    assert d.gpu_count == 2


def test_descriptor_without_resources_defaults_to_one_gpu():
    d = ComponentDescriptor.from_dict(_desc())
    assert d.gpu_count == 1


def test_descriptor_rejects_bad_gpu_resources():
    for resources in [
        {'gpu': {'count': 0}},
        {'gpu': {'count': 1.5}},
        {'gpu': {'count': True}},
    ]:
        with pytest.raises(ValueError, match = 'resources.gpu'):
            ComponentDescriptor.from_dict(_desc(resources = resources))


def test_descriptor_resources_gpu_parses_memory_gib():
    d = ComponentDescriptor.from_dict(_desc(resources = {'gpu': {'memoryGiB': 20}}))
    assert d.gpu_memory_gib == 20
    assert ComponentDescriptor.from_dict(_desc()).gpu_memory_gib is None


def test_descriptor_rejects_bad_memory_gib():
    for gpu in [{'memoryGiB': 0}, {'memoryGiB': -1}, {'memoryGiB': True}, {'memoryGiB': 'lots'}]:
        with pytest.raises(ValueError, match = 'memoryGiB'):
            ComponentDescriptor.from_dict(_desc(resources = {'gpu': gpu}))
    # A memory demand and a multi-device span are mutually exclusive (RFC 0004).
    with pytest.raises(ValueError, match = 'mutually exclusive'):
        ComponentDescriptor.from_dict(_desc(resources = {'gpu': {'count': 2, 'memoryGiB': 20}}))
    # ...and a memory demand needs the gpu device declared.
    with pytest.raises(ValueError, match = "requires 'gpu'"):
        ComponentDescriptor.from_dict(_desc(device = ['cpu'], resources = {'gpu': {'memoryGiB': 20}}))


def test_descriptor_rejects_gpu_resources_on_non_processor_roles():
    # Bug 5 regression: a producer/consumer descriptor declaring a GPU need used
    # to validate cleanly and mean nothing — an underprovisioned pod waiting to
    # happen. Now it is a load-time error.
    for role in ('producer', 'consumer'):
        with pytest.raises(ValueError, match = 'processor components only'):
            ComponentDescriptor.from_dict(_desc(role = role,
                                                resources = {'gpu': {'count': 2}}))


def test_descriptor_rejects_multi_gpu_without_gpu_device():
    with pytest.raises(ValueError, match = "requires 'gpu' in spec.device"):
        ComponentDescriptor.from_dict(_desc(device = ['cpu'], resources = {'gpu': {'count': 2}}))


def test_input_accepts_defaults_false():
    d = ComponentDescriptor.from_dict(_desc(io = {
        'inputs': [
            {'name': 'a', 'type': 'any'},
            {'name': 'b', 'type': 'any', 'accepts': {'missing': True, 'collected': True}},
        ]}))
    assert d.input_accepts(0, 'missing') is False
    assert d.input_accepts(1, 'missing') is True and d.input_accepts(1, 'collected') is True
    assert d.all_inputs_accept('missing') is False


def test_load_descriptor_from_file(tmp_path):
    import yaml
    p = tmp_path / 'component.yaml'
    p.write_text(yaml.dump(_desc()))
    d = load_descriptor(str(tmp_path))       # directory form
    assert d.name == 'acme/sort' and d.source.endswith('component.yaml')
    d2 = load_descriptor(str(p))             # file form
    assert d2.name == 'acme/sort'


if __name__ == '__main__':
    pytest.main([__file__])
