'''
Tests for component descriptor loading and validation (videoflow/component.py).
'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.component import ComponentDescriptor, load_descriptor


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
