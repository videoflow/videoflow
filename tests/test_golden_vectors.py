'''
Replays the checked-in golden wire vectors (spec/vectors/) against the Python
codec. These are the language-neutral fixtures every SDK must agree on: the same
.bin envelope bytes must decode to the same fields, and derive_message_id must
produce the same 32-hex ids. When a non-Python SDK lands (Phase 3), it replays the
exact same files — this test proves the reference implementation honors them.
'''
from __future__ import absolute_import, division, print_function

import base64
import json
import os

import numpy as np
import pytest

from videoflow import serialization as s

VECTORS = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'spec', 'vectors')

def _from_typed(t : dict):
    '''Inverse of spec/vectors/generate.py::typed — reconstruct a python value.'''
    if 'null' in t:
        return None
    if 'bool' in t:
        return t['bool']
    if 'i' in t:
        return t['i']
    if 'd' in t:
        return t['d']
    if 's' in t:
        return t['s']
    if 'b64' in t:
        return base64.b64decode(t['b64'])
    if 'list' in t:
        return [_from_typed(x) for x in t['list']]
    if 'map' in t:
        return {k: _from_typed(v) for k, v in t['map'].items()}
    raise ValueError(f'bad typed value: {t}')

def _assert_typed_equal(actual, typed_desc : dict) -> None:
    '''Compare a decoded value to a typed descriptor, enforcing int-vs-float type.'''
    if 'i' in typed_desc:
        assert isinstance(actual, int) and not isinstance(actual, bool)
        assert actual == typed_desc['i']
    elif 'd' in typed_desc:
        assert isinstance(actual, float) and actual == typed_desc['d']
    elif 'bool' in typed_desc:
        assert isinstance(actual, bool) and actual == typed_desc['bool']
    elif 'null' in typed_desc:
        assert actual is None
    elif 's' in typed_desc:
        assert actual == typed_desc['s']
    elif 'b64' in typed_desc:
        assert actual == base64.b64decode(typed_desc['b64'])
    elif 'list' in typed_desc:
        assert len(actual) == len(typed_desc['list'])
        for a, t in zip(actual, typed_desc['list']):
            _assert_typed_equal(a, t)
    elif 'map' in typed_desc:
        assert set(actual.keys()) == set(typed_desc['map'].keys())
        for k, t in typed_desc['map'].items():
            _assert_typed_equal(actual[k], t)
    else:
        raise ValueError(f'bad typed descriptor: {typed_desc}')

def _load_manifest():
    with open(os.path.join(VECTORS, 'envelope', 'manifest.json')) as f:
        return json.load(f)

@pytest.mark.parametrize('case', _load_manifest(), ids = lambda c: c['name'])
def test_envelope_vector_decodes_to_expected_fields(case):
    with open(os.path.join(VECTORS, 'envelope', case['file']), 'rb') as f:
        buf = f.read()
    d = s.decode_envelope(buf)

    assert d['type'] == case['type']
    assert d['producer_name'] == case['producer_name']
    assert d['flow_id'] == case['flow_id']
    assert d['run_id'] == case['run_id']
    assert d['trace_id'] == case['trace_id']
    assert d['seq'] == case['seq']
    assert d['event_ts'] == case['event_ts']
    assert d['replica_id'] == case['replica_id']
    _assert_typed_equal(d['metadata'], case['metadata'])

    payload = case['payload']
    if 'none' in payload:
        assert d['message'] is None
    elif 'tensor' in payload:
        t = payload['tensor']
        expected = np.frombuffer(base64.b64decode(t['data_b64']), dtype = np.dtype(t['dtype'])).reshape(t['shape'])
        assert isinstance(d['message'], np.ndarray)
        assert d['message'].dtype == np.dtype(t['dtype'])
        assert list(d['message'].shape) == list(t['shape'])
        assert np.array_equal(d['message'], expected)
    elif 'value' in payload:
        _assert_typed_equal(d['message'], payload['value'])
    else:
        raise AssertionError(f'unknown payload descriptor: {payload}')

def test_envelope_vectors_are_v4_protobuf():
    # Every envelope vector is protobuf on the wire (leading byte is a field tag,
    # not a msgpack map header) — the discriminator decode_envelope relies on.
    for case in _load_manifest():
        with open(os.path.join(VECTORS, 'envelope', case['file']), 'rb') as f:
            first = f.read(1)[0]
        assert not s._is_msgpack_map(first), case['name']

def _load_msgid():
    with open(os.path.join(VECTORS, 'message_id', 'vectors.json')) as f:
        return json.load(f)

@pytest.mark.parametrize('vec', _load_msgid(), ids = lambda v: v['trace_id'])
def test_message_id_vector(vec):
    got = s.derive_message_id(vec['flow_id'], vec['run_id'], vec['producer_name'],
                            vec['trace_id'], vec['seq'], vec['msg_type'])
    assert got == vec['expected_id']
    assert len(got) == 32

if __name__ == '__main__':
    pytest.main([__file__])
