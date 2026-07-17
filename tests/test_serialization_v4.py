'''
Unit tests for the v4 (protobuf) wire codec in videoflow.serialization, alongside
the untouched v2/v3 msgpack path (tests/test_serialization.py). Covers the codec
behaviors an SDK must reproduce: tensor/value/proto/pickle payload selection,
the int-vs-double distinction, EOS, event_ts presence, blob offload, opaque
passthrough of unknown types, version selection, and the decode auto-discriminator.
'''
from __future__ import absolute_import, division, print_function

import numpy as np
import pytest

from videoflow import serialization as s
from videoflow.v1 import payloads_pb2


def _rt(payload, *, metadata = None, msg_type = None, event_ts = None,
        version = 4, allow_pickle = False, blob_store = None):
    '''Encode then decode one envelope; return the decoded dict.'''
    buf = s.encode_envelope('node', 'flow', 'run', 'trace', 7,
                            msg_type or s.MSG_TYPE_DATA, metadata or {}, payload,
                            event_ts = event_ts, version = version,
                            allow_pickle = allow_pickle, blob_store = blob_store)
    return s.decode_envelope(buf, blob_store = blob_store)


def test_ndarray_payload_roundtrips_as_tensor():
    arr = np.arange(24, dtype = np.float32).reshape(2, 3, 4)
    d = _rt(arr)
    assert isinstance(d['message'], np.ndarray)
    assert d['message'].dtype == np.float32 and d['message'].shape == (2, 3, 4)
    assert np.array_equal(d['message'], arr)


def test_value_preserves_int_vs_double():
    d = _rt({'count': 7, 'ratio': 1.5, 'flag': True, 'name': 'x', 'empty': None})
    msg = d['message']
    assert msg == {'count': 7, 'ratio': 1.5, 'flag': True, 'name': 'x', 'empty': None}
    assert type(msg['count']) is int and type(msg['ratio']) is float
    assert type(msg['flag']) is bool and msg['empty'] is None


def test_value_list_and_bytes():
    d = _rt([1, 2.0, b'\x00\xff', 'z', [True, None]])
    assert d['message'] == [1, 2.0, b'\x00\xff', 'z', [True, None]]
    assert type(d['message'][0]) is int and type(d['message'][1]) is float


def test_metadata_int_vs_double_distinct():
    d = _rt(np.zeros(1), metadata = {'seqno': 42, 'proctime': 0.5})
    assert d['metadata'] == {'seqno': 42, 'proctime': 0.5}
    assert type(d['metadata']['seqno']) is int and type(d['metadata']['proctime']) is float


def test_eos_has_no_payload():
    d = _rt(None, msg_type = s.MSG_TYPE_EOS)
    assert d['is_stop_signal'] and d['type'] == s.MSG_TYPE_EOS and d['message'] is None


def test_event_ts_presence_roundtrips():
    assert _rt(np.zeros(1), event_ts = None)['event_ts'] is None
    assert _rt(np.zeros(1), event_ts = 1700000000.5)['event_ts'] == 1700000000.5


def test_well_known_proto_payload_roundtrips():
    det = payloads_pb2.Detections(
        boxes = payloads_pb2.Tensor(shape = [1, 6], dtype = 'float32',
                                    data = np.zeros((1, 6), np.float32).tobytes()),
        class_names = ['car', 'person'])
    d = _rt(det)
    assert isinstance(d['message'], payloads_pb2.Detections)
    assert list(d['message'].class_names) == ['car', 'person']


def test_unknown_payload_type_passes_through_opaquely():
    raw = s.RawPayload('vendor.acme.Widget', b'\x01\x02\x03')
    d = _rt(raw)
    assert isinstance(d['message'], s.RawPayload)
    assert d['message'].payload_type == 'vendor.acme.Widget'
    assert d['message'].data == b'\x01\x02\x03'


class _PickleOnly:
    '''Module-level (so it is picklable) and not ndarray/proto/Value-encodable.'''
    def __init__(self, x = 5):
        self.x = x

    def __eq__(self, other):
        return isinstance(other, _PickleOnly) and other.x == self.x


def test_pickle_is_gated():
    with pytest.raises(TypeError, match = 'pickle'):
        _rt(_PickleOnly(), allow_pickle = False)
    # With the gate open it round-trips (legacy Python-only path).
    d = _rt(_PickleOnly(9), allow_pickle = True)
    assert isinstance(d['message'], _PickleOnly) and d['message'].x == 9


class _FakeBlobStore(s.BlobStore):
    def __init__(self):
        self._d = {}

    def put(self, data, ttl_seconds = 3600):
        import uuid
        key = 'vf-blob-' + uuid.uuid4().hex
        self._d[key] = data
        return key

    def get(self, ref):
        return self._d[ref]


def test_large_tensor_offloads_to_blob_and_resolves():
    big = np.zeros((512, 1024), dtype = np.uint8)  # 512KiB, over the inline threshold
    store = _FakeBlobStore()
    buf = s.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {}, big,
                            version = 4, blob_store = store)
    assert len(store._d) == 1
    # The inline envelope carries only a small BlobRef, not the payload.
    assert len(buf) < 1024
    d = s.decode_envelope(buf, blob_store = store)
    assert np.array_equal(d['message'], big)


def test_large_payload_without_blob_store_raises():
    big = np.zeros((512, 1024), dtype = np.uint8)
    with pytest.raises(ValueError, match = 'MAX_INLINE_PAYLOAD_BYTES'):
        s.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {}, big, version = 4)


def test_decode_auto_discriminates_v3_and_v4():
    arr = np.arange(6, dtype = np.int16)
    for version in (3, 4):
        d = _rt(arr, version = version)
        assert np.array_equal(d['message'], arr)


def test_v4_leading_byte_is_not_a_msgpack_map():
    buf = s.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {}, np.zeros(1), version = 4)
    assert not s._is_msgpack_map(buf[0])


def test_unsupported_emit_version_rejected():
    with pytest.raises(ValueError, match = 'emit'):
        s.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {}, np.zeros(1), version = 99)


def test_v4_decode_rejects_wrong_version_field():
    from videoflow.v1 import envelope_pb2
    env = envelope_pb2.Envelope(v = 7, type = envelope_pb2.MSG_TYPE_DATA, payload_type = s.PAYLOAD_VALUE)
    with pytest.raises(ValueError, match = 'version'):
        s._decode_envelope_v4(env.SerializeToString())


if __name__ == '__main__':
    pytest.main([__file__])
