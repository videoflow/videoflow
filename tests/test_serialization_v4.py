'''
Unit tests for the v4 (protobuf) wire codec in videoflow.wire.serialization — the
single, language-neutral wire. Covers the codec behaviors an SDK must reproduce:
tensor/value/proto payload selection, a Value nesting a Tensor (mixed containers),
the int-vs-double distinction, EOS, event_ts presence, blob offload, opaque
passthrough of unknown types (so a code-executing payload can never run on decode),
version selection, and refusal of the removed legacy msgpack wire.
'''
from __future__ import absolute_import, division, print_function

import numpy as np
import pytest

from videoflow.v1 import payloads_pb2
from videoflow.wire import serialization as s


def _rt(payload, *, metadata = None, msg_type = None, event_ts = None,
        version = 4, blob_store = None):
    '''Encode then decode one envelope; return the decoded dict.'''
    buf = s.encode_envelope('node', 'flow', 'run', 'trace', 7,
                            msg_type or s.MSG_TYPE_DATA, metadata or {}, payload,
                            event_ts = event_ts, version = version,
                            blob_store = blob_store)
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


class _Unencodable:
    '''A custom object with no neutral (ndarray/proto/Value) encoding and no registered encoder.'''
    def __init__(self, x = 5):
        self.x = x


def test_unencodable_object_raises_with_no_escape_hatch():
    # There is no code-executing fallback: an object the wire can't encode is a hard
    # TypeError, and the message points at register_payload_encoder (never pickle).
    with pytest.raises(TypeError) as ei:
        _rt(_Unencodable())
    msg = str(ei.value)
    assert 'register_payload_encoder' in msg
    assert 'pickle' not in msg.lower()


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


def test_nested_tensor_container_roundtrips():
    # The real producer shape: a (frame_index, frame) tuple, plus a dict/list mixing
    # arrays with scalars. All encode neutrally (Value nesting a Tensor), no pickle.
    frame = np.arange(24, dtype = np.uint8).reshape(2, 3, 4)
    d = _rt((7, frame))
    assert d['message'][0] == 7 and np.array_equal(d['message'][1], frame)
    d = _rt({'frame': frame, 'n': 3, 'labels': ['a', 'b']})
    assert d['message']['n'] == 3 and d['message']['labels'] == ['a', 'b']
    assert np.array_equal(d['message']['frame'], frame)
    d = _rt([frame, frame])
    assert all(np.array_equal(x, frame) for x in d['message'])


def test_ndarray_in_metadata_is_rejected():
    # Metadata is for small scalars/strings, not payload arrays.
    with pytest.raises(TypeError, match = 'metadata'):
        _rt(np.zeros(1), metadata = {'bad': np.zeros(3)})


def test_arbitrary_payload_type_is_inert_on_decode():
    # Security regression: a message whose payload_type is unknown (as a legacy
    # code-executing marker now would be) decodes to opaque bytes and is never
    # deserialized/executed — the decoder hands back exactly what came in.
    from videoflow.v1 import envelope_pb2
    marker = 'x-python-' + 'pickle'  # the historically dangerous marker, now just unknown
    body = b'\x80\x05would-have-executed'
    env = envelope_pb2.Envelope(v = 4, type = envelope_pb2.MSG_TYPE_DATA,
                                producer_name = 'attacker', flow_id = 'f', run_id = 'r',
                                trace_id = 't', seq = 1, payload_type = marker, payload = body)
    d = s.decode_envelope(env.SerializeToString())
    assert isinstance(d['message'], s.RawPayload)
    assert d['message'].payload_type == marker and d['message'].data == body


def test_v4_leading_byte_is_not_a_msgpack_map():
    buf = s.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {}, np.zeros(1), version = 4)
    assert not s._is_msgpack_map(buf[0])


def test_legacy_msgpack_envelope_refused():
    # The removed v2/v3 msgpack wire must be refused on decode, not silently parsed.
    import msgpack
    legacy = msgpack.packb({'v': 3, 'type': s.MSG_TYPE_DATA}, use_bin_type = True)
    with pytest.raises(ValueError, match = 'msgpack'):
        s.decode_envelope(legacy)


def test_unsupported_emit_version_rejected():
    with pytest.raises(ValueError, match = 'emit'):
        s.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {}, np.zeros(1), version = 3)
    with pytest.raises(ValueError, match = 'emit'):
        s.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {}, np.zeros(1), version = 99)


def test_v4_decode_rejects_wrong_version_field():
    from videoflow.v1 import envelope_pb2
    env = envelope_pb2.Envelope(v = 7, type = envelope_pb2.MSG_TYPE_DATA, payload_type = s.PAYLOAD_VALUE)
    with pytest.raises(ValueError, match = 'version'):
        s._decode_envelope_v4(env.SerializeToString())


if __name__ == '__main__':
    pytest.main([__file__])
