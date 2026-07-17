import numpy as np
import pytest

from videoflow.serialization import (
    CODEC_EXTERNAL_REF,
    CODEC_PICKLE,
    CODEC_RAW_NDARRAY,
    MAX_INLINE_PAYLOAD_BYTES,
    MSG_TYPE_DATA,
    MSG_TYPE_EOS,
    BlobStore,
    decode_envelope,
    decode_payload,
    derive_message_id,
    encode_envelope,
    encode_payload,
)


def test_ndarray_round_trip():
    arr = np.random.randint(0, 255, size = (64, 48, 3), dtype = np.uint8)
    buf = encode_envelope('nodeA', 'flow1', 'run1', 'trace-1', 3, MSG_TYPE_DATA,
                        {'proctime': 0.1}, arr, replica_id = 2)
    out = decode_envelope(buf)
    assert np.array_equal(out['message'], arr)
    assert out['run_id'] == 'run1'
    assert out['trace_id'] == 'trace-1'
    assert out['seq'] == 3
    assert out['type'] == MSG_TYPE_DATA
    assert out['is_stop_signal'] is False
    assert out['replica_id'] == 2
    assert out['metadata'] == {'proctime': 0.1}

def test_arbitrary_object_round_trip():
    payload = (np.zeros((2, 2)), {'boxes': [1, 2, 3], 'label': 'cat'})
    buf = encode_envelope('nodeB', 'flow1', 'run1', 'trace-2', 1, MSG_TYPE_DATA, None, payload)
    out = decode_envelope(buf)
    assert out['message'][1]['label'] == 'cat'
    assert np.array_equal(out['message'][0], payload[0])

def test_stop_signal_round_trip():
    buf = encode_envelope('nodeA', 'flow1', 'run1', '', 99, MSG_TYPE_EOS, None, None)
    out = decode_envelope(buf)
    assert out['type'] == MSG_TYPE_EOS
    assert out['is_stop_signal'] is True
    assert out['message'] is None

def test_message_id_is_deterministic_and_type_sensitive():
    a = derive_message_id('f', 'r', 'node', 'trace-1', 5, MSG_TYPE_DATA)
    b = derive_message_id('f', 'r', 'node', 'trace-1', 5, MSG_TYPE_DATA)
    c = derive_message_id('f', 'r', 'node', 'trace-1', 6, MSG_TYPE_DATA)
    d = derive_message_id('f', 'r', 'node', 'trace-1', 5, MSG_TYPE_EOS)
    assert a == b            # same inputs → same id (enables dedup across retries)
    assert a != c and a != d  # different seq / type → different id
    assert len(a) == 32

def test_wrong_version_rejected():
    import msgpack
    bad = msgpack.packb({'v': 999, 'type': MSG_TYPE_DATA}, use_bin_type = True)
    with pytest.raises(ValueError):
        decode_envelope(bad)

def test_event_ts_round_trip():
    ts = 1721234567.123456
    buf = encode_envelope('cam1', 'flow1', 'run1', 'cam1:1', 1, MSG_TYPE_DATA,
                        None, 'frame', event_ts = ts)
    out = decode_envelope(buf)
    assert out['event_ts'] == ts
    # Unstamped messages carry None, not a fabricated time.
    buf = encode_envelope('n', 'flow1', 'run1', 't', 1, MSG_TYPE_DATA, None, 1)
    assert decode_envelope(buf)['event_ts'] is None

def test_v2_envelope_decodes_without_event_ts():
    # A v2 (pre-event_ts) envelope from an older build still decodes.
    import msgpack
    v2 = msgpack.packb({
        'v': 2, 'type': MSG_TYPE_DATA, 'producer_name': 'n', 'flow_id': 'f',
        'run_id': 'r', 'trace_id': 't', 'seq': 1, 'span_id': '',
        'parent_span_id': '', 'replica_id': 0, 'metadata': None,
        'payload_codec': CODEC_PICKLE, 'payload': __import__('pickle').dumps(7),
    }, use_bin_type = True)
    out = decode_envelope(v2)
    assert out['message'] == 7
    assert out['event_ts'] is None

def test_codec_selection():
    codec, _ = encode_payload(np.zeros((3, 3)))
    assert codec == CODEC_RAW_NDARRAY
    codec, _ = encode_payload({'a': 1})
    assert codec == CODEC_PICKLE

class _FakeBlobStore(BlobStore):
    def __init__(self):
        self._store = {}

    def put(self, data, ttl_seconds = 3600):
        ref = f'ref-{len(self._store)}'
        self._store[ref] = data
        return ref

    def get(self, ref):
        return self._store[ref]

def test_large_payload_uses_blob_store():
    big = np.zeros((1024, 1024), dtype = np.uint8)  # 1MB, over the default 512KB threshold
    assert big.nbytes > MAX_INLINE_PAYLOAD_BYTES
    blob_store = _FakeBlobStore()
    codec, buf = encode_payload(big, blob_store = blob_store)
    assert codec == CODEC_EXTERNAL_REF
    assert len(blob_store._store) == 1
    decoded = decode_payload(codec, buf, blob_store = blob_store)
    assert np.array_equal(decoded, big)

def test_large_payload_without_blob_store_raises():
    big = np.zeros((1024, 1024), dtype = np.uint8)
    with pytest.raises(ValueError):
        encode_payload(big)

if __name__ == "__main__":
    pytest.main([__file__])
