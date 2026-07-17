import numpy as np
import pytest

from videoflow.serialization import (
    encode_envelope, decode_envelope, encode_payload, decode_payload,
    CODEC_RAW_NDARRAY, CODEC_PICKLE, CODEC_EXTERNAL_REF, MAX_INLINE_PAYLOAD_BYTES,
    BlobStore,
)

def test_ndarray_round_trip():
    arr = np.random.randint(0, 255, size = (64, 48, 3), dtype = np.uint8)
    buf = encode_envelope('nodeA', 'flow1', 'trace-1', 3, False, {'proctime': 0.1}, arr)
    out = decode_envelope(buf)
    assert np.array_equal(out['message'], arr)
    assert out['trace_id'] == 'trace-1'
    assert out['seq'] == 3
    assert out['is_stop_signal'] is False
    assert out['metadata'] == {'proctime': 0.1}

def test_arbitrary_object_round_trip():
    payload = (np.zeros((2, 2)), {'boxes': [1, 2, 3], 'label': 'cat'})
    buf = encode_envelope('nodeB', 'flow1', 'trace-2', 1, False, None, payload)
    out = decode_envelope(buf)
    assert out['message'][1]['label'] == 'cat'
    assert np.array_equal(out['message'][0], payload[0])

def test_stop_signal_round_trip():
    buf = encode_envelope('nodeA', 'flow1', '', 99, True, None, None)
    out = decode_envelope(buf)
    assert out['is_stop_signal'] is True
    assert out['message'] is None

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
