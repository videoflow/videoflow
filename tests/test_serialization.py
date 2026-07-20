'''
Round-trip tests for the public wire API (``encode_envelope`` / ``decode_envelope``),
which speaks the single protobuf v4 envelope. Codec-level behaviors live in
tests/test_serialization_v4.py.
'''
import numpy as np
import pytest

from videoflow.wire.serialization import (
    MSG_TYPE_DATA,
    MSG_TYPE_EOS,
    decode_envelope,
    derive_message_id,
    encode_envelope,
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

def test_heterogeneous_container_round_trip():
    # A container that mixes an ndarray with scalars/dicts encodes neutrally on the
    # v4 wire (a Value nesting a Tensor) — no code-executing codec involved.
    payload = (7, np.zeros((2, 2)), {'boxes': [1, 2, 3], 'label': 'cat'})
    buf = encode_envelope('nodeB', 'flow1', 'run1', 'trace-2', 1, MSG_TYPE_DATA, None, payload)
    out = decode_envelope(buf)
    assert out['message'][0] == 7
    assert np.array_equal(out['message'][1], payload[1])
    assert out['message'][2]['label'] == 'cat'

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

def test_legacy_msgpack_envelope_rejected():
    # The removed v2/v3 msgpack wire must be refused on decode, not silently parsed.
    import msgpack
    legacy = msgpack.packb({'v': 999, 'type': MSG_TYPE_DATA}, use_bin_type = True)
    with pytest.raises(ValueError):
        decode_envelope(legacy)

def test_event_ts_round_trip():
    ts = 1721234567.123456
    buf = encode_envelope('cam1', 'flow1', 'run1', 'cam1:1', 1, MSG_TYPE_DATA,
                        None, 'frame', event_ts = ts)
    out = decode_envelope(buf)
    assert out['event_ts'] == ts
    # Unstamped messages carry None, not a fabricated time.
    buf = encode_envelope('n', 'flow1', 'run1', 't', 1, MSG_TYPE_DATA, None, 1)
    assert decode_envelope(buf)['event_ts'] is None

if __name__ == "__main__":
    pytest.main([__file__])
