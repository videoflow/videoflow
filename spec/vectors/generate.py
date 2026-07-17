'''
Regenerate the golden wire vectors checked in under spec/vectors/.

    python spec/vectors/generate.py

These are the language-neutral fixtures every SDK must agree on:

- ``envelope/*.bin`` + ``envelope/manifest.json`` — frozen v4 (protobuf) envelope
  bytes plus a typed description of the fields they must decode to. A conformance
  run in any language decodes each .bin and checks the fields match (decode-and-
  compare, since protobuf encoding is not canonical). The typed value scheme
  preserves the int-vs-double distinction (PROTOCOL.md WIRE-12).
- ``message_id/vectors.json`` — input tuples and their expected 32-hex
  ``derive_message_id`` output. This IS byte-exact across languages (it is a
  SHA-256 over a fixed string) and is the primary dedup anchor.

Run this and commit the result whenever the wire changes intentionally.
'''
from __future__ import annotations

import base64
import json
import os

import numpy as np

from videoflow import serialization as s

VECTORS_DIR = os.path.dirname(os.path.abspath(__file__))
ENVELOPE_DIR = os.path.join(VECTORS_DIR, 'envelope')
MSGID_DIR = os.path.join(VECTORS_DIR, 'message_id')

def tensor_desc(arr : np.ndarray) -> dict:
    arr = np.ascontiguousarray(arr)
    return {'tensor': {'shape': list(arr.shape), 'dtype': str(arr.dtype),
                    'data_b64': base64.b64encode(arr.tobytes()).decode('ascii')}}

def typed(v) -> dict:
    '''A language-neutral, type-preserving description of a Value payload/metadata.'''
    if v is None:
        return {'null': True}
    if isinstance(v, bool):
        return {'bool': v}
    if isinstance(v, int):
        return {'i': v}
    if isinstance(v, float):
        return {'d': v}
    if isinstance(v, str):
        return {'s': v}
    if isinstance(v, (bytes, bytearray)):
        return {'b64': base64.b64encode(bytes(v)).decode('ascii')}
    if isinstance(v, (list, tuple)):
        return {'list': [typed(x) for x in v]}
    if isinstance(v, dict):
        return {'map': {k: typed(val) for k, val in v.items()}}
    raise TypeError(f'unsupported typed value: {type(v)}')

def build_cases() -> list:
    frame = np.arange(2 * 3 * 3, dtype = np.uint8).reshape(2, 3, 3)  # tiny BGR frame
    dets = np.array([[10, 20, 30, 40, 0.9, 1.0]], dtype = np.float32)
    value_map = {'count': 7, 'ratio': 1.5, 'flag': True, 'label': 'car', 'nothing': None}
    value_list = [1, 2.0, 'three', False, None]
    return [
        {
            'name': 'data_tensor_frame',
            'fields': {'producer_name': 'cam', 'flow_id': 'flow-A', 'run_id': 'run-1',
                    'trace_id': 'cam:1', 'seq': 1, 'msg_type': s.MSG_TYPE_DATA,
                    'metadata': {'proctime': 0.25}, 'event_ts': 1_700_000_000.5,
                    'replica_id': 0},
            'payload': frame, 'payload_desc': tensor_desc(frame),
        },
        {
            'name': 'data_tensor_detections',
            'fields': {'producer_name': 'detector', 'flow_id': 'flow-A', 'run_id': 'run-1',
                    'trace_id': 'cam:1', 'seq': 1, 'msg_type': s.MSG_TYPE_DATA,
                    'metadata': {}, 'event_ts': 1_700_000_000.5, 'replica_id': 2},
            'payload': dets, 'payload_desc': tensor_desc(dets),
        },
        {
            'name': 'data_value_map',
            'fields': {'producer_name': 'meta', 'flow_id': 'f', 'run_id': 'r',
                    'trace_id': 't', 'seq': 42, 'msg_type': s.MSG_TYPE_DATA,
                    'metadata': {'proctime': 0.1, 'actual_proctime': 0.2},
                    'event_ts': None, 'replica_id': 0},
            'payload': value_map, 'payload_desc': {'value': typed(value_map)},
        },
        {
            'name': 'data_value_list',
            'fields': {'producer_name': 'meta', 'flow_id': 'f', 'run_id': 'r',
                    'trace_id': 't', 'seq': 43, 'msg_type': s.MSG_TYPE_DATA,
                    'metadata': {}, 'event_ts': None, 'replica_id': 0},
            'payload': value_list, 'payload_desc': {'value': typed(value_list)},
        },
        {
            'name': 'eos',
            'fields': {'producer_name': 'cam', 'flow_id': 'flow-A', 'run_id': 'run-1',
                    'trace_id': 'eos-r0', 'seq': 9, 'msg_type': s.MSG_TYPE_EOS,
                    'metadata': None, 'event_ts': None, 'replica_id': 0},
            'payload': None, 'payload_desc': {'none': True},
        },
        {
            'name': 'unicode_names_big_seq',
            'fields': {'producer_name': 'caméra-Ω', 'flow_id': 'flujo', 'run_id': 'ejecución',
                    'trace_id': 'caméra-Ω:1', 'seq': 9_000_000_000, 'msg_type': s.MSG_TYPE_DATA,
                    'metadata': {'note': 'ünïcödé'}, 'event_ts': 1.5, 'replica_id': 7},
            'payload': np.array([1, 2, 3], dtype = np.int64),
            'payload_desc': tensor_desc(np.array([1, 2, 3], dtype = np.int64)),
        },
    ]

def main() -> None:
    os.makedirs(ENVELOPE_DIR, exist_ok = True)
    os.makedirs(MSGID_DIR, exist_ok = True)

    manifest = []
    for case in build_cases():
        f = case['fields']
        buf = s.encode_envelope(
            f['producer_name'], f['flow_id'], f['run_id'], f['trace_id'], f['seq'],
            f['msg_type'], f['metadata'], case['payload'], replica_id = f['replica_id'],
            event_ts = f['event_ts'], version = 4,
        )
        fname = case['name'] + '.bin'
        with open(os.path.join(ENVELOPE_DIR, fname), 'wb') as fh:
            fh.write(buf)
        manifest.append({
            'name': case['name'],
            'file': fname,
            'version': 4,
            'type': f['msg_type'],
            'producer_name': f['producer_name'],
            'flow_id': f['flow_id'],
            'run_id': f['run_id'],
            'trace_id': f['trace_id'],
            'seq': f['seq'],
            'event_ts': f['event_ts'],
            'replica_id': f['replica_id'],
            'metadata': typed(f['metadata'] or {}),
            'payload': case['payload_desc'],
        })
    with open(os.path.join(ENVELOPE_DIR, 'manifest.json'), 'w') as fh:
        json.dump(manifest, fh, indent = 2, ensure_ascii = False)
        fh.write('\n')

    # message-id vectors: byte-exact SHA-256-derived ids.
    msgid = []
    for args in [
        ('flow-A', 'run-1', 'cam', 'cam:1', 1, s.MSG_TYPE_DATA),
        ('flow-A', 'run-1', 'cam', 'eos-r0', 9, s.MSG_TYPE_EOS),
        ('f', 'r', 'proc', 'tw-1700000000500000', 1700000000500000, s.MSG_TYPE_DATA),
        ('flujo', 'ejecución', 'caméra-Ω', 'caméra-Ω:1', 9_000_000_000, s.MSG_TYPE_DATA),
    ]:
        msgid.append({
            'flow_id': args[0], 'run_id': args[1], 'producer_name': args[2],
            'trace_id': args[3], 'seq': args[4], 'msg_type': args[5],
            'expected_id': s.derive_message_id(*args),
        })
    with open(os.path.join(MSGID_DIR, 'vectors.json'), 'w') as fh:
        json.dump(msgid, fh, indent = 2, ensure_ascii = False)
        fh.write('\n')

    print(f'Wrote {len(manifest)} envelope vectors and {len(msgid)} message-id vectors.')

if __name__ == '__main__':
    main()
