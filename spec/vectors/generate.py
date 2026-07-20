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
- ``reject/*.bin`` + ``reject/manifest.json`` — negative fixtures: bytes a decoder
  MUST refuse (the removed legacy msgpack wire) or MUST keep opaque without
  deserializing (an unrecognized payload_type). Each entry's ``expect`` is
  ``reject`` (decode raises) or ``opaque`` (decode returns the payload_type + bytes
  verbatim). This makes the "never execute an untrusted payload" posture a
  cross-SDK contract.

Run this and commit the result whenever the wire changes intentionally.
'''
from __future__ import annotations

import base64
import json
import os

import msgpack
import numpy as np

from videoflow import serialization as s
from videoflow.v1 import envelope_pb2

VECTORS_DIR = os.path.dirname(os.path.abspath(__file__))
ENVELOPE_DIR = os.path.join(VECTORS_DIR, 'envelope')
REJECT_DIR = os.path.join(VECTORS_DIR, 'reject')
MSGID_DIR = os.path.join(VECTORS_DIR, 'message_id')

def tensor_desc(arr : np.ndarray) -> dict:
    arr = np.ascontiguousarray(arr)
    return {'tensor': {'shape': list(arr.shape), 'dtype': str(arr.dtype),
                    'data_b64': base64.b64encode(arr.tobytes()).decode('ascii')}}

def typed(v) -> dict:
    '''A language-neutral, type-preserving description of a Value payload/metadata.'''
    if isinstance(v, np.ndarray):
        return tensor_desc(v)          # a Tensor nested inside a container (WIRE-15)
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
            # A structured container mixing an array with a scalar — the real
            # producer (frame_index, frame) shape — encoded neutrally as a Value
            # nesting a Tensor (WIRE-15). No code-executing codec is involved.
            'name': 'data_value_nested_tensor',
            'fields': {'producer_name': 'cam', 'flow_id': 'flow-A', 'run_id': 'run-1',
                    'trace_id': 'cam:1', 'seq': 2, 'msg_type': s.MSG_TYPE_DATA,
                    'metadata': {}, 'event_ts': None, 'replica_id': 0},
            'payload': (7, frame), 'payload_desc': {'value': typed((7, frame))},
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

def write_reject_vectors() -> list:
    '''
    Negative fixtures: bytes a conformant decoder MUST refuse or MUST keep opaque,
    so the security posture is a cross-SDK contract rather than a Python detail.
    Each entry's ``expect`` is either ``reject`` (decode raises) or ``opaque`` (decode
    returns the payload_type + bytes verbatim, never deserializing them).
    '''
    os.makedirs(REJECT_DIR, exist_ok = True)
    manifest = []

    # 1. A legacy msgpack envelope (the removed v2/v3 wire, which carried a
    #    code-executing payload codec). A v4-only decoder MUST refuse it.
    legacy = msgpack.packb({'v': 3, 'type': s.MSG_TYPE_DATA, 'producer_name': 'x',
                            'flow_id': 'f', 'run_id': 'r', 'trace_id': 't', 'seq': 1,
                            'payload_codec': 'raw-ndarray', 'payload': b''},
                           use_bin_type = True)
    with open(os.path.join(REJECT_DIR, 'legacy_msgpack.bin'), 'wb') as fh:
        fh.write(legacy)
    manifest.append({'name': 'legacy_msgpack', 'file': 'legacy_msgpack.bin',
                     'expect': {'reject': {'message_contains': 'msgpack'}}})

    # 2. A v4 envelope whose payload_type is not recognized (as a code-executing
    #    marker now would be): it MUST pass through opaquely, never deserialized.
    body = b'\x80\x05would-have-executed-if-deserialized'
    env = envelope_pb2.Envelope(v = 4, type = envelope_pb2.MSG_TYPE_DATA,
                                producer_name = 'attacker', flow_id = 'f', run_id = 'r',
                                trace_id = 't', seq = 1,
                                payload_type = 'vendor.acme.Unknown', payload = body)
    with open(os.path.join(REJECT_DIR, 'unknown_payload_type.bin'), 'wb') as fh:
        fh.write(env.SerializeToString())
    manifest.append({'name': 'unknown_payload_type', 'file': 'unknown_payload_type.bin',
                     'expect': {'opaque': {'payload_type': 'vendor.acme.Unknown',
                                           'data_b64': base64.b64encode(body).decode('ascii')}}})

    with open(os.path.join(REJECT_DIR, 'manifest.json'), 'w') as fh:
        json.dump(manifest, fh, indent = 2, ensure_ascii = False)
        fh.write('\n')
    return manifest

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

    reject = write_reject_vectors()

    print(f'Wrote {len(manifest)} envelope vectors, {len(reject)} reject vectors, '
          f'and {len(msgid)} message-id vectors.')

if __name__ == '__main__':
    main()
