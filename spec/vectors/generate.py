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
from videoflow.backends.runtime import group_identity, replayable_trace_id, source_epoch_trace_id
from videoflow.v1 import envelope_pb2

VECTORS_DIR = os.path.dirname(os.path.abspath(__file__))
ENVELOPE_DIR = os.path.join(VECTORS_DIR, 'envelope')
REJECT_DIR = os.path.join(VECTORS_DIR, 'reject')
MSGID_DIR = os.path.join(VECTORS_DIR, 'message_id')
JOIN_DIR = os.path.join(VECTORS_DIR, 'join')

#: JOIN-23 examples: the RFC 0006 section 4 pair (same rounded time, one member
#: differs by one seq), plus a collect-free single-parent group and a Unicode one.
GROUP_CASES = [
    ({'cam': ('cam', 'cam:3f9c1a2b7d4e:5', 5), 'imu': ('imu', 'imu:77e1b2c3d4f5:11', 11)}, 1700000000.5),
    ({'cam': ('cam', 'cam:3f9c1a2b7d4e:6', 6), 'imu': ('imu', 'imu:77e1b2c3d4f5:11', 11)}, 1700000000.5),
    ({'cam': ('cam', 'cam:3f9c1a2b7d4e:1', 1)}, 0.0000005),
    ({'caméra-Ω': ('caméra-Ω', 'caméra-Ω:v2:42', 42), 'lidar': ('lidar', 'lidar:a01b7c2d9e3f:7', 7)}, 1700000001.25),
]
GROUP_EXAMPLE_ID = group_identity(GROUP_CASES[0][0], None, int(round(GROUP_CASES[0][1] * 1e6)))

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
            # EOS-7 (RFC 0006): a terminator's seq is the number of distinct DATA
            # messages its replica published — replica 0 of cam published 9.
            'name': 'eos',
            'fields': {'producer_name': 'cam', 'flow_id': 'flow-A', 'run_id': 'run-1',
                    'trace_id': 'eos-r0', 'seq': 9, 'msg_type': s.MSG_TYPE_EOS,
                    'metadata': None, 'event_ts': None, 'replica_id': 0},
            'payload': None, 'payload_desc': {'none': True},
            'description': 'replica 0 published 9 DATA messages (EOS-7: seq is the per-replica final count)',
        },
        {
            # RFC 0005: an abnormal terminator. Rides the same _eos subject as a
            # clean EOS and carries the error that killed the emitting node, so a
            # crash propagates through the graph instead of hanging its children.
            'name': 'abort',
            'fields': {'producer_name': 'detector', 'flow_id': 'flow-A', 'run_id': 'run-1',
                    'trace_id': 'abort-r2', 'seq': 11, 'msg_type': s.MSG_TYPE_ABORT,
                    'metadata': None, 'event_ts': None, 'replica_id': 2},
            'payload': None, 'payload_desc': {'none': True},
            'error': {
                'code': 'VF_DEVICE',
                'message': 'CUDA out of memory on device 0',
                'remedy': 'Lower the batch size, or grant the node more VRAM.',
                'disposition': 'worker_fatal',
                'node': 'detector',
                'trace_id': 'cam:41',
                'num_delivered': 2,
                'context': {'replica': '2'},
            },
        },
        {
            # The minimum a conformant ABORT must carry: a code and a message.
            # Everything else is optional and must round-trip as absent.
            'name': 'abort_minimal',
            'fields': {'producer_name': 'cam', 'flow_id': 'flow-A', 'run_id': 'run-1',
                    'trace_id': 'abort-r0', 'seq': 0, 'msg_type': s.MSG_TYPE_ABORT,
                    'metadata': None, 'event_ts': None, 'replica_id': 0},
            'payload': None, 'payload_desc': {'none': True},
            'error': {'code': 'VF_UNKNOWN', 'message': 'producer died'},
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
            event_ts = f['event_ts'], version = 4, error = case.get('error'),
        )
        fname = case['name'] + '.bin'
        path = os.path.join(ENVELOPE_DIR, fname)
        # A checked-in vector is the vector: its bytes are the contract other SDKs
        # replay, and protobuf serializes map entries in an unspecified order, so a
        # regenerated file can differ in bytes while meaning the same thing. Only a
        # vector that does not exist yet is written; to change one, delete it first.
        if not os.path.exists(path):
            with open(path, 'wb') as fh:
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
        if case.get('error') is not None:
            manifest[-1]['error'] = case['error']
        if case.get('description') is not None:
            manifest[-1]['description'] = case['description']
    with open(os.path.join(ENVELOPE_DIR, 'manifest.json'), 'w') as fh:
        json.dump(manifest, fh, indent = 2, ensure_ascii = False)
        fh.write('\n')

    # message-id vectors: byte-exact SHA-256-derived ids.
    msgid = []
    for args in [
        ('flow-A', 'run-1', 'cam', 'cam:1', 1, s.MSG_TYPE_DATA),
        ('flow-A', 'run-1', 'cam', 'eos-r0', 9, s.MSG_TYPE_EOS),
        # An abort's id must differ from the clean marker's, or a node that died
        # would be de-duplicated against one that finished (ABORT-1).
        ('flow-A', 'run-1', 'detector', 'abort-r2', 11, s.MSG_TYPE_ABORT),
        ('f', 'r', 'proc', 'tw-1700000000500000', 1700000000500000, s.MSG_TYPE_DATA),
        ('flujo', 'ejecución', 'caméra-Ω', 'caméra-Ω:1', 9_000_000_000, s.MSG_TYPE_DATA),
        # RFC 0006: the three trace-id forms a source mints — a live source's
        # per-process epoch (MSGID-5), a replayable source's offset and a declared
        # analysis version over it (MSGID-6) — and a member-hashed time-group id
        # (JOIN-23) carried forward by a downstream node.
        ('flow-A', 'run-1', 'cam', source_epoch_trace_id('cam', '3f9c1a2b7d4e', 1), 1, s.MSG_TYPE_DATA),
        ('flow-A', 'run-1', 'file', replayable_trace_id('file', 42), 42, s.MSG_TYPE_DATA),
        ('flow-A', 'run-1', 'file', replayable_trace_id('file', 42, 'v2'), 42, s.MSG_TYPE_DATA),
        ('f', 'r', 'proc', GROUP_EXAMPLE_ID, 1700000000500000, s.MSG_TYPE_DATA),
    ]:
        msgid.append({
            'flow_id': args[0], 'run_id': args[1], 'producer_name': args[2],
            'trace_id': args[3], 'seq': args[4], 'msg_type': args[5],
            'expected_id': s.derive_message_id(*args),
        })
    with open(os.path.join(MSGID_DIR, 'vectors.json'), 'w') as fh:
        json.dump(msgid, fh, indent = 2, ensure_ascii = False)
        fh.write('\n')

    # JOIN-23 group identities: members -> id, the rounding and the digest pinned.
    os.makedirs(JOIN_DIR, exist_ok = True)
    join = []
    for members, ts in GROUP_CASES:
        rounded = int(round(ts * 1e6))
        canonical = '|'.join(f'{parent}={producer}:{trace}:{seq}' for parent, (producer, trace, seq) in sorted(members.items()))
        join.append({'members': {p: {'producer_name': m[0], 'trace_id': m[1], 'seq': m[2]} for p, m in members.items()},
                     'group_ts': ts, 'window_id': None, 'rounded_micros': rounded, 'hash_input': '|' + canonical,
                     'expected_trace_id': group_identity(members, None, rounded)})
    with open(os.path.join(JOIN_DIR, 'group_identity.json'), 'w') as fh:
        json.dump(join, fh, indent = 2, ensure_ascii = False)
        fh.write('\n')

    reject = write_reject_vectors()

    print(f'Wrote {len(manifest)} envelope vectors, {len(reject)} reject vectors, '
          f'{len(msgid)} message-id vectors and {len(join)} group-identity vectors.')

if __name__ == '__main__':
    main()
