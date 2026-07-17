'''
Wire format used to move messages between nodes over the message broker
(``videoflow.messaging.nats_messenger.NATSMessenger``). Not used by anything
running purely in a single local process — it's the boundary format for bytes
that cross a network/process boundary.

Two envelope encodings coexist during the migration to a language-agnostic wire
(see ``spec/PROTOCOL.md`` §4 and the migration plan):

- **v2/v3 — msgpack** (legacy): a msgpack map with a ``pickle`` fallback for any
  non-ndarray payload. Python-only on the wire. Retained for decode compatibility
  and still the default *emit* version during the bake period.
- **v4 — protobuf** (``videoflow.v1.Envelope``): a language-neutral envelope whose
  payload is a typed protobuf message — ``Tensor`` for arrays (incl. video frames),
  ``Value`` for structured scalars/maps/lists, any vendor proto by its FQN, or the
  gated ``x-python-pickle`` legacy codec. This is what non-Python SDKs implement.

``encode_envelope`` picks the version (default ``DEFAULT_ENVELOPE_VERSION``);
``decode_envelope`` auto-detects it from the leading byte, so a receiver needs no
out-of-band hint (a ``VF-Env`` broker header is published as a belt-and-suspenders
aid for tooling). Runs are version-homogeneous — streams are run-scoped — so a
single run never mixes versions.
'''
from __future__ import absolute_import, division, print_function

import hashlib
import os
import pickle
from typing import Any, Tuple

import msgpack
import numpy as np

from .v1 import envelope_pb2, payloads_pb2, value_pb2

# -- codecs / versions -----------------------------------------------------

#: v2/v3 msgpack payload codecs (the ``payload_codec`` field of a msgpack envelope).
CODEC_RAW_NDARRAY = 'raw-ndarray'
CODEC_PICKLE = 'pickle'
CODEC_EXTERNAL_REF = 'external-ref'

#: v4 protobuf payload-type identifiers (the ``payload_type`` field of an
#: ``Envelope``). Proto messages use their descriptor FQN; these three are the ones
#: the codec special-cases, plus the legacy pickle marker.
PAYLOAD_TENSOR = 'videoflow.v1.Tensor'
PAYLOAD_VALUE = 'videoflow.v1.Value'
PAYLOAD_BLOBREF = 'videoflow.v1.BlobRef'
PAYLOAD_PICKLE = 'x-python-pickle'

#: Highest envelope version this build understands. Protocol v1 targets v4.
ENVELOPE_VERSION = 3  # retained name/value: the legacy default emit version
LATEST_ENVELOPE_VERSION = 4

#: Default *emit* version. Stays 3 during the bake period, flips to 4 in a
#: subsequent minor release (see the migration plan). Overridable per run via
#: ``VF_ENVELOPE_VERSION``.
DEFAULT_ENVELOPE_VERSION = int(os.environ.get('VF_ENVELOPE_VERSION', '3'))

#: Versions this build can *emit*.
EMITTABLE_ENVELOPE_VERSIONS = (3, 4)

#: Versions this build can *decode*. v2 lacks ``event_ts`` (decodes as ``None``).
COMPATIBLE_ENVELOPE_VERSIONS = (2, 3, 4)

#: Message kinds carried in the envelope ``type`` field. ``data`` is a normal
#: payload; ``eos`` is an end-of-stream marker with no payload.
MSG_TYPE_DATA = 'data'
MSG_TYPE_EOS = 'eos'

_PROTO_MSG_TYPE = {
    MSG_TYPE_DATA: envelope_pb2.MSG_TYPE_DATA,
    MSG_TYPE_EOS: envelope_pb2.MSG_TYPE_EOS,
}
_PROTO_MSG_TYPE_REV = {v: k for k, v in _PROTO_MSG_TYPE.items()}

def derive_message_id(flow_id : str, run_id : str, producer_name : str,
                    trace_id : str, seq : int, msg_type : str) -> str:
    '''
    Deterministic, content-derived message id. Two publishes of the *same logical
    message* (e.g. a processor that crashed after publishing but is re-run and
    recomputes the same output for the same input group) produce the same id, so
    JetStream's ``Nats-Msg-Id`` de-duplication drops the retry copy. It is
    therefore essential that the inputs here are stable across retries — in
    particular ``seq`` must be carried forward from the input group, not a local
    wall-clock or attempt counter.
    '''
    raw = f'{flow_id}:{run_id}:{producer_name}:{trace_id}:{seq}:{msg_type}'
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:32]

#: Payloads whose serialized size (in bytes) exceeds this threshold are written to
#: a BlobStore instead of being inlined in the broker message. Large uncompressed
#: video frames (a 1080p RGB frame is ~6.2MB) would otherwise blow past a typical
#: broker's per-message size limit (NATS defaults to a 1MB `max_payload`).
MAX_INLINE_PAYLOAD_BYTES = int(os.environ.get('VIDEOFLOW_MAX_INLINE_PAYLOAD_BYTES', 512 * 1024))

class BlobStore:
    '''
    Interface for the external blob store used for payloads over \
        ``MAX_INLINE_PAYLOAD_BYTES``. Not tied to any particular broker — Redis is a \
        convenient default (large string values, simple TTL-based expiry) even when \
        NATS is the primary messaging broker.
    '''
    def put(self, data : bytes, ttl_seconds : int = 3600) -> str:
        '''Stores ``data`` and returns an opaque reference string that ``get()`` can resolve later.'''
        raise NotImplementedError('BlobStore subclass must implement put()')

    def get(self, ref : str) -> bytes:
        '''Resolves a reference previously returned by ``put()`` back into bytes.'''
        raise NotImplementedError('BlobStore subclass must implement get()')

class RedisBlobStore(BlobStore):
    '''Uses a Redis server purely as a large-value TTL cache, independent of whether Redis is used for messaging.'''
    def __init__(self, url : str = None) -> None:
        import redis
        self._client = redis.Redis.from_url(url or os.environ.get('VIDEOFLOW_BLOB_REDIS_URL', 'redis://localhost:6379/0'))

    def put(self, data : bytes, ttl_seconds : int = 3600) -> str:
        import uuid
        key = f'vf-blob-{uuid.uuid4().hex}'
        self._client.set(key, data, ex = ttl_seconds)
        return key

    def get(self, ref : str) -> bytes:
        data = self._client.get(ref)
        if data is None:
            raise KeyError(f'Blob {ref} not found (expired or never existed)')
        # redis-py types get() more broadly than what we store (always bytes here).
        return data  # type: ignore[return-value]

# ==========================================================================
# v2 / v3 — msgpack payload codec (legacy, unchanged behavior)
# ==========================================================================

def _encode_ndarray(arr : np.ndarray) -> bytes:
    return msgpack.packb(
        {'shape': list(arr.shape), 'dtype': str(arr.dtype), 'data': arr.tobytes()},
        use_bin_type = True
    )

def _decode_ndarray(buf : bytes) -> np.ndarray:
    d = msgpack.unpackb(buf, raw = False)
    return np.frombuffer(d['data'], dtype = np.dtype(d['dtype'])).reshape(d['shape'])

def encode_payload(payload, blob_store : BlobStore = None) -> tuple:
    '''
    Encodes an arbitrary payload (numpy array, or any picklable Python object) into \
        ``(codec, bytes)``. If the encoded size exceeds ``MAX_INLINE_PAYLOAD_BYTES`` \
        and a ``blob_store`` is given, the bytes are written there instead and a \
        small reference is returned in their place. (v2/v3 msgpack codec.)
    '''
    if isinstance(payload, np.ndarray):
        codec, buf = CODEC_RAW_NDARRAY, _encode_ndarray(payload)
    else:
        codec, buf = CODEC_PICKLE, pickle.dumps(payload, protocol = 5)

    if len(buf) > MAX_INLINE_PAYLOAD_BYTES:
        if blob_store is None:
            raise ValueError(
                f'Payload of {len(buf)} bytes exceeds MAX_INLINE_PAYLOAD_BYTES '
                f'({MAX_INLINE_PAYLOAD_BYTES}) and no blob_store was configured to '
                'offload it to. Configure VIDEOFLOW_BLOB_REDIS_URL or pass a BlobStore.'
            )
        ref = blob_store.put(buf)
        return CODEC_EXTERNAL_REF, msgpack.packb({'ref': ref, 'inner_codec': codec}, use_bin_type = True)
    return codec, buf

def decode_payload(codec : str, buf : bytes, blob_store : BlobStore = None) -> Any:
    '''Inverse of ``encode_payload`` (v2/v3 msgpack codec).'''
    if codec == CODEC_EXTERNAL_REF:
        d = msgpack.unpackb(buf, raw = False)
        if blob_store is None:
            raise ValueError('Payload is an external-ref but no blob_store was configured to resolve it.')
        inner_buf = blob_store.get(d['ref'])
        return decode_payload(d['inner_codec'], inner_buf, blob_store = blob_store)
    if codec == CODEC_RAW_NDARRAY:
        return _decode_ndarray(buf)
    if codec == CODEC_PICKLE:
        return pickle.loads(buf)
    raise ValueError(f'Unknown payload codec: {codec}')

def _encode_envelope_v3(producer_name, flow_id, run_id, trace_id, seq, msg_type,
                        metadata, payload, span_id, parent_span_id, replica_id,
                        event_ts, blob_store) -> bytes:
    if msg_type == MSG_TYPE_EOS:
        payload_codec, payload_buf = CODEC_PICKLE, b''
    else:
        payload_codec, payload_buf = encode_payload(payload, blob_store = blob_store)
    envelope = {
        'v': 3,
        'type': msg_type,
        'producer_name': producer_name,
        'flow_id': flow_id,
        'run_id': run_id,
        'trace_id': trace_id,
        'seq': seq,
        'event_ts': event_ts,
        'span_id': span_id,
        'parent_span_id': parent_span_id,
        'replica_id': replica_id,
        'metadata': metadata,
        'payload_codec': payload_codec,
        'payload': payload_buf,
    }
    return msgpack.packb(envelope, use_bin_type = True)

def _decode_envelope_v3(buf : bytes, blob_store : BlobStore = None) -> dict:
    envelope = msgpack.unpackb(buf, raw = False)
    version = envelope.get('v')
    if version not in (2, 3):
        raise ValueError(f'Unsupported msgpack envelope version {version!r}')
    msg_type = envelope['type']
    is_stop_signal = msg_type == MSG_TYPE_EOS
    message = None if is_stop_signal else decode_payload(
        envelope['payload_codec'], envelope['payload'], blob_store = blob_store)
    return {
        'producer_name': envelope['producer_name'],
        'flow_id': envelope['flow_id'],
        'run_id': envelope['run_id'],
        'trace_id': envelope['trace_id'],
        'seq': envelope['seq'],
        'event_ts': envelope.get('event_ts'),
        'type': msg_type,
        'is_stop_signal': is_stop_signal,
        'span_id': envelope['span_id'],
        'parent_span_id': envelope['parent_span_id'],
        'replica_id': envelope['replica_id'],
        'metadata': envelope['metadata'],
        'message': message,
    }

# ==========================================================================
# v4 — protobuf payload codec (language-neutral wire)
# ==========================================================================

class RawPayload:
    '''
    An opaque, already-encoded payload: a ``payload_type`` FQN and its raw bytes.
    Produced when decoding a message whose ``payload_type`` is not registered (so a
    forwarding/storing node need not understand it), and accepted by the encoder so
    such a payload can be re-published losslessly (PROTOCOL.md WIRE-9).
    '''
    __slots__ = ('payload_type', 'data')

    def __init__(self, payload_type : str, data : bytes) -> None:
        self.payload_type = payload_type
        self.data = data

    def __repr__(self) -> str:
        return f'RawPayload({self.payload_type!r}, {len(self.data)} bytes)'

#: FQN -> protobuf message class, for decoding registered well-known / vendor types.
#: Tensor/Value/BlobRef are handled specially (converted to ndarray / python / a
#: blob resolve); other registered types decode to their proto message instance.
_PAYLOAD_REGISTRY : dict = {}

def register_payload_type(message_cls) -> None:
    '''
    Register a protobuf message class so envelopes carrying its FQN decode to an
    instance of it (rather than an opaque ``RawPayload``). The well-known videoflow
    types are pre-registered; vendors register their own payload messages.
    '''
    _PAYLOAD_REGISTRY[message_cls.DESCRIPTOR.full_name] = message_cls

for _m in (payloads_pb2.Tensor, payloads_pb2.Frame, payloads_pb2.Detections,
        payloads_pb2.Tracks, payloads_pb2.BlobRef, value_pb2.Value):
    register_payload_type(_m)

def _value_to_proto(v : Any) -> value_pb2.Value:
    out = value_pb2.Value()
    if v is None:
        out.null_value = value_pb2.NULL_VALUE
    elif isinstance(v, bool):  # bool before int (bool is a subclass of int)
        out.bool_value = v
    elif isinstance(v, int):
        out.int_value = v
    elif isinstance(v, float):
        out.double_value = v
    elif isinstance(v, str):
        out.string_value = v
    elif isinstance(v, (bytes, bytearray)):
        out.bytes_value = bytes(v)
    elif isinstance(v, np.generic):  # numpy scalar -> python scalar
        return _value_to_proto(v.item())
    elif isinstance(v, (list, tuple)):
        out.list_value.values.extend(_value_to_proto(x) for x in v)
    elif isinstance(v, dict):
        for k, val in v.items():
            if not isinstance(k, str):
                raise TypeError(f'Value map keys must be strings, got {type(k).__name__}')
            out.map_value.fields[k].CopyFrom(_value_to_proto(val))
    else:
        raise TypeError(f'Cannot encode {type(v).__name__} as videoflow.v1.Value')
    return out

def _value_from_proto(v : value_pb2.Value) -> Any:
    kind = v.WhichOneof('kind')
    if kind is None or kind == 'null_value':
        return None
    if kind == 'bool_value':
        return v.bool_value
    if kind == 'int_value':
        return v.int_value
    if kind == 'double_value':
        return v.double_value
    if kind == 'string_value':
        return v.string_value
    if kind == 'bytes_value':
        return v.bytes_value
    if kind == 'list_value':
        return [_value_from_proto(x) for x in v.list_value.values]
    if kind == 'map_value':
        return {k: _value_from_proto(val) for k, val in v.map_value.fields.items()}
    raise ValueError(f'Unknown Value kind: {kind}')  # pragma: no cover

def _ndarray_to_tensor(arr : np.ndarray) -> payloads_pb2.Tensor:
    arr = np.ascontiguousarray(arr)
    return payloads_pb2.Tensor(shape = list(arr.shape), dtype = str(arr.dtype), data = arr.tobytes())

def _tensor_to_ndarray(t : payloads_pb2.Tensor) -> np.ndarray:
    return np.frombuffer(t.data, dtype = np.dtype(t.dtype)).reshape(list(t.shape))

# Protobuf message classes are recognized structurally (they expose SerializeToString
# and a DESCRIPTOR) so any generated message — well-known or vendor — can be a payload.
def _is_proto_message(obj) -> bool:
    return hasattr(obj, 'DESCRIPTOR') and hasattr(obj, 'SerializeToString')

def _encode_payload_v4(payload, allow_pickle : bool) -> Tuple[str, bytes]:
    '''Encodes a payload to ``(payload_type, bytes)`` without blob offload (§4.4).'''
    if isinstance(payload, np.ndarray):
        return PAYLOAD_TENSOR, _ndarray_to_tensor(payload).SerializeToString()
    if isinstance(payload, RawPayload):
        return payload.payload_type, payload.data
    if _is_proto_message(payload):
        return payload.DESCRIPTOR.full_name, payload.SerializeToString()
    if isinstance(payload, (bool, int, float, str, bytes, bytearray, list, tuple, dict, np.generic)) or payload is None:
        return PAYLOAD_VALUE, _value_to_proto(payload).SerializeToString()
    if allow_pickle:
        return PAYLOAD_PICKLE, pickle.dumps(payload, protocol = 5)
    raise TypeError(
        f'Cannot encode payload of type {type(payload).__name__} for the v4 wire. '
        'Use a numpy ndarray, a registered videoflow.v1 payload, a protobuf message, '
        'a JSON-like scalar/list/dict (videoflow.v1.Value), or enable the legacy '
        'pickle codec (VF_ALLOW_PICKLE=1, Python-only flows).'
    )

def _decode_payload_v4(payload_type : str, buf : bytes, blob_store : BlobStore = None) -> Any:
    if payload_type == PAYLOAD_BLOBREF:
        ref = payloads_pb2.BlobRef()
        ref.ParseFromString(buf)
        if blob_store is None:
            raise ValueError('Payload is a BlobRef but no blob_store was configured to resolve it.')
        return _decode_payload_v4(ref.inner_payload_type, blob_store.get(ref.ref), blob_store = blob_store)
    if payload_type == PAYLOAD_TENSOR:
        t = payloads_pb2.Tensor()
        t.ParseFromString(buf)
        return _tensor_to_ndarray(t)
    if payload_type == PAYLOAD_VALUE:
        v = value_pb2.Value()
        v.ParseFromString(buf)
        return _value_from_proto(v)
    if payload_type == PAYLOAD_PICKLE:
        return pickle.loads(buf)
    cls = _PAYLOAD_REGISTRY.get(payload_type)
    if cls is not None:
        msg = cls()
        msg.ParseFromString(buf)
        return msg
    # Unknown type: hand back opaque bytes so a forwarding node can re-emit them.
    return RawPayload(payload_type, buf)

def _encode_envelope_v4(producer_name, flow_id, run_id, trace_id, seq, msg_type,
                        metadata, payload, span_id, parent_span_id, replica_id,
                        event_ts, blob_store, allow_pickle) -> bytes:
    env = envelope_pb2.Envelope(
        v = 4,
        type = _PROTO_MSG_TYPE[msg_type],
        producer_name = producer_name,
        flow_id = flow_id,
        run_id = run_id,
        trace_id = trace_id,
        seq = seq,
        span_id = span_id or '',
        parent_span_id = parent_span_id or '',
        replica_id = replica_id,
    )
    if event_ts is not None:
        env.event_ts = event_ts
    for k, v in (metadata or {}).items():
        env.metadata[k].CopyFrom(_value_to_proto(v))

    if msg_type == MSG_TYPE_EOS:
        env.payload_type = ''
        env.payload = b''
    else:
        payload_type, payload_buf = _encode_payload_v4(payload, allow_pickle)
        # Blob offload: over the inline threshold, stash the encoded bytes and carry
        # a small BlobRef in their place (PROTOCOL.md §13).
        if len(payload_buf) > MAX_INLINE_PAYLOAD_BYTES:
            if blob_store is None:
                raise ValueError(
                    f'Payload of {len(payload_buf)} bytes exceeds MAX_INLINE_PAYLOAD_BYTES '
                    f'({MAX_INLINE_PAYLOAD_BYTES}) and no blob_store was configured to offload '
                    'it to. Configure VIDEOFLOW_BLOB_REDIS_URL or pass a BlobStore.')
            ref = blob_store.put(payload_buf)
            blobref = payloads_pb2.BlobRef(ref = ref, inner_payload_type = payload_type, size = len(payload_buf))
            payload_type, payload_buf = PAYLOAD_BLOBREF, blobref.SerializeToString()
        env.payload_type = payload_type
        env.payload = payload_buf
    return env.SerializeToString()

def _decode_envelope_v4(buf : bytes, blob_store : BlobStore = None) -> dict:
    env = envelope_pb2.Envelope()
    env.ParseFromString(buf)
    if env.v != 4:
        raise ValueError(f'Unsupported protobuf envelope version {env.v!r}; expected 4')
    msg_type = _PROTO_MSG_TYPE_REV.get(env.type)
    if msg_type is None:
        raise ValueError(f'Unspecified/unknown envelope message type {env.type!r}')
    is_stop_signal = msg_type == MSG_TYPE_EOS
    message = None if is_stop_signal else _decode_payload_v4(env.payload_type, env.payload, blob_store = blob_store)
    return {
        'producer_name': env.producer_name,
        'flow_id': env.flow_id,
        'run_id': env.run_id,
        'trace_id': env.trace_id,
        'seq': env.seq,
        'event_ts': env.event_ts if env.HasField('event_ts') else None,
        'type': msg_type,
        'is_stop_signal': is_stop_signal,
        'span_id': env.span_id,
        'parent_span_id': env.parent_span_id,
        'replica_id': env.replica_id,
        'metadata': {k: _value_from_proto(v) for k, v in env.metadata.items()},
        'message': message,
    }

# ==========================================================================
# version-dispatching public API
# ==========================================================================

def encode_envelope(producer_name : str, flow_id : str, run_id : str, trace_id : str,
                    seq : int, msg_type : str, metadata : dict, payload,
                    span_id : str = '', parent_span_id : str = '', replica_id : int = 0,
                    event_ts : float = None, blob_store : BlobStore = None,
                    version : int = None, allow_pickle : bool = False) -> bytes:
    '''
    Encodes a full wire message and returns the bytes to publish to a broker subject.

    - Arguments:
        - msg_type: ``MSG_TYPE_DATA`` or ``MSG_TYPE_EOS``. EOS carries no payload.
        - run_id: the per-run identifier that scopes this flow execution.
        - span_id / parent_span_id: hex ids for log/trace correlation (optional).
        - replica_id: index of the emitting replica (0 for single-task nodes); \
            distinguishes EOS markers from different replicas of one node.
        - event_ts: event time of the message in epoch seconds — when the \
            underlying real-world event was captured — minted by the producer and \
            carried forward unchanged; time-aligned joins group on it.
        - version: envelope version to emit (``3`` msgpack or ``4`` protobuf). \
            Defaults to ``DEFAULT_ENVELOPE_VERSION``.
        - allow_pickle: (v4 only) permit the legacy Python-only pickle codec for a \
            payload that has no neutral encoding. Ignored by v3 (always pickles).
    '''
    version = DEFAULT_ENVELOPE_VERSION if version is None else version
    if version == 4:
        return _encode_envelope_v4(producer_name, flow_id, run_id, trace_id, seq, msg_type,
                                metadata, payload, span_id, parent_span_id, replica_id,
                                event_ts, blob_store, allow_pickle)
    if version == 3:
        return _encode_envelope_v3(producer_name, flow_id, run_id, trace_id, seq, msg_type,
                                metadata, payload, span_id, parent_span_id, replica_id,
                                event_ts, blob_store)
    raise ValueError(f'Cannot emit envelope version {version!r}; emittable: {EMITTABLE_ENVELOPE_VERSIONS}')

def _is_msgpack_map(first_byte : int) -> bool:
    # A v2/v3 envelope is a msgpack map at top level: fixmap (0x80-0x8f) for our
    # 12-14 fields, or map16/map32 (0xde/0xdf). A v4 protobuf envelope starts with a
    # field tag (field 1 ``v`` => 0x08) and never lands in these ranges — so the
    # leading byte unambiguously selects the decoder without any out-of-band hint.
    return 0x80 <= first_byte <= 0x8f or first_byte in (0xde, 0xdf)

def decode_envelope(buf : bytes, blob_store : BlobStore = None) -> dict:
    '''
    Decodes wire bytes back into a dict with keys ``producer_name``, ``flow_id``, \
        ``run_id``, ``trace_id``, ``seq``, ``event_ts`` (``None`` when absent), \
        ``type``, ``is_stop_signal`` (derived: True iff ``type == MSG_TYPE_EOS``), \
        ``span_id``, ``parent_span_id``, ``replica_id``, ``metadata``, and \
        ``message`` (the fully decoded payload — ``None`` for EOS). The envelope \
        version (msgpack v2/v3 vs protobuf v4) is detected from the leading byte.
    '''
    if not buf:
        raise ValueError('Cannot decode an empty envelope buffer')
    if _is_msgpack_map(buf[0]):
        return _decode_envelope_v3(buf, blob_store = blob_store)
    return _decode_envelope_v4(buf, blob_store = blob_store)
