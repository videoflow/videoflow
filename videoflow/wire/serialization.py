'''
Wire format used to move messages between nodes over the message broker
(``videoflow.messaging.nats_messenger.NATSMessenger``). Not used by anything
running purely in a single local process — it's the boundary format for bytes
that cross a network/process boundary.

The wire is a single, language-neutral protobuf envelope (``videoflow.v1.Envelope``,
envelope version 4). Its payload is a typed protobuf message:

- ``Tensor`` for arrays, including video frames;
- ``Value`` for structured scalars/maps/lists — and a ``Value`` may nest a ``Tensor``
  (``tensor_value``), so a mixed container like a ``(frame_index, frame)`` tuple has a
  neutral encoding (``spec/PROTOCOL.md`` WIRE-15);
- any vendor proto by its fully-qualified name.

Arbitrary Python objects are **never** put on the wire: there is no code-executing
fallback codec, because deserializing attacker-controlled bytes that way is remote
code execution (see ``spec/rfcs/0001``). A payload type with no built-in encoding
registers one via ``register_payload_encoder``; an unknown ``payload_type`` on decode
is handed back as opaque ``RawPayload`` bytes and never deserialized.

``decode_envelope`` reads the envelope; a legacy msgpack (v2/v3) envelope is refused
with a clear error rather than decoded. Runs are version-homogeneous — streams are
run-scoped — so a single run never mixes versions.
'''
from __future__ import absolute_import, division, print_function

import hashlib
import os
import uuid
from typing import Any, Callable, Optional, Tuple, TypeGuard

import numpy as np
from google.protobuf.message import Message

from ..utils import plugins
from ..v1 import envelope_pb2, payloads_pb2, value_pb2

# -- payload types / versions ----------------------------------------------

#: v4 protobuf payload-type identifiers (the ``payload_type`` field of an
#: ``Envelope``). Proto messages use their descriptor FQN; these three are the ones
#: the codec special-cases. Any other FQN round-trips as an opaque ``RawPayload``.
PAYLOAD_TENSOR = 'videoflow.v1.Tensor'
PAYLOAD_VALUE = 'videoflow.v1.Value'
PAYLOAD_BLOBREF = 'videoflow.v1.BlobRef'

#: The sole envelope version: a language-neutral ``videoflow.v1.Envelope`` (protobuf).
#: Overridable per run via ``VF_ENVELOPE_VERSION`` only to a version this build speaks.
DEFAULT_ENVELOPE_VERSION = int(os.environ.get('VF_ENVELOPE_VERSION', '4'))

#: Versions this build can *emit*.
EMITTABLE_ENVELOPE_VERSIONS = (4,)

#: Versions this build can *decode*. The legacy msgpack wire (v2/v3) has been removed;
#: such envelopes are refused on decode.
COMPATIBLE_ENVELOPE_VERSIONS = (4,)

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

#: Blob lifetime when nothing plumbs an explicit TTL. Serialization is flow-type
#: agnostic, so the flow-aware defaults (3600s realtime / 86400s batch, BLOB-7) live
#: with the messenger that knows the flow type; this is only the fallback.
DEFAULT_BLOB_TTL_SECONDS = 3600

class BlobStore:
    '''
    Interface for the external blob store used for payloads over \
        ``MAX_INLINE_PAYLOAD_BYTES``. Not tied to any particular broker — Redis is a \
        convenient default (large string values, simple TTL-based expiry) even when \
        NATS is the primary messaging broker.

    Reclamation (RFC 0002): ``put_with_readers``/``release`` default to TTL-only \
        behaviour so a subclass that only implements ``put``/``get`` keeps working; \
        a store that can refcount overrides both.
    '''
    def put(self, data : bytes, ttl_seconds : int = DEFAULT_BLOB_TTL_SECONDS) -> str:
        '''Stores ``data`` and returns an opaque reference string that ``get()`` can resolve later.'''
        raise NotImplementedError('BlobStore subclass must implement put()')

    def get(self, ref : str) -> bytes:
        '''Resolves a reference previously returned by ``put()`` back into bytes.'''
        raise NotImplementedError('BlobStore subclass must implement get()')

    def put_with_readers(self, data : bytes, readers : int, ttl_seconds : int = DEFAULT_BLOB_TTL_SECONDS) -> str:
        '''
        Stores ``data`` expecting exactly ``readers`` downstream reads, enabling the \
            store to reclaim the blob once every reader has released it (BLOB-5).

        Default implementation ignores ``readers`` and delegates to ``put()`` — a \
            store with no reclamation support degrades to plain TTL expiry.
        '''
        return self.put(data, ttl_seconds)

    def release(self, ref : str) -> None:
        '''One downstream reader is finished with ``ref`` (its message was acked, BLOB-6). Default no-op.'''
        return None

class RedisBlobStore(BlobStore):
    '''Uses a Redis server purely as a large-value TTL cache, independent of whether Redis is used for messaging.'''
    def __init__(self, url : str | None = None) -> None:
        import redis  # optional dependency (extra): only this store needs it
        self._client = redis.Redis.from_url(url or os.environ.get('VIDEOFLOW_BLOB_REDIS_URL', 'redis://localhost:6379/0'))

    def put(self, data : bytes, ttl_seconds : int = DEFAULT_BLOB_TTL_SECONDS) -> str:
        key = f'vf-blob-{uuid.uuid4().hex}'
        self._client.set(key, data, ex = ttl_seconds)
        return key

    def get(self, ref : str) -> bytes:
        data = self._client.get(ref)
        if data is None:
            raise KeyError(f'Blob {ref} not found (expired or never existed)')
        # redis-py types get() more broadly than what we store (always bytes here).
        return data  # type: ignore[return-value]

    def _refcount_key(self, ref : str) -> str:
        return 'vf-blobrc-' + ref.removeprefix('vf-blob-')

    def put_with_readers(self, data : bytes, readers : int, ttl_seconds : int = DEFAULT_BLOB_TTL_SECONDS) -> str:
        # Blob first, counter second: a crash in between leaves a counterless blob,
        # which release() treats as TTL-only — the safe direction (BLOB-5).
        key = self.put(data, ttl_seconds)
        if readers > 0:
            self._client.set(self._refcount_key(key), readers, ex = ttl_seconds)
        return key

    def release(self, ref : str) -> None:
        rc = self._refcount_key(ref)
        # The EXISTS guard is load-bearing: releasing a blob written without a
        # counter (older publisher, VF_BLOB_READERS unset, or counter expired) must
        # not DECR-create a negative key and delete a blob other readers still need.
        if not self._client.exists(rc):
            return
        if int(self._client.decr(rc)) <= 0:
            # Two single-key UNLINKs, not one multi-key call: the blob and counter
            # keys hash to different slots on Redis Cluster.
            self._client.unlink(ref)
            self._client.unlink(rc)

# -- blob store selection --------------------------------------------------
#
# The store is chosen by the URL scheme of the configured blob URL, so adding one
# (S3, MinIO, a filesystem store for tests) means registering a factory rather
# than editing the worker's construction site.

BLOB_STORE_ENTRY_POINT_GROUP = 'videoflow.blob_stores'

_BLOB_STORE_SCHEMES : dict[str, Callable[[str], BlobStore]] = {}

def _normalize_scheme(scheme : str) -> str:
    '''
    Canonical form of a URL scheme. Registration and lookup must agree on this,
    or a store registered as ``'S3'`` is unreachable from ``s3://bucket/key``
    while still being listed as known.
    '''
    return scheme.strip().lower()

def register_blob_store(scheme : str, factory : Callable[[str], BlobStore]) -> None:
    '''
    Registers a ``BlobStore`` factory for a URL scheme. ``factory`` receives the
    full URL (not just the remainder) and returns a ready store.

    Third-party packages may register either by calling this on import, or by
    declaring an entry point in the ``videoflow.blob_stores`` group — the latter
    is what lets a store be selected purely by configuration, with nothing in the
    flow importing the package that provides it.

    - Arguments:
        - scheme: URL scheme without the separator, e.g. ``'s3'``. Case-insensitive: \
            normalized here to match how ``make_blob_store`` parses a URL, so \
            ``register_blob_store('S3', ...)`` is reachable from ``s3://bucket/key``.
        - factory: callable taking the blob URL and returning a ``BlobStore``.
    '''
    _BLOB_STORE_SCHEMES[_normalize_scheme(scheme)] = factory

def registered_blob_store_schemes() -> list[str]:
    '''The URL schemes a blob store is currently registered for, sorted.'''
    return sorted(_BLOB_STORE_SCHEMES)

def make_blob_store(url : str) -> BlobStore:
    '''
    Builds the blob store for ``url``, dispatching on its scheme.

    - Arguments:
        - url: blob store URL, e.g. ``redis://localhost:6379/0``.

    - Returns: a ready ``BlobStore``.

    - Raises:
        - ValueError: the URL has no scheme, or no store is registered for it. \
            The message names the known schemes and ``register_blob_store``.
    '''
    scheme = _normalize_scheme(url.split('://', 1)[0]) if '://' in url else ''
    if not scheme:
        raise ValueError(
            f'Blob store URL must include a scheme, got {url!r}. '
            f'Known schemes: {", ".join(registered_blob_store_schemes())}.'
        )
    if scheme not in _BLOB_STORE_SCHEMES:
        # A scheme we do not know may belong to an installed-but-unimported
        # plugin; consult entry points once before giving up.
        plugins.load_plugin_group(BLOB_STORE_ENTRY_POINT_GROUP)
    if scheme not in _BLOB_STORE_SCHEMES:
        raise ValueError(
            f'No blob store registered for URL scheme {scheme!r} (from {url!r}). '
            f'Known schemes: {", ".join(registered_blob_store_schemes())}. '
            f'Register one with videoflow.wire.serialization.register_blob_store('
            f'{scheme!r}, factory), or declare a '
            f'{BLOB_STORE_ENTRY_POINT_GROUP!r} entry point.'
        )
    return _BLOB_STORE_SCHEMES[scheme](url)

# Every scheme redis-py's from_url accepts. The worker used to hand VF_BLOB_REDIS_URL
# straight to RedisBlobStore whatever its scheme, so all three must stay registered
# or scheme dispatch would silently drop support for a URL that used to work.
register_blob_store('redis', RedisBlobStore)
register_blob_store('rediss', RedisBlobStore)   # TLS
register_blob_store('unix', RedisBlobStore)     # Unix domain socket

# ==========================================================================
# v4 — protobuf payload codec (the language-neutral wire)
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
_PAYLOAD_REGISTRY : dict[str, type[Message]] = {}

def register_payload_type(message_cls : type[Message]) -> None:
    '''
    Register a protobuf message class so envelopes carrying its FQN decode to an
    instance of it (rather than an opaque ``RawPayload``). The well-known videoflow
    types are pre-registered; vendors register their own payload messages.
    '''
    _PAYLOAD_REGISTRY[message_cls.DESCRIPTOR.full_name] = message_cls

for _m in (payloads_pb2.Tensor, payloads_pb2.Frame, payloads_pb2.Detections,
        payloads_pb2.Tracks, payloads_pb2.BlobRef, value_pb2.Value):
    register_payload_type(_m)

#: Entry-point group for third-party payload codecs. Registration normally happens
#: as a side effect of importing the component module (the worker imports it via
#: ``VF_NODE_CLASS`` anyway); the group covers host-side tools such as
#: ``videoflow debug decode``, which must understand a vendor payload without
#: knowing which package defines it.
PAYLOAD_ENTRY_POINT_GROUP = 'videoflow.payload_types'

#: Encode rules, consulted in registration order by ``isinstance``. The encode-side
#: complement of ``_PAYLOAD_REGISTRY``: that one makes a *received* FQN decode to a
#: class, this one gives an *outgoing* Python object a wire type.
_PAYLOAD_ENCODERS : list[Tuple[type, Callable[[Any], Tuple[str, bytes]]]] = []

def register_payload_encoder(python_type : type,
                            encoder : Callable[[Any], Tuple[str, bytes]]) -> None:
    '''
    Registers an encode rule mapping instances of ``python_type`` to a
    ``(payload_type, serialized_bytes)`` pair on the v4 wire — the encode-side
    complement of ``register_payload_type``.

    Rules are checked in registration order, and only *after* every built-in
    check (ndarray, ``RawPayload``, protobuf message, and the JSON-like types that
    become ``Value``). That ordering is deliberate and worth preserving: it lets a
    vendor type get a real wire type instead of a ``TypeError``, while making it
    impossible for any registration — even one matching ``object`` — to change how a
    built-in payload encodes. Those mappings are fixed by ``spec/PROTOCOL.md`` §4.4
    and proven by the golden vectors, so they are not an extension point. This is the
    only way to give a type with no built-in encoding a place on the wire — there is
    no code-executing fallback (see ``spec/rfcs/0001``).

    The corollary: a type that is *already* encodable (a ``dict`` subclass, say)
    is encoded as the built-in it resembles, and its registered rule never runs.

    Pair this with ``register_payload_type`` (or a decoder that understands the
    ``payload_type`` string) so the receiving side can decode what you emit.

    - Arguments:
        - python_type: the type to match with ``isinstance``.
        - encoder: callable taking the payload and returning \
            ``(payload_type, serialized_bytes)``.
    '''
    _PAYLOAD_ENCODERS.append((python_type, encoder))

def _encode_registered_payload(payload : Any) -> Optional[Tuple[str, bytes]]:
    '''The first registered encode rule matching ``payload``, or None.'''
    for python_type, encoder in _PAYLOAD_ENCODERS:
        if isinstance(payload, python_type):
            return encoder(payload)
    return None

def _value_to_proto(v : Any, allow_tensor : bool = True) -> value_pb2.Value:
    '''
    Encodes a JSON-like Python value to a ``videoflow.v1.Value``. A nested numpy \
        ndarray becomes a ``tensor_value`` (WIRE-15), so a structured container that \
        mixes arrays with scalars — e.g. a ``(frame_index, frame)`` tuple — has a \
        neutral encoding. ``allow_tensor=False`` forbids nested arrays; it is used for \
        the envelope metadata map, which is for small scalars, not payload arrays.
    '''
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
        return _value_to_proto(v.item(), allow_tensor)
    elif isinstance(v, np.ndarray):
        if not allow_tensor:
            raise TypeError(
                'Cannot put a numpy ndarray in message metadata: metadata is for small '
                'scalars/strings, not payload arrays. Send the array as the payload (or a '
                'field of the payload) instead.')
        out.tensor_value.CopyFrom(_ndarray_to_tensor(v))
    elif isinstance(v, (list, tuple)):
        out.list_value.values.extend(_value_to_proto(x, allow_tensor) for x in v)
    elif isinstance(v, dict):
        for k, val in v.items():
            if not isinstance(k, str):
                raise TypeError(f'Value map keys must be strings, got {type(k).__name__}')
            out.map_value.fields[k].CopyFrom(_value_to_proto(val, allow_tensor))
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
    if kind == 'tensor_value':
        return _tensor_to_ndarray(v.tensor_value)
    raise ValueError(f'Unknown Value kind: {kind}')  # pragma: no cover

def _ndarray_to_tensor(arr : np.ndarray) -> payloads_pb2.Tensor:
    arr = np.ascontiguousarray(arr)
    return payloads_pb2.Tensor(shape = list(arr.shape), dtype = str(arr.dtype), data = arr.tobytes())

def _tensor_to_ndarray(t : payloads_pb2.Tensor) -> np.ndarray:
    return np.frombuffer(t.data, dtype = np.dtype(t.dtype)).reshape(list(t.shape))

# Any generated message — well-known or vendor — can be a payload: they all derive from
# the protobuf runtime's Message base, so isinstance covers what the old structural
# DESCRIPTOR/SerializeToString probe did, and narrows the type for callers.
def _is_proto_message(obj : object) -> TypeGuard[Message]:
    return isinstance(obj, Message)

def _encode_payload_v4(payload : Any) -> Tuple[str, bytes]:
    '''Encodes a payload to ``(payload_type, bytes)`` without blob offload (§4.4).'''
    if isinstance(payload, np.ndarray):
        return PAYLOAD_TENSOR, _ndarray_to_tensor(payload).SerializeToString()
    if isinstance(payload, RawPayload):
        return payload.payload_type, payload.data
    if _is_proto_message(payload):
        return payload.DESCRIPTOR.full_name, payload.SerializeToString()
    if isinstance(payload, (bool, int, float, str, bytes, bytearray, list, tuple, dict, np.generic)) or payload is None:
        return PAYLOAD_VALUE, _value_to_proto(payload).SerializeToString()
    # Vendor encode rules sit after *every* built-in check, so registering one can
    # never change how a built-in payload encodes.
    registered = _encode_registered_payload(payload)
    if registered is not None:
        return registered
    raise TypeError(
        f'Cannot encode payload of type {type(payload).__name__} for the wire. '
        'Send a numpy ndarray, a videoflow.v1 payload, a protobuf message, or a '
        'JSON-like scalar/list/dict (which may nest ndarrays); or register an encoder '
        'with videoflow.wire.serialization.register_payload_encoder so the type gets a '
        'neutral wire encoding. (Arbitrary Python objects are not accepted on the wire; '
        'see spec/rfcs/0001.)'
    )

def _decode_payload_v4(payload_type : str, buf : bytes, blob_store : BlobStore | None = None) -> Any:
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
    cls = _PAYLOAD_REGISTRY.get(payload_type)
    if cls is None:
        # An unregistered FQN may belong to an installed-but-unimported package.
        # A worker normally imports the component that registers it, but host-side
        # tools (``videoflow debug decode``) have no such import; consult entry
        # points once before falling back to opaque bytes.
        plugins.load_plugin_group(PAYLOAD_ENTRY_POINT_GROUP)
        cls = _PAYLOAD_REGISTRY.get(payload_type)
    if cls is not None:
        msg = cls()
        msg.ParseFromString(buf)
        return msg
    # Unknown type: hand back opaque bytes so a forwarding node can re-emit them.
    return RawPayload(payload_type, buf)

def _encode_envelope_v4(producer_name : str, flow_id : str, run_id : str, trace_id : str,
                        seq : int, msg_type : str, metadata : dict | None, payload : Any,
                        span_id : str | None, parent_span_id : str | None, replica_id : int,
                        event_ts : float | None, blob_store : BlobStore | None,
                        blob_readers : int | None = None,
                        blob_ttl_seconds : int | None = None) -> bytes:
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
        env.metadata[k].CopyFrom(_value_to_proto(v, allow_tensor = False))

    if msg_type == MSG_TYPE_EOS:
        env.payload_type = ''
        env.payload = b''
    else:
        payload_type, payload_buf = _encode_payload_v4(payload)
        # Blob offload: over the inline threshold, stash the encoded bytes and carry
        # a small BlobRef in their place (PROTOCOL.md §13).
        if len(payload_buf) > MAX_INLINE_PAYLOAD_BYTES:
            if blob_store is None:
                raise ValueError(
                    f'Payload of {len(payload_buf)} bytes exceeds MAX_INLINE_PAYLOAD_BYTES '
                    f'({MAX_INLINE_PAYLOAD_BYTES}) and no blob_store was configured to offload '
                    'it to. Configure VIDEOFLOW_BLOB_REDIS_URL or pass a BlobStore.')
            ttl = blob_ttl_seconds if blob_ttl_seconds is not None else DEFAULT_BLOB_TTL_SECONDS
            # Reader-counted put (BLOB-5) enables delete-after-last-ack; without a
            # count the blob is TTL-only exactly as before.
            if blob_readers is not None and blob_readers > 0:
                ref = blob_store.put_with_readers(payload_buf, blob_readers, ttl)
            else:
                ref = blob_store.put(payload_buf, ttl)
            blobref = payloads_pb2.BlobRef(ref = ref, inner_payload_type = payload_type, size = len(payload_buf))
            payload_type, payload_buf = PAYLOAD_BLOBREF, blobref.SerializeToString()
        env.payload_type = payload_type
        env.payload = payload_buf
    return env.SerializeToString()

def _decode_envelope_v4(buf : bytes, blob_store : BlobStore | None = None) -> dict:
    env = envelope_pb2.Envelope()
    env.ParseFromString(buf)
    if env.v != 4:
        raise ValueError(f'Unsupported protobuf envelope version {env.v!r}; expected 4')
    msg_type = _PROTO_MSG_TYPE_REV.get(env.type)
    if msg_type is None:
        raise ValueError(f'Unspecified/unknown envelope message type {env.type!r}')
    is_stop_signal = msg_type == MSG_TYPE_EOS
    # Surface the blob ref (if any) so the messenger can release it after the
    # message is acked (BLOB-6). Re-parsing the tiny BlobRef here is cheaper than a
    # second full-envelope parse at the messenger layer.
    blob_ref : str | None = None
    if not is_stop_signal and env.payload_type == PAYLOAD_BLOBREF:
        _br = payloads_pb2.BlobRef()
        _br.ParseFromString(env.payload)
        blob_ref = _br.ref
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
        'blob_ref': blob_ref,
    }

# ==========================================================================
# version-dispatching public API
# ==========================================================================

def encode_envelope(producer_name : str, flow_id : str, run_id : str, trace_id : str,
                    seq : int, msg_type : str, metadata : dict | None, payload : Any,
                    span_id : str = '', parent_span_id : str = '', replica_id : int = 0,
                    event_ts : float | None = None, blob_store : BlobStore | None = None,
                    version : int | None = None, blob_readers : int | None = None,
                    blob_ttl_seconds : int | None = None) -> bytes:
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
        - version: envelope version to emit. The only supported version is ``4`` \
            (protobuf); defaults to ``DEFAULT_ENVELOPE_VERSION``.
        - blob_readers: how many downstream reads an offloaded payload will receive; \
            enables refcounted blob reclamation (BLOB-5). ``None`` ⇒ TTL-only blobs.
        - blob_ttl_seconds: TTL for an offloaded payload (and its counter); \
            ``None`` ⇒ ``DEFAULT_BLOB_TTL_SECONDS``.
    '''
    version = DEFAULT_ENVELOPE_VERSION if version is None else version
    if version == 4:
        return _encode_envelope_v4(producer_name, flow_id, run_id, trace_id, seq, msg_type,
                                metadata, payload, span_id, parent_span_id, replica_id,
                                event_ts, blob_store, blob_readers = blob_readers,
                                blob_ttl_seconds = blob_ttl_seconds)
    raise ValueError(f'Cannot emit envelope version {version!r}; emittable: {EMITTABLE_ENVELOPE_VERSIONS}')

def _is_msgpack_map(first_byte : int) -> bool:
    # The removed legacy v2/v3 wire was a msgpack map at top level: fixmap (0x80-0x8f)
    # for our 12-14 fields, or map16/map32 (0xde/0xdf). A v4 protobuf envelope starts
    # with a field tag (field 1 ``v`` => 0x08) and never lands in these ranges, so this
    # cheaply recognizes a legacy envelope in order to refuse it with a clear error.
    return 0x80 <= first_byte <= 0x8f or first_byte in (0xde, 0xdf)

def decode_envelope(buf : bytes, blob_store : BlobStore | None = None) -> dict:
    '''
    Decodes wire bytes back into a dict with keys ``producer_name``, ``flow_id``, \
        ``run_id``, ``trace_id``, ``seq``, ``event_ts`` (``None`` when absent), \
        ``type``, ``is_stop_signal`` (derived: True iff ``type == MSG_TYPE_EOS``), \
        ``span_id``, ``parent_span_id``, ``replica_id``, ``metadata``, \
        ``message`` (the fully decoded payload — ``None`` for EOS), and \
        ``blob_ref`` (the blob store reference the payload was resolved from, or \
        ``None`` when the payload was inline — lets the caller release the blob \
        after the message is acked, BLOB-6). Only the protobuf \
        v4 envelope is supported; a legacy msgpack (v2/v3) envelope is refused.
    '''
    if not buf:
        raise ValueError('Cannot decode an empty envelope buffer')
    if _is_msgpack_map(buf[0]):
        raise ValueError(
            'Refusing to decode a legacy msgpack (v2/v3) envelope: that wire has been '
            'removed. Re-emit the message on the protobuf v4 wire.')
    return _decode_envelope_v4(buf, blob_store = blob_store)
