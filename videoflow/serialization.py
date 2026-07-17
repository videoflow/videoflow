'''
Wire format used to move messages between nodes over the message broker
(``videoflow.messaging.nats_messenger.NATSMessenger``). Not used by anything
running purely in a single local process — it's the boundary format for bytes
that cross a network/process boundary.
'''
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import os
import pickle

import msgpack
import numpy as np

CODEC_RAW_NDARRAY = 'raw-ndarray'
CODEC_PICKLE = 'pickle'
CODEC_EXTERNAL_REF = 'external-ref'

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
    def __init__(self, url : str = None):
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
        return data

def _encode_ndarray(arr : np.ndarray) -> bytes:
    return msgpack.packb(
        {'shape': list(arr.shape), 'dtype': str(arr.dtype), 'data': arr.tobytes()},
        use_bin_type = True
    )

def _decode_ndarray(buf : bytes) -> np.ndarray:
    d = msgpack.unpackb(buf, raw = False)
    return np.frombuffer(d['data'], dtype = np.dtype(d['dtype'])).reshape(d['shape'])

def encode_payload(payload, blob_store : BlobStore = None):
    '''
    Encodes an arbitrary payload (numpy array, or any picklable Python object) into \
        ``(codec, bytes)``. If the encoded size exceeds ``MAX_INLINE_PAYLOAD_BYTES`` \
        and a ``blob_store`` is given, the bytes are written there instead and a \
        small reference is returned in their place.
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

def decode_payload(codec : str, buf : bytes, blob_store : BlobStore = None):
    '''Inverse of ``encode_payload``.'''
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

def encode_envelope(producer_name : str, flow_id : str, trace_id : str, seq : int,
                    is_stop_signal : bool, metadata : dict, payload, blob_store : BlobStore = None) -> bytes:
    '''
    Encodes a full wire message: a small msgpack header plus the (possibly \
        blob-referenced) payload, as a single msgpack-encoded byte string suitable \
        for publishing directly to a broker subject.
    '''
    if is_stop_signal:
        payload_codec, payload_buf = CODEC_PICKLE, b''
    else:
        payload_codec, payload_buf = encode_payload(payload, blob_store = blob_store)

    envelope = {
        'producer_name': producer_name,
        'flow_id': flow_id,
        'trace_id': trace_id,
        'seq': seq,
        'is_stop_signal': is_stop_signal,
        'metadata': metadata,
        'payload_codec': payload_codec,
        'payload': payload_buf,
    }
    return msgpack.packb(envelope, use_bin_type = True)

def decode_envelope(buf : bytes, blob_store : BlobStore = None) -> dict:
    '''
    Decodes wire bytes back into a dict with keys ``producer_name``, ``flow_id``, \
        ``trace_id``, ``seq``, ``is_stop_signal``, ``metadata``, and ``message`` \
        (the fully decoded payload — absent/``None`` for stop-signal envelopes).
    '''
    envelope = msgpack.unpackb(buf, raw = False)
    if envelope['is_stop_signal']:
        message = None
    else:
        message = decode_payload(envelope['payload_codec'], envelope['payload'], blob_store = blob_store)
    return {
        'producer_name': envelope['producer_name'],
        'flow_id': envelope['flow_id'],
        'trace_id': envelope['trace_id'],
        'seq': envelope['seq'],
        'is_stop_signal': envelope['is_stop_signal'],
        'metadata': envelope['metadata'],
        'message': message,
    }
