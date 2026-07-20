'''
Refcounted blob reclamation (RFC 0002, PROTOCOL.md BLOB-5/6/7).

Three layers, no live Redis or broker:
- ``RedisBlobStore`` counter arithmetic against a dict-backed fake redis client
  (put_with_readers writes the counter, release decrements and deletes at zero,
  a counterless blob is never touched).
- ``BlobStore`` base-class defaults — a third-party store that only implements
  ``put``/``get`` must keep working when the encoder starts calling
  ``put_with_readers`` and the messenger starts calling ``release``.
- ``_AckHandle`` release discipline — release exactly once on ack success, never
  on nak/term/failed ack.
'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.wire import serialization as s


class _FakeRedis:
    '''The five commands RedisBlobStore uses, over a plain dict (ttl tracked, never enforced).'''
    def __init__(self):
        self.data = {}
        self.ttls = {}

    def set(self, key, value, ex = None):
        self.data[key] = value
        self.ttls[key] = ex

    def get(self, key):
        return self.data.get(key)

    def exists(self, key):
        return 1 if key in self.data else 0

    def decr(self, key):
        self.data[key] = int(self.data.get(key, 0)) - 1
        return self.data[key]

    def unlink(self, key):
        self.data.pop(key, None)
        self.ttls.pop(key, None)


def _store():
    store = s.RedisBlobStore.__new__(s.RedisBlobStore)  # skip __init__: no redis import/server
    store._client = _FakeRedis()
    return store


def test_put_with_readers_writes_counter():
    store = _store()
    ref = store.put_with_readers(b'payload', 3, ttl_seconds = 120)
    rc = 'vf-blobrc-' + ref.removeprefix('vf-blob-')
    fake = store._client
    assert fake.data[ref] == b'payload'
    assert int(fake.data[rc]) == 3
    # BLOB-5: counter shares the blob's TTL, so both expire together as backstop.
    assert fake.ttls[ref] == 120 and fake.ttls[rc] == 120


def test_put_with_zero_readers_writes_no_counter():
    # A leaf node has 0 readers; a counter of 0 would be deleted by the first
    # release of an unrelated blob generation. No counter ⇒ TTL-only.
    store = _store()
    ref = store.put_with_readers(b'payload', 0)
    assert ref in store._client.data
    assert not any(k.startswith('vf-blobrc-') for k in store._client.data)


def test_release_decrements_then_deletes_at_zero():
    store = _store()
    ref = store.put_with_readers(b'payload', 2)
    rc = 'vf-blobrc-' + ref.removeprefix('vf-blob-')
    store.release(ref)
    assert ref in store._client.data and int(store._client.data[rc]) == 1
    store.release(ref)
    # Last reader: blob and counter both gone (BLOB-6).
    assert ref not in store._client.data and rc not in store._client.data


def test_release_without_counter_is_noop():
    # BLOB-6/7: a counterless blob (old publisher, expired counter, VF_BLOB_READERS
    # unset) belongs to its TTL. release must not DECR-create a key or delete it.
    store = _store()
    ref = store.put(b'payload')
    store.release(ref)
    assert ref in store._client.data
    assert not any(k.startswith('vf-blobrc-') for k in store._client.data)


class _PutGetOnlyStore(s.BlobStore):
    '''A third-party store predating RFC 0002: implements only the original interface.'''
    def __init__(self):
        self.d = {}
        self.puts = []

    def put(self, data, ttl_seconds = 3600):
        key = f'k{len(self.d)}'
        self.d[key] = data
        self.puts.append(ttl_seconds)
        return key

    def get(self, ref):
        return self.d[ref]


def test_base_class_defaults_keep_put_get_stores_working():
    store = _PutGetOnlyStore()
    # put_with_readers degrades to plain put (readers ignored, ttl forwarded)...
    ref = store.put_with_readers(b'x', 5, ttl_seconds = 42)
    assert store.d[ref] == b'x' and store.puts == [42]
    # ...and release is a no-op rather than an AttributeError.
    store.release(ref)
    assert store.d[ref] == b'x'


def test_encode_envelope_routes_readers_and_ttl_to_the_store():
    import numpy as np

    class _Recording(_PutGetOnlyStore):
        def __init__(self):
            super().__init__()
            self.calls = []

        def put_with_readers(self, data, readers, ttl_seconds = s.DEFAULT_BLOB_TTL_SECONDS):
            self.calls.append(('put_with_readers', readers, ttl_seconds))
            return self.put(data, ttl_seconds)

    big = np.zeros((600, 1024), dtype = np.uint8)  # over the 512KiB inline threshold
    store = _Recording()
    s.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {}, big,
                      version = 4, blob_store = store,
                      blob_readers = 2, blob_ttl_seconds = 99)
    assert store.calls == [('put_with_readers', 2, 99)]
    # Without a reader count the encoder uses plain put (TTL-only, BLOB-5).
    store2 = _Recording()
    s.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {}, big,
                      version = 4, blob_store = store2)
    assert store2.calls == [] and store2.puts == [s.DEFAULT_BLOB_TTL_SECONDS]


def test_decode_envelope_surfaces_blob_ref():
    import numpy as np

    store = _PutGetOnlyStore()
    big = np.zeros((600, 1024), dtype = np.uint8)
    buf = s.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {}, big,
                            version = 4, blob_store = store)
    d = s.decode_envelope(buf, blob_store = store)
    assert d['blob_ref'] in store.d
    # Inline payloads carry no ref.
    small = s.encode_envelope('n', 'f', 'r', 't', 2, s.MSG_TYPE_DATA, {}, 5, version = 4)
    assert s.decode_envelope(small)['blob_ref'] is None


# -- _AckHandle release discipline ----------------------------------------

class _StubMessenger:
    '''Just enough of NATSMessenger for _AckHandle: forget + release recording.'''
    def __init__(self):
        self.released = []

    def _forget_handle(self, handle):
        pass

    def _release_blob(self, blob_ref):
        self.released.append(blob_ref)


class _StubMsg:
    def __init__(self, fail_ack = False):
        self._fail_ack = fail_ack
        self.acked = 0

    async def ack(self):
        if self._fail_ack:
            raise RuntimeError('broker gone')
        self.acked += 1

    async def nak(self, delay = None):
        pass

    async def term(self):
        pass


def _handle(msg, messenger, blob_ref):
    import asyncio
    import threading

    pytest.importorskip('nats')   # the messenger module imports the optional extra
    from videoflow.messaging.nats_messenger import _AckHandle

    # _AckHandle._run bridges to the messenger's event loop; give the stub one.
    loop = asyncio.new_event_loop()
    t = threading.Thread(target = loop.run_forever, daemon = True)
    t.start()
    messenger._loop = loop
    h = _AckHandle(msg, messenger, blob_ref = blob_ref)
    return h, loop


def test_ack_releases_exactly_once():
    m = _StubMessenger()
    msg = _StubMsg()
    h, loop = _handle(msg, m, 'vf-blob-abc')
    try:
        h.ack()
        h.ack()  # idempotent: _resolved guard
        assert msg.acked == 1
        assert m.released == ['vf-blob-abc']
    finally:
        loop.call_soon_threadsafe(loop.stop)


def test_failed_ack_does_not_release():
    # BLOB-6: a failed ack may mean redelivery, and a redelivery re-reads the blob.
    m = _StubMessenger()
    h, loop = _handle(_StubMsg(fail_ack = True), m, 'vf-blob-abc')
    try:
        h.ack()
        assert m.released == []
    finally:
        loop.call_soon_threadsafe(loop.stop)


def test_nak_and_term_never_release():
    for resolve in ('nak', 'term'):
        m = _StubMessenger()
        h, loop = _handle(_StubMsg(), m, 'vf-blob-abc')
        try:
            getattr(h, resolve)()
            assert m.released == [], resolve
        finally:
            loop.call_soon_threadsafe(loop.stop)


# -- messenger TTL resolution (BLOB-7) -------------------------------------

def test_messenger_ttl_defaults_by_flow_type(monkeypatch):
    pytest.importorskip('nats')
    from videoflow.core.constants import BATCH, REALTIME
    from videoflow.messaging import nats_messenger as nm
    from videoflow.producers import IntProducer

    # __init__ runs _setup() (broker connect + stream provisioning) on its loop;
    # stub it out — this test is about the TTL/readers attribute resolution only.
    async def _no_setup(self):
        pass
    monkeypatch.setattr(nm.NATSMessenger, '_setup', _no_setup)

    made = []
    def make(flow_type, **kwargs):
        m = nm.NATSMessenger(IntProducer(0, 1, name = 'p'), [], 'nats://x:4222',
                             'f', flow_type, 'r', **kwargs)
        made.append(m)
        return m

    try:
        assert make(REALTIME)._blob_ttl_seconds == nm.DEFAULT_BLOB_TTL_REALTIME_SECONDS
        assert make(BATCH)._blob_ttl_seconds == nm.DEFAULT_BLOB_TTL_BATCH_SECONDS
        # An explicit override wins over both.
        assert make(BATCH, blob_ttl_seconds = 7)._blob_ttl_seconds == 7
        # And the reader count is stored as given (None ⇒ reclamation off).
        assert make(REALTIME)._blob_readers is None
        assert make(REALTIME, blob_readers = 3)._blob_readers == 3
    finally:
        for m in made:
            m._loop.call_soon_threadsafe(m._loop.stop)
