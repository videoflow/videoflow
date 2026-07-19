'''
Blob-store selection by URL scheme (videoflow.wire.serialization).

The registry exists so that adding a store (S3, MinIO, a filesystem store for
tests) is a registration rather than an edit to the worker's construction site.
These tests pin the dispatch, the built-in seeding, the actionable error, and the
entry-point fallback.
'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.utils import plugins
from videoflow.wire import serialization as s


class _MemoryBlobStore(s.BlobStore):
    def __init__(self, url = None):
        self.url = url
        self._d = {}

    def put(self, data, ttl_seconds = 3600):
        key = f'mem-{len(self._d)}'
        self._d[key] = data
        return key

    def get(self, ref):
        return self._d[ref]


@pytest.fixture
def registry_sandbox(monkeypatch):
    '''Isolates the module-level registry so a test's registrations do not leak.'''
    monkeypatch.setattr(s, '_BLOB_STORE_SCHEMES', dict(s._BLOB_STORE_SCHEMES))
    monkeypatch.setattr(plugins, '_loaded', set(plugins._loaded))
    return s


def test_redis_schemes_are_registered_by_default():
    assert 'redis' in s.registered_blob_store_schemes()
    assert 'rediss' in s.registered_blob_store_schemes()


def test_dispatches_on_scheme_and_passes_the_full_url(registry_sandbox):
    registry_sandbox.register_blob_store('mem', _MemoryBlobStore)
    store = registry_sandbox.make_blob_store('mem://host/path')
    assert isinstance(store, _MemoryBlobStore)
    # The factory gets the whole URL, not the remainder — it may need the host.
    assert store.url == 'mem://host/path'


def test_scheme_matching_is_case_insensitive(registry_sandbox):
    registry_sandbox.register_blob_store('mem', _MemoryBlobStore)
    assert isinstance(registry_sandbox.make_blob_store('MEM://h'), _MemoryBlobStore)


def test_registration_normalizes_the_scheme_key(registry_sandbox):
    '''
    A store registered under a non-lowercase scheme must still be reachable, or it
    would be listed as known yet fail to resolve — the two sides must normalize the
    same way.
    '''
    registry_sandbox.register_blob_store('S3', _MemoryBlobStore)
    assert 's3' in registry_sandbox.registered_blob_store_schemes()
    assert 'S3' not in registry_sandbox.registered_blob_store_schemes()
    assert isinstance(registry_sandbox.make_blob_store('s3://bucket/key'), _MemoryBlobStore)
    assert isinstance(registry_sandbox.make_blob_store('S3://bucket/key'), _MemoryBlobStore)


def test_unknown_scheme_names_the_fix(registry_sandbox):
    with pytest.raises(ValueError) as excinfo:
        registry_sandbox.make_blob_store('s3://bucket/key')
    msg = str(excinfo.value)
    assert 's3' in msg
    assert 'register_blob_store' in msg      # names the fix
    assert 'redis' in msg                    # names what is known


def test_missing_scheme_is_rejected(registry_sandbox):
    with pytest.raises(ValueError, match = 'scheme'):
        registry_sandbox.make_blob_store('localhost:6379')


def test_unknown_scheme_consults_entry_points_once(registry_sandbox, monkeypatch):
    '''
    A scheme may belong to an installed-but-unimported plugin, so a miss triggers
    exactly one entry-point scan before the error.
    '''
    calls = []

    def fake_load(group):
        calls.append(group)
        registry_sandbox.register_blob_store('lazy', _MemoryBlobStore)

    monkeypatch.setattr(plugins, 'load_plugin_group', fake_load)
    store = registry_sandbox.make_blob_store('lazy://x')
    assert isinstance(store, _MemoryBlobStore)
    assert calls == [registry_sandbox.BLOB_STORE_ENTRY_POINT_GROUP]


def test_registered_store_round_trips_a_payload(registry_sandbox):
    '''The registry hands back a store the codec can actually offload to.'''
    import numpy as np

    registry_sandbox.register_blob_store('mem', _MemoryBlobStore)
    store = registry_sandbox.make_blob_store('mem://x')
    big = np.zeros((512, 1024), dtype = np.uint8)      # over MAX_INLINE_PAYLOAD_BYTES
    buf = s.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {}, big,
                            version = 4, blob_store = store)
    assert len(buf) < 1024                              # only a BlobRef inline
    assert np.array_equal(s.decode_envelope(buf, blob_store = store)['message'], big)


def test_plugin_group_is_scanned_at_most_once(monkeypatch):
    seen = []

    def fake_entry_points(group = None):
        seen.append(group)
        return []

    # Patch the name bound in plugins, not importlib.metadata: plugins imports
    # entry_points at module scope, so the module-level binding is what runs.
    monkeypatch.setattr(plugins, '_loaded', set())
    monkeypatch.setattr(plugins, 'entry_points', fake_entry_points)
    plugins.load_plugin_group('videoflow.test_group')
    plugins.load_plugin_group('videoflow.test_group')
    assert seen == ['videoflow.test_group']


def test_broken_plugin_is_logged_not_raised(monkeypatch):
    '''One broken third-party package must not stop a worker from starting.'''
    class _BadEntryPoint:
        name = 'bad'

        def load(self):
            raise RuntimeError('boom')

    monkeypatch.setattr(plugins, '_loaded', set())
    monkeypatch.setattr(plugins, 'entry_points', lambda group = None: [_BadEntryPoint()])
    plugins.load_plugin_group('videoflow.test_group')   # must not raise


if __name__ == '__main__':
    pytest.main([__file__])
