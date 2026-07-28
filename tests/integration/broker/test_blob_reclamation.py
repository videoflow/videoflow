'''
Refcounted blob reclamation end-to-end against a live NATS + Redis
(RFC 0002, PROTOCOL.md BLOB-5/6/7): blobs written by a publisher with a reader
count are deleted from Redis as soon as every downstream reader acks — and are
deliberately *not* deleted on partial consumption or dead-letter.

These drive NATSMessenger instances directly (no worker subprocesses), like
test_ack_semantics.py. Needs NATS (conftest skips otherwise) and Redis; the Redis
probe is a raw socket connect for the same reason as the NATS one — a client
library's retry loop at collection time is exactly the trap conftest documents.
'''
import asyncio
import hashlib
import os
import socket
import time
import uuid
from urllib.parse import urlparse

import numpy as np
import pytest

from videoflow.core.compiler import NodeSpec
from videoflow.core.constants import BATCH
from videoflow.messaging import topology
from videoflow.messaging.nats_messenger import NATSMessenger
from videoflow.messaging.topology import provision_flow_sync
from videoflow.wire.serialization import (
    MSG_TYPE_DATA,
    RedisBlobStore,
    derive_message_id,
    encode_envelope,
)

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')
REDIS_URL = os.environ.get('VF_TEST_REDIS_URL', 'redis://localhost:6379/0')

def _redis_available(url = REDIS_URL) -> bool:
    parsed = urlparse(url)
    try:
        with socket.create_connection((parsed.hostname or 'localhost', parsed.port or 6379),
                                      timeout = 1):
            return True
    except OSError:
        return False

pytestmark = pytest.mark.skipif(not _redis_available(),
                                reason = f'Redis not reachable at {REDIS_URL}')

#: Big enough to exceed the 512KiB inline threshold, so every publish offloads.
BIG = np.zeros((700, 1024), dtype = np.uint8)

class _StubNode:
    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name

def _spec(name, parents, kind, has_children, nb_tasks = 1, partition_by = None,
          blob_readers = None):
    return NodeSpec(name = name, node_class = 'videoflow.processors.basic.IdentityProcessor',
                    params = {}, parents = parents, kind = kind, has_children = has_children,
                    nb_tasks = nb_tasks, device_type = 'cpu', is_finite = True,
                    partition_by = partition_by, blob_readers = blob_readers)

def _publish_blob_message(store, flow_id, run_id, parent, trace, seq, blob_readers,
                          ttl_seconds = 60):
    '''One over-threshold data envelope on the parent's stream, reader-counted (BLOB-5).'''
    import nats

    async def _go():
        nc = await nats.connect(NATS_URL)
        js = nc.jetstream()
        buf = encode_envelope(parent, flow_id, run_id, trace, seq, MSG_TYPE_DATA, None, BIG,
                              blob_store = store, blob_readers = blob_readers,
                              blob_ttl_seconds = ttl_seconds)
        mid = derive_message_id(flow_id, run_id, parent, trace, seq, MSG_TYPE_DATA)
        await js.publish(topology.subject_for(flow_id, run_id, parent), buf,
                         headers = {'Nats-Msg-Id': mid})
        await nc.drain()

    asyncio.run(_go())

def _blob_keys(client):
    return sorted(k.decode() for k in client.keys('vf-blob*'))

def _wait_until(pred, timeout = 10.0, interval = 0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return pred()

def _cleanup(flow_id, run_id, client, refs):
    import nats

    async def _go():
        nc = await nats.connect(NATS_URL)
        await topology.delete_run_streams(nc, flow_id, run_id)
        await nc.drain()

    asyncio.run(_go())
    for ref in refs:
        client.delete(ref, 'vf-blobrc-' + ref.removeprefix('vf-blob-'))

@pytest.fixture
def store():
    pytest.importorskip('redis')
    return RedisBlobStore(REDIS_URL)

def test_fanout_blob_deleted_after_every_child_acks(store):
    '''Two children, both ack → blob and counter gone well before their TTL (BLOB-6).'''
    flow_id, run_id = 'blobrc', uuid.uuid4().hex[:8]
    specs = [_spec('parent', [], 'producer', True, blob_readers = 2),
             _spec('c1', ['parent'], 'consumer', False),
             _spec('c2', ['parent'], 'consumer', False)]
    provision_flow_sync(NATS_URL, specs, flow_id, run_id, BATCH)
    m1 = NATSMessenger(_StubNode('c1'), ['parent'], NATS_URL, flow_id, BATCH, run_id,
                       blob_store = store)
    m2 = NATSMessenger(_StubNode('c2'), ['parent'], NATS_URL, flow_id, BATCH, run_id,
                       blob_store = store)
    ref = None
    try:
        _publish_blob_message(store, flow_id, run_id, 'parent', 't1', 1, blob_readers = 2)
        for m in (m1, m2):
            inputs = m.receive_message()
            assert inputs['parent']['message'].shape == BIG.shape
        ref = store._client.keys('vf-blob-*')
        m1.ack_inputs()
        # One of two readers acked: blob must still resolve for the other (BLOB-6).
        rc_keys = store._client.keys('vf-blobrc-*')
        assert rc_keys and int(store._client.get(rc_keys[0])) == 1
        m2.ack_inputs()
        assert _wait_until(lambda: not store._client.keys('vf-blob*')), \
            f'blob keys survived both acks: {_blob_keys(store._client)}'
    finally:
        m1.close()
        m2.close()
        _cleanup(flow_id, run_id, store._client, [k.decode() for k in (ref or [])])

def test_partial_consumption_keeps_the_blob(store):
    '''Only one of two counted readers acks → blob survives (its TTL is the backstop, BLOB-7).'''
    flow_id, run_id = 'blobrc', uuid.uuid4().hex[:8]
    specs = [_spec('parent', [], 'producer', True, blob_readers = 2),
             _spec('c1', ['parent'], 'consumer', False)]
    provision_flow_sync(NATS_URL, specs, flow_id, run_id, BATCH)
    m1 = NATSMessenger(_StubNode('c1'), ['parent'], NATS_URL, flow_id, BATCH, run_id,
                       blob_store = store)
    refs = []
    try:
        _publish_blob_message(store, flow_id, run_id, 'parent', 't1', 1, blob_readers = 2)
        m1.receive_message()
        m1.ack_inputs()
        refs = [k.decode() for k in store._client.keys('vf-blob-*')]
        assert refs, 'blob was deleted with an outstanding reader'
        rc = 'vf-blobrc-' + refs[0].removeprefix('vf-blob-')
        assert int(store._client.get(rc)) == 1
    finally:
        m1.close()
        _cleanup(flow_id, run_id, store._client, refs)

def test_dead_letter_preserves_the_blob(store):
    '''fail_inputs → DLQ must NOT release: DLQ inspection re-reads the blob (BLOB-6).'''
    flow_id, run_id = 'blobrc', uuid.uuid4().hex[:8]
    specs = [_spec('parent', [], 'producer', True, blob_readers = 1),
             _spec('c1', ['parent'], 'consumer', False)]
    provision_flow_sync(NATS_URL, specs, flow_id, run_id, BATCH, max_retries = 0)
    m1 = NATSMessenger(_StubNode('c1'), ['parent'], NATS_URL, flow_id, BATCH, run_id,
                       blob_store = store, max_retries = 0)
    refs = []
    try:
        _publish_blob_message(store, flow_id, run_id, 'parent', 't1', 1, blob_readers = 1)
        m1.receive_message()
        m1.fail_inputs(ValueError('permanent boom'))
        time.sleep(0.5)
        refs = [k.decode() for k in store._client.keys('vf-blob-*')]
        assert refs, 'dead-lettering must leave the blob to its TTL, not delete it'
    finally:
        m1.close()
        _cleanup(flow_id, run_id, store._client, refs)

def test_partitioned_ack_skip_counts_as_a_release(store):
    '''
    A partitioned child: every replica decodes every message; the non-owner
    ack-skips. Both the owner's ack and the skip's ack must decrement, so the
    blob is gone once both replicas are done (BLOB-5/6).
    '''
    flow_id, run_id = 'blobrc', uuid.uuid4().hex[:8]
    specs = [_spec('parent', [], 'producer', True, blob_readers = 2),
             _spec('c1', ['parent'], 'consumer', False, nb_tasks = 2,
                   partition_by = 'trace_id')]
    provision_flow_sync(NATS_URL, specs, flow_id, run_id, BATCH)
    # Stable ownership (sha256, same arithmetic as NATSMessenger._owns): pick the
    # owning replica for trace 't1' so the test doesn't depend on hash luck.
    owner = int(hashlib.sha256(b't1').hexdigest()[:8], 16) % 2
    replicas = [NATSMessenger(_StubNode('c1'), ['parent'], NATS_URL, flow_id, BATCH, run_id,
                              blob_store = store, nb_tasks = 2, partition_by = 'trace_id',
                              replica_id = r) for r in (0, 1)]
    try:
        _publish_blob_message(store, flow_id, run_id, 'parent', 't1', 1, blob_readers = 2)
        # The owner processes and acks; the non-owner's pull loop ack-skips on its own.
        inputs = replicas[owner].receive_message()
        assert inputs['parent']['message'].shape == BIG.shape
        replicas[owner].ack_inputs()
        assert _wait_until(lambda: not store._client.keys('vf-blob*')), \
            f'blob keys survived owner ack + non-owner skip: {_blob_keys(store._client)}'
    finally:
        for m in replicas:
            m.close()
        _cleanup(flow_id, run_id, store._client, [])
