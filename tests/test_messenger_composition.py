'''
The composed messenger's Phase-2 seams, without a broker: the negotiated inline
threshold (PAY-001), drops reported from below the ``Messenger`` seam, the
loop-side admission decisions (ownership and replay scope) and their release
after a confirmed skip, the payload-free envelope peek, and the reader
obligation ids the compiler derives (BLOB-13).
'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.backends.messaging import ChannelId, Delivery, DeliveryToken, SubscriptionId
from videoflow.backends.outcomes import known, unknown
from videoflow.core import constants
from videoflow.core.compiler import NodeSpec, blob_reader_ids
from videoflow.core.constants import BATCH
from videoflow.core.errors import ConfigError
from videoflow.core.policies import JoinPolicy
from videoflow.messaging.grouping import TraceGroupAssembler
from videoflow.messaging.nats_messenger import NATSMessenger
from videoflow.wire.serialization import (
    ENVELOPE_OVERHEAD_BYTES,
    MAX_INLINE_PAYLOAD_BYTES,
    MSG_TYPE_DATA,
    MSG_TYPE_EOS,
    encode_envelope,
    peek_envelope,
)


class _Node:
    name = 'child'


def _bare(partition_by = None, nb_tasks = 1, replica_id = 0, payload_store = None, blob_store = None):
    '''Just the attributes the seams under test read; no backend, no broker.'''
    m = NATSMessenger.__new__(NATSMessenger)
    m._node = _Node()
    m._partition_by = partition_by if (partition_by and nb_tasks > 1) else None
    m._nb_tasks = nb_tasks
    m._replica_id = replica_id
    m._payload_store = payload_store
    m._blob_store = blob_store
    m._inline_threshold = MAX_INLINE_PAYLOAD_BYTES
    m._drops = {}
    m._evictions_seen = 0
    m._assembler = TraceGroupAssembler('child', ['p'], JoinPolicy.default_for(BATCH))
    return m


def _delivery(buf, headers = None, seq = 1):
    sub = SubscriptionId(ChannelId('f', 'r', 'p'), 'child', None, 'data')
    return Delivery(DeliveryToken(sub, f'm{seq}', seq, 1, 'g'), buf, len(buf), 0.0, dict(headers or {}))


# -- inline threshold negotiation (PAY-001) ---------------------------------------------

def test_threshold_is_lowered_to_what_the_broker_carries_when_a_store_can_offload():
    m = _bare(blob_store = object())
    m._negotiate_inline_threshold(known(200_000))
    assert m._inline_threshold == 200_000 - ENVELOPE_OVERHEAD_BYTES


def test_threshold_is_untouched_by_a_roomy_or_unread_limit():
    m = _bare(blob_store = object())
    m._negotiate_inline_threshold(known(1 << 30))
    assert m._inline_threshold == MAX_INLINE_PAYLOAD_BYTES
    m._negotiate_inline_threshold(unknown('unread', 'not connected'))
    assert m._inline_threshold == MAX_INLINE_PAYLOAD_BYTES


def test_unsafe_threshold_without_a_store_is_a_config_error_under_the_switch(monkeypatch):
    m = _bare()
    monkeypatch.setattr(constants, 'RFC0006', True)
    with pytest.raises(ConfigError) as e:
        m._negotiate_inline_threshold(known(200_000))
    assert 'VIDEOFLOW_MAX_INLINE_PAYLOAD_BYTES' in e.value.remedy
    # Off: lowered with a warning, so a publish is never refused after the fact.
    monkeypatch.setattr(constants, 'RFC0006', False)
    m._negotiate_inline_threshold(known(200_000))
    assert m._inline_threshold == 200_000 - ENVELOPE_OVERHEAD_BYTES


def test_encoder_honours_a_negotiated_threshold():
    big = b'x' * 10_000
    # Below the module threshold, so inline by default...
    inline = encode_envelope('p', 'f', 'r', 't', 1, MSG_TYPE_DATA, None, big)
    assert peek_envelope(inline)['blob_ref'] is None
    # ...and refused at a lower negotiated one when nothing can offload it.
    with pytest.raises(ValueError):
        encode_envelope('p', 'f', 'r', 't', 1, MSG_TYPE_DATA, None, big, inline_threshold = 1_000)


# -- the payload-free peek -----------------------------------------------------------------

def test_peek_reports_routing_fields_without_the_payload():
    buf = encode_envelope('p', 'f', 'r', 'trace-9', 9, MSG_TYPE_DATA, {'k': 'v'}, {'value': list(range(1000))},
                          event_ts = 12.5)
    peeked = peek_envelope(buf)
    assert peeked['trace_id'] == 'trace-9' and peeked['seq'] == 9 and peeked['metadata'] == {'k': 'v'}
    assert peeked['message'] is None and peeked['hydrated'] is False and peeked['blob_ref'] is None
    eos = peek_envelope(encode_envelope('p', 'f', 'r', 'eos-r0', 9, MSG_TYPE_EOS, None, None))
    assert eos['is_stop_signal'] and eos['hydrated']


# -- loop-side admission ------------------------------------------------------------------

def _owner_of(trace, nb_tasks = 2):
    import hashlib
    return int(hashlib.sha256(trace.encode()).hexdigest()[:8], 16) % nb_tasks


def test_partitioned_replicas_admit_only_what_they_own():
    buf = encode_envelope('p', 'f', 'r', 't1', 1, MSG_TYPE_DATA, None, {'v': 1})
    owner = _owner_of('t1')
    decisions = {r: _bare('trace_id', 2, r)._admit_on_loop(_delivery(buf)) for r in (0, 1)}
    assert decisions == {owner: True, 1 - owner: False}
    # Terminators and undecodable bytes are always admitted: the receiving side classifies them.
    eos = encode_envelope('p', 'f', 'r', 'eos-r0', 1, MSG_TYPE_EOS, None, None)
    assert _bare('trace_id', 2, 1 - owner)._admit_on_loop(_delivery(eos))
    assert _bare('trace_id', 2, 1 - owner)._admit_on_loop(_delivery(b'\x80\x81 garbage'))


def test_replay_target_is_honoured_only_under_the_switch(monkeypatch):
    buf = encode_envelope('p', 'f', 'r', 't1', 1, MSG_TYPE_DATA, None, {'v': 1})
    m = _bare()
    monkeypatch.setattr(constants, 'RFC0006', True)
    assert m._admit_on_loop(_delivery(buf, {'VF-Replay-Target': 'sibling'})) is False
    assert m._admit_on_loop(_delivery(buf, {'VF-Replay-Target': 'child'})) is True
    monkeypatch.setattr(constants, 'RFC0006', False)
    assert m._admit_on_loop(_delivery(buf, {'VF-Replay-Target': 'sibling'})) is True


class _Refcount:
    def __init__(self):
        self.released = []

    def release(self, key):
        self.released.append(key)


class _Blobs:
    '''A blob store that offloads everything, so the envelope carries a reference.'''
    def put(self, data, ttl):
        return 'vf-blob-abc'

    def put_with_readers(self, data, readers, ttl):
        return 'vf-blob-abc'


def test_a_confirmed_skip_releases_the_share_but_a_scoped_replay_does_not(monkeypatch):
    store = _Refcount()
    m = _bare('trace_id', 2, 0, blob_store = store)
    buf = encode_envelope('p', 'f', 'r', 't1', 1, MSG_TYPE_DATA, None, b'x' * 10, blob_store = _Blobs(),
                          inline_threshold = 1)
    assert peek_envelope(buf)['blob_ref'] == 'vf-blob-abc'
    m._on_skipped(_delivery(buf))
    assert store.released == ['vf-blob-abc']
    monkeypatch.setattr(constants, 'RFC0006', True)
    m._on_skipped(_delivery(buf, {'VF-Replay-Target': 'sibling'}))
    assert store.released == ['vf-blob-abc']                    # nothing acquired, nothing released


# -- drops from below the seam -------------------------------------------------------------

def test_take_drops_hands_over_counts_once_including_join_evictions():
    m = _bare()
    m._count_drop('exhausted')
    m._count_drop('exhausted')
    m._count_drop('publish_discarded')
    m._assembler.evictions = 3
    assert m.take_drops() == {'exhausted': 2, 'publish_discarded': 1, 'join_evicted': 3}
    assert m.take_drops() == {}
    m._assembler.evictions = 4
    assert m.take_drops() == {'join_evicted': 1}


# -- reader obligation ids (BLOB-13) --------------------------------------------------------

def _spec(name, parents, nb_tasks = 1, partition_by = None):
    return NodeSpec(name = name, node_class = 'x', params = {}, parents = parents, kind = 'processor',
                    has_children = True, nb_tasks = nb_tasks, device_type = 'cpu', is_finite = True,
                    partition_by = partition_by)


def test_reader_ids_name_competing_children_once_and_partitioned_replicas_each():
    specs = [_spec('src', []), _spec('det', ['src'], nb_tasks = 3), _spec('track', ['src'], 2, 'trace_id'),
             _spec('other', ['det'])]
    assert blob_reader_ids(specs[0], specs) == ['det', 'track/p0', 'track/p1']
    assert blob_reader_ids(specs[1], specs) == ['other']
    assert blob_reader_ids(specs[3], specs) == []
