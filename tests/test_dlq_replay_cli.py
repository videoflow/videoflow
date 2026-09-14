'''
``videoflow dlq replay`` routes from envelope metadata and verifies payload bytes
separately (PAY-015, DELIV-16): the target of a dead letter is decided without a
payload store, an offloaded entry is never skipped for lack of one, and a real
replay with ``--blob-redis-url`` refuses — typed — when the store cannot return a
payload, leaving every entry in place.
'''
from __future__ import absolute_import, division, print_function

import argparse

import pytest

from videoflow.core.errors import ResourceUnavailable
from videoflow.deploy import cli
from videoflow.messaging.topology import subject_for
from videoflow.wire.serialization import MSG_TYPE_DATA, encode_envelope


class _Offloading:
    '''A blob store that offloads every payload and remembers what it was asked for.'''
    def __init__(self, available = True):
        self.available = available
        self.gets = []

    def put(self, data, ttl):
        return 'vf-blob-deadbeef'

    def put_with_readers(self, data, readers, ttl):
        return 'vf-blob-deadbeef'

    def get(self, key):
        self.gets.append(key)
        if not self.available:
            raise ConnectionError('store down')
        return b'x' * 10


def _entries():
    inline = encode_envelope('parent', 'f', 'r', 't1', 1, MSG_TYPE_DATA, None, {'v': 1})
    offloaded = encode_envelope('parent', 'f', 'r', 't2', 2, MSG_TYPE_DATA, None, b'x' * 10,
                                blob_store = _Offloading(), inline_threshold = 1)
    headers = {'VF-Origin-Node': 'child', 'VF-Run-Id': 'r', 'VF-Code': 'VF_POISON_SCHEMA'}
    return [('dlq.sub', dict(headers), inline), ('dlq.sub', dict(headers), offloaded),
            ('dlq.sub', dict(headers), b'\x80\x81 not an envelope')]


def _args(**overrides):
    base = dict(nats = 'nats://unused:4222', flow_id = 'f', run_id = 'r', node = None, limit = 10, code = None,
                to_run = 'r2', dry_run = False, blob_redis_url = None)
    base.update(overrides)
    return argparse.Namespace(**base)


def test_dry_run_routes_inline_and_offloaded_entries_without_a_store(monkeypatch, capsys):
    monkeypatch.setattr(cli, '_dlq_fetch', lambda *a: _entries())
    cli._cmd_dlq_replay(_args(dry_run = True))
    out = capsys.readouterr()
    target = subject_for('f', 'r2', 'parent')
    assert out.out.count(f'on {target}') == 2                       # both decodable entries, same parent subject
    assert 'payload vf-blob-deadbeef offloaded' in out.out
    assert '2 message(s) would be replayed' in out.out
    assert '1 entry(ies) could not be decoded' in out.err


def test_replay_refuses_typed_when_an_offloaded_payload_cannot_be_read(monkeypatch):
    monkeypatch.setattr(cli, '_dlq_fetch', lambda *a: _entries())
    store = _Offloading(available = False)
    import videoflow.wire.serialization as ser
    monkeypatch.setattr(ser, 'make_blob_store', lambda url: store)
    published = []
    monkeypatch.setattr(cli.asyncio, 'run', lambda coro: published.append(coro) or coro.close())
    with pytest.raises(ResourceUnavailable) as e:
        cli._cmd_dlq_replay(_args(blob_redis_url = 'redis://x:6379/0'))
    assert e.value.code == 'VF_RESOURCE_UNAVAILABLE' and 'vf-blob-deadbeef' in str(e.value)
    assert store.gets == ['vf-blob-deadbeef']                       # the inline entry needed no read
    assert published == []                                          # nothing was replayed
