'''
Exact-ownership teardown and RFC 0006 owner metadata (videoflow.messaging.topology).

Drives ``delete_run_streams``, ``_ensure_stream`` and ``_ensure_consumer`` against
an in-memory JetStream manager that speaks the real ``nats.js.api`` objects and
answers the way the server does: configs are stored and listed in wire form
(``as_dict()`` -> JSON -> ``from_response``), so metadata and the second/nanosecond
duration conversions round-trip exactly as they would over the API, and stream
listings come back as ``StreamsListIterator`` pages with the server's ``total``.
No broker.

The naming and config functions are pinned by tests/test_topology_naming.py,
which this file leaves alone: with ``RFC0006`` off every config here must be
what it was before ownership metadata existed.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import json
import logging

import pytest
from nats.js.api import ConsumerConfig, ConsumerInfo, RetentionPolicy, StreamConfig, StreamInfo, StreamsListIterator
from nats.js.errors import NotFoundError

from videoflow.backends.identity import (
    LABEL_FLOW,
    LABEL_GENERATION,
    LABEL_KIND,
    LABEL_NODE,
    LABEL_RUN,
    flow_labels,
    owner_labels,
)
from videoflow.backends.outcomes import CleanupObservation
from videoflow.core import constants
from videoflow.core.compiler import NodeSpec
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.errors import BrokerUnavailable, IncompatibleProfile
from videoflow.messaging import topology

_STATE = {'messages': 0, 'bytes': 0, 'first_seq': 0, 'last_seq': 0, 'consumer_count': 0}
_CREATED = '2026-09-11T00:00:00Z'
_LOGGER = 'videoflow.messaging'

def _run(coro):
    return asyncio.run(coro)

def _wire(config):
    '''The request as the server sees it: JSON, so str enums become their values and durations nanoseconds.'''
    return json.loads(json.dumps(config.as_dict()))

def _applied_stream(config, **overrides):
    '''The StreamConfig the server holds after applying ``config`` (a wire round trip), plus any server-side changes.'''
    return StreamConfig.from_response(_wire(config)).evolve(**overrides)

def _applied_consumer(config, **overrides):
    return ConsumerConfig.from_response(_wire(config)).evolve(**overrides)

class FakeJetStream:
    '''
    The management surface of ``nats.js.JetStreamContext`` that topology uses,
    in memory. Faults are injected by setting the ``*_error`` attributes; deletes
    can be made to fail once per name (``delete_errors``); ``create_overrides``
    plays a server that clamps or drops a requested field; ``server_metadata``
    plays nats-server adding its own ``_nats.*`` metadata keys.
    '''
    def __init__(self, page_size = 256):
        self.streams = {}
        self.consumers = {}
        self.calls = []
        self.deleted = []
        self.page_size = page_size
        self.listing_error = None
        self.delete_errors = {}
        self.add_stream_error = None
        self.update_stream_error = None
        self.add_consumer_error = None
        self.create_overrides = {}
        self.server_metadata = {}

    def put_stream(self, config):
        self.streams[config.name] = config
        return config

    def put_consumer(self, stream, config):
        self.consumers[(stream, config.durable_name)] = config

    def _stream_info(self, config):
        return StreamInfo.from_response({'config': _wire(config), 'state': dict(_STATE)})

    def _consumer_info(self, stream, config):
        return ConsumerInfo.from_response({'name': config.durable_name, 'stream_name': stream,
                                           'config': _wire(config), 'created': _CREATED})

    async def streams_info_iterator(self, offset = 0):
        self.calls.append(('list', offset))
        if self.listing_error is not None:
            raise self.listing_error
        wire = [{'config': _wire(c), 'state': dict(_STATE)} for c in self.streams.values()]
        return StreamsListIterator(offset, len(wire), wire[offset:offset + self.page_size])

    async def streams_info(self, offset = 0):
        return list(await self.streams_info_iterator(offset))

    async def stream_info(self, name):
        self.calls.append(('stream_info', name))
        if name not in self.streams:
            raise NotFoundError(code = 404, description = 'stream not found')
        return self._stream_info(self.streams[name])

    async def delete_stream(self, name):
        self.calls.append(('delete', name))
        pending = self.delete_errors.get(name)
        if pending:
            raise pending.pop(0)
        if name not in self.streams:
            raise NotFoundError(code = 404, description = 'stream not found')
        del self.streams[name]
        self.deleted.append(name)
        return True

    async def add_stream(self, config):
        self.calls.append(('add_stream', config.name))
        if self.add_stream_error is not None:
            raise self.add_stream_error
        applied = _applied_stream(config, **self.create_overrides)
        if self.server_metadata:
            applied = applied.evolve(metadata = {**(applied.metadata or {}), **self.server_metadata})
        self.streams[config.name] = applied
        return self._stream_info(applied)

    async def update_stream(self, config):
        self.calls.append(('update_stream', config.name))
        if self.update_stream_error is not None:
            raise self.update_stream_error
        applied = _applied_stream(config)
        self.streams[config.name] = applied
        return self._stream_info(applied)

    async def consumer_info(self, stream, durable):
        self.calls.append(('consumer_info', stream, durable))
        if (stream, durable) not in self.consumers:
            raise NotFoundError(code = 404, description = 'consumer not found')
        return self._consumer_info(stream, self.consumers[(stream, durable)])

    async def add_consumer(self, stream, config):
        self.calls.append(('add_consumer', stream, config.durable_name))
        if self.add_consumer_error is not None:
            raise self.add_consumer_error
        applied = _applied_consumer(config)
        self.consumers[(stream, config.durable_name)] = applied
        return self._consumer_info(stream, applied)

class FakeClient:
    def __init__(self, js):
        self._js = js

    def jetstream(self):
        return self._js

def _legacy(flow, run, node):
    '''A stream provisioned before owner labels existed: today's config, no metadata (RFC0006 is off by default).'''
    return topology.stream_config_for(flow, run, node, BATCH).evolve(metadata = None)

def _labelled(flow, run, node, generation = None):
    return topology.stream_config_for(flow, run, node, BATCH).evolve(
        metadata = owner_labels(flow, run, node = node, kind = 'stream', generation = generation))

def _deletes(js):
    return [c[1] for c in js.calls if c[0] == 'delete']

# -- subject parsing ------------------------------------------------------------

def test_subject_owner_parses_only_data_subjects():
    assert topology.subject_owner(topology.subject_for('f', 'r', 'x-n')) == ('f', 'r', 'x-n')
    assert topology.subject_owner(topology.eos_subject_for('f', 'r-x', 'n')) == ('f', 'r-x', 'n')
    # The hyphen ambiguity of the stream name 'vf-f-r-x-n' does not exist in the subject.
    assert topology.subject_owner('vf.f.r.x-n') != topology.subject_owner('vf.f.r-x.n')
    assert topology.subject_owner(topology.control_subject_for('f', 'r')) is None
    assert topology.subject_owner(topology.dlq_subject_for('f', 'r', 'n')) is None
    assert topology.subject_owner('vf.f._dlq.>') is None
    assert topology.subject_owner('orders.>') is None
    assert topology.subject_owner('vf.f.r') is None
    # Tokens come back sanitized, exactly as subject_for wrote them.
    assert topology.subject_owner(topology.subject_for('a.b', 'r 1', 'n')) == ('a_b', 'r_1', 'n')

# -- teardown ---------------------------------------------------------------------

def test_teardown_of_run_r_leaves_run_r_x_standing():
    js = FakeJetStream()
    js.put_stream(_labelled('f', 'r', 'a'))
    js.put_stream(_labelled('f', 'r-x', 'a'))
    js.put_stream(_labelled('f', 'r-x', 'n'))          # 'vf-f-r-x-n': the prefix trap
    js.put_stream(topology.dlq_stream_config('f').evolve(metadata = flow_labels('f', 'dlq')))
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r'))
    assert obs == CleanupObservation(complete = True, removed = ('vf-f-r-a',), remaining = (), reason = '')
    assert set(js.streams) == {'vf-f-r-x-a', 'vf-f-r-x-n', 'vf-f-dlq'}
    assert _deletes(js) == ['vf-f-r-a']

def test_legacy_streams_are_attributed_by_subject_not_by_prefix():
    js = FakeJetStream()
    js.put_stream(_legacy('f', 'r', 'a'))
    js.put_stream(_legacy('f', 'r', 'pairs-log'))      # a hyphenated node of run r (toy_calculator has one)
    js.put_stream(_legacy('f', 'r-x', 'n'))            # 'vf-f-r-x-n' belonging to run r-x
    js.put_stream(_legacy('f', 'r-x', 'a'))
    js.put_stream(topology.dlq_stream_config('f').evolve(metadata = None))
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r'))
    assert obs.complete and set(obs.removed) == {'vf-f-r-a', 'vf-f-r-pairs-log'}
    assert set(js.streams) == {'vf-f-r-x-n', 'vf-f-r-x-a', 'vf-f-dlq'}

def test_legacy_stream_of_run_r_with_a_hyphenated_node_is_still_deleted():
    # The same string 'vf-f-r-x-n' as above, but this one is run r's (node 'x-n'): its subject says so.
    js = FakeJetStream()
    js.put_stream(_legacy('f', 'r', 'x-n'))
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r'))
    assert obs.removed == ('vf-f-r-x-n',) and not js.streams

def test_listing_failure_is_incomplete_and_deletes_nothing():
    js = FakeJetStream()
    js.put_stream(_labelled('f', 'r', 'a'))
    js.listing_error = TimeoutError('nats: timeout')
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r'))
    assert obs == CleanupObservation(complete = False, removed = (), remaining = (),
                                     reason = 'stream listing failed: nats: timeout')
    assert _deletes(js) == [] and 'vf-f-r-a' in js.streams

def test_legacy_streams_deleted_only_by_exact_name_when_nodes_are_given():
    js = FakeJetStream()
    js.put_stream(_legacy('f', 'r', 'a'))
    js.put_stream(_legacy('f', 'r', 'b'))
    js.put_stream(_legacy('f', 'r-x', 'a'))
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r', node_names = ['a', 'not-provisioned']))
    assert obs.complete and obs.removed == ('vf-f-r-a',)
    assert set(js.streams) == {'vf-f-r-b', 'vf-f-r-x-a'}

def test_exact_name_membership_does_not_override_a_subject_naming_another_run():
    # Run r's graph has a node 'x-a', so its exact name set contains 'vf-f-r-x-a' — but
    # the stream of that name in the broker routes run r-x's subject. A name is not
    # ownership; the stream is left to run r-x.
    js = FakeJetStream()
    js.put_stream(_legacy('f', 'r-x', 'a'))
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r', node_names = ['x-a']))
    assert obs.complete and obs.removed == () and 'vf-f-r-x-a' in js.streams

def test_owner_labels_win_over_name_and_subject():
    js = FakeJetStream()
    js.put_stream(topology.stream_config_for('f', 'r', 'a', BATCH).evolve(
        metadata = owner_labels('f', 'other', node = 'a', kind = 'stream')))
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r', node_names = ['a']))
    assert obs.complete and obs.removed == () and 'vf-f-r-a' in js.streams

def test_labelled_streams_narrow_by_node_label_when_nodes_are_given():
    js = FakeJetStream()
    js.put_stream(_labelled('f', 'r', 'a'))
    js.put_stream(_labelled('f', 'r', 'b'))
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r', node_names = ['b']))
    assert obs.removed == ('vf-f-r-b',) and set(js.streams) == {'vf-f-r-a'}

def test_generation_filter_deletes_only_that_provisioning():
    js = FakeJetStream()
    js.put_stream(_labelled('f', 'r', 'a', generation = '1'))
    js.put_stream(_labelled('f', 'r', 'b', generation = '2'))
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r', generation = '2'))
    assert obs.removed == ('vf-f-r-b',) and set(js.streams) == {'vf-f-r-a'}
    # Without a generation, every generation of the run is the run's.
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r'))
    assert obs.removed == ('vf-f-r-a',) and not js.streams

def test_delete_failure_is_incomplete_until_a_retry_succeeds():
    js = FakeJetStream()
    js.put_stream(_labelled('f', 'r', 'a'))
    js.put_stream(_labelled('f', 'r', 'b'))
    js.delete_errors['vf-f-r-a'] = [RuntimeError('nats: injected delete failure')]
    first = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r'))
    assert not first.complete
    assert first.removed == ('vf-f-r-b',) and first.remaining == ('vf-f-r-a',)
    assert 'injected delete failure' in first.reason
    assert 'vf-f-r-a' in js.streams
    second = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r'))
    assert second == CleanupObservation(complete = True, removed = ('vf-f-r-a',), remaining = (), reason = '')
    assert not js.streams

def test_stream_already_gone_counts_as_removed():
    js = FakeJetStream()
    js.put_stream(_labelled('f', 'r', 'a'))
    js.delete_errors['vf-f-r-a'] = [NotFoundError(code = 404, description = 'stream not found')]
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r'))
    assert obs.complete and obs.removed == ('vf-f-r-a',)

def test_listing_is_paged_to_the_servers_total():
    js = FakeJetStream(page_size = 2)
    for node in 'abcde':
        js.put_stream(_labelled('f', 'r', node))
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r'))
    assert obs.complete and len(obs.removed) == 5 and not js.streams
    assert [c for c in js.calls if c[0] == 'list'] == [('list', 0), ('list', 2), ('list', 4)]

def test_short_page_before_the_total_is_a_listing_failure():
    class ShortPages(FakeJetStream):
        async def streams_info_iterator(self, offset = 0):
            page = await super().streams_info_iterator(offset)
            return page if offset == 0 else StreamsListIterator(offset, page.total, [])

    js = ShortPages(page_size = 1)
    js.put_stream(_labelled('f', 'r', 'a'))
    js.put_stream(_labelled('f', 'r', 'b'))
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r'))
    assert not obs.complete and obs.removed == () and _deletes(js) == []
    assert 'stopped after 1 of 2' in obs.reason

# -- owner metadata ---------------------------------------------------------------

def test_metadata_is_written_only_under_rfc0006(monkeypatch):
    # Off (the default): byte-for-byte today's configs — nothing carries metadata,
    # and the wire form has no ``metadata`` key at all.
    off = {ft: topology.stream_config_for('f', 'r', 'n', ft) for ft in (REALTIME, BATCH)}
    assert all(cfg.metadata is None and 'metadata' not in cfg.as_dict() for cfg in off.values())
    assert topology.dlq_stream_config('f').metadata is None
    assert topology.consumer_config_for('f', 'r', 'c', 'p').metadata is None
    assert topology.eos_consumer_config('f', 'r', 'c', 'p', 'i1').metadata is None
    assert topology.eos_anchor_config('f', 'r', 'n').metadata is None
    assert 'metadata' not in topology.consumer_config_for('f', 'r', 'c', 'p').as_dict()

    monkeypatch.setattr(constants, 'RFC0006', True)
    for ft, before in off.items():
        on = topology.stream_config_for('f', 'r', 'n', ft)
        assert on.metadata == owner_labels('f', 'r', node = 'n', kind = 'stream')
        assert on.evolve(metadata = None) == before          # the labels are the only difference
    assert topology.stream_config_for('f', 'r', 'n', REALTIME, generation = 'g1').metadata[LABEL_GENERATION] == 'g1'
    assert topology.dlq_stream_config('f').metadata == flow_labels('f', 'dlq')
    assert LABEL_RUN not in topology.dlq_stream_config('f').metadata
    assert topology.consumer_config_for('f', 'r', 'c', 'p').metadata == owner_labels('f', 'r', node = 'c', kind = 'durable')
    assert topology.eos_consumer_config('f', 'r', 'c', 'p', 'i1').metadata == \
        owner_labels('f', 'r', node = 'c', kind = 'eos_durable')
    assert topology.eos_anchor_config('f', 'r', 'n').metadata == owner_labels('f', 'r', node = 'n', kind = 'eos_anchor')
    # Labels hold the raw ids, not the sanitized names: exactness survives sanitization.
    assert topology.stream_config_for('a.b', 'r 1', 'n', BATCH).metadata[LABEL_FLOW] == 'a.b'

def _spec(name, parents, has_children, nb_tasks = 1, partition_by = None):
    return NodeSpec(name = name, node_class = 'videoflow.processors.basic.IdentityProcessor', params = {},
                    parents = parents, kind = 'processor', has_children = has_children, nb_tasks = nb_tasks,
                    device_type = 'cpu', is_finite = True, partition_by = partition_by)

_SPECS = [_spec('producer', [], True),
          _spec('router', ['producer'], True, nb_tasks = 2, partition_by = 'key'),
          _spec('sink', ['router'], False)]

def test_provision_flow_labels_every_resource_under_rfc0006_only(monkeypatch):
    js = FakeJetStream()
    _run(topology.provision_flow(FakeClient(js), _SPECS, 'f', 'r', BATCH))
    assert set(js.streams) == {'vf-f-r-producer', 'vf-f-r-router', 'vf-f-r-sink', 'vf-f-dlq'}
    assert all(cfg.metadata is None for cfg in js.streams.values())
    assert all(cfg.metadata is None for cfg in js.consumers.values())

    monkeypatch.setattr(constants, 'RFC0006', True)
    js = FakeJetStream()
    _run(topology.provision_flow(FakeClient(js), _SPECS, 'f', 'r', BATCH, generation = 'g7'))
    for node in ('producer', 'router', 'sink'):
        assert js.streams[f'vf-f-r-{node}'].metadata == \
            owner_labels('f', 'r', node = node, kind = 'stream', generation = 'g7')
    assert js.streams['vf-f-dlq'].metadata == flow_labels('f', 'dlq')
    kinds = {key: cfg.metadata[LABEL_KIND] for key, cfg in js.consumers.items()}
    assert kinds == {
        ('vf-f-r-producer', 'producer--eos--anchor'): 'eos_anchor',
        ('vf-f-r-router', 'router--eos--anchor'): 'eos_anchor',
        ('vf-f-r-producer', 'router--from--producer--p0'): 'partitioned_durable',
        ('vf-f-r-producer', 'router--from--producer--p1'): 'partitioned_durable',
        ('vf-f-r-router', 'sink--from--router'): 'durable',
    }
    assert all(cfg.metadata[LABEL_GENERATION] == 'g7' for cfg in js.consumers.values())
    # A durable's owner is the node that consumes through it, not the parent whose stream it sits on.
    assert js.consumers[('vf-f-r-router', 'sink--from--router')].metadata[LABEL_NODE] == 'sink'
    assert js.consumers[('vf-f-r-producer', 'router--from--producer--p1')].metadata[LABEL_NODE] == 'router'
    # And a labelled provisioning is torn down exactly, the flow-level DLQ excluded.
    obs = _run(topology.delete_run_streams(FakeClient(js), 'f', 'r', generation = 'g7'))
    assert obs.complete and set(obs.removed) == {'vf-f-r-producer', 'vf-f-r-router', 'vf-f-r-sink'}
    assert set(js.streams) == {'vf-f-dlq'}

# -- read-back verification -------------------------------------------------------

def test_identical_readback_has_no_mismatches(caplog):
    js = FakeJetStream()
    with caplog.at_level(logging.WARNING, logger = _LOGGER):
        for ft in (REALTIME, BATCH):
            v = _run(topology._ensure_stream(js, topology.stream_config_for('f', 'r', 'n', ft)))
            assert v.mismatches == () and v.effective.retention == v.requested.retention
        v = _run(topology._ensure_stream(js, topology.dlq_stream_config('f')))
        assert v.mismatches == () and v.effective.max_age == 7 * 24 * 3600
        c = _run(topology._ensure_consumer(js, 'vf-f-r-n', topology.consumer_config_for('f', 'r', 'c', 'n', max_deliver = 4)))
        assert c.mismatches == () and c.effective.ack_wait == 60 and c.effective.max_deliver == 4
        c = _run(topology._ensure_consumer(js, 'vf-f-r-n', topology.eos_consumer_config('f', 'r', 'c', 'n', 'i1')))
        assert c.mismatches == () and c.effective.inactive_threshold == 3600
        c = _run(topology._ensure_consumer(js, 'vf-f-r-n', topology.eos_anchor_config('f', 'r', 'n')))
        assert c.mismatches == ()
    assert caplog.text == ''

def test_provisioning_twice_is_idempotent_and_quiet(caplog):
    js = FakeJetStream()
    with caplog.at_level(logging.WARNING, logger = _LOGGER):
        _run(topology.provision_flow(FakeClient(js), _SPECS, 'f', 'r', BATCH))
        _run(topology.provision_flow(FakeClient(js), _SPECS, 'f', 'r', BATCH))
    assert caplog.text == ''
    assert len(js.streams) == 4 and len(js.consumers) == 5

def test_server_added_metadata_keys_are_not_mismatches(monkeypatch):
    monkeypatch.setattr(constants, 'RFC0006', True)
    js = FakeJetStream()
    js.server_metadata = {'_nats.req.level': '1', '_nats.ver': '2.11.0'}
    v = _run(topology._ensure_stream(js, topology.stream_config_for('f', 'r', 'n', BATCH)))
    assert v.mismatches == () and v.effective.metadata[LABEL_RUN] == 'r'

def test_metadata_dropped_by_the_broker_is_a_mismatch_under_rfc0006(monkeypatch):
    monkeypatch.setattr(constants, 'RFC0006', True)
    js = FakeJetStream()
    js.create_overrides = {'metadata': None}          # a server that predates JetStream metadata
    with pytest.raises(IncompatibleProfile) as info:
        _run(topology._ensure_stream(js, topology.stream_config_for('f', 'r', 'n', BATCH)))
    assert 'metadata' in info.value.message and 'VF_RFC0006' in info.value.remedy

def _broker_holding_a_realtime_stream():
    js = FakeJetStream()
    js.put_stream(_applied_stream(topology.stream_config_for('f', 'r', 'n', REALTIME).evolve(metadata = None)))
    js.add_stream_error = Exception("nats: BadRequestError: code=400 description='stream name already in use "
                                    "with a different configuration'")
    js.update_stream_error = Exception("nats: BadRequestError: code=400 description='stream configuration "
                                       "update can not change retention policy'")
    return js

def test_immutable_field_mismatch_raises_under_rfc0006_and_warns_otherwise(monkeypatch, caplog):
    wanted = topology.stream_config_for('f', 'r', 'n', BATCH)

    js = _broker_holding_a_realtime_stream()
    with caplog.at_level(logging.WARNING, logger = _LOGGER):
        v = _run(topology._ensure_stream(js, wanted))
    assert v.effective.retention == RetentionPolicy.LIMITS
    assert any(m.startswith('retention: ') for m in v.mismatches)
    assert any(m.startswith('discard: ') for m in v.mismatches)
    assert 'vf-f-r-n' in caplog.text and 'retention' in caplog.text
    assert [c[0] for c in js.calls] == ['add_stream', 'update_stream', 'stream_info']

    monkeypatch.setattr(constants, 'RFC0006', True)
    js = _broker_holding_a_realtime_stream()
    with pytest.raises(IncompatibleProfile) as info:
        _run(topology._ensure_stream(js, wanted))
    assert info.value.code == 'VF_INCOMPATIBLE_PROFILE'
    assert 'retention' in info.value.message and 'videoflow teardown' in info.value.remedy
    assert info.value.context['resource'] == 'vf-f-r-n'

def test_stream_that_can_be_neither_updated_nor_read_back(monkeypatch, caplog):
    js = _broker_holding_a_realtime_stream()
    del js.streams['vf-f-r-n']                       # stream_info now fails too
    wanted = topology.stream_config_for('f', 'r', 'n', BATCH)
    with caplog.at_level(logging.WARNING, logger = _LOGGER):
        v = _run(topology._ensure_stream(js, wanted))
    assert v.effective is None and v.mismatches[0].startswith('read-back failed')
    assert caplog.text == ''                          # today's behaviour: a debug line, nothing louder
    monkeypatch.setattr(constants, 'RFC0006', True)
    with pytest.raises(BrokerUnavailable) as info:
        _run(topology._ensure_stream(js, wanted))
    assert info.value.remedy

def test_non_conflict_create_errors_still_propagate():
    js = FakeJetStream()
    js.add_stream_error = Exception('nats: ServiceUnavailableError: code=503')
    with pytest.raises(Exception, match = '503'):
        _run(topology._ensure_stream(js, topology.stream_config_for('f', 'r', 'n', BATCH)))

def test_clamped_value_on_create_is_reported():
    js = FakeJetStream()
    js.create_overrides = {'max_msgs': 100}           # an account limit clamping the request
    v = _run(topology._ensure_stream(js, topology.stream_config_for('f', 'r', 'n', BATCH)))
    assert v.mismatches == ('max_msgs: requested 10000, effective 100',)

def test_consumer_mismatch_and_unverifiable_consumer(monkeypatch, caplog):
    existing = _applied_consumer(topology.consumer_config_for('f', 'r', 'c', 'p', ack_wait = 30))
    wanted = topology.consumer_config_for('f', 'r', 'c', 'p', ack_wait = 60)
    js = FakeJetStream()
    js.put_consumer('vf-f-r-p', existing)
    js.add_consumer_error = Exception("nats: BadRequestError: code=400 description='consumer already exists'")
    with caplog.at_level(logging.WARNING, logger = _LOGGER):
        v = _run(topology._ensure_consumer(js, 'vf-f-r-p', wanted))
    assert v.mismatches == ('ack_wait: requested 60, effective 30.0',)
    assert 'vf-f-r-p/c--from--p' in caplog.text
    # Nothing to read back at all: a debug line today, an error under RFC 0006.
    js.consumers.clear()
    v = _run(topology._ensure_consumer(js, 'vf-f-r-p', wanted))
    assert v.effective is None and v.mismatches[0].startswith('read-back failed')

    monkeypatch.setattr(constants, 'RFC0006', True)
    js.put_consumer('vf-f-r-p', existing)
    with pytest.raises(IncompatibleProfile):
        _run(topology._ensure_consumer(js, 'vf-f-r-p', wanted))
    js.consumers.clear()
    with pytest.raises(BrokerUnavailable):
        _run(topology._ensure_consumer(js, 'vf-f-r-p', wanted))

if __name__ == '__main__':
    pytest.main([__file__])
