'''
Reading a run's streams back and binding explicit profile requests to them
(videoflow.messaging.topology): the profile semantics of a stream config, the
failure vocabulary of an observation, ``read_back_streams`` over a fake JetStream
and a fake ``nats.connect``, and the verdicts ``verify_channel_profiles`` reaches.
No broker; the same functions run against the compose broker in
tests/integration/broker/test_topology.py.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import logging

import nats.errors
import nats.js.errors
import pytest
from nats.js.api import DiscardPolicy, RetentionPolicy, StreamConfig, StreamInfo, StreamState

from videoflow.backends.capabilities import LIVE_LATEST, RELIABLE_WORK, ProfileRequest
from videoflow.backends.outcomes import Unknown, unknown
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.errors import BrokerUnavailable, IncompatibleProfile, UnobservableState
from videoflow.messaging import topology
from videoflow.messaging.topology import VerifiedStream, profile_mismatches, verify_channel_profiles

_STATE = StreamState(0, 0, 0, 0, 0)


def _batch(node = 'work', **overrides):
    return topology.stream_config_for('f', 'r', node, BATCH).evolve(**overrides)


def _realtime(node = 'work', **overrides):
    return topology.stream_config_for('f', 'r', node, REALTIME).evolve(**overrides)


# -- profile semantics --------------------------------------------------------------------

def test_the_flow_types_streams_carry_their_own_profile_and_not_the_other():
    assert profile_mismatches(RELIABLE_WORK, _batch()) == ()
    assert profile_mismatches(LIVE_LATEST, _realtime()) == ()
    found = profile_mismatches(RELIABLE_WORK, _realtime())
    assert len(found) == 2 and found[0].startswith('retention: limits') and found[1].startswith('discard: old')
    found = profile_mismatches(LIVE_LATEST, _batch())
    assert len(found) == 2 and found[0].startswith('retention: interest') and found[1].startswith('discard: new')


def test_profile_semantics_are_about_the_guarantee_not_the_request():
    # A stream nobody here provisioned can carry a profile with fields no request sets.
    foreign = StreamConfig(name = 'x', subjects = ['x.>'], retention = RetentionPolicy.WORK_QUEUE,
                           discard = DiscardPolicy.NEW, max_bytes = 1 << 30)
    assert profile_mismatches(RELIABLE_WORK, foreign) == ()
    latest = StreamConfig(name = 'x', subjects = ['x.>'], retention = RetentionPolicy.LIMITS,
                          discard = DiscardPolicy.OLD, max_msgs = -1, max_bytes = 64 << 20)
    assert profile_mismatches(LIVE_LATEST, latest) == ()
    # An INTEREST stream that evicts on overflow loses unacknowledged work: not reliable.
    assert profile_mismatches(RELIABLE_WORK, _batch(discard = DiscardPolicy.OLD)) == (
        'discard: old evicts unacknowledged messages when the stream is full',)
    # A LIMITS stream with no bound at all is not "latest", it is "everything".
    unbounded = StreamConfig(name = 'x', subjects = ['x.>'], retention = RetentionPolicy.LIMITS,
                             discard = DiscardPolicy.OLD, max_msgs = -1, max_bytes = -1, max_age = 0)
    assert profile_mismatches(LIVE_LATEST, unbounded) == ('limits: no max_msgs, max_bytes or max_age bounds the backlog',)
    # Not stream-level promises on JetStream: nothing to find.
    assert profile_mismatches('durable_control', _batch()) == () and profile_mismatches('replay_archive', _batch()) == ()


def test_observation_failures_are_classified_by_what_the_client_saw():
    assert topology.observation_failure(nats.errors.Error("nats: 'Authorization Violation'"))[0] == 'auth'
    assert topology.observation_failure(nats.errors.NoServersError())[0] == 'unreachable'
    assert topology.observation_failure(OSError(111, 'refused'))[0] == 'unreachable'
    assert topology.observation_failure(nats.errors.TimeoutError())[0] == 'timeout'
    assert topology.observation_failure(TimeoutError())[0] == 'timeout'
    # nats-py keeps cycling a refused server until the caller's deadline: the
    # refusal reaches the error callback, so a timeout with one on record is "unreachable".
    reason, detail = topology.observation_failure(TimeoutError(), ["ConnectionRefusedError: [Errno 111] Connect call failed ('127.0.0.1', 1)"])
    assert reason == 'unreachable' and 'Errno 111' in detail
    # A permissions violation on the API subject also only shows up there.
    reason, detail = topology.observation_failure(nats.errors.TimeoutError(),
                                                  ['Error: nats: permissions violation for publish to "$JS.API.INFO"'])
    assert reason == 'auth' and 'permissions violation' in detail
    assert topology.observation_failure(nats.js.errors.ServerError(code = 500, description = 'boom'))[0] == 'api'
    assert topology.observation_failure(nats.errors.ConnectionClosedError())[0] == 'unreachable'
    assert topology.observation_failure(KeyError('config'))[0] == 'malformed'


# -- read-back ----------------------------------------------------------------------------

class _FakeJetStream:
    def __init__(self, streams = (), error = None, report = None):
        self.streams = {c.name: c for c in streams}
        self.error = error
        self.report = report
        self.reported = []

    async def stream_info(self, name):
        if self.report:
            self.reported.append(self.report)
        if self.error is not None:
            raise self.error
        if name not in self.streams:
            raise nats.js.errors.NotFoundError(code = 404, description = 'stream not found')
        return StreamInfo(config = self.streams[name], state = _STATE)


def _read(js, nodes, flow_type = BATCH, **kw):
    return asyncio.run(topology.read_back_streams_async(js, 'f', 'r', nodes, flow_type, reported = js.reported, **kw))


def test_read_back_reports_effective_configs_missing_streams_and_failures():
    js = _FakeJetStream([_batch('a'), _batch('b', num_replicas = 1)])
    read = _read(js, ['a', 'b', 'ghost'], replicas = 3)
    assert isinstance(read['a'], VerifiedStream) and read['a'].effective == _batch('a')
    assert read['a'].mismatches == ('num_replicas: requested 3, effective None',)
    assert read['b'].mismatches == ('num_replicas: requested 3, effective 1',)
    assert isinstance(read['ghost'], Unknown) and read['ghost'].reason == 'missing' and 'vf-f-r-ghost' in read['ghost'].detail
    assert _read(_FakeJetStream([_batch('a')]), ['a'])['a'].mismatches == ()
    assert _read(_FakeJetStream(error = nats.errors.TimeoutError()), ['a'])['a'].reason == 'timeout'
    auth = _FakeJetStream(error = nats.errors.TimeoutError(),
                          report = 'Error: nats: permissions violation for publish to "$JS.API.STREAM.INFO.vf-f-r-a"')
    assert _read(auth, ['a'])['a'].reason == 'auth'
    assert _read(_FakeJetStream(error = nats.js.errors.ServerError(code = 500, description = 'x')), ['a'])['a'].reason == 'api'


def test_read_back_streams_uses_a_connection_of_its_own(monkeypatch):
    js = _FakeJetStream([_realtime('a')])
    seen = {}

    class _Nc:
        closed = False

        def jetstream(self, timeout = None):
            seen['js_timeout'] = timeout
            return js

        async def close(self):
            self.closed = True

    nc = _Nc()

    async def connect(url, **options):
        seen['url'], seen['options'] = url, options
        return nc

    monkeypatch.setattr('nats.connect', connect)
    read = topology.read_back_streams('nats://x:4222', 'f', 'r', ['a', 'b'], REALTIME, timeout = 2.5)
    assert seen['url'] == 'nats://x:4222' and seen['js_timeout'] == 2.5 and nc.closed
    assert seen['options']['allow_reconnect'] is False and seen['options']['connect_timeout'] == 2.5
    assert read['a'].mismatches == () and read['b'].reason == 'missing'
    # Without fail_fast the client's own retry schedule applies (a broker still starting).
    topology.read_back_streams('nats://x:4222', 'f', 'r', ['a'], REALTIME, timeout = 2.5, fail_fast = False)
    assert 'allow_reconnect' not in seen['options'] and 'error_cb' in seen['options']

    async def refused(url, **options):
        await options['error_cb'](ConnectionRefusedError(111, 'Connect call failed'))
        await asyncio.sleep(30)

    monkeypatch.setattr('nats.connect', refused)
    read = topology.read_back_streams('nats://x:1', 'f', 'r', ['a', 'b'], REALTIME, timeout = 0.2)
    assert {v.reason for v in read.values()} == {'unreachable'} and 'nats://x:1' in read['a'].detail

    async def denied(url, **options):
        raise nats.errors.Error("nats: 'Authorization Violation'")

    monkeypatch.setattr('nats.connect', denied)
    assert topology.read_back_streams('nats://x:4222', 'f', 'r', ['a'], REALTIME, timeout = 0.2)['a'].reason == 'auth'


# -- the verdict --------------------------------------------------------------------------

def _verified(config, mismatches = ()):
    return VerifiedStream(config, config, mismatches)


def test_verify_binds_requests_to_what_the_broker_holds():
    read = {'work': _verified(_batch()), 'src': _verified(_batch('src'))}
    verify_channel_profiles(read, [ProfileRequest('work', RELIABLE_WORK)], unknown_is_fatal = True, where = 'test')
    with pytest.raises(IncompatibleProfile, match = "channel 'work' requests live_latest, but its stream vf-f-r-work") as info:
        verify_channel_profiles(read, [ProfileRequest('work', LIVE_LATEST), ProfileRequest('src', RELIABLE_WORK)],
                                unknown_is_fatal = True, where = 'test')
    assert info.value.context['channels'] == ['work'] and 'flow type' in info.value.remedy
    # A request for a channel that was not read back is not this caller's to judge.
    verify_channel_profiles(read, [ProfileRequest('other', LIVE_LATEST)], unknown_is_fatal = True, where = 'test')


def test_verify_counts_field_mismatches_only_when_asked():
    short = _verified(_batch(num_replicas = 1), ('num_replicas: requested 3, effective 1',))
    verify_channel_profiles({'work': short}, [ProfileRequest('work', RELIABLE_WORK)], unknown_is_fatal = True, where = 'worker')
    with pytest.raises(IncompatibleProfile, match = 'num_replicas: requested 3, effective 1'):
        verify_channel_profiles({'work': short}, [ProfileRequest('work', RELIABLE_WORK)], unknown_is_fatal = True,
                                where = 'provision', config_mismatches = True)


def test_verify_distinguishes_missing_unobservable_and_unverified(caplog):
    requests = [ProfileRequest('work', RELIABLE_WORK)]
    with pytest.raises(BrokerUnavailable, match = 'no stream on the broker') as info:
        verify_channel_profiles({'work': unknown('missing', 'stream vf-f-r-work does not exist')}, requests,
                                unknown_is_fatal = True, where = 'worker w')
    assert 'Provision the run first' in info.value.remedy
    with pytest.raises(UnobservableState, match = r'(?s)could not be read back.*\[timeout\]'):
        verify_channel_profiles({'work': unknown('timeout', 'slow')}, requests, unknown_is_fatal = True, where = 'worker w')
    with caplog.at_level(logging.WARNING, logger = 'videoflow.messaging'):
        verify_channel_profiles({'work': unknown('timeout', 'slow')}, requests, unknown_is_fatal = False, where = 'worker w')
        verify_channel_profiles({'work': VerifiedStream(_batch(), None, ('read-back failed',))}, requests,
                                unknown_is_fatal = False, where = 'worker w')
    assert caplog.text.count('unverified') == 2
    with pytest.raises(UnobservableState, match = 'was not read back'):
        verify_channel_profiles({'work': VerifiedStream(_batch(), None, ('read-back failed',))}, requests,
                                unknown_is_fatal = True, where = 'worker w')
    # A definite contradiction outranks a missing or unobservable sibling.
    with pytest.raises(IncompatibleProfile):
        verify_channel_profiles({'work': _verified(_realtime()), 'src': unknown('missing', 'gone')},
                                [ProfileRequest('work', RELIABLE_WORK), ProfileRequest('src', RELIABLE_WORK)],
                                unknown_is_fatal = True, where = 'test')


if __name__ == '__main__':
    pytest.main([__file__])
