'''
In-container admission (RFC 0006 ENV-13): the provision entrypoint admits the
composition against the live broker and store before it creates anything and
verifies what it created against the explicit requests; the worker binds the
same requests to its own channels before it opens. Both are driven by the
environment, both raise the taxonomy's typed errors, and neither prints a
traceback for a rejection. The broker, the store and the read-back are
monkeypatched; the verdict logic they feed is what is under test.
'''
from __future__ import absolute_import, division, print_function

import json

import pytest

from videoflow.backends.capabilities import (
    ADMISSION_TIMEOUT_ENV,
    LIVE_LATEST,
    PROFILE_REQUESTS_ENV,
    RELIABLE_WORK,
    ProfileRequest,
    admission_timeout_from_env,
    requests_env,
)
from videoflow.backends.outcomes import unknown
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow, constants
from videoflow.core.compiler import compile_flow
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.errors import (
    EXIT_ENVIRONMENT,
    EXIT_USER,
    BrokerUnavailable,
    ConfigError,
    IncompatibleProfile,
    UnobservableState,
)
from videoflow.deploy import admission
from videoflow.deploy.broker_profiles import BrokerProfile, RedisProfile
from videoflow.messaging import topology
from videoflow.messaging.topology import VerifiedStream
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer
from videoflow.runtime import provision, worker


def _specs(flow_type = BATCH):
    p = IntProducer(0, 3, name = 'src')
    a = IdentityProcessor(name = 'work')(p)
    return compile_flow(Flow([CommandlineConsumer(name = 'sink')(a)], flow_type = flow_type, flow_id = 'prov'))


@pytest.fixture
def env(monkeypatch):
    '''The provision Job's environment for a BATCH flow, without explicit requests.'''
    monkeypatch.setattr(constants, 'RFC0006', False)
    monkeypatch.setenv('VF_FLOW_SPECS_JSON', json.dumps([s.to_dict() for s in _specs()]))
    monkeypatch.setenv('VF_FLOW_ID', 'prov')
    monkeypatch.setenv('VF_RUN_ID', 'r1')
    monkeypatch.setenv('VF_FLOW_TYPE', BATCH)
    monkeypatch.setenv('VF_NATS_URL', 'nats://broker:4222')
    monkeypatch.setenv('VF_BLOB_REDIS_URL', 'redis://store:6379/0')
    monkeypatch.delenv(PROFILE_REQUESTS_ENV, raising = False)
    monkeypatch.delenv(ADMISSION_TIMEOUT_ENV, raising = False)
    return monkeypatch


@pytest.fixture
def stubs(env):
    '''Records the calls provision makes; the broker is durable JetStream, the store is set per test.'''
    calls = {'provisioned': [], 'probes': [], 'read_back': [], 'store': admission.redis_payload_capabilities(RedisProfile.durable()),
             'read': {}}

    def provisioned(nats_url, specs, flow_id, run_id, flow_type, **kw):
        calls['provisioned'].append((nats_url, [s.name for s in specs], flow_id, run_id, flow_type, kw))

    def probe_nats(url, **kw):
        calls['probes'].append(('nats', url, kw))
        return admission.jetstream_capabilities(BrokerProfile.durable())

    def probe_redis(url, **kw):
        calls['probes'].append(('redis', url, kw))
        return calls['store']

    def read_back(nats_url, flow_id, run_id, node_names, flow_type, **kw):
        calls['read_back'].append((nats_url, flow_id, run_id, list(node_names), flow_type, kw))
        return {n: calls['read'].get(n, _held(n, flow_type, kw.get('replicas', 1))) for n in node_names}

    env.setattr(provision, 'provision_flow_sync', provisioned)
    env.setattr(provision, 'jetstream_capabilities_observed', probe_nats)
    env.setattr(provision, 'redis_payload_capabilities_observed', probe_redis)
    env.setattr(provision, 'read_back_streams', read_back)
    return calls


def _held(node, flow_type, replicas = 1, held_type = None, held_replicas = None):
    '''
    What the broker holds for ``node`` against what this run requested: the stream
    as provisioned (the request honoured), one of the other flow type, or one with
    ``held_replicas`` copies instead of the ``replicas`` asked for.
    '''
    requested = topology.stream_config_for('prov', 'r1', node, flow_type, replicas = replicas)
    effective = topology.stream_config_for('prov', 'r1', node, held_type or flow_type,
                                           replicas = replicas if held_replicas is None else held_replicas)
    return VerifiedStream(requested, effective, topology._mismatches(requested, effective, topology._STREAM_FIELDS))


def _explicit(env, *requests):
    env.setenv(PROFILE_REQUESTS_ENV, requests_env(list(requests))[PROFILE_REQUESTS_ENV])


# -- the provision entrypoint ------------------------------------------------------------------

def test_without_requests_and_without_the_switch_nothing_is_read_back(stubs):
    provision.provision()
    assert stubs['probes'] == [] and stubs['read_back'] == []
    assert stubs['provisioned'] == [('nats://broker:4222', ['src', 'work', 'sink'], 'prov', 'r1', BATCH,
                                     {'max_retries': 3, 'replicas': 1})]


def test_a_rejection_before_provisioning_creates_nothing(stubs, env, capsys):
    _explicit(env, ProfileRequest('work', RELIABLE_WORK))
    stubs['store'] = admission.redis_payload_capabilities(RedisProfile.dev())        # evictable
    with pytest.raises(IncompatibleProfile, match = 'evictable'):
        provision.provision()
    assert stubs['provisioned'] == [] and stubs['read_back'] == []
    assert [p[:2] for p in stubs['probes']] == [('nats', 'nats://broker:4222'), ('redis', 'redis://store:6379/0')]
    # The broker probe reads this run's streams if they exist and tolerates a broker still starting.
    assert stubs['probes'][0][2] == {'timeout': 60.0, 'stream_names': ['vf-prov-r1-src', 'vf-prov-r1-work', 'vf-prov-r1-sink'],
                                     'fail_fast': False}
    # The entrypoint renders it like the CLI and exits with the class's code — no traceback.
    with pytest.raises(SystemExit) as info:
        provision.main()
    assert info.value.code == EXIT_USER
    err = capsys.readouterr().err
    assert 'ERROR [VF_INCOMPATIBLE_PROFILE]:' in err and 'Remedy' not in err and 'Traceback' not in err
    assert '  Use a durable payload store' in err
    assert stubs['provisioned'] == []


def test_an_unobserved_store_binds_only_with_explicit_requests(stubs, env, capsys):
    stubs['store'] = admission.redis_payload_capabilities(None)                      # the probe could not read it
    env.setattr(constants, 'RFC0006', True)
    provision.provision()                                                            # a warning under the switch alone
    assert len(stubs['provisioned']) == 1 and stubs['read_back'] == []
    assert 'WARNING: provision' in capsys.readouterr().err
    _explicit(env, ProfileRequest('work', RELIABLE_WORK))
    with pytest.raises(UnobservableState):
        provision.provision()
    with pytest.raises(SystemExit) as info:
        provision.main()
    assert info.value.code == EXIT_ENVIRONMENT
    assert len(stubs['provisioned']) == 1


def test_explicit_requests_are_verified_against_what_was_provisioned(stubs, env):
    _explicit(env, ProfileRequest('work', RELIABLE_WORK), ProfileRequest('src', RELIABLE_WORK))
    env.setenv('VF_STREAM_REPLICAS', '3')
    provision.provision()
    assert stubs['provisioned'][0][5] == {'max_retries': 3, 'replicas': 3}
    assert stubs['read_back'] == [('nats://broker:4222', 'prov', 'r1', ['work', 'src'], BATCH,
                                   {'timeout': 60.0, 'replicas': 3, 'fail_fast': False})]
    # The broker kept 'work' at the other flow type's shape (a stream that pre-dates this run).
    stubs['read'] = {'work': _held('work', BATCH, replicas = 3, held_type = REALTIME)}
    with pytest.raises(IncompatibleProfile, match = "channel 'work' requests reliable_work, but its stream vf-prov-r1-work carries: retention: limits"):
        provision.provision()
    assert len(stubs['provisioned']) == 2                  # provisioning ran; the verification is what refused
    # Fewer copies than requested is a contradiction of the request even with the switch off.
    stubs['read'] = {'work': _held('work', BATCH, replicas = 3, held_replicas = 1)}     # the server kept one copy
    with pytest.raises(IncompatibleProfile, match = 'num_replicas: requested 3'):
        provision.provision()
    stubs['read'] = {'src': unknown('missing', 'stream vf-prov-r1-src does not exist')}
    with pytest.raises(BrokerUnavailable):
        provision.provision()
    stubs['read'] = {'src': unknown('timeout', 'slow')}
    with pytest.raises(UnobservableState):
        provision.provision()
    with pytest.raises(SystemExit) as info:
        provision.main()
    assert info.value.code == EXIT_ENVIRONMENT


def test_the_admission_timeout_comes_from_the_environment(stubs, env):
    _explicit(env, ProfileRequest('work', RELIABLE_WORK))
    env.setenv(ADMISSION_TIMEOUT_ENV, '7.5')
    provision.provision()
    assert stubs['probes'][0][2]['timeout'] == 7.5 and stubs['probes'][1][2] == {'timeout': 7.5}
    assert stubs['read_back'][0][5]['timeout'] == 7.5
    assert admission_timeout_from_env(None) == 60.0 and admission_timeout_from_env('') == 60.0
    for bad in ('soon', '0', '-1'):
        with pytest.raises(ConfigError, match = ADMISSION_TIMEOUT_ENV):
            admission_timeout_from_env(bad)


# -- the worker's bind-time check ---------------------------------------------------------------

@pytest.fixture
def read_back(monkeypatch):
    '''``topology.read_back_streams`` as the worker resolves it at call time; ``held`` is what the broker has per node.'''
    calls = {'read': [], 'held': {}}

    def fake(nats_url, flow_id, run_id, node_names, flow_type, **kw):
        calls['read'].append((nats_url, flow_id, run_id, list(node_names), flow_type, kw))
        return {n: calls['held'].get(n, _held(n, flow_type)) for n in node_names}

    monkeypatch.setattr(topology, 'read_back_streams', fake)
    return calls


def test_worker_verifies_only_the_channels_it_touches(read_back):
    requests = [ProfileRequest('src', RELIABLE_WORK), ProfileRequest('work', RELIABLE_WORK), ProfileRequest('other', LIVE_LATEST)]
    worker.verify_explicit_profiles('nats://b:4222', 'prov', 'r1', BATCH, 'work', ['src'], True, requests, timeout = 9.0)
    assert read_back['read'] == [('nats://b:4222', 'prov', 'r1', ['src', 'work'], BATCH, {'timeout': 9.0})]
    # A sink has no output channel of its own; a producer has no parents.
    worker.verify_explicit_profiles('nats://b:4222', 'prov', 'r1', BATCH, 'sink', ['work'], False, requests, timeout = 9.0)
    assert read_back['read'][-1][3] == ['work']
    worker.verify_explicit_profiles('nats://b:4222', 'prov', 'r1', BATCH, 'src', [], True, requests, timeout = 9.0)
    assert read_back['read'][-1][3] == ['src']
    # Nothing of this node's was requested: nothing is read.
    worker.verify_explicit_profiles('nats://b:4222', 'prov', 'r1', BATCH, 'lonely', ['nobody'], True, requests, timeout = 9.0)
    assert len(read_back['read']) == 3


def test_worker_refuses_a_stream_that_does_not_carry_the_requested_profile(read_back):
    read_back['held'] = {'src': _held('src', BATCH, held_type = REALTIME)}
    with pytest.raises(IncompatibleProfile, match = "(?s)worker work: .*channel 'src' requests reliable_work"):
        worker.verify_explicit_profiles('nats://b:4222', 'prov', 'r1', BATCH, 'work', ['src'], True,
                                        [ProfileRequest('src', RELIABLE_WORK)], timeout = 9.0)
    # Unread is fatal for an explicit request; missing is the broker's problem.
    read_back['held'] = {'src': unknown('unreachable', 'nats://b:4222: refused')}
    with pytest.raises(UnobservableState):
        worker.verify_explicit_profiles('nats://b:4222', 'prov', 'r1', BATCH, 'work', ['src'], True,
                                        [ProfileRequest('src', RELIABLE_WORK)], timeout = 9.0)
    read_back['held'] = {'src': unknown('missing', 'stream vf-prov-r1-src does not exist')}
    with pytest.raises(BrokerUnavailable):
        worker.verify_explicit_profiles('nats://b:4222', 'prov', 'r1', BATCH, 'work', ['src'], True,
                                        [ProfileRequest('src', RELIABLE_WORK)], timeout = 9.0)


def _worker_env(monkeypatch, tmp_path, requests):
    for key, value in {
        'VF_NODE_CLASS': 'videoflow.processors.basic.IdentityProcessor', 'VF_NODE_PARAMS_JSON': '{}',
        'VF_NODE_KIND': 'processor', 'VF_NODE_NAME': 'work', 'VF_PARENT_NAMES': 'src', 'VF_HAS_CHILDREN': '1',
        'VF_NATS_URL': 'nats://b:4222', 'VF_FLOW_ID': 'prov', 'VF_RUN_ID': 'r1', 'VF_FLOW_TYPE': BATCH,
        'VF_TERMINATION_LOG': str(tmp_path / 'term.json'),
    }.items():
        monkeypatch.setenv(key, value)
    monkeypatch.delenv(PROFILE_REQUESTS_ENV, raising = False)
    if requests:
        monkeypatch.setenv(PROFILE_REQUESTS_ENV, requests_env(requests)[PROFILE_REQUESTS_ENV])


def test_worker_binds_before_the_node_is_built(monkeypatch, tmp_path, read_back, capsys):
    _worker_env(monkeypatch, tmp_path, [ProfileRequest('src', RELIABLE_WORK)])
    read_back['held'] = {'src': _held('src', BATCH, held_type = REALTIME)}
    built = []
    monkeypatch.setattr(worker, 'build_node_from_env', lambda: built.append(1) or pytest.fail('the node was built'))
    with pytest.raises(IncompatibleProfile):
        worker.run_from_env()
    assert built == []
    assert worker.main() == EXIT_USER
    err = capsys.readouterr().err
    assert 'VF_INCOMPATIBLE_PROFILE' in err and 'Traceback' not in err
    reason = json.loads((tmp_path / 'term.json').read_text())
    assert reason['code'] == 'VF_INCOMPATIBLE_PROFILE' and "channel 'src'" in reason['message']
    # Without requests nothing is read back — the env contract is unchanged.
    _worker_env(monkeypatch, tmp_path, [])
    read_back['read'].clear()

    def stop():
        raise ConfigError('stop before the messenger', remedy = 'n/a')

    monkeypatch.setattr(worker, 'build_node_from_env', stop)
    with pytest.raises(ConfigError, match = 'stop before the messenger'):
        worker.run_from_env()
    assert read_back['read'] == []


if __name__ == '__main__':
    pytest.main([__file__])
