'''
Deploy-time composition admission (videoflow.deploy.admission): declared
capabilities from broker/store profiles, the live read-back of a bring-your-own
broker and store, explicit --require-profile requests, the flow-type shape check,
the advisory-versus-binding verdict, and how ``deploy`` wires it all up.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import json

import nats.errors
import nats.js.errors
import pytest
from nats.js.api import AccountInfo, AccountLimits, APIStats, StorageType, StreamConfig, StreamInfo, StreamState

from videoflow.backends.capabilities import (
    LIVE_LATEST,
    PROFILE_REQUESTS_ENV,
    RELIABLE_WORK,
    ProfileRequest,
    requests_env,
    requests_from_env,
)
from videoflow.backends.outcomes import Known, Unknown
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow, constants
from videoflow.core.compiler import compile_flow
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.errors import EXIT_USER, ConfigError, IncompatibleProfile, UnobservableState
from videoflow.deploy import admission, build, cli
from videoflow.deploy.broker_profiles import BrokerProfile, RedisProfile
from videoflow.deploy.manifests import render_manifests
from videoflow.engines.local import _worker_env
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer


def _specs(flow_type = BATCH):
    p = IntProducer(0, 3, name = 'src')
    a = IdentityProcessor(name = 'work')(p)
    return compile_flow(Flow([CommandlineConsumer(name = 'sink')(a)], flow_type = flow_type, flow_id = 'adm'))


def test_declared_broker_capabilities_follow_the_profile():
    dev = admission.jetstream_capabilities(BrokerProfile.dev())
    assert dev.retained_backlog and dev.recoverable_delivery
    assert isinstance(dev.persistent_storage, Known) and dev.persistent_storage.value is False
    assert dev.replication_factor.value == 1
    durable = admission.jetstream_capabilities(BrokerProfile.durable())
    assert durable.persistent_storage.value is True and durable.replication_factor.value == 3
    byo = admission.jetstream_capabilities(None)
    assert isinstance(byo.persistent_storage, Unknown) and isinstance(byo.replication_factor, Unknown)


def test_declared_store_capabilities_need_persistence_and_noeviction():
    dev = admission.redis_payload_capabilities(RedisProfile.dev())
    assert dev.durable.value is False
    assert admission.redis_payload_capabilities(RedisProfile.durable()).durable.value is True
    lru = admission.redis_payload_capabilities(RedisProfile(persistence = 'appendonly', eviction = 'volatile-lru'))
    assert lru.durable.value is False and lru.evictable.value is True
    assert isinstance(admission.redis_payload_capabilities(None).durable, Unknown)


def test_parse_profile_requests_validates_channels_and_profiles():
    specs = _specs()
    assert admission.parse_profile_requests(None, specs) == []
    assert admission.parse_profile_requests(['work=live_latest'], specs) == [ProfileRequest('work', LIVE_LATEST)]
    with pytest.raises(ConfigError, match = 'CHANNEL=PROFILE'):
        admission.parse_profile_requests(['work'], specs)
    with pytest.raises(ConfigError, match = 'no node in this flow publishes') as info:
        admission.parse_profile_requests(['sink=reliable_work'], specs)      # a sink publishes nothing
    assert 'src, work' in info.value.remedy
    with pytest.raises(ConfigError, match = 'unknown profile'):
        admission.parse_profile_requests(['work=bulletproof'], specs)
    with pytest.raises(ConfigError, match = 'twice'):
        admission.parse_profile_requests(['work=live_latest', 'work=reliable_work'], specs)


def test_requirements_merge_explicit_requests_over_the_presets():
    specs = _specs(REALTIME)
    plain = admission.requirements_for(REALTIME, specs)
    assert {r.channel: r.profile for r in plain.profiles} == {'src': LIVE_LATEST, 'work': LIVE_LATEST}
    merged = admission.requirements_for(REALTIME, specs, [ProfileRequest('work', RELIABLE_WORK)])
    assert {r.channel: r.profile for r in merged.profiles} == {'src': LIVE_LATEST, 'work': RELIABLE_WORK}


def test_admission_is_advisory_without_explicit_requests(capsys, monkeypatch):
    monkeypatch.setattr(constants, 'RFC0006', False)
    specs = _specs(BATCH)
    requirements = admission.requirements_for(BATCH, specs)
    plan = admission.admit(requirements, admission.jetstream_capabilities(BrokerProfile.dev()),
                           admission.redis_payload_capabilities(RedisProfile.dev()),
                           payload_refs_in_use = True, enforce = admission.enforce_admission([]),
                           unknown_is_fatal = admission.unknown_admission([]), where = 'deploy')
    assert plan is None
    err = capsys.readouterr().err
    assert 'WARNING: deploy' in err and 'evictable' in err and '--require-profile' in err
    # With durable infrastructure the same flow is admitted outright.
    plan = admission.admit(requirements, admission.jetstream_capabilities(BrokerProfile.durable()),
                           admission.redis_payload_capabilities(RedisProfile.durable()),
                           payload_refs_in_use = True, enforce = False, unknown_is_fatal = False, where = 'deploy')
    assert plan is not None and plan.channel_profiles == {'src': RELIABLE_WORK, 'work': RELIABLE_WORK}


def test_admission_binds_with_an_explicit_request_or_the_switch(monkeypatch):
    specs = _specs(BATCH)
    explicit = [ProfileRequest('work', RELIABLE_WORK)]
    requirements = admission.requirements_for(BATCH, specs, explicit)
    with pytest.raises(IncompatibleProfile, match = 'evictable'):
        admission.admit(requirements, admission.jetstream_capabilities(BrokerProfile.dev()),
                        admission.redis_payload_capabilities(RedisProfile.dev()),
                        payload_refs_in_use = True, enforce = admission.enforce_admission(explicit),
                        unknown_is_fatal = admission.unknown_admission(explicit), where = 'deploy')
    monkeypatch.setattr(constants, 'RFC0006', True)
    assert admission.enforce_admission([]) is True and admission.unknown_admission([]) is False
    # A bring-your-own broker is unread, not unavailable: under the switch that is
    # still a warning until the read-back lands; an explicit request makes it binding.
    plan = admission.admit(requirements, admission.jetstream_capabilities(None), admission.redis_payload_capabilities(None),
                           payload_refs_in_use = True, enforce = True, unknown_is_fatal = False, where = 'deploy')
    assert plan is None
    with pytest.raises(UnobservableState):
        admission.admit(requirements, admission.jetstream_capabilities(None), admission.redis_payload_capabilities(None),
                        payload_refs_in_use = True, enforce = True, unknown_is_fatal = True, where = 'deploy')


def test_explicit_requests_reach_workers_only_when_made():
    assert requests_env([]) == {}
    env = requests_env([ProfileRequest('work', RELIABLE_WORK)])
    assert set(env) == {PROFILE_REQUESTS_ENV}
    assert requests_from_env(env[PROFILE_REQUESTS_ENV]) == [ProfileRequest('work', RELIABLE_WORK)]
    assert requests_from_env(None) == []
    specs = _specs(BATCH)
    plain = render_manifests(specs, 'adm', BATCH, 'nats://nats:4222', 'run1', default_image = 'img:1')
    marked = render_manifests(specs, 'adm', BATCH, 'nats://nats:4222', 'run1', default_image = 'img:1', profile_requests = env)
    configmaps = lambda ms: {m['metadata']['name']: m['data'] for m in ms if m['kind'] == 'ConfigMap' and 'VF_NODE_NAME' in m.get('data', {})}
    assert all(PROFILE_REQUESTS_ENV not in data for data in configmaps(plain).values())
    assert all(json.loads(data[PROFILE_REQUESTS_ENV]) == [{'channel': 'work', 'profile': RELIABLE_WORK, 'options': {}}]
               or PROFILE_REQUESTS_ENV in data for data in configmaps(marked).values())
    assert all(PROFILE_REQUESTS_ENV in data for data in configmaps(marked).values())
    # The provision Job gets them too (ENV-13): it admits before creating anything.
    job_env = lambda ms: {e['name']: e['value'] for m in ms if m['kind'] == 'Job' and m['metadata']['name'].endswith('-provision')
                          for e in m['spec']['template']['spec']['containers'][0]['env']}
    assert PROFILE_REQUESTS_ENV not in job_env(plain)
    assert job_env(marked)[PROFILE_REQUESTS_ENV] == env[PROFILE_REQUESTS_ENV]
    work = next(s for s in specs if s.name == 'work')
    assert PROFILE_REQUESTS_ENV not in _worker_env(work, 'nats://x:4222', 'adm', BATCH, 'run1', None, 0, 3)
    assert _worker_env(work, 'nats://x:4222', 'adm', BATCH, 'run1', None, 0, 3, profile_requests = env)[PROFILE_REQUESTS_ENV] == env[PROFILE_REQUESTS_ENV]


# -- live read-back of a bring-your-own broker -------------------------------------------

_LIMITS = dict(max_memory = -1, max_storage = -1, max_streams = -1, max_consumers = -1, max_ack_pending = -1,
               memory_max_stream_bytes = -1, storage_max_stream_bytes = -1, max_bytes_required = False)

def _account(max_storage = -1):
    '''A real ``nats.js.api.AccountInfo`` (nats-py 2.15.0): ``limits.max_storage`` is the file-store allowance.'''
    return AccountInfo(memory = 0, storage = 7615, streams = 2, consumers = 0,
                       limits = AccountLimits(**{**_LIMITS, 'max_storage': max_storage}), api = APIStats(0, 0))

def _stream(name, storage = None, num_replicas = None):
    config = StreamConfig(name = name, subjects = [f'{name}.>'], storage = storage, num_replicas = num_replicas)
    return StreamInfo(config = config, state = StreamState(0, 0, 0, 0, 0))


class _Version:
    def __str__(self):
        return '<nats server v2.10.29>'


class _FakeJetStream:
    '''``account_info`` / ``stream_info`` with injectable failures; ``report`` is what the server tells the error callback.'''
    def __init__(self, account = None, streams = (), account_error = None, stream_error = None, report = None):
        self.account = account if account is not None else _account()
        self.streams = {s.config.name: s for s in streams}
        self.account_error = account_error
        self.stream_error = stream_error
        self.report = report
        self.nc = None

    async def account_info(self):
        if self.report and self.nc is not None:
            await self.nc.error_cb(nats.errors.Error(self.report))
        if self.account_error is not None:
            raise self.account_error
        return self.account

    async def stream_info(self, name):
        if self.stream_error is not None:
            raise self.stream_error
        if name not in self.streams:
            raise nats.js.errors.NotFoundError(code = 404, description = 'stream not found')
        return self.streams[name]


class _FakeNats:
    def __init__(self, js, max_payload = 1048576):
        self._js = js
        self.max_payload = max_payload
        self.connected_server_version = _Version()
        self.closed = False
        self.error_cb = None
        js.nc = self

    def jetstream(self, timeout = None):
        return self._js

    async def close(self):
        self.closed = True


def _connect(monkeypatch, nc = None, error = None, refuse = False):
    '''
    Stands in for ``nats.connect``: returns ``nc``, raises ``error``, or (``refuse``)
    behaves like nats-py 2.15.0 against a closed port: reports the refusal through
    the error callback and keeps trying until the caller's deadline.
    '''
    seen = {}

    async def connect(url, **options):
        seen['url'] = url
        seen['options'] = options
        if refuse:
            await options['error_cb'](ConnectionRefusedError(111, "Connect call failed ('127.0.0.1', 1)"))
            await asyncio.sleep(30)
        if error is not None:
            raise error
        nc.error_cb = options['error_cb']
        return nc

    monkeypatch.setattr('nats.connect', connect)
    return seen


def test_observed_broker_reads_the_payload_limit_the_account_and_existing_streams(monkeypatch):
    nc = _FakeNats(_FakeJetStream())
    seen = _connect(monkeypatch, nc)
    caps = admission.jetstream_capabilities_observed('nats://byo:4222', timeout = 1.0)
    assert seen['url'] == 'nats://byo:4222'
    assert seen['options']['allow_reconnect'] is False and seen['options']['max_reconnect_attempts'] == 0
    assert nc.closed
    assert caps.adapter == 'jetstream' and caps.version == '2.10.29'
    assert caps.retained_backlog and caps.recoverable_delivery
    assert isinstance(caps.max_payload_bytes, Known) and caps.max_payload_bytes.value == 1048576
    # File storage allowed => the run's streams (storage unset) will be file streams.
    assert isinstance(caps.persistent_storage, Known) and caps.persistent_storage.value is True
    # No stream of the run exists yet: the copies are decided at creation, so unread, not guessed.
    assert isinstance(caps.replication_factor, Unknown) and caps.replication_factor.reason == 'unread'
    # A memory-only account (max_storage 0) cannot keep a file stream.
    _connect(monkeypatch, _FakeNats(_FakeJetStream(account = _account(max_storage = 0))))
    assert admission.jetstream_capabilities_observed('nats://byo:4222', timeout = 1.0).persistent_storage.value is False
    # Existing streams are the answer: the fewest copies, file-backed only if all are.
    js = _FakeJetStream(streams = [_stream('vf-f-r-a', StorageType.FILE, 3), _stream('vf-f-r-b', None, None)])
    _connect(monkeypatch, _FakeNats(js))
    caps = admission.jetstream_capabilities_observed('nats://byo:4222', timeout = 1.0,
                                                     stream_names = ['vf-f-r-a', 'vf-f-r-b', 'vf-f-r-missing'])
    assert caps.replication_factor.value == 1 and caps.persistent_storage.value is True
    js = _FakeJetStream(streams = [_stream('vf-f-r-a', StorageType.MEMORY, 3)])
    _connect(monkeypatch, _FakeNats(js))
    caps = admission.jetstream_capabilities_observed('nats://byo:4222', timeout = 1.0, stream_names = ['vf-f-r-a'])
    assert caps.replication_factor.value == 3 and caps.persistent_storage.value is False


def test_observed_broker_without_jetstream_offers_no_retained_delivery(monkeypatch):
    disabled = nats.js.errors.ServiceUnavailableError(code = 503, description = 'JetStream not enabled for account')
    _connect(monkeypatch, _FakeNats(_FakeJetStream(account_error = disabled)))
    caps = admission.jetstream_capabilities_observed('nats://core:4222', timeout = 1.0)
    assert 'JetStream not enabled' in caps.adapter
    assert caps.retained_backlog is False and caps.recoverable_delivery is False
    assert caps.persistent_storage.value is False and caps.replication_factor.value == 0
    assert caps.max_payload_bytes.value == 1048576
    # Definite, so the planner rejects by name, not as "unobservable".
    specs = _specs(BATCH)
    with pytest.raises(IncompatibleProfile, match = 'no retained, recoverable delivery'):
        admission.admit(admission.requirements_for(BATCH, specs), caps, None, payload_refs_in_use = False,
                        enforce = True, unknown_is_fatal = False, where = 'deploy')
    # A 503 for any other reason (no meta leader yet) is transient: unknown, not "no JetStream".
    busy = nats.js.errors.ServiceUnavailableError(code = 503, description = 'JetStream system temporarily unavailable')
    _connect(monkeypatch, _FakeNats(_FakeJetStream(account_error = busy)))
    caps = admission.jetstream_capabilities_observed('nats://core:4222', timeout = 1.0)
    assert caps.retained_backlog and isinstance(caps.persistent_storage, Unknown)
    assert caps.persistent_storage.reason == 'unreachable'


@pytest.mark.parametrize('setup, reason', [
    (dict(error = nats.errors.Error("nats: 'Authorization Violation'")), 'auth'),
    (dict(refuse = True), 'unreachable'),
    (dict(error = nats.errors.NoServersError()), 'unreachable'),
])
def test_observed_broker_connect_failures_are_unknown_with_their_reason(monkeypatch, setup, reason):
    _connect(monkeypatch, _FakeNats(_FakeJetStream()), **setup)
    caps = admission.jetstream_capabilities_observed('nats://byo:4222', timeout = 0.2)
    assert caps.retained_backlog                       # the adapter's static promise still describes JetStream
    for observation in (caps.max_payload_bytes, caps.persistent_storage, caps.replication_factor):
        assert isinstance(observation, Unknown) and observation.reason == reason, observation
        assert 'nats://byo:4222' in observation.detail


def test_observed_broker_api_failures_are_unknown_with_their_reason(monkeypatch):
    # The request timed out: slow broker.
    _connect(monkeypatch, _FakeNats(_FakeJetStream(account_error = nats.errors.TimeoutError())))
    caps = admission.jetstream_capabilities_observed('nats://byo:4222', timeout = 1.0)
    assert caps.max_payload_bytes.value == 1048576         # the connection itself was fine
    assert caps.persistent_storage.reason == 'timeout' and caps.replication_factor.reason == 'timeout'
    # The request timed out *after* the server reported a permissions violation on
    # the API subject (nats-py surfaces that through the error callback only): auth.
    js = _FakeJetStream(account_error = nats.errors.TimeoutError(),
                        report = 'nats: permissions violation for publish to "$JS.API.INFO"')
    _connect(monkeypatch, _FakeNats(js))
    caps = admission.jetstream_capabilities_observed('nats://restricted@byo:4222', timeout = 1.0)
    assert caps.persistent_storage.reason == 'auth' and 'permissions violation' in caps.persistent_storage.detail
    # An answer the client could not interpret.
    class _Odd:
        limits = None
    _connect(monkeypatch, _FakeNats(_FakeJetStream(account = _Odd())))
    assert admission.jetstream_capabilities_observed('nats://byo:4222', timeout = 1.0).persistent_storage.reason == 'malformed'
    # A stream read that fails (not "not found") makes both observations unknown.
    js = _FakeJetStream(stream_error = nats.errors.TimeoutError())
    _connect(monkeypatch, _FakeNats(js))
    caps = admission.jetstream_capabilities_observed('nats://byo:4222', timeout = 1.0, stream_names = ['vf-f-r-a'])
    assert caps.persistent_storage.reason == 'timeout' and 'vf-f-r-a' in caps.persistent_storage.detail


# -- live read-back of a bring-your-own store ---------------------------------------------

def test_observed_store_is_read_back_live(monkeypatch):
    import redis
    from support_redis import FakeRedis

    class _Store(FakeRedis):
        closed = False
        auth_error = None

        def ping(self):
            if self.auth_error is not None:
                raise self.auth_error
            return True

        def close(self):
            self.closed = True

    built = {}

    def from_url(url, **kwargs):
        built['url'] = url
        built['kwargs'] = kwargs
        return built['store']

    monkeypatch.setattr(redis.Redis, 'from_url', staticmethod(from_url))
    built['store'] = _Store(config = {'appendonly': 'yes', 'save': '', 'maxmemory-policy': 'noeviction'})
    caps = admission.redis_payload_capabilities_observed('redis://byo:6379/2', timeout = 1.5)
    assert built['url'] == 'redis://byo:6379/2'
    assert built['kwargs'] == {'socket_timeout': 1.5, 'socket_connect_timeout': 1.5}
    assert built['store'].closed
    assert isinstance(caps.durable, Known) and caps.durable.value is True
    assert caps.evictable.value is False and caps.atomic_multikey.value is True
    built['store'] = _Store()                                 # the compose dev shape: a cache
    assert admission.redis_payload_capabilities_observed('redis://byo:6379/0').durable.value is False
    built['store'] = _Store(deny_config = True)
    assert admission.redis_payload_capabilities_observed('redis://byo:6379/0').durable.reason == 'auth'
    built['store'] = _Store()
    built['store'].auth_error = redis.exceptions.AuthenticationError('WRONGPASS invalid username-password pair')
    caps = admission.redis_payload_capabilities_observed('redis://:nope@byo:6379/0')
    for observation in (caps.durable, caps.evictable, caps.atomic_multikey):
        assert isinstance(observation, Unknown) and observation.reason == 'auth'
    assert built['store'].closed


# -- the flow type's own streams -----------------------------------------------------------

def test_topology_shape_rejects_a_profile_the_flow_type_cannot_carry():
    admission.verify_topology_shape(BATCH, 'adm', 'run', [ProfileRequest('work', RELIABLE_WORK)])
    admission.verify_topology_shape(REALTIME, 'adm', 'run', [ProfileRequest('work', LIVE_LATEST)])
    admission.verify_topology_shape(REALTIME, 'adm', 'run', [])
    with pytest.raises(IncompatibleProfile, match = 'REALTIME flow provisions its streams as: retention: limits') as info:
        admission.verify_topology_shape(REALTIME, 'adm', 'run', [ProfileRequest('work', RELIABLE_WORK)])
    assert info.value.context['channels'] == ['work'] and 'BATCH' in info.value.remedy
    with pytest.raises(IncompatibleProfile, match = 'BATCH flow provisions its streams as: retention: interest'):
        admission.verify_topology_shape(BATCH, 'adm', 'run', [ProfileRequest('src', LIVE_LATEST)])


def test_run_stream_names_follow_the_topology():
    assert admission.run_stream_names('adm', 'r1', _specs()) == ['vf-adm-r1-src', 'vf-adm-r1-work', 'vf-adm-r1-sink']


# -- deploy wiring ---------------------------------------------------------------------------

class _Stop(ConfigError):
    '''Raised from the stubbed ``admit`` so the command stops right after admission.'''


def _deploy(tmp_path, monkeypatch, *argv_extra):
    '''
    Runs ``deploy`` with the image, graph load and cluster mechanics stubbed, the
    probes recorded, and ``admit`` replaced by a recorder that stops the command
    (exit 2) before any infrastructure would be created.
    '''
    graph = tmp_path / 'mygraph.py'
    graph.write_text('# staged by the test; _load_flow is monkeypatched\n')
    seen = {'probes': [], 'admit': None}
    monkeypatch.setattr(cli, '_load_flow', lambda target: Flow(
        [CommandlineConsumer(name = 'printer')(IntProducer(0, 3, name = 'numbers'))], flow_type = BATCH, flow_id = 'render'))
    monkeypatch.setattr(build, 'docker_gpus_available', lambda: False)
    monkeypatch.setattr(cli, 'docker_gpus_available', lambda: False)
    monkeypatch.setattr(cli, 'autobuild', lambda *a, **kw: 'videoflow-mygraph:latest')
    monkeypatch.setattr(cli, 'detect_cluster', lambda kubectl: 'k3s')
    monkeypatch.setattr(cli, 'image_exists', lambda ref: False)
    monkeypatch.setattr(cli, 'hostpath_warning', lambda flavor: None)
    monkeypatch.setattr(cli, 'jetstream_capabilities_observed',
                        lambda url, **kw: seen['probes'].append(('nats', url, kw)) or admission.jetstream_capabilities(None))
    monkeypatch.setattr(cli, 'redis_payload_capabilities_observed',
                        lambda url, **kw: seen['probes'].append(('redis', url, kw)) or admission.redis_payload_capabilities(None))

    def admit(requirements, messaging, payload, **kw):
        seen['admit'] = (requirements, messaging, payload, kw)
        raise _Stop('stopped after admission')

    monkeypatch.setattr(cli, 'admit', admit)
    code = cli.main(['deploy', str(graph), '--non-interactive', '--run-id', 'r1', *argv_extra])
    return code, seen


def test_deploy_reads_a_bring_your_own_broker_and_store_back_before_any_infra(tmp_path, monkeypatch):
    code, seen = _deploy(tmp_path, monkeypatch, '--nats', 'nats://byo:4222', '--blob-redis-url', 'redis://byo:6379/0')
    assert code == EXIT_USER                       # _Stop: admission ran, nothing after it did
    assert [p[:2] for p in seen['probes']] == [('nats', 'nats://byo:4222'), ('redis', 'redis://byo:6379/0')]
    assert seen['probes'][0][2] == {'stream_names': ['vf-render-r1-numbers', 'vf-render-r1-printer']}
    _requirements, messaging, payload, kw = seen['admit']
    assert isinstance(messaging.persistent_storage, Unknown) and isinstance(payload.durable, Unknown)
    assert kw['payload_refs_in_use'] is True and kw['where'] == 'deploy'


def test_deploy_judges_an_auto_provisioned_broker_by_its_declared_profile(tmp_path, monkeypatch):
    code, seen = _deploy(tmp_path, monkeypatch, '--broker-profile', 'durable')
    assert code == EXIT_USER
    assert seen['probes'] == []
    _requirements, messaging, payload, _kw = seen['admit']
    assert messaging.persistent_storage.value is True and messaging.replication_factor.value == 3
    assert payload.durable.value is True
    # A bring-your-own store beside an auto-provisioned broker is still read back.
    code, seen = _deploy(tmp_path, monkeypatch, '--blob-redis-url', 'redis://byo:6379/0')
    assert [p[:2] for p in seen['probes']] == [('redis', 'redis://byo:6379/0')]


def test_deploy_never_probes_for_a_render(tmp_path, monkeypatch):
    for flag in (['--render-only', '--output', str(tmp_path / 'out')], ['--dry-run']):
        code, seen = _deploy(tmp_path, monkeypatch, '--nats', 'nats://byo:4222', '--blob-redis-url', 'redis://byo:6379/0', *flag)
        assert code == 0 and seen['probes'] == [] and seen['admit'] is None


def test_deploy_refuses_a_profile_the_flow_type_cannot_carry_before_probing(tmp_path, monkeypatch, capsys):
    code, seen = _deploy(tmp_path, monkeypatch, '--nats', 'nats://byo:4222', '--require-profile', 'numbers=live_latest')
    assert code == EXIT_USER and seen['probes'] == [] and seen['admit'] is None
    err = capsys.readouterr().err
    assert 'ERROR [VF_INCOMPATIBLE_PROFILE]' in err and 'BATCH flow provisions its streams' in err
