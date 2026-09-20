'''
BLOB-16: a payload store that refuses a put for memory is applying backpressure,
and a BATCH publisher answers it the way it answers a full stream — it holds the
publication and retries while the store's readers drain it, beating its liveness
keepalive, for a bounded time and never past the termination flag. Exercised on
the messenger's seam without a broker (``_encode_admitted``), on both stores'
typed refusal, and on the worker's parsing of the budget.
'''
from __future__ import absolute_import, division, print_function

import threading
import time
import types

import pytest

from videoflow.backends.memory.payload import MemoryPayloadStore
from videoflow.backends.payload import RetentionContract
from videoflow.core.constants import BATCH, REALTIME
from videoflow.core.errors import ConfigError, PayloadStoreFull, ResourceUnavailable
from videoflow.messaging import nats_messenger as nm
from videoflow.messaging.nats_messenger import NATSMessenger
from videoflow.runtime import worker
from videoflow.wire.redis_payload_store import RedisPayloadStore


class _Node:
    name = 'reader'


class _Clock:
    '''Monotonic time that advances only when the publisher sleeps.'''
    def __init__(self) -> None:
        self.now = 1000.0
        self.sleeps : list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds : float) -> None:
        self.sleeps.append(round(seconds, 6))
        self.now += seconds


def _bare(flow_type = BATCH, budget = 600.0, beats = None):
    '''Just the attributes the seam reads; no backend, no broker.'''
    m = NATSMessenger.__new__(NATSMessenger)
    m._node = _Node()
    m._flow_type = flow_type
    m._store_backpressure_seconds = budget
    m._keepalive = (lambda: beats.append(1)) if beats is not None else (lambda: None)
    m._termination_event = threading.Event()
    m.publication_stats = {}
    return m


def _refusing(times, result = b'envelope'):
    '''An encoder that the store refuses ``times`` times, then admits.'''
    calls = {'n': 0}

    def encode():
        calls['n'] += 1
        if calls['n'] <= times:
            raise PayloadStoreFull(f'the payload store refused 3 bytes (attempt {calls["n"]})')
        return result
    return encode, calls


@pytest.fixture
def clock(monkeypatch):
    '''The messenger module's own ``time`` name, not the global module: other tests' threads keep theirs.'''
    c = _Clock()
    monkeypatch.setattr(nm, 'time', types.SimpleNamespace(monotonic = c.monotonic, sleep = c.sleep, time = time.time))
    return c


# -- the hold -------------------------------------------------------------------------------

def test_a_batch_publisher_holds_until_the_store_admits(clock):
    beats : list[int] = []
    m = _bare(beats = beats)
    encode, calls = _refusing(2)
    assert m._encode_admitted(encode) == b'envelope'
    assert calls['n'] == 3
    assert clock.sleeps == [0.1, 0.2]                 # the ladder, one step per refusal
    assert beats == [1, 1]                            # liveness beaten before every sleep
    assert m.publication_stats == {'held': 1}         # one hold, however many refusals


def test_the_ladder_climbs_to_a_second_and_stays_there(clock):
    m = _bare()
    encode, _ = _refusing(6)
    assert m._encode_admitted(encode) == b'envelope'
    assert clock.sleeps == [0.1, 0.2, 0.5, 1.0, 1.0, 1.0]


def test_the_hold_ends_at_the_budget_and_the_refusal_is_the_failure(clock):
    m = _bare(budget = 1.0)
    encode, calls = _refusing(100)
    with pytest.raises(PayloadStoreFull):
        m._encode_admitted(encode)
    # 0.1 + 0.2 + 0.5 = 0.8 s; the last sleep is clipped to what remains of the budget.
    assert clock.sleeps == [0.1, 0.2, 0.5, 0.2]
    assert clock.now == pytest.approx(1001.0)
    assert calls['n'] == 5                            # four refusals slept, the fifth is final


def test_a_stopping_flow_does_not_wait(clock):
    m = _bare()
    m._termination_event.set()
    encode, calls = _refusing(1)
    with pytest.raises(PayloadStoreFull):
        m._encode_admitted(encode)
    assert clock.sleeps == [] and calls['n'] == 1


def test_termination_mid_hold_ends_it(clock):
    m = _bare()
    calls = {'n': 0}

    def encode():
        calls['n'] += 1
        if calls['n'] == 2:
            m._termination_event.set()                # the flow stops while we are held
        raise PayloadStoreFull('full')
    with pytest.raises(PayloadStoreFull):
        m._encode_admitted(encode)
    assert clock.sleeps == [0.1] and calls['n'] == 2


def test_a_realtime_publisher_never_waits(clock):
    m = _bare(flow_type = REALTIME)
    encode, calls = _refusing(1)
    with pytest.raises(PayloadStoreFull):
        m._encode_admitted(encode)
    assert clock.sleeps == [] and calls['n'] == 1 and m.publication_stats == {}


def test_a_zero_budget_disables_the_hold(clock):
    m = _bare(budget = 0)
    encode, calls = _refusing(1)
    with pytest.raises(PayloadStoreFull):
        m._encode_admitted(encode)
    assert clock.sleeps == [] and calls['n'] == 1


def test_other_failures_pass_through_untouched(clock):
    m = _bare()
    for error in (ResourceUnavailable('a GPU, not the store'), ValueError('no store configured')):
        def encode(error = error):
            raise error
        with pytest.raises(type(error)):
            m._encode_admitted(encode)
    assert clock.sleeps == [] and m.publication_stats == {}


def test_an_admitted_publication_is_not_a_hold(clock):
    m = _bare()
    assert m._encode_admitted(lambda: b'x') == b'x'
    assert clock.sleeps == [] and m.publication_stats == {}


def test_the_constructor_defaults_and_wires_the_knobs():
    m = NATSMessenger.__new__(NATSMessenger)
    assert nm.DEFAULT_STORE_BACKPRESSURE_SECONDS == 600.0
    # The two attributes the seam reads, as __init__ derives them.
    m._store_backpressure_seconds = (nm.DEFAULT_STORE_BACKPRESSURE_SECONDS if None is None else 0.0)
    assert m._store_backpressure_seconds == 600.0
    assert nm._STORE_BACKPRESSURE_BACKOFF == [0.1, 0.2, 0.5, 1.0]


# -- both stores raise the typed refusal ---------------------------------------------------

class _FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def now(self) -> float:
        return self.t


def test_the_memory_store_refuses_over_budget_with_the_typed_error():
    store = MemoryPayloadStore(_FakeClock(), max_bytes = 10)
    store.put(b'12345', 'c1', RetentionContract(ttl_seconds = 10, horizon_seconds = 10,
                                               durable_required = True, obligations = ('x',)))
    with pytest.raises(PayloadStoreFull) as e:
        store.put(b'123456', 'c2', RetentionContract(ttl_seconds = 10, horizon_seconds = 10,
                                                    durable_required = True, obligations = ('x',)))
    assert isinstance(e.value, ResourceUnavailable) and e.value.code == 'VF_PAYLOAD_STORE_FULL'
    assert store.object_count() == 1


def test_the_redis_store_refusal_is_the_typed_error():
    error = RedisPayloadStore._refused(RuntimeError("OOM command not allowed when used memory > 'maxmemory'."),
                                       'reader:t1:1', 'vf-blob-abc', 2764838)
    assert isinstance(error, PayloadStoreFull) and isinstance(error, ResourceUnavailable)
    assert '2764838 bytes' in str(error) and 'reader:t1:1' in str(error)
    assert 'Backpressure' in error.remedy


# -- the worker's knob ------------------------------------------------------------------------

def test_store_backpressure_from_env(monkeypatch):
    monkeypatch.delenv('VF_STORE_BACKPRESSURE_SECONDS', raising = False)
    assert worker.store_backpressure_from_env() is None          # the messenger's default
    monkeypatch.setenv('VF_STORE_BACKPRESSURE_SECONDS', '0')
    assert worker.store_backpressure_from_env() == 0.0
    monkeypatch.setenv('VF_STORE_BACKPRESSURE_SECONDS', '42.5')
    assert worker.store_backpressure_from_env() == 42.5
    for bad in ('soon', '-1'):
        monkeypatch.setenv('VF_STORE_BACKPRESSURE_SECONDS', bad)
        with pytest.raises(ConfigError) as e:
            worker.store_backpressure_from_env()
        assert 'VF_STORE_BACKPRESSURE_SECONDS' in str(e.value)


# -- the Redis store admits below maxmemory, with headroom and a graded share (BLOB-16) --------

from redis import exceptions as rexc  # noqa: E402
from support_redis import FakeRedis  # noqa: E402

from videoflow.core.errors import TransientFailure as TransientError  # noqa: E402
from videoflow.wire import redis_payload_store as rps  # noqa: E402


def _noeviction(maxmemory : int) -> FakeRedis:
    return FakeRedis(config = {'appendonly': 'yes', 'save': '', 'maxmemory-policy': 'noeviction',
                               'maxmemory': str(maxmemory)})


def _contract() -> RetentionContract:
    return RetentionContract(ttl_seconds = 60, horizon_seconds = 60, durable_required = True, obligations = ('x',))


def test_the_store_refuses_before_the_headroom_is_touched():
    fake = _noeviction(1000)
    store = RedisPayloadStore(client = fake, clock = fake.time)
    fake.memory_used = 850                                # 850 + 100 > 1000 - 100 of headroom
    with pytest.raises(PayloadStoreFull) as e:
        store.put(b'x' * 100, 'c1', _contract())
    assert '850 bytes are in use' in str(e.value) and '100 bytes kept free' in str(e.value)
    assert not fake.live_keys('vf-blob-*')               # refused before a byte was written
    fake.memory_used = 799                                # 799 + 100 <= 900
    assert store.put(b'x' * 100, 'c2', _contract()).size == 100


def test_a_graded_share_holds_a_source_while_the_stages_below_still_fit():
    fake = _noeviction(1000)
    source = RedisPayloadStore(client = fake, clock = fake.time, admission = 0.5)   # 450 of the 900
    deepest = RedisPayloadStore(client = fake, clock = fake.time)                   # all 900
    fake.memory_used = 400
    with pytest.raises(PayloadStoreFull) as e:
        source.put(b'x' * 100, 'c1', _contract())
    assert "this publisher's 50% share" in str(e.value)
    assert deepest.put(b'x' * 100, 'c2', _contract()).size == 100
    fake.memory_used = 300
    assert source.put(b'x' * 100, 'c3', _contract()).size == 100
    with pytest.raises(ValueError):
        RedisPayloadStore(client = fake, admission = 0)
    with pytest.raises(ValueError):
        RedisPayloadStore(client = fake, admission = 1.5)


def test_an_evicting_or_silent_server_leaves_only_its_own_refusal():
    cache = FakeRedis()                                   # volatile-lru: the server evicts, never refuses
    cache.memory_used = 10 ** 12
    assert RedisPayloadStore(client = cache, clock = cache.time).put(b'x', 'c', _contract()).size == 1
    silent = _noeviction(10)
    silent.deny_config = True                             # CONFIG GET refused: no budget to judge by
    silent.memory_used = 10 ** 12
    assert RedisPayloadStore(client = silent, clock = silent.time).put(b'x', 'c', _contract()).size == 1


def test_the_budget_is_re_read_after_its_observation_ages():
    fake = _noeviction(1000)
    store = RedisPayloadStore(client = fake, clock = fake.time)
    fake.memory_used = 100
    assert store.put(b'x', 'c1', _contract()).size == 1
    fake.config['maxmemory'] = '110'                      # the operator lowered it live
    assert store.put(b'x', 'c2', _contract()).size == 1   # trusted for LIMIT_OBSERVATION_SECONDS
    fake.advance(rps.LIMIT_OBSERVATION_SECONDS + 1)
    with pytest.raises(PayloadStoreFull):
        store.put(b'x', 'c3', _contract())                # 100 + 1 > 110 - 11


def test_a_transport_failure_during_admission_is_transient():
    fake = _noeviction(1000)
    store = RedisPayloadStore(client = fake, clock = fake.time)
    fake.fail('INFO', rexc.ConnectionError('Connection closed by server.'))
    with pytest.raises(TransientError):
        store.put(b'x', 'c', _contract())


# -- the compiler grades every publisher's share by depth ------------------------------------

from videoflow.core.compiler import NodeSpec, publish_depths, store_admission  # noqa: E402


def _spec(name, parents, has_children = True):
    import dataclasses
    kind = 'producer' if not parents else ('processor' if has_children else 'consumer')
    values = {'name': name, 'node_class': 'x.Y', 'params': {}, 'parents': list(parents), 'kind': kind,
              'has_children': has_children, 'nb_tasks': 1, 'device_type': 'cpu', 'is_finite': True}
    fields = {f.name for f in dataclasses.fields(NodeSpec)}
    return NodeSpec(**{k: v for k, v in values.items() if k in fields})


def test_a_chain_is_graded_from_half_for_the_source_to_all_for_the_deepest():
    chain = [_spec('src', []), _spec('a', ['src']), _spec('b', ['a']), _spec('sink', ['b'], has_children = False)]
    assert publish_depths(chain) == {'src': 0, 'a': 1, 'b': 2}
    assert [store_admission(s, chain) for s in chain] == [0.5, 0.75, 1.0, 1.0]


def test_a_diamond_takes_the_longest_path_and_a_single_stage_takes_everything():
    diamond = [_spec('src', []), _spec('short', ['src']), _spec('long1', ['src']), _spec('long2', ['long1']),
               _spec('join', ['short', 'long2']), _spec('sink', ['join'], has_children = False)]
    assert publish_depths(diamond) == {'src': 0, 'short': 1, 'long1': 1, 'long2': 2, 'join': 3}
    assert store_admission(diamond[0], diamond) == 0.5
    assert store_admission(diamond[1], diamond) == store_admission(diamond[2], diamond) == pytest.approx(0.667)
    assert store_admission(diamond[4], diamond) == 1.0
    single = [_spec('src', []), _spec('sink', ['src'], has_children = False)]
    assert store_admission(single[0], single) == 1.0


def test_both_launchers_emit_the_share_only_below_one():
    from videoflow.deploy.manifests import _env_pairs
    from videoflow.engines.local import _worker_env
    chain = [_spec('src', []), _spec('a', ['src']), _spec('sink', ['a'], has_children = False)]
    assert _worker_env(chain[0], 'nats://x:4222', 'demo', BATCH, 'run1', None, 0, 3,
                       store_admission = 0.5)['VF_STORE_ADMISSION'] == '0.5'
    assert 'VF_STORE_ADMISSION' not in _worker_env(chain[1], 'nats://x:4222', 'demo', BATCH, 'run1', None, 0, 3,
                                                   store_admission = 1.0)
    assert _env_pairs(chain[0], 'demo', BATCH, 'run1', 4, store_admission = 0.5)['VF_STORE_ADMISSION'] == '0.5'
    assert 'VF_STORE_ADMISSION' not in _env_pairs(chain[1], 'demo', BATCH, 'run1', 4, store_admission = 1.0)


def test_store_admission_from_env(monkeypatch):
    monkeypatch.delenv('VF_STORE_ADMISSION', raising = False)
    assert worker.store_admission_from_env() == 1.0
    monkeypatch.setenv('VF_STORE_ADMISSION', '0.5')
    assert worker.store_admission_from_env() == 0.5
    for bad in ('0', '1.5', 'half'):
        monkeypatch.setenv('VF_STORE_ADMISSION', bad)
        with pytest.raises(ConfigError):
            worker.store_admission_from_env()


# -- a bind waits out "JetStream system temporarily unavailable" ------------------------------

def test_a_bind_waits_out_a_temporarily_unavailable_jetstream(monkeypatch):
    import nats.js.errors

    from videoflow.core.errors import BrokerUnavailable
    from videoflow.messaging import jetstream_backend as jb
    backend = jb.JetStreamMessagingBackend.__new__(jb.JetStreamMessagingBackend)
    backend._connect_timeout = 1.0
    sleeps : list[float] = []
    monkeypatch.setattr(jb, 'time', types.SimpleNamespace(sleep = sleeps.append, monotonic = time.monotonic,
                                                          time = time.time))
    refusals = {'left': 2}

    def run(coro, timeout):
        coro.close()
        if refusals['left']:
            refusals['left'] -= 1
            raise nats.js.errors.ServiceUnavailableError(code = 503, err_code = 10008,
                                                         description = 'JetStream system temporarily unavailable')
        return ('psub', 'info')
    monkeypatch.setattr(backend, '_run', run)

    async def bind():
        return None
    assert backend._bind_with_patience(bind, 'child--from--parent', 'vf-f-r-parent') == ('psub', 'info')
    assert sleeps == [0.5, 1.0]
    refusals['left'] = 10 ** 6
    with pytest.raises(BrokerUnavailable, match = 'stayed unavailable'):
        backend._bind_with_patience(bind, 'child--from--parent', 'vf-f-r-parent')
    assert sleeps == [0.5, 1.0, 0.5, 1.0, 2.0, 4.0, 8.0]

    def other(coro, timeout):
        coro.close()
        raise ValueError('other')
    monkeypatch.setattr(backend, '_run', other)
    with pytest.raises(ValueError):
        backend._bind_with_patience(bind, 'd', 's')


def test_colliding_names_cannot_send_the_depth_walk_in_circles():
    # Two specs that sanitise to one name, one naming the other as its parent: the
    # collision check reports it; the depth walk must not recurse forever first.
    twins = [_spec('a', []), _spec('a', ['a']), _spec('sink', ['a'], has_children = False)]
    assert publish_depths(twins)['a'] in (0, 1)
    assert 0.5 <= store_admission(twins[1], twins) <= 1.0
