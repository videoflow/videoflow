'''
Conformance cases: MSG-005, MSG-006.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions over a driver (``_msgdrivers``)
or a provisioner, so the broker-level primary, the ``memory`` variant and the negative
control decide with the same assertions.

The two cases are the two ways provisioning used to lie: ``_ensure_consumer``
swallowed every create error at debug level (a producer then published into absent
interest, and INTEREST retention discarded the input), and an existing stream or
durable was accepted on its name alone. Both are decided here by what the broker
reads back, not by what the request said.
'''
from __future__ import absolute_import, division, print_function

import json
import threading
import time
from typing import Any, Callable, Dict, List

import defects
import pytest
from _brokers import run_async, stream_state, unique_ids
from _msgdrivers import JetStreamDriver, MemoryDriver, spec

from videoflow.backends import faults
from videoflow.backends.capabilities import RELIABLE_WORK, RETENTION_INTEREST, RETENTION_LIMITS
from videoflow.backends.identity import owner_labels
from videoflow.backends.messaging import Completed, SubscriptionId, SubscriptionSpec
from videoflow.backends.outcomes import Accepted
from videoflow.core import constants
from videoflow.core.constants import BATCH
from videoflow.core.errors import IncompatibleProfile, ResourceUnavailable, VideoflowError
from videoflow.messaging import topology

NATS_TIMEOUT = 180


def _write(evidence_dir : Any, name : str, record : Dict[str, Any]) -> None:
    (evidence_dir / name).write_text(json.dumps(record, indent = 2, default = str))


# -- MSG-005 -------------------------------------------------------------------------

SPECS = [spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False)]
FAULTS = ('auth', 'timeout', 'resource-limit')


class _MemoryProvisioning:
    '''Provisioning on the model: the required subscription's creation fails at its barrier.'''
    typed_errors = False

    def __init__(self, driver : MemoryDriver, record_faults : Callable[..., None] | None) -> None:
        self.driver = driver
        self._record_faults = record_faults
        self.trace : List[Dict[str, Any]] = []

    def attempt(self, fault : str | None) -> None:
        if fault is None:
            self.driver.provision(SPECS, BATCH)
            return
        factory = {
            'auth': lambda: PermissionError('injected: consumer creation denied'),
            'timeout': lambda: TimeoutError('injected: consumer API timed out'),
            'resource-limit': lambda: ResourceUnavailable('injected: maximum consumers limit reached',
                                                          remedy = 'raise the account limit'),
        }[fault]
        schedule = faults.FaultSchedule({'provision.subscription.before': faults.Nth(1, faults.RaiseError(factory))})
        try:
            with schedule:
                self.driver.provision(SPECS, BATCH)
        finally:
            if self._record_faults is not None:
                self._record_faults(schedule)
            self.trace.append({'fault': fault, 'fired': schedule.fired()})

    def published_inputs(self) -> int:
        return self.driver.retained(self.driver.channel_spec('parent', RELIABLE_WORK, RETENTION_INTEREST).id)

    def interest(self) -> List[str]:
        return self.driver.subscriptions(self.driver.channel_spec('parent', RELIABLE_WORK, RETENTION_INTEREST).id)

    def conflicting_reprovision(self) -> None:
        channel = self.driver.channel_spec('parent', RELIABLE_WORK, RETENTION_INTEREST).id
        sub = SubscriptionId(channel, 'child', None)
        self.driver.backend.ensure_subscription(SubscriptionSpec(sub, False, 60, 4, 8, 1 << 20), 'conflict')


class _JetStreamProvisioning:
    '''
    Provisioning on the broker through ``topology.provision_flow`` — the deploy
    path — with three real refusals of the required consumer creation: an
    authorization denial (the ``restricted`` user), a client timeout of the
    consumer API, and a stream-level ``max_consumers`` limit.
    '''
    typed_errors = True

    def __init__(self, plain_url : str, restricted_url : str, flow : str, run : str) -> None:
        self.plain_url, self.restricted_url = plain_url, restricted_url
        self.flow, self.run = flow, run
        self.trace : List[Dict[str, Any]] = []
        self.server_errors : List[str] = []
        self.limited_run = f'{run}-rl'

    def attempt(self, fault : str | None) -> None:
        import nats  # optional dep (distributed extras)
        from nats.js.api import DiscardPolicy, RetentionPolicy, StreamConfig
        if fault is None:
            topology.provision_flow_sync(self.plain_url, SPECS, self.flow, self.run, BATCH, timeout = 60)
            return
        if fault == 'auth':
            async def _record(e : BaseException) -> None:
                self.server_errors.append(str(e))
            topology.provision_flow_sync(self.restricted_url, SPECS, self.flow, self.run, BATCH, timeout = 90,
                                         connect_options = {'error_cb': _record})
            return
        if fault == 'timeout':
            async def _go_timeout() -> None:
                nc = await nats.connect(self.plain_url)
                try:
                    real = nc.jetstream()

                    class _Unanswered:
                        '''The consumer API never answers: create and read-back both time out.'''
                        def __getattr__(self, name : str) -> Any:
                            return getattr(real, name)

                        async def add_consumer(self, *args : Any, **kwargs : Any) -> Any:
                            raise nats.errors.TimeoutError('injected: consumer API timed out')

                        async def consumer_info(self, *args : Any, **kwargs : Any) -> Any:
                            raise nats.errors.TimeoutError('injected: consumer API timed out')

                    nc.jetstream = lambda **kw: _Unanswered()  # type: ignore[method-assign]
                    await topology.provision_flow(nc, SPECS, self.flow, self.run, BATCH)
                finally:
                    await nc.drain()
            run_async(_go_timeout, timeout = 90)
            return
        assert fault == 'resource-limit'

        async def _go_limit() -> None:
            nc = await nats.connect(self.plain_url)
            try:
                js = nc.jetstream()
                name = topology.stream_name_for(self.flow, self.limited_run, 'parent')
                # An externally created parent stream that admits one consumer: the
                # EOS anchor takes it, the child's durable hits the limit.
                await js.add_stream(StreamConfig(
                    name = name, retention = RetentionPolicy.INTEREST, discard = DiscardPolicy.NEW, max_consumers = 1,
                    subjects = [topology.subject_for(self.flow, self.limited_run, 'parent'),
                                topology.eos_subject_for(self.flow, self.limited_run, 'parent')],
                    metadata = owner_labels(self.flow, self.limited_run, node = 'parent', kind = 'stream')))
                await topology.provision_flow(nc, SPECS, self.flow, self.limited_run, BATCH)
            finally:
                await nc.drain()
        run_async(_go_limit, timeout = 90)

    def published_inputs(self) -> int:
        total = 0
        for run in (self.run, self.limited_run):
            try:
                total += int(stream_state(self.plain_url, topology.stream_name_for(self.flow, run, 'parent')).messages)
            except Exception:  # noqa: BLE001 — a stream that was never created holds nothing
                pass
        return total

    def interest(self) -> List[str]:
        import nats  # optional dep (distributed extras)

        async def _go() -> List[str]:
            nc = await nats.connect(self.plain_url)
            try:
                infos = await nc.jetstream().consumers_info(topology.stream_name_for(self.flow, self.run, 'parent'))
                return sorted(i.name for i in infos if '--eos--' not in i.name)
            finally:
                await nc.drain()
        return run_async(_go)

    def conflicting_reprovision(self) -> None:
        import nats  # optional dep (distributed extras)
        from nats.js.api import AckPolicy

        async def _go() -> None:
            nc = await nats.connect(self.plain_url)
            try:
                config = topology.consumer_config_for(self.flow, self.run, 'child', 'parent')
                config.ack_policy = AckPolicy.ALL
                await topology._ensure_consumer(nc.jetstream(), topology.stream_name_for(self.flow, self.run, 'parent'),
                                                config)
            finally:
                await nc.drain()
        run_async(_go)

    def close(self) -> None:
        from _brokers import delete_dlq, delete_run
        for run in (self.run, self.limited_run):
            delete_run(self.plain_url, self.flow, run)
        delete_dlq(self.plain_url, self.flow)


def _oracle_msg_005(prov : Any, driver : Any, record : Dict[str, Any]) -> None:
    '''
    Every refused consumer creation leaves provisioning not-ready — an error, never a
    return — so the producer is never started and nothing is published into absent
    interest. The successful retry reads the interest back, and inputs published
    before the worker starts wait for it.
    '''
    for fault in FAULTS:
        started = time.monotonic()
        try:
            prov.attempt(fault)
        except Exception as e:  # noqa: BLE001 — the kind is recorded; the point is that it was raised
            entry = {'fault': fault, 'outcome': 'not-ready', 'error': f'{type(e).__name__}: {e}'[:300],
                     'seconds': time.monotonic() - started}
            if prov.typed_errors:
                assert isinstance(e, VideoflowError), f'{fault}: {type(e).__name__} is not a typed videoflow error'
                assert e.remedy, e
                entry['code'] = e.code
            record.setdefault('attempts', []).append(entry)
        else:
            raise AssertionError(f'{fault}: provisioning reported ready although the required consumer creation failed')
        assert prov.published_inputs() == 0, f'{fault}: inputs were published during failed provisioning'
    record['server_errors'] = list(getattr(prov, 'server_errors', ()))
    # Restore access and retry the same run.
    prov.attempt(None)
    interest = prov.interest()
    assert topology.durable_name_for('child', 'parent') in interest or 'child' in interest, interest
    record['interest_after_retry'] = interest
    # Idempotent: an identical retry changes nothing; a conflicting one is not accepted on the name.
    prov.attempt(None)
    assert prov.interest() == interest
    with pytest.raises(IncompatibleProfile):
        prov.conflicting_reprovision()
    # Inputs published before the delayed worker starts remain available to it.
    channel = driver.channel_spec('parent', RELIABLE_WORK, RETENTION_INTEREST).id
    ids = [f'early-{i}' for i in range(5)]
    for pid in ids:
        assert isinstance(driver.publish(channel, pid, pid.encode()), Accepted)
    record['published_before_worker'] = ids
    bound = driver.bind(channel, 'child', receiver = 'delayed-worker', ack_wait = 5, max_deliver = 4, credit = 8)
    received : List[str] = []

    def collect() -> bool:
        for delivery in driver.receive(bound, timeout = 0.5):
            received.append(delivery.token.message_id)
            driver.settle(bound, delivery.token, Completed())
        return len(received) >= len(ids)
    assert driver.until(collect, 30), received
    assert sorted(received) == sorted(ids), received
    record['delayed_worker_received'] = received


@pytest.mark.case('MSG-005')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_005_provisioning_failures_prevent_producers_from_publishing(nats_restricted_url, evidence_dir,
                                                                          monkeypatch) -> None:
    '''
    MSG-005 (P0, messaging, broker): Provisioning failures prevent producers from publishing
    into absent interest.

    Acceptance: Zero published application inputs during failed provisioning. After successful
    retry, publish-before-worker-start inputs remain available to the delayed worker.

    Runs on the ``restricted`` broker: its ``restricted`` user is denied
    ``$JS.API.CONSUMER.CREATE.>`` (a real authorization failure the client sees as a
    timed-out request plus a permissions-violation callback), while the plain URL
    on the same server has full permissions for the retry.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    plain_url = _plain_url(nats_restricted_url)
    flow, run = unique_ids('msg005')
    prov = _JetStreamProvisioning(plain_url, nats_restricted_url, flow, run)
    driver = JetStreamDriver(plain_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_005(prov, driver, record)
        assert any('permissions violation' in e.lower() for e in record['server_errors']), record['server_errors']
    finally:
        driver.close()
        prov.close()
        record['trace'] = prov.trace
        _write(evidence_dir, 'provisioning_trace.json', record)


def _plain_url(restricted_url : str) -> str:
    '''The same server without credentials: its ``no_auth_user`` has full permissions.'''
    from urllib.parse import urlsplit, urlunsplit
    parts = urlsplit(restricted_url)
    host = parts.hostname or 'localhost'
    netloc = f'{host}:{parts.port}' if parts.port else host
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


@pytest.mark.case('MSG-005')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_005_memory_refused_subscription_creation_is_never_ready(evidence_dir, record_faults) -> None:
    driver = MemoryDriver('f', 'r')
    prov = _MemoryProvisioning(driver, record_faults)
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_005(prov, driver, record)
    finally:
        driver.close()
        record['trace'] = prov.trace
        _write(evidence_dir, 'provisioning_trace.json', record)


@pytest.mark.negative_control(of = 'MSG-005')
def test_msg_005_detects_a_swallowing_subscription_provisioner(monkeypatch) -> None:
    '''A provisioner that logs a refused consumer creation and reports ready must fail the oracle.'''
    defects.swallowing_subscription_provisioner(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_005, _MemoryProvisioning(driver, None), driver, {})
    finally:
        driver.close()


@pytest.mark.negative_control(of = 'MSG-005')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_005_detects_the_swallowing_consumer_provisioner_on_the_broker(nats_restricted_url, monkeypatch) -> None:
    '''The reviewed ``_ensure_consumer`` (debug log, no read-back) against the real denial.'''
    monkeypatch.setattr(constants, 'RFC0006', True)
    defects.swallowing_consumer_provisioner(monkeypatch)
    plain_url = _plain_url(nats_restricted_url)
    flow, run = unique_ids('msg005nc')
    prov = _JetStreamProvisioning(plain_url, nats_restricted_url, flow, run)
    driver = JetStreamDriver(plain_url, flow, run)
    try:
        assert defects.detects(_oracle_msg_005, prov, driver, {})
    finally:
        driver.close()
        prov.close()


# -- MSG-006 -------------------------------------------------------------------------

class _MemorySeeds:
    '''Externally created resources on the model: an earlier provisioning under the desired names.'''
    def __init__(self, driver : MemoryDriver) -> None:
        self.driver = driver

    def immutable_stream(self, node : str) -> None:
        '''The desired name, provisioned earlier without persistence — an immutable channel setting.'''
        self.driver.backend.ensure_channel(self.driver.channel_spec(node, RELIABLE_WORK, RETENTION_INTEREST,
                                                                    persistent = False), 'external')

    def mutable_stream(self, node : str) -> None:
        '''The desired name with a smaller message limit — a mutable setting the reconciliation converges.'''
        self.driver.backend.ensure_channel(self.driver.channel_spec(node, RELIABLE_WORK, RETENTION_INTEREST, max_msgs = 5,
                                                                    persistent = True), 'external')

    def subscription(self, channel : Any, consumer : str, immutable_conflict : bool = False, credit : int = 8) -> None:
        sub = SubscriptionId(channel, consumer, None)
        self.driver.backend.ensure_subscription(SubscriptionSpec(sub, not immutable_conflict, 60, 4, credit, 1 << 20),
                                                'external')

    def deny_updates(self) -> Any:
        return None                                              # the model has no update path to deny

    def effective_stream(self, node : str) -> Dict[str, Any]:
        cid = self.driver.channel_spec(node, RELIABLE_WORK, RETENTION_INTEREST).id
        spec_ = self.driver.backend._channels[cid].spec
        return {'retention': spec_.retention, 'max_msgs': spec_.max_msgs, 'subjects': ['data', 'eos']}


class _JetStreamSeeds:
    def __init__(self, driver : JetStreamDriver) -> None:
        self.driver = driver

    def immutable_stream(self, node : str) -> None:
        '''The desired name on memory storage, which no update can change (nats-server 2.10, err_code 10052).'''
        from nats.js.api import StorageType
        self._stream(node, RETENTION_INTEREST, 10_000, False, StorageType.MEMORY)

    def mutable_stream(self, node : str) -> None:
        '''The desired name with the wrong retention, limit and subjects — all of which this server updates in place.'''
        self._stream(node, RETENTION_LIMITS, 5, True, None)

    def _stream(self, node : str, retention : str, max_msgs : int, data_subject_only : bool, storage : Any) -> None:
        import nats  # optional dep (distributed extras)
        from nats.js.api import DiscardPolicy, RetentionPolicy, StreamConfig
        flow, run = self.driver.flow_id, self.driver.run_id
        subjects = [topology.subject_for(flow, run, node)]
        if not data_subject_only:
            subjects.append(topology.eos_subject_for(flow, run, node))
        config = StreamConfig(name = topology.stream_name_for(flow, run, node), subjects = subjects,
                              retention = RetentionPolicy.LIMITS if retention == RETENTION_LIMITS else RetentionPolicy.INTEREST,
                              discard = DiscardPolicy.OLD if retention == RETENTION_LIMITS else DiscardPolicy.NEW,
                              max_msgs = max_msgs, storage = storage,
                              metadata = owner_labels(flow, run, node = node, kind = 'stream'))

        async def _go() -> None:
            nc = await nats.connect(self.driver.nats_url)
            try:
                await nc.jetstream().add_stream(config)
            finally:
                await nc.drain()
        run_async(_go)

    def subscription(self, channel : Any, consumer : str, immutable_conflict : bool = False, credit : int = 8) -> None:
        import nats  # optional dep (distributed extras)
        from nats.js.api import AckPolicy
        config = topology.consumer_config_for(channel.flow_id, channel.run_id, consumer, channel.node,
                                              ack_wait = 2, max_deliver = 4, max_ack_pending = credit)
        if immutable_conflict:
            config.ack_policy = AckPolicy.ALL

        async def _go() -> None:
            nc = await nats.connect(self.driver.nats_url)
            try:
                await nc.jetstream().add_consumer(self.driver.stream(channel), config)
            finally:
                await nc.drain()
        run_async(_go)

    def deny_updates(self) -> Any:
        '''A context in which every stream update is refused by policy.'''
        backend = self.driver._prov
        real = backend._js

        class _Denied:
            def __getattr__(self, name : str) -> Any:
                return getattr(real, name)

            async def update_stream(self, *args : Any, **kwargs : Any) -> Any:
                raise PermissionError('injected: stream update denied by policy')

        class _Context:
            def __enter__(self_) -> None:
                backend._js = _Denied()

            def __exit__(self_, *exc : Any) -> None:
                backend._js = real
        return _Context()

    def effective_stream(self, node : str) -> Dict[str, Any]:
        import nats  # optional dep (distributed extras)

        async def _go() -> Dict[str, Any]:
            nc = await nats.connect(self.driver.nats_url)
            try:
                info = await nc.jetstream().stream_info(topology.stream_name_for(self.driver.flow_id, self.driver.run_id, node))
                config = info.config
                return {'retention': topology._plain(config.retention), 'max_msgs': config.max_msgs,
                        'subjects': list(config.subjects or []), 'discard': topology._plain(config.discard),
                        'profile_findings': list(topology.profile_mismatches(RELIABLE_WORK, config))}
            finally:
                await nc.drain()
        return run_async(_go)


def _oracle_msg_006(driver : Any, seeds : Any, record : Dict[str, Any]) -> None:
    trace : List[Dict[str, Any]] = []
    # Immutable stream mismatch: the desired name on volatile storage where a
    # persistent channel is required — an update cannot change it.
    seeds.immutable_stream('p_vol')
    with pytest.raises(IncompatibleProfile) as info:
        driver.channel_verified('p_vol', RELIABLE_WORK, RETENTION_INTEREST, persistent = True)
    assert ('storage' in str(info.value) or 'persistence' in str(info.value)) and info.value.remedy, info.value
    trace.append({'step': 'immutable stream', 'outcome': 'rejected', 'message': str(info.value)[:300]})
    # Mutable stream fields converge and are read back — the read-back, not the
    # request, is what proves the broker changed them.
    seeds.mutable_stream('p_mut')
    verified = driver.channel_verified('p_mut', RELIABLE_WORK, RETENTION_INTEREST, max_msgs = 10_000, persistent = True)
    assert verified.mismatches == (), verified.mismatches
    effective = seeds.effective_stream('p_mut')
    assert effective['max_msgs'] == 10_000 and 'interest' in str(effective['retention']).lower(), effective
    assert len(effective['subjects']) == 2, effective
    assert not effective.get('profile_findings'), effective
    trace.append({'step': 'mutable stream', 'outcome': 'converged', 'effective': effective})
    channel = verified.spec.id
    # Immutable consumer mismatch (ack policy / delivery mode) on the desired durable name.
    seeds.subscription(channel, 'c', immutable_conflict = True)
    with pytest.raises(IncompatibleProfile) as info:
        driver.provision_subscription(channel, 'c', ack_wait = 2, max_deliver = 4, credit = 8)
    trace.append({'step': 'immutable consumer', 'outcome': 'rejected', 'message': str(info.value)[:300]})
    # Mutable consumer credit converges.
    seeds.subscription(channel, 'd', credit = 3)
    driver.provision_subscription(channel, 'd', ack_wait = 2, max_deliver = 4, credit = 10)
    effective_d = driver.subscription_effective(channel, 'd')
    assert effective_d['max_ack_pending'] == 10, effective_d
    trace.append({'step': 'mutable consumer', 'outcome': 'converged', 'effective': effective_d})
    # A denied update is a failed validation, never a success on the name.
    denial = seeds.deny_updates()
    if denial is not None:
        seeds.mutable_stream('p_denied')
        with denial, pytest.raises(IncompatibleProfile) as info:
            driver.channel_verified('p_denied', RELIABLE_WORK, RETENTION_INTEREST, max_msgs = 10_000, persistent = True)
        assert 'max_msgs' in str(info.value), info.value
        trace.append({'step': 'denied update', 'outcome': 'rejected', 'message': str(info.value)[:300]})
    else:
        trace.append({'step': 'denied update', 'outcome': 'not applicable to this driver'})
    # Concurrent identical provisioning: harmless, and nothing relaxed.
    results : List[Any] = []
    errors : List[BaseException] = []

    def provision() -> None:
        try:
            results.append(driver.channel_verified('p_conc', RELIABLE_WORK, RETENTION_INTEREST, max_msgs = 42))
        except BaseException as e:  # noqa: BLE001
            errors.append(e)
    threads = [threading.Thread(target = provision) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(60)
    assert not errors, errors
    assert len(results) == 2 and all(r.mismatches == () for r in results), results
    concurrent = seeds.effective_stream('p_conc')
    assert concurrent['max_msgs'] == 42 and 'interest' in str(concurrent['retention']).lower(), concurrent
    trace.append({'step': 'concurrent identical', 'outcome': 'both verified', 'effective': concurrent})
    record['trace'] = trace


@pytest.mark.case('MSG-006')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_006_reconciliation_detects_incompatible_existing_broker(nats_url, evidence_dir, monkeypatch) -> None:
    '''
    MSG-006 (P0, messaging, broker): Reconciliation detects incompatible existing broker
    configuration.

    Acceptance: Producer starts only when the read-back effective configuration satisfies every
    mandatory field. Each denied incompatible update fails validation.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    flow, run = unique_ids('msg006')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_006(driver, _JetStreamSeeds(driver), record)
    finally:
        driver.close()
        _write(evidence_dir, 'reconciliation_trace.json', record)


@pytest.mark.case('MSG-006')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_006_memory_existing_resources_are_inspected_not_trusted(evidence_dir) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_006(driver, _MemorySeeds(driver), record)
    finally:
        driver.close()
        _write(evidence_dir, 'reconciliation_trace.json', record)


@pytest.mark.negative_control(of = 'MSG-006')
def test_msg_006_detects_a_name_match_provisioner(monkeypatch) -> None:
    defects.name_match_provisioner(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_006, driver, _MemorySeeds(driver), {})
    finally:
        driver.close()
