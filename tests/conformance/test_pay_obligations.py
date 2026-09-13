'''
Conformance cases: PAY-004, PAY-005, PAY-006, PAY-008, PAY-021.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions taking the component under
test — a ``PayloadStore``, or a rig standing a flow up on the memory backends or on
the compose broker (``_payloads.py``) — so the paired negative control can run the
same oracle against the reviewed defect (``defects_pay.py``) and prove it fails.
'''
from __future__ import absolute_import, division, print_function

import os
import subprocess
import sys
import textwrap
import threading
import time
from typing import Any, Callable, Dict, List, Optional

import defects
import defects_pay
import numpy as np
import pytest
from _payloads import (
    JetStreamRig,
    MemoryRig,
    frame_array,
    frame_bytes,
    ledger,
    object_exists,
    obligations_of,
    spec,
    wait_until,
    write_evidence,
)
from _toxiproxy import Toxiproxy

from videoflow.backends import faults
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.payload import MemoryPayloadStore
from videoflow.backends.outcomes import Known
from videoflow.backends.payload import ImmutablePayloadRef, PayloadBytes, PayloadStore, RetentionContract
from videoflow.core import constants
from videoflow.core.compiler import blob_reader_ids
from videoflow.core.constants import BATCH
from videoflow.wire.serialization import peek_envelope

pytestmark = pytest.mark.timeout(180)

FRAME = frame_array((1000, 1000), seed = 4)
FRAME_BYTES = frame_bytes(256 * 1024, seed = 5)
#: G — the declared reclamation bound after the last reader completes / after recovery.
G_SECONDS = 10.0


def _specs() -> list:
    return [spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False),
            spec('other', ['parent'], 'consumer', False)]


def _last_key(rig : Any, node : str) -> str:
    bodies = rig.retained(node)
    assert bodies, f'nothing retained on {node}'
    key = peek_envelope(bodies[-1]).get('blob_ref')
    assert key, 'the newest envelope carries no payload reference'
    return key


def _contract(obligations : tuple, ttl : int = 3600, horizon : int = 3600) -> RetentionContract:
    return RetentionContract(ttl, horizon, True, tuple(obligations))


def _fake_redis_store() -> PayloadStore:
    '''The Redis store on the in-process fake: real WATCH/MULTI/EXEC semantics, no server.'''
    from support_redis import FakeRedis  # tests/support_redis.py, on sys.path via conftest

    from videoflow.wire.redis_payload_store import RedisPayloadStore
    fake = FakeRedis()
    return RedisPayloadStore(client = fake, clock = fake.time)


# -- PAY-004 ----------------------------------------------------------------------

class _Sever:
    '''How a rig loses an acknowledgment before the broker takes it (the toxiproxy cut, or its model).'''
    def arm(self) -> None:
        raise NotImplementedError

    def restore(self) -> None:
        raise NotImplementedError

    def child_url(self) -> Optional[str]:
        return None

    def fired(self) -> Dict[str, int]:
        return {}


class _ToxiproxySever(_Sever):
    '''Disable the ``nats`` proxy: every live connection through it is cut and the buffered ack is lost.'''
    def __init__(self, toxiproxy : Toxiproxy, proxied_url : str) -> None:
        self._toxiproxy = toxiproxy
        self._url = proxied_url

    def arm(self) -> None:
        self._toxiproxy.enable('nats', False)

    def restore(self) -> None:
        self._toxiproxy.enable('nats', True)

    def child_url(self) -> Optional[str]:
        return self._url


class _ModelSever(_Sever):
    '''The model: the settlement raises before the broker sees it, and the lease later expires.'''
    def __init__(self, rig : MemoryRig, ack_wait : int) -> None:
        self._rig = rig
        self._ack_wait = ack_wait
        self._schedule = faults.FaultSchedule({'settle.before': faults.Nth(1, faults.RaiseError(
            lambda: ConnectionError('the acknowledgment never reached the broker')))})

    def arm(self) -> None:
        self._schedule.install()

    def restore(self) -> None:
        self._schedule.uninstall()
        self._rig.clock.advance(self._ack_wait + 1)         # the lease of the un-acked delivery expires

    def fired(self) -> Dict[str, int]:
        return self._schedule.fired()


def _oracle_pay_004(rig : Any, sever : _Sever, evidence : Dict[str, Any], ack_wait : int) -> faults.FaultSchedule:
    '''
    (b) The broker takes the ack but its confirmation is lost: no release; the
    certified release is the broker's durable evidence, reconciled. (a) The ack
    never reaches the broker: no release; a restarted worker is redelivered the
    message, resolves the original bytes and settles it for good.
    '''
    parent = rig.messenger('parent', [], blob_reader_ids = ['child'])
    child = rig.messenger('child', ['parent'], ack_wait = ack_wait)
    schedule = faults.FaultSchedule({'settle.after': faults.Nth(1, faults.DropResponse())})
    with schedule:
        parent.publish_message(FRAME)
        key_b = _last_key(rig, 'parent')
        inputs = child.receive_message()
        assert np.array_equal(inputs['parent']['message'], FRAME)
        child.ack_inputs()                                   # sent, taken, confirmation lost
    held = obligations_of(rig.store, key_b)
    evidence['lost_confirmation'] = {'key': key_b, 'obligations_after_ack': sorted(held),
                                     'object_exists': object_exists(rig.store, key_b)}
    assert held == {'child'} and object_exists(rig.store, key_b), \
        'a client-side send result was taken for a confirmed settlement: the obligation was released'
    state = rig.consumer_state('child', 'parent')
    assert state.pending == 0 and state.unacked == 0, state          # the broker did take the ack
    started = time.monotonic()
    result = rig.store.reconcile(ledger({key_b: ()}), 'pay-004:after-confirmed-settlement')
    evidence['lost_confirmation']['reconcile'] = {'reclaimed': list(result.reclaimed), 'retained': list(result.retained),
                                                  'took_s': time.monotonic() - started}
    assert key_b in result.reclaimed and not object_exists(rig.store, key_b)
    child.close()

    # (a) the ack that never reaches the broker.
    first = rig.messenger('child', ['parent'], nats_url = sever.child_url(), ack_wait = ack_wait) if sever.child_url() \
        else rig.messenger('child', ['parent'], ack_wait = ack_wait)
    parent.publish_message(FRAME)
    key_a = _last_key(rig, 'parent')
    inputs = first.receive_message()
    assert np.array_equal(inputs['parent']['message'], FRAME)
    attempt_first = first._inflight_handles[0].num_delivered
    sever.arm()
    try:
        try:
            first.ack_inputs()
        except ConnectionError:
            pass                                             # the model's severed send
        held = obligations_of(rig.store, key_a)
        evidence['severed_ack'] = {'key': key_a, 'obligations_after_ack': sorted(held),
                                   'object_exists': object_exists(rig.store, key_a), 'first_attempt': attempt_first}
        assert held == {'child'} and object_exists(rig.store, key_a), \
            'an unconfirmed acknowledgment released the payload the redelivery needs'
        first.close()                                        # the worker dies with its handle
    finally:
        sever.restore()
    second = rig.messenger('child', ['parent'], ack_wait = ack_wait)
    inputs = second.receive_message()
    replacement_attempt = second._inflight_handles[0].num_delivered
    assert np.array_equal(inputs['parent']['message'], FRAME), 'the redelivery did not resolve the original bytes'
    assert replacement_attempt == attempt_first + 1, (attempt_first, replacement_attempt)
    second.ack_inputs()
    state = rig.consumer_state('child', 'parent')
    evidence['severed_ack'].update({'replacement_attempt': replacement_attempt, 'consumer_after': state.__dict__,
                                    'object_exists_after': object_exists(rig.store, key_a)})
    assert state.pending == 0 and state.unacked == 0, state
    assert not object_exists(rig.store, key_a), 'the confirmed settlement of the redelivery did not reclaim the object'
    evidence['faults'] = dict(schedule.fired(), **{f'model:{k}': v for k, v in sever.fired().items()})
    return schedule


@pytest.mark.case('PAY-004')
@pytest.mark.level('broker')
def test_pay_004_lost_broker_ack_cannot_reclaim_payload_needed_for(nats_url, redis_url, toxiproxy_url,
                                                                  nats_proxied_url, evidence_dir, record_faults,
                                                                  monkeypatch) -> None:
    '''
    PAY-004 (P0, integration, broker): Lost broker ACK cannot reclaim payload needed for
    redelivery.

    Acceptance: No redeliverable accepted envelope points to unavailable bytes; replacement
    completes successfully and reclamation occurs only after the certified release condition.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    toxiproxy = Toxiproxy(toxiproxy_url)
    evidence : Dict[str, Any] = {}
    rig = JetStreamRig(nats_url, BATCH, _specs()[:2], redis_url = redis_url, max_retries = 3, ack_wait = 3)
    try:
        schedule = _oracle_pay_004(rig, _ToxiproxySever(toxiproxy, nats_proxied_url), evidence, ack_wait = 3)
    finally:
        toxiproxy.reset()
        rig.close()
        write_evidence(evidence_dir, 'ownership_snapshots.json', evidence)
    record_faults(schedule)


@pytest.mark.case('PAY-004')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_pay_004_memory_backends_keep_the_obligation_until_the_settlement_is_certified(evidence_dir, record_faults,
                                                                                       monkeypatch) -> None:
    '''The model: a dropped confirmation, then a settlement that never reaches the broker and a lapsed lease.'''
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    rig = MemoryRig(BATCH)
    sever = _ModelSever(rig, ack_wait = 30)
    try:
        schedule = _oracle_pay_004(rig, sever, evidence, ack_wait = 30)
    finally:
        rig.close()
    write_evidence(evidence_dir, 'ownership_snapshots.json', evidence)
    record_faults(schedule)
    record_faults(sever._schedule)


@pytest.mark.negative_control(of = 'PAY-004')
def test_pay_004_detects_a_release_on_an_unconfirmed_settlement(monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    defects_pay.release_on_any_settlement(monkeypatch)
    rig = MemoryRig(BATCH)
    try:
        assert defects.detects(_oracle_pay_004, rig, _ModelSever(rig, 30), {}, 30)
    finally:
        rig.close()


# -- PAY-005 ----------------------------------------------------------------------

def _oracle_pay_005(store : PayloadStore, evidence : Dict[str, Any]) -> faults.FaultSchedule:
    '''X and Y own one object; X releases through every handle it will ever hold; Y reads and finishes.'''
    schedule = faults.FaultSchedule({'obligation.release.after': faults.Nth(3, faults.DropResponse())})
    receipts : List[Dict[str, Any]] = []

    def release(ref : ImmutablePayloadRef, reader : str, handle : str) -> Any:
        receipt = store.release_obligation(ref, reader, handle)
        receipts.append({'reader': reader, 'handle': handle, 'remaining': receipt.remaining,
                         'reclaimed': receipt.reclaimed, 'stale': receipt.stale, 'unknown': receipt.unknown,
                         'obligations': sorted(obligations_of(store, ref.key)) if object_exists(store, ref.key) else []})
        return receipt

    with schedule:
        ref = store.put(FRAME_BYTES, 'parent:t1:1', _contract(('X', 'Y')))
        key = ref.key
        assert obligations_of(store, key) == {'X', 'Y'}
        r1 = release(ref, 'X', 'ack:attempt-2')             # X completes via its newer delivery handle
        assert r1.remaining == 1 and not r1.reclaimed and not r1.unknown
        r2 = release(ref, 'X', 'ack:attempt-1')             # the older handle replays the release
        assert r2.remaining == 1 and not r2.reclaimed
        r3 = release(ref, 'X', 'ack:retry')                 # the release response is lost ...
        assert r3.unknown and r3.remaining is None and not r3.reclaimed
        r4 = release(ref, 'X', 'ack:retry-again')           # ... and retried
        assert r4.remaining == 1 and not r4.unknown
        restarted = store.ref_for_key(key)                  # X restarts: only the wire key survived
        assert restarted == ref
        r5 = release(restarted, 'X', 'ack:after-restart')
        assert r5.remaining == 1 and not r5.reclaimed
        assert obligations_of(store, key) == {'Y'}, 'X did not reduce ownership exactly once'
        out = store.read(store.ref_for_key(key))
        assert isinstance(out, PayloadBytes) and out.data == FRAME_BYTES, 'Y did not read the original bytes'
        started = time.monotonic()
        r6 = release(ref, 'Y', 'ack')
        reclaimed_after = time.monotonic() - started
        assert r6.reclaimed and r6.remaining == 0 and not object_exists(store, key)
        assert reclaimed_after < G_SECONDS
        # After the final cleanup: harmless, and nothing is recreated for a key nobody owns.
        r7 = release(ref, 'X', 'ack:very-late')
        assert not r7.reclaimed and not r7.stale and r7.remaining is None and not object_exists(store, key)
        # ... nor can it touch a different object holding the same content.
        fresh = store.put(FRAME_BYTES, 'parent:t1:1', _contract(('X', 'Y')))
        assert fresh.key != key or fresh.generation != ref.generation
        stale = ImmutablePayloadRef(fresh.store, fresh.key, fresh.size, fresh.digest, ref.generation, fresh.content_id)
        r8 = release(stale, 'X', 'ack:from-the-old-generation')
        assert r8.stale and obligations_of(store, fresh.key) == {'X', 'Y'}
    evidence.update({'key': key, 'receipts': receipts, 'reclaimed_after_s': reclaimed_after, 'faults': schedule.fired()})
    return schedule


@pytest.mark.case('PAY-005')
@pytest.mark.level('process')
def test_pay_005_release_is_idempotent_by_logical_reader_rather_than(evidence_dir, record_faults) -> None:
    '''
    PAY-005 (P0, payload, process): Release is idempotent by logical reader rather than delivery
    handle.

    Acceptance: Ownership count after any number of X releases remains one; Y reads original
    bytes; after Y completes the object is reclaimed within the declared GC bound G.
    '''
    evidence : Dict[str, Any] = {}
    schedule = _oracle_pay_005(MemoryPayloadStore(FakeClock()), evidence)
    write_evidence(evidence_dir, 'ownership_snapshots.json', evidence)
    record_faults(schedule)


@pytest.mark.case('PAY-005')
@pytest.mark.level('process')
@pytest.mark.variant('redis')
def test_pay_005_redis_store_releases_by_reader_id(evidence_dir, record_faults) -> None:
    '''The Redis store's ``WATCH``/``MULTI`` release on the in-process fake with a lost ``EXEC`` reply.'''
    pytest.importorskip('redis')
    evidence : Dict[str, Any] = {}
    schedule = _oracle_pay_005(_fake_redis_store(), evidence)
    write_evidence(evidence_dir, 'ownership_snapshots.json', evidence)
    record_faults(schedule)


@pytest.mark.negative_control(of = 'PAY-005')
def test_pay_005_detects_a_decrement_only_counter(monkeypatch) -> None:
    defects_pay.decrementing_release(monkeypatch)
    assert defects.detects(_oracle_pay_005, MemoryPayloadStore(FakeClock()), {})


# -- PAY-006 ----------------------------------------------------------------------

class SimulatedCrash(Exception):
    '''The in-process stand-in for a worker dying: raised from the barrier, never caught below the test.'''


_CHILD_SCRIPT = textwrap.dedent('''
    import os, sys
    from videoflow.backends import faults
    faults.FaultSchedule.from_env().install()                 # what the worker does at start (ENV-16)
    from videoflow.messaging.nats_messenger import NATSMessenger
    from videoflow.wire.redis_payload_store import RedisPayloadStore

    class Node:
        name = 'child'
        def open(self): pass
        def close(self): pass

    m = NATSMessenger(Node(), ['parent'], os.environ['PAY_NATS_URL'], os.environ['PAY_FLOW'], 'batch',
                      os.environ['PAY_RUN'], payload_store = RedisPayloadStore(os.environ['PAY_REDIS_URL']),
                      ack_wait = int(os.environ['PAY_ACK_WAIT']), max_retries = int(os.environ['PAY_MAX_RETRIES']))
    for _ in range(int(os.environ['PAY_EXPECTED'])):
        inputs = m.receive_message()
        if all(v.get('is_stop_signal') for v in inputs.values()):
            break
        m.ack_inputs()                                          # the crash fires inside the first confirmed ack
    m.close()
    sys.exit(0)
''')


def _subprocess_child(rig : JetStreamRig, redis_url : str, schedule : faults.FaultSchedule) -> Callable[[int], Dict[str, Any]]:
    '''A real worker process hosting the child's messenger; it dies by ``os._exit`` at the barrier.'''
    def run(expected : int) -> Dict[str, Any]:
        env = dict(os.environ)
        env.update(schedule.to_env())
        env.update({'VF_RFC0006': '1', 'PAY_NATS_URL': rig.nats_url, 'PAY_REDIS_URL': redis_url,
                    'PAY_FLOW': rig.flow_id, 'PAY_RUN': rig.run_id, 'PAY_ACK_WAIT': str(rig.ack_wait),
                    'PAY_MAX_RETRIES': str(rig.max_retries), 'PAY_EXPECTED': str(expected)})
        started = time.monotonic()
        proc = subprocess.run([sys.executable, '-c', _CHILD_SCRIPT], env = env, capture_output = True, text = True,
                              timeout = 90, cwd = os.getcwd())
        return {'exit_code': proc.returncode, 'took_s': time.monotonic() - started,
                'stderr_tail': proc.stderr[-2000:]}
    return run


def _inprocess_child(rig : MemoryRig, schedule : faults.FaultSchedule) -> Callable[[int], Dict[str, Any]]:
    '''The model: the child's settlement is confirmed and the process "dies" before it can release.'''
    def run(expected : int) -> Dict[str, Any]:
        child = rig.messenger('child', ['parent'])
        acked = 0
        with schedule:
            try:
                for _ in range(expected):
                    child.receive_message()
                    child.ack_inputs()
                    acked += 1
            except SimulatedCrash:
                return {'exit_code': 137, 'acked_before_crash': acked}
        return {'exit_code': 0, 'acked_before_crash': acked}
    return run


def _ledger_from_broker(rig : Any, readers : List[str], pins : Dict[str, List[str]],
                        known_keys : List[str]) -> Any:
    '''
    The durable evidence a restarted runtime reconciles against: a retained
    message still owes its object to every reader whose ack floor is below it;
    an object no retained message references owes nothing but its pins. (The
    Phase-3 runtime ledger derives exactly this; the test derives it by hand.)
    '''
    floors = {reader: rig.ack_floor(reader, 'parent') for reader in readers}
    required : Dict[str, List[str]] = {key: list(pins.get(key, [])) for key in known_keys}
    for seq, body in rig.retained_with_seq('parent'):
        key = peek_envelope(body).get('blob_ref')
        if key is None:
            continue
        required.setdefault(key, list(pins.get(key, [])))
        required[key] += [reader for reader in readers if seq > floors[reader]]
    return ledger({k: tuple(v) for k, v in required.items()}), {'floors': floors, 'required': required}


def _oracle_pay_006(rig : Any, run_child : Callable[[int], Dict[str, Any]], evidence : Dict[str, Any]) -> None:
    '''
    A: released by ``other`` already, so the child is its final reader — the
    child acks A (confirmed) and dies before releasing it. B: still owned by
    ``other`` and pinned by an archive throughout.
    '''
    parent = rig.messenger('parent', [], blob_reader_ids = ['child', 'other'])
    other = rig.messenger('other', ['parent'])
    parent.publish_message(FRAME)
    key_a = _last_key(rig, 'parent')
    parent.publish_message(frame_array((1000, 1000), seed = 6))
    key_b = _last_key(rig, 'parent')
    archive = f'archive/{rig.flow_id}'
    rig.store.acquire_obligation(rig.store.ref_for_key(key_b), archive, time.time() + 3600)
    assert np.array_equal(other.receive_message()['parent']['message'], FRAME)
    other.ack_inputs()                                       # other is done with A
    other.receive_message()                                  # other holds B, unsettled
    assert obligations_of(rig.store, key_a) == {'child'} and obligations_of(rig.store, key_b) == {'child', 'other', archive}

    outcome = run_child(expected = 2)
    evidence['child'] = outcome
    assert outcome['exit_code'] == 137, f'the child did not die at the barrier: {outcome}'
    # The broker recorded the child's settlement of A; the release never happened.
    assert rig.ack_floor('child', 'parent') >= 1, 'A was not settled before the crash'
    leaked = obligations_of(rig.store, key_a)
    evidence['after_crash'] = {'a_obligations': sorted(leaked), 'b_obligations': sorted(obligations_of(rig.store, key_b)),
                               'a_exists': object_exists(rig.store, key_a), 'b_exists': object_exists(rig.store, key_b)}
    assert leaked == {'child'} and object_exists(rig.store, key_a), 'the crash did not leave the leak this case is about'
    assert obligations_of(rig.store, key_b) == {'child', 'other', archive}

    # Recovery: a fresh process with no handle reconciles against durable evidence.
    started = time.monotonic()
    evidence_ledger, facts = _ledger_from_broker(rig, ['child', 'other'], {key_b: [archive]}, [key_a, key_b])
    result = rig.store.reconcile(evidence_ledger, 'pay-006:restart')
    took = time.monotonic() - started
    evidence['recovery'] = {'ledger': facts, 'reclaimed': list(result.reclaimed), 'retained': list(result.retained),
                            'unknown': list(result.unknown), 'took_s': took}
    assert key_a in result.reclaimed and not object_exists(rig.store, key_a), 'the final reader\'s leaked object survived'
    assert took < G_SECONDS
    assert key_b in result.retained and object_exists(rig.store, key_b)
    # B is still owed to the child (never delivered to it), to ``other`` and to the archive.
    assert obligations_of(rig.store, key_b) == {'child', 'other', archive}, \
        'reconciliation touched a reader that has not finished, or the archive pin'
    out = rig.store.read(rig.store.ref_for_key(key_b))
    assert isinstance(out, PayloadBytes), 'B, still owned by another reader, became unreadable'
    # The restarted child finishes B; other and the archive let go; zero exactly once.
    restarted = rig.messenger('child', ['parent'])
    inputs = restarted.receive_message()
    assert isinstance(inputs['parent']['message'], np.ndarray) and inputs['parent']['message'].shape == FRAME.shape
    restarted.ack_inputs()
    assert obligations_of(rig.store, key_b) == {'other', archive}, 'the restarted child did not release its share'
    other.ack_inputs()
    assert obligations_of(rig.store, key_b) == {archive}
    receipt = rig.store.release_obligation(rig.store.ref_for_key(key_b), archive, 'archive expired')
    assert receipt.reclaimed and not object_exists(rig.store, key_b)


@pytest.mark.case('PAY-006')
@pytest.mark.level('process')
def test_pay_006_crash_after_confirmed_settlement_does_not_leak_a_payload(nats_url, redis_url, evidence_dir,
                                                                          record_faults, monkeypatch) -> None:
    '''
    PAY-006 (P1, payload, process): Crash after confirmed settlement does not leak a payload
    forever.

    Acceptance: Final-reader objects are reclaimed within G after recovery; objects still owned
    by another reader remain readable throughout.

    A real worker process hosts the child's messenger against the compose broker
    and Redis; it installs the fault schedule from its environment (``ENV-16``)
    and ``os._exit``s at ``settle.after`` — after the broker confirmed the ack,
    before the release. The recovering side reconciles with a ledger built from
    the broker's ack floors and retained messages (what the Phase-3 runtime
    ledger will derive on its own; see the ``ledger`` variant).
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    schedule = faults.FaultSchedule({'settle.after': faults.Nth(1, faults.Crash(137))},
                                    marker_dir = str(evidence_dir / 'markers'))
    rig = JetStreamRig(nats_url, BATCH, _specs(), redis_url = redis_url, max_retries = 3, ack_wait = 30)
    try:
        _oracle_pay_006(rig, _subprocess_child(rig, redis_url, schedule), evidence)
    finally:
        rig.close()
        evidence['faults'] = schedule.fired()
        write_evidence(evidence_dir, 'recovery_ledger.json', evidence)
    record_faults(schedule)


@pytest.mark.case('PAY-006')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_pay_006_memory_backends_reconcile_a_settled_readers_leak(evidence_dir, record_faults, monkeypatch) -> None:
    '''The model: the settlement is confirmed, the process raises at the barrier and never releases.'''
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    schedule = faults.FaultSchedule({'settle.after': faults.Nth(1, faults.RaiseError(
        lambda: SimulatedCrash('the worker died after its ack was confirmed')))})
    rig = MemoryRig(BATCH, specs = _specs())
    try:
        _oracle_pay_006(rig, _inprocess_child(rig, schedule), evidence)
    finally:
        rig.close()
    evidence['faults'] = schedule.fired()
    write_evidence(evidence_dir, 'recovery_ledger.json', evidence)
    record_faults(schedule)


@pytest.mark.case('PAY-006')
@pytest.mark.level('process')
@pytest.mark.variant('ledger')
@pytest.mark.pending('phase 3')
def test_pay_006_the_runtime_ledger_reconciles_on_worker_start() -> None:
    '''
    The Phase-3 sub-assertion: a restarted worker's ``FlowRuntime`` builds the
    ``ObligationLedger`` from the broker's ack floors and its own durable records
    and runs ``reconcile`` at start, with no test-built ledger. Pending the
    runtime ledger (RFC 0006 §9 / plan §C step 3).
    '''


@pytest.mark.negative_control(of = 'PAY-006')
def test_pay_006_detects_a_store_without_reconciliation(monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    defects_pay.leaking_reconciler(monkeypatch)
    schedule = faults.FaultSchedule({'settle.after': faults.Nth(1, faults.RaiseError(
        lambda: SimulatedCrash('the worker died after its ack was confirmed')))})
    rig = MemoryRig(BATCH, specs = _specs())
    try:
        assert defects.detects(_oracle_pay_006, rig, _inprocess_child(rig, schedule), {})
    finally:
        rig.close()


# -- PAY-008 ----------------------------------------------------------------------

def _vanish_set(store : PayloadStore, key : str) -> None:
    '''The ownership record disappears while the bytes remain (expiry or eviction of the set).'''
    if isinstance(store, MemoryPayloadStore):
        store.expire_obligation_set(key)
        return
    from videoflow.wire.redis_payload_store import obligation_key
    client = store.client   # type: ignore[attr-defined]
    if hasattr(client, 'expire_now'):
        client.expire_now(obligation_key(key))
    else:
        client.unlink(obligation_key(key))


def _released_in_thread(store : PayloadStore, ref : ImmutablePayloadRef, reader : str, results : Dict[str, Any]) -> threading.Thread:
    def go() -> None:
        try:
            results[reader] = store.release_obligation(ref, reader, f'ack:{reader}')
        except BaseException as e:  # noqa: BLE001 — surfaced by the test
            results[reader] = e
    thread = threading.Thread(target = go, name = f'pay-008-{reader}', daemon = True)
    thread.start()
    return thread


def _oracle_pay_008(store : PayloadStore, evidence : Dict[str, Any],
                    between_read_and_write : Optional[Callable[[PayloadStore, str, Callable[[], None]], None]] = None
                    ) -> List[faults.FaultSchedule]:
    caps = store.capabilities()
    assert isinstance(caps.atomic_multikey, Known) and caps.atomic_multikey.value, \
        f'atomic multi-key release is not certified for this topology: {caps.atomic_multikey}'
    schedules : List[faults.FaultSchedule] = []
    interleavings : Dict[str, Any] = {}

    # I1: X pauses between reading the record and writing it; the record vanishes; X resumes.
    ref = store.put(FRAME_BYTES, 'parent:t1:1', _contract(('X', 'Y')))
    paused = faults.FaultSchedule({'obligation.release.before': faults.Nth(1, faults.Pause('x'))})
    results : Dict[str, Any] = {}
    with paused:
        thread = _released_in_thread(store, ref, 'X', results)
        assert wait_until(lambda: paused.fired().get('obligation.release.before', 0) >= 1, 10)
        _vanish_set(store, ref.key)
        paused.release('x')
        thread.join(15)
    schedules.append(paused)
    rx = results['X']
    assert not isinstance(rx, BaseException), rx
    interleavings['record_vanished'] = {'receipt': rx.__dict__ | {'ref': ref.key}, 'object_exists': object_exists(store, ref.key),
                                        'obligations': sorted(obligations_of(store, ref.key))}
    assert not rx.reclaimed and rx.remaining is None and not rx.unknown, rx
    assert object_exists(store, ref.key), 'X deleted the object Y still needs after its record vanished'
    out = store.read(store.ref_for_key(ref.key))
    assert isinstance(out, PayloadBytes) and out.data == FRAME_BYTES
    assert obligations_of(store, ref.key) == set(), 'a release re-created an ownership record'

    # I1b (stores with a read-then-write transaction): the record vanishes between the read and the write.
    if between_read_and_write is not None:
        ref_b = store.put(FRAME_BYTES, 'parent:t1:2', _contract(('X', 'Y')))
        between_read_and_write(store, ref_b.key, lambda: _vanish_set(store, ref_b.key))
        rb = store.release_obligation(ref_b, 'X', 'ack:X')
        interleavings['record_vanished_mid_transaction'] = {'receipt': rb.__dict__ | {'ref': ref_b.key},
                                                            'object_exists': object_exists(store, ref_b.key)}
        assert not rb.reclaimed and object_exists(store, ref_b.key) and obligations_of(store, ref_b.key) == set(), rb

    # I2: the two final readers release concurrently; one response is lost and retried.
    ref2 = store.put(FRAME_BYTES, 'parent:t1:3', _contract(('X', 'Y')))
    dropped = faults.FaultSchedule({'obligation.release.after': faults.Nth(1, faults.DropResponse())})
    results = {}
    with dropped:
        threads = [_released_in_thread(store, ref2, reader, results) for reader in ('X', 'Y')]
        for thread in threads:
            thread.join(15)
        retried = {}
        for reader, receipt in list(results.items()):
            assert not isinstance(receipt, BaseException), receipt
            if receipt.unknown:
                retried[reader] = store.release_obligation(ref2, reader, f'ack:{reader}:retry')
    schedules.append(dropped)
    receipts = [r for r in results.values()] + list(retried.values())
    reclaimed = [r for r in receipts if r.reclaimed]
    interleavings['concurrent_final_releases'] = {'receipts': [r.__dict__ | {'ref': ref2.key} for r in receipts],
                                                  'retried': sorted(retried), 'object_exists': object_exists(store, ref2.key)}
    assert len(reclaimed) == 1, f'reclaimed {len(reclaimed)} times: {receipts}'
    assert all(r.remaining is None or r.remaining >= 0 for r in receipts), receipts
    assert not object_exists(store, ref2.key)

    # I3: a stale generation fences a late release.
    ref3 = store.put(FRAME_BYTES, 'parent:t1:4', _contract(('X', 'Y')))
    stale = ImmutablePayloadRef(ref3.store, ref3.key, ref3.size, ref3.digest, 'deadbeef', ref3.content_id)
    r3 = store.release_obligation(stale, 'X', 'ack:fenced')
    assert r3.stale and not r3.reclaimed and obligations_of(store, ref3.key) == {'X', 'Y'}
    interleavings['stale_generation'] = r3.__dict__ | {'ref': ref3.key}

    # I5: the final cleanup is idempotent and creates nothing.
    assert store.release_obligation(ref3, 'X', 'ack').remaining == 1
    last = store.release_obligation(ref3, 'Y', 'ack')
    assert last.reclaimed and not object_exists(store, ref3.key)
    again = store.release_obligation(ref3, 'Y', 'ack:again')
    assert not again.reclaimed and not again.stale and again.remaining is None
    assert not object_exists(store, ref3.key) and obligations_of(store, ref3.key) == set()
    interleavings['idempotent_cleanup'] = again.__dict__ | {'ref': ref3.key}
    evidence.update({'atomic_multikey': caps.atomic_multikey.value, 'interleavings': interleavings,
                     'faults': {k: v for s in schedules for k, v in s.fired().items()}})
    return schedules


@pytest.mark.case('PAY-008')
@pytest.mark.level('process')
def test_pay_008_release_remains_atomic_when_ownership_metadata_expires_or(evidence_dir, record_faults) -> None:
    '''
    PAY-008 (P0, payload, process): Release remains atomic when ownership metadata expires or is
    evicted.

    Acceptance: All enumerated interleavings preserve live references; no negative ownership
    record or cross-generation delete occurs. Final cleanup is idempotent.
    '''
    evidence : Dict[str, Any] = {}
    schedules = _oracle_pay_008(MemoryPayloadStore(FakeClock()), evidence)
    write_evidence(evidence_dir, 'interleavings.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


def _between_watch_and_exec(store : PayloadStore, key : str, action : Callable[[], None]) -> None:
    '''On the fake server: run ``action`` after the transaction's reads and before its ``EXEC``.'''
    store.client.hook('EXEC', lambda server: action())   # type: ignore[attr-defined]


@pytest.mark.case('PAY-008')
@pytest.mark.level('process')
@pytest.mark.variant('redis')
def test_pay_008_redis_store_release_is_one_transaction(evidence_dir, record_faults) -> None:
    '''The Redis store on the in-process fake: the set vanishing between ``WATCH`` and ``EXEC`` aborts the release.'''
    pytest.importorskip('redis')
    evidence : Dict[str, Any] = {}
    schedules = _oracle_pay_008(_fake_redis_store(), evidence, _between_watch_and_exec)
    write_evidence(evidence_dir, 'interleavings.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.case('PAY-008')
@pytest.mark.level('broker')
@pytest.mark.variant('cluster')
def test_pay_008_redis_cluster_keeps_the_three_keys_in_one_slot(redis_cluster_url, evidence_dir, record_faults) -> None:
    '''The store against a Redis Cluster: ``CLUSTER KEYSLOT`` must agree for the three keys, then the same interleavings.'''
    pytest.importorskip('redis')
    from _brokers import redis_client

    from videoflow.wire.redis_payload_store import RedisPayloadStore
    evidence : Dict[str, Any] = {}
    from _brokers import sweep_refs
    with redis_client(redis_cluster_url) as client:
        store = RedisPayloadStore(redis_cluster_url)
        try:
            schedules = _oracle_pay_008(store, evidence)
        finally:
            sweep_refs(client, [entry.get('ref') for entry in _receipts(evidence)])
    write_evidence(evidence_dir, 'interleavings.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


def _receipts(evidence : Dict[str, Any]) -> List[Dict[str, Any]]:
    '''Every receipt an interleaving recorded — each names its object's key.'''
    out : List[Dict[str, Any]] = []
    for entry in evidence.get('interleavings', {}).values():
        if 'receipts' in entry:
            out.extend(entry['receipts'])
        elif 'receipt' in entry:
            out.append(entry['receipt'])
        elif 'ref' in entry:
            out.append(entry)
    return [e for e in out if e.get('ref')]


@pytest.mark.negative_control(of = 'PAY-008')
def test_pay_008_detects_an_exists_then_decrement_release(monkeypatch) -> None:
    defects_pay.exists_then_decrement(monkeypatch)
    assert defects.detects(_oracle_pay_008, MemoryPayloadStore(FakeClock()), {})


# -- PAY-021 ----------------------------------------------------------------------

def _graph(det_replicas : int, track_replicas : int) -> list:
    return [spec('cam', [], 'producer', True),
            spec('det', ['cam'], 'processor', True, nb_tasks = det_replicas),
            spec('track', ['cam'], 'consumer', False, nb_tasks = track_replicas, partition_by = 'trace_id')]


def _oracle_pay_021(store : PayloadStore, membership : Callable[[Any, Any], List[str]],
                    evidence : Dict[str, Any]) -> None:
    '''
    ``det`` competes on one durable (3 replicas), ``track`` is partitioned (3
    partitions). A frame is outstanding while a det worker is replaced, det is
    scaled, and one track partition is retired by policy.
    '''
    original = _graph(det_replicas = 3, track_replicas = 3)
    readers = membership(original[0], original)
    snapshots : List[Dict[str, Any]] = []

    def snap(step : str, key : str) -> None:
        snapshots.append({'step': step, 'obligations': sorted(obligations_of(store, key)) if object_exists(store, key) else None})

    assert readers == ['det', 'track/p0', 'track/p1', 'track/p2'], readers
    ref = store.put(FRAME_BYTES, 'cam:t1:1', _contract(tuple(readers)))
    key = ref.key
    snap('created', key)
    # A det worker is replaced: the replacement releases under the same logical reader id.
    r = store.release_obligation(ref, 'det', 'ack:replacement-worker')
    assert r.remaining == len(readers) - 1, r
    snap('replacement released det', key)
    # The original worker returns late: nothing to release twice, nothing else touched.
    late = store.release_obligation(ref, 'det', 'ack:original-worker-late')
    assert late.remaining == len(readers) - 1 and not late.reclaimed, late
    fenced = store.release_obligation(ImmutablePayloadRef(ref.store, ref.key, ref.size, ref.digest, 'fenced00',
                                                          ref.content_id), 'track/p0', 'ack:fenced-worker')
    assert fenced.stale and obligations_of(store, key) == {'track/p0', 'track/p1', 'track/p2'}, fenced
    snap('late and fenced releases', key)
    # A permitted stateless scale event on the competing node changes no share.
    for det_replicas in (5, 1):
        scaled = membership(original[0], _graph(det_replicas = det_replicas, track_replicas = 3))
        assert scaled == readers, f'scaling det to {det_replicas} changed the reader membership: {scaled}'
    # Retire one track partition by the explicit policy: the new membership is the ledger.
    retired = membership(original[0], _graph(det_replicas = 1, track_replicas = 2))
    assert retired == ['det', 'track/p0', 'track/p1'], retired
    started = time.monotonic()
    result = store.reconcile(ledger({key: tuple(retired)}), 'pay-021:retire-track-p2')
    assert key in result.retained and obligations_of(store, key) == {'track/p0', 'track/p1'}, result
    snap('track/p2 retired', key)
    for reader in ('track/p0', 'track/p1'):
        out = store.read(store.ref_for_key(key))
        assert isinstance(out, PayloadBytes) and out.data == FRAME_BYTES, f'{reader} could not resolve the frame'
    first = store.release_obligation(ref, 'track/p0', 'ack')
    assert first.remaining == 1 and not first.reclaimed
    final = store.release_obligation(ref, 'track/p1', 'ack')
    assert final.reclaimed and final.remaining == 0 and not object_exists(store, key), final
    assert time.monotonic() - started < G_SECONDS
    snap('every required reader completed', key)
    again = store.release_obligation(ref, 'track/p1', 'ack:again')
    assert not again.reclaimed and again.remaining is None
    evidence.update({'membership': readers, 'after_retirement': retired, 'snapshots': snapshots})


@pytest.mark.case('PAY-021')
@pytest.mark.level('process')
def test_pay_021_reader_replacement_and_membership_changes_preserve_logical(evidence_dir) -> None:
    '''
    PAY-021 (P0, integration, process): Reader replacement and membership changes preserve
    logical ownership.

    Acceptance: Every still-required reader can resolve the frame throughout transition; final-
    reader count reaches zero exactly once after allowed retirement/completion and GC finishes
    within G.
    '''
    evidence : Dict[str, Any] = {}
    _oracle_pay_021(MemoryPayloadStore(FakeClock()), blob_reader_ids, evidence)
    write_evidence(evidence_dir, 'membership_snapshots.json', evidence)


@pytest.mark.case('PAY-021')
@pytest.mark.level('process')
@pytest.mark.variant('redis')
def test_pay_021_redis_store_keeps_ownership_by_logical_reader(evidence_dir) -> None:
    pytest.importorskip('redis')
    evidence : Dict[str, Any] = {}
    _oracle_pay_021(_fake_redis_store(), blob_reader_ids, evidence)
    write_evidence(evidence_dir, 'membership_snapshots.json', evidence)


@pytest.mark.negative_control(of = 'PAY-021')
def test_pay_021_detects_shares_counted_from_the_initial_replica_count() -> None:
    assert defects.detects(_oracle_pay_021, MemoryPayloadStore(FakeClock()), defects_pay.count_based_membership, {})
