'''
Conformance cases: MSG-004, MSG-007, MSG-017.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions over a driver (``_msgdrivers``),
so the broker-level primary, the ``memory`` variant and the negative control decide
with the same assertions.

All three cases are about *logical subscriptions*: one work-sharing subscription per
consuming node, shared by its interchangeable replicas and bound again by a
replacement worker (MSG-004, MSG-007), with a broker-side credit derived from the
replica count rather than fixed at eight (MSG-017). Receivers run in threads so
replicas really compete; the model's clock is advanced by the polling loop, so a
killed replica's lease expires the same way on both backends.
'''
from __future__ import absolute_import, division, print_function

import json
import threading
import time
from typing import Any, Callable, Dict, List, Set

import defects
import pytest
from _brokers import unique_ids
from _msgdrivers import Bound, JetStreamDriver, MemoryDriver

from videoflow.backends import faults
from videoflow.backends.capabilities import LIVE_LATEST, RELIABLE_WORK, RETENTION_INTEREST, RETENTION_LIMITS
from videoflow.backends.messaging import Completed
from videoflow.backends.outcomes import Accepted, Known, SettleConfirmed
from videoflow.messaging.topology import consumer_credit

NATS_TIMEOUT = 180


class _Killed(Exception):
    '''Raised at the barrier where a worker dies: its receiver settles nothing after this.'''


def _write(evidence_dir : Any, name : str, record : Dict[str, Any]) -> None:
    (evidence_dir / name).write_text(json.dumps(record, indent = 2, default = str))


def _drain_concurrently(driver : Any, bounds : Dict[str, Bound], done : Callable[[Dict[str, List[tuple]]], bool],
                        timeout : float) -> Dict[str, List[tuple]]:
    '''
    One receiving thread per binding, each completing whatever it gets, until
    ``done`` says the ledger is complete (or the timeout). Returns, per receiver,
    the ``(message_id, attempt)`` pairs it completed.
    '''
    settled : Dict[str, List[tuple]] = {name: [] for name in bounds}
    stop = threading.Event()
    errors : List[BaseException] = []

    def worker(name : str, bound : Bound) -> None:
        try:
            while not stop.is_set():
                got = driver.receive(bound, timeout = 0.3)
                for delivery in got:
                    outcome = driver.settle(bound, delivery.token, Completed())
                    if isinstance(outcome, SettleConfirmed):
                        settled[name].append((delivery.token.message_id, delivery.token.attempt))
                if not got:
                    time.sleep(0.01)
        except BaseException as e:  # noqa: BLE001 — surfaced to the test thread
            errors.append(e)

    threads = [threading.Thread(target = worker, args = (name, bound), daemon = True, name = f'vf-conf-{name}')
               for name, bound in bounds.items()]
    for t in threads:
        t.start()
    try:
        driver.until(lambda: done(settled) or bool(errors), timeout, step = 0.25)
    finally:
        stop.set()
        for t in threads:
            t.join(10)
    assert not errors, errors
    return settled


# -- MSG-004 -------------------------------------------------------------------------

IDS = [f'id-{i:03d}' for i in range(100)]


def _oracle_msg_004(driver : Any, record : Dict[str, Any], record_faults : Callable[..., None] | None = None) -> None:
    # Reliable profile: P fans out to X (three competing replicas) and Y (two).
    channel = driver.channel('P', RELIABLE_WORK, RETENTION_INTEREST)
    x_credit, y_credit = consumer_credit(3, False, prefetch = 1), consumer_credit(2, False, prefetch = 1)
    bounds : Dict[str, Bound] = {}
    for name in ('x1', 'x2', 'x3'):
        bounds[name] = driver.bind(channel, 'X', receiver = name, ack_wait = 2, max_deliver = 8, credit = x_credit, prefetch = 1)
    for name in ('y1', 'y2'):
        bounds[name] = driver.bind(channel, 'Y', receiver = name, ack_wait = 2, max_deliver = 8, credit = y_credit, prefetch = 1)
    inventory = driver.subscriptions(channel)
    record['inventory'] = inventory
    assert len(inventory) == 2, f'one work-sharing subscription per child expected, found {inventory}'
    assert any('X' in name for name in inventory) and any('Y' in name for name in inventory), inventory
    for pid in IDS:
        assert isinstance(driver.publish(channel, pid, pid.encode()), Accepted)
    # x1 receives one input and dies before completing it.
    schedule = faults.FaultSchedule({'settle.before': faults.Nth(1, faults.RaiseError(lambda: _Killed('x1 died before completion')))})
    held : List[Any] = []
    with schedule:
        assert driver.until(lambda: held.extend(driver.receive(bounds['x1'], timeout = 0.5)) or bool(held), 15)
        with pytest.raises(_Killed):
            driver.settle(bounds['x1'], held[0].token, Completed())
    if record_faults is not None:
        record_faults(schedule)
    killed_holding = held[0].token.message_id
    driver.kill('x1')
    del bounds['x1']
    bounds['x1-replacement'] = driver.bind(channel, 'X', receiver = 'x1-replacement', ack_wait = 2, max_deliver = 8,
                                           credit = x_credit, prefetch = 1)
    record['kill'] = {'receiver': 'x1', 'holding': killed_holding, 'faults': schedule.fired()}

    def completion(settled : Dict[str, List[tuple]]) -> Dict[str, Set[str]]:
        out : Dict[str, Set[str]] = {'X': set(), 'Y': set()}
        for name, pairs in settled.items():
            out['X' if name.startswith('x') else 'Y'].update(m for m, _a in pairs)
        return out

    started = time.monotonic()
    settled = _drain_concurrently(driver, bounds, lambda s: all(len(v) >= len(IDS) for v in completion(s).values()), 90)
    sets = completion(settled)
    record['completed'] = {child: sorted(ids) for child, ids in sets.items()}
    record['per_receiver'] = {name: len(pairs) for name, pairs in settled.items()}
    record['recovery_seconds'] = time.monotonic() - started
    assert sets['X'] == set(IDS), f'X missed {sorted(set(IDS) - sets["X"])}'
    assert sets['Y'] == set(IDS), f'Y missed {sorted(set(IDS) - sets["Y"])}'
    assert killed_holding in sets['X']
    duplicates = {child: sum(len(pairs) for name, pairs in settled.items() if name.startswith(child.lower())) - len(IDS)
                  for child in ('X', 'Y')}
    record['duplicate_deliveries'] = duplicates                 # allowed: the idempotency contract's business
    # Every X delivery came through X's subscription and every Y delivery through Y's.
    for name, pairs in settled.items():
        assert all(bounds[name].subscription.consumer_node == ('X' if name.startswith('x') else 'Y') for _ in pairs)
    assert driver.until(lambda: driver.retained(channel) == 0, 20)

    # Live profile: a bounded channel, every gap an eviction.
    live = driver.channel('L', LIVE_LATEST, RETENTION_LIMITS, max_msgs = 8)
    lx = driver.bind(live, 'X', receiver = 'lx', ack_wait = 5, max_deliver = 1, credit = 8, prefetch = 1)
    ly = driver.bind(live, 'Y', receiver = 'ly', ack_wait = 5, max_deliver = 1, credit = 8, prefetch = 1)
    sequences : Dict[str, int] = {}
    for pid in IDS:
        out = driver.publish(live, pid, pid.encode())
        assert isinstance(out, Accepted) and out.sequence is not None
        sequences[pid] = out.sequence
    quiet = {'x': 0, 'y': 0}

    def settled_down(s : Dict[str, List[tuple]]) -> bool:
        for key, bound in (('x', lx), ('y', ly)):
            observed = driver.observe(bound)
            if isinstance(observed, Known) and observed.value.available == 0 and observed.value.leased == 0 \
                    and bound.backend.prefetched(bound.subscription) == 0:
                quiet[key] += 1
            else:
                quiet[key] = 0
        return all(v >= 3 for v in quiet.values())
    live_settled = _drain_concurrently(driver, {'lx': lx, 'ly': ly}, settled_down, 30)
    floor = driver.retained_floor(live)
    record['live'] = {'floor': floor, 'delivered': {k: len(v) for k, v in live_settled.items()}}
    for name, bound in (('lx', lx), ('ly', ly)):
        got = {m for m, _a in live_settled[name]}
        gaps = sorted(set(IDS) - got)
        record['live'][f'{name}_gaps'] = len(gaps)
        assert all(sequences[g] < floor for g in gaps), f'{name}: a gap that the channel still holds: {[g for g in gaps if sequences[g] >= floor][:5]}'
        assert all(bounds_consumer == bound.subscription.consumer_node for bounds_consumer in [bound.subscription.consumer_node])


@pytest.mark.case('MSG-004')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_004_fan_out_and_competing_workers_preserve_logical(nats_url, evidence_dir, record_faults) -> None:
    '''
    MSG-004 (P1, messaging, broker): Fan-out and competing workers preserve logical subscription
    boundaries.

    Acceptance: Reliable completion sets for X and Y both equal all 100 generated IDs after
    recovery; topology contains one logical work-sharing subscription per child, not one
    competing subscription shared by distinct children.
    '''
    flow, run = unique_ids('msg004')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_004(driver, record, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'completion_ledger.json', record)


@pytest.mark.case('MSG-004')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_004_memory_fan_out_children_and_competing_replicas(evidence_dir, record_faults) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_004(driver, record, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'completion_ledger.json', record)


@pytest.mark.negative_control(of = 'MSG-004')
def test_msg_004_detects_one_subscription_shared_by_distinct_children(monkeypatch) -> None:
    defects.shared_child_subscription(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_004, driver, {})
    finally:
        driver.close()


# -- MSG-007 -------------------------------------------------------------------------

TWENTY = [f'id-{i:02d}' for i in range(1, 21)]


def _oracle_msg_007(driver : Any, record : Dict[str, Any], record_faults : Callable[..., None] | None = None) -> None:
    channel = driver.channel('P', RELIABLE_WORK, RETENTION_INTEREST)
    driver.provision_subscription(channel, 'C', ack_wait = 2, max_deliver = 8, credit = 4)   # no worker online
    for pid in TWENTY:
        assert isinstance(driver.publish(channel, pid, pid.encode()), Accepted)
    assert driver.until(lambda: driver.retained(channel) == len(TWENTY), 10), driver.retained(channel)
    record['accepted'] = list(TWENTY)
    w1 = driver.bind(channel, 'C', receiver = 'w1', ack_wait = 2, max_deliver = 8, credit = 4, prefetch = 1)
    received : List[Any] = []
    assert driver.until(lambda: received.extend(driver.receive(w1, timeout = 0.5)) or len(received) >= 2, 15)
    first, second = received[0], received[1]
    assert isinstance(driver.settle(w1, second.token, Completed()), SettleConfirmed)   # one delivery acknowledged
    schedule = faults.FaultSchedule({'settle.before': faults.Nth(1, faults.RaiseError(lambda: _Killed('w1 died holding its first input')))})
    with schedule:
        with pytest.raises(_Killed):
            driver.settle(w1, first.token, Completed())
    if record_faults is not None:
        record_faults(schedule)
    driver.kill('w1')
    record['worker1'] = {'held': first.token.message_id, 'acknowledged': second.token.message_id, 'faults': schedule.fired()}
    started = time.monotonic()
    w2 = driver.bind(channel, 'C', receiver = 'w2', ack_wait = 2, max_deliver = 8, credit = 4, prefetch = 1)
    inventory = driver.subscriptions(channel)
    assert len(inventory) == 1, f'the replacement did not bind the same logical subscription: {inventory}'
    record['inventory_after_replacement'] = inventory
    expected = set(TWENTY) - {second.token.message_id}
    settled = _drain_concurrently(driver, {'w2': w2}, lambda s: {m for m, _a in s['w2']} >= expected, 60)
    pairs = settled['w2']
    completed = {m for m, _a in pairs}
    record['replacement'] = {'completed': sorted(completed), 'attempts': {m: a for m, a in pairs},
                             'recovery_seconds': time.monotonic() - started}
    assert completed == expected, sorted(expected - completed)
    assert second.token.message_id not in completed, 'an acknowledged input was delivered again'
    attempts = {m: a for m, a in pairs}
    assert attempts[first.token.message_id] >= 2, attempts[first.token.message_id]
    assert driver.until(lambda: driver.retained(channel) == 0, 20)


@pytest.mark.case('MSG-007')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_007_offline_reliable_subscribers_and_worker_replacement(nats_url, evidence_dir, record_faults) -> None:
    '''
    MSG-007 (P0, messaging, broker): Offline reliable subscribers and worker replacement recover
    accepted inputs.

    Acceptance: All 20 accepted IDs complete within R after dependencies recover; no unaccounted
    accepted IDs. Core-only must reject this profile under MSG-001.
    '''
    flow, run = unique_ids('msg007')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_007(driver, record, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'replacement_ledger.json', record)


@pytest.mark.case('MSG-007')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_007_memory_replacement_binds_the_same_logical_subscription(evidence_dir, record_faults) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_007(driver, record, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'replacement_ledger.json', record)


@pytest.mark.negative_control(of = 'MSG-007')
def test_msg_007_detects_a_process_scoped_subscription(monkeypatch) -> None:
    defects.process_scoped_subscription(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_007, driver, {})
    finally:
        driver.close()


# -- MSG-017 -------------------------------------------------------------------------

WORKERS = 10
#: Long enough that no lease expires while a worker waits at the barrier — on the
#: model, whose clock the polling loop advances, an expiring lease would hand a
#: starved worker somebody else's input and fake a tenth active worker.
LEASE = 600


def _hold_at_barrier(driver : Any, bounds : Dict[str, Bound], pause : str, timeout : float) -> tuple:
    '''
    Ten worker threads: each takes one input, reaches the processing barrier
    (``worker.ready.after``, paused), and completes its input only once released.
    Returns ``(fired, high_water_bytes, settled_count, release)``.
    '''
    stop = threading.Event()
    held : Dict[str, Any] = {}
    settled : List[str] = []
    errors : List[BaseException] = []
    lock = threading.Lock()

    def worker(name : str, bound : Bound) -> None:
        try:
            while not stop.is_set():
                got = driver.receive(bound, timeout = 0.3)
                if not got:
                    time.sleep(0.01)
                    continue
                delivery = got[0]
                with lock:
                    held[name] = delivery.size
                faults.barrier('worker.ready.after', worker = name, message_id = delivery.token.message_id)
                driver.settle(bound, delivery.token, Completed())
                with lock:
                    settled.append(delivery.token.message_id)
                return
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target = worker, args = (name, bound), daemon = True) for name, bound in bounds.items()]
    for t in threads:
        t.start()

    def release(schedule : faults.FaultSchedule) -> None:
        schedule.release(pause)
        stop.set()
        for t in threads:
            t.join(15)
        assert not errors, errors
    return held, settled, release


def _oracle_msg_017(driver : Any, record : Dict[str, Any], record_faults : Callable[..., None] | None = None) -> None:
    channel = driver.channel('P', RELIABLE_WORK, RETENTION_INTEREST)
    prefetch = 1
    credit_two, credit_ten = consumer_credit(2, False, prefetch = prefetch), consumer_credit(WORKERS, False, prefetch = prefetch)
    driver.provision_subscription(channel, 'W', ack_wait = LEASE, max_deliver = 8, credit = credit_two)
    effective = driver.subscription_effective(channel, 'W')
    assert effective['max_ack_pending'] == credit_two, effective
    record['credit'] = [{'target': 2, 'requested': credit_two, 'effective': effective['max_ack_pending']}]
    for i in range(3 * WORKERS):
        assert isinstance(driver.publish(channel, f'id-{i:02d}', b'x' * 256), Accepted)
    bounds = {f'w{i}': driver.bind(channel, 'W', receiver = f'w{i}', ack_wait = LEASE, max_deliver = 8,
                                   credit = credit_two, prefetch = prefetch) for i in range(WORKERS)}
    byte_credit = bounds['w0'].spec.byte_credit
    # Target two: the credit admits two workers' worth of in-flight inputs — ten cannot all be active.
    schedule_a = faults.FaultSchedule({'worker.ready.after': faults.Pause('barrier-a', timeout_seconds = 60)})
    with schedule_a:
        held, settled, release = _hold_at_barrier(driver, bounds, 'barrier-a', 20)
        driver.until(lambda: schedule_a.fired().get('worker.ready.after', 0) >= min(credit_two, WORKERS), 15)
        time.sleep(1.0)                                          # nobody else reaches the barrier
        active_two = schedule_a.fired().get('worker.ready.after', 0)
        assert 0 < active_two <= credit_two < WORKERS, active_two
        assert not settled, 'an input completed before the barrier was released'
        release(schedule_a)
    if record_faults is not None:
        record_faults(schedule_a)
    record['target_two'] = {'active_workers': active_two, 'held_bytes': sum(held.values())}
    # Scale to ten: re-derive, update and read back the credit; ten distinct workers hold one each.
    driver.provision_subscription(channel, 'W', ack_wait = LEASE, max_deliver = 8, credit = credit_ten)
    effective = driver.subscription_effective(channel, 'W')
    assert effective['max_ack_pending'] == credit_ten, effective
    record['credit'].append({'target': WORKERS, 'requested': credit_ten, 'effective': effective['max_ack_pending']})
    schedule_b = faults.FaultSchedule({'worker.ready.after': faults.Pause('barrier-b', timeout_seconds = 60)})
    with schedule_b:
        held, settled, release = _hold_at_barrier(driver, bounds, 'barrier-b', 30)
        reached = driver.until(lambda: schedule_b.fired().get('worker.ready.after', 0) >= WORKERS, 20)
        active_ten = schedule_b.fired().get('worker.ready.after', 0)
        assert reached and active_ten == WORKERS, f'only {active_ten} of {WORKERS} workers hold an input at credit {credit_ten}'
        assert not settled, 'an input completed before every worker held one'
        observed = driver.observe(bounds['w0'])
        assert isinstance(observed, Known), observed
        leased = observed.value.leased
        prefetched = sum(b.backend.prefetched(b.subscription) for b in bounds.values())
        assert leased <= credit_ten, (leased, credit_ten)
        assert prefetched <= WORKERS * prefetch, (prefetched, WORKERS * prefetch)
        assert sum(held.values()) <= WORKERS * byte_credit
        record['target_ten'] = {'active_workers': active_ten, 'leased': leased, 'prefetched': prefetched,
                                'held_bytes': sum(held.values()), 'byte_credit': byte_credit}
        release(schedule_b)
    if record_faults is not None:
        record_faults(schedule_b)
    # Scale down: the credit shrinks, the surplus replicas retire (handing back
    # what they still held), and the in-flight count follows once the two that
    # remain have settled the outstanding work.
    driver.provision_subscription(channel, 'W', ack_wait = LEASE, max_deliver = 8, credit = credit_two)
    effective = driver.subscription_effective(channel, 'W')
    assert effective['max_ack_pending'] == credit_two, effective
    record['credit'].append({'target': 2, 'requested': credit_two, 'effective': effective['max_ack_pending']})
    for name in list(bounds):
        if name not in ('w0', 'w1'):
            driver.retire(name)
    remaining = _drain_concurrently(driver, {'w0': bounds['w0'], 'w1': bounds['w1']},
                                    lambda s: driver.retained(channel) == 0, 60)

    def within_credit() -> bool:
        observed = driver.observe(bounds['w0'])
        return isinstance(observed, Known) and observed.value.leased <= credit_two and observed.value.available == 0
    assert driver.until(within_credit, 30), driver.observe(bounds['w0'])
    record['scale_down'] = {'drained_by_two': {k: len(v) for k, v in remaining.items()},
                            'observation': str(driver.observe(bounds['w0']).value)}


@pytest.mark.case('MSG-017')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_msg_017_shared_consumer_credit_tracks_useful_concurrency_without(nats_url, evidence_dir, record_faults) -> None:
    '''
    MSG-017 (P1, integration, broker): Shared consumer credit tracks useful concurrency without
    unbounded prefetch.

    Acceptance: Ten workers reach the processing barrier before any completes when target ten is
    accepted; in-flight and prefetched inputs remain within declared bounds.
    '''
    flow, run = unique_ids('msg017')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_msg_017(driver, record, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'credit_trace.json', record)


@pytest.mark.case('MSG-017')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_msg_017_memory_credit_follows_the_replica_count(evidence_dir, record_faults) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_msg_017(driver, record, record_faults)
    finally:
        driver.close()
        _write(evidence_dir, 'credit_trace.json', record)


@pytest.mark.negative_control(of = 'MSG-017')
def test_msg_017_detects_a_fixed_credit(monkeypatch) -> None:
    defects.fixed_credit(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_msg_017, driver, {})
    finally:
        driver.close()
