'''
Conformance cases: RUN-024, RUN-025.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions over a driver (``_msgdrivers``),
so the broker-level test, the ``memory`` variant and the negative control decide with
the same assertions.

Both cases are about the two credits a receiving stage lives under. RUN-024 is the
*broker-side* one: ``max_ack_pending`` on the shared durable, which STREAM-15 derives
from the admitted concurrency (``topology.consumer_credit``) instead of the historical
eight — ten one-at-a-time workers need ten in-flight inputs, and a durable left at
eight starts eight. RUN-025 is the *receiver-side* one: the byte budget a worker's
adapter holds unsettled (``JetStreamMessagingBackend(byte_budget = ...)``,
``VF_PREFETCH_BYTES``), enforced at the receive rather than by the broker, so a slow
worker cannot park a queue of 1 MB frames while faster workers idle.
'''
from __future__ import absolute_import, division, print_function

import threading
import time
from typing import Any, Callable, Dict, List

import defects
import defects_run3
import pytest
from _brokers import unique_ids
from _msgdrivers import Bound, JetStreamDriver, MemoryDriver
from _runs3 import BudgetedJetStreamDriver, drain_concurrently, hold_at_barrier, write_evidence

from videoflow.backends import faults
from videoflow.backends.capabilities import RELIABLE_WORK, RETENTION_INTEREST
from videoflow.backends.messaging import Completed, Delivery
from videoflow.backends.outcomes import Accepted, Known
from videoflow.messaging import topology
from videoflow.messaging.jetstream_backend import JetStreamMessagingBackend

NATS_TIMEOUT = 180

# -- RUN-024 -------------------------------------------------------------------------

WORKERS = 10
TASKS = 20
#: One parked input per worker beyond the one in processing (the receiver-side prefetch).
PREFETCH = 1
#: Long enough that no lease expires while a worker waits at the barrier — on the
#: model, whose clock the polling loop advances, an expiring lease would hand a
#: starved worker somebody else's input and fake an extra active worker.
LEASE = 600


def _reached(schedule : faults.FaultSchedule) -> int:
    return schedule.fired().get('worker.ready.after', 0)


def _oracle_run_024(driver : Any, record : Dict[str, Any], record_faults : Callable[..., None] | None = None) -> None:
    '''
    Twenty pending tasks, ten one-at-a-time workers. First the historical
    eight-credit durable: fewer than ten workers can hold an input, and the
    ten-worker plan is detected as incompatible with the durable as it stands.
    Then the plan's credit is applied (re-derived, updated, read back): all ten
    reach the start barrier before any completes. Then a scale-down to four
    reconciles the credit again and the survivors finish the backlog.
    '''
    channel = driver.channel('P', RELIABLE_WORK, RETENTION_INTEREST)
    legacy = topology.DEFAULT_MAX_ACK_PENDING
    plan_ten = topology.consumer_credit(WORKERS, False, prefetch = PREFETCH)
    plan_four = topology.consumer_credit(4, False, prefetch = PREFETCH)
    # The regression control: the durable as pre-RFC provisioning left it.
    driver.provision_subscription(channel, 'W', ack_wait = LEASE, max_deliver = 8, credit = legacy)
    effective = driver.subscription_effective(channel, 'W')['max_ack_pending']
    assert effective == legacy, effective
    admits = topology.credit_admits(effective, False, prefetch = PREFETCH)
    assert admits < WORKERS, f'a credit of {effective} admits {admits} workers: the ten-worker plan is not incompatible?'
    record['control'] = {'credit': effective, 'admits': admits, 'plan_requires': plan_ten}
    for i in range(WORKERS):
        assert isinstance(driver.publish(channel, f'control-{i:02d}', b'x' * 256), Accepted)
    bounds = {f'w{i}': driver.bind(channel, 'W', receiver = f'w{i}', ack_wait = LEASE, max_deliver = 8,
                                   credit = legacy, prefetch = PREFETCH) for i in range(WORKERS)}
    byte_credit = bounds['w0'].spec.byte_credit
    schedule_a = faults.FaultSchedule({'worker.ready.after': faults.Pause('barrier-control', timeout_seconds = 60)})
    with schedule_a:
        held, settled, release = hold_at_barrier(driver, bounds)
        driver.until(lambda: _reached(schedule_a) >= admits, 15)
        time.sleep(1.0)                                          # nobody else reaches the barrier
        active_control = _reached(schedule_a)
        observed = driver.observe(bounds['w0'])
        assert isinstance(observed, Known), observed
        # Each active worker leases at most 1 + PREFETCH, so at least `admits` are active and never ten.
        assert admits <= active_control <= legacy < WORKERS, active_control
        assert observed.value.leased <= legacy, observed.value
        assert not settled, 'an input completed before the barrier was released'
        record['control'].update(active_workers = active_control, leased = observed.value.leased,
                                 available = observed.value.available)
        release(schedule_a, 'barrier-control')
    completed_control = len(settled)                             # a worker released late completes one too
    if record_faults is not None:
        record_faults(schedule_a)
    # The ten-worker plan against the eight-credit durable: a worker binding at
    # the plan's credit reads back what the broker holds, and the shortfall is
    # named — never a tenth replica that starts and holds nothing.
    probe = driver.bind(channel, 'W', receiver = 'probe', ack_wait = LEASE, max_deliver = 8, credit = plan_ten,
                        prefetch = PREFETCH)
    bound_credit = int(probe.effective.get('max_ack_pending', probe.effective.get('item_credit', 0)))
    driver.retire('probe')
    incompatible = topology.credit_admits(bound_credit, False, prefetch = PREFETCH) < WORKERS
    record['plan_against_control'] = {'requested': plan_ten, 'bound': bound_credit, 'incompatible': incompatible}
    if bound_credit == legacy:
        assert incompatible, 'a bind at the plan credit was accepted against the eight-credit durable'
    else:
        assert bound_credit == plan_ten, bound_credit      # a backend that resizes on bind honours the request
    # Apply the plan: re-derive, update, read back — then ten workers hold one each.
    driver.provision_subscription(channel, 'W', ack_wait = LEASE, max_deliver = 8, credit = plan_ten)
    effective = driver.subscription_effective(channel, 'W')['max_ack_pending']
    assert effective == plan_ten, effective
    assert topology.credit_admits(effective, False, prefetch = PREFETCH) >= WORKERS
    record['credit_changes'] = [{'target': WORKERS, 'requested': plan_ten, 'effective': effective}]
    for i in range(TASKS):                                       # the twenty pending tasks of the plan
        assert isinstance(driver.publish(channel, f'task-{i:02d}', b'x' * 256), Accepted)
    schedule_b = faults.FaultSchedule({'worker.ready.after': faults.Pause('barrier-plan', timeout_seconds = 60)})
    with schedule_b:
        held, settled, release = hold_at_barrier(driver, bounds)
        reached = driver.until(lambda: _reached(schedule_b) >= WORKERS, 20)
        active_plan = _reached(schedule_b)
        assert reached and active_plan == WORKERS, f'only {active_plan} of {WORKERS} workers hold an input at credit {effective}'
        assert not settled, 'an input completed before every worker held one'
        observed = driver.observe(bounds['w0'])
        assert isinstance(observed, Known), observed
        prefetched = sum(b.backend.prefetched(b.subscription) for b in bounds.values())
        assert observed.value.leased <= plan_ten, (observed.value.leased, plan_ten)
        assert prefetched <= WORKERS * PREFETCH, (prefetched, WORKERS * PREFETCH)
        assert sum(held.values()) <= WORKERS * byte_credit
        record['plan'] = {'active_workers': active_plan, 'leased': observed.value.leased,
                          'available': observed.value.available, 'prefetched': prefetched,
                          'held_bytes': sum(held.values()), 'byte_credit': byte_credit}
        release(schedule_b, 'barrier-plan')
    completed_plan = len(settled)
    if record_faults is not None:
        record_faults(schedule_b)
    # Scale down to four: the credit shrinks, the surplus retire handing back
    # what they parked, and the survivors finish within the new credit.
    driver.provision_subscription(channel, 'W', ack_wait = LEASE, max_deliver = 8, credit = plan_four)
    effective = driver.subscription_effective(channel, 'W')['max_ack_pending']
    assert effective == plan_four, effective
    record['credit_changes'].append({'target': 4, 'requested': plan_four, 'effective': effective})
    survivors = {name: bounds[name] for name in ('w0', 'w1', 'w2', 'w3')}
    for name in list(bounds):
        if name not in survivors:
            driver.retire(name)
    remaining = drain_concurrently(driver, survivors, lambda s: driver.retained(channel) == 0, 60)

    def within_credit() -> bool:
        observed = driver.observe(bounds['w0'])
        return isinstance(observed, Known) and observed.value.leased <= plan_four and observed.value.available == 0
    assert driver.until(within_credit, 30), driver.observe(bounds['w0'])
    completed = completed_control + completed_plan + sum(len(v) for v in remaining.values())
    assert completed == WORKERS + TASKS, f'{completed} of {WORKERS + TASKS} tasks completed'
    assert driver.retained(channel) == 0
    record['scale_down'] = {'drained_by_four': {k: len(v) for k, v in remaining.items()},
                            'held_bytes_bound': 4 * byte_credit, 'observation': str(driver.observe(bounds['w0']).value)}
    record['completed'] = completed


@pytest.mark.case('RUN-024')
@pytest.mark.level('broker')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_run_024_worker_concurrency_reconciles_stage_wide_work_credits(nats_url, evidence_dir, record_faults) -> None:
    '''
    RUN-024 (P1, integration, broker): Worker concurrency reconciles stage-wide work credits.

    Acceptance: With ten admitted tasks of concurrency and sufficient explicit budget, ten
    workers start; the historical eight-credit control starts only eight and is detected as
    incompatible.
    '''
    flow, run = unique_ids('run024')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_run_024(driver, record, record_faults)
    finally:
        driver.close()
        write_evidence(evidence_dir, 'credit_ledger.json', record)


@pytest.mark.case('RUN-024')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_run_024_memory_credit_admits_the_declared_concurrency(evidence_dir, record_faults) -> None:
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_run_024(driver, record, record_faults)
    finally:
        driver.close()
        write_evidence(evidence_dir, 'credit_ledger.json', record)


@pytest.mark.negative_control(of = 'RUN-024')
def test_run_024_detects_the_fixed_eight_credit(monkeypatch) -> None:
    '''A credit that stays at eight whatever the plan admits starts eight of ten: the oracle must catch it.'''
    defects_run3.fixed_credit(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_run_024, driver, {})
    finally:
        driver.close()


# -- RUN-025 -------------------------------------------------------------------------

SMALL = 256
FRAME = 512 * 1024
#: The per-worker resident-byte budget B: one frame fills it.
BUDGET = 512 * 1024
#: The documented atomic-admission tolerance: the one message in flight when the budget filled.
TOLERANCE = FRAME
#: Per-worker prefetch depth (messages), the count-only bound of the reviewed code.
DEPTH = 4
INPUTS = 12
FAST_WORKERS = ('f1', 'f2')


class _Worker(threading.Thread):
    '''
    One receiver with its own budget ``B``. On the adapter the budget lives in the
    pull loop (``byte_budget``), which keeps parking inputs while the worker is
    busy; on the model a prefetch thread does the same through the runtime's
    receive, admitting by the remaining budget (``byte_credit``) — the seam
    STREAM-15 names, with the same one-message tolerance. Either way the worker
    holds at most ``DEPTH`` parked inputs beyond the one it processes, records
    every input it completes, and reports the bytes it holds and how often
    admission had to wait.
    '''
    def __init__(self, driver : Any, bound : Bound, budget : int, pause : bool) -> None:
        super().__init__(daemon = True, name = f'vf-conf-{bound.receiver}')
        self.driver, self.bound, self.budget, self.pause = driver, bound, budget, pause
        self.completed : List[str] = []
        self.error : BaseException | None = None
        self.waits = 0
        self._parked : List[Delivery] = []
        self._processing : Delivery | None = None
        self._lock = threading.Lock()
        self._halt = threading.Event()
        self._adapter = isinstance(bound.backend, JetStreamMessagingBackend)
        self._prefetcher : threading.Thread | None = None

    # -- what the oracle measures ------------------------------------------------------
    def resident_bytes(self) -> int:
        if self._adapter:
            assert isinstance(self.bound.backend, JetStreamMessagingBackend)
            return self.bound.backend.resident_bytes()
        with self._lock:
            return sum(d.size for d in self._parked) + (self._processing.size if self._processing else 0)

    def resident_items(self) -> int:
        parked = 0
        if self._adapter:
            assert isinstance(self.bound.backend, JetStreamMessagingBackend)
            parked = self.bound.backend.prefetched(self.bound.subscription)
        with self._lock:
            return parked + len(self._parked) + (1 if self._processing else 0)

    def budget_waits(self) -> int:
        if self._adapter:
            assert isinstance(self.bound.backend, JetStreamMessagingBackend)
            return self.bound.backend.budget_waits()
        return self.waits

    # -- the model's prefetch: the runtime's byte admission ---------------------------------
    def _prefetch_loop(self) -> None:
        try:
            while not self._halt.is_set():
                with self._lock:
                    parked = len(self._parked)
                    resident = sum(d.size for d in self._parked) + (self._processing.size if self._processing else 0)
                if parked >= DEPTH:
                    time.sleep(0.01)
                    continue
                remaining = self.budget - resident
                if remaining <= 0:
                    self.waits += 1                             # an explicit pressure decision, not a stall
                    time.sleep(0.01)
                    continue
                got = self.driver.receive(self.bound, timeout = 0.2, item_credit = DEPTH - parked, byte_credit = remaining)
                with self._lock:
                    self._parked.extend(got)
                if not got:
                    time.sleep(0.01)
        except BaseException as e:  # noqa: BLE001 — surfaced to the test thread
            self.error = e

    # -- the loop ----------------------------------------------------------------------
    def _take(self) -> Delivery | None:
        with self._lock:
            if self._parked:
                return self._parked.pop(0)
        if not self._adapter:
            time.sleep(0.01)                                    # the prefetcher fills the parked list
            return None
        got = self.driver.receive(self.bound, timeout = 0.2)
        with self._lock:
            self._parked.extend(got[1:])
        return got[0] if got else None

    def run(self) -> None:
        if not self._adapter:
            self._prefetcher = threading.Thread(target = self._prefetch_loop, daemon = True,
                                                name = f'vf-conf-{self.bound.receiver}-prefetch')
            self._prefetcher.start()
        try:
            while not self._halt.is_set():
                delivery = self._take()
                if delivery is None:
                    continue
                with self._lock:
                    self._processing = delivery
                if self.pause:
                    faults.barrier('worker.ready.after', worker = self.bound.receiver,
                                   message_id = delivery.token.message_id)
                self.driver.settle(self.bound, delivery.token, Completed())
                with self._lock:
                    self._processing = None
                    self.completed.append(delivery.token.message_id)
        except BaseException as e:  # noqa: BLE001 — surfaced to the test thread
            self.error = e

    def stop(self) -> None:
        self._halt.set()

    def join(self, timeout : float | None = None) -> None:
        super().join(timeout)
        if self._prefetcher is not None:
            self._prefetcher.join(timeout)


def _sampler(workers : Dict[str, _Worker], samples : Dict[str, List[int]], stop : threading.Event) -> threading.Thread:
    def run() -> None:
        while not stop.is_set():
            for name, worker in workers.items():
                samples[name].append(worker.resident_bytes())
            time.sleep(0.02)
    thread = threading.Thread(target = run, daemon = True, name = 'vf-conf-sampler')
    thread.start()
    return thread


def _oracle_run_025(driver : Any, record : Dict[str, Any], record_faults : Callable[..., None] | None = None) -> None:
    '''
    Inputs alternate small records and frames; the slow worker starts first and
    is paused holding its first input while it prefetches under budget B; two
    fast workers start after it. Measured resident bytes never exceed B plus one
    message; the fast workers finish every input the slow one does not hold;
    every blocked admission is a counted pressure decision.
    '''
    channel = driver.channel('P', RELIABLE_WORK, RETENTION_INTEREST)
    credit = topology.consumer_credit(1 + len(FAST_WORKERS), False, prefetch = DEPTH)
    driver.provision_subscription(channel, 'W', ack_wait = LEASE, max_deliver = 8, credit = credit)
    sizes : Dict[str, int] = {}
    for i in range(INPUTS):
        size = FRAME if i % 2 else SMALL
        pid = f'in-{i:02d}'
        assert isinstance(driver.publish(channel, pid, bytes([i % 251]) * size), Accepted)
        sizes[pid] = size
    record['inputs'] = sizes
    record['budget'] = {'B': BUDGET, 'tolerance': TOLERANCE, 'depth': DEPTH, 'credit': credit}
    slow = _Worker(driver, driver.bind(channel, 'W', receiver = 'slow', ack_wait = LEASE, max_deliver = 8,
                                       credit = credit, prefetch = DEPTH), BUDGET, pause = True)
    workers : Dict[str, _Worker] = {'slow': slow}
    samples : Dict[str, List[int]] = {'slow': []}
    stop_sampling = threading.Event()
    sampler = _sampler(workers, samples, stop_sampling)
    schedule = faults.FaultSchedule({'worker.ready.after': faults.Pause('slow-holds', timeout_seconds = 90)})
    try:
        with schedule:
            slow.start()                                        # the slow worker captures credit first
            assert driver.until(lambda: schedule.fired().get('worker.ready.after', 0) >= 1, 20), 'the slow worker never took an input'
            # Admission becomes limiting: the slow worker's prefetch fills its budget and waits.
            assert driver.until(lambda: slow.budget_waits() > 0 or slow.error is not None, 20), \
                f'the slow worker never reached its byte budget (resident {slow.resident_bytes()})'
            assert slow.error is None, slow.error
            hoarded_bytes, hoarded_items = slow.resident_bytes(), slow.resident_items()
            assert BUDGET <= hoarded_bytes <= BUDGET + TOLERANCE, (hoarded_bytes, BUDGET, TOLERANCE)
            assert hoarded_items <= DEPTH + 1, hoarded_items
            record['slow_paused'] = {'resident_bytes': hoarded_bytes, 'resident_items': hoarded_items,
                                     'budget_waits': slow.budget_waits()}
            for name in FAST_WORKERS:
                worker = _Worker(driver, driver.bind(channel, 'W', receiver = name, ack_wait = LEASE, max_deliver = 8,
                                                     credit = credit, prefetch = DEPTH), BUDGET, pause = False)
                workers[name] = worker
                samples[name] = []
                worker.start()
            # Runnable capacity is not starved: everything the slow worker does not hold completes now.
            started = time.monotonic()
            assert driver.until(lambda: driver.retained(channel) <= slow.resident_items()
                                or any(w.error for w in workers.values()), 60), \
                f'{driver.retained(channel)} retained while the slow worker holds {slow.resident_items()}'
            for worker in workers.values():
                assert worker.error is None, worker.error
            fast_done = {name: list(workers[name].completed) for name in FAST_WORKERS}
            assert sum(len(v) for v in fast_done.values()) >= INPUTS - (DEPTH + 1), fast_done
            assert all(len(v) > 0 for v in fast_done.values()), fast_done
            record['while_slow_paused'] = {'fast_completed': fast_done, 'seconds': time.monotonic() - started,
                                           'slow_resident_items': slow.resident_items()}
            schedule.release('slow-holds')
        if record_faults is not None:
            record_faults(schedule)
        assert driver.until(lambda: driver.retained(channel) == 0 or any(w.error for w in workers.values()), 60), \
            driver.retained(channel)
    finally:
        for worker in workers.values():
            worker.stop()
        for worker in workers.values():
            worker.join(15)
        stop_sampling.set()
        sampler.join(5)
    for worker in workers.values():
        assert worker.error is None, worker.error
    completed = [pid for w in workers.values() for pid in w.completed]
    assert sorted(completed) == sorted(sizes), sorted(set(sizes) - set(completed))
    peak = {name: max(values) for name, values in samples.items() if values}
    for name, high in peak.items():
        assert high <= BUDGET + TOLERANCE, f'{name} held {high} bytes: over B + one message ({BUDGET + TOLERANCE})'
    aggregate = max(sum(values[i] for values in samples.values() if i < len(values))
                    for i in range(max(len(v) for v in samples.values())))
    assert aggregate <= len(workers) * (BUDGET + TOLERANCE)
    record['resident_peak_bytes'] = peak
    record['aggregate_peak_bytes'] = aggregate
    record['assignment'] = {name: list(w.completed) for name, w in workers.items()}
    record['pressure'] = {name: w.budget_waits() for name, w in workers.items()}
    assert record['pressure']['slow'] > 0


@pytest.mark.case('RUN-025')
@pytest.mark.level('process')
def test_run_025_prefetch_fairness_and_byte_admission_remain_bounded_under(evidence_dir, record_faults) -> None:
    '''
    RUN-025 (P1, integration, process): Prefetch fairness and byte admission remain bounded
    under skewed workers.

    Acceptance: Measured resident queue payload never exceeds B plus the documented atomic-
    admission tolerance; runnable capacity is not indefinitely starved by another worker
    prefetching.

    The runtime's byte admission over the reference model: each worker's receive
    asks for at most its remaining budget, and the model admits one message over
    it at most — the same tolerance the JetStream adapter's pull loop documents.
    '''
    driver = MemoryDriver('f', 'r')
    record : Dict[str, Any] = {}
    try:
        _oracle_run_025(driver, record, record_faults)
    finally:
        driver.close()
        write_evidence(evidence_dir, 'admission_trace.json', record)


@pytest.mark.case('RUN-025')
@pytest.mark.level('broker')
@pytest.mark.variant('jetstream')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_run_025_jetstream_byte_budget_bounds_the_prefetch_queue(nats_url, evidence_dir, record_faults) -> None:
    '''The adapter's budget (``byte_budget``, what ``VF_PREFETCH_BYTES`` sets on a worker): its pull loop pauses at B.'''
    flow, run = unique_ids('run025')
    driver = BudgetedJetStreamDriver(nats_url, flow, run, byte_budget = BUDGET)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _oracle_run_025(driver, record, record_faults)
    finally:
        driver.close()
        write_evidence(evidence_dir, 'admission_trace.json', record)


@pytest.mark.negative_control(of = 'RUN-025')
def test_run_025_detects_a_count_only_admission(monkeypatch) -> None:
    '''A receive bounded by message count alone lets the slow worker hold frames past B: the oracle must catch it.'''
    defects_run3.count_only_admission(monkeypatch)
    driver = MemoryDriver('f', 'r')
    try:
        assert defects.detects(_oracle_run_025, driver, {})
    finally:
        driver.close()
