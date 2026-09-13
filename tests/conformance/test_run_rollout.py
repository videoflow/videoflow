'''
Conformance cases: RUN-030, RUN-031, RUN-032, RUN-033, RUN-047.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. A ``pending`` marker means the case is a skeleton reporting
NOT_RUN until its implementation phase lands.

RUN-030 is decided at process level here, on the real worker: a
``videoflow.runtime.worker`` subprocess holding one input inside ``consume`` and
four more in its adapter's prefetch queue is SIGTERMed. The production hook runs
``messenger.quiesce()`` and then lets the process die of the signal — so what the
test can show is exactly the drain's first half: admission stops, the parked
inputs go back to the broker at once (not after ``ack_wait``), the in-flight one is
redelivered when its lease lapses, and a replacement completes every input after
the old process is gone. The graceful branch — the in-flight input committed
before the process leaves — is the in-process ``graceful`` variant, which is also
what the negative control runs. The kubernetes-level primary (a pod deleted mid-
inference with a real grant to release) stays pending. RUN-031/032/033 render
requests, placement constraints and asset identity that plan Phase 4 ships
(``--resources``, ``--gpu-nodes``, digest verification); RUN-047's run-scoped names
are RFC 0006's Phase-6 flip.
'''
from __future__ import absolute_import, division, print_function

import signal
import time
from typing import Any, Callable, Dict, List

import defects
import defects_run3
import pytest
from _brokers import unique_ids
from _msgdrivers import JetStreamDriver, spec
from _runs3 import messengers, read_sink_log, start_worker, stop_flow, worker_env, write_evidence

from videoflow.backends import faults
from videoflow.backends.messaging import ChannelId
from videoflow.backends.outcomes import Known
from videoflow.core import constants
from videoflow.core.constants import BATCH
from videoflow.messaging import topology

NATS_TIMEOUT = 180
INPUTS = 6
#: The in-flight input's lease: long enough that the hand-back of the parked ones
#: (seconds) is distinguishable from the lease lapsing on a dead process.
ACK_WAIT = 12
HANDBACK_DEADLINE = 4.0


def _specs() -> list:
    return [spec('parent', [], 'producer', True), spec('sink', ['parent'], 'consumer', False)]


def _provision(driver : JetStreamDriver) -> None:
    '''The provision Job's work, with the durables' ``ack_wait`` at the lease this test watches lapse.'''
    topology.provision_flow_sync(driver.nats_url, _specs(), driver.flow_id, driver.run_id, BATCH, max_retries = 3,
                                 ack_wait = ACK_WAIT, timeout = 60)


def _leased(driver : JetStreamDriver) -> tuple[int, int]:
    observed = driver.consumer_state('sink', 'parent')
    assert isinstance(observed, Known), observed
    return observed.value                                        # (pending, leased)


def _publish(driver : JetStreamDriver, count : int, prefix : str = 'in') -> List[str]:
    ids = []
    for i in range(count):
        driver.publish_parent('parent', f'{prefix}-{i}', i + 1, {'input': f'{prefix}-{i}'})
        ids.append(f'{prefix}-{i}')
    return ids


def _oracle_run_030_forced(driver : JetStreamDriver, nats_url : str, evidence_dir : Any, record : Dict[str, Any],
                           record_faults : Callable[..., None] | None = None) -> None:
    '''
    The forced branch on the real worker: SIGTERM while one input is held inside
    ``consume`` and the rest are prefetched. Admission stops and the parked
    inputs are handed back before the process dies — the replacement, started
    once the old process has exited, commits them at once, while the in-flight
    one it never committed comes back only when its lease lapses. Every input is
    committed exactly once, all of it by the replacement.

    JetStream keeps a NAKed message in ``num_ack_pending`` until it is delivered
    again and acknowledged, so the broker's counters cannot show a hand-back on
    their own; what shows it is *when* the survivor gets the input — seconds,
    not the lease.
    '''
    credit = topology.consumer_credit(1, False)                  # 1 in processing + DEFAULT_PREFETCH parked
    # A fresh directory per run: markers and logs of an earlier run must not read as this one's.
    evidence_dir = evidence_dir / driver.run_id
    evidence_dir.mkdir(parents = True, exist_ok = True)
    sink_log = evidence_dir / 'sink_effects.jsonl'
    schedule = faults.FaultSchedule({'sink.effect.before': faults.Nth(1, faults.Pause('hold', timeout_seconds = 120))},
                                    marker_dir = str(evidence_dir / 'markers'))
    env = worker_env(nats_url, driver.flow_id, driver.run_id, 'sink', '_runs3_nodes.PausingSink',
                     {'log_path': str(sink_log), 'name': 'sink'}, ['parent'], ack_wait = ACK_WAIT,
                     extra = {**schedule.to_env(), 'VF_TERMINATION_LOG': str(evidence_dir / 'termination-log')})
    ids = _publish(driver, INPUTS)
    worker = start_worker(env, evidence_dir / 'worker-1.log')
    replacement = None
    try:
        # The worker holds its first input inside consume() and parks the next four.
        assert driver.until(lambda: schedule.fired().get('sink.effect.before', 0) >= 1, 30), 'the worker never reached its first input'
        assert driver.until(lambda: _leased(driver)[1] >= credit, 20), _leased(driver)
        before = _leased(driver)
        assert before == (INPUTS - credit, credit), before
        assert worker.poll() is None
        record['before_sigterm'] = {'pending': before[0], 'leased': before[1], 'credit': credit}
        t_term = time.time()
        worker.send_signal(signal.SIGTERM)
        worker.wait(timeout = 30)
        t_exit = time.time()
        assert worker.returncode == -signal.SIGTERM, worker.returncode   # dies of SIGTERM, as it always has
        assert read_sink_log(sink_log) == [], 'the SIGTERMed worker committed an input it was holding at the barrier'
        # The replacement starts only once the old process is gone (its grant is
        # free). The four handed-back inputs and the pending one reach it at once;
        # the in-flight one only when the dead process's lease lapses.
        schedule.release('hold')
        replacement = start_worker(env, evidence_dir / 'worker-2.log')
        t_start = time.time()
        assert driver.until(lambda: len(read_sink_log(sink_log)) >= INPUTS - 1, HANDBACK_DEADLINE), \
            f'{len(read_sink_log(sink_log))} inputs committed within {HANDBACK_DEADLINE}s: the parked inputs were not handed back'
        early = read_sink_log(sink_log)
        assert ids[0] not in {e['item']['input'] for e in early}, 'the in-flight input came back before its lease lapsed'
        assert driver.until(lambda: len(read_sink_log(sink_log)) >= INPUTS, ACK_WAIT * 3), read_sink_log(sink_log)
        effects = read_sink_log(sink_log)
        items = sorted(e['item']['input'] for e in effects)
        assert items == sorted(ids), items                         # no admitted input disappears, none twice
        assert {e['pid'] for e in effects} == {replacement.pid}, 'an input was committed by the dead process'
        in_flight = next(e for e in effects if e['item']['input'] == ids[0])
        assert in_flight['at'] - t_exit >= ACK_WAIT / 2, 'the in-flight input was not left to its lease'
        assert driver.until(lambda: _leased(driver) == (0, 0), 20), _leased(driver)
        record['after_sigterm'] = {'quiesce_to_exit_s': t_exit - t_term, 'exit_code': worker.returncode,
                                   'handed_back_committed_after_start_s': max(e['at'] for e in early) - t_start,
                                   'in_flight_committed_after_exit_s': in_flight['at'] - t_exit}
        record['replacement'] = {'started_after_exit_s': t_start - t_exit, 'completed': items, 'pid': replacement.pid,
                                 'old_pid': worker.pid}
    finally:
        for proc in (worker, replacement):
            if proc is not None and proc.poll() is None:
                stop_flow(nats_url, driver.flow_id, driver.run_id)
                try:
                    proc.wait(timeout = 15)
                except Exception:  # noqa: BLE001
                    proc.kill()
    if record_faults is not None:
        record_faults(schedule)
    record['faults'] = schedule.fired()


def _oracle_run_030_graceful(driver : JetStreamDriver, record : Dict[str, Any]) -> None:
    '''
    The graceful branch, in-process: ``quiesce()`` while one input is held stops
    admission and hands the parked inputs back at once — a survivor bound while
    the leaving worker is still alive gets them within seconds, as second
    attempts; the held input is then committed (acked) and the worker leaves,
    and the survivor never sees it.
    '''
    credit = topology.consumer_credit(1, False)
    ids = _publish(driver, INPUTS, prefix = 'g')
    with messengers() as pool:
        leaving = pool.messenger(driver, 'sink', ['parent'], ack_wait = ACK_WAIT)
        group = driver.receive_group(leaving, timeout = 30)
        held = group['parent']['message']['input']
        assert driver.until(lambda: _leased(driver)[1] >= credit, 20), _leased(driver)
        before = _leased(driver)
        t_quiesce = time.monotonic()
        leaving.quiesce()
        survivor = pool.messenger(driver, 'sink', ['parent'], ack_wait = ACK_WAIT)
        completed : List[tuple] = []
        for _ in range(INPUTS - 1):
            group = driver.receive_group(survivor, timeout = HANDBACK_DEADLINE)
            completed.append((group['parent']['message']['input'], survivor._inflight_handles[0].num_delivered))
            survivor.ack_inputs()
        t_received = time.monotonic()
        assert t_received - t_quiesce < HANDBACK_DEADLINE + 1.0, 'the parked inputs came back on the lease, not on the quiesce'
        assert sorted(item for item, _a in completed) == sorted(set(ids) - {held}), completed
        # Handed back, never a first attempt (a fetch in flight at the quiesce bounces once more).
        assert all(attempt >= 2 for item, attempt in completed if item != ids[-1]), completed
        # The leaving worker admits nothing more, commits what it held, and leaves.
        leaving.ack_inputs()
        terminal = driver.receive_group(leaving, timeout = 10)
        assert all(v.get('is_stop_signal') for v in terminal.values()), terminal
        pool.close(leaving)
        assert driver.until(lambda: _leased(driver) == (0, 0), 20), _leased(driver)
        assert driver.until(lambda: driver.retained(ChannelId(driver.flow_id, driver.run_id, 'parent')) == 0, 10)
        record['graceful'] = {'held': held, 'before_quiesce': before, 'handback_s': t_received - t_quiesce,
                              'survivor_completed': completed}


@pytest.mark.case('RUN-030')
@pytest.mark.level('process')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_run_030_gpu_scale_down_and_rollout_drain_outstanding_work_before(nats_url, evidence_dir, record_faults,
                                                                          monkeypatch) -> None:
    '''
    RUN-030 (P0, integration, kubernetes): GPU scale-down and rollout drain outstanding work
    before releasing grants.

    Acceptance: No admitted input disappears and no old/new processes concurrently use the same
    exclusive grant after reassignment; forced drains have explicit failure/retry outcomes.

    Process level, on the real worker and the compose broker: the forced branch
    (SIGTERM mid-inference) and the graceful branch (quiesce, commit, leave). The
    grant here is the process itself — a replacement starts only after the old
    process has exited. The pod-level half with a real accelerator grant is the
    pending kubernetes primary.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    flow, run = unique_ids('run030')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _provision(driver)
        _oracle_run_030_forced(driver, nats_url, evidence_dir, record, record_faults)
        graceful_run = unique_ids('run030g')[1]
        graceful = JetStreamDriver(nats_url, flow, graceful_run)
        try:
            _provision(graceful)
            _oracle_run_030_graceful(graceful, record)
        finally:
            graceful.close()
    finally:
        driver.close()
        write_evidence(evidence_dir, 'drain_timeline.json', record)


@pytest.mark.case('RUN-030')
@pytest.mark.level('kubernetes')
@pytest.mark.variant('cluster')
@pytest.mark.pending('phase 4')
def test_run_030_pod_deletion_releases_the_grant_only_after_the_drain() -> None:
    '''Pending phase 4: a pod deleted mid-inference with a real accelerator grant to release needs the allocation backend and the rollout orchestration.'''


@pytest.mark.negative_control(of = 'RUN-030')
def test_run_030_detects_a_quiesce_that_only_raises_a_flag(nats_url, monkeypatch) -> None:
    '''A quiesce that hands nothing back leaves the parked inputs leased on a leaving process: the oracle must catch it.'''
    monkeypatch.setattr(constants, 'RFC0006', True)
    defects_run3.flag_only_quiesce(monkeypatch)
    flow, run = unique_ids('run030n')
    driver = JetStreamDriver(nats_url, flow, run)
    try:
        _provision(driver)
        assert defects.detects(_oracle_run_030_graceful, driver, {})
    finally:
        driver.close()


@pytest.mark.case('RUN-031')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 4')
def test_run_031_host_cpu_and_ram_requests_participate_in_gpu_worker() -> None:
    '''
    RUN-031 (P1, deployment, kubernetes): Host CPU and RAM requests participate in GPU worker
    admission.

    Acceptance: The resource-insufficient plan is rejected or remains explicitly unadmitted, and
    admitted startup stays within the declared resource policy without relying on unconfigured
    namespace defaults.

    Pending phase 4: ``--resources`` / descriptor ``resources.cpu|memory`` rendering
    does not exist yet (Phase 1 renders GPU limits and ``--priority-class`` only).
    '''


@pytest.mark.case('RUN-032')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 4')
def test_run_032_model_compatibility_and_locality_requirements_constrain() -> None:
    '''
    RUN-032 (P1, deployment, kubernetes): Model compatibility and locality requirements
    constrain placement.

    Acceptance: No fixture executes on an incompatible or prohibited host; successful admission
    demonstrates the declared capability on the actual runtime resource.

    Pending phase 4: hard capability/locality constraints (``--gpu-nodes``, GFD-label
    node affinity) are not rendered before the allocation phase.
    '''


@pytest.mark.case('RUN-033')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 4')
def test_run_033_worker_relocation_preserves_model_and_input_asset_identity() -> None:
    '''
    RUN-033 (P1, deployment, kubernetes): Worker relocation preserves model and input asset
    identity.

    Acceptance: Relocation never processes with silently missing or changed model/input bytes;
    the portable variant resumes with the verified asset.

    Pending phase 4: asset digest verification at readiness and hostPath placement
    constraints land with the allocation/rollout phase; a render-only check of PVC
    versus hostPath mounts would not decide the acceptance.
    '''


@pytest.mark.case('RUN-047')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 6')
def test_run_047_concurrent_kubernetes_runs_cannot_overwrite_another_runs() -> None:
    '''
    RUN-047 (P0, deployment, kubernetes): Concurrent Kubernetes runs cannot overwrite another
    runs configuration or workload.

    Acceptance: RunA remains unchanged and correctly configured throughout runB
    creation/deletion, or runB is rejected before any mutation under an explicit single-run
    policy.

    Pending phase 6: run-scoped Kubernetes names (RFC 0006 §10) and the
    ``--single-run`` policy are the acceptance flip.
    '''
