'''
Conformance cases: RUN-011, RUN-012, RUN-048.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions so the paired negative control
can run them against the reviewed defect (``defects.py``) and prove it fails.

These are process-level cases: RUN-011 wedges a real thread and lets the watchdog
thread free it, RUN-012 drives the same watchdog with a fake clock through the
startup / idle / working / stalled states, and RUN-048 supervises real child
processes through ``LocalProcessEngine``.
'''
from __future__ import absolute_import, division, print_function

import json
import subprocess
import sys
import threading
import time
from typing import Any, Callable, Dict, List

import defects
import pytest

from videoflow.backends.capabilities import RELIABLE_WORK, RETENTION_INTEREST
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.messaging import MemoryMessagingBackend, make_channel, make_envelope, make_subscription
from videoflow.backends.messaging import ChannelId
from videoflow.backends.outcomes import known
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.core.errors import EXIT_FLOW_STALLED, ProgressStalled
from videoflow.core.supervision import ProgressDeadline, SupervisionPolicy
from videoflow.engines.local import LocalProcessEngine
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer
from videoflow.runtime.watchdog import ProgressWatchdog
from videoflow.runtime.worker import exit_on_stall

#: Captured at import: the oracles patch ``subprocess.Popen`` and must still launch real children.
_REAL_POPEN = subprocess.Popen


def _wait_until(condition : Callable[[], bool], timeout : float, step : float = 0.02) -> bool:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if condition():
            return True
        time.sleep(step)
    return condition()


# -- RUN-011 ---------------------------------------------------------------------

CID = ChannelId('f', 'r', 'p')


def _oracle_run_011(termination_log : str, evidence : Dict[str, Any],
                    watchdog_factory : Callable[..., ProgressWatchdog] = ProgressWatchdog) -> None:
    deadline_s, interval, tolerance = 0.3, 0.02, 0.5
    # The input's ownership: a leased delivery on the reference backend, with a
    # heartbeat thread renewing the lease exactly as the messenger's keepalive would.
    clock = FakeClock()
    backend = MemoryMessagingBackend(clock)
    sub = make_subscription(CID, 'c')
    backend.ensure_channel(make_channel('f', 'r', 'p', RELIABLE_WORK, RETENTION_INTEREST, required = [sub.id]), 'op')
    backend.ensure_subscription(sub, 'op')
    backend.publish(make_envelope(CID, 'm1'), 0)
    (delivery,) = backend.receive(sub.id, 1, 1 << 20, 0)
    heartbeats : List[tuple] = []
    stop_heartbeat = threading.Event()

    def heartbeat() -> None:
        while not stop_heartbeat.is_set():
            heartbeats.append((time.monotonic(), backend.renew(delivery.token).renewed))
            stop_heartbeat.wait(0.01)

    latch = threading.Event()
    seen : Dict[str, Any] = {}
    deadline = ProgressDeadline(deadline_s, lambda: known(1), 'wedged')

    def on_stall(error : BaseException) -> None:
        seen.update(error = error, fired_at = time.monotonic(), still_blocked = not latch.is_set(),
                    heartbeats_at_fire = len(heartbeats))
        latch.set()

    watchdog = watchdog_factory(deadline, interval, on_stall, name = 'wedged')
    pulse = threading.Thread(target = heartbeat, daemon = True)
    pulse.start()
    entered = time.monotonic()
    deadline.record_progress()                 # callback entry: the input was received
    watchdog.start()
    bail = threading.Timer(deadline_s + tolerance + 1.0, latch.set)   # a watchdog that never fires must not hang the suite
    bail.daemon = True
    bail.start()
    latch.wait()                               # the wedged callback: returns only when released
    returned = time.monotonic()
    bail.cancel()
    watchdog.stop()
    stop_heartbeat.set()
    pulse.join(1.0)
    evidence.update(callback_entered = entered, callback_returned = returned,
                    heartbeats = len(heartbeats), watchdog = {k: str(v) for k, v in seen.items()})
    assert 'error' in seen, 'never diagnosed: the callback returned only because the safety timer released it'
    assert isinstance(seen['error'], ProgressStalled)
    assert seen['fired_at'] - entered <= deadline_s + interval + tolerance, seen
    assert seen['still_blocked'] is True
    assert seen['heartbeats_at_fire'] > 0 and all(ok for _t, ok in heartbeats[:seen['heartbeats_at_fire']])
    # Terminal handling begins: the production callback records the reason and ends the process.
    exits : List[int] = []
    exit_on_stall(seen['error'], exit_process = exits.append)
    assert exits == [EXIT_FLOW_STALLED]
    with open(termination_log) as f:
        record = json.load(f)
    assert record['code'] == seen['error'].code
    evidence['termination_log'] = record
    # The input was never settled: after the lease expires a replacement receives it again.
    clock.advance(31)
    (again,) = backend.receive(sub.id, 1, 1 << 20, 0)
    assert again.token.message_id == 'm1' and again.token.attempt == 2


@pytest.mark.case('RUN-011')
@pytest.mark.level('process')
@pytest.mark.timeout(30)
def test_run_011_independent_watchdog_detects_a_hung_callback_despite(tmp_path, monkeypatch, evidence_dir) -> None:
    '''
    RUN-011 (P1, runtime, process): Independent watchdog detects a hung callback despite
    healthy heartbeats.

    Acceptance: A callback that never returns is diagnosed by D plus declared tolerance even
    though broker heartbeats continue.
    '''
    log = str(tmp_path / 'termination.json')
    monkeypatch.setenv('VF_TERMINATION_LOG', log)
    evidence : Dict[str, Any] = {}
    _oracle_run_011(log, evidence)
    (evidence_dir / 'lifecycle_trace.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-011')
@pytest.mark.timeout(30)
def test_run_011_detects_a_loop_only_deadline(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv('VF_TERMINATION_LOG', str(tmp_path / 'termination.json'))
    defects.loop_only_watchdog(monkeypatch)
    assert defects.detects(_oracle_run_011, str(tmp_path / 'termination.json'), {})


# -- RUN-012 ---------------------------------------------------------------------

def _oracle_run_012(trace : List[Dict[str, Any]]) -> None:
    clock = FakeClock()
    state = {'pending': known(0)}
    deadline = ProgressDeadline(10.0, lambda: state['pending'], 'infer', clock = clock.monotonic)
    fires : List[BaseException] = []
    watchdog = ProgressWatchdog(deadline, 1.0, fires.append, name = 'infer')

    def step(phase : str, seconds : float, pending : int, progress : bool) -> None:
        state['pending'] = known(pending)
        clock.advance(seconds)
        if progress:
            deadline.record_progress()
        fired = watchdog.tick()
        trace.append({'state': phase, 'advanced': seconds, 'pending': pending, 'progress': progress,
                      'silent_for': deadline.silent_for(), 'fired': fired, 'restarts': len(fires)})

    # Startup: a warm-up four times the deadline, then the task records progress
    # once open() returns — silence during warm-up is not a stall.
    step('startup', 40.0, 0, progress = True)
    # Idle: nothing upstream for far longer than the deadline.
    for _ in range(10):
        step('idle', 10.0, 0, progress = False)
    # Working, slowly: each item takes 8 s of a 10 s budget and reports progress.
    for _ in range(5):
        step('working', 8.0, 1, progress = True)
    assert fires == [], f'false restart(s) in the control phases: {fires}'
    # A real stall: an item starts, progress is withheld past the deadline.
    step('stalled', 10.0, 1, progress = False)
    assert len(fires) == 1 and isinstance(fires[0], ProgressStalled), fires
    assert watchdog.fired is True


@pytest.mark.case('RUN-012')
@pytest.mark.level('process')
def test_run_012_legitimate_slow_inference_and_idle_sources_do_not_trigger(evidence_dir) -> None:
    '''
    RUN-012 (P1, runtime, process): Legitimate slow inference and idle sources do not
    trigger false progress failures.

    Acceptance: Control runs complete with zero false restarts, while the injected
    active-work stall triggers the configured recovery action.
    '''
    trace : List[Dict[str, Any]] = []
    _oracle_run_012(trace)
    (evidence_dir / 'state_trace.json').write_text(json.dumps(trace, indent = 2))
    assert [t['fired'] for t in trace].count(True) == 1 and trace[-1]['state'] == 'stalled'


@pytest.mark.negative_control(of = 'RUN-012')
def test_run_012_detects_a_wall_clock_deadline(monkeypatch) -> None:
    defects.wall_clock_deadline(monkeypatch)
    assert defects.detects(_oracle_run_012, [])


# -- RUN-048 ---------------------------------------------------------------------

def _flow() -> Flow:
    p = IntProducer(0, 3, name = 'producer')
    a = IdentityProcessor(name = 'work')(p)
    out = CommandlineConsumer(name = 'printer')(a)
    return Flow([out], flow_type = BATCH, flow_id = 'demo')


def _oracle_run_048(monkeypatch : pytest.MonkeyPatch, evidence : Dict[str, Any],
                    exhaust : bool = False, observation_deadline : float = 3.0) -> None:
    '''
    Real child processes: the source loops forever (until the oracle terminates
    it), ``work`` exits 1 on its first launch, and the supervisor must react
    while the source is still alive.
    '''
    codes = {'work': [1] if exhaust else [1, 0], 'printer': [0]}
    procs : Dict[str, List[subprocess.Popen]] = {}
    launches : List[Dict[str, Any]] = []

    def fake_popen(cmd : List[str], env : Dict[str, str] | None = None, **kwargs : Any) -> subprocess.Popen:
        assert env is not None
        name = env['VF_NODE_NAME']
        if name == 'producer':
            script = 'import time\nwhile True:\n    time.sleep(0.05)\n'
        else:
            code = codes[name].pop(0) if codes.get(name) else 0
            script = f'import sys\nsys.exit({code})\n'
        proc = _REAL_POPEN([sys.executable, '-c', script], env = env)
        procs.setdefault(name, []).append(proc)
        launches.append({'node': name, 'pid': proc.pid, 'at': time.monotonic()})
        return proc

    monkeypatch.setattr(subprocess, 'Popen', fake_popen)
    monkeypatch.setattr('videoflow.messaging.topology.provision_flow_sync', lambda *a, **kw: None)
    policy = SupervisionPolicy.disabled() if exhaust else SupervisionPolicy(max_restarts = 1, backoff_seconds = (0.0,))
    engine = LocalProcessEngine(supervision = policy)
    monkeypatch.setattr(engine, '_teardown_streams', lambda: None)
    aborted : List[str] = []
    monkeypatch.setattr(engine, '_abort_flow', aborted.append)     # the real one announces over NATS
    engine.allocate_and_run_tasks(_flow().tasks_data(), 'demo', BATCH, 'run1')
    assert launches[0]['node'] == 'producer'
    result : List[List[str]] = []
    supervisor = threading.Thread(target = lambda: result.append(engine.wait_for_completion()), daemon = True)
    supervisor.start()
    started = time.monotonic()
    try:
        if exhaust:
            handled = _wait_until(lambda: aborted == ['work'], observation_deadline)
        else:
            handled = _wait_until(lambda: engine.events().restart_count('work') == 1, observation_deadline)
        handled_at = time.monotonic()
        producer = procs['producer'][0]
        evidence.update(launches = launches, handled = handled, handled_after = handled_at - started,
                        producer_alive_when_handled = producer.poll() is None, exhaust = exhaust)
        assert handled, f'the work failure was not handled within {observation_deadline}s while the source ran'
        assert producer.poll() is None, 'the source exited before the failure was handled'
        assert supervisor.is_alive()
    finally:
        for proc in procs.get('producer', []):
            proc.terminate()
        supervisor.join(10.0)
    assert not supervisor.is_alive()
    evidence['exit_codes'] = {name: [p.returncode for p in ps] for name, ps in procs.items()}
    if exhaust:
        assert result == [['work']] and engine.events().failed_nodes() == ['work']
        assert engine.failures() == [('work', 0, 1)]
    else:
        assert result == [[]] and engine.events().restart_count('work') == 1


@pytest.mark.case('RUN-048')
@pytest.mark.level('process')
@pytest.mark.timeout(60)
def test_run_048_local_supervision_notices_a_later_worker_failure_while_an(monkeypatch, evidence_dir) -> None:
    '''
    RUN-048 (P1, runtime, process): Local supervision notices a later worker failure while an
    earlier worker stays alive.

    Acceptance: The later-process failure is handled within the declared observation deadline
    while the first process is still running, and exhaustion produces the specified terminal
    behavior.
    '''
    evidence : Dict[str, Any] = {}
    _oracle_run_048(monkeypatch, evidence)
    exhausted : Dict[str, Any] = {}
    _oracle_run_048(monkeypatch, exhausted, exhaust = True)
    (evidence_dir / 'supervisor_trace.json').write_text(json.dumps({'restart': evidence, 'exhaustion': exhausted},
                                                                    indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-048')
@pytest.mark.timeout(60)
def test_run_048_detects_a_sequential_supervisor(monkeypatch) -> None:
    defects.sequential_supervisor(monkeypatch)
    assert defects.detects(_oracle_run_048, monkeypatch, {})
