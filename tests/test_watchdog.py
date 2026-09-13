'''
The progress watchdog (``videoflow.runtime.watchdog``): the thread that catches a
node wedged *inside* ``process()``/``consume()`` — which the run loop's own
between-messages check never can — plus the worker's stall callback and its
SIGTERM → ``quiesce()`` hook.

A fake clock wherever the question is "does it fire, and does it *not*": the
RUN-012 controls (an idle source, a slow-but-progressing node) and the RUN-011
stall itself. One real thread against a latched ``process()`` for the property
that is the whole point — it fires while the node is still blocked — kept under
a few hundred milliseconds by small timeouts.
'''
from __future__ import absolute_import, division, print_function

import json
import os
import signal
import subprocess
import sys
import threading
import time

import pytest
from support_messenger import RecordingMessenger

from videoflow.backends.outcomes import known, unknown
from videoflow.core.errors import (
    EXIT_ENVIRONMENT,
    EXIT_FLOW_STALLED,
    WORKER_FATAL,
    BrokerUnavailable,
    ConfigError,
    ProgressStalled,
    UpstreamAborted,
)
from videoflow.core.node import ProcessorNode
from videoflow.core.supervision import ProgressDeadline
from videoflow.core.task import ProcessorTask
from videoflow.runtime import worker
from videoflow.runtime.watchdog import DEFAULT_WATCHDOG_INTERVAL_SECONDS, ProgressWatchdog


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds : float) -> None:
        self.now += seconds


class _Stalls:
    '''Records what the watchdog fired with.'''
    def __init__(self) -> None:
        self.errors = []

    def __call__(self, error : BaseException) -> None:
        self.errors.append(error)


def _watchdog(deadline : ProgressDeadline, interval : float = 1.0):
    stalls = _Stalls()
    return ProgressWatchdog(deadline, interval, stalls, name = 'n'), stalls


# -- ticks against a fake clock (RUN-012 controls, RUN-011 detection) ----------

def test_an_idle_source_never_fires():
    '''Nothing pending is idle, not stalled — however long the silence.'''
    clock = _Clock()
    deadline = ProgressDeadline(10.0, pending_probe = lambda: known(0), clock = clock)
    watchdog, stalls = _watchdog(deadline)
    clock.advance(1000.0)
    assert watchdog.tick() is False
    clock.advance(1000.0)
    assert watchdog.tick() is False
    assert stalls.errors == [] and watchdog.fired is False


def test_a_slow_but_progressing_node_never_fires():
    '''
    The false-positive control: a node that takes nearly the whole deadline per
    message but keeps acking is slow, not stuck, whatever the pending count says.
    '''
    clock = _Clock()
    deadline = ProgressDeadline(10.0, pending_probe = lambda: known(50), clock = clock)
    watchdog, stalls = _watchdog(deadline)
    for _ in range(50):
        clock.advance(9.0)
        deadline.record_progress()             # the loop's ack
        assert watchdog.tick() is False
    assert stalls.errors == []


def test_a_startup_as_long_as_the_deadline_is_not_a_stall():
    '''
    A model load in open() is silence by the clock but not by intent: the task
    resets the window after open(), and only then does the watchdog look.
    '''
    clock = _Clock()
    deadline = ProgressDeadline(10.0, pending_probe = lambda: known(5), clock = clock)
    watchdog, stalls = _watchdog(deadline)
    clock.advance(100.0)                       # open() took a hundred seconds
    deadline.record_progress()                 # ...which NodeTask.run() forgives
    assert watchdog.tick() is False
    clock.advance(9.0)
    assert watchdog.tick() is False            # a full window from that point
    assert stalls.errors == []


def test_a_stall_fires_once_with_the_deadline_error():
    clock = _Clock()
    deadline = ProgressDeadline(10.0, pending_probe = lambda: known(3), clock = clock,
                                node_name = 'detector')
    watchdog, stalls = _watchdog(deadline)
    clock.advance(9.0)
    assert watchdog.tick() is False            # within the window
    clock.advance(2.0)
    assert watchdog.tick() is True             # D exceeded with work pending
    assert watchdog.fired is True
    assert isinstance(watchdog.stall, ProgressStalled)
    assert watchdog.stall.disposition == WORKER_FATAL      # the inputs are never blamed
    assert 'detector' in str(watchdog.stall)
    # Exactly once: later ticks report the fired state, never a second callback.
    clock.advance(100.0)
    assert watchdog.tick() is True
    assert stalls.errors == [watchdog.stall]


def test_an_unobservable_broker_fires_as_broker_unavailable():
    '''
    Unknown is not zero: it neither resets nor trips the window, and only once
    it has lasted past the grace does the watchdog fire — with the environment
    error, so nobody reads a broken broker as a stuck node.
    '''
    clock = _Clock()
    deadline = ProgressDeadline(10.0, pending_probe = lambda: unknown('timeout', 'no reply'),
                                clock = clock, unknown_grace_seconds = 5.0)
    watchdog, stalls = _watchdog(deadline)
    clock.advance(11.0)
    assert watchdog.tick() is False            # first Unknown: the grace clock starts
    clock.advance(4.0)
    assert watchdog.tick() is False
    clock.advance(1.0)
    assert watchdog.tick() is True
    assert isinstance(watchdog.stall, BrokerUnavailable)
    assert stalls.errors == [watchdog.stall]


def test_a_probe_that_raises_is_not_a_stall(caplog):
    '''A flaky observation is not evidence that the node stopped.'''
    def broken():
        raise RuntimeError('consumer_info exploded')

    clock = _Clock()
    deadline = ProgressDeadline(10.0, pending_probe = broken, clock = clock)
    watchdog, stalls = _watchdog(deadline)
    clock.advance(100.0)
    assert watchdog.tick() is False
    assert stalls.errors == [] and watchdog.fired is False
    assert any('could not check' in r.message for r in caplog.records)


def test_the_interval_must_be_positive():
    deadline = ProgressDeadline(10.0, pending_probe = lambda: known(0))
    with pytest.raises(ConfigError, match = 'VF_WATCHDOG_INTERVAL_SECONDS'):
        ProgressWatchdog(deadline, 0.0, lambda e: None)


# -- the thread ----------------------------------------------------------------

def test_start_and_stop_are_idempotent_and_stop_from_the_callback_is_safe():
    deadline = ProgressDeadline(0.05, pending_probe = lambda: known(1))
    fired = threading.Event()

    def on_stall(error : BaseException) -> None:
        watchdog.stop()                        # from its own thread: must not deadlock
        fired.set()

    watchdog = ProgressWatchdog(deadline, 0.01, on_stall)
    watchdog.start()
    watchdog.start()                           # already running: no second thread
    assert fired.wait(timeout = 2.0)
    watchdog.stop()
    watchdog.stop()
    assert watchdog.fired and not watchdog.running
    watchdog.start()                           # fired: nothing left to watch
    assert not watchdog.running


def test_stop_ends_a_healthy_watchdog_promptly():
    deadline = ProgressDeadline(60.0, pending_probe = lambda: known(0))
    watchdog, stalls = _watchdog(deadline, interval = 30.0)
    watchdog.start()
    assert watchdog.running
    started = time.monotonic()
    watchdog.stop()                            # must not wait out the interval
    assert time.monotonic() - started < 1.0
    assert not watchdog.running and stalls.errors == []


class _LatchedProcessor(ProcessorNode):
    '''
    Passes its first message and then blocks on a latch the test holds — a wedge
    that can be released, unlike ``WedgedProcessor``'s, so the thread it hangs
    can be joined once the watchdog has done its job.
    '''
    def __init__(self, latch : threading.Event, **kwargs) -> None:
        self._latch = latch
        self._seen = 0
        super().__init__(**kwargs)

    def process(self, item):               # type: ignore[override]
        self._seen += 1
        if self._seen > 1:
            self._latch.wait()
        return item


def test_the_watchdog_fires_while_process_is_still_blocked():
    '''
    RUN-011. The loop's own check runs between messages, so a process() that
    never returns is invisible to it; the watchdog thread catches it within
    D + interval while the loop is still inside the call. The production
    callback would os._exit here; releasing the latch stands in for that.

    The task runs on this (main) thread, as in a worker — its loop installs a
    signal handler, which only the main thread may do — so the thread that is
    wedged is the one running the test, and only the watchdog can free it. A
    safety timer releases the latch after 3s so a watchdog that never fires
    fails the test instead of hanging the suite.
    '''
    latch = threading.Event()
    messenger = RecordingMessenger([RecordingMessenger.data(i) for i in range(2)],
                                   pending = 1)
    deadline = ProgressDeadline(0.2, messenger.pending_observation, 'wedged')
    seen = {}

    def on_stall(error : BaseException) -> None:
        seen['error'] = error
        seen['acks_at_fire'] = messenger.acks
        seen['still_blocked'] = not latch.is_set()
        seen['fired_at'] = time.monotonic()
        latch.set()

    watchdog = ProgressWatchdog(deadline, 0.02, on_stall, name = 'wedged')
    task = ProcessorTask(_LatchedProcessor(latch, name = 'wedged'), messenger, True, ['p'],
                        deadline = deadline, watchdog = watchdog)
    bail = threading.Timer(3.0, latch.set)
    bail.daemon = True
    bail.start()
    started = time.monotonic()
    try:
        task.run()                             # wedges in process() until something sets the latch
    finally:
        bail.cancel()

    assert 'error' in seen, 'the watchdog never fired; the safety timer released the wedge'
    assert isinstance(seen['error'], ProgressStalled)
    assert seen['still_blocked'] is True
    assert seen['acks_at_fire'] == 1          # message 1 acked; message 2 was the wedge
    assert 0.2 <= seen['fired_at'] - started < 3.0
    assert watchdog.fired and not watchdog.running
    # Once released the loop finished normally: the wedge was never acked early.
    assert messenger.call_names()[-1] == 'stop' and messenger.acks == 2


# -- the task's side of the contract ---------------------------------------------

class _StubWatchdog:
    '''Records when the task starts and stops it, and how silent the deadline was at start.'''
    def __init__(self, deadline : ProgressDeadline) -> None:
        self._deadline = deadline
        self.calls = []
        self.silence_at_start = None

    def start(self) -> None:
        self.silence_at_start = self._deadline.silent_for()
        self.calls.append('start')

    def stop(self) -> None:
        self.calls.append('stop')


def test_the_task_resets_the_deadline_after_open_and_brackets_the_loop():
    clock = _Clock()

    class _SlowToOpen(ProcessorNode):
        def open(self) -> None:
            clock.advance(100.0)               # a long model load

        def process(self, item):           # type: ignore[override]
            return item

    messenger = RecordingMessenger([RecordingMessenger.data(1)], pending = 5)
    deadline = ProgressDeadline(10.0, messenger.pending_observation, clock = clock)
    stub = _StubWatchdog(deadline)
    ProcessorTask(_SlowToOpen(name = 's'), messenger, True, ['p'],
                  deadline = deadline, watchdog = stub).run()   # type: ignore[arg-type]
    assert stub.silence_at_start == 0.0        # open() was forgiven before the watchdog looked
    assert stub.calls == ['start', 'stop']
    assert messenger.acks == 1


def test_the_watchdog_is_stopped_even_when_the_loop_raises():
    class _Plain(ProcessorNode):
        def process(self, item):           # type: ignore[override]
            return item

    messenger = RecordingMessenger([RecordingMessenger.abort('p')], parents = ['p'])
    deadline = ProgressDeadline(10.0, messenger.pending_observation)
    stub = _StubWatchdog(deadline)
    with pytest.raises(UpstreamAborted):
        ProcessorTask(_Plain(name = 'x'), messenger, True, ['p'],
                      deadline = deadline, watchdog = stub).run()   # type: ignore[arg-type]
    assert stub.calls == ['start', 'stop']


def test_a_failing_open_never_starts_the_watchdog():
    class _BadOpen(ProcessorNode):
        def open(self) -> None:
            raise RuntimeError('no device')

        def process(self, item):           # type: ignore[override]
            return item

    messenger = RecordingMessenger()
    deadline = ProgressDeadline(10.0, messenger.pending_observation)
    stub = _StubWatchdog(deadline)
    with pytest.raises(Exception):
        ProcessorTask(_BadOpen(name = 'x'), messenger, True, ['p'],
                      deadline = deadline, watchdog = stub).run()   # type: ignore[arg-type]
    assert stub.calls == []


# -- the worker's side: the stall callback and its env knobs ---------------------

def test_exit_on_stall_records_the_reason_and_exits_with_the_stall_code(tmp_path, monkeypatch):
    log = tmp_path / 'term.json'
    monkeypatch.setenv('VF_TERMINATION_LOG', str(log))
    exits = []
    error = ProgressStalled('stuck', remedy = 'look for a blocking call', node = 'n', pending = 4)
    worker.exit_on_stall(error, exit_process = exits.append)
    assert exits == [EXIT_FLOW_STALLED]
    written = json.loads(log.read_text())
    assert written['code'] == 'VF_PROGRESS_STALLED'
    assert written['disposition'] == WORKER_FATAL       # restartable: the inputs were fine
    assert written['remedy'] == 'look for a blocking call'
    assert written['context']['pending'] == 4


def test_exit_on_stall_keeps_an_environment_errors_own_exit_code(tmp_path, monkeypatch):
    '''A broker that could not be observed is exit 3, as it is everywhere else.'''
    monkeypatch.setenv('VF_TERMINATION_LOG', str(tmp_path / 'term.json'))
    exits = []
    worker.exit_on_stall(BrokerUnavailable('unobservable', remedy = 'restore it'),
                        exit_process = exits.append)
    assert exits == [EXIT_ENVIRONMENT]


def test_watchdog_interval_from_env(monkeypatch):
    monkeypatch.delenv('VF_WATCHDOG_INTERVAL_SECONDS', raising = False)
    assert worker.watchdog_interval_from_env() == DEFAULT_WATCHDOG_INTERVAL_SECONDS == 5.0
    monkeypatch.setenv('VF_WATCHDOG_INTERVAL_SECONDS', '2.5')
    assert worker.watchdog_interval_from_env() == 2.5
    monkeypatch.setenv('VF_WATCHDOG_INTERVAL_SECONDS', '0')
    assert worker.watchdog_interval_from_env() == 0.0
    for bad in ('-1', 'soon'):
        monkeypatch.setenv('VF_WATCHDOG_INTERVAL_SECONDS', bad)
        with pytest.raises(ConfigError, match = 'VF_WATCHDOG_INTERVAL_SECONDS'):
            worker.watchdog_interval_from_env()


def test_build_watchdog_is_off_when_either_knob_is_zero():
    deadline = ProgressDeadline(300.0, pending_probe = lambda: known(0))
    assert worker.build_watchdog(deadline, 0.0, 300.0, 'n') is None      # no thread
    assert worker.build_watchdog(deadline, 5.0, 0.0, 'n') is None        # deadline disabled
    built = worker.build_watchdog(deadline, 5.0, 300.0, 'n')
    assert isinstance(built, ProgressWatchdog) and not built.running


# -- SIGTERM → quiesce ------------------------------------------------------------

class _Quiescing(RecordingMessenger):
    def __init__(self) -> None:
        super().__init__()
        self.quiesced = 0

    def quiesce(self) -> None:
        self.quiesced += 1


def test_sigterm_quiesces_the_messenger_then_takes_the_follow_up():
    '''
    Delivered for real, to this process: the handler runs on the main thread,
    quiesces, and only then hands the signal on. The follow-up is injected so the
    test survives what the default (SIG_DFL and re-raise) would do to it.
    '''
    previous = signal.getsignal(signal.SIGTERM)
    messenger = _Quiescing()
    followed = []
    restore = worker.install_sigterm_quiesce(messenger, then = followed.append)
    try:
        os.kill(os.getpid(), signal.SIGTERM)
        deadline = time.monotonic() + 2.0
        while not followed and time.monotonic() < deadline:
            time.sleep(0.01)                   # a Python handler runs on the next bytecode
    finally:
        restore()
    assert messenger.quiesced == 1
    assert followed == [signal.SIGTERM]
    assert signal.getsignal(signal.SIGTERM) == previous


def test_the_hook_is_not_installed_off_the_main_thread():
    before = signal.getsignal(signal.SIGTERM)
    outcome = []
    thread = threading.Thread(target = lambda: outcome.append(
        worker.install_sigterm_quiesce(_Quiescing())))
    thread.start()
    thread.join()
    assert signal.getsignal(signal.SIGTERM) == before
    outcome[0]()                               # the restore is a harmless no-op


def test_a_sigtermed_worker_still_dies_of_sigterm_after_quiescing():
    '''
    The production follow-up, in a throwaway interpreter: after quiesce() the
    worker takes the signal's default action, so every existing contract about
    a SIGTERMed worker (exit status, no clean end-of-stream, un-acked inputs
    redelivered) is exactly what it was.
    '''
    script = '\n'.join([
        'import os, signal',
        'from videoflow.core.engine import Messenger',
        'from videoflow.runtime.worker import install_sigterm_quiesce',
        'class M(Messenger):',
        '    def quiesce(self):',
        "        print('quiesced', flush = True)",
        'install_sigterm_quiesce(M())',
        'os.kill(os.getpid(), signal.SIGTERM)',
        "print('survived', flush = True)",
    ])
    proc = subprocess.run([sys.executable, '-c', script], capture_output = True,
                          text = True, timeout = 60)
    assert proc.returncode == -signal.SIGTERM, proc.stderr
    assert proc.stdout.split() == ['quiesced']


if __name__ == '__main__':
    pytest.main([__file__])
