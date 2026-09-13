'''
The progress watchdog: the thread that re-checks ``ProgressDeadline`` while the
node is *inside* ``process()``/``consume()``.

Why a thread at all. The task loop consults the deadline between messages, which
catches a node that is alive but no longer acking — and misses the one failure
the deadline was written for: a callback that never returns. A ``process()``
wedged on a lock, an unbounded read or a device call never gets back to the loop,
so the loop never checks, and the pod sits there with its broker lease heartbeats
perfectly healthy (the messenger extends acks from its own I/O thread) while the
message it holds goes nowhere. The watchdog owns nothing new: it calls the *same*
``ProgressDeadline`` the loop uses, on its own daemon thread, every
``interval_seconds``. A node that is merely slow keeps calling
``record_progress()`` on every ack and is never touched; a node that is stuck
with work pending trips the deadline within ``timeout + interval`` — the
"declared tolerance" of RUN-011 — and the watchdog hands the resulting error to
``on_stall`` exactly once, then stops.

What ``on_stall`` does is the caller's decision, which is what keeps this module
pure enough to test with a fake clock: the worker's callback writes the
termination reason and calls ``os._exit`` (a thread cannot unwind the main thread's
wedged frame, so exiting the process is the only recovery there is); a test's
callback records the error and releases whatever latch stood in for the hang.

``tick()`` is public for the same reason ``ProgressDeadline`` takes a clock: one
check, no thread, so "does it fire, and does it *not* fire" is answerable without
waiting on real time. ``start()`` merely runs ``tick()`` on a schedule.
'''
from __future__ import absolute_import, division, print_function

import logging
import threading
from typing import Callable, Optional

from ..core.errors import BrokerUnavailable, ConfigError, ProgressStalled
from ..core.supervision import ProgressDeadline

logger = logging.getLogger(__package__)

#: Seconds between deadline checks. Small next to the default 300s timeout, so
#: the detection tolerance it adds is negligible, and large enough that the
#: broker query behind ``pending_observation`` (consulted only once the silence
#: threshold is crossed) is never a load of its own.
DEFAULT_WATCHDOG_INTERVAL_SECONDS = 5.0

class ProgressWatchdog:
    '''
    Re-checks a ``ProgressDeadline`` on a daemon thread and reports the first
    stall to ``on_stall``.

    - Arguments:
        - deadline: the node's deadline — the **same** instance the task loop \
            calls ``record_progress()``/``check()`` on, or the watchdog would be \
            measuring silence nobody resets.
        - interval_seconds: how often ``check()`` runs. Must be positive; the \
            worker treats 0 as "no watchdog" and never constructs one.
        - on_stall: called exactly once, on the watchdog thread, with the \
            ``ProgressStalled`` or ``BrokerUnavailable`` the deadline raised. \
            May not return (``os._exit``); the watchdog is finished either way.
        - name: names the thread, so a stack dump says which node it belongs to.

    - Raises:
        - ConfigError: if ``interval_seconds`` is not positive.
    '''
    def __init__(self, deadline : ProgressDeadline, interval_seconds : float,
                on_stall : Callable[[BaseException], None], name : str = '') -> None:
        if interval_seconds <= 0:
            raise ConfigError(
                f'A progress watchdog needs a positive interval, not {interval_seconds!r}.',
                remedy = 'Set VF_WATCHDOG_INTERVAL_SECONDS to a positive number of seconds, '
                        'or to 0 to run without the watchdog thread.')
        self._deadline = deadline
        self._interval = interval_seconds
        self._on_stall = on_stall
        self._name = name
        self._stop = threading.Event()
        self._thread : Optional[threading.Thread] = None
        self._stall : Optional[BaseException] = None

    @property
    def fired(self) -> bool:
        '''Whether a stall was detected (and ``on_stall`` called).'''
        return self._stall is not None

    @property
    def stall(self) -> Optional[BaseException]:
        '''The error the watchdog fired with, or ``None`` while nothing has stalled.'''
        return self._stall

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def tick(self) -> bool:
        '''
        One deadline check. Returns whether the watchdog has fired — on this
        tick or an earlier one; ``on_stall`` is never called twice.

        A probe that raises something the deadline does not classify is logged
        and ignored: the watchdog exists to notice a *node* that stopped, and a
        flaky observation is not evidence of that.
        '''
        if self._stall is not None:
            return True
        try:
            self._deadline.check()
        except (ProgressStalled, BrokerUnavailable) as e:
            # Recorded before the callback runs: the production callback exits
            # the process, and a test's callback may want to read it.
            self._stall = e
            self._stop.set()
            self._on_stall(e)
            return True
        except Exception:
            logger.warning('progress watchdog could not check the deadline; will retry',
                        exc_info = True)
        return False

    def start(self) -> None:
        '''Starts the thread. A second call while running, or after firing, is a no-op.'''
        if self.running or self._stall is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target = self._run, daemon = True,
            name = f'vf-watchdog-{self._name}' if self._name else 'vf-watchdog')
        self._thread.start()

    def stop(self) -> None:
        '''
        Stops the thread and joins it. Idempotent, and safe from ``on_stall``
        itself (a thread cannot join itself, so that call only signals).
        '''
        self._stop.set()
        thread, self._thread = self._thread, None
        if thread is not None and thread is not threading.current_thread():
            thread.join()

    def _run(self) -> None:
        # ``wait`` doubles as the sleep and the stop signal: a stop() call ends the
        # thread within one scheduling quantum rather than one interval.
        while not self._stop.wait(self._interval):
            try:
                if self.tick():
                    return
            except Exception:
                # on_stall raised: it had its one call, and there is no second.
                logger.exception('progress watchdog stall callback failed')
                return

__all__ = ['ProgressWatchdog', 'DEFAULT_WATCHDOG_INTERVAL_SECONDS']
