'''
Named fault barriers for deterministic failure injection.

A conformance case is a story with a fault in the middle: "crash after the group
is persisted but before its outputs are published", "drop the acknowledgment of
the settlement", "let a second owner write between the read and the update".
Repeating a happy-path test many times does not reach those interleavings; a
barrier does. Adapters and the runtime call ``barrier(name, **context)`` at each
named point; in production nothing is installed and the call is one attribute
read. A test installs a ``FaultSchedule`` mapping barrier names to actions, and
afterwards asks which barriers actually fired — a scheduled fault that never
fired makes the test ``INVALID_TEST``, not ``PASS``.

Barriers reach worker subprocesses too: ``FaultSchedule.to_env()`` serialises the
schedule into ``VF_FAULT_SCHEDULE_JSON`` and names a marker directory in
``VF_FAULT_MARKER_DIR``; ``videoflow.runtime.worker`` installs it from the
environment, and every hit appends a marker file the parent can count.

Actions:

- ``Crash(exit_code)``: write the marker, then ``os._exit`` — no ``finally``, no
  atexit, exactly the death a kill produces.
- ``RaiseError(factory)`` / ``RaiseError.typed(code, message, disposition)``: raise.
- ``Delay(seconds)``: sleep, then continue.
- ``DropResponse``: continue, but the adapter reports the call's outcome as
  ``Unknown`` — the lost-receipt model at the adapter boundary.
- ``Pause(name)``: block until ``FaultSchedule.release(name)`` (or the marker
  ``<dir>/<name>.release`` appears), so two processes can be interleaved by hand.
- ``Nth(n, action)``: apply ``action`` on the n-th hit only.
'''
from __future__ import absolute_import, division, print_function

import json
import os
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping

BARRIERS : frozenset[str] = frozenset({
    'payload.write.before', 'payload.write.after', 'payload.read.before', 'payload.read.after',
    'obligation.acquire.after', 'obligation.release.before', 'obligation.release.after',
    'publish.send.before', 'publish.send.after', 'publish.receipt.before', 'publish.receipt.after',
    'group.commit.before', 'group.commit.after', 'settle.before', 'settle.after',
    'sink.effect.before', 'sink.effect.after', 'eos.record.before', 'eos.record.after',
    'checkpoint.write.before', 'checkpoint.write.after',
    'owner.read.after', 'owner.update.before', 'owner.update.after',
    'claim.create.after', 'claim.schedule.after', 'claim.prepare.after', 'claim.ready.after',
    'restore.before', 'restore.after', 'delete.before', 'delete.after',
    'provision.channel.before', 'provision.channel.after',
    'provision.subscription.before', 'provision.subscription.after',
    'observe.subscription.before', 'worker.ready.after', 'source.publish.after',
})

ENV_SCHEDULE = 'VF_FAULT_SCHEDULE_JSON'
ENV_MARKER_DIR = 'VF_FAULT_MARKER_DIR'
DEFAULT_PAUSE_TIMEOUT_SECONDS = 60.0

class UnknownBarrier(ValueError):
    '''A schedule or a call named a barrier that is not in ``BARRIERS``.'''

@dataclass(frozen = True)
class Crash:
    exit_code : int = 137

@dataclass(frozen = True)
class RaiseError:
    factory : Callable[[], BaseException]
    #: Serialisable form for cross-process schedules (see ``typed``); None when the
    #: factory is an in-process callable that cannot travel.
    spec : Mapping[str, str] | None = None

    @staticmethod
    def typed(code : str, message : str, disposition : str = 'transient') -> 'RaiseError':
        '''A raise action built from the error taxonomy, serialisable into a worker's env.'''
        spec = {'code': code, 'message': message, 'disposition': disposition}

        def factory() -> BaseException:
            # Function-level: keep this module importable without the taxonomy's
            # dependencies when only the barrier registry is wanted.
            from ..core.errors import PoisonMessage, TransientFailure, WorkerFatal
            cls = {'poison': PoisonMessage, 'transient': TransientFailure,
                   'worker_fatal': WorkerFatal}[disposition]
            error = cls(message, remedy = 'injected by the conformance fault schedule')
            error.code = code
            return error

        return RaiseError(factory, spec)

@dataclass(frozen = True)
class Delay:
    seconds : float

@dataclass(frozen = True)
class DropResponse:
    pass

@dataclass(frozen = True)
class Pause:
    name : str
    timeout_seconds : float = DEFAULT_PAUSE_TIMEOUT_SECONDS

@dataclass(frozen = True)
class Nth:
    n : int
    action : Any

Action = Crash | RaiseError | Delay | DropResponse | Pause | Nth

@dataclass(frozen = True)
class Hit:
    '''What ``barrier()`` returns: whether the adapter must report this call's outcome as unknown.'''
    fired : bool
    drop_response : bool = False

NO_HIT = Hit(False)

class FaultSchedule:
    '''
    - Arguments:
        - actions: barrier name -> action.
        - marker_dir: directory for cross-process markers; created on install.
    '''
    def __init__(self, actions : Mapping[str, Action], marker_dir : str | None = None) -> None:
        unknown = sorted(set(actions) - BARRIERS)
        if unknown:
            raise UnknownBarrier(f'unknown barrier(s) {unknown}; known: {sorted(BARRIERS)}')
        self._actions = dict(actions)
        self._marker_dir = marker_dir
        self._counts : dict[str, int] = {}
        self._released : set[str] = set()
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)

    # -- lifecycle -------------------------------------------------------------

    def install(self) -> 'FaultSchedule':
        global _SCHEDULE
        if self._marker_dir:
            os.makedirs(self._marker_dir, exist_ok = True)
        _SCHEDULE = self
        return self

    def uninstall(self) -> None:
        global _SCHEDULE
        if _SCHEDULE is self:
            _SCHEDULE = None

    def __enter__(self) -> 'FaultSchedule':
        return self.install()

    def __exit__(self, *exc : object) -> None:
        self.uninstall()

    # -- evidence ----------------------------------------------------------------

    def fired(self) -> dict[str, int]:
        '''Hit counts per barrier: in-process hits plus marker files written by other processes.'''
        with self._lock:
            counts = dict(self._counts)
        if self._marker_dir and os.path.isdir(self._marker_dir):
            for entry in os.listdir(self._marker_dir):
                name, _, suffix = entry.rpartition('.')
                if suffix == 'fired' and name in BARRIERS:
                    with open(os.path.join(self._marker_dir, entry)) as f:
                        counts[name] = counts.get(name, 0) + sum(1 for _ in f)
        return counts

    def unfired(self) -> list[str]:
        fired = self.fired()
        return sorted(name for name in self._actions if fired.get(name, 0) == 0)

    def release(self, name : str) -> None:
        '''Let a ``Pause(name)`` continue, in this process and in any process sharing the marker dir.'''
        with self._condition:
            self._released.add(name)
            self._condition.notify_all()
        if self._marker_dir:
            with open(os.path.join(self._marker_dir, f'{name}.release'), 'w') as f:
                f.write('released\n')

    # -- cross-process transport -------------------------------------------------

    def to_env(self) -> dict[str, str]:
        '''Environment variables a worker subprocess needs to install the same schedule.'''
        if not self._marker_dir:
            raise ValueError('a marker_dir is required to hand a schedule to another process')
        serialised : dict[str, Any] = {}
        for name, action in self._actions.items():
            serialised[name] = _serialise(action)
        return {ENV_SCHEDULE: json.dumps(serialised, sort_keys = True), ENV_MARKER_DIR: self._marker_dir}

    @classmethod
    def from_env(cls, environ : Mapping[str, str] | None = None) -> 'FaultSchedule | None':
        env = os.environ if environ is None else environ
        raw = env.get(ENV_SCHEDULE)
        if not raw:
            return None
        actions = {name: _deserialise(spec) for name, spec in json.loads(raw).items()}
        return cls(actions, env.get(ENV_MARKER_DIR) or None)

    # -- the hit -------------------------------------------------------------------

    def hit(self, name : str, context : Mapping[str, Any]) -> Hit:
        if name not in BARRIERS:
            raise UnknownBarrier(f'unknown barrier {name!r}')
        action = self._actions.get(name)
        with self._lock:
            self._counts[name] = self._counts.get(name, 0) + 1
            count = self._counts[name]
        if action is None:
            return NO_HIT
        if isinstance(action, Nth):
            if count != action.n:
                return NO_HIT
            action = action.action
        self._mark(name)
        if isinstance(action, Crash):
            os._exit(action.exit_code)
        if isinstance(action, RaiseError):
            raise action.factory()
        if isinstance(action, Delay):
            time.sleep(action.seconds)
            return Hit(True)
        if isinstance(action, DropResponse):
            return Hit(True, drop_response = True)
        if isinstance(action, Pause):
            self._wait(action)
            return Hit(True)
        raise TypeError(f'unsupported fault action {action!r}')

    def _mark(self, name : str) -> None:
        if not self._marker_dir:
            return
        with open(os.path.join(self._marker_dir, f'{name}.fired'), 'a') as f:
            f.write(f'{os.getpid()} {time.time():.6f}\n')
            f.flush()
            os.fsync(f.fileno())

    def _wait(self, pause : Pause) -> None:
        deadline = time.monotonic() + pause.timeout_seconds
        marker = os.path.join(self._marker_dir, f'{pause.name}.release') if self._marker_dir else None
        with self._condition:
            while pause.name not in self._released:
                if marker and os.path.exists(marker):
                    self._released.add(pause.name)
                    break
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f'Pause({pause.name!r}) was never released within '
                                       f'{pause.timeout_seconds}s')
                self._condition.wait(timeout = min(0.05, remaining))

_SCHEDULE : FaultSchedule | None = None

def barrier(name : str, **context : Any) -> Hit:
    '''
    The production-side call. One set lookup when no schedule is installed;
    otherwise delegates to it. Adapters check ``.drop_response`` where the lost-receipt model
    applies, and let ``RaiseError``/``Crash`` propagate.
    '''
    if name not in BARRIERS:
        raise UnknownBarrier(f'unknown barrier {name!r}')
    schedule = _SCHEDULE
    if schedule is None:
        return NO_HIT
    return schedule.hit(name, context)

def installed() -> FaultSchedule | None:
    return _SCHEDULE

def _serialise(action : Action) -> dict[str, Any]:
    if isinstance(action, Crash):
        return {'kind': 'crash', 'exit_code': action.exit_code}
    if isinstance(action, RaiseError):
        if action.spec is None:
            raise ValueError('RaiseError with an in-process factory cannot cross a process boundary; '
                             'use RaiseError.typed(code, message, disposition)')
        return {'kind': 'raise', **action.spec}
    if isinstance(action, Delay):
        return {'kind': 'delay', 'seconds': action.seconds}
    if isinstance(action, DropResponse):
        return {'kind': 'drop'}
    if isinstance(action, Pause):
        return {'kind': 'pause', 'name': action.name, 'timeout_seconds': action.timeout_seconds}
    if isinstance(action, Nth):
        return {'kind': 'nth', 'n': action.n, 'action': _serialise(action.action)}
    raise TypeError(f'unsupported fault action {action!r}')

def _deserialise(spec : Mapping[str, Any]) -> Action:
    kind = spec['kind']
    if kind == 'crash':
        return Crash(int(spec.get('exit_code', 137)))
    if kind == 'raise':
        return RaiseError.typed(spec['code'], spec['message'], spec.get('disposition', 'transient'))
    if kind == 'delay':
        return Delay(float(spec['seconds']))
    if kind == 'drop':
        return DropResponse()
    if kind == 'pause':
        return Pause(spec['name'], float(spec.get('timeout_seconds', DEFAULT_PAUSE_TIMEOUT_SECONDS)))
    if kind == 'nth':
        return Nth(int(spec['n']), _deserialise(spec['action']))
    raise ValueError(f'unknown fault action kind {kind!r}')
