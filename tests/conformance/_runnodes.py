'''
Node fixtures for the RUN conformance cases that exercise the runtime ledger:
the stateful accumulators, the stochastic component, the live and replayable
sources, the ordered tracker, the sinks with and without an external
idempotency key, and the file-backed *external system* they act on.

This module is deliberately light — stdlib and ``videoflow.core`` only — because
a worker subprocess imports it by ``VF_NODE_CLASS`` (``_runnodes.Accumulator``
with ``tests/conformance`` on ``PYTHONPATH``): the node must be reconstructible
from ``get_params()`` in a process that never saw the test, exactly as a
deployed node is. Everything a fixture records (invocations, effects, captures)
goes through ``JsonlLog``/``ExternalLedger`` files so the test process can read
it back from either side of a process boundary.
'''
from __future__ import absolute_import, division, print_function

import fcntl
import json
import os
import time
import uuid
from typing import Any, Dict, List, Optional

from videoflow.backends.capabilities import EFFECT_IDEMPOTENT_KEY
from videoflow.core.context import RuntimeContext
from videoflow.core.node import ConsumerNode, ProcessorNode, ProducerNode
from videoflow.core.policies import OrderingPolicy, ReorderBuffer


class SimulatedCrash(BaseException):
    '''
    The in-process stand-in for a worker dying at a barrier: a ``BaseException``,
    so it escapes the task loop's per-message ``except Exception`` exactly as a
    ``Crash`` (``os._exit``) leaves nothing behind — no ack, no nak, no close.
    '''


class JsonlLog:
    '''An append-only JSON-lines file shared across processes (one ``flock`` per append).'''
    def __init__(self, path : str) -> None:
        self.path = path

    def append(self, record : Dict[str, Any]) -> None:
        with open(self.path, 'a') as f:
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            f.write(json.dumps(dict(record, pid = os.getpid(), at = time.time()), sort_keys = True) + '\n')
            f.flush()

    def records(self) -> List[Dict[str, Any]]:
        if not os.path.exists(self.path):
            return []
        with open(self.path) as f:
            return [json.loads(line) for line in f if line.strip()]


class ExternalLedger:
    '''
    The *external system* a sink acts on (RUN-017): an observable counter plus
    an atomic idempotency-key API, both under one file lock so two processes —
    or two threads racing through the runtime's seen-then-consume window — see
    one serialised history. ``increment`` is the plain side effect; ``apply_once``
    is the transaction keyed by the sink's input identity.
    '''
    def __init__(self, path : str) -> None:
        self.path = path

    def _load(self) -> Dict[str, Any]:
        if not os.path.exists(self.path):
            return {'count': 0, 'effects': 0, 'keys': [], 'history': []}
        with open(self.path) as f:
            raw = f.read()
        return json.loads(raw) if raw.strip() else {'count': 0, 'effects': 0, 'keys': [], 'history': []}

    def _save(self, doc : Dict[str, Any]) -> None:
        tmp = self.path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(doc, f, sort_keys = True)
        os.replace(tmp, self.path)

    def _locked(self) -> Any:
        handle = open(self.path + '.lock', 'w')
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        return handle

    def increment(self, delta : int, actor : str) -> None:
        handle = self._locked()
        try:
            doc = self._load()
            doc['count'] += delta
            doc['effects'] += 1
            doc['history'].append({'op': 'increment', 'delta': delta, 'actor': actor, 'pid': os.getpid()})
            self._save(doc)
        finally:
            handle.close()

    def apply_once(self, key : str, delta : int, actor : str) -> bool:
        '''Apply ``delta`` under ``key`` exactly once; False when the key was already applied.'''
        handle = self._locked()
        try:
            doc = self._load()
            if key in doc['keys']:
                doc['history'].append({'op': 'apply_once', 'key': key, 'applied': False, 'actor': actor,
                                       'pid': os.getpid()})
                self._save(doc)
                return False
            doc['keys'].append(key)
            doc['count'] += delta
            doc['effects'] += 1
            doc['history'].append({'op': 'apply_once', 'key': key, 'applied': True, 'actor': actor,
                                   'pid': os.getpid()})
            self._save(doc)
            return True
        finally:
            handle.close()

    def snapshot(self) -> Dict[str, Any]:
        handle = self._locked()
        try:
            return self._load()
        finally:
            handle.close()


# -- stateful processors -----------------------------------------------------------------

class Accumulator(ProcessorNode):
    '''
    A keyed accumulator: ``totals[key] += value`` per input, the applied member
    identities recorded, the whole state checkpointed with every group. Its
    output is the derived event for the group (the new total). Deterministic;
    ``replay_policy = 'recompute'`` (the default).
    '''
    def __init__(self, log_path : Optional[str] = None, nb_tasks : int = 1,
                 partition_by : Optional[str] = None, name : Optional[str] = None, **kwargs : Any) -> None:
        self._log_path = log_path
        super().__init__(nb_tasks = nb_tasks, partition_by = partition_by, name = name, **kwargs)

    def open(self, ctx : RuntimeContext) -> None:  # type: ignore[override]  # the ctx-taking form of the lifecycle hook
        raw = ctx.restore_checkpoint()
        self.state : Dict[str, Any] = (json.loads(raw.decode()) if raw
                                       else {'totals': {}, 'applied': [], 'revision': 0})
        self.restored_from = json.loads(raw.decode()) if raw else None      # a copy: the state mutates on
        self.invocations : List[Dict[str, Any]] = []

    def process(self, record : Any, ctx : RuntimeContext) -> Any:  # type: ignore[override]  # one positional per parent
        key, value = str(record['key']), int(record['value'])
        member = ctx.input_key
        if member in self.state['applied']:
            # A redelivery this process already applied: state is idempotent by member id.
            return self._event(key, member, replayed = True)
        self.state['totals'][key] = self.state['totals'].get(key, 0) + value
        self.state['applied'].append(member)
        self.state['revision'] += 1
        ctx.checkpoint(json.dumps(self.state, sort_keys = True).encode())
        return self._event(key, member, replayed = False)

    def _event(self, key : str, member : Optional[str], replayed : bool) -> Dict[str, Any]:
        event = {'key': key, 'total': self.state['totals'][key], 'revision': self.state['revision'],
                 'input': member, 'replayed': replayed}
        self.invocations.append(event)
        if self._log_path:
            JsonlLog(self._log_path).append(dict(event, node = self.name))
        return event


class StochasticInference(ProcessorNode):
    '''
    RUN-004's deliberately stochastic component: a fresh nonce on every
    invocation, stateless. Declares itself nondeterministic with a committed
    replay policy: a result committed before a crash is replayed byte-for-byte,
    never recomputed.
    '''
    deterministic = False
    replay_policy = 'committed'

    def __init__(self, log_path : Optional[str] = None, name : Optional[str] = None, **kwargs : Any) -> None:
        self._log_path = log_path
        super().__init__(name = name, **kwargs)

    def open(self) -> None:
        self.invocations : List[Dict[str, Any]] = []

    def process(self, record : Any, ctx : RuntimeContext) -> Any:  # type: ignore[override]  # one positional per parent
        nonce = uuid.uuid4().hex
        out = {'input': ctx.input_key, 'value': record, 'nonce': nonce}
        self.invocations.append(out)
        if self._log_path:
            JsonlLog(self._log_path).append(dict(out, node = self.name))
        return out


class RecomputingStochastic(StochasticInference):
    '''The same component under the default policy: a redelivery is recomputed (a different nonce).'''
    replay_policy = 'recompute'


# -- sources -------------------------------------------------------------------------------

class LiveSource(ProducerNode):
    '''
    A live camera (RUN-014): no replayable offset, a local frame counter that
    restarts from 0 in every process, and a capture payload that names the
    process that captured it (``capture_seed``) — a replacement's frames are
    genuinely new content under the same local sequence numbers.
    '''
    def __init__(self, frames : int = 5, capture_seed : str = 'a', log_path : Optional[str] = None,
                 name : Optional[str] = None, **kwargs : Any) -> None:
        self._frames = frames
        self._capture_seed = capture_seed
        self._log_path = log_path
        kwargs.setdefault('is_finite', True)
        super().__init__(name = name, **kwargs)

    def open(self) -> None:
        self._local = 0

    def next(self) -> Any:
        if self._local >= self._frames:
            raise StopIteration()
        frame = {'local_seq': self._local, 'capture': f'{self._capture_seed}:{self._local}', 'pid': os.getpid()}
        self._local += 1
        if self._log_path:
            JsonlLog(self._log_path).append(dict(frame, node = self.name, op = 'capture'))
        return frame


class ReplayableSource(ProducerNode):
    '''
    A finite media file (RUN-015): stable frame offsets, ``seek`` to resume
    after the last accepted one, a payload derived from the media and the
    offset alone — so a replayed offset is byte-identical to its first read.
    '''
    replayable = True

    def __init__(self, frames : int = 6, media : str = 'clip', log_path : Optional[str] = None,
                 name : Optional[str] = None, **kwargs : Any) -> None:
        self._frames = frames
        self._media = media
        self._log_path = log_path
        kwargs.setdefault('is_finite', True)
        super().__init__(name = name, **kwargs)

    def open(self) -> None:
        self._position = 0
        self.seeks : List[int] = []

    def seek(self, offset : int) -> None:
        self._position = offset
        self.seeks.append(offset)
        if self._log_path:
            JsonlLog(self._log_path).append({'node': self.name, 'op': 'seek', 'offset': offset})

    def next(self) -> Any:
        if self._position >= self._frames:
            raise StopIteration()
        self._position += 1
        frame = {'offset': self._position, 'media': self._media, 'payload': f'{self._media}#{self._position}'}
        if self._log_path:
            JsonlLog(self._log_path).append(dict(frame, node = self.name, op = 'read'))
        return frame


class ReplayableSourceV2(ReplayableSource):
    '''The same media under a deliberately new analysis version (its own identity namespace).'''
    analysis_version = 'v2'


# -- the ordered tracker (RUN-021) -----------------------------------------------------------

class Tracker(ProcessorNode):
    '''
    A deterministic per-camera tracker: records are applied in the order the
    declared ``OrderingPolicy`` releases them (a ``ReorderBuffer`` per camera),
    the state is the applied sequence per camera, and late/dropped records are
    accounted for explicitly. Buffer positions travel in the checkpoint, so a
    replacement resumes the declared ordering where the crashed process left it.
    '''
    def __init__(self, ordering : Optional[Dict[str, Any]] = None, nb_tasks : int = 1,
                 partition_by : Optional[str] = 'camera_id', name : Optional[str] = None, **kwargs : Any) -> None:
        self._ordering = ordering
        super().__init__(nb_tasks = nb_tasks, partition_by = partition_by, name = name, **kwargs)

    def open(self, ctx : RuntimeContext) -> None:  # type: ignore[override]  # the ctx-taking form of the lifecycle hook
        self.policy = OrderingPolicy.from_dict(self._ordering)
        raw = ctx.restore_checkpoint()
        doc = json.loads(raw.decode()) if raw else {'state': {}, 'late': [], 'inputs': [], 'buffers': {}}
        self.state : Dict[str, List[Dict[str, Any]]] = doc['state']
        self.late : List[Dict[str, Any]] = doc['late']
        self.inputs : List[Dict[str, Any]] = doc['inputs']
        self.buffers : Dict[str, ReorderBuffer] = {cam: ReorderBuffer.restore(self.policy, snap)
                                                   for cam, snap in doc['buffers'].items()}
        self.restored = raw is not None

    def process(self, record : Any, ctx : RuntimeContext) -> Any:  # type: ignore[override]  # one positional per parent
        camera, seq = str(record['camera_id']), int(record['seq'])
        self.inputs.append({'camera': camera, 'seq': seq, 'input': ctx.input_key})
        buffer = self.buffers.get(camera)
        if buffer is None:
            buffer = self.buffers[camera] = ReorderBuffer(self.policy)
        transitions = []
        for released_seq, released, late in buffer.offer(seq, record):
            if late:
                self.late.append({'camera': camera, 'seq': released_seq, 'outcome': 'marked'})
                continue
            self.state.setdefault(camera, []).append({'seq': released_seq, 'value': released.get('value')})
            transitions.append({'camera': camera, 'seq': released_seq})
        dropped = buffer.dropped - sum(1 for l in self.late if l['camera'] == camera and l['outcome'] == 'dropped')
        for _ in range(dropped):
            self.late.append({'camera': camera, 'seq': seq, 'outcome': 'dropped'})
        ctx.checkpoint(json.dumps({'state': self.state, 'late': self.late, 'inputs': self.inputs,
                                   'buffers': {cam: b.snapshot() for cam, b in self.buffers.items()}},
                                  sort_keys = True).encode())
        return {'camera': camera, 'seq': seq, 'transitions': transitions}


# -- sinks (RUN-017) --------------------------------------------------------------------------

class KeyedSink(ConsumerNode):
    '''A sink that applies its effect through the external system's idempotency key (``ctx.input_key``).'''
    effect_guarantee = EFFECT_IDEMPOTENT_KEY

    def __init__(self, ledger_path : str, actor : str = 'keyed', idempotent : bool = True,
                 name : Optional[str] = None, **kwargs : Any) -> None:
        self._ledger_path = ledger_path
        self._actor = actor
        super().__init__(idempotent = idempotent, name = name, **kwargs)

    def consume(self, record : Any, ctx : RuntimeContext) -> None:  # type: ignore[override]  # one positional per parent
        assert ctx.input_key is not None
        ExternalLedger(self._ledger_path).apply_once(ctx.input_key, _value_of(record), self._actor)


class PlainSink(ConsumerNode):
    '''A sink whose effect is a plain increment: at-least-once by declaration (the default).'''
    def __init__(self, ledger_path : str, actor : str = 'plain', idempotent : bool = True,
                 name : Optional[str] = None, **kwargs : Any) -> None:
        self._ledger_path = ledger_path
        self._actor = actor
        super().__init__(idempotent = idempotent, name = name, **kwargs)

    def consume(self, record : Any) -> None:  # type: ignore[override]  # one positional per parent
        ExternalLedger(self._ledger_path).increment(_value_of(record), self._actor)


def _value_of(record : Any) -> int:
    '''The amount a sink record carries: a bare integer, or a ``{'value': n}`` mapping.'''
    return int(record['value']) if isinstance(record, dict) else int(record)


# -- partitioned stages (RUN-020) -----------------------------------------------------------------

class RejectingStage(ProcessorNode):
    '''A camera stage requiring a string ``camera_id``; an unusable key is rejected (the default policy).'''
    def __init__(self, nb_tasks : int = 3, name : Optional[str] = None, **kwargs : Any) -> None:
        kwargs.setdefault('partition_by', 'camera_id')
        super().__init__(nb_tasks = nb_tasks, name = name, **kwargs)


class FallbackStage(ProcessorNode):
    '''The same stage declaring a fallback partition for unusable keys.'''
    partition_key_policy = {'invalid': 'fallback', 'fallback_partition': 2}

    def __init__(self, nb_tasks : int = 3, name : Optional[str] = None, **kwargs : Any) -> None:
        kwargs.setdefault('partition_by', 'camera_id')
        super().__init__(nb_tasks = nb_tasks, name = name, **kwargs)
