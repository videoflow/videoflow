'''
Conformance cases: RUN-020, RUN-021, RUN-023, RUN-034.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Four cases about *who owns a record*: an unusable partition
key follows a declared policy instead of hashing as ``"None"`` (RUN-020); a keyed
stateful node applies records under its declared ordering whatever the arrival order
(RUN-021); a superseded owner cannot commit, and a checkpoint transfer is truthfully
unsupported (RUN-023); a non-owner replica decides from metadata and never fetches a
body (RUN-034, on PAY-018's rig). The oracles are plain functions over a rig
(``_runs2``, ``_payloads``) so the paired negative controls (``defects_run2.py``,
``defects_pay.py``) run the same assertions against the reviewed defects.
'''
from __future__ import absolute_import, division, print_function

import collections
import hashlib
import itertools
import json
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Sequence

import defects
import defects_run2
import pytest
from _payloads import CountingStore, KeyRecordingStore, ReadLog, Receiver, frame_array, object_exists
from _runnodes import Accumulator, FallbackStage, RejectingStage, Tracker
from _runs2 import (
    Collector,
    ContextSchedule,
    JetStreamRig,
    TaskThread,
    allow_task_threads,
    crash_at,
    data_ids,
    ledger,
    memory_rig,
    messenger_for,
    outbox_snapshot,
    output_commit,
    processor_task,
    provision_run,
    publication_id,
    reference_totals,
    runtime_for,
    settling,
    spec,
    wait_until,
    write_evidence,
)
from _status import unsupported

from videoflow.backends import faults
from videoflow.backends.capabilities import RuntimeCapabilities
from videoflow.backends.runtime import RuntimeStore
from videoflow.core import constants
from videoflow.core.constants import BATCH
from videoflow.core.errors import StaleAuthority
from videoflow.core.policies import LATE_MARK, ORDER_SEQUENCE, OrderingPolicy
from videoflow.messaging import nats_messenger
from videoflow.wire.serialization import serialized_payload_size

pytestmark = pytest.mark.timeout(180)


# -- RUN-020 ----------------------------------------------------------------------

REPLICAS_020 = 3
CAMERAS = ('cam-A', 'cam-B', 'cam-C')
#: The invalid-key fixtures: absent, null, empty, and three wrong types.
INVALID_KEYS : List[Any] = ['<absent>', None, '', ['cam-A'], {'id': 'cam-A'}, b'cam-A']


def _owner_of(key : Any, replicas : int) -> int:
    return int(hashlib.sha256(str(key).encode('utf-8')).hexdigest()[:8], 16) % replicas


def _records_020() -> List[Dict[str, Any]]:
    '''Valid camera histories interleaved with the invalid fixtures, each record a distinct source offset.'''
    out : List[Dict[str, Any]] = []
    seqs = {cam: 0 for cam in CAMERAS}
    pending = list(INVALID_KEYS)
    for round_ in range(4):
        for cam in CAMERAS:
            seqs[cam] += 1
            out.append({'camera_id': cam, 'seq': seqs[cam], 'valid': True})
        if pending:
            out.append({'camera_id': pending.pop(0), 'seq': round_, 'valid': False})
    for i, bad in enumerate(pending):
        out.append({'camera_id': bad, 'seq': 9 + i, 'valid': False})
    return out


class _Replica(threading.Thread):
    '''One replica of the stage: receives, records every record it was handed, acks; stops on end of stream or on request.'''
    def __init__(self, rig : Any, store : Any, node_factory : Callable[[], Any], replica_id : int, run_id : str) -> None:
        super().__init__(name = f'stage-p{replica_id}', daemon = True)
        self.replica_id = replica_id
        self.node = node_factory()
        self.runtime = runtime_for(store, rig, 'stage', replica_id = replica_id, nb_tasks = REPLICAS_020,
                                   partition_by = 'camera_id', parent_replicas = {'src': 1}, run_id = run_id)
        self.messenger = messenger_for(rig, self.node, ['src'], runtime = self.runtime, run_id = run_id,
                                       replica_id = replica_id)
        self.handled : List[Dict[str, Any]] = []
        self.error : Optional[BaseException] = None
        self.ended_by = ''
        self._stop_requested = threading.Event()

    def run(self) -> None:
        try:
            while not self._stop_requested.is_set():
                inputs = self.messenger.receive_message()
                entry = inputs['src']
                if entry.get('is_stop_signal'):
                    self.ended_by = 'end-of-stream'
                    return
                info = (self.messenger.last_input_info() or {}).get('src') or {}
                self.handled.append({'record': entry['message'], 'metadata': entry.get('metadata'),
                                     'trace_id': info.get('trace_id'), 'seq': info.get('seq'),
                                     'replica': self.replica_id})
                self.messenger.ack_inputs()
            self.ended_by = 'stopped'
        except BaseException as e:  # noqa: BLE001 — surfaced to the test thread
            self.error = e
            self.ended_by = type(e).__name__

    def stop(self) -> None:
        self._stop_requested.set()
        self.messenger.quiesce()


def _oracle_run_020(rig : Any, store_for : Callable[[str], Any], evidence : Dict[str, Any]) -> None:
    '''
    Two runs over the same records — three valid camera histories interleaved
    with six invalid keys — on three partitioned replicas: one with the reject
    policy (an invalid record is dead-lettered by replica 0 as
    ``VF_POISON_PARTITION_KEY``, after a retry when its first dead-letter publish
    failed), one with a declared fallback partition (every invalid record goes
    to replica 2 and is counted). In both, one owner is restarted mid-stream.
    '''
    records = _records_020()
    valid = [r for r in records if r['valid']]
    invalid = [r for r in records if not r['valid']]
    assert len(invalid) == len(INVALID_KEYS)
    for policy, node_cls in (('reject', RejectingStage), ('fallback', FallbackStage)):
        _run_020_policy(rig, store_for(policy), policy, node_cls, records, valid, invalid, evidence)


def _run_020_policy(rig : Any, store : Any, policy : str, node_cls : type, records : List[Dict[str, Any]],
                    valid : List[Dict[str, Any]], invalid : List[Dict[str, Any]], evidence : Dict[str, Any]) -> None:
    '''One policy's run: three replicas, one restarted mid-stream, the records published in two halves.'''
    run_id = f'{rig.run_id}-{policy}'
    provision_run(rig, run_id, [spec('src', [], 'producer', True),
                                spec('stage', ['src'], 'processor', False, nb_tasks = REPLICAS_020,
                                     partition_by = 'camera_id')])
    src = rig.messenger('src', [], run_id = run_id, replayable = True)
    ids = data_ids(rig, 'src', len(records), run_id)
    factory = lambda cls = node_cls: cls(nb_tasks = REPLICAS_020, name = 'stage')  # noqa: E731
    replicas = {r: _Replica(rig, store, factory, r, run_id) for r in range(REPLICAS_020)}
    for replica in replicas.values():
        replica.start()
    # The first invalid records' dead-letter publishes must fail once: the stage's
    # own channel (where the model keeps dead letters) accepts nothing for a while.
    if policy == 'reject':
        rig.backend.pause_acceptance(rig.channel('stage', run_id))
    half = len(records) // 2
    for record in records[:half]:
        metadata = {} if record['camera_id'] == '<absent>' else {'camera_id': record['camera_id']}
        src.publish_message(record, metadata)
    handled_in_first_half = sum(1 for r in records[:half] if r['valid'] or policy == 'fallback')
    assert wait_until(lambda: sum(len(r.handled) for r in replicas.values()) >= handled_in_first_half, 60)
    # Restart one owner mid-stream: replica 1's replacement keeps its partition.
    restarted = 1
    replicas[restarted].stop()
    replicas[restarted].join(30)
    first_half_by_restarted = list(replicas[restarted].handled)
    replacement = _Replica(rig, store, factory, restarted, run_id)
    replacement.start()
    if policy == 'reject':
        rig.backend.resume_acceptance(rig.channel('stage', run_id))
    for record in records[half:]:
        metadata = {} if record['camera_id'] == '<absent>' else {'camera_id': record['camera_id']}
        src.publish_message(record, metadata)
    src.publish_stop_signal()
    survivors = [r for k, r in replicas.items() if k != restarted] + [replacement]
    for replica in survivors:
        replica.join(90)
        assert not replica.is_alive(), f'{replica.name} never saw the end of stream ({replica.error!r})'
    handled = [h for r in replicas.values() if r is not replicas[restarted] for h in r.handled] + \
        first_half_by_restarted + list(replacement.handled)
    dead = rig.dead_letters('stage', run_id = run_id)
    per_key : Dict[str, List[Dict[str, Any]]] = collections.defaultdict(list)
    for h in handled:
        per_key[str(h['record']['camera_id'])].append({'seq': h['record']['seq'], 'replica': h['replica'],
                                                        'valid': h['record']['valid']})
    stats = {f'p{r}': dict(replicas[r].messenger.publication_stats) for r in replicas}
    stats[f'p{restarted}-replacement'] = dict(replacement.messenger.publication_stats)
    attempts = {ids[i][:8]: runtime_for(store, rig, 'stage', run_id = run_id).attempts_for(ids[i])
                for i, r in enumerate(records) if not r['valid']}
    evidence[policy] = {
        'per_key': dict(per_key), 'dead_letters': [{'code': d.headers.get('VF-Code'), 'disposition': d.headers.get('VF-Disposition'),
                                                    'num_delivered': d.headers.get('VF-Num-Delivered')} for d in dead],
        'publication_stats': stats, 'attempts': attempts,
        'restarted_owner': {'replica': restarted, 'before': len(first_half_by_restarted), 'after': len(replacement.handled)},
        'replicas_ended_by': {r.name: r.ended_by for r in survivors},
    }
    # Valid keys: every record handled exactly once, by the owner its hash names,
    # in order — the restart moved nothing to another partition.
    for cam in CAMERAS:
        seen = sorted(per_key[cam], key = lambda h: h['seq'])
        assert [h['seq'] for h in seen] == list(range(1, 5)), (cam, seen)
        assert {h['replica'] for h in seen} == {_owner_of(cam, REPLICAS_020)}, (cam, seen)
    assert sum(len(per_key[cam]) for cam in CAMERAS) == len(valid)
    invalid_handled = [h for h in handled if not h['record']['valid']]
    if policy == 'reject':
        # Every invalid record has one traceable terminal outcome: a dead letter
        # under the partition-key code, from replica 0 — and none was processed.
        assert not invalid_handled, invalid_handled
        assert len(dead) == len(invalid), (len(dead), len(invalid))
        assert all(d.headers.get('VF-Code') == 'VF_POISON_PARTITION_KEY' for d in dead), dead
        # The retried ones: an invalid record whose first dead-letter publish
        # failed was kept (never dropped) and dead-lettered on its second attempt;
        # the attempt ledger says so, and every other one took exactly one.
        retried = {ids[i][:8] for i, r in enumerate(records[:half]) if not r['valid']}
        assert retried and all(attempts[key] == 2 for key in retried), (retried, attempts)
        assert all(count == 1 for key, count in attempts.items() if key not in retried), attempts
    else:
        # A declared fallback: every invalid record went to replica 2, was counted
        # as a fallback (observable), and nothing was dead-lettered.
        assert len(invalid_handled) == len(invalid), invalid_handled
        assert {h['replica'] for h in invalid_handled} == {2}, invalid_handled
        assert dead == [], dead
        fallbacks = stats['p2'].get('partition_fallbacks', 0)
        assert fallbacks == len(invalid), (fallbacks, len(invalid))
        assert 'partition_fallbacks' not in stats['p0'] and 'partition_fallbacks' not in stats['p1'], stats
    # Whatever the policy, an invalid key never rode the normal distribution as
    # the string "None" (or "", "[]", ...): no replica but the declared one saw one.
    for h in invalid_handled:
        assert h['replica'] == 2 and policy == 'fallback', h
    # Every replica reached the end of stream cleanly — including the siblings
    # of the restarted owner, whose completion commits must not be fenced by
    # another partition's epoch.
    for replica in survivors:
        assert replica.error is None, \
            f'{replica.name} ended with {replica.error!r} — FlowRuntime.commit_completion keeps one completion ' \
            f'record per parent and compares the committing replica\'s epoch with whichever partition wrote it ' \
            f'last, so a restarted sibling (epoch 2 on its own partition) fences every other replica (epoch 1)'


@pytest.mark.case('RUN-020')
@pytest.mark.level('process')
def test_run_020_missing_and_malformed_partition_keys_follow_an_explicit(tmp_path, evidence_dir, monkeypatch) -> None:
    '''
    RUN-020 (P1, runtime, process): Missing and malformed partition keys follow an explicit
    policy.

    Acceptance: Every invalid-key fixture follows its declared policy and no implicit all-
    invalid hotspot is accepted as normal distribution.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    rig = memory_rig([spec('src', [], 'producer', True),
                      spec('stage', ['src'], 'processor', False, nb_tasks = REPLICAS_020, partition_by = 'camera_id')])
    try:
        with rig.ticking():
            _oracle_run_020(rig, lambda label: ledger(tmp_path, label), evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'partition_decisions.json', evidence)


@pytest.mark.negative_control(of = 'RUN-020')
def test_run_020_detects_keys_hashed_as_their_str(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    defects_run2.hash_str_of_anything(monkeypatch)
    rig = memory_rig([spec('src', [], 'producer', True),
                      spec('stage', ['src'], 'processor', False, nb_tasks = REPLICAS_020, partition_by = 'camera_id')])
    try:
        with rig.ticking():
            assert defects.detects(_oracle_run_020, rig, lambda label: ledger(tmp_path, label), {})
    finally:
        rig.close()


# -- RUN-021 ----------------------------------------------------------------------

ORDERING = {'mode': ORDER_SEQUENCE, 'horizon': 1, 'late': LATE_MARK}
CAMERA_A = [1, 3, 2, 3]            # the catalog's sequence for camera A: a gap, a late fill, a duplicate
CAMERA_B = [1, 2, 3]


def _reference(policy : OrderingPolicy, arrivals : Sequence[tuple], start : int = 1) -> list:
    '''An independent interpreter of the declared semantics (the one ``test_runtime_policies`` pins the buffer to).'''
    if policy.mode != ORDER_SEQUENCE:
        seen, out = set(), []
        for seq, rec in arrivals:
            if seq not in seen:
                seen.add(seq)
                out.append((seq, rec, False))
        return out
    applied, held, out, expected = set(), {}, [], start
    for seq, rec in arrivals:
        if seq in applied or seq in held:
            continue
        if seq < expected:
            applied.add(seq)
            if policy.late == LATE_MARK:
                out.append((seq, rec, True))
            continue
        held[seq] = rec
        while True:
            while expected in held:
                out.append((expected, held.pop(expected), False))
                applied.add(expected)
                expected += 1
            if held and len(held) > policy.horizon:
                expected = min(held)
                continue
            break
    return out


def _expected_state(policy : OrderingPolicy, arrivals_by_camera : Dict[str, List[int]]) -> Dict[str, Any]:
    '''What the tracker's state and late log must be after ``arrivals_by_camera`` (records never flushed).'''
    state : Dict[str, List[Dict[str, Any]]] = {}
    late : List[Dict[str, Any]] = []
    for camera, seqs in arrivals_by_camera.items():
        for seq, rec, is_late in _reference(policy, [(s, {'camera_id': camera, 'seq': s, 'value': f'{camera}#{s}'}) for s in seqs]):
            if is_late:
                late.append({'camera': camera, 'seq': seq, 'outcome': 'marked' if policy.late == LATE_MARK else 'dropped'})
            else:
                state.setdefault(camera, []).append({'seq': seq, 'value': rec['value']})
    return {'state': state, 'late': late}


def _tracker_specs(nb_tasks : int) -> List[Any]:
    return [spec('src', [], 'producer', True),
            spec('tracker', ['src'], 'processor', True, nb_tasks = nb_tasks, partition_by = 'camera_id'),
            spec('sink', ['tracker'], 'consumer', False)]


def _run_tracker(rig : Any, store : Any, run_id : str, replica_id : int, nb_tasks : int, schedule : Any,
                 ordering : Dict[str, Any]) -> TaskThread:
    node = Tracker(ordering = ordering, nb_tasks = nb_tasks, name = 'tracker')
    runtime = runtime_for(store, rig, 'tracker', replica_id = replica_id, nb_tasks = nb_tasks,
                          partition_by = 'camera_id', parent_replicas = {'src': 1}, run_id = run_id)
    messenger = messenger_for(rig, node, ['src'], runtime = runtime, run_id = run_id, replica_id = replica_id)
    task = TaskThread(processor_task(rig, node, messenger, ['src'], replica_id = replica_id, run_id = run_id),
                      name = f'tracker-p{replica_id}')
    task.node = node  # type: ignore[attr-defined]
    task.runtime = runtime  # type: ignore[attr-defined]
    if schedule is not None:
        schedule.install()
    task.start()
    return task


def _final_state(store : Any, rig : Any, run_id : str, replicas : int) -> Dict[str, Any]:
    '''The checkpointed tracker state, merged over its replicas (each owns its cameras).'''
    state : Dict[str, Any] = {}
    late : List[Dict[str, Any]] = []
    inputs : List[Dict[str, Any]] = []
    for replica in range(replicas):
        raw, _position = runtime_for(store, rig, 'tracker', replica_id = replica, nb_tasks = replicas,
                                     partition_by = 'camera_id', run_id = run_id).restore_checkpoint()
        if raw is None:
            continue
        doc = json.loads(raw.decode())
        state.update(doc['state'])
        late.extend(doc['late'])
        inputs.extend(doc['inputs'])
    return {'state': state, 'late': late, 'inputs': inputs}


def _oracle_run_021(rig : Any, store_for : Callable[[str], Any], evidence : Dict[str, Any],
                    crash : Callable[[], faults.RaiseError] = crash_at) -> List[Any]:
    '''
    Run 1, for every distinct arrival permutation of camera A's records (1, 3, 2
    and a duplicate 3) interleaved with camera B, on two partitioned replicas:
    the tracker's state and late log equal the reference interpreter's. Run 2:
    sequence 2 arrives past the horizon, the worker dies before acknowledging it
    (``settle.before``), and the replacement sees it again — accounted once.
    '''
    policy = OrderingPolicy.from_dict(ORDERING)
    permutations = sorted(set(itertools.permutations(CAMERA_A)))
    evidence['permutations'] = []
    for index, order in enumerate(permutations):
        run_id = f'{rig.run_id}-perm{index}'
        provision_run(rig, run_id, _tracker_specs(2))
        store = store_for(f'perm{index}')
        sink = Collector(rig, 'tracker', run_id = run_id)
        sink.start()
        tasks = [_run_tracker(rig, store, run_id, r, 2, None, ORDERING) for r in range(2)]
        src = rig.messenger('src', [], run_id = run_id, replayable = True)
        # Camera A in this order, camera B interleaved: one source offset per record.
        interleaved : List[tuple] = []
        b = iter(CAMERA_B)
        for seq in order:
            interleaved.append(('cam-A', seq))
            nxt = next(b, None)
            if nxt is not None:
                interleaved.append(('cam-B', nxt))
        for camera, seq in interleaved:
            src.publish_message({'camera_id': camera, 'seq': seq, 'value': f'{camera}#{seq}'}, {'camera_id': camera})
        src.publish_stop_signal()
        for task in tasks:
            task.wait(60)
            assert task.error is None, (order, task.error)
        sink.finish(60)
        expected = _expected_state(policy, {'cam-A': list(order), 'cam-B': CAMERA_B})
        got = _final_state(store, rig, run_id, 2)
        applied_order = [t for r in sorted(sink.snapshot(), key = lambda r: r['seq']) for t in r['message']['transitions']]
        evidence['permutations'].append({'order': list(order), 'state': got['state'], 'late': got['late'],
                                         'applied_order': applied_order})
        assert got['state'] == expected['state'], (order, got['state'], expected['state'])
        assert sorted(got['late'], key = str) == sorted(expected['late'], key = str), (order, got['late'], expected['late'])
        # The applied order the sink saw is the reference's, per camera.
        for camera in ('cam-A', 'cam-B'):
            assert [t['seq'] for t in applied_order if t['camera'] == camera] == \
                [s['seq'] for s in expected['state'].get(camera, [])], (order, applied_order)
        assert [i['seq'] for i in got['inputs'] if i['camera'] == 'cam-A'] == list(order)
    # Run 2: 2 arrives past the horizon; the worker dies before acknowledging it.
    run_id = f'{rig.run_id}-late'
    provision_run(rig, run_id, _tracker_specs(1))
    store = store_for('late')
    sink = Collector(rig, 'tracker', run_id = run_id)
    sink.start()
    src = rig.messenger('src', [], run_id = run_id, replayable = True)
    late_order = [1, 3, 4, 2]
    schedule = ContextSchedule({'settle.before': faults.Nth(len(late_order), crash())},
                               {'settle.before': settling('tracker', data_ids(rig, 'src', len(late_order), run_id))})
    first = _run_tracker(rig, store, run_id, 0, 1, schedule, ORDERING)
    for seq in late_order:
        src.publish_message({'camera_id': 'cam-A', 'seq': seq, 'value': f'cam-A#{seq}'}, {'camera_id': 'cam-A'})
    first.wait(60)
    schedule.uninstall()
    assert first.crashed, f'the tracker did not die at the barrier: {first.error!r}'
    at_crash = _final_state(store, rig, run_id, 1)
    src.publish_stop_signal()
    replacement = _run_tracker(rig, store, run_id, 0, 1, None, ORDERING)
    replacement.wait(60)
    assert replacement.error is None, replacement.error
    sink.finish(60)
    got = _final_state(store, rig, run_id, 1)
    expected = _expected_state(policy, {'cam-A': late_order})
    evidence['late'] = {'order': late_order, 'at_crash': at_crash, 'final': got, 'restored': replacement.node.restored,  # type: ignore[attr-defined]
                        'replacement_inputs': list(replacement.node.inputs)}  # type: ignore[attr-defined]
    assert at_crash['state'] == expected['state'] and at_crash['late'] == expected['late'], at_crash
    assert got['state'] == expected['state'], (got['state'], expected['state'])
    assert got['late'] == expected['late'], (got['late'], expected['late'])
    # The redelivered late record was accounted for exactly once: the replacement
    # restored the checkpoint that already covered it and applied nothing twice.
    assert [i['seq'] for i in got['inputs']] == late_order, got['inputs']
    assert replacement.node.restored and replacement.node.inputs == at_crash['inputs']  # type: ignore[attr-defined]
    return [schedule]


@pytest.mark.case('RUN-021')
@pytest.mark.level('process')
def test_run_021_camera_state_follows_declared_ordering_under_delayed_and(tmp_path, evidence_dir, record_faults,
                                                                          monkeypatch) -> None:
    '''
    RUN-021 (P0, runtime, process): Camera state follows declared ordering under delayed and
    duplicate frames.

    Acceptance: Final state and late/drop outcomes match the declared reference policy for every
    delivery permutation.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    allow_task_threads(monkeypatch)
    evidence : Dict[str, Any] = {}
    rig = memory_rig(_tracker_specs(2))
    try:
        with rig.ticking():
            schedules = _oracle_run_021(rig, lambda label: ledger(tmp_path, label), evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'ordering.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.negative_control(of = 'RUN-021')
def test_run_021_detects_a_tracker_that_applies_in_arrival_order(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    allow_task_threads(monkeypatch)
    defects_run2.arrival_order_buffer(monkeypatch)
    rig = memory_rig(_tracker_specs(2))
    try:
        with rig.ticking():
            assert defects.detects(_oracle_run_021, rig, lambda label: ledger(tmp_path, label), {})
    finally:
        rig.close()


# -- RUN-023 ----------------------------------------------------------------------

class _PartitionedStore(RuntimeStore):
    '''
    The ownership service behind a network partition: every call raises while
    ``cut`` is set. Wraps the real store so the messaging side stays reachable.
    '''
    def __init__(self, inner : RuntimeStore) -> None:
        self._inner = inner
        self.cut = threading.Event()
        self.refused = 0

    def _check(self) -> None:
        if self.cut.is_set():
            self.refused += 1
            raise ConnectionError('ownership service unreachable (network partition)')

    def get(self, key : str) -> Any:
        self._check()
        return self._inner.get(key)

    def cas(self, key : str, expected_version : Any, value : bytes) -> bool:
        self._check()
        return self._inner.cas(key, expected_version, value)

    def append(self, log : str, record : bytes) -> int:
        self._check()
        return self._inner.append(log, record)

    def scan(self, prefix : str) -> Any:
        self._check()
        return self._inner.scan(prefix)

    def delete(self, key : str, expected_version : Any) -> bool:
        self._check()
        return self._inner.delete(key, expected_version)

    def log_entries(self, log : str) -> Any:
        self._check()
        return self._inner.log_entries(log)

    def capabilities(self) -> RuntimeCapabilities:
        return self._inner.capabilities()


RECORDS_023 = [{'key': 'k', 'value': v} for v in (1, 2, 3, 4, 5, 6)]


def _owner(rig : Any, store : Any, run_id : str, schedule : Any = None) -> Dict[str, Any]:
    '''One owner process of the accumulator's single partition: a node, its ledger, its messenger, its task.'''
    node = Accumulator(name = 'acc')
    runtime = runtime_for(store, rig, 'acc', parent_replicas = {'src': 1}, run_id = run_id)
    messenger = messenger_for(rig, node, ['src'], runtime = runtime, run_id = run_id)
    task = TaskThread(processor_task(rig, node, messenger, ['src'], run_id = run_id), name = f'owner-{runtime.current_epoch()}')
    if schedule is not None:
        schedule.install()
    task.start()
    return {'node': node, 'runtime': runtime, 'messenger': messenger, 'task': task, 'epoch': runtime.current_epoch()}


def _oracle_run_023(rig : Any, store : Any, evidence : Dict[str, Any]) -> List[Any]:
    '''
    The fencing half. Owner A (epoch 1) commits records 1 and 2 and is paused
    with record 3 in hand, before its commit. Owner B takes the partition (epoch
    2), starts from A's committed prefix, and commits the redelivered 3 and then
    4. A resumes: its commit is refused (``StaleAuthority``), it hands the input
    back unblamed and stops. Then owner C (epoch 3) loses its ownership service
    while its transport still works: it commits nothing; owner D (epoch 4) does.
    '''
    run_id = rig.run_id
    sink = Collector(rig, 'acc', run_id = run_id)
    sink.start()
    src = rig.messenger('src', [], run_id = run_id, replayable = True)
    history : List[Dict[str, Any]] = []

    def note(event : str, **facts : Any) -> None:
        history.append(dict(facts, event = event))

    for record in RECORDS_023[:3]:
        src.publish_message(record)
    schedule = ContextSchedule({'group.commit.before': faults.Nth(3, faults.Pause('old-owner', timeout_seconds = 120))},
                               {'group.commit.before': output_commit})
    a = _owner(rig, store, run_id, schedule)
    assert wait_until(lambda: schedule.fired().get('group.commit.before', 0) >= 3, 60), 'A never reached its third commit'
    note('A committed 1 and 2, paused before committing 3', epoch = a['epoch'], applied = len(a['node'].state['applied']))
    assert a['epoch'] == 1
    # The transfer: B takes the partition and starts from the committed prefix.
    b = _owner(rig, store, run_id)
    assert wait_until(lambda: 'restored_from' in vars(b['node']), 30), 'B never opened'
    note('B acquired the partition', epoch = b['epoch'], restored_applied = len(b['node'].restored_from['applied']),
         restored_totals = b['node'].restored_from['totals'])
    assert b['epoch'] == 2 and b['runtime'].current_epoch() == 2
    assert b['node'].restored_from['totals'] == reference_totals(RECORDS_023[:2])
    assert wait_until(lambda: sink.count() >= 3, 60), 'B never committed the redelivered record 3'
    src.publish_message(RECORDS_023[3])
    assert wait_until(lambda: sink.count() >= 4, 60), 'B never committed record 4'
    note('B committed 3 and 4', sink = sink.count())
    # A resumes with its buffered old input: refused at the commit, handed back, stopped.
    schedule.release('old-owner')
    schedule.uninstall()
    a['task'].wait(20)
    assert isinstance(a['task'].error, StaleAuthority), f'the stale owner was not fenced: {a["task"].error!r}'
    assert a['task'].error.disposition == 'worker_fatal'
    note('A resumed and was fenced', error = str(a['task'].error), disposition = a['task'].error.disposition)
    # B is unaffected by A's attempt and keeps going.
    src.publish_message(RECORDS_023[4])
    assert wait_until(lambda: sink.count() >= 5, 60)
    note('B committed 5', sink = sink.count())
    # C takes the partition — B is superseded but alive — and then loses its
    # ownership service while its transport still works. Record 6 reaches one of
    # them first: B is fenced at its commit, C cannot even record the delivery;
    # neither commits, both stop, and the record stays pending for D.
    partitioned = _PartitionedStore(store)
    c = _owner(rig, partitioned, run_id)
    assert c['epoch'] == 3
    partitioned.cut.set()
    note('C acquired the partition, then lost the ownership service', epoch = c['epoch'])
    src.publish_message(RECORDS_023[5])
    b['task'].wait(60)
    c['task'].wait(60)
    assert isinstance(b['task'].error, StaleAuthority), f'B was not fenced by C\'s takeover: {b["task"].error!r}'
    assert c['task'].error is not None, 'C kept running without its ownership service'
    assert sink.count() == 5, 'a superseded or partitioned owner committed record 6'
    assert partitioned.refused >= 1
    outbox_6 = [e for e in outbox_snapshot(runtime_for(store, rig, 'acc', run_id = run_id))
                if e['publication_id'] == publication_id(rig, 'acc', 'src:6', 6, run_id)]
    assert outbox_6 == [], f'a stale owner wrote an intent for record 6: {outbox_6}'
    note('B fenced, C committed nothing', b_ended_by = type(b['task'].error).__name__,
         c_ended_by = type(c['task'].error).__name__, refused_store_calls = partitioned.refused,
         transport_connected = True)
    partitioned.cut.clear()
    with contextlib_suppress():
        c['messenger'].close()
    # D: the current owner finishes the stream.
    d = _owner(rig, store, run_id)
    assert d['epoch'] == 4
    assert wait_until(lambda: sink.count() >= 6, 60), 'D never committed the pending record 6'
    src.publish_stop_signal()
    d['task'].wait(60)
    assert d['task'].error is None, d['task'].error
    sink.finish(60)
    received = sink.snapshot()
    by_seq = collections.Counter(r['seq'] for r in received)
    final_state, _ = runtime_for(store, rig, 'acc', run_id = run_id).restore_checkpoint()
    totals = json.loads(final_state.decode())['totals']
    note('D committed 6 and the stream ended', epoch = d['epoch'], sink = dict(by_seq), totals = totals)
    evidence.update({'history': history, 'final_epoch': runtime_for(store, rig, 'acc', run_id = run_id).current_epoch(),
                     'sink': [{'seq': r['seq'], 'total': r['message']['total'], 'revision': r['message']['revision']}
                              for r in received]})
    # The combined result is a single-owner history: every record once, the fold intact.
    assert dict(by_seq) == {i: 1 for i in range(1, len(RECORDS_023) + 1)}, by_seq
    assert totals == reference_totals(RECORDS_023), (totals, reference_totals(RECORDS_023))
    assert sorted(r['message']['revision'] for r in received) == list(range(1, len(RECORDS_023) + 1))
    return [schedule]


def contextlib_suppress() -> Any:
    import contextlib
    return contextlib.suppress(Exception)


@pytest.mark.case('RUN-023')
@pytest.mark.level('process')
def test_run_023_state_handoff_fences_old_owners_during_scale_and_network(tmp_path, evidence_dir, record_faults,
                                                                          monkeypatch) -> None:
    '''
    RUN-023 (P0, runtime, process): State handoff fences old owners during scale and network
    partitions.

    Acceptance: No stale owner commits after transfer, and the combined result matches a single-
    owner reference history.

    The fencing sub-assertions run and pass; the case then reports UNSUPPORTED,
    truthfully: no runtime store advertises ``elastic_state``, so a checkpoint
    transfer between partitions is not a capability this composition claims.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    allow_task_threads(monkeypatch)
    evidence : Dict[str, Any] = {}
    store = ledger(tmp_path)
    rig = memory_rig([spec('src', [], 'producer', True), spec('acc', ['src'], 'processor', True),
                      spec('sink', ['acc'], 'consumer', False)])
    try:
        with rig.ticking():
            schedules = _oracle_run_023(rig, store, evidence)
    finally:
        rig.close()
        caps = store.capabilities()
        evidence['capabilities'] = {'store': caps.store, 'elastic_state': caps.elastic_state,
                                    'fencing_sub_assertions': 'passed' if 'sink' in evidence else 'failed'}
        write_evidence(evidence_dir, 'ownership_epochs.json', evidence)
    for schedule in schedules:
        record_faults(schedule)
    assert not caps.elastic_state
    unsupported(f'checkpoint transfer between partitions is not advertised: RuntimeCapabilities.elastic_state is '
                f'False for the {caps.store!r} store (fencing sub-assertions passed: epochs 1..4, stale commit refused)')


@pytest.mark.negative_control(of = 'RUN-023')
def test_run_023_detects_an_unfenced_commit(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    allow_task_threads(monkeypatch)
    defects_run2.unfenced_commit(monkeypatch)
    rig = memory_rig([spec('src', [], 'producer', True), spec('acc', ['src'], 'processor', True),
                      spec('sink', ['acc'], 'consumer', False)])
    try:
        with rig.ticking():
            assert defects.detects(_oracle_run_023, rig, ledger(tmp_path), {})
    finally:
        rig.close()


# -- RUN-034 ----------------------------------------------------------------------

MEGAFRAME = (1000, 1000)          # the catalog's 1 MB reference body
FRAMES_034 = 24


def _owner_034(trace : str, replicas : int) -> int:
    '''The partition owner of a trace id — the messenger's own arithmetic, computed independently.'''
    return int(hashlib.sha256(trace.encode('utf-8')).hexdigest()[:8], 16) % replicas


def _specs_034(replicas : int) -> List[Any]:
    return [spec('parent', [], 'producer', True),
            spec('child', ['parent'], 'consumer', False, nb_tasks = replicas, partition_by = 'trace_id')]


def _workload_034(rig : Any, run_id : str, replicas : int, evidence : Dict[str, Any], label : str,
                  paused : Optional[int] = None) -> None:
    '''
    One finite unique-frame set over ``replicas`` partitioned replicas of the
    child, each reading through its own GET-counting store. The parent is a
    replayable source, so its trace ids (``parent:{i}``) — and therefore each
    frame's owner — are known before anything is delivered. With ``paused`` set,
    that replica starts only after every other replica has drained its frames.
    '''
    log = ReadLog()
    owner_of_key : Dict[str, int] = {}
    children : Dict[int, Any] = {}
    for r in range(replicas):
        store = CountingStore(rig.store, f'child/p{r}', log)
        children[r] = rig.messenger('child', ['parent'], store = store, replica_id = r, nb_tasks = replicas,
                                    partition_by = 'trace_id', run_id = run_id)
    publisher_store = KeyRecordingStore(rig.store, 'parent', log)
    parent = rig.messenger('parent', [], store = publisher_store, run_id = run_id, replayable = True,
                           blob_reader_ids = [f'child/p{r}' for r in range(replicas)])
    expected = collections.Counter(_owner_034(f'parent:{i}', replicas) for i in range(1, FRAMES_034 + 1))
    receivers = {r: Receiver(children[r], expected = expected[r], name = f'child/p{r}') for r in range(replicas)}
    started = time.monotonic()
    for r, receiver in receivers.items():
        if r != paused:
            receiver.start()
    for i in range(1, FRAMES_034 + 1):
        parent.publish_message(frame_array(MEGAFRAME, seed = i))
        owner_of_key[publisher_store.keys[-1]] = _owner_034(f'parent:{i}', replicas)
    for r, receiver in receivers.items():
        if r != paused:
            receiver.join(timeout = 120)
            assert receiver.error is None, f'{receiver.name}: {receiver.error!r}'
            assert not receiver.is_alive(), f'{receiver.name} did not finish: {len(receiver.inputs)} of {receiver.expected}'
    if paused is not None:
        # The paused non-owner resumes: it decides every parked frame from
        # metadata, fetches only its own, and the others are unaffected.
        receivers[paused].start()
        receivers[paused].join(timeout = 120)
        assert receivers[paused].error is None, receivers[paused].error
        assert not receivers[paused].is_alive(), 'the resumed replica did not finish'
    took = time.monotonic() - started
    parent.publish_stop_signal()
    drains = [threading.Thread(target = child.receive_message, name = f'drain-p{r}', daemon = True)
              for r, child in children.items()]
    for drain in drains:
        drain.start()
    for drain in drains:
        drain.join(timeout = 60)
    assert not any(d.is_alive() for d in drains), 'a replica did not reach end of stream'
    assert wait_until(lambda: not any(object_exists(rig.store, k) for k in publisher_store.keys), 30), \
        'objects still owned after every replica settled: a share was never released'
    reads = [r for r in log.records() if r.reader != 'parent']
    owner_reads = [r for r in reads if owner_of_key.get(r.key) == int(r.reader.rsplit('p', 1)[1])]
    non_owner_reads = [r for r in reads if r not in owner_reads]
    per_replica = {r: len(receivers[r].inputs) for r in range(replicas)}
    serialized = serialized_payload_size(frame_array(MEGAFRAME, seed = 1))
    useful = {}
    for r in owner_reads:
        if r.outcome == 'bytes':
            useful.setdefault(r.key, r.size)
    evidence[label] = {
        'replicas': replicas, 'paused_replica': paused, 'frames': FRAMES_034, 'frame_serialized_bytes': serialized,
        'per_replica': per_replica, 'expected_per_replica': dict(expected), 'owner_map': {k[-8:]: v for k, v in owner_of_key.items()},
        'owner_get_count': len(owner_reads), 'non_owner_get_count': len(non_owner_reads),
        'non_owner_get_bytes': sum(r.size for r in non_owner_reads), 'useful_fetch_bytes': sum(useful.values()),
        'reads_by_replica': log.by_reader(), 'completion_s': round(took, 3),
    }
    assert per_replica == dict(expected), (per_replica, dict(expected))
    assert not non_owner_reads, f'{len(non_owner_reads)} non-owner GET(s): {non_owner_reads[:3]}'
    assert len(useful) == FRAMES_034 and sum(useful.values()) == FRAMES_034 * serialized, (len(useful), sum(useful.values()))
    assert len(owner_reads) == FRAMES_034, f'{len(owner_reads)} owner GETs for {FRAMES_034} frames: a body was fetched twice'
    for r in range(replicas):
        state = rig.consumer_state('child', 'parent', replica = r, run_id = run_id)
        assert state.pending == 0 and state.unacked == 0, (r, state)


def _oracle_run_034(rig : Any, evidence : Dict[str, Any]) -> None:
    '''Four replicas, then eight, then eight with a non-owner paused through the workload.'''
    for label, replicas, paused in (('four', 4, None), ('eight', 8, None), ('eight-paused', 8, 5)):
        run_id = f'{rig.run_id}-{label}'
        provision_run(rig, run_id, _specs_034(replicas))
        _workload_034(rig, run_id, replicas, evidence, label, paused = paused)


@pytest.mark.case('RUN-034')
@pytest.mark.level('broker')
def test_run_034_nonowner_replicas_route_from_metadata_without_downloading(nats_url, redis_url, evidence_dir,
                                                                           monkeypatch) -> None:
    '''
    RUN-034 (P2, integration, broker): Nonowner replicas route from metadata without downloading
    image bodies.

    Acceptance: With no retries and one intended processing reader, each frame has one hydration
    regardless of replica count; duplicate full-body fetches by nonowners fail.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, 'MAX_INLINE_PAYLOAD_BYTES', 1024)     # every frame offloads
    evidence : Dict[str, Any] = {}
    rig = JetStreamRig(nats_url, BATCH, _specs_034(4), redis_url = redis_url, ack_wait = 30)
    try:
        _oracle_run_034(rig, evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'body_reads.json', evidence)


@pytest.mark.case('RUN-034')
@pytest.mark.level('broker')
@pytest.mark.variant('memory')
def test_run_034_memory_backends_route_from_metadata(evidence_dir, monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, 'MAX_INLINE_PAYLOAD_BYTES', 1024)
    evidence : Dict[str, Any] = {}
    rig = memory_rig(_specs_034(4), ack_wait = 30)
    try:
        with rig.ticking():
            _oracle_run_034(rig, evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'body_reads.json', evidence)


@pytest.mark.negative_control(of = 'RUN-034')
def test_run_034_detects_a_replica_that_hydrates_before_deciding_ownership(monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, 'MAX_INLINE_PAYLOAD_BYTES', 1024)
    defects_run2.hydrate_before_ownership(monkeypatch)
    rig = memory_rig(_specs_034(4), ack_wait = 30)
    try:
        with rig.ticking():
            assert defects.detects(_oracle_run_034, rig, {})
    finally:
        rig.close()
