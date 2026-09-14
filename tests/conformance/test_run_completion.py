'''
Conformance cases: RUN-007, RUN-008, RUN-009, RUN-010.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions over a *rig* (``_runs1``): the
memory variants run them on the in-memory backend, the broker primary (RUN-009) on
the compose broker with in-process messengers, the process primaries (RUN-007,
RUN-010) with real ``videoflow.runtime.worker`` subprocesses — parent replicas and
the downstream sink alike — driven through their environment, fault schedule
included (``ENV-16``). The paired negative control runs the same oracle on the
memory rig with the reviewed defect monkeypatched in (``defects_run1.py``).

What every case here turns on is the EOS-7 completion barrier over a durable,
shared ledger (a ``FileRuntimeStore``): terminators recorded per
``(parent, replica, kind)`` before they are acked, received sets aggregated in the
store, an ``Unknown`` broker observation that never reads as empty, and the
commit fenced by the child's ownership epoch.
'''
from __future__ import absolute_import, division, print_function

import json
import os
import threading
import time
from typing import Any, Dict, List

import defects
import defects_run1
import pytest
from _runs1 import (
    HISTORICAL_QUIESCENCE_MS,
    BrokerRig,
    ModelRig,
    Receiver,
    SimulatedCrash,
    ledger_snapshot,
    read_jsonl,
    run_worker,
    spawn_worker,
    spec,
    terminator_id,
    wait_until,
    wait_worker,
    worker_env,
    write_evidence,
)

from videoflow.backends import faults
from videoflow.backends.memory.runtime_store import FileRuntimeStore
from videoflow.backends.messaging import KIND_ABORT, KIND_EOS
from videoflow.backends.outcomes import Known, Unknown
from videoflow.backends.runtime import (
    COMPLETION_ABORTED,
    COMPLETION_DRAINING,
    COMPLETION_OPEN,
    COMPLETION_UNKNOWN,
    FlowRuntime,
)
from videoflow.core.errors import StaleAuthority, UpstreamAborted
from videoflow.core.task import raise_if_aborted

pytestmark = pytest.mark.timeout(180)

#: Longer than the historical quiescence window: an interval in which the old
#: drain rule would have declared the parent complete.
QUIET_INTERVAL_S = 3 * HISTORICAL_QUIESCENCE_MS / 1000.0
ABORT_ERROR = {'code': 'VF_DEVICE', 'message': 'replica 1: the device fell off the bus', 'disposition': 'worker_fatal'}


def _child_runtime(store_dir : str, rig : Any, replicas : int, node : str = 'child') -> FlowRuntime:
    return FlowRuntime(FileRuntimeStore(store_dir), rig.flow_id, rig.run_id, node, parent_replicas = {'parent': replicas})


def _parent_runtime(store_dir : str, rig : Any, replica_id : int, nb_tasks : int) -> FlowRuntime:
    return FlowRuntime(FileRuntimeStore(store_dir), rig.flow_id, rig.run_id, 'parent', replica_id, nb_tasks)


def _barrier_state(child : Any, parent : str = 'parent') -> str:
    return child._runtime.completion_state(parent, child._data_durable_name(parent), child._consumer_pending(parent),
                                           pending_halves = child._has_pending_from(parent))


def _input_ids(inputs : List[dict], parent : str = 'parent') -> List[str]:
    return [f"{g[parent]['message']['r']}:{g[parent]['message']['n']}" for g in inputs]


# -- RUN-007 --------------------------------------------------------------------------

N0, N1 = 5, 4


def _oracle_run_007_inprocess(rig : Any, store_dir : str, schedule : faults.FaultSchedule,
                              record : Dict[str, Any]) -> None:
    '''
    Replica 0 publishes N0 outputs and its EOS (``seq = N0``); replica 1 publishes
    N1 - 1, pauses at its final publish for longer than the historical quiescence
    window, then publishes it and its EOS (``seq = N1``). The child must not
    complete during the pause — one terminator and a quiet, empty durable are not
    completion — and must complete, cleanly, once both final sequences are met.
    '''
    rig.provision([spec('parent', [], 'producer', True, nb_tasks = 2), spec('child', ['parent'], 'consumer', False)],
                  ack_wait = 30)
    child = rig.messenger('child', ['parent'], runtime = _child_runtime(store_dir, rig, 2), ack_wait = 30,
                          eos_quiescence_ms = HISTORICAL_QUIESCENCE_MS)
    receiver = Receiver(child, name = 'run007-child')
    receiver.start()
    r0 = rig.messenger('parent', [], runtime = _parent_runtime(store_dir, rig, 0, 2), replica_id = 0, nb_tasks = 2)
    for i in range(N0):
        r0.publish_message({'r': 0, 'n': i})
    r0.publish_stop_signal()
    r1 = rig.messenger('parent', [], runtime = _parent_runtime(store_dir, rig, 1, 2), replica_id = 1, nb_tasks = 2)
    for i in range(N1 - 1):
        r1.publish_message({'r': 1, 'n': i})
    assert rig.until(lambda: len(receiver.inputs) == N0 + N1 - 1, 20), len(receiver.inputs)
    transitions : List[tuple[float, str]] = []

    def final_output() -> None:
        r1.publish_message({'r': 1, 'n': N1 - 1})
        r1.publish_stop_signal()
    with schedule:
        publisher = threading.Thread(target = final_output, name = 'run007-r1-final', daemon = True)
        publisher.start()
        try:
            assert wait_until(lambda: schedule.fired().get('publish.send.before', 0) >= 1, 10), 'replica 1 never paused'
            # The durable is empty and has been quiet for longer than the old window;
            # the child recorded replica 0's terminator; it must still be open.
            assert rig.until(lambda: child._runtime.terminators('parent') != [], 10)
            paused_at = time.monotonic()
            while time.monotonic() - paused_at < QUIET_INTERVAL_S:
                transitions.append((time.monotonic() - paused_at, _barrier_state(child)))
                time.sleep(0.1)
            assert receiver.terminal is None, 'the child completed on replica 0\'s EOS and a quiet interval alone'
            assert all(state == COMPLETION_OPEN for _t, state in transitions), transitions
            observed = child._consumer_pending('parent')
            assert isinstance(observed, Known) and observed.value == (0, 0), observed
            record['during_pause'] = {'inputs': len(receiver.inputs), 'observation': observed.value,
                                      'terminators': [(t.replica_id, t.kind, t.seq)
                                                      for t in child._runtime.terminators('parent')],
                                      'states': transitions}
        finally:
            released_at = time.monotonic()
            schedule.release('final')
            publisher.join(20)
    assert rig.until(lambda: receiver.terminal is not None, 20), \
        f'slow completion was mistaken for a stall: {_barrier_state(child)}, inputs={len(receiver.inputs)}'
    assert receiver.stopped_cleanly and receiver.error is None, (receiver.terminal, receiver.error)
    assert receiver.completed_at is not None and receiver.completed_at > released_at
    ids = _input_ids(receiver.inputs)
    assert sorted(ids) == sorted(f'0:{i}' for i in range(N0)) + sorted(f'1:{i}' for i in range(N1)), ids
    persisted = _child_runtime(store_dir, rig, 2)
    terminators = [(t.replica_id, t.kind, t.seq) for t in persisted.terminators('parent')]
    assert terminators == [(0, 'eos', N0), (1, 'eos', N1)], terminators
    assert persisted.completed_parents() == {'parent'}
    completion = persisted.store.get(persisted.key('completion', 'parent', persisted.partition_id()))[0]
    record['completion'] = {'terminators': terminators, 'record': completion.decode() if completion else None,
                            'completed_after_release_s': receiver.completed_at - released_at,
                            'sink_ids': ids}


@pytest.mark.case('RUN-007')
@pytest.mark.level('process')
def test_run_007_completion_waits_for_every_expected_replica_and_its_final(nats_url, tmp_path, evidence_dir,
                                                                            record_faults, monkeypatch) -> None:
    '''
    RUN-007 (P0, runtime, process): Completion waits for every expected replica and its final
    sequence.

    Acceptance: Successful downstream completion occurs only after both final-sequence
    obligations are resolved, including the deliberately delayed output.

    Real workers on the compose broker: two ``CountingProducer`` replicas of one
    parent and a ``FileSink`` child, all over one file ledger. Replica 1's
    schedule pauses its final publish (``publish.send.before``) until the test
    releases it through the shared marker directory.
    '''
    store_dir = str(tmp_path / 'ledger')
    sink_path = str(tmp_path / 'sink.jsonl')
    schedule = faults.FaultSchedule({'publish.send.before': faults.Nth(N1, faults.Pause('final', 120.0))},
                                    marker_dir = str(tmp_path / 'markers'))
    rig = BrokerRig(nats_url, 'run007')
    record : Dict[str, Any] = {'flow': rig.flow_id, 'run': rig.run_id}
    child = None
    try:
        rig.provision([spec('parent', [], 'producer', True, nb_tasks = 2, node_class = '_runs1.CountingProducer'),
                       spec('child', ['parent'], 'consumer', False, node_class = '_runs1.FileSink')], ack_wait = 30)
        child_env = worker_env('child', 'consumer', '_runs1.FileSink', nats_url, rig.flow_id, rig.run_id,
                               parents = ['parent'], params = {'path': sink_path}, parent_replicas = [2],
                               store_url = f'file://{store_dir}', ack_wait = 30)
        child = spawn_worker(child_env, str(evidence_dir / 'child.log'))
        r0_env = worker_env('parent', 'producer', '_runs1.CountingProducer', nats_url, rig.flow_id, rig.run_id,
                            has_children = True, params = {'count': N0}, replica_id = 0, nb_tasks = 2,
                            store_url = f'file://{store_dir}')
        r0 = run_worker(r0_env, str(evidence_dir / 'replica0.log'), timeout = 60)
        assert r0.exit_code == 0, r0
        r1_env = worker_env('parent', 'producer', '_runs1.CountingProducer', nats_url, rig.flow_id, rig.run_id,
                            has_children = True, params = {'count': N1}, replica_id = 1, nb_tasks = 2,
                            store_url = f'file://{store_dir}', schedule = schedule)
        r1 = spawn_worker(r1_env, str(evidence_dir / 'replica1.log'))
        assert wait_until(lambda: schedule.fired().get('publish.send.before', 0) >= 1, 30), 'replica 1 never paused'
        assert wait_until(lambda: len([r for r in read_jsonl(sink_path) if r['event'] == 'input']) == N0 + N1 - 1, 30)
        persisted = _child_runtime(store_dir, rig, 2)
        assert wait_until(lambda: persisted.terminators('parent') != [], 20), 'replica 0\'s terminator was not recorded'
        time.sleep(QUIET_INTERVAL_S)
        assert child.poll() is None, f'the child completed on replica 0\'s EOS and a quiet interval alone: {child.poll()}'
        assert persisted.completed_parents() == set(), 'a completion was committed during the pause'
        record['during_pause'] = {'sink_inputs': N0 + N1 - 1, 'consumer': rig.consumer_info('child', 'parent'),
                                  'terminators': [(t.replica_id, t.kind, t.seq) for t in persisted.terminators('parent')]}
        released_at = time.time()
        schedule.release('final')
        r1_out = wait_worker(r1, 60, str(evidence_dir / 'replica1.log'))
        assert r1_out.exit_code == 0, r1_out
        child_out = wait_worker(child, 60, str(evidence_dir / 'child.log'))
        assert child_out.exit_code == 0, f'slow completion was mistaken for a failure: {child_out}'
        lines = read_jsonl(sink_path)
        inputs = [r for r in lines if r['event'] == 'input']
        ids = sorted(f"{r['item']['r']}:{r['item']['n']}" for r in inputs)
        assert ids == sorted(f'0:{i}' for i in range(N0)) + sorted(f'1:{i}' for i in range(N1)), ids
        assert [r['event'] for r in lines][-1] == 'closed'
        terminators = [(t.replica_id, t.kind, t.seq) for t in persisted.terminators('parent')]
        assert terminators == [(0, 'eos', N0), (1, 'eos', N1)], terminators
        assert persisted.completed_parents() == {'parent'}
        completion = persisted.store.get(persisted.key('completion', 'parent', persisted.partition_id()))[0]
        assert completion is not None
        doc = json.loads(completion.decode())
        assert doc['final'] == N0 + N1 and doc['at'] > released_at, (doc, released_at)
        record['completion'] = {'terminators': terminators, 'record': doc, 'released_at': released_at,
                                'child': {'exit_code': child_out.exit_code, 'took_s': child_out.took_s}}
    finally:
        if child is not None and child.poll() is None:
            child.kill()
        rig.close()
        record['faults'] = schedule.fired()
        record['ledger'] = ledger_snapshot(store_dir) if os.path.isdir(store_dir) else {}
        write_evidence(evidence_dir, 'completion.json', record)
    record_faults(schedule)


@pytest.mark.case('RUN-007')
@pytest.mark.level('process')
@pytest.mark.variant('memory')
def test_run_007_memory_child_waits_for_the_delayed_replica(tmp_path, evidence_dir, record_faults, monkeypatch) -> None:
    schedule = faults.FaultSchedule({'publish.send.before': faults.Nth(1, faults.Pause('final', 60.0))})
    rig = ModelRig(auto_advance = 0.0)
    record : Dict[str, Any] = {}
    try:
        _oracle_run_007_inprocess(rig, str(tmp_path / 'ledger'), schedule, record)
    finally:
        rig.close()
        record['faults'] = schedule.fired()
        write_evidence(evidence_dir, 'completion.json', record)
    record_faults(schedule)


@pytest.mark.negative_control(of = 'RUN-007')
def test_run_007_detects_completion_on_the_first_terminator(tmp_path, monkeypatch) -> None:
    '''The historical drain completes on one replica's EOS plus 500 ms of quiet: the oracle must catch it.'''
    defects_run1.quiescence_completion(monkeypatch)
    schedule = faults.FaultSchedule({'publish.send.before': faults.Nth(1, faults.Pause('final', 60.0))})
    rig = ModelRig(auto_advance = 0.0)
    try:
        assert defects.detects(_oracle_run_007_inprocess, rig, str(tmp_path / 'ledger'), schedule, {})
    finally:
        rig.close()


# -- RUN-008 --------------------------------------------------------------------------

def _oracle_run_008(rig : Any, store_dir : str, schedule : faults.FaultSchedule, record : Dict[str, Any]) -> None:
    '''
    The child (epoch 1) has replica 0's data and EOS recorded when it dies at
    ``eos.record.after``; its replacement (epoch 2) sees the old EOS again (a
    duplicate: acked, never counted twice), must not finish on it, and finishes
    exactly once — under its own epoch — only after replica 1's EOS (delivered
    twice) *and* its final output. The superseded epoch cannot commit.
    '''
    rig.provision([spec('parent', [], 'producer', True, nb_tasks = 2), spec('child', ['parent'], 'consumer', False)],
                  ack_wait = 30)
    for i in range(2):
        rig.publish_data('parent', f'p0:{i + 1}', i + 1, {'r': 0, 'n': i}, replica_id = 0)
    rig.publish_data('parent', 'p1:1', 1, {'r': 1, 'n': 0}, replica_id = 1)
    p1 = rig.messenger('child', ['parent'], runtime = _child_runtime(store_dir, rig, 2), ack_wait = 30,
                       eos_quiescence_ms = HISTORICAL_QUIESCENCE_MS)
    r1 = Receiver(p1, name = 'run008-epoch1')
    r1.start()
    assert rig.until(lambda: len(r1.inputs) == 3, 20), len(r1.inputs)
    token_1 = p1._authority
    assert token_1 is not None and token_1.epoch == 1, token_1
    with schedule:
        rig.publish_terminator('parent', KIND_EOS, 0, 2)
        assert rig.until(lambda: r1.error is not None or r1.terminal is not None, 20), 'no crash at the barrier'
    assert isinstance(r1.error, SimulatedCrash), (r1.error, r1.terminal)
    rig.kill(p1)
    r1.join(10)
    record['epoch1'] = {'inputs': _input_ids(r1.inputs), 'ledger': ledger_snapshot(store_dir)}
    # The replacement: a new epoch, the old EOS redelivered and fenced to one record.
    p2 = rig.messenger('child', ['parent'], runtime = _child_runtime(store_dir, rig, 2), ack_wait = 30,
                       eos_quiescence_ms = HISTORICAL_QUIESCENCE_MS)
    assert p2._authority is not None and p2._authority.epoch == 2, p2._authority
    r2 = Receiver(p2, name = 'run008-epoch2')
    r2.start()
    if isinstance(rig, ModelRig):
        # The old EOS — left un-acked by the crash — reaches the replacement's own
        # EOS subscription (a second delivery) and is acked as a duplicate.
        old_eos_id = terminator_id(rig.flow_id, rig.run_id, 'parent', KIND_EOS, 0, 2)
        assert rig.until(lambda: len([d for d in rig.deliveries('child') if d['op_id'] == old_eos_id]) == 2
                         and any(s.get('status') == 'completed' for s in rig.settlements(old_eos_id)), 10), \
            (rig.deliveries('child'), rig.settlements(old_eos_id))
    time.sleep(QUIET_INTERVAL_S)
    terminators = lambda: [(t.replica_id, t.kind, t.seq) for t in p2._runtime.terminators('parent')]  # noqa: E731
    assert terminators() == [(0, 'eos', 2)], terminators()
    assert r2.terminal is None, 'the duplicate old EOS finished the replacement'
    assert _barrier_state(p2) == COMPLETION_OPEN, _barrier_state(p2)
    record['after_duplicate_old_eos'] = {'terminators': terminators(), 'state': _barrier_state(p2)}
    # New-epoch obligations: replica 1's EOS, delivered twice, then its final output.
    rig.publish_terminator('parent', KIND_EOS, 1, 2)
    rig.publish_terminator('parent', KIND_EOS, 1, 2, msg_id = f'{rig.run_id}-eos-r1-repeat')
    assert rig.until(lambda: terminators() == [(0, 'eos', 2), (1, 'eos', 2)], 10), terminators()
    time.sleep(QUIET_INTERVAL_S)
    assert r2.terminal is None, 'the replacement finished before its final-sequence obligation'
    assert _barrier_state(p2) == COMPLETION_DRAINING, _barrier_state(p2)
    assert len(p2._runtime.received_ids('parent', p2._data_durable_name('parent'))) == 3
    record['after_duplicate_new_eos'] = {'terminators': terminators(), 'state': _barrier_state(p2)}
    rig.publish_data('parent', 'p1:2', 2, {'r': 1, 'n': 1}, replica_id = 1)
    assert rig.until(lambda: r2.terminal is not None, 20), f'no completion after the final obligation: {_barrier_state(p2)}'
    assert r2.stopped_cleanly and r2.error is None, (r2.terminal, r2.error)
    assert _input_ids(r2.inputs) == ['1:1'], _input_ids(r2.inputs)
    persisted = _child_runtime(store_dir, rig, 2)
    assert persisted.completed_parents() == {'parent'}
    raw, version = persisted.store.get(persisted.key('completion', 'parent', persisted.partition_id()))
    assert raw is not None
    doc = json.loads(raw.decode())
    assert doc['epoch'] == 2 and doc['final'] == 4, doc
    # Old EOS fenced to its epoch: the superseded owner can commit nothing.
    with pytest.raises(StaleAuthority):
        p1._runtime.commit_completion('parent', token_1)
    assert persisted.store.get(persisted.key('completion', 'parent', persisted.partition_id()))[1] == version, 'a stale commit changed the record'
    record['completion'] = {'record': doc, 'version': version, 'terminators': terminators(),
                            'stale_epoch_refused': token_1.epoch}


@pytest.mark.case('RUN-008')
@pytest.mark.level('process')
def test_run_008_duplicate_and_stale_epoch_eos_cannot_finish_a_replacement(tmp_path, evidence_dir, record_faults,
                                                                            monkeypatch) -> None:
    '''
    RUN-008 (P0, runtime, process): Duplicate and stale-epoch EOS cannot finish a replacement
    execution.

    Acceptance: Neither stale nor duplicate EOS can produce early success; exactly one current-
    epoch terminal decision is recorded.

    In-process on the memory backend: the first child dies at ``eos.record.after``
    (the terminator recorded, not acked), the replacement takes the partition
    under epoch 2 over the same file ledger.
    '''
    schedule = faults.FaultSchedule({'eos.record.after': faults.Nth(1, faults.RaiseError(
        lambda: SimulatedCrash('the child died after recording the terminator')))})
    rig = ModelRig(auto_advance = 0.0)
    record : Dict[str, Any] = {}
    try:
        _oracle_run_008(rig, str(tmp_path / 'ledger'), schedule, record)
    finally:
        rig.close()
        record['faults'] = schedule.fired()
        write_evidence(evidence_dir, 'epochs.json', record)
    record_faults(schedule)


@pytest.mark.negative_control(of = 'RUN-008')
def test_run_008_detects_terminators_collapsed_onto_the_first(tmp_path, monkeypatch) -> None:
    '''A receiver that ends the parent on its first marker finishes the replacement on the redelivered old EOS: the oracle must catch it.'''
    defects_run1.collapsing_terminators(monkeypatch)
    schedule = faults.FaultSchedule({'eos.record.after': faults.Nth(1, faults.RaiseError(
        lambda: SimulatedCrash('the child died after recording the terminator')))})
    rig = ModelRig(auto_advance = 0.0)
    try:
        assert defects.detects(_oracle_run_008, rig, str(tmp_path / 'ledger'), schedule, {})
    finally:
        rig.close()


# -- RUN-009 --------------------------------------------------------------------------

class _QueryFailures:
    '''Alternating timeouts and authorization failures from the delivery-state query.'''
    def __init__(self) -> None:
        self.raised : List[str] = []

    def __call__(self) -> BaseException:
        error : BaseException = TimeoutError('injected: consumer_info timed out') if len(self.raised) % 2 == 0 \
            else PermissionError('injected: consumer_info not authorized')
        self.raised.append(type(error).__name__)
        return error


def _oracle_run_009(rig : Any, store_dir : str, record : Dict[str, Any]) -> faults.FaultSchedule:
    '''
    The parent's EOS is recorded while m3 is still held: with every state query
    failing the barrier reads *unknown* (never empty) and the child waits; with
    the query restored it reads *draining* until m3 is settled, and completes
    only then.
    '''
    rig.provision([spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False)], ack_wait = 30)
    child = rig.messenger('child', ['parent'], runtime = _child_runtime(store_dir, rig, 1), ack_wait = 30)
    held = {'seq': 3}

    def on_input(messenger : Any, inputs : dict) -> bool:
        return inputs['parent']['message']['n'] != held['seq']
    receiver = Receiver(child, on_input = on_input, name = 'run009-child')
    for i in range(1, 4):
        rig.publish_data('parent', f'p0:{i}', i, {'r': 0, 'n': i})
    rig.publish_terminator('parent', KIND_EOS, 0, 3)
    receiver.start()
    assert rig.until(lambda: len(receiver.inputs) == 3, 20), len(receiver.inputs)
    assert rig.until(lambda: child._runtime.terminators('parent') != [], 10)
    failures = _QueryFailures()
    schedule = faults.FaultSchedule({'observe.subscription.before': faults.RaiseError(failures)})
    transitions : List[tuple[str, str]] = []
    with schedule:
        pending = child.pending_observation()
        assert isinstance(pending, Unknown), f'a failed query read as a count: {pending}'
        observed = child._consumer_pending('parent')
        assert isinstance(observed, Unknown) and 'injected' in observed.detail, observed
        started = time.monotonic()
        while time.monotonic() - started < QUIET_INTERVAL_S:
            transitions.append(('query-failing', _barrier_state(child)))
            time.sleep(0.1)
        assert all(state == COMPLETION_UNKNOWN for _p, state in transitions), transitions
        assert receiver.terminal is None, 'the child completed while the only evidence of emptiness was a failed query'
        assert child._runtime.completed_parents() == set()
        record['during_outage'] = {'errors': list(failures.raised), 'pending_observation': str(pending),
                                   'states': sorted({s for _p, s in transitions})}
    # Queries restored while m3 is still unresolved: draining, not complete.
    assert rig.until(lambda: _barrier_state(child) == COMPLETION_DRAINING, 10), _barrier_state(child)
    time.sleep(QUIET_INTERVAL_S / 2)
    assert receiver.terminal is None, 'the child completed with m3 still held'
    observed = child._consumer_pending('parent')
    assert isinstance(observed, Known) and observed.value == (0, 1), observed
    transitions.append(('query-restored', _barrier_state(child)))
    # The outstanding work resolves: completion follows.
    child.ack_inputs()
    assert rig.until(lambda: receiver.terminal is not None, 20), _barrier_state(child)
    assert receiver.stopped_cleanly and receiver.error is None
    persisted = _child_runtime(store_dir, rig, 1)
    assert persisted.completed_parents() == {'parent'}
    transitions.append(('m3-settled', 'complete'))
    record['transitions'] = transitions
    record['final_ledger'] = {'terminators': [(t.replica_id, t.kind, t.seq) for t in persisted.terminators('parent')],
                              'received': sorted(persisted.received_ids('parent', child._data_durable_name('parent'))),
                              'completed': sorted(persisted.completed_parents())}
    return schedule


@pytest.mark.case('RUN-009')
@pytest.mark.level('broker')
def test_run_009_unknown_delivery_state_cannot_satisfy_a_completion_barrier(nats_url, tmp_path, evidence_dir,
                                                                            record_faults, monkeypatch) -> None:
    '''
    RUN-009 (P0, runtime, broker): Unknown delivery state cannot satisfy a completion barrier.

    Acceptance: No successful completion is recorded while the only evidence of emptiness is a
    failed query.

    On the compose broker, with the child's ``consumer_info`` reads failing at the
    ``observe.subscription.before`` barrier (timeouts and authorization errors,
    alternating) while a delivery is still held.
    '''
    rig = BrokerRig(nats_url, 'run009')
    record : Dict[str, Any] = {'flow': rig.flow_id, 'run': rig.run_id}
    schedule = None
    try:
        schedule = _oracle_run_009(rig, str(tmp_path / 'ledger'), record)
    finally:
        rig.close()
        if schedule is not None:
            record['faults'] = schedule.fired()
        write_evidence(evidence_dir, 'completion_state.json', record)
    assert schedule is not None
    record_faults(schedule)


@pytest.mark.case('RUN-009')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_run_009_memory_unknown_observation_never_reads_as_empty(tmp_path, evidence_dir, record_faults,
                                                                 monkeypatch) -> None:
    rig = ModelRig(auto_advance = 0.0)
    record : Dict[str, Any] = {}
    schedule = None
    try:
        schedule = _oracle_run_009(rig, str(tmp_path / 'ledger'), record)
    finally:
        rig.close()
        if schedule is not None:
            record['faults'] = schedule.fired()
        write_evidence(evidence_dir, 'completion_state.json', record)
    assert schedule is not None
    record_faults(schedule)


@pytest.mark.negative_control(of = 'RUN-009')
def test_run_009_detects_a_failed_query_read_as_zero(tmp_path, monkeypatch) -> None:
    '''``(0, 0)`` for a failed ``consumer_info`` completes the drain over a held delivery: the oracle must catch it.'''
    defects_run1.zero_on_failure_pending(monkeypatch)
    rig = ModelRig(auto_advance = 0.0)
    try:
        assert defects.detects(_oracle_run_009, rig, str(tmp_path / 'ledger'), {})
    finally:
        rig.close()


# -- RUN-010 --------------------------------------------------------------------------

ORDERS = {
    # order: (terminators in publish order, the crash boundary, the ordinal of the ABORT among terminators)
    'eos-first': ([('eos', 0, 2, None), ('abort', 1, 1, ABORT_ERROR), ('abort-repeat', 1, 1, ABORT_ERROR)],
                  'eos.record.after', 2),
    'abort-first': ([('abort', 1, 1, ABORT_ERROR), ('abort-repeat', 1, 1, ABORT_ERROR), ('eos', 0, 2, None)],
                    'eos.record.before', 1),
}


def _publish_terminators(rig : Any, parent : str, markers : list) -> None:
    for kind, replica, seq, error in markers:
        if kind == 'abort-repeat':
            rig.publish_terminator(parent, KIND_ABORT, replica, seq, error = error,
                                   msg_id = f'{rig.run_id}-{parent}-abort-r{replica}-repeat')
        else:
            rig.publish_terminator(parent, KIND_EOS if kind == 'eos' else KIND_ABORT, replica, seq, error = error)


def _seed_run_010(rig : Any, parent : str) -> None:
    rig.publish_data(parent, 'p0:1', 1, {'r': 0, 'n': 0}, replica_id = 0)
    rig.publish_data(parent, 'p0:2', 2, {'r': 0, 'n': 1}, replica_id = 0)
    rig.publish_data(parent, 'p1:1', 1, {'r': 1, 'n': 0}, replica_id = 1)


def _oracle_run_010_inprocess(rig : Any, store_dir : str, order : str, schedule : faults.FaultSchedule,
                              record : Dict[str, Any]) -> None:
    '''
    One ordering: the child has every replica's data when the terminators land
    (EOS from replica 0, ABORT from replica 1 — repeated — in the given order)
    and dies at the boundary before it reports terminal status; its replacement
    reports the abort, with the cause, and fabricates nothing.
    '''
    parent = f'parent_{order}'.replace('-', '_')
    child = f'child_{order}'.replace('-', '_')
    markers, _barrier, _nth = ORDERS[order]
    rig.provision([spec(parent, [], 'producer', True, nb_tasks = 2), spec(child, [parent], 'consumer', False)],
                  ack_wait = 30)
    entry : Dict[str, Any] = {'order': order}
    record[order] = entry
    _seed_run_010(rig, parent)
    runtime = lambda: FlowRuntime(FileRuntimeStore(store_dir), rig.flow_id, rig.run_id, child,  # noqa: E731
                                  parent_replicas = {parent: 2})
    p1 = rig.messenger(child, [parent], runtime = runtime(), ack_wait = 30)
    r1 = Receiver(p1, name = f'run010-{order}-p1')
    r1.start()
    assert rig.until(lambda: len(r1.inputs) == 3, 20), len(r1.inputs)
    with schedule:
        _publish_terminators(rig, parent, markers)
        assert rig.until(lambda: r1.error is not None or r1.terminal is not None, 20), 'no crash at the barrier'
    assert isinstance(r1.error, SimulatedCrash), (r1.error, r1.terminal)
    rig.kill(p1)
    r1.join(10)
    entry['ledger_after_crash'] = {k: v for k, v in ledger_snapshot(store_dir).items() if child in k}
    p2 = rig.messenger(child, [parent], runtime = runtime(), ack_wait = 30)
    r2 = Receiver(p2, name = f'run010-{order}-p2')
    r2.start()
    assert rig.until(lambda: r2.terminal is not None or r2.error is not None, 30), _barrier_state(p2, parent)
    assert r2.error is None, r2.error
    assert r2.aborted, f'{order}: the run finished as a success: {r2.terminal}'
    terminal = r2.terminal[parent]
    assert terminal['is_abort'] and terminal['abort_origin'] == parent, terminal
    assert terminal['abort_error']['code'] == ABORT_ERROR['code'], terminal
    entries = [r2.terminal[name] for name in [parent]]
    with pytest.raises(UpstreamAborted) as raised:
        raise_if_aborted(entries, p2, has_children = False)
    assert raised.value.context.get('origin') == parent and ABORT_ERROR['code'] in str(raised.value)
    assert r2.inputs == [], f'{order}: results were fabricated on recovery: {_input_ids(r2.inputs, parent)}'
    persisted = runtime()
    terminators = [(t.replica_id, t.kind, t.seq) for t in persisted.terminators(parent)]
    # An aborted parent stops the child once drained (ABORT-4): after the ABORT the
    # clean EOS may never even be observed — and can never hide the abort.
    assert (1, 'abort', 1) in terminators and (1, 'eos', 1) not in terminators, terminators
    if order == 'eos-first':
        assert sorted(terminators) == [(0, 'eos', 2), (1, 'abort', 1)], terminators
    assert persisted.aborted_parents() == {parent: ABORT_ERROR}, persisted.aborted_parents()
    assert _barrier_state(p2, parent) == COMPLETION_ABORTED
    entry['recovered'] = {'terminal': terminal, 'cause': str(raised.value), 'terminators': terminators,
                          'inputs_before_crash': _input_ids(r1.inputs, parent),
                          'inputs_after': _input_ids(r2.inputs, parent)}


def _oracle_run_010_subprocess(rig : BrokerRig, store_dir : str, order : str, schedule : faults.FaultSchedule,
                               record : Dict[str, Any], log_dir : str, sink_path : str) -> None:
    '''One ordering on a real ``FileSink`` worker: it dies of ``os._exit`` at the boundary and is started again.'''
    parent, child = 'parent', 'child'
    markers, _barrier, _nth = ORDERS[order]
    rig.provision([spec(parent, [], 'producer', True, nb_tasks = 2),
                   spec(child, [parent], 'consumer', False, node_class = '_runs1.FileSink')], ack_wait = 3)
    entry : Dict[str, Any] = {'order': order}
    record[order] = entry
    _seed_run_010(rig, parent)
    _publish_terminators(rig, parent, markers)
    termination_log = os.path.join(log_dir, f'{order}-termination.json')
    env = worker_env(child, 'consumer', '_runs1.FileSink', rig.nats_url, rig.flow_id, rig.run_id, parents = [parent],
                     params = {'path': sink_path}, parent_replicas = [2], store_url = f'file://{store_dir}',
                     ack_wait = 3, termination_log = termination_log)
    first = run_worker(dict(env, **schedule.to_env()), os.path.join(log_dir, f'{order}-first.log'), timeout = 60,
                       termination_log = termination_log)
    entry['first'] = {'exit_code': first.exit_code, 'took_s': first.took_s, 'termination': first.termination}
    assert first.exit_code == 137, f'{order}: the child did not die at the barrier: {first}'
    entry['ledger_after_crash'] = ledger_snapshot(store_dir)
    second = run_worker(env, os.path.join(log_dir, f'{order}-second.log'), timeout = 60,
                        termination_log = termination_log)
    entry['second'] = {'exit_code': second.exit_code, 'took_s': second.took_s, 'termination': second.termination}
    assert second.exit_code == UpstreamAborted.exit_code, f'{order}: the run did not fail: {second}'
    assert second.termination is not None, f'{order}: no terminal status was recorded: {second}'
    assert second.termination['code'] == 'VF_UPSTREAM_ABORTED', second.termination
    assert second.termination.get('context', {}).get('origin') == parent, second.termination
    assert ABORT_ERROR['code'] in second.termination['message'], second.termination
    lines = read_jsonl(sink_path)
    ids = sorted(f"{r['item']['r']}:{r['item']['n']}" for r in lines if r['event'] == 'input')
    assert ids == ['0:0', '0:1', '1:0'], f'{order}: delivered data does not match what the replicas published: {ids}'
    # Only the replacement closes: a crash has no close(), and no result was fabricated on recovery.
    assert [r['event'] for r in lines].count('closed') == 1, [r['event'] for r in lines]
    persisted = FlowRuntime(FileRuntimeStore(store_dir), rig.flow_id, rig.run_id, child, parent_replicas = {parent: 2})
    terminators = [(t.replica_id, t.kind, t.seq) for t in persisted.terminators(parent)]
    assert (1, 'abort', 1) in terminators and (1, 'eos', 1) not in terminators, terminators
    if order == 'eos-first':
        assert sorted(terminators) == [(0, 'eos', 2), (1, 'abort', 1)], terminators
    assert persisted.aborted_parents() == {parent: ABORT_ERROR}, persisted.aborted_parents()
    entry['recovered'] = {'termination': second.termination, 'terminators': terminators, 'sink_ids': ids}


def _oracle_run_010_real_replicas(rig : BrokerRig, store_dir : str, record : Dict[str, Any], log_dir : str,
                                  sink_path : str) -> None:
    '''Real replicas: one ``CountingProducer`` ends cleanly, one ``FailingProducer`` dies of poison and publishes ABORT itself.'''
    rig.provision([spec('parent', [], 'producer', True, nb_tasks = 2, node_class = '_runs1.CountingProducer'),
                   spec('child', ['parent'], 'consumer', False, node_class = '_runs1.FileSink')], ack_wait = 30)
    termination_log = os.path.join(log_dir, 'real-termination.json')
    child = spawn_worker(worker_env('child', 'consumer', '_runs1.FileSink', rig.nats_url, rig.flow_id, rig.run_id,
                                    parents = ['parent'], params = {'path': sink_path}, parent_replicas = [2],
                                    store_url = f'file://{store_dir}', ack_wait = 30, termination_log = termination_log),
                         os.path.join(log_dir, 'real-child.log'))
    r0 = spawn_worker(worker_env('parent', 'producer', '_runs1.CountingProducer', rig.nats_url, rig.flow_id, rig.run_id,
                                 has_children = True, params = {'count': 2}, replica_id = 0, nb_tasks = 2,
                                 store_url = f'file://{store_dir}'), os.path.join(log_dir, 'real-r0.log'))
    r1 = spawn_worker(worker_env('parent', 'producer', '_runs1.FailingProducer', rig.nats_url, rig.flow_id, rig.run_id,
                                 has_children = True, params = {'count': 1}, replica_id = 1, nb_tasks = 2,
                                 store_url = f'file://{store_dir}'), os.path.join(log_dir, 'real-r1.log'))
    r0_out = wait_worker(r0, 60, os.path.join(log_dir, 'real-r0.log'))
    r1_out = wait_worker(r1, 60, os.path.join(log_dir, 'real-r1.log'))
    child_out = wait_worker(child, 60, os.path.join(log_dir, 'real-child.log'), termination_log)
    record['real'] = {'r0': r0_out.exit_code, 'r1': r1_out.exit_code,
                      'child': {'exit_code': child_out.exit_code, 'termination': child_out.termination}}
    assert r0_out.exit_code == 0 and r1_out.exit_code != 0, (r0_out, r1_out)
    assert child_out.exit_code == UpstreamAborted.exit_code, f'a replica died terminally yet the run succeeded: {child_out}'
    assert child_out.termination is not None and child_out.termination['code'] == 'VF_UPSTREAM_ABORTED', child_out
    assert 'VF_POISON_SCHEMA' in child_out.termination['message'], child_out.termination
    persisted = FlowRuntime(FileRuntimeStore(store_dir), rig.flow_id, rig.run_id, 'child', parent_replicas = {'parent': 2})
    terminators = sorted((t.replica_id, t.kind, t.seq) for t in persisted.terminators('parent'))
    # The failing replica usually dies first, and an aborted parent stops the child
    # once drained (ABORT-4): the surviving replica's EOS may never be observed.
    assert (1, 'abort', 1) in terminators and (1, 'eos', 1) not in terminators, terminators


@pytest.mark.case('RUN-010')
@pytest.mark.level('process')
def test_run_010_abort_remains_distinct_from_normal_end_of_stream_across(nats_url, tmp_path, evidence_dir,
                                                                          record_faults, monkeypatch) -> None:
    '''
    RUN-010 (P0, runtime, process): ABORT remains distinct from normal end of stream across
    replicas.

    Acceptance: All orderings and restarts retain the declared failure outcome and its cause; no
    case becomes success merely because EOS arrived first.

    Three runs on the compose broker, each with a real ``FileSink`` worker: the
    two orderings with a repeated ABORT and a crash of the sink before it reports
    terminal status (``eos.record.after`` once the ABORT is recorded, and
    ``eos.record.before`` before it is), then real replica workers where the
    failing one publishes its own ABORT.
    '''
    record : Dict[str, Any] = {}
    schedules : Dict[str, faults.FaultSchedule] = {}
    try:
        for order, (_markers, barrier, nth) in ORDERS.items():
            schedule = faults.FaultSchedule({barrier: faults.Nth(nth, faults.Crash(137))},
                                            marker_dir = str(tmp_path / 'markers' / order))
            schedules[order] = schedule
            rig = BrokerRig(nats_url, f'run010{order[:1]}')
            try:
                _oracle_run_010_subprocess(rig, str(tmp_path / order / 'ledger'), order, schedule, record,
                                           str(evidence_dir), str(tmp_path / f'{order}-sink.jsonl'))
            finally:
                rig.close()
        rig = BrokerRig(nats_url, 'run010r')
        try:
            _oracle_run_010_real_replicas(rig, str(tmp_path / 'real' / 'ledger'), record, str(evidence_dir),
                                          str(tmp_path / 'real-sink.jsonl'))
        finally:
            rig.close()
    finally:
        record['faults'] = {order: s.fired() for order, s in schedules.items()}
        write_evidence(evidence_dir, 'terminal_lineage.json', record)
    for schedule in schedules.values():
        record_faults(schedule)


@pytest.mark.case('RUN-010')
@pytest.mark.level('process')
@pytest.mark.variant('memory')
def test_run_010_memory_abort_survives_ordering_repeats_and_a_restart(tmp_path, evidence_dir, record_faults,
                                                                      monkeypatch) -> None:
    record : Dict[str, Any] = {}
    schedules = []
    rig = ModelRig(auto_advance = 0.0)
    try:
        for order, (_markers, barrier, nth) in ORDERS.items():
            schedule = faults.FaultSchedule({barrier: faults.Nth(nth, faults.RaiseError(
                lambda: SimulatedCrash('the child died before reporting terminal status')))})
            schedules.append(schedule)
            _oracle_run_010_inprocess(rig, str(tmp_path / 'ledger'), order, schedule, record)
    finally:
        rig.close()
        record['faults'] = [s.fired() for s in schedules]
        write_evidence(evidence_dir, 'terminal_lineage.json', record)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.negative_control(of = 'RUN-010')
def test_run_010_detects_an_abort_acked_away_behind_an_eos(tmp_path, monkeypatch) -> None:
    '''A receiver that acks the ABORT that followed an EOS, keeping it only in memory, restarts into a clean finish: the oracle must catch it.'''
    defects_run1.acked_extra_terminators(monkeypatch)
    schedule = faults.FaultSchedule({'eos.record.after': faults.Nth(2, faults.RaiseError(
        lambda: SimulatedCrash('the child died before reporting terminal status')))})
    rig = ModelRig(auto_advance = 0.0)
    try:
        assert defects.detects(_oracle_run_010_inprocess, rig, str(tmp_path / 'ledger'), 'eos-first', schedule, {})
    finally:
        rig.close()
