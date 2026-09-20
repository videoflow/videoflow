'''
Conformance cases: RUN-001, RUN-002, RUN-005, RUN-006, RUN-016.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions over a *rig* (``_runs1``): the
memory variants run them on the in-memory backend under a fake clock, the broker
and process primaries on the compose broker — in-process messengers at the broker
level, real ``videoflow.runtime.worker`` subprocesses driven through their
environment (fault schedule included, ``ENV-16``) at the process level — and the
paired negative control runs the same oracle on the memory rig with the reviewed
defect monkeypatched in (``defects_run1.py``).

The ledger under every rig is a ``FileRuntimeStore``: RUN-001/002's group ledger
(``CTRL-4``) and the EOS-7 barrier that lets a recovered join *finish* engage only
over a durable, shared store, which the memory store truthfully is not.
'''
from __future__ import absolute_import, division, print_function

import logging
import os
import threading
import time
from typing import Any, Dict, List, Optional

import defects
import defects_run1
import pytest
from _runs1 import (
    BrokerRig,
    ModelRig,
    Receiver,
    SimulatedCrash,
    data_id,
    ledger,
    ledger_snapshot,
    run_worker,
    spec,
    worker_env,
    write_evidence,
)

from videoflow.backends import faults
from videoflow.backends.memory.runtime_store import FileRuntimeStore
from videoflow.backends.outcomes import Known
from videoflow.backends.runtime import FlowRuntime, group_identity
from videoflow.core.errors import VideoflowUserError
from videoflow.core.policies import JoinPolicy
from videoflow.messaging import grouping
from videoflow.messaging.grouping import EnvelopeEntry, TimeGroupAssembler, TraceGroupAssembler

pytestmark = pytest.mark.timeout(180)

#: Recovery deadlines: what a replacement gets to reach the committed outcome.
MEMORY_RECOVERY_S = 15.0
BROKER_RECOVERY_S = 40.0


def _join_runtime(store_dir : str, rig : Any, node : str = 'join', parents : Dict[str, int] | None = None) -> FlowRuntime:
    return FlowRuntime(FileRuntimeStore(store_dir), rig.flow_id, rig.run_id, node,
                       parent_replicas = parents or {'A': 1, 'B': 1})


def _publish_join_result(messenger : Any, inputs : dict) -> bool:
    '''What ``PairJoin.process`` + the task loop do with a complete group: publish, then (the caller) ack.'''
    messenger.publish_message({'a': inputs['A']['message'], 'b': inputs['B']['message']})
    return True


def _held_attempt(messenger : Any, parent : str, trace : str) -> Optional[int]:
    '''The delivery attempt of the half a trace join currently buffers for ``parent`` (None when none).'''
    assembler = messenger._assembler
    assert isinstance(assembler, TraceGroupAssembler)
    handle = assembler._handles.get(trace, {}).get(parent)
    return None if handle is None else int(handle.num_delivered)


def _held_groups(messenger : Any) -> Dict[str, List[str]]:
    assembler = messenger._assembler
    assert isinstance(assembler, TraceGroupAssembler)
    return {trace: sorted(group) for trace, group in assembler._groups.items()}


def _settled(rig : Any, child : str, parents : List[str]) -> bool:
    '''Every parent's data subscription for ``child`` observed with nothing pending and nothing leased.'''
    for parent in parents:
        observed = rig.consumer_state(child, parent)
        if not isinstance(observed, Known) or observed.value != (0, 0):
            return False
    return True


# -- RUN-001 --------------------------------------------------------------------------

def _oracle_run_001(rig : Any, store_dir : str, schedule : faults.FaultSchedule, record : Dict[str, Any],
                    recovery_s : float, keepalive : bool = True) -> None:
    '''
    A is delivered to the join, its lease lapses and attempt 2 supersedes attempt 1
    while B is still absent (no broker settlement: A is retained and leased, never
    terminated); the worker is replaced before B arrives; the replacement gets A
    (attempt 3) and B, publishes the result and dies at its first settlement
    (``settle.before``); a third worker reaches the committed outcome — the
    persisted group decision, or both members recomputed — exactly once.
    '''
    specs = [spec('A', [], 'producer', True), spec('B', [], 'producer', True),
             spec('join', ['A', 'B'], 'processor', True), spec('sink', ['join'], 'consumer', False)]
    rig.provision(specs, ack_wait = 2)
    extra = {'keepalive': keepalive} if isinstance(rig, BrokerRig) else {}
    w1 = rig.messenger('join', ['A', 'B'], runtime = _join_runtime(store_dir, rig), ack_wait = 2, **extra)
    a_id = rig.publish_data('A', 't1', 1, 1)
    r1 = Receiver(w1, name = 'run001-w1')
    r1.start()
    assert rig.until(lambda: _held_attempt(w1, 'A', 't1') == 1, 15), 'A attempt 1 never reached the join'
    assert rig.until(lambda: _held_attempt(w1, 'A', 't1') == 2, 20), \
        f'the lease of A never lapsed into a second attempt (held attempt {_held_attempt(w1, "A", "t1")})'
    # Replacing the attempt handle emitted no terminal outcome: A is still the
    # broker's (retained and leased), buffered exactly once in the join.
    assert w1.join_status()['pending_groups'] == 1, w1.join_status()
    retained = rig.retained('A')
    assert [r.publication_id for r in retained] == [a_id], f'A was terminated or duplicated: {retained}'
    observed = rig.consumer_state('join', 'A')
    assert isinstance(observed, Known) and observed.value == (0, 1), observed
    record['after_supersede'] = {'held_attempt': _held_attempt(w1, 'A', 't1'), 'consumer_state': observed.value,
                                 'open_groups': [g.group_id for g in _join_runtime(store_dir, rig).open_groups()]}
    if isinstance(rig, ModelRig):
        assert not [s for s in rig.settlements(a_id) if s.get('status') == 'terminal'], rig.settlements(a_id)
    # The worker is replaced before B arrives.
    rig.kill(w1)
    r1.join(10)
    w2 = rig.messenger('join', ['A', 'B'], runtime = _join_runtime(store_dir, rig), ack_wait = 2)
    r2 = Receiver(w2, on_input = _publish_join_result, name = 'run001-w2')
    b_id = rig.publish_data('B', 't1', 1, 2)
    with schedule:
        r2.start()
        assert rig.until(lambda: r2.error is not None or r2.terminal is not None, recovery_s), \
            'the replacement neither completed the group nor reached the scheduled crash'
    assert isinstance(r2.error, SimulatedCrash), r2.error
    rig.kill(w2)
    r2.join(10)
    persisted = _join_runtime(store_dir, rig)
    open_groups = persisted.open_groups()
    output_id = data_id(rig.flow_id, rig.run_id, 'join', 't1', 1)
    entry = persisted.outbox_entry(output_id)
    record['after_crash'] = {'open_groups': [(g.group_id, dict(g.members)) for g in open_groups],
                             'outbox': None if entry is None else entry.outcome,
                             'retained_a': [r.publication_id for r in rig.retained('A')],
                             'retained_b': [r.publication_id for r in rig.retained('B')]}
    assert [g.group_id for g in open_groups] == [output_id], 'the group decision was not persisted before the crash'
    assert entry is not None and entry.outcome in ('accepted', 'duplicate'), entry
    assert [r.publication_id for r in rig.retained('A')] == [a_id], 'the crash lost A'
    assert [r.publication_id for r in rig.retained('B')] == [b_id], 'the crash lost B'
    # Recovery: the third worker settles both members against the persisted decision.
    started = time.monotonic()
    w3 = rig.messenger('join', ['A', 'B'], runtime = _join_runtime(store_dir, rig), ack_wait = 2)
    r3 = Receiver(w3, on_input = _publish_join_result, name = 'run001-w3')
    r3.start()
    assert rig.until(lambda: _settled(rig, 'join', ['A', 'B']) and not rig.retained('A') and not rig.retained('B'),
                     recovery_s), \
        f'A+B never reached a committed terminal outcome: {rig.consumer_state("join", "A")}, ' \
        f'{rig.consumer_state("join", "B")}, error={r3.error!r}'
    outputs = rig.retained('join')
    record['recovery'] = {'seconds': time.monotonic() - started,
                          'outputs': [(o.publication_id, o.decoded['trace_id'], o.decoded['seq']) for o in outputs],
                          'sink_members': [o.decoded['message'] for o in outputs]}
    assert [o.publication_id for o in outputs] == [output_id], f'not exactly one committed result: {outputs}'
    assert outputs[0].decoded['message'] == {'a': 1, 'b': 2}
    r3.stop()
    r3.join(10)
    assert r3.error is None, r3.error


@pytest.mark.case('RUN-001')
@pytest.mark.level('broker')
def test_run_001_superseding_a_buffered_join_delivery_does_not_terminate(nats_url, tmp_path, evidence_dir,
                                                                          record_faults, monkeypatch) -> None:
    '''
    RUN-001 (P0, runtime, broker): Superseding a buffered join delivery does not terminate its
    logical input.

    Acceptance: After the scheduled crash, A+B reaches a committed terminal outcome and no
    superseded attempt terminally removes A before that outcome.

    On the compose broker: the first worker's backend keeps no lease alive
    (``keepalive = False``), so A's 2 s ack window lapses into a real JetStream
    redelivery; the replacement dies at ``settle.before`` in-process (the
    broker-level crash model), its connection dropped without a hand-back.
    '''
    schedule = faults.FaultSchedule({'settle.before': faults.Nth(1, faults.RaiseError(
        lambda: SimulatedCrash('the replacement died before its first settlement')))})
    rig = BrokerRig(nats_url, 'run001')
    record : Dict[str, Any] = {'flow': rig.flow_id, 'run': rig.run_id}
    try:
        _oracle_run_001(rig, str(tmp_path / 'ledger'), schedule, record, BROKER_RECOVERY_S, keepalive = False)
        record['consumer_a'] = rig.consumer_info('join', 'A')
    finally:
        rig.close()
        record['faults'] = schedule.fired()
        record['ledger'] = ledger_snapshot(str(tmp_path / 'ledger'))
        write_evidence(evidence_dir, 'attempt_trace.json', record)
    record_faults(schedule)


@pytest.mark.case('RUN-001')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_run_001_memory_supersede_keeps_a_for_the_replacement(tmp_path, evidence_dir, record_faults,
                                                              monkeypatch) -> None:
    schedule = faults.FaultSchedule({'settle.before': faults.Nth(1, faults.RaiseError(
        lambda: SimulatedCrash('the replacement died before its first settlement')))})
    rig = ModelRig()
    record : Dict[str, Any] = {}
    try:
        _oracle_run_001(rig, str(tmp_path / 'ledger'), schedule, record, MEMORY_RECOVERY_S)
        record['deliveries'] = rig.deliveries('join')
    finally:
        rig.close()
        record['faults'] = schedule.fired()
        record['ledger'] = ledger_snapshot(str(tmp_path / 'ledger'))
        write_evidence(evidence_dir, 'attempt_trace.json', record)
    record_faults(schedule)


@pytest.mark.negative_control(of = 'RUN-001')
def test_run_001_detects_a_supersede_that_terminates_the_input(tmp_path, monkeypatch) -> None:
    '''The reviewed grouping TERMed the stale handle — and the transport applied it to the message: the oracle must catch it.'''
    defects_run1.term_on_supersede(monkeypatch)
    schedule = faults.FaultSchedule({'settle.before': faults.Nth(1, faults.RaiseError(
        lambda: SimulatedCrash('the replacement died before its first settlement')))})
    rig = ModelRig()
    try:
        assert defects.detects(_oracle_run_001, rig, str(tmp_path / 'ledger'), schedule, {}, 6.0)
    finally:
        rig.close()


# -- RUN-002 --------------------------------------------------------------------------

#: The enumerated crash boundaries of a join commit, each as the barrier that fires it.
BOUNDARIES : Dict[str, tuple[str, int]] = {
    'before-persist': ('group.commit.before', 1),
    'after-persist': ('group.commit.after', 1),
    'after-commit-before-ack-a': ('settle.before', 1),
    'after-ack-a': ('settle.after', 1),
    'before-ack-b': ('settle.before', 2),
    'after-ack-b': ('settle.after', 2),
}


def _run_002_specs(join_class : str = 'videoflow.processors.basic.IdentityProcessor') -> list:
    return [spec('A', [], 'producer', True), spec('B', [], 'producer', True),
            spec('join', ['A', 'B'], 'processor', True, node_class = join_class),
            spec('sink', ['join'], 'consumer', False)]


def _seed_run_002(rig : Any) -> tuple[str, str]:
    '''A and B for trace t1 (one replica, one message each).'''
    return rig.publish_data('A', 't1', 1, 1), rig.publish_data('B', 't1', 1, 2)


def _end_run_002(rig : Any) -> None:
    '''
    Both parents' end-of-stream, published once the first process is dead: a
    recorded terminator is acked at once under the ledger, and a settlement is
    a settlement to the ``settle.*`` barriers — the boundary ordinals count only
    the group's own acks this way.
    '''
    rig.publish_terminator('A', 'eos', 0, 1)
    rig.publish_terminator('B', 'eos', 0, 1)


def _check_run_002_outcome(rig : Any, store_dir : str, boundary : str, entry : Dict[str, Any]) -> None:
    '''Exactly one committed logical result, both members settled, both parents complete.'''
    output_id = data_id(rig.flow_id, rig.run_id, 'join', 't1', 1)
    outputs = rig.retained('join')
    entry['outputs'] = [(o.publication_id, o.decoded['trace_id'], o.decoded['seq'], o.decoded['message'])
                        for o in outputs]
    entry['consumer_a'] = rig.consumer_state('join', 'A')
    entry['consumer_b'] = rig.consumer_state('join', 'B')
    persisted = ledger(store_dir, rig.flow_id, rig.run_id, 'join', parent_replicas = {'A': 1, 'B': 1})
    entry['completed_parents'] = sorted(persisted.completed_parents())
    entry['open_groups'] = [g.group_id for g in persisted.open_groups()]
    assert [o.publication_id for o in outputs] == [output_id], \
        f'{boundary}: not exactly one committed logical result on the join channel: {entry["outputs"]}'
    assert outputs[0].decoded['message'] == {'a': 1, 'b': 2}, outputs[0].decoded
    assert _settled(rig, 'join', ['A', 'B']), f'{boundary}: a member outcome is unaccounted for: {entry}'
    assert not rig.retained('A') and not rig.retained('B'), f'{boundary}: a member is still retained'
    assert persisted.completed_parents() == {'A', 'B'}, f'{boundary}: the join did not complete its parents: {entry}'


def _oracle_run_002_inprocess(rig : Any, store_dir : str, boundary : str, schedule : faults.FaultSchedule,
                              record : Dict[str, Any], recovery_s : float) -> None:
    '''One boundary: the first join process dies at the barrier, a fresh one recovers from the same ledger.'''
    rig.provision(_run_002_specs(), ack_wait = 2)
    _seed_run_002(rig)
    entry : Dict[str, Any] = {'boundary': boundary}
    record[boundary] = entry
    w1 = rig.messenger('join', ['A', 'B'], runtime = _join_runtime(store_dir, rig), ack_wait = 2)
    r1 = Receiver(w1, on_input = _publish_join_result, name = f'run002-{boundary}-w1')
    with schedule:
        r1.start()
        assert rig.until(lambda: r1.error is not None or r1.terminal is not None, recovery_s), \
            f'{boundary}: the first process neither crashed at the barrier nor finished'
    assert isinstance(r1.error, SimulatedCrash), f'{boundary}: {r1.error!r} (terminal={r1.terminal})'
    rig.kill(w1)
    r1.join(10)
    entry['ledger_after_crash'] = ledger_snapshot(store_dir)
    _end_run_002(rig)
    started = time.monotonic()
    w2 = rig.messenger('join', ['A', 'B'], runtime = _join_runtime(store_dir, rig), ack_wait = 2)
    r2 = Receiver(w2, on_input = _publish_join_result, name = f'run002-{boundary}-w2')
    r2.start()
    assert rig.until(lambda: r2.terminal is not None or r2.error is not None, recovery_s), \
        f'{boundary}: the replacement did not finish within {recovery_s}s: {w2.join_status()}, ' \
        f'A={rig.consumer_state("join", "A")}, B={rig.consumer_state("join", "B")}'
    assert r2.error is None, f'{boundary}: {r2.error!r}'
    assert r2.stopped_cleanly, r2.terminal
    entry['recovery_seconds'] = time.monotonic() - started
    entry['recomputed'] = len(r2.inputs)
    _check_run_002_outcome(rig, store_dir, boundary, entry)


def _oracle_run_002_subprocess(rig : BrokerRig, store_dir : str, boundary : str, schedule : faults.FaultSchedule,
                               record : Dict[str, Any], log_dir : str) -> None:
    '''One boundary on real workers: the join dies of ``os._exit`` at the barrier and is started again.'''
    rig.provision(_run_002_specs('_runs1.PairJoin'), ack_wait = 2)
    _seed_run_002(rig)
    entry : Dict[str, Any] = {'boundary': boundary}
    record[boundary] = entry
    env = worker_env('join', 'processor', '_runs1.PairJoin', rig.nats_url, rig.flow_id, rig.run_id,
                     parents = ['A', 'B'], has_children = True, parent_replicas = [1, 1],
                     store_url = f'file://{store_dir}', ack_wait = 2)
    first = run_worker(dict(env, **schedule.to_env()), os.path.join(log_dir, f'{boundary}-first.log'), timeout = 60)
    entry['first'] = {'exit_code': first.exit_code, 'took_s': first.took_s}
    assert first.exit_code == 137, f'{boundary}: the join did not die at the barrier: {first}'
    entry['ledger_after_crash'] = ledger_snapshot(store_dir)
    _end_run_002(rig)
    started = time.monotonic()
    second = run_worker(env, os.path.join(log_dir, f'{boundary}-second.log'), timeout = 60)
    entry['second'] = {'exit_code': second.exit_code, 'took_s': second.took_s}
    assert second.exit_code == 0, f'{boundary}: the replacement did not finish cleanly: {second}'
    entry['recovery_seconds'] = time.monotonic() - started
    _check_run_002_outcome(rig, store_dir, boundary, entry)


@pytest.mark.case('RUN-002')
@pytest.mark.level('process')
def test_run_002_join_commit_survives_every_partial_input_acknowledgment(nats_url, tmp_path, evidence_dir,
                                                                          record_faults, monkeypatch) -> None:
    '''
    RUN-002 (P0, runtime, process): Join commit survives every partial input-acknowledgment
    crash boundary.

    Acceptance: Every enumerated boundary terminates with exactly one committed logical group
    result and all member outcomes accounted for within the configured recovery deadline.

    Six boundaries, each on its own run of the compose broker with its own file
    ledger: a real ``videoflow.runtime.worker`` hosts the join, installs the
    schedule from its environment and ``os._exit``s at the barrier; a second
    worker with the same logical run recovers. The boundaries run concurrently.
    '''
    record : Dict[str, Any] = {}
    schedules : Dict[str, faults.FaultSchedule] = {}
    errors : Dict[str, BaseException] = {}
    rigs : Dict[str, BrokerRig] = {}

    def one(boundary : str) -> None:
        barrier, nth = BOUNDARIES[boundary]
        schedule = faults.FaultSchedule({barrier: faults.Nth(nth, faults.Crash(137))},
                                        marker_dir = str(tmp_path / 'markers' / boundary))
        schedules[boundary] = schedule
        rig = BrokerRig(nats_url, 'run002')
        rigs[boundary] = rig
        store_dir = str(tmp_path / boundary / 'ledger')
        try:
            _oracle_run_002_subprocess(rig, store_dir, boundary, schedule, record, str(evidence_dir))
        except BaseException as e:  # noqa: BLE001 — collected per boundary, re-raised below
            errors[boundary] = e

    threads = [threading.Thread(target = one, args = (b,), name = f'run002-{b}') for b in BOUNDARIES]
    try:
        for t in threads:
            t.start()
        for t in threads:
            t.join(170)
    finally:
        for rig in rigs.values():
            rig.close()
        record['faults'] = {b: s.fired() for b, s in schedules.items()}
        write_evidence(evidence_dir, 'boundaries.json', record)
    for schedule in schedules.values():
        record_faults(schedule)
    assert not errors, {b: f'{type(e).__name__}: {e}' for b, e in errors.items()}


@pytest.mark.case('RUN-002')
@pytest.mark.level('process')
@pytest.mark.variant('memory')
def test_run_002_memory_every_boundary_recovers_to_one_result(tmp_path, evidence_dir, record_faults,
                                                              monkeypatch) -> None:
    '''The model: a barrier raises the crash in-process; a second messenger over the same file ledger recovers.'''
    record : Dict[str, Any] = {}
    schedules = []
    try:
        for boundary, (barrier, nth) in BOUNDARIES.items():
            schedule = faults.FaultSchedule({barrier: faults.Nth(nth, faults.RaiseError(
                lambda: SimulatedCrash('the join process died at the boundary')))})
            schedules.append(schedule)
            rig = ModelRig()
            try:
                _oracle_run_002_inprocess(rig, str(tmp_path / boundary / 'ledger'), boundary, schedule, record,
                                          MEMORY_RECOVERY_S)
            finally:
                rig.close()
    finally:
        record['faults'] = [s.fired() for s in schedules]
        write_evidence(evidence_dir, 'boundaries.json', record)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.negative_control(of = 'RUN-002')
def test_run_002_detects_a_join_without_a_group_ledger(tmp_path, monkeypatch) -> None:
    '''Without the persisted group decision the replacement waits forever for the already reclaimed A: the oracle must catch it.'''
    defects_run1.no_group_ledger(monkeypatch)
    schedule = faults.FaultSchedule({'settle.after': faults.Nth(1, faults.RaiseError(
        lambda: SimulatedCrash('the join process died after acking A')))})
    rig = ModelRig()
    try:
        # The first process crashes at its barrier within milliseconds; only the
        # replacement's wait is bounded here, and under the defect it never ends.
        assert defects.detects(_oracle_run_002_inprocess, rig, str(tmp_path / 'ledger'), 'after-ack-a', schedule,
                               {}, 1.5)
    finally:
        rig.close()


# -- RUN-005 --------------------------------------------------------------------------

def _oracle_run_005(rig : Any, store_dir : str, record : Dict[str, Any], n_traces : int, credit : int,
                    max_pending : int, stagnation_s : float) -> str:
    '''
    Two parents carry the same trace ids in opposing orders into a join whose
    durables are provisioned with ``credit`` un-acked deliveries each. The runtime
    must either reject the configuration before execution, naming the working
    set, or complete every group; a healthy broker with no progress is the
    deadlock this case exists for. Returns ``'rejected'`` or ``'completed'``.
    '''
    policy = JoinPolicy(missing = 'wait', max_pending = max_pending)
    working_set = policy.working_set(2)
    entry : Dict[str, Any] = {'n_traces': n_traces, 'credit': credit, 'max_pending': max_pending,
                              'working_set': working_set}
    record[f'credit{credit}-n{n_traces}'] = entry
    specs = [spec('A', [], 'producer', True), spec('B', [], 'producer', True),
             spec('join', ['A', 'B'], 'processor', True, join_policy = policy.to_dict()),
             spec('sink', ['join'], 'consumer', False)]
    rig.provision(specs, ack_wait = 2, credit = credit)
    try:
        join = rig.messenger('join', ['A', 'B'], runtime = _join_runtime(store_dir, rig), ack_wait = 2,
                             join_policy = policy.to_dict())
    except VideoflowUserError as e:
        entry['rejected'] = {'code': e.code, 'message': str(e)}
        assert credit < working_set, f'a compatible working set ({working_set} <= credit {credit}) was rejected: {e}'
        assert 'working set' in str(e).lower(), f'the rejection does not name the working set: {e}'
        return 'rejected'
    traces = [f't{i}' for i in range(1, n_traces + 1)]
    for i, trace in enumerate(traces):
        rig.publish_data('A', trace, i + 1, {'a': trace})
    for i, trace in enumerate(reversed(traces)):
        rig.publish_data('B', trace, n_traces - i, {'b': trace})
    receiver = Receiver(join, on_input = _publish_join_result, name = f'run005-c{credit}')
    receiver.start()
    done = rig.until(lambda: len(receiver.inputs) == n_traces or receiver.error is not None, stagnation_s)
    progress = {'groups_completed': len(receiver.inputs), 'join_status': join.join_status(),
                'buffered': _held_groups(join), 'credits': {p: join.subscription_status()[p] for p in ('A', 'B')},
                'error': repr(receiver.error) if receiver.error else None}
    entry['progress'] = progress
    # Stalled matching, not a broker outage: every credit observation is Known.
    broker_healthy = all(isinstance(o, Known) for o in progress['credits'].values())
    assert broker_healthy, f'the broker could not be observed; this is an outage, not a matching stall: {progress}'
    assert done and receiver.error is None, (
        f'RUN-005: the join accepted credit {credit} < working set {working_set} and then stagnated on a healthy '
        f'broker for {stagnation_s}s: {progress}. The runtime has neither a working-set admission '
        f'(JoinPolicy.working_set is unused by topology.provision_flow and NATSMessenger._setup) nor a durable '
        f'spill: expected a rejection before execution naming the working set.')
    for inputs in receiver.inputs:
        assert list(inputs) == ['A', 'B'], f'declared parent order not preserved: {list(inputs)}'
        assert inputs['A']['message']['a'] == inputs['B']['message']['b'], inputs
    assert {g['A']['message']['a'] for g in receiver.inputs} == set(traces)
    assert rig.until(lambda: len(rig.retained('join')) == n_traces, 10), rig.retained('join')
    receiver.stop()
    receiver.join(10)
    return 'completed'


@pytest.mark.case('RUN-005')
@pytest.mark.level('broker')
def test_run_005_adversarial_parent_ordering_cannot_deadlock_join_credits(nats_url, tmp_path, evidence_dir,
                                                                          monkeypatch) -> None:
    '''
    RUN-005 (P1, runtime, broker): Adversarial parent ordering cannot deadlock join credits.

    Acceptance: The finite workload either completes every expected group or is rejected before
    execution with a specific working-set incompatibility; indefinite healthy-network stagnation
    fails.

    Two workloads on the compose broker: four traces under a credit that covers
    the declared working set (must complete), then twelve traces under a credit
    of five that cannot (must be rejected before execution, or complete).
    '''
    record : Dict[str, Any] = {}
    outcomes : Dict[str, str] = {}
    try:
        rig = BrokerRig(nats_url, 'run005a')
        try:
            outcomes['compatible'] = _oracle_run_005(rig, str(tmp_path / 'a'), record, 4, 5, 4, 30.0)
        finally:
            rig.close()
        assert outcomes['compatible'] == 'completed', outcomes
        rig = BrokerRig(nats_url, 'run005b')
        try:
            outcomes['incompatible'] = _oracle_run_005(rig, str(tmp_path / 'b'), record, 12, 5, 12, 20.0)
        finally:
            rig.close()
    finally:
        record['outcomes'] = outcomes
        write_evidence(evidence_dir, 'working_set.json', record)


@pytest.mark.case('RUN-005')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_run_005_memory_small_credits_reject_or_complete(tmp_path, evidence_dir, monkeypatch) -> None:
    record : Dict[str, Any] = {}
    outcomes : Dict[str, str] = {}
    try:
        rig = ModelRig()
        try:
            outcomes['compatible'] = _oracle_run_005(rig, str(tmp_path / 'a'), record, 4, 5, 4, 15.0)
        finally:
            rig.close()
        assert outcomes['compatible'] == 'completed', outcomes
        rig = ModelRig()
        try:
            outcomes['incompatible'] = _oracle_run_005(rig, str(tmp_path / 'b'), record, 12, 5, 12, 8.0)
        finally:
            rig.close()
    finally:
        record['outcomes'] = outcomes
        write_evidence(evidence_dir, 'working_set.json', record)


@pytest.mark.negative_control(of = 'RUN-005')
def test_run_005_detects_a_fixed_credit_that_deadlocks(tmp_path, monkeypatch) -> None:
    '''A runtime that computes no working set (credit fixed at bind, wait forever) deadlocks: the oracle must catch it.'''
    defects_run1.fixed_join_credit(monkeypatch)
    rig = ModelRig()
    try:
        # A join that deadlocks shows no progress at all; 1.5 s of it is as telling as 4.
        assert defects.detects(_oracle_run_005, rig, str(tmp_path / 'ledger'), {}, 12, 5, 12, 1.5)
    finally:
        rig.close()


# -- RUN-006 --------------------------------------------------------------------------

class _ClockShim:
    '''``time`` as the grouping module sees it: the rig's fake clock, so a join timeout is a clock the test moves.'''
    def __init__(self, rig : ModelRig) -> None:
        self._rig = rig

    def monotonic(self) -> float:
        return self._rig.clock.monotonic()

    def time(self) -> float:
        return self._rig.clock.time()


class _Warnings(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level = logging.WARNING)
        self.messages : List[str] = []

    def emit(self, record : logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def _oracle_run_006(rig : ModelRig, store_dir : str, record : Dict[str, Any], observe_s : float = 10.0) -> None:
    '''
    Three children of the same two parents, each with a declared policy for a
    branch that never comes: a bounded ``drop`` resolves trace1 once at its
    deadline (the half acked, the eviction counted, the missing member named);
    a bounded ``error`` hands the half back to the broker; the explicitly
    unbounded ``wait`` stays observable as *waiting* — never as healthy progress
    — until the control stop cancels it and its half returns to the broker for a
    replacement.

    ``observe_s`` bounds how long a held half may take to show up as a pending
    group. It shows up at once on a correct join, and never on one that reports
    nothing, so the negative control passes a short bound.
    '''
    timeout = 5.0
    specs = [spec('A', [], 'producer', True), spec('B', [], 'producer', True),
             spec('bounded', ['A', 'B'], 'consumer', False), spec('errored', ['A', 'B'], 'consumer', False),
             spec('waiting', ['A', 'B'], 'consumer', False)]
    rig.provision(specs, ack_wait = 60)
    warnings = _Warnings()
    logging.getLogger('videoflow.messaging').addHandler(warnings)
    try:
        bounded = rig.messenger('bounded', ['A', 'B'], runtime = _join_runtime(store_dir, rig, 'bounded'), ack_wait = 60,
                                join_policy = JoinPolicy(timeout_seconds = timeout, missing = 'drop').to_dict())
        errored = rig.messenger('errored', ['A', 'B'], runtime = _join_runtime(store_dir, rig, 'errored'), ack_wait = 60,
                                join_policy = JoinPolicy(timeout_seconds = timeout, missing = 'error').to_dict())
        waiting = rig.messenger('waiting', ['A', 'B'], runtime = _join_runtime(store_dir, rig, 'waiting'), ack_wait = 60,
                                join_policy = JoinPolicy(missing = 'wait').to_dict())
        a_id = rig.publish_data('A', 'trace1', 1, {'a': 1})
        receivers = [Receiver(m, name = f'run006-{m._node.name}') for m in (bounded, errored, waiting)]
        for r in receivers:
            r.start()
        for m in (bounded, errored, waiting):
            assert rig.until(lambda m = m: m.join_status()['pending_groups'] == 1, observe_s), m.join_status()
        clock_0 = rig.clock.monotonic()
        # Short of the deadline nothing resolves; past it (plus one poll of scheduler
        # tolerance) the bounded policies resolve trace1 exactly once.
        rig.wait(timeout - 1.0)
        time.sleep(0.3)
        assert bounded.join_status()['pending_groups'] == 1 and errored.join_status()['pending_groups'] == 1
        record['before_deadline'] = {'clock': rig.clock.monotonic() - clock_0, 'bounded': bounded.join_status(),
                                     'waiting': waiting.join_status()}
        rig.wait(1.5)
        assert rig.until(lambda: bounded.join_status()['pending_groups'] == 0, 10), bounded.join_status()
        assert rig.until(lambda: bounded.take_drops().get('join_evicted') == 1, 5), 'the eviction was not counted'
        assert rig.until(lambda: _settled(rig, 'bounded', ['A']), 5), rig.consumer_state('bounded', 'A')
        named = [m for m in warnings.messages if 'bounded' in m and 'trace1' in m]
        assert named and "had ['A'], needed ['A', 'B']" in named[0], warnings.messages
        record['bounded'] = {'clock': rig.clock.monotonic() - clock_0, 'evictions': 1, 'report': named[0],
                             'consumer_a': rig.consumer_state('bounded', 'A').value}
        # ``error``: the half goes back to the broker (naked, never terminated) for another attempt.
        assert rig.until(lambda: errored.take_drops().get('join_evicted') == 1, 5)
        assert rig.until(lambda: any(d['attempt'] >= 2 for d in rig.deliveries('errored')), 10), \
            rig.deliveries('errored')
        assert [r.publication_id for r in rig.retained('A')] == [a_id]
        record['errored'] = {'deliveries': rig.deliveries('errored')}
        # Nothing resolves twice: more time changes neither bounded outcome.
        rig.wait(3 * timeout)
        time.sleep(0.3)
        assert bounded.take_drops() == {} and bounded.join_status()['pending_groups'] == 0
        # ``wait``: declared unbounded, reported as waiting, never as healthy progress.
        status = waiting.join_status()
        assert status['missing'] == 'wait' and status['bounded'] is False and status['timeout_seconds'] is None, status
        assert status['pending_groups'] == 1 and status['oldest_wait_seconds'] >= 4 * timeout, status
        pending = waiting.pending_observation()
        assert isinstance(pending, Known) and pending.value >= 1, pending
        record['waiting'] = {'status': status, 'pending_observation': pending.value, 'clock': rig.clock.monotonic() - clock_0}
        # Cancellation: the control stop ends the wait at once...
        cancelled_at = time.monotonic()
        waiting.quiesce()
        r_wait = receivers[2]
        assert rig.until(lambda: r_wait.terminal is not None, 10), 'the control stop did not interrupt WAIT'
        record['cancel'] = {'seconds': time.monotonic() - cancelled_at}
        # ...and ownership of the half is released: once its lease lapses the broker
        # hands trace1's A to a replacement of the same logical node.
        waiting.close()
        rig.wait(61)
        replacement = rig.messenger('waiting', ['A', 'B'], runtime = _join_runtime(store_dir, rig, 'waiting'),
                                    ack_wait = 60, join_policy = JoinPolicy(missing = 'wait').to_dict())
        r_repl = Receiver(replacement, name = 'run006-replacement')
        r_repl.start()
        assert rig.until(lambda: _held_attempt(replacement, 'A', 'trace1') is not None, 10), \
            'the cancelled worker\'s half was not transferred to the replacement'
        record['transfer'] = {'deliveries': rig.deliveries('waiting'), 'held_attempt': _held_attempt(replacement, 'A', 'trace1')}
        for r in receivers + [r_repl]:
            r.stop()
        for r in receivers + [r_repl]:
            r.join(10)
            assert r.error is None, r.error
    finally:
        logging.getLogger('videoflow.messaging').removeHandler(warnings)


@pytest.mark.case('RUN-006')
@pytest.mark.level('process')
def test_run_006_a_permanently_missing_join_branch_follows_a_declared(tmp_path, evidence_dir, monkeypatch) -> None:
    '''
    RUN-006 (P1, runtime, process): A permanently missing join branch follows a declared
    outcome.

    Acceptance: Bounded joins resolve by their configured deadline plus declared scheduler
    tolerance; explicitly unbounded joins remain observable and cancellable.

    In-process on the memory backend under a controllable clock: the grouping
    module's ``time`` is the rig's fake clock, so the join deadline is crossed by
    the test's hand, not by waiting.
    '''
    rig = ModelRig(auto_advance = 0.0)
    monkeypatch.setattr(grouping, 'time', _ClockShim(rig))
    record : Dict[str, Any] = {}
    try:
        _oracle_run_006(rig, str(tmp_path / 'ledger'), record)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'join_outcomes.json', record)


@pytest.mark.negative_control(of = 'RUN-006')
def test_run_006_detects_an_unobservable_wait(tmp_path, monkeypatch) -> None:
    '''A join whose indefinite wait reports nothing (no pending groups, no age) reads as healthy: the oracle must catch it.'''
    defects_run1.unobservable_wait(monkeypatch)
    rig = ModelRig(auto_advance = 0.0)
    monkeypatch.setattr(grouping, 'time', _ClockShim(rig))
    try:
        assert defects.detects(_oracle_run_006, rig, str(tmp_path / 'ledger'), {}, observe_s = 2.0)
    finally:
        rig.close()


# -- RUN-016 --------------------------------------------------------------------------

#: Exactly representable event times that round to the same microsecond: 2^-22 s
#: apart, one binary ulp of a double near 1.7e9.
T_EXACT = 1700000000.5
T_ULP = 2.0 ** -22
EPOCH_A = '3f9c1a2b7d4e'
EPOCH_B = '77e1b2c3d4f5'


def _entry(parent : str, epoch : str, seq : int, ts : float) -> EnvelopeEntry:
    return EnvelopeEntry(trace_id = f'{parent}:{epoch}:{seq}', seq = seq, event_ts = ts, message = seq,
                         metadata = None, is_stop_signal = False, producer_name = parent)


def _assemble(policy : JoinPolicy, arrivals : List[tuple[str, EnvelopeEntry]]) -> List[Any]:
    assembler = TimeGroupAssembler('fuse', ['cam', 'imu'], policy)
    for parent, entry in arrivals:
        assembler.add(parent, entry, object())
    groups = []
    while (ready := assembler.pop_ready(now = 1e9)) is not None:
        groups.append(ready)
    return groups


def _oracle_run_016(record : Dict[str, Any]) -> None:
    '''
    Two groups of different members whose event times round to the same
    microsecond assemble, in every parent-arrival order, to distinct ids; the
    first set reassembled after a simulated recovery keeps its id; the id changes
    with the members' source epoch and with the window namespace.
    '''
    policy = JoinPolicy(mode = 'time', tolerance_ms = 1.0)
    first = {'cam': _entry('cam', EPOCH_A, 5, T_EXACT), 'imu': _entry('imu', EPOCH_B, 11, T_EXACT)}
    second = {'cam': _entry('cam', EPOCH_A, 6, T_EXACT + T_ULP), 'imu': _entry('imu', EPOCH_B, 12, T_EXACT + T_ULP)}
    assert T_EXACT != T_EXACT + T_ULP and round(T_EXACT * 1e6) == round((T_EXACT + T_ULP) * 1e6)
    orders = {
        'cam-first': [('cam', first['cam']), ('imu', first['imu']), ('cam', second['cam']), ('imu', second['imu'])],
        'imu-first': [('imu', first['imu']), ('cam', first['cam']), ('imu', second['imu']), ('cam', second['cam'])],
        'interleaved': [('cam', first['cam']), ('cam', second['cam']), ('imu', first['imu']), ('imu', second['imu'])],
    }
    table : Dict[str, Any] = {}
    ids_of_first : set[str] = set()
    for name, arrivals in orders.items():
        groups = _assemble(policy, arrivals)
        assert len(groups) == 2, f'{name}: {len(groups)} groups assembled'
        by_members = {tuple(sorted((p, e.trace_id) for p, e in g.entries.items())): g for g in groups}
        table[name] = {str(members): {'trace_id': g.trace_id, 'seq': g.seq} for members, g in by_members.items()}
        first_key = tuple(sorted((p, e.trace_id) for p, e in first.items()))
        second_key = tuple(sorted((p, e.trace_id) for p, e in second.items()))
        g1, g2 = by_members[first_key], by_members[second_key]
        assert g1.seq == g2.seq == round(T_EXACT * 1e6), (g1.seq, g2.seq)
        assert g1.trace_id != g2.trace_id, f'{name}: distinct member sets collided on {g1.trace_id}'
        ids_of_first.add(g1.trace_id)
    assert len(ids_of_first) == 1, f'the first member set minted different ids per arrival order: {ids_of_first}'
    # Replay after recovery: the same member set, a fresh assembler, the same id.
    replay = _assemble(policy, [('imu', first['imu']), ('cam', first['cam'])])
    assert len(replay) == 1 and replay[0].trace_id in ids_of_first, (replay[0].trace_id, ids_of_first)
    # The reference identity, pinned to the RFC 0006 §4 worked example.
    members = {p: (e.producer_name, e.trace_id, e.seq) for p, e in first.items()}
    reference = group_identity(members, None, round(T_EXACT * 1e6))
    assert reference == 'tw-1700000000500000-2604c79085f2' and reference in ids_of_first, (reference, ids_of_first)
    second_members = {p: (e.producer_name, e.trace_id, e.seq) for p, e in second.items()}
    assert group_identity({**members, 'cam': ('cam', f'cam:{EPOCH_A}:6', 6)}, None, round(T_EXACT * 1e6)) \
        == 'tw-1700000000500000-c5024ad18fdc'
    # Namespaces: a member from another source epoch, or another window, is another group.
    other_epoch = {**members, 'cam': ('cam', 'cam:0badc0ffee00:5', 5)}
    assert group_identity(other_epoch, None, round(T_EXACT * 1e6)) != reference
    assert group_identity(members, 'window-1', round(T_EXACT * 1e6)) != reference
    record['table'] = table
    record['replay'] = replay[0].trace_id
    record['reference'] = {'first': reference, 'second': group_identity(second_members, None, round(T_EXACT * 1e6)),
                           'other_epoch': group_identity(other_epoch, None, round(T_EXACT * 1e6)),
                           'windowed': group_identity(members, 'window-1', round(T_EXACT * 1e6))}


@pytest.mark.case('RUN-016')
@pytest.mark.level('model')
def test_run_016_distinct_time_aligned_groups_cannot_collide_after(evidence_dir, monkeypatch) -> None:
    '''
    RUN-016 (P0, runtime, model): Distinct time-aligned groups cannot collide after timestamp
    rounding.

    Acceptance: No collision occurs for the adversarial member sets, and replay of an identical
    set is identity-stable.
    '''
    record : Dict[str, Any] = {}
    try:
        _oracle_run_016(record)
    finally:
        write_evidence(evidence_dir, 'group_ids.json', record)


@pytest.mark.negative_control(of = 'RUN-016')
def test_run_016_detects_timestamp_only_group_ids(monkeypatch) -> None:
    '''``tw-{µs}`` alone collides two groups at one rounded time: the oracle must catch it.'''
    defects_run1.timestamp_only_group_identity(monkeypatch)
    assert defects.detects(_oracle_run_016, {})
