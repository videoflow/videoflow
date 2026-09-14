'''
Conformance cases: RUN-003, RUN-004, RUN-013, RUN-017, RUN-022.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. The oracles are plain functions over a rig (``_runs2``) —
the memory backends with a ``file://`` ledger for the process-level primaries, the
compose broker for the subprocess and toxiproxy variants — so the paired negative
control (``defects_run2.py``) runs the same assertions against the reviewed defect.

What they have in common: the runtime ledger's *commit* — the outbox intent, the
checkpoint written in the same record as the output it belongs to, the committed
body a nondeterministic component replays, the effect marker a sink writes after its
external effect — and what a crash at each boundary must not be able to undo.
'''
from __future__ import absolute_import, division, print_function

import json
import time
from typing import Any, Callable, Dict, List, Optional

import defects
import defects_run2
import pytest
from _runnodes import (
    Accumulator,
    ExternalLedger,
    KeyedSink,
    PlainSink,
    StochasticInference,
)
from _runs2 import (
    Collector,
    ContextSchedule,
    JetStreamRig,
    TaskThread,
    WorkerProcess,
    allow_task_threads,
    batch_specs,
    checkpoint_snapshot,
    consumer_task,
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
    wait_until,
    worker_env,
    write_evidence,
)
from _toxiproxy import Toxiproxy

from videoflow.backends import faults
from videoflow.backends.capabilities import (
    EFFECT_IDEMPOTENT_KEY,
    RELIABLE_WORK,
    FlowRequirements,
    ProfileRequest,
    plan_composition,
)
from videoflow.backends.memory.runtime_store import MemoryRuntimeStore
from videoflow.backends.runtime import OUTCOME_ACCEPTED, OUTCOME_DUPLICATE, OUTCOME_INTENT, OUTCOME_UNKNOWN
from videoflow.core.compiler import sink_guarantees
from videoflow.core.constants import BATCH
from videoflow.core.errors import IncompatibleProfile, StaleAuthority
from videoflow.messaging import nats_messenger
from videoflow.runtime.idempotency import EFFECT_RETENTION_SECONDS, LedgerIdempotencyStore, RedisIdempotencyStore

pytestmark = pytest.mark.timeout(180)

RECORDS = [{'key': 'a', 'value': 1}, {'key': 'b', 'value': 10}, {'key': 'a', 'value': 2},
           {'key': 'b', 'value': 20}, {'key': 'a', 'value': 3}]


def _publish_records(src : Any, records : List[Dict[str, Any]], eos : bool = True) -> None:
    for record in records:
        src.publish_message(record)
    if eos:
        src.publish_stop_signal()


def _run_accumulator(rig : Any, store : Any, node_factory : Callable[[], Accumulator], schedule : Any,
                     evidence : Dict[str, Any], label : str, run_id : Optional[str] = None,
                     timeout : float = 60.0) -> TaskThread:
    '''One process of the accumulator: a fresh node, ledger and messenger, run until it ends.'''
    node = node_factory()
    runtime = runtime_for(store, rig, 'acc', parent_replicas = {'src': 1}, run_id = run_id)
    messenger = messenger_for(rig, node, ['src'], runtime = runtime, run_id = run_id)
    task = TaskThread(processor_task(rig, node, messenger, ['src'], run_id = run_id), name = f'acc-{label}')
    if schedule is not None:
        with schedule:
            task.start()
            task.wait(timeout)
    else:
        task.start()
        task.wait(timeout)
    evidence.setdefault('processes', []).append({
        'label': label, 'ended_by': type(task.error).__name__ if task.error else 'end-of-stream',
        'restored_from': node.restored_from, 'invocations': list(node.invocations),
        'publication_stats': dict(messenger.publication_stats), 'outbox': outbox_snapshot(runtime),
        'checkpoint': checkpoint_snapshot(runtime),
    })
    task.node = node  # type: ignore[attr-defined]
    task.messenger = messenger  # type: ignore[attr-defined]
    return task


# -- RUN-003 ----------------------------------------------------------------------

def _oracle_run_003(rig : Any, store_for : Callable[[str], Any], evidence : Dict[str, Any],
                    crash : Callable[[], faults.RaiseError] = crash_at) -> List[faults.FaultSchedule]:
    '''
    Three runs of src → acc (stateful) → sink, each with one fault in acc's commit
    sequence, each finished by a replacement process over the same ledger:

    - ``commit-before-send``: acc dies after its state and output are committed \
      (``group.commit.after``) and before the send.
    - ``accepted-ack-lost``: the broker accepted the output and acc dies before it \
      reads the receipt (``publish.receipt.before``) — the outbox must be replayed.
    - ``receipt-dropped``: the receipt is lost (``PublicationUnknown``) and acc \
      reconciles it in-process through the same identity.
    '''
    schedules = []
    runs = [
        ('commit-before-send', {'group.commit.after': faults.Nth(2, crash())}, 2),
        ('accepted-ack-lost', {'publish.receipt.before': faults.Nth(3, crash())}, 3),
        ('receipt-dropped', {'publish.receipt.before': faults.Nth(2, faults.DropResponse())}, 2),
    ]
    # Only acc publishes while a schedule is installed (src published before it),
    # and only the output commit's group.commit hits count — not the group record's.
    when = {'group.commit.after': output_commit}
    for label, actions, faulted_index in runs:
        run_id = f'{rig.run_id}-{label}'
        provision_run(rig, run_id, batch_specs())
        store = store_for(label)
        sink = Collector(rig, 'acc', run_id = run_id)
        sink.start()
        src = rig.messenger('src', [], run_id = run_id, replayable = True)
        crashes = label != 'receipt-dropped'
        # The end of stream follows the crash: a terminator's ack is a settle too,
        # and the replacement must find the stream still open when it starts.
        _publish_records(src, RECORDS, eos = not crashes)
        schedule = ContextSchedule(actions, when)
        schedules.append(schedule)
        run_evidence : Dict[str, Any] = {'faults': {k: repr(v) for k, v in actions.items()}}
        first = _run_accumulator(rig, store, lambda: Accumulator(name = 'acc'), schedule, run_evidence, 'first', run_id)
        if crashes:
            src.publish_stop_signal()
        faulted_pid = publication_id(rig, 'acc', f'src:{faulted_index}', faulted_index, run_id)
        run_evidence['faulted_group'] = faulted_pid
        if label == 'receipt-dropped':
            # No crash: the ambiguity was reconciled in-process (unknown → accepted, same id).
            assert first.error is None, first.error
            entry = runtime_for(store, rig, 'acc', run_id = run_id).outbox_entry(faulted_pid)
            assert entry is not None and entry.outcome in (OUTCOME_ACCEPTED, OUTCOME_DUPLICATE), entry
            assert first.messenger.publication_stats.get('unknown') == 1, first.messenger.publication_stats  # type: ignore[attr-defined]
        else:
            assert first.crashed, f'{label}: the first process did not die at the barrier: {first.error!r}'
            # Durably visible before any replacement starts: the intent, and the
            # state/output record it belongs to, in the ledger.
            after_crash = runtime_for(store, rig, 'acc', run_id = run_id)
            entry = after_crash.outbox_entry(faulted_pid)
            assert entry is not None and entry.outcome == OUTCOME_INTENT, entry
            state, position = after_crash.restore_checkpoint()
            assert state is not None and position.get('group') == faulted_pid, position
            assert position.get('output', {}).get('publication_id') == faulted_pid, position
            run_evidence['after_crash'] = {'outbox': entry.outcome, 'checkpoint_group': position.get('group'),
                                           'state': json.loads(state.decode())}
            second = _run_accumulator(rig, store, lambda: Accumulator(name = 'acc'), None, run_evidence, 'replacement', run_id)
            assert second.error is None, f'{label}: the replacement failed: {second.error!r}'
            # The replacement restored the committed state and never recomputed the faulted group.
            assert second.node.restored_from is not None  # type: ignore[attr-defined]
            recomputed = [i for i in second.node.invocations if i['input'] == faulted_pid]  # type: ignore[attr-defined]
            assert not recomputed, f'{label}: the committed group was handed to the node again: {recomputed}'
            final = runtime_for(store, rig, 'acc', run_id = run_id).outbox_entry(faulted_pid)
            assert final is not None and final.outcome in (OUTCOME_ACCEPTED, OUTCOME_DUPLICATE), final
            run_evidence['after_recovery'] = {'outbox': final.outcome}
        sink.finish(60)
        received = sink.snapshot()
        identities = [(r['trace_id'], r['seq']) for r in received]
        run_evidence['sink'] = [{'trace_id': r['trace_id'], 'seq': r['seq'], 'message': r['message']} for r in received]
        # One output per group, under the original identity, carrying the committed
        # payload (a redelivery after the crash may arrive after later groups).
        assert sorted(identities) == sorted((f'src:{i}', i) for i in range(1, len(RECORDS) + 1)), identities
        state_final, _ = runtime_for(store, rig, 'acc', run_id = run_id).restore_checkpoint()
        totals = json.loads(state_final.decode())['totals']
        assert totals == reference_totals(RECORDS), (totals, reference_totals(RECORDS))
        # State and output agree: each output is one state revision, and the running
        # totals the sink saw are exactly the fold over the groups in the order the
        # state applied them.
        revisions = [r['message']['revision'] for r in received]
        assert sorted(revisions) == list(range(1, len(RECORDS) + 1)), revisions
        by_revision = sorted(received, key = lambda r: r['message']['revision'])
        running : Dict[str, int] = {}
        for r in by_revision:
            record = RECORDS[r['seq'] - 1]
            running[record['key']] = running.get(record['key'], 0) + record['value']
            assert r['message']['total'] == running[record['key']], (r, running)
        assert all(not r['message']['replayed'] for r in received)
        evidence[label] = run_evidence
    return schedules


@pytest.mark.case('RUN-003')
@pytest.mark.level('process')
def test_run_003_state_transition_and_ambiguous_output_publication_recover(tmp_path, evidence_dir, record_faults,
                                                                             monkeypatch) -> None:
    '''
    RUN-003 (P0, runtime, process): State transition and ambiguous output publication recover
    from an outbox.

    Acceptance: All committed state transitions have a corresponding eventually resolved output;
    replay introduces no duplicate logical state transition.
    '''
    allow_task_threads(monkeypatch)
    evidence : Dict[str, Any] = {}
    rig = memory_rig(batch_specs())
    try:
        with rig.ticking():
            schedules = _oracle_run_003(rig, lambda label: ledger(tmp_path, label), evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'outbox_lifecycle.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


def _run_accumulator_worker(rig : Any, store_dir : str, schedule : Any, evidence : Dict[str, Any], label : str,
                            run_id : str, timeout : float = 90.0) -> WorkerProcess:
    '''One real worker process hosting acc, over the compose broker and the file ledger.'''
    env = worker_env(rig, Accumulator(name = 'acc'), 'processor', ['src'], True, f'file://{store_dir}',
                     run_id = run_id, schedule = schedule, parent_replicas = [1])
    worker = WorkerProcess(env, name = f'acc-{label}')
    worker.run(timeout)
    evidence.setdefault('processes', []).append(worker.record())
    return worker


@pytest.mark.case('RUN-003')
@pytest.mark.level('process')
@pytest.mark.variant('subprocess')
def test_run_003_a_real_worker_recovers_its_outbox_on_the_broker(nats_url, tmp_path, evidence_dir, record_faults,
                                                                  monkeypatch) -> None:
    '''
    The same story with real deaths: ``videoflow.runtime.worker`` processes on the
    compose broker, ``os._exit`` at the barrier, a ``file://`` ledger, and a
    replacement process that reconciles the outbox before it receives anything.
    '''
    evidence : Dict[str, Any] = {}
    rig = JetStreamRig(nats_url, BATCH, batch_specs(), ack_wait = 5)
    schedules = []
    try:
        runs = [
            ('commit-before-send', {'checkpoint.write.after': faults.Nth(2, faults.Crash(137))}, 2),
            ('accepted-ack-lost', {'publish.receipt.before': faults.Nth(3, faults.Crash(137))}, 3),
        ]
        for label, actions, faulted_index in runs:
            run_id = f'{rig.run_id}-{label}'
            rig.provision_run(run_id)
            store_dir = str(tmp_path / label)
            sink = Collector(rig, 'acc', run_id = run_id)
            sink.start()
            src = rig.messenger('src', [], run_id = run_id, replayable = True)
            _publish_records(src, RECORDS, eos = False)
            schedule = faults.FaultSchedule(actions, marker_dir = str(tmp_path / f'markers-{label}'))
            schedules.append(schedule)
            run_evidence : Dict[str, Any] = {'faults': {k: repr(v) for k, v in actions.items()}}
            first = _run_accumulator_worker(rig, store_dir, schedule, run_evidence, 'first', run_id)
            assert first.exit_code == 137, f'{label}: the worker did not die at the barrier: {first.record()}'
            src.publish_stop_signal()
            store = ledger(tmp_path, label)
            faulted_pid = publication_id(rig, 'acc', f'src:{faulted_index}', faulted_index, run_id)
            after_crash = runtime_for(store, rig, 'acc', run_id = run_id)
            entry = after_crash.outbox_entry(faulted_pid)
            assert entry is not None and entry.outcome == OUTCOME_INTENT, entry
            state, position = after_crash.restore_checkpoint()
            assert state is not None and position.get('group') == faulted_pid, position
            run_evidence['after_crash'] = {'outbox': entry.outcome, 'checkpoint': checkpoint_snapshot(after_crash)}
            second = _run_accumulator_worker(rig, store_dir, None, run_evidence, 'replacement', run_id)
            assert second.exit_code == 0, f'{label}: the replacement failed: {second.record()}'
            sink.finish(60)
            received = sink.snapshot()
            identities = [(r['trace_id'], r['seq']) for r in received]
            run_evidence['sink'] = [{'trace_id': r['trace_id'], 'seq': r['seq'], 'message': r['message']} for r in received]
            assert sorted(identities) == sorted((f'src:{i}', i) for i in range(1, len(RECORDS) + 1)), identities
            assert sorted(r['message']['revision'] for r in received) == list(range(1, len(RECORDS) + 1))
            final_state, _ = runtime_for(store, rig, 'acc', run_id = run_id).restore_checkpoint()
            assert json.loads(final_state.decode())['totals'] == reference_totals(RECORDS)
            final = runtime_for(store, rig, 'acc', run_id = run_id).outbox_entry(faulted_pid)
            assert final is not None and final.outcome in (OUTCOME_ACCEPTED, OUTCOME_DUPLICATE), final
            run_evidence['after_recovery'] = {'outbox': final.outcome}
            evidence[label] = run_evidence
    finally:
        rig.close()
        write_evidence(evidence_dir, 'outbox_lifecycle.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.negative_control(of = 'RUN-003')
def test_run_003_detects_a_checkpoint_written_apart_from_its_output(tmp_path, monkeypatch) -> None:
    allow_task_threads(monkeypatch)
    defects_run2.eager_checkpoint(monkeypatch)
    rig = memory_rig(batch_specs())
    try:
        with rig.ticking():
            assert defects.detects(_oracle_run_003, rig, lambda label: ledger(tmp_path, label), {})
    finally:
        rig.close()


# -- RUN-004 ----------------------------------------------------------------------

INPUTS = [1, 2, 3, 4]


def _run_inference(rig : Any, store : Any, node_factory : Callable[[], Any], schedule : Any,
                   evidence : Dict[str, Any], label : str, run_id : str, timeout : float = 60.0) -> TaskThread:
    node = node_factory()
    runtime = runtime_for(store, rig, 'inf', parent_replicas = {'src': 1}, run_id = run_id)
    messenger = messenger_for(rig, node, ['src'], runtime = runtime, run_id = run_id)
    task = TaskThread(processor_task(rig, node, messenger, ['src'], run_id = run_id), name = f'inf-{label}')
    if schedule is not None:
        with schedule:
            task.start()
            task.wait(timeout)
    else:
        task.start()
        task.wait(timeout)
    evidence.setdefault('processes', []).append({
        'label': label, 'ended_by': type(task.error).__name__ if task.error else 'end-of-stream',
        'invocations': list(node.invocations), 'publication_stats': dict(messenger.publication_stats),
        'outbox': outbox_snapshot(runtime),
    })
    task.node = node  # type: ignore[attr-defined]
    return task


def _committed_nonce(store : Any, rig : Any, run_id : str, pid : str, node : str = 'inf') -> Optional[str]:
    '''The nonce inside the result the ledger committed for ``pid`` — the bytes a recovery replays.'''
    from videoflow.wire.serialization import decode_envelope
    raw, _v = store.get(runtime_for(store, rig, node, run_id = run_id).key('committed', pid))
    if raw is None:
        return None
    return str(decode_envelope(raw)['message']['nonce'])


def _oracle_run_004(rig : Any, store_for : Callable[[str], Any], evidence : Dict[str, Any],
                    crash : Callable[[], faults.RaiseError] = crash_at) -> List[faults.FaultSchedule]:
    '''
    Two crashes around the commitment of a stochastic result, each finished by a
    replacement over the same ledger, then the admission half:

    - ``committed-unacked``: the result is committed and accepted by the broker; \
      the process dies before the receipt (``publish.receipt.before``). Recovery \
      must republish the stored bytes under the same identity and never invoke \
      the component again for that input.
    - ``uncommitted``: the process dies after computing and before committing \
      (``group.commit.before``). Recomputation is allowed — and visible: two \
      invocations with two nonces, only the second of which was ever committed.
    '''
    schedules = []
    faulted = 2
    when = {'group.commit.before': output_commit, 'group.commit.after': output_commit}
    for label, actions in (('committed-unacked', {'publish.receipt.before': faults.Nth(faulted, crash())}),
                           ('uncommitted', {'group.commit.before': faults.Nth(faulted, crash())})):
        run_id = f'{rig.run_id}-{label}'
        provision_run(rig, run_id, batch_specs('inf'))
        store = store_for(label)
        sink = Collector(rig, 'inf', run_id = run_id)
        sink.start()
        src = rig.messenger('src', [], run_id = run_id, replayable = True)
        _publish_records(src, INPUTS, eos = False)
        schedule = ContextSchedule(actions, when)
        schedules.append(schedule)
        run_evidence : Dict[str, Any] = {'faults': {k: repr(v) for k, v in actions.items()}}
        first = _run_inference(rig, store, lambda: StochasticInference(name = 'inf'), schedule, run_evidence,
                               'first', run_id)
        assert first.crashed, f'{label}: the first process did not die at the barrier: {first.error!r}'
        src.publish_stop_signal()
        pid = publication_id(rig, 'inf', f'src:{faulted}', faulted, run_id)
        first_nonces = [i['nonce'] for i in first.node.invocations if i['input'] == pid]  # type: ignore[attr-defined]
        assert len(first_nonces) == 1, first_nonces
        committed_before = _committed_nonce(store, rig, run_id, pid)
        entry = runtime_for(store, rig, 'inf', run_id = run_id).outbox_entry(pid)
        run_evidence['after_crash'] = {'outbox': entry.outcome if entry else None, 'committed_nonce': committed_before,
                                       'computed_nonce': first_nonces[0]}
        second = _run_inference(rig, store, lambda: StochasticInference(name = 'inf'), None, run_evidence,
                                'replacement', run_id)
        assert second.error is None, f'{label}: the replacement failed: {second.error!r}'
        sink.finish(60)
        received = sink.snapshot()
        identities = [(r['trace_id'], r['seq']) for r in received]
        assert sorted(identities) == sorted((f'src:{i}', i) for i in range(1, len(INPUTS) + 1)), identities
        delivered = [r['message']['nonce'] for r in received if r['seq'] == faulted]
        second_nonces = [i['nonce'] for i in second.node.invocations if i['input'] == pid]  # type: ignore[attr-defined]
        committed_after = _committed_nonce(store, rig, run_id, pid)
        final = runtime_for(store, rig, 'inf', run_id = run_id).outbox_entry(pid)
        run_evidence['after_recovery'] = {'outbox': final.outcome if final else None, 'committed_nonce': committed_after,
                                          'recomputed_nonces': second_nonces, 'delivered_nonce': delivered}
        run_evidence['sink'] = [{'trace_id': r['trace_id'], 'seq': r['seq'], 'nonce': r['message']['nonce']}
                                for r in received]
        assert final is not None and final.outcome in (OUTCOME_ACCEPTED, OUTCOME_DUPLICATE), final
        if label == 'committed-unacked':
            # Committed: the stored result is what recovery published — the same
            # nonce as computed, byte-for-byte the committed bytes — and the
            # component was not invoked again.
            assert committed_before == first_nonces[0] and entry is not None and entry.outcome == OUTCOME_INTENT
            assert committed_after == committed_before, (committed_before, committed_after)
            assert second_nonces == [], f'recovery recomputed a committed result: {second_nonces}'
            assert delivered == [committed_before], (delivered, committed_before)
        else:
            # Uncommitted: nothing in the ledger for it, so recomputation is the
            # recovery — and lineage tells it from a replay: a new nonce, computed
            # by the replacement, is the one committed and delivered.
            assert committed_before is None and entry is None
            assert len(second_nonces) == 1 and second_nonces[0] != first_nonces[0], (first_nonces, second_nonces)
            assert committed_after == second_nonces[0] and delivered == [second_nonces[0]]
        evidence[label] = run_evidence
    return schedules


def _admission_run_004(rig : Any, evidence : Dict[str, Any]) -> None:
    '''
    The committed replay policy on a ledger that dies with the process is
    refused before anything is published: the messenger raises the diagnostic
    at the first output, the task hands the input back unblamed and stops with
    it, the channel stays empty. The planner says the same of any restart-safe
    claim on that store.
    '''
    run_id = f'{rig.run_id}-admission'
    provision_run(rig, run_id, batch_specs('inf'))
    src = rig.messenger('src', [], run_id = run_id, replayable = True)
    _publish_records(src, INPUTS[:1], eos = False)
    memory = MemoryRuntimeStore()
    node = StochasticInference(name = 'inf')
    runtime = runtime_for(memory, rig, 'inf', run_id = run_id)
    messenger = messenger_for(rig, node, ['src'], runtime = runtime, run_id = run_id)
    task = TaskThread(processor_task(rig, node, messenger, ['src'], run_id = run_id), name = 'inf-admission')
    task.start()
    task.wait(30)
    assert isinstance(task.error, IncompatibleProfile), f'expected the refusal, got {task.error!r}'
    assert 'replay_policy' in str(task.error) and 'VF_RUNTIME_STORE_URL' in (task.error.remedy or '')
    assert not rig.retained('inf', run_id), 'data was published before the refusal'
    assert messenger.publication_stats == {}, messenger.publication_stats
    assert runtime.outbox_entry(publication_id(rig, 'inf', 'src:1', 1, run_id)) is None
    # Handed back, never blamed: the input is still deliverable, and not dead-lettered.
    assert wait_until(lambda: rig.consumer_state('inf', 'src', run_id = run_id).unacked == 0, 10)
    assert rig.dead_letters('inf', run_id = run_id) == []
    with pytest.raises(IncompatibleProfile) as planned:
        plan_composition(FlowRequirements(profiles = (ProfileRequest('inf', RELIABLE_WORK),), restart_safe = True),
                         _messaging_caps(), runtime = memory.capabilities())
    evidence['admission'] = {'messenger': str(task.error), 'remedy': task.error.remedy,
                             'planner': str(planned.value), 'store': memory.capabilities().store}


def _messaging_caps() -> Any:
    from videoflow.backends.capabilities import LEDGER_WINDOW, MessagingCapabilities
    from videoflow.backends.outcomes import known
    return MessagingCapabilities(adapter = 'memory', version = '1', retained_backlog = True, recoverable_delivery = True,
                                 latest_per_key = False, dedup_window_seconds = 120, publication_ledger = LEDGER_WINDOW,
                                 replication_factor = known(1), persistent_storage = known(False),
                                 max_payload_bytes = known(1 << 20), credit_resizable = True,
                                 control_shares_data_slot = True)


@pytest.mark.case('RUN-004')
@pytest.mark.level('process')
def test_run_004_nondeterministic_inference_has_an_explicit_committed(tmp_path, evidence_dir, record_faults,
                                                                      monkeypatch) -> None:
    '''
    RUN-004 (P1, component, process): Nondeterministic inference has an explicit committed-
    result replay policy.

    Acceptance: The committed payload hash is invariant across recovery; an unsupported replay
    guarantee fails admission without publishing data.
    '''
    allow_task_threads(monkeypatch)
    evidence : Dict[str, Any] = {}
    rig = memory_rig(batch_specs('inf'))
    try:
        with rig.ticking():
            schedules = _oracle_run_004(rig, lambda label: ledger(tmp_path, label), evidence)
            _admission_run_004(rig, evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'lineage.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.negative_control(of = 'RUN-004')
def test_run_004_detects_a_recovery_that_recomputes_a_committed_result(tmp_path, monkeypatch) -> None:
    allow_task_threads(monkeypatch)
    defects_run2.recomputing_recovery(monkeypatch)
    rig = memory_rig(batch_specs('inf'))
    try:
        with rig.ticking():
            assert defects.detects(_oracle_run_004, rig, lambda label: ledger(tmp_path, label), {})
    finally:
        rig.close()


# -- RUN-022 ----------------------------------------------------------------------

def _applied_prefix_matches_position(store : Any, rig : Any, run_id : str) -> Dict[str, Any]:
    '''
    The ledger's consistency clause: the checkpointed state's applied members end
    with the group the position names, and that group's output is committed in
    the same record (or already accepted). Returns what was read, for evidence.
    '''
    runtime = runtime_for(store, rig, 'acc', run_id = run_id)
    state, position = runtime.restore_checkpoint()
    if state is None:
        return {'checkpoint': None}
    applied = json.loads(state.decode())['applied']
    group = position.get('group')
    assert applied and applied[-1] == group, (applied, group)
    entry = runtime.outbox_entry(group)
    assert entry is not None, f'checkpointed group {group} has no outbox entry'
    embedded = (position.get('output') or {}).get('publication_id') == group
    assert embedded, f'the checkpoint for {group} does not carry its output: {position}'
    return {'applied': applied, 'position_group': group, 'outbox': entry.outcome, 'embedded_output': embedded}


def _oracle_run_022(rig : Any, store_for : Callable[[str], Any], evidence : Dict[str, Any],
                    crash : Callable[[], faults.RaiseError] = crash_at) -> List[faults.FaultSchedule]:
    '''
    Three crash boundaries around the accumulator's second group — before its
    checkpoint is written, after it, and after its inputs are acknowledged — each
    followed by a replacement over the same ledger. After every crash the
    checkpoint's state and position describe one committed prefix; after every
    recovery the state equals the uninterrupted fold and the sink holds one
    output per group.
    '''
    schedules = []
    boundaries = [
        ('before-checkpoint', {'checkpoint.write.before': faults.Nth(2, crash())}),
        ('after-checkpoint', {'checkpoint.write.after': faults.Nth(2, crash())}),
        ('after-ack', {'settle.after': faults.Nth(2, crash())}),
    ]
    for label, actions in boundaries:
        run_id = f'{rig.run_id}-{label}'
        provision_run(rig, run_id, batch_specs())
        store = store_for(label)
        sink = Collector(rig, 'acc', run_id = run_id)
        sink.start()
        src = rig.messenger('src', [], run_id = run_id, replayable = True)
        _publish_records(src, RECORDS, eos = False)
        # Only acc's settlements of src's data count (its terminator ack is a settle too).
        when = {'settle.after': settling('acc', data_ids(rig, 'src', len(RECORDS), run_id))}
        schedule = ContextSchedule(actions, when)
        schedules.append(schedule)
        run_evidence : Dict[str, Any] = {'faults': {k: repr(v) for k, v in actions.items()}}
        first = _run_accumulator(rig, store, lambda: Accumulator(name = 'acc'), schedule, run_evidence, 'first', run_id)
        assert first.crashed, f'{label}: the first process did not die at the barrier: {first.error!r}'
        src.publish_stop_signal()
        run_evidence['after_crash'] = _applied_prefix_matches_position(store, rig, run_id)
        applied_at_crash = len(run_evidence['after_crash'].get('applied', []))
        assert applied_at_crash == {'before-checkpoint': 1, 'after-checkpoint': 2, 'after-ack': 2}[label], run_evidence
        second = _run_accumulator(rig, store, lambda: Accumulator(name = 'acc'), None, run_evidence, 'replacement', run_id)
        assert second.error is None, f'{label}: the replacement failed: {second.error!r}'
        restored = second.node.restored_from  # type: ignore[attr-defined]
        assert restored is not None and len(restored['applied']) == applied_at_crash, restored
        sink.finish(60)
        received = sink.snapshot()
        identities = sorted((r['trace_id'], r['seq']) for r in received)
        assert identities == sorted((f'src:{i}', i) for i in range(1, len(RECORDS) + 1)), identities
        final_state, _ = runtime_for(store, rig, 'acc', run_id = run_id).restore_checkpoint()
        final = json.loads(final_state.decode())
        assert final['totals'] == reference_totals(RECORDS), (final['totals'], reference_totals(RECORDS))
        assert len(final['applied']) == len(RECORDS) == len(set(final['applied'])), final['applied']
        assert sorted(r['message']['revision'] for r in received) == list(range(1, len(RECORDS) + 1))
        run_evidence['after_recovery'] = _applied_prefix_matches_position(store, rig, run_id)
        run_evidence['sink'] = [{'trace_id': r['trace_id'], 'seq': r['seq'], 'message': r['message']} for r in received]
        evidence[label] = run_evidence
    return schedules


def _admission_run_022(rig : Any, evidence : Dict[str, Any]) -> None:
    '''A checkpointing component on a ledger that dies with the process: refused at its first checkpoint, nothing published.'''
    run_id = f'{rig.run_id}-admission'
    provision_run(rig, run_id, batch_specs())
    src = rig.messenger('src', [], run_id = run_id, replayable = True)
    _publish_records(src, RECORDS[:1], eos = False)
    memory = MemoryRuntimeStore()
    node = Accumulator(name = 'acc')
    runtime = runtime_for(memory, rig, 'acc', run_id = run_id)
    messenger = messenger_for(rig, node, ['src'], runtime = runtime, run_id = run_id)
    task = TaskThread(processor_task(rig, node, messenger, ['src'], run_id = run_id), name = 'acc-admission')
    task.start()
    task.wait(30)
    assert isinstance(task.error, IncompatibleProfile), f'expected the refusal, got {task.error!r}'
    assert 'checkpoint' in str(task.error) and 'VF_RUNTIME_STORE_URL' in (task.error.remedy or '')
    assert not rig.retained('acc', run_id), 'data was published before the refusal'
    assert runtime.restore_checkpoint() == (None, {})
    assert wait_until(lambda: rig.consumer_state('acc', 'src', run_id = run_id).unacked == 0, 10)
    with pytest.raises(IncompatibleProfile) as planned:
        plan_composition(FlowRequirements(profiles = (ProfileRequest('acc', RELIABLE_WORK),), restart_safe = True),
                         _messaging_caps(), runtime = memory.capabilities())
    evidence['admission'] = {'messenger': str(task.error), 'planner': str(planned.value)}


@pytest.mark.case('RUN-022')
@pytest.mark.level('process')
def test_run_022_stateful_worker_restart_restores_a_consistent_checkpoint(tmp_path, evidence_dir, record_faults,
                                                                          monkeypatch) -> None:
    '''
    RUN-022 (P0, runtime, process): Stateful worker restart restores a consistent checkpoint and
    replay position.

    Acceptance: Every crash boundary converges to the reference state or is rejected before
    execution as an unsupported recovery profile.
    '''
    allow_task_threads(monkeypatch)
    evidence : Dict[str, Any] = {}
    rig = memory_rig(batch_specs())
    try:
        with rig.ticking():
            schedules = _oracle_run_022(rig, lambda label: ledger(tmp_path, label), evidence)
            _admission_run_022(rig, evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'checkpoints.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.case('RUN-022')
@pytest.mark.level('process')
@pytest.mark.variant('subprocess')
def test_run_022_a_replacement_process_restores_the_checkpoint(nats_url, tmp_path, evidence_dir, record_faults,
                                                               monkeypatch) -> None:
    '''The stateless placement replacement: a new worker process on the broker, the same ``file://`` ledger.'''
    evidence : Dict[str, Any] = {}
    rig = JetStreamRig(nats_url, BATCH, batch_specs(), ack_wait = 5)
    schedules = []
    try:
        boundaries = [
            ('before-checkpoint', {'checkpoint.write.before': faults.Nth(2, faults.Crash(137))}, 1),
            ('after-checkpoint', {'checkpoint.write.after': faults.Nth(2, faults.Crash(137))}, 2),
            ('after-ack', {'settle.after': faults.Nth(2, faults.Crash(137))}, 2),
        ]
        for label, actions, applied_at_crash in boundaries:
            run_id = f'{rig.run_id}-{label}'
            rig.provision_run(run_id)
            store_dir = str(tmp_path / label)
            sink = Collector(rig, 'acc', run_id = run_id)
            sink.start()
            src = rig.messenger('src', [], run_id = run_id, replayable = True)
            _publish_records(src, RECORDS, eos = False)
            schedule = faults.FaultSchedule(actions, marker_dir = str(tmp_path / f'markers-{label}'))
            schedules.append(schedule)
            run_evidence : Dict[str, Any] = {'faults': {k: repr(v) for k, v in actions.items()}}
            first = _run_accumulator_worker(rig, store_dir, schedule, run_evidence, 'first', run_id)
            assert first.exit_code == 137, f'{label}: the worker did not die at the barrier: {first.record()}'
            src.publish_stop_signal()
            store = ledger(tmp_path, label)
            run_evidence['after_crash'] = _applied_prefix_matches_position(store, rig, run_id)
            assert len(run_evidence['after_crash'].get('applied', [])) == applied_at_crash, run_evidence
            second = _run_accumulator_worker(rig, store_dir, None, run_evidence, 'replacement', run_id)
            assert second.exit_code == 0, f'{label}: the replacement failed: {second.record()}'
            sink.finish(60)
            received = sink.snapshot()
            identities = sorted((r['trace_id'], r['seq']) for r in received)
            assert identities == sorted((f'src:{i}', i) for i in range(1, len(RECORDS) + 1)), identities
            final_state, _ = runtime_for(store, rig, 'acc', run_id = run_id).restore_checkpoint()
            final = json.loads(final_state.decode())
            assert final['totals'] == reference_totals(RECORDS) and len(set(final['applied'])) == len(RECORDS)
            run_evidence['after_recovery'] = _applied_prefix_matches_position(store, rig, run_id)
            run_evidence['sink'] = [{'trace_id': r['trace_id'], 'seq': r['seq'], 'message': r['message']} for r in received]
            evidence[label] = run_evidence
    finally:
        rig.close()
        write_evidence(evidence_dir, 'checkpoints.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.negative_control(of = 'RUN-022')
def test_run_022_detects_a_checkpoint_that_does_not_carry_its_output(tmp_path, monkeypatch) -> None:
    allow_task_threads(monkeypatch)
    defects_run2.eager_checkpoint(monkeypatch)
    rig = memory_rig(batch_specs())
    try:
        with rig.ticking():
            assert defects.detects(_oracle_run_022, rig, lambda label: ledger(tmp_path, label), {})
    finally:
        rig.close()


# -- RUN-017 ----------------------------------------------------------------------

VALUES = [1, 2, 3, 4, 5]
RETENTION = 60.0                 # the accelerated marker window (model seconds)
HORIZON = 7 * 86400.0            # the dead-letter replay horizon a deployment admits


def _sink_env(rig : Any, root : Any, sink_cls : type, store_factory : Callable[[Any], Any], label : str,
              run_id : str) -> Dict[str, Any]:
    '''A sink node, its ledger and messenger, and the marker store the task is given.'''
    external = ExternalLedger(str(root / f'{label}-external.json'))
    node = sink_cls(ledger_path = external.path, actor = label, name = 'sink')
    store = ledger(root, f'{label}-ledger')
    runtime = runtime_for(store, rig, 'sink', parent_replicas = {'src': 1}, run_id = run_id,
                          clock = rig.clock.now if hasattr(rig, 'clock') else None)
    messenger = messenger_for(rig, node, ['src'], runtime = runtime, run_id = run_id)
    return {'node': node, 'external': external, 'runtime': runtime, 'messenger': messenger,
            'markers': store_factory(runtime)}


def _sink_task(rig : Any, env : Dict[str, Any], run_id : str, name : str) -> TaskThread:
    return TaskThread(consumer_task(rig, env['node'], env['messenger'], ['src'], run_id = run_id,
                                    idempotency_store = env['markers']), name = name)


def _replay(rig : Any, run_id : str, index : int) -> None:
    '''What ``videoflow dlq replay`` does: the same envelope bytes under a fresh publication id.'''
    import uuid

    from videoflow.wire.serialization import MSG_TYPE_DATA, encode_envelope
    body = encode_envelope('src', rig.flow_id, run_id, f'src:{index}', index, MSG_TYPE_DATA, None, VALUES[index - 1])
    rig.publish_raw('src', body, headers = {'Nats-Msg-Id': f'replay:{uuid.uuid4().hex}'}, run_id = run_id)


def _input_keys(rig : Any, run_id : str) -> List[str]:
    return [publication_id(rig, 'sink', f'src:{i}', i, run_id) for i in range(1, len(VALUES) + 1)]


def _oracle_run_017(rig : Any, root : Any, store_factory : Callable[[Any], Any], evidence : Dict[str, Any],
                    crash : Callable[[], faults.RaiseError] = crash_at,
                    advance : Optional[Callable[[float], None]] = None) -> List[faults.FaultSchedule]:
    '''
    For each sink — the one keyed on the external system's idempotency primitive
    and the plain one — five schedules over one src → sink run each:

    - ``before-effect``: die before the effect; the replay applies it once.
    - ``after-effect``: die after the effect, before the marker: the keyed sink's \
      replay is a no-op at the external system; the plain sink duplicates.
    - ``race``: an old worker paused inside its effect window while a replacement \
      consumes the redelivery — the seen-then-consume window is not atomic.
    - ``after-marker``: die after the marker, before the ack: the marker skips the replay.
    - ``expired``: two replays after the marker window: the keyed sink is still \
      exactly-once (the key outlives the marker), the plain sink is not; the \
      second replay meets the marker the first one re-wrote.

    The plain sink's duplicates are *expected*: its declaration says at-least-once,
    and the admission half refuses to certify more.
    '''
    schedules : List[faults.FaultSchedule] = []
    expected_effects = {'keyed': {label: len(VALUES) for label in SCHEDULES_017},
                        'plain': {'before-effect': len(VALUES), 'after-effect': len(VALUES) + 1,
                                  'race': len(VALUES) + 1, 'after-marker': len(VALUES), 'expired': len(VALUES) + 1}}
    for kind, sink_cls in (('keyed', KeyedSink), ('plain', PlainSink)):
        for label in SCHEDULES_017:
            schedule, snapshot, keys = _run_017_schedule(rig, root, sink_cls, store_factory, kind, label, evidence,
                                                         crash, advance)
            schedules.append(schedule)
            assert snapshot['effects'] == expected_effects[kind][label], \
                f'{kind}/{label}: {snapshot["effects"]} effects, expected {expected_effects[kind][label]}'
            if kind == 'keyed':
                assert snapshot['count'] == sum(VALUES) and sorted(snapshot['keys']) == sorted(keys)
            else:
                assert snapshot['count'] == sum(VALUES) + (snapshot['effects'] - len(VALUES)) * VALUES[FAULTED_017 - 1]
    return schedules


SCHEDULES_017 = ('before-effect', 'after-effect', 'race', 'after-marker', 'expired')
FAULTED_017 = 3


def _run_017_schedule(rig : Any, root : Any, sink_cls : type, store_factory : Callable[[Any], Any], kind : str,
                      label : str, evidence : Dict[str, Any], crash : Callable[[], faults.RaiseError],
                      advance : Optional[Callable[[float], None]]) -> tuple:
    '''One src → sink run under one schedule; returns the schedule, the external system's final snapshot and the input keys.'''
    faulted = FAULTED_017
    run_id = f'{rig.run_id}-{kind}-{label}'
    provision_run(rig, run_id, batch_specs(processor = 'sink', processor_kind = 'consumer', sink = None))
    src = rig.messenger('src', [], run_id = run_id, replayable = True)
    _publish_records(src, VALUES, eos = False)
    keys = _input_keys(rig, run_id)
    when = {'sink.effect.before': lambda c: c.get('node') == 'sink', 'sink.effect.after': lambda c: c.get('node') == 'sink',
            'settle.before': settling('sink', data_ids(rig, 'src', len(VALUES), run_id))}
    actions : Dict[str, Any] = {
        'before-effect': {'sink.effect.before': faults.Nth(faulted, crash())},
        'after-effect': {'sink.effect.after': faults.Nth(faulted, crash())},
        'race': {'sink.effect.before': faults.Nth(faulted, faults.Pause('old-owner', timeout_seconds = 120))},
        'after-marker': {'settle.before': faults.Nth(faulted, crash())},
        'expired': {},
    }[label]
    schedule = ContextSchedule(actions, when)
    trace : List[Dict[str, Any]] = []
    first = _sink_env(rig, root, sink_cls, store_factory, f'{kind}-{label}', run_id)
    external, markers = first['external'], first['markers']
    task_a = _sink_task(rig, first, run_id, f'sink-{kind}-{label}-a')

    def history_reached(n : int) -> bool:
        return len(external.snapshot()['history']) >= n

    with schedule:
        task_a.start()
        if label == 'race':
            # A holds input 3 inside its effect window; its lease lapses and a
            # replacement B consumes the redelivery; then A resumes and finishes.
            assert wait_until(lambda: schedule.fired().get('sink.effect.before', 0) >= faulted, 30), \
                'the old owner never reached its effect window'
            trace.append({'event': 'A paused inside the effect window', 'external': external.snapshot()})
            second = _sink_env(rig, root, sink_cls, store_factory, f'{kind}-{label}', run_id)
            task_b = _sink_task(rig, second, run_id, f'sink-{kind}-{label}-b')
            task_b.start()
            assert wait_until(lambda: history_reached(faulted), 60), 'the replacement never consumed the redelivered input'
            trace.append({'event': 'B consumed the redelivery', 'external': external.snapshot()})
            src.publish_stop_signal()
            task_b.wait(60)
            assert task_b.error is None, task_b.error
            schedule.release('old-owner')
            task_a.wait(60)
            # A finishes its effect and marker; at its next commit (the
            # completion barrier) its epoch is stale and it is fenced out.
            assert task_a.error is None or isinstance(task_a.error, StaleAuthority), task_a.error
            trace.append({'event': 'A resumed and finished', 'external': external.snapshot(),
                          'old_owner_ended_by': type(task_a.error).__name__ if task_a.error else 'end-of-stream'})
        elif label == 'expired':
            # The run stays open (a replay is addressed to the run that
            # minted the identity); the markers of the five inputs lapse,
            # then two replays of input 3 race in together.
            assert wait_until(lambda: history_reached(len(VALUES)), 60)
            assert wait_until(lambda: all(markers.seen(k) for k in keys), 30), 'markers not written'
            trace.append({'event': 'all inputs consumed and marked', 'external': external.snapshot()})
            if advance is not None:
                advance(RETENTION + 1)
            assert wait_until(lambda: not any(markers.seen(k) for k in keys), 30), 'markers survived their retention'
            _replay(rig, run_id, faulted)
            _replay(rig, run_id, faulted)
            # The first replay reaches the sink past its marker: the external key
            # is what decides. The second finds the marker the first re-wrote.
            assert wait_until(lambda: history_reached(len(VALUES) + 1), 60), 'the replay was not delivered'
            assert wait_until(lambda: markers.seen(keys[faulted - 1]), 30)
            src.publish_stop_signal()
            task_a.wait(60)
            assert task_a.error is None, task_a.error
            trace.append({'event': 'two replays after expiry', 'external': external.snapshot(),
                          'marker_rewritten': markers.seen(keys[faulted - 1])})
            assert history_reached(len(VALUES) + 1) and not history_reached(len(VALUES) + 2), \
                'the second replay reached the external system past a fresh marker'
        else:
            task_a.wait(60)
            assert task_a.crashed, f'{kind}/{label}: the sink did not die at the barrier: {task_a.error!r}'
            trace.append({'event': 'crashed', 'external': external.snapshot(),
                          'marker_seen': markers.seen(keys[faulted - 1])})
            src.publish_stop_signal()
            second = _sink_env(rig, root, sink_cls, store_factory, f'{kind}-{label}', run_id)
            task_b = _sink_task(rig, second, run_id, f'sink-{kind}-{label}-b')
            task_b.start()
            task_b.wait(60)
            assert task_b.error is None, task_b.error
    snapshot = external.snapshot()
    trace.append({'event': 'final', 'external': snapshot})
    evidence[f'{kind}/{label}'] = {
        'faults': {k: repr(v) for k, v in actions.items()}, 'effects': snapshot['effects'],
        'count': snapshot['count'], 'keys': len(snapshot['keys']), 'history': snapshot['history'],
        'markers': {k[:8]: markers.seen(k) for k in keys}, 'trace': trace,
    }
    return schedule, snapshot, keys


def _admission_run_017(evidence : Dict[str, Any]) -> None:
    '''
    What admission says: the plain sink cannot be certified exactly-once by a
    marker (its declaration is at-least-once, and the flow's declared guarantees
    omit it); the keyed sink can — but not with markers kept shorter than the
    replay horizon, and the reviewed 86400 s default is shorter than a week.
    '''
    from videoflow.consumers import CommandlineConsumer
    from videoflow.core import Flow
    from videoflow.producers import IntProducer
    producer = IntProducer(0, 3, name = 'src')
    plain = PlainSink(ledger_path = '/dev/null', name = 'plain')(producer)
    keyed = KeyedSink(ledger_path = '/dev/null', name = 'keyed')(producer)
    declared = sink_guarantees(Flow([plain, keyed, CommandlineConsumer(name = 'cli')(producer)]))
    assert declared == {'keyed': EFFECT_IDEMPOTENT_KEY}, declared
    profiles = (ProfileRequest('src', RELIABLE_WORK),)
    with pytest.raises(IncompatibleProfile) as plain_rejected:
        plan_composition(FlowRequirements(profiles = profiles, exactly_once_effects = ('plain',),
                                          sink_guarantees = declared), _messaging_caps())
    assert 'plain' in str(plain_rejected.value) and 'marker' in str(plain_rejected.value)
    with pytest.raises(IncompatibleProfile) as short_markers:
        plan_composition(FlowRequirements(profiles = profiles, exactly_once_effects = ('keyed',),
                                          sink_guarantees = declared, effect_retention_seconds = EFFECT_RETENTION_SECONDS,
                                          replay_horizon_seconds = HORIZON), _messaging_caps())
    assert '86400' in str(short_markers.value)
    admitted = plan_composition(FlowRequirements(profiles = profiles, exactly_once_effects = ('keyed',),
                                                 sink_guarantees = declared, effect_retention_seconds = HORIZON + 1,
                                                 replay_horizon_seconds = HORIZON), _messaging_caps())
    assert admitted is not None
    evidence['admission'] = {
        'declared_guarantees': declared, 'plain_sink': str(plain_rejected.value),
        'keyed_sink_markers_shorter_than_horizon': str(short_markers.value),
        'reviewed_default_retention_s': EFFECT_RETENTION_SECONDS, 'replay_horizon_s': HORIZON,
        'keyed_sink_admitted_with_retention_s': HORIZON + 1,
    }


@pytest.mark.case('RUN-017')
@pytest.mark.level('process')
def test_run_017_sink_side_effects_and_idempotency_markers_survive_crash(tmp_path, evidence_dir, record_faults,
                                                                         monkeypatch) -> None:
    '''
    RUN-017 (P0, component, process): Sink side effects and idempotency markers survive crash
    and concurrent replay.

    Acceptance: Idempotent mode produces one external effect for all schedules; unsupported
    stronger guarantees are rejected rather than inferred from a local marker.
    '''
    allow_task_threads(monkeypatch)
    evidence : Dict[str, Any] = {}
    rig = memory_rig(batch_specs(processor = 'sink', processor_kind = 'consumer', sink = None))
    try:
        with rig.ticking():
            schedules = _oracle_run_017(rig, tmp_path, lambda runtime: LedgerIdempotencyStore(runtime, RETENTION),
                                        evidence, advance = rig.clock.advance)
        _admission_run_017(evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'effects.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.case('RUN-017')
@pytest.mark.level('process')
@pytest.mark.variant('redis')
def test_run_017_redis_markers_expire_under_the_same_rules(redis_url, tmp_path, evidence_dir, record_faults,
                                                           monkeypatch) -> None:
    '''The legacy marker store: ``RedisIdempotencyStore`` with a real, accelerated TTL on the dev Redis.'''
    import redis  # optional dep (redis extra)
    allow_task_threads(monkeypatch)
    evidence : Dict[str, Any] = {}
    ttl = 2
    rig = memory_rig(batch_specs(processor = 'sink', processor_kind = 'consumer', sink = None))
    client = redis.Redis.from_url(redis_url)
    stores : List[Any] = []

    def redis_markers(runtime : Any) -> Any:
        store = RedisIdempotencyStore(redis_url, ttl_seconds = ttl)
        stores.append(store)
        return store
    try:
        with rig.ticking():
            schedules = _oracle_run_017(rig, tmp_path, redis_markers, evidence,
                                        advance = lambda seconds: time.sleep(ttl + 0.5))
    finally:
        rig.close()
        # Only the keys this test wrote (the sink's input keys), never a pattern sweep.
        for kind in ('keyed', 'plain'):
            for label in SCHEDULES_017:
                keys = _input_keys(rig, f'{rig.run_id}-{kind}-{label}')
                if keys:
                    client.delete(*keys)
        client.close()
        write_evidence(evidence_dir, 'effects.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.negative_control(of = 'RUN-017')
def test_run_017_detects_a_marker_that_certifies_before_the_effect(tmp_path, monkeypatch) -> None:
    allow_task_threads(monkeypatch)
    defects_run2.marker_before_effect(monkeypatch)
    rig = memory_rig(batch_specs(processor = 'sink', processor_kind = 'consumer', sink = None))
    try:
        with rig.ticking():
            assert defects.detects(_oracle_run_017, rig, tmp_path,
                                   lambda runtime: LedgerIdempotencyStore(runtime, RETENTION), {},
                                   advance = rig.clock.advance)
    finally:
        rig.close()


# -- RUN-013 ----------------------------------------------------------------------

def _oracle_run_013(rig : Any, store : Any, evidence : Dict[str, Any], stall : Callable[[], Any],
                    proc_url : Optional[str] = None, crash : Callable[[], faults.RaiseError] = crash_at) -> List[Any]:
    '''
    Worker A takes input 1, commits its output and sends it into a broker that
    accepts nothing (the stall); the synchronous publication times out, the
    input is handed back for retry and A dies. The stall lifts: whatever A had in
    flight lands now or never. Replacement B reconciles A's outbox through the
    same identity before it receives anything, takes the redelivered input
    without recomputing, then meets a lost receipt of its own on input 2.
    '''
    run_id = rig.run_id
    ids = data_ids(rig, 'src', 2, run_id)
    pid1, pid2 = (publication_id(rig, 'proc', f'src:{i}', i, run_id) for i in (1, 2))
    sink = Collector(rig, 'proc', run_id = run_id)
    sink.start()
    src = rig.messenger('src', [], run_id = run_id, replayable = True)
    _publish_records(src, [1], eos = False)
    timeline : List[Dict[str, Any]] = []

    def mark(event : str, **facts : Any) -> None:
        timeline.append(dict(facts, event = event, at = round(time.monotonic(), 3)))

    # A pauses right before committing so the stall is in place for its send only.
    schedule_a = ContextSchedule({'group.commit.before': faults.Nth(1, faults.Pause('before-commit', timeout_seconds = 120)),
                                  'settle.after': faults.Nth(1, crash())},
                                 {'group.commit.before': output_commit, 'settle.after': settling('proc', ids)})
    node_a = StochasticInference(name = 'proc')
    runtime_a = runtime_for(store, rig, 'proc', parent_replicas = {'src': 1}, run_id = run_id)
    messenger_a = messenger_for(rig, node_a, ['src'], runtime = runtime_a, run_id = run_id, nats_url = proc_url)
    task_a = TaskThread(processor_task(rig, node_a, messenger_a, ['src'], run_id = run_id), name = 'proc-A')
    with schedule_a:
        task_a.start()
        assert wait_until(lambda: schedule_a.fired().get('group.commit.before', 0) >= 1, 60), 'A never reached its commit'
        mark('A computed input 1, paused before its commit', nonce = node_a.invocations[0]['nonce'])
        with stall():
            mark('broker acceptance stalled')
            schedule_a.release('before-commit')
            task_a.wait(90)
            assert task_a.crashed, f'A did not die after handing the input back: {task_a.error!r}'
            entry = runtime_a.outbox_entry(pid1)
            assert entry is not None and entry.outcome == OUTCOME_UNKNOWN, entry
            committed_nonce = _committed_nonce(store, rig, run_id, pid1, node = 'proc')
            assert committed_nonce == node_a.invocations[0]['nonce']
            attempts = runtime_a.attempts_for(ids[0])
            assert attempts == 1, f'the input attempt was not recorded before the competing attempt: {attempts}'
            mark('A timed out, handed input 1 back and died', outbox = entry.outcome, attempts = attempts,
                 publication_stats = dict(messenger_a.publication_stats))
        mark('stall lifted: whatever A sent lands now or never')
    # A's process is gone; its connection goes with it.
    import contextlib
    with contextlib.suppress(Exception):
        messenger_a.close()
    # B: the replacement reconciles the outbox at start, before it receives anything.
    node_b = StochasticInference(name = 'proc')
    runtime_b = runtime_for(store, rig, 'proc', parent_replicas = {'src': 1}, run_id = run_id)
    messenger_b = messenger_for(rig, node_b, ['src'], runtime = runtime_b, run_id = run_id, nats_url = proc_url)
    reconciled = runtime_b.outbox_entry(pid1)
    assert reconciled is not None and reconciled.outcome in (OUTCOME_ACCEPTED, OUTCOME_DUPLICATE), reconciled
    orphan_landed = reconciled.outcome == OUTCOME_DUPLICATE
    mark('B reconciled A\'s intent under the same identity', outbox = reconciled.outcome, orphan_landed = orphan_landed)
    _publish_records(src, [2], eos = False)
    schedule_b = ContextSchedule({'publish.receipt.before': faults.Nth(1, faults.DropResponse())})
    task_b = TaskThread(processor_task(rig, node_b, messenger_b, ['src'], run_id = run_id), name = 'proc-B')
    with schedule_b:
        task_b.start()
        assert wait_until(lambda: sink.count() >= 2, 90), f'the sink saw {sink.count()} outputs'
        src.publish_stop_signal()
        task_b.wait(90)
    assert task_b.error is None, task_b.error
    sink.finish(60)
    received = sink.snapshot()
    by_seq : Dict[int, List[str]] = {}
    for r in received:
        by_seq.setdefault(r['seq'], []).append(r['message']['nonce'])
    entry2 = runtime_b.outbox_entry(pid2)
    mark('B finished', publication_stats = dict(messenger_b.publication_stats), outbox_2 = entry2.outcome if entry2 else None)
    evidence.update({
        'timeline': timeline, 'committed_nonce_A': committed_nonce, 'orphan_landed': orphan_landed,
        'sink': [{'trace_id': r['trace_id'], 'seq': r['seq'], 'nonce': r['message']['nonce']} for r in received],
        'B_invocations': [i['input'] for i in node_b.invocations], 'outbox': outbox_snapshot(runtime_b),
    })
    # One reconciled outcome per logical output, and no second result for input 1:
    # the sink saw exactly A's committed nonce, once; B never computed input 1.
    assert sorted(by_seq) == [1, 2], by_seq
    assert by_seq[1] == [committed_nonce], (by_seq[1], committed_nonce)
    assert pid1 not in [i['input'] for i in node_b.invocations], 'B recomputed the committed output'
    # The lost receipt on input 2 was reconciled through the same identity, in-process.
    assert messenger_b.publication_stats.get('unknown', 0) >= 1, messenger_b.publication_stats
    assert entry2 is not None and entry2.outcome in (OUTCOME_ACCEPTED, OUTCOME_DUPLICATE), entry2
    assert len(by_seq[2]) == 1 and by_seq[2] == [node_b.invocations[0]['nonce']]
    return [schedule_a, schedule_b]


@pytest.mark.case('RUN-013')
@pytest.mark.level('broker')
def test_run_013_timed_out_output_publication_has_one_authoritative_owner(nats_url, nats_proxied_url, toxiproxy_url,
                                                                          tmp_path, evidence_dir, record_faults,
                                                                          monkeypatch) -> None:
    '''
    RUN-013 (P0, runtime, broker): Timed-out output publication has one authoritative owner.

    Acceptance: Each logical output has one reconciled final outcome and no unowned task can
    create a conflicting result after timeout.

    The worker's connection goes through toxiproxy; a ``timeout`` toxic holds every
    byte it sends until the toxic is removed, so the publication times out on the
    caller's side while the bytes may still land later — the orphan.
    '''
    import contextlib
    monkeypatch.setattr(nats_messenger, '_PUBLISH_TIMEOUT', 4)
    allow_task_threads(monkeypatch)
    toxiproxy = Toxiproxy(toxiproxy_url)

    @contextlib.contextmanager
    def stalled() -> Any:
        name = toxiproxy.add_toxic('nats', 'timeout', {'timeout': 0}, name = 'vf-run013-stall', stream = 'upstream')
        try:
            yield
        finally:
            toxiproxy.remove_toxic('nats', name)
    evidence : Dict[str, Any] = {}
    rig = JetStreamRig(nats_url, BATCH, batch_specs('proc'), ack_wait = 5)
    try:
        schedules = _oracle_run_013(rig, ledger(tmp_path), evidence, stalled, proc_url = nats_proxied_url)
    finally:
        toxiproxy.reset()
        rig.close()
        write_evidence(evidence_dir, 'publication_timeline.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.case('RUN-013')
@pytest.mark.level('model')
@pytest.mark.variant('memory')
def test_run_013_memory_backends_hold_the_send_until_the_stall_lifts(tmp_path, evidence_dir, record_faults,
                                                                     monkeypatch) -> None:
    monkeypatch.setattr(nats_messenger, '_PUBLISH_TIMEOUT', 1)
    allow_task_threads(monkeypatch)
    evidence : Dict[str, Any] = {}
    rig = memory_rig(batch_specs('proc'))
    try:
        with rig.ticking():
            schedules = _oracle_run_013(rig, ledger(tmp_path), evidence,
                                        lambda: rig.backend_stall(rig.channel('proc')) if hasattr(rig, 'backend_stall')
                                        else _paused_acceptance(rig, rig.channel('proc')))
    finally:
        rig.close()
        write_evidence(evidence_dir, 'publication_timeline.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


def _paused_acceptance(rig : Any, channel : Any) -> Any:
    '''The model's stall: the channel accepts nothing until the block ends, then everything held lands.'''
    import contextlib

    @contextlib.contextmanager
    def paused() -> Any:
        rig.backend.pause_acceptance(channel)
        try:
            yield
        finally:
            rig.backend.resume_acceptance(channel)
    return paused()


@pytest.mark.negative_control(of = 'RUN-013')
def test_run_013_detects_a_retry_that_mints_a_new_identity(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(nats_messenger, '_PUBLISH_TIMEOUT', 1)
    allow_task_threads(monkeypatch)
    defects_run2.fresh_identity_per_attempt(monkeypatch)
    rig = memory_rig(batch_specs('proc'))
    try:
        with rig.ticking():
            assert defects.detects(_oracle_run_013, rig, ledger(tmp_path), {},
                                   lambda: _paused_acceptance(rig, rig.channel('proc')))
    finally:
        rig.close()
