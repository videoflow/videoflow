'''
Conformance cases: RUN-014, RUN-015.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Both cases are about where a source's identities come
from: a live source mints a capture epoch per process (``MSGID-5``), so a
replacement's frames never wear a dead process's ids; a replayable source mints
its offsets (``MSGID-6``), so a replayed offset *is* its original identity, the
checkpoint follows acceptance and never a send, and a new analysis of the same
media declares its own namespace. The oracles run the real ``ProducerTask`` over a
rig (``_runs2``) — the memory backends with a ``file://`` ledger for the primaries,
the compose broker with real worker processes for the subprocess variant — so the
paired negative controls (``defects_run2.py``) run the same assertions against the
reviewed defects.
'''
from __future__ import absolute_import, division, print_function

from typing import Any, Callable, Dict, List, Optional

import defects
import defects_run2
import pytest
from _runnodes import JsonlLog, LiveSource, ReplayableSource, ReplayableSourceV2
from _runs2 import (
    Collector,
    ContextSchedule,
    JetStreamRig,
    TaskThread,
    WorkerProcess,
    allow_task_threads,
    crash_at,
    ledger,
    memory_rig,
    messenger_for,
    outbox_snapshot,
    producer_task,
    provision_run,
    runtime_for,
    spec,
    worker_env,
    write_evidence,
)

from videoflow.backends import faults
from videoflow.backends.runtime import FlowRuntime
from videoflow.core import constants
from videoflow.core.constants import BATCH

pytestmark = pytest.mark.timeout(180)

FRAMES = 5


def _specs() -> List[Any]:
    return [spec('cam', [], 'producer', True), spec('sink', ['cam'], 'consumer', False)]


# -- RUN-014 ----------------------------------------------------------------------

def _run_live_source(rig : Any, store : Any, node : LiveSource, schedule : Any, run_id : str,
                     evidence : Dict[str, Any], label : str) -> Dict[str, Any]:
    '''One process of the live source: a fresh ledger (a fresh capture epoch) and messenger, run until it ends.'''
    runtime = runtime_for(store, rig, 'cam', run_id = run_id)
    messenger = messenger_for(rig, node, [], runtime = runtime, run_id = run_id)
    task = TaskThread(producer_task(rig, node, messenger, run_id = run_id), name = f'cam-{label}')
    if schedule is not None:
        with schedule:
            task.start()
            task.wait(60)
    else:
        task.start()
        task.wait(60)
    record = {'label': label, 'epoch': runtime.source_epoch(), 'ended_by': type(task.error).__name__ if task.error
              else 'end-of-stream', 'publication_stats': dict(messenger.publication_stats),
              'outbox': outbox_snapshot(runtime), 'crashed': task.crashed, 'error': repr(task.error)}
    evidence.setdefault('processes', []).append(record)
    return record


def _oracle_run_014(rig : Any, store : Any, evidence : Dict[str, Any], crash : Callable[[], faults.RaiseError] = crash_at,
                    run_process : Optional[Callable[..., Dict[str, Any]]] = None) -> List[Any]:
    '''
    A live camera captures five frames; the process dies as its fifth capture is
    on the wire (``source.publish.after``), within the dedup window; a
    replacement captures five genuinely new frames from local sequence 0. On the
    way, one send of the first process loses its receipt and is retried under
    its own identity. Ten distinct captures must reach the sink as ten distinct
    identities, in two epochs.
    '''
    run_id = rig.run_id
    sink = Collector(rig, 'cam', run_id = run_id)
    sink.start()
    schedule = ContextSchedule({'source.publish.after': faults.Nth(FRAMES, crash()),
                                'publish.receipt.before': faults.Nth(2, faults.DropResponse())})
    runner = run_process or _run_live_source
    first = runner(rig, store, LiveSource(frames = FRAMES, capture_seed = 'first', name = 'cam'), schedule, run_id,
                   evidence, 'first')
    assert first['crashed'], f'the source did not die at the barrier: {first["error"]}'
    assert first['publication_stats'].get('unknown') == 1 and first['publication_stats'].get('duplicate') == 1, \
        first['publication_stats']
    second = runner(rig, store, LiveSource(frames = FRAMES, capture_seed = 'second', name = 'cam'), None, run_id,
                    evidence, 'replacement')
    assert not second['crashed'] and second['ended_by'] == 'end-of-stream', second
    sink.finish(60)
    received = sink.snapshot()
    lineage = [{'trace_id': r['trace_id'], 'seq': r['seq'], 'capture': r['message']['capture'],
                'local_seq': r['message']['local_seq']} for r in received]
    evidence['lineage'] = lineage
    epochs = {first['epoch'], second['epoch']}
    evidence['epochs'] = sorted(epochs)
    assert len(epochs) == 2, epochs
    # Every capture reached the sink under an identity of its own: ten frames, ten
    # trace ids, two epochs, five local sequence numbers each.
    assert sorted(r['capture'] for r in lineage) == sorted([f'first:{i}' for i in range(FRAMES)] + [f'second:{i}' for i in range(FRAMES)]), lineage
    assert len({r['trace_id'] for r in lineage}) == 2 * FRAMES, lineage
    for r in lineage:
        node, epoch, n = r['trace_id'].split(':')
        assert node == 'cam' and epoch in epochs and int(n) == r['seq'] == r['local_seq'] + 1, r
        assert epoch == (first['epoch'] if r['capture'].startswith('first') else second['epoch']), r
    # The retried send kept its identity: one copy of every first-epoch frame, none minted twice.
    first_ids = [e['publication_id'] for e in first['outbox'] if e['kind'] == 'data']
    assert len(first_ids) == FRAMES and len(set(first_ids)) == FRAMES, first['outbox']
    return [schedule]


@pytest.mark.case('RUN-014')
@pytest.mark.level('process')
def test_run_014_live_source_restarts_create_new_epochs_without_colliding(tmp_path, evidence_dir, record_faults,
                                                                          monkeypatch) -> None:
    '''
    RUN-014 (P0, runtime, process): Live-source restarts create new epochs without colliding
    with earlier frames.

    Acceptance: No genuinely new post-restart frame is suppressed because its local counter
    matches a pre-restart frame.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    allow_task_threads(monkeypatch)
    evidence : Dict[str, Any] = {}
    rig = memory_rig(_specs())
    try:
        with rig.ticking():
            schedules = _oracle_run_014(rig, ledger(tmp_path), evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'source_epochs.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


def _run_live_source_worker(rig : Any, root : Any, label : str, node : LiveSource, schedule : Any,
                            evidence : Dict[str, Any]) -> Dict[str, Any]:
    '''
    One real worker process hosting the live source on the compose broker. The
    process mints its epoch itself; the sink's lineage is where it is read back.
    Here the outbox proves five distinct publications per process.
    '''
    env = worker_env(rig, node, 'producer', [], True, f'file://{root / label}', schedule = schedule)
    worker = WorkerProcess(env, name = f'cam-{label}')
    worker.run(90)
    runtime = runtime_for(ledger(root, label), rig, 'cam')
    log = JsonlLog(node.get_params()['log_path'])
    record = {'label': label, 'exit_code': worker.exit_code, 'crashed': worker.exit_code == 137,
              'ended_by': 'crash' if worker.exit_code == 137 else ('end-of-stream' if worker.exit_code == 0 else 'error'),
              'error': worker.stderr[-800:], 'captures': [r for r in log.records() if r.get('op') == 'capture'],
              'outbox': outbox_snapshot(runtime)}
    evidence.setdefault('processes', []).append(record)
    return record


@pytest.mark.case('RUN-014')
@pytest.mark.level('process')
@pytest.mark.variant('subprocess')
def test_run_014_a_replacement_worker_process_mints_its_own_epoch(nats_url, tmp_path, evidence_dir, record_faults,
                                                                  monkeypatch) -> None:
    '''Real worker processes on the compose broker: the second process is a genuinely new epoch.'''
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    rig = JetStreamRig(nats_url, BATCH, _specs(), ack_wait = 5)
    schedule = faults.FaultSchedule({'source.publish.after': faults.Nth(FRAMES, faults.Crash(137)),
                                     'publish.receipt.before': faults.Nth(2, faults.DropResponse())},
                                    marker_dir = str(tmp_path / 'markers'))
    try:
        sink = Collector(rig, 'cam')
        sink.start()
        log_path = str(tmp_path / 'captures.jsonl')
        first = _run_live_source_worker(rig, tmp_path, 'first', LiveSource(frames = FRAMES, capture_seed = 'first',
                                                                           log_path = log_path, name = 'cam'),
                                        schedule, evidence)
        assert first['crashed'], first
        assert len(first['outbox']) == FRAMES and len(first['captures']) == FRAMES, first
        second = _run_live_source_worker(rig, tmp_path, 'replacement', LiveSource(frames = FRAMES, capture_seed = 'second',
                                                                                  log_path = log_path, name = 'cam'),
                                         None, evidence)
        assert second['ended_by'] == 'end-of-stream', second
        sink.finish(60)
        lineage = [{'trace_id': r['trace_id'], 'seq': r['seq'], 'capture': r['message']['capture']} for r in sink.snapshot()]
        evidence['lineage'] = lineage
        assert sorted(r['capture'] for r in lineage) == sorted([f'first:{i}' for i in range(FRAMES)] + [f'second:{i}' for i in range(FRAMES)]), lineage
        epochs = {r['trace_id'].split(':')[1] for r in lineage}
        assert len(epochs) == 2 and len({r['trace_id'] for r in lineage}) == 2 * FRAMES, lineage
        assert {r['trace_id'].split(':')[1] for r in lineage if r['capture'].startswith('first')} != \
            {r['trace_id'].split(':')[1] for r in lineage if r['capture'].startswith('second')}
        evidence['epochs'] = sorted(epochs)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'source_epochs.json', evidence)
    record_faults(schedule)


@pytest.mark.negative_control(of = 'RUN-014')
def test_run_014_detects_a_source_that_counts_without_an_epoch(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    allow_task_threads(monkeypatch)
    defects_run2.epochless_source(monkeypatch)
    rig = memory_rig(_specs())
    try:
        with rig.ticking():
            assert defects.detects(_oracle_run_014, rig, ledger(tmp_path), {})
    finally:
        rig.close()


# -- RUN-015 ----------------------------------------------------------------------

MEDIA_FRAMES = 6


class _CheckpointHistory(FlowRuntime):
    '''A ``FlowRuntime`` that records every checkpoint write — the source checkpoint history.'''
    def __init__(self, *args : Any, history : List[Dict[str, Any]], **kwargs : Any) -> None:
        super().__init__(*args, **kwargs)
        self._history = history

    def checkpoint(self, state : bytes, replay_position : Any) -> str:
        version = super().checkpoint(state, replay_position)
        self._history.append({'position': dict(replay_position), 'version': version})
        return version


def _run_replayable(rig : Any, store : Any, node : ReplayableSource, schedule : Any, run_id : str,
                    history : List[Dict[str, Any]], evidence : Dict[str, Any], label : str) -> Dict[str, Any]:
    runtime = _CheckpointHistory(store, rig.flow_id, run_id, 'cam', history = history)
    messenger = messenger_for(rig, node, [], runtime = runtime, run_id = run_id)
    resume = messenger.resume_offset()
    task = TaskThread(producer_task(rig, node, messenger, run_id = run_id, resume_offset = resume), name = f'cam-{label}')
    if schedule is not None:
        with schedule:
            task.start()
            task.wait(60)
    else:
        task.start()
        task.wait(60)
    record = {'label': label, 'resume_offset': resume, 'seeks': list(node.seeks), 'crashed': task.crashed,
              'ended_by': type(task.error).__name__ if task.error else 'end-of-stream', 'error': repr(task.error),
              'publication_stats': dict(messenger.publication_stats), 'outbox': outbox_snapshot(runtime),
              'checkpoint': runtime.restore_checkpoint()[1]}
    evidence.setdefault('processes', []).append(record)
    return record


def _oracle_run_015(rig : Any, store_for : Callable[[str], Any], evidence : Dict[str, Any],
                    crash : Callable[[], faults.RaiseError] = crash_at) -> List[Any]:
    '''
    A six-frame clip. The source dies after offset 3 was accepted and before its
    checkpoint (``checkpoint.write.before``); offset 2's receipt is lost on the
    way. The replacement resumes from the last *accepted* offset — 2 — and
    re-reads 3, whose identity is the same string, so the broker collapses it.
    Then a new analysis version of the same clip, in a run of its own, mints ids
    in its declared namespace and collides with nothing.
    '''
    run_id = rig.run_id
    sink = Collector(rig, 'cam', run_id = run_id)
    sink.start()
    history : List[Dict[str, Any]] = []
    store = store_for('recovery')
    schedule = ContextSchedule({'checkpoint.write.before': faults.Nth(3, crash()),
                                'publish.receipt.before': faults.Nth(2, faults.DropResponse())})
    first = _run_replayable(rig, store, ReplayableSource(frames = MEDIA_FRAMES, name = 'cam'), schedule, run_id, history,
                            evidence, 'first')
    assert first['crashed'], f'the source did not die at the barrier: {first["error"]}'
    # Offset 2's send was ambiguous and reconciled through the same identity: the
    # checkpoint advanced to 2 only on that acceptance, and stopped short of the
    # unconfirmed 3.
    assert first['publication_stats'].get('unknown') == 1 and first['publication_stats'].get('duplicate') == 1, \
        first['publication_stats']
    assert [h['position']['offset'] for h in history] == [1, 2], history
    assert first['checkpoint'].get('offset') == 2, first['checkpoint']
    second = _run_replayable(rig, store, ReplayableSource(frames = MEDIA_FRAMES, name = 'cam'), None, run_id, history,
                             evidence, 'replacement')
    assert second['ended_by'] == 'end-of-stream', second
    assert second['resume_offset'] == 2 and second['seeks'] == [2], second
    assert [h['position']['offset'] for h in history] == [1, 2, 3, 4, 5, 6], history
    # Offset 3 was re-read and re-minted as the same identity: a duplicate acceptance, one copy.
    assert second['publication_stats'].get('duplicate', 0) >= 1, second['publication_stats']
    sink.finish(60)
    recovered = [(r['trace_id'], r['seq'], r['message']['payload']) for r in sink.snapshot()]
    evidence['recovery'] = {'lineage': recovered, 'checkpoint_history': history}
    assert sorted(recovered) == [(f'cam:{i}', i, f'clip#{i}') for i in range(1, MEDIA_FRAMES + 1)], recovered
    # A new analysis of the same media: its own namespace, its own run, no collision.
    run_v2 = f'{run_id}-v2'
    provision_run(rig, run_v2, _specs())
    sink_v2 = Collector(rig, 'cam', run_id = run_v2)
    sink_v2.start()
    history_v2 : List[Dict[str, Any]] = []
    version = _run_replayable(rig, store_for('v2'), ReplayableSourceV2(frames = MEDIA_FRAMES, name = 'cam'), None, run_v2,
                              history_v2, evidence, 'analysis-v2')
    assert version['ended_by'] == 'end-of-stream', version
    sink_v2.finish(60)
    analysed = [(r['trace_id'], r['seq'], r['message']['payload']) for r in sink_v2.snapshot()]
    evidence['analysis_v2'] = {'lineage': analysed, 'checkpoint_history': history_v2}
    assert sorted(analysed) == [(f'cam:v2:{i}', i, f'clip#{i}') for i in range(1, MEDIA_FRAMES + 1)], analysed
    assert not ({t for t, _s, _p in analysed} & {t for t, _s, _p in recovered})
    return [schedule]


@pytest.mark.case('RUN-015')
@pytest.mark.level('process')
def test_run_015_replayable_sources_preserve_stable_offset_identities(tmp_path, evidence_dir, record_faults,
                                                                      monkeypatch) -> None:
    '''
    RUN-015 (P1, runtime, process): Replayable sources preserve stable offset identities across
    attempts.

    Acceptance: Recovered offsets map one-to-one to original logical identities; new-analysis
    identities follow the declared namespace and no offset is silently lost.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    allow_task_threads(monkeypatch)
    evidence : Dict[str, Any] = {}
    rig = memory_rig(_specs())
    try:
        with rig.ticking():
            schedules = _oracle_run_015(rig, lambda label: ledger(tmp_path, label), evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'offset_identities.json', evidence)
    for schedule in schedules:
        record_faults(schedule)


@pytest.mark.negative_control(of = 'RUN-015')
def test_run_015_detects_a_checkpoint_that_advances_on_the_send(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    allow_task_threads(monkeypatch)
    defects_run2.checkpoint_on_send(monkeypatch)
    rig = memory_rig(_specs())
    try:
        with rig.ticking():
            assert defects.detects(_oracle_run_015, rig, lambda label: ledger(tmp_path, label), {})
    finally:
        rig.close()


@pytest.mark.negative_control(of = 'RUN-015')
def test_run_015_detects_a_new_analysis_without_a_namespace(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    allow_task_threads(monkeypatch)
    defects_run2.versionless_analysis(monkeypatch)
    rig = memory_rig(_specs())
    try:
        with rig.ticking():
            assert defects.detects(_oracle_run_015, rig, lambda label: ledger(tmp_path, label), {})
    finally:
        rig.close()
