'''
Conformance cases: PAY-015, PAY-016.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions taking the component under
test — the ``dlq replay`` command through a runner, or a rig standing a flow up on
the memory backends or on the compose broker (``_payloads.py``) — so the paired
negative control can run the same oracle against the reviewed defect
(``defects_pay.py``) and prove it fails.
'''
from __future__ import absolute_import, division, print_function

import argparse
import contextlib
import os
import subprocess
import sys
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import defects
import defects_pay
import numpy as np
import pytest
from _payloads import (
    INLINE_THRESHOLD,
    CountingStore,
    DeadLetter,
    JetStreamRig,
    MemoryRig,
    ReadLog,
    Receiver,
    frame_array,
    object_exists,
    obligations_of,
    spec,
    wait_until,
    write_evidence,
)

from videoflow.backends import faults
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.payload import MemoryPayloadStore
from videoflow.backends.messaging import KIND_DATA, ChannelId, Envelope
from videoflow.backends.outcomes import Accepted, PublicationUnknown
from videoflow.backends.payload import PayloadBytes
from videoflow.backends.payload_bridge import PayloadStoreBlobBridge
from videoflow.core import constants
from videoflow.core.constants import BATCH
from videoflow.core.errors import EXIT_ENVIRONMENT, SchemaError, VideoflowError
from videoflow.messaging import topology
from videoflow.wire.serialization import MSG_TYPE_DATA, decode_envelope, encode_envelope, peek_envelope

pytestmark = pytest.mark.timeout(180)

FRAME = frame_array((1000, 1000), seed = 15)


# -- PAY-015 ----------------------------------------------------------------------

def _dead_letter_entries(flow_id : str, run_id : str, store : Any) -> Tuple[List[tuple], str]:
    '''
    Two dead letters of ``child`` under ``parent``: one inline, one whose 1 MB
    payload the store holds under the dead letter's own pin. Returned in the
    ``(subject, headers, body)`` shape ``_dlq_fetch`` hands the replay command.
    '''
    inline = encode_envelope('parent', flow_id, run_id, 't-inline', 1, MSG_TYPE_DATA, None, {'v': 1})
    bridge = PayloadStoreBlobBridge(store, [f'dlq/{flow_id}'], horizon_seconds = 3600, durable_required = True,
                                    content_id = lambda: 'parent:t-blob:2')
    offloaded = encode_envelope('parent', flow_id, run_id, 't-blob', 2, MSG_TYPE_DATA, None, FRAME,
                                blob_store = bridge, inline_threshold = INLINE_THRESHOLD)
    assert bridge.last_ref is not None
    subject = topology.dlq_subject_for(flow_id, run_id, 'child')
    entries = []
    for n, body in enumerate((inline, offloaded), start = 1):
        headers = {'VF-Origin-Node': 'child', 'VF-Run-Id': run_id, 'VF-Code': 'VF_POISON_SCHEMA',
                   'VF-Disposition': 'poison', 'Nats-Msg-Id': f'dlq:{flow_id}:{run_id}:child:{n}'}
        entries.append((subject, headers, body))
    return entries, bridge.last_ref.key


def _unreachable(url : str) -> str:
    '''The same store address on a port nothing listens on: every GET fails, nothing else changes.'''
    parsed = urlparse(url)
    return parsed._replace(netloc = f'{parsed.hostname or "localhost"}:1').geturl()


def _plan_lines(stdout : str) -> List[str]:
    return [line for line in stdout.splitlines() if line.startswith('would replay')]


def _oracle_pay_015_routing(run_replay : Callable[[bool, str], Tuple[int, str, str]], flow_id : str,
                            target_run : str, key : str, store_url : str, evidence : Dict[str, Any]) -> None:
    '''Both entries route to the parent's subject of the target run while the store is unreachable.'''
    rc, out, err = run_replay(True, _unreachable(store_url))
    evidence['dry_run'] = {'rc': rc, 'stdout': out, 'stderr': err}
    assert rc == 0, (rc, out, err)
    lines = _plan_lines(out)
    target = topology.subject_for(flow_id, target_run, 'parent')
    assert len(lines) == 2, f'expected both entries in the plan, got {lines!r} (stderr: {err!r})'
    assert all(f'on {target}' in line for line in lines), lines
    assert sum(f'payload {key} offloaded' in line for line in lines) == 1, lines
    assert '2 message(s) would be replayed' in out and 'could not be decoded' not in err, (out, err)


def _oracle_pay_015_execution(run_replay : Callable[[bool, str], Tuple[int, str, str]], rig : JetStreamRig,
                              source_run : str, target_run : str, key : str, store_url : str,
                              originals : List[bytes], evidence : Dict[str, Any]) -> None:
    '''With the store unreachable the replay refuses, typed, and publishes nothing; restored, both replay.'''
    rc, out, err = run_replay(False, _unreachable(store_url))
    evidence['refused'] = {'rc': rc, 'stdout': out, 'stderr': err}
    assert rc == EXIT_ENVIRONMENT, (rc, out, err)
    assert 'VF_RESOURCE_UNAVAILABLE' in (out + err) and key in (out + err), (out, err)
    assert rig.retained('parent', target_run) == [], 'a replay was published although a payload was unavailable'
    rc, out, err = run_replay(False, store_url)
    evidence['restored'] = {'rc': rc, 'stdout': out, 'stderr': err}
    assert rc == 0 and 'Replayed 2 message(s)' in out, (rc, out, err)
    landed = [entry for _seq, entry in rig.retained_entries('parent', target_run)]
    evidence['landed'] = [dict(e.headers) for e in landed]
    assert sorted(e.body for e in landed) == sorted(originals), 'the replayed bytes are not the dead letters\' bytes'
    for entry in landed:
        assert entry.headers.get('VF-Replay-Target') == 'child', entry.headers
        assert entry.headers.get('VF-Replay') == source_run, entry.headers
        assert entry.headers.get('Nats-Msg-Id', '').startswith('replay:'), entry.headers


def _cli_runner(nats_url : str, flow_id : str, source_run : str, target_run : str) -> Callable[[bool, str], Tuple[int, str, str]]:
    '''``videoflow dlq replay`` in its own process, as an operator runs it.'''
    def run(dry_run : bool, blob_url : str) -> Tuple[int, str, str]:
        command = [sys.executable, '-m', 'videoflow.deploy.cli', 'dlq', 'replay', '--nats', nats_url,
                   '--flow-id', flow_id, '--run-id', source_run, '--to-run', target_run, '--blob-redis-url', blob_url]
        if dry_run:
            command.append('--dry-run')
        proc = subprocess.run(command, capture_output = True, text = True, timeout = 90,
                              env = dict(os.environ, VF_RFC0006 = '1'), cwd = os.getcwd())
        return proc.returncode, proc.stdout, proc.stderr
    return run


def _inprocess_runner(entries : List[tuple], flow_id : str, source_run : str, target_run : str,
                      monkeypatch : pytest.MonkeyPatch, capsys : Any) -> Callable[[bool, str], Tuple[int, str, str]]:
    '''The command's routing in-process over fetched entries (what the negative control drives).'''
    from videoflow.deploy import cli
    monkeypatch.setattr(cli, '_dlq_fetch', lambda *args: list(entries))

    def run(dry_run : bool, blob_url : str) -> Tuple[int, str, str]:
        rc = 0
        try:
            cli._cmd_dlq_replay(argparse.Namespace(nats = 'nats://unused:4222', flow_id = flow_id, run_id = source_run,
                                                   node = None, limit = 10, code = None, to_run = target_run,
                                                   dry_run = dry_run, blob_redis_url = blob_url))
        except VideoflowError as e:
            rc = e.exit_code
            print(f'{e.code}: {e}', file = sys.stderr)
        captured = capsys.readouterr()
        return rc, captured.out, captured.err
    return run


@pytest.mark.case('PAY-015')
@pytest.mark.level('process')
def test_pay_015_large_payload_dlq_routing_inspects_metadata_without(nats_url, redis_url, evidence_dir,
                                                                     monkeypatch) -> None:
    '''
    PAY-015 (P0, integration, process): Large-payload DLQ routing inspects metadata without
    hydrating the image.

    Acceptance: Inline and offloaded valid entries both produce the correct routing plan while
    GET is unavailable; execution succeeds for both after restoration or emits an explicit
    payload error.
    '''
    from _brokers import sweep_refs

    from videoflow.wire.redis_payload_store import RedisPayloadStore
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    specs = [spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False)]
    store = RedisPayloadStore(redis_url)
    rig = JetStreamRig(nats_url, BATCH, specs, store = store)
    target_run = f'{rig.run_id}-fixed'
    key : Optional[str] = None
    try:
        rig.provision_run(target_run)
        entries, key = _dead_letter_entries(rig.flow_id, rig.run_id, store)
        for subject, headers, body in entries:
            rig.publish_raw('child', body, headers = headers, subject = subject)
        assert len(rig.dead_letters('child')) == 2
        run_replay = _cli_runner(nats_url, rig.flow_id, rig.run_id, target_run)
        _oracle_pay_015_routing(run_replay, rig.flow_id, target_run, key, redis_url, evidence)
        _oracle_pay_015_execution(run_replay, rig, rig.run_id, target_run, key, redis_url,
                                  [body for _s, _h, body in entries], evidence)
    finally:
        rig.close()
        if key is not None:
            sweep_refs(store.client, [key])
        write_evidence(evidence_dir, 'routing_plan.json', evidence)


@pytest.mark.negative_control(of = 'PAY-015')
def test_pay_015_detects_routing_that_hydrates_to_find_the_target(monkeypatch, capsys) -> None:
    entries, key = _dead_letter_entries('f', 'r', MemoryPayloadStore(FakeClock()))
    defects_pay.hydrating_router(monkeypatch)
    run_replay = _inprocess_runner(entries, 'f', 'r', 'r2', monkeypatch, capsys)
    assert defects.detects(_oracle_pay_015_routing, run_replay, 'f', 'r2', key, 'redis://localhost:6379/0', {})


# -- PAY-016 ----------------------------------------------------------------------

def _specs_pay_016() -> list:
    return [spec('parent', [], 'producer', True), spec('X', ['parent'], 'consumer', False),
            spec('Y', ['parent'], 'consumer', False)]


def _replay_via_backend(backend : Any, flow_id : str, target_run : str, letter : DeadLetter, source_run : str,
                        scope : Optional[str], drop_receipt : bool) -> Dict[str, Any]:
    '''
    The replay publication exactly as ``dlq replay`` shapes it — a fresh id, the
    source run, the target node — through a messaging adapter, so a lost
    acknowledgment can be injected and resolved by the idempotent re-publish.
    '''
    decoded = decode_envelope(letter.body, resolve_blobs = False)
    pid = f'replay:{uuid.uuid4().hex}'
    headers = {'Nats-Msg-Id': pid, 'VF-Replay': source_run}
    if scope != defects_pay.UNSCOPED_REPLAY:
        headers['VF-Replay-Target'] = scope or letter.headers['VF-Origin-Node']
    envelope = Envelope(channel = ChannelId(flow_id, target_run, decoded['producer_name']), publication_id = pid,
                        headers = headers, body = letter.body, size = len(letter.body), event_id = pid,
                        partition_key = None, event_ts = None, source_epoch = None, source_offset = None,
                        schema_version = 4, kind = KIND_DATA)
    schedule = faults.FaultSchedule({'publish.receipt.before': faults.Nth(1, faults.DropResponse())}) if drop_receipt else None
    outcomes : List[str] = []
    with (schedule if schedule is not None else contextlib.nullcontext()):
        outcome = backend.publish(envelope, time.monotonic() + 10.0)
        outcomes.append(type(outcome).__name__)
        if isinstance(outcome, PublicationUnknown):
            outcome = backend.observe_publication(envelope)      # the retried acknowledgment
            outcomes.append(type(outcome).__name__)
    assert isinstance(outcome, Accepted), outcome
    return {'pid': pid, 'headers': headers, 'outcomes': outcomes, 'duplicate': outcome.duplicate,
            'faults': schedule.fired() if schedule is not None else {}, 'lineage': {
                'flow_id': decoded['flow_id'], 'run_id': decoded['run_id'], 'producer_name': decoded['producer_name'],
                'trace_id': decoded['trace_id'], 'seq': decoded['seq']}}


def _oracle_pay_016(rig : Any, replay : Callable[[str, DeadLetter, str, bool], Dict[str, Any]],
                    other_run : Callable[[str], Any], evidence : Dict[str, Any]) -> None:
    '''
    ``parent`` fed X and Y; Y finished, X dead-lettered the frame. The dead letter
    is replayed into the same run and into another, each time while Y is busy
    with a normal input; only X may see it, the bytes stay readable, and nothing
    about the original readers' ownership moves.
    '''
    log = ReadLog()
    parent = rig.messenger('parent', [], blob_reader_ids = ['X', 'Y'])
    x = rig.messenger('X', ['parent'], store = CountingStore(rig.store, 'X', log))
    y = rig.messenger('Y', ['parent'], store = CountingStore(rig.store, 'Y', log))
    parent.publish_message(FRAME)
    original = rig.retained_entries('parent')[-1][1]
    key = peek_envelope(original.body)['blob_ref']
    assert np.array_equal(y.receive_message()['parent']['message'], FRAME)
    y.ack_inputs()
    assert np.array_equal(x.receive_message()['parent']['message'], FRAME)
    x.fail_inputs(SchemaError('this frame will never parse', remedy = 'fix the producer'))
    assert wait_until(lambda: len(rig.dead_letters('X')) == 1, 20)
    letter = rig.dead_letters('X')[0]
    pin = f'dlq/{rig.flow_id}'
    assert obligations_of(rig.store, key) == {pin}, obligations_of(rig.store, key)
    evidence['original'] = {'pid': original.headers.get('Nats-Msg-Id'), 'key': key,
                            'obligations_after_dead_letter': sorted(obligations_of(rig.store, key))}
    fixtures : List[Dict[str, Any]] = []
    for target_run, label in ((rig.run_id, 'same_run'), (f'{rig.run_id}-b', 'other_run')):
        if target_run == rig.run_id:
            xr, yr, pr = x, y, parent
        else:
            other_run(target_run)
            xr = rig.messenger('X', ['parent'], run_id = target_run, store = CountingStore(rig.store, 'X', log))
            yr = rig.messenger('Y', ['parent'], run_id = target_run, store = CountingStore(rig.store, 'Y', log))
            pr = rig.messenger('parent', [], run_id = target_run, blob_reader_ids = ['X', 'Y'])
        reads_before = log.count('Y', 'bytes'), log.count('X', 'bytes')
        busy = Receiver(yr, expected = 1, name = f'pay-016-Y-{label}')
        busy.start()
        record = replay(target_run, letter, rig.run_id, label == 'other_run')
        pr.publish_message({'normal': label})                    # Y's concurrent, unrelated input
        busy.join(30)
        assert busy.error is None and len(busy.inputs) == 1, (busy.error, len(busy.inputs))
        seen_by_y = busy.inputs[0]['parent']['message']
        assert isinstance(seen_by_y, dict) and seen_by_y == {'normal': label}, \
            f'Y processed the replay addressed to X: {type(seen_by_y).__name__} {getattr(seen_by_y, "shape", seen_by_y)}'
        inputs = xr.receive_message()
        attempt = xr._inflight_handles[0].num_delivered
        assert np.array_equal(inputs['parent']['message'], FRAME), 'X did not receive the replayed frame'
        xr.ack_inputs()
        assert wait_until(lambda run = target_run: rig.consumer_state('Y', 'parent', run_id = run).unacked == 0, 20)
        state_y = rig.consumer_state('Y', 'parent', run_id = target_run)
        held = obligations_of(rig.store, key)
        fixtures.append({'target_run': target_run, 'replay': record, 'x_attempt': attempt,
                         'y_reads': log.count('Y', 'bytes') - reads_before[0], 'x_reads': log.count('X', 'bytes') - reads_before[1],
                         'y_consumer': state_y.__dict__, 'obligations_after': sorted(held)})
        assert log.count('Y', 'bytes') - reads_before[0] == 0, 'Y fetched the replayed payload'
        assert log.count('X', 'bytes') - reads_before[1] == 1
        assert state_y.pending == 0 and state_y.unacked == 0, state_y
        assert attempt == 1, f'the replay is not a fresh delivery to X (attempt {attempt})'
        assert held == {pin} and object_exists(rig.store, key), \
            f'original ownership moved or the bytes were reclaimed: {sorted(held)}'
        assert isinstance(rig.store.read(rig.store.ref_for_key(key)), PayloadBytes)
        assert record['pid'] != original.headers.get('Nats-Msg-Id')
        assert record['headers']['VF-Replay'] == rig.run_id
        assert record['lineage']['run_id'] == rig.run_id and record['lineage']['producer_name'] == 'parent'
        if label == 'other_run':
            assert record['duplicate'] and 'PublicationUnknown' in record['outcomes'], record
    evidence['fixtures'] = fixtures
    receipt = rig.store.release_obligation(rig.store.ref_for_key(key), pin, 'dlq entry aged out')
    assert receipt.reclaimed and not object_exists(rig.store, key)


@pytest.mark.case('PAY-016')
@pytest.mark.level('broker')
def test_pay_016_replay_has_fresh_ownership_and_intentional_recipient_scope(nats_url, redis_url, evidence_dir,
                                                                            record_faults, monkeypatch) -> None:
    '''
    PAY-016 (P0, integration, broker): Replay has fresh ownership and intentional recipient
    scope.

    Acceptance: Observed recipient set equals the requested replay scope; replayed bytes remain
    readable until all replay owners finish, and original ownership cannot be decremented again.

    The same-run replay goes through ``videoflow dlq replay`` itself; the other-run
    replay goes through the JetStream adapter so its acknowledgment can be lost
    and retried.
    '''
    from videoflow.deploy import cli
    from videoflow.messaging.jetstream_backend import JetStreamMessagingBackend
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    rig = JetStreamRig(nats_url, BATCH, _specs_pay_016(), redis_url = redis_url, max_retries = 0)
    schedules : List[Dict[str, int]] = []

    def replay(target_run : str, letter : DeadLetter, source_run : str, drop_receipt : bool) -> Dict[str, Any]:
        if target_run == source_run:
            cli._cmd_dlq_replay(argparse.Namespace(nats = nats_url, flow_id = rig.flow_id, run_id = source_run,
                                                   node = 'X', limit = 10, code = None, to_run = target_run,
                                                   dry_run = False, blob_redis_url = None))
            landed = [e for _seq, e in rig.retained_entries('parent', target_run)
                      if e.headers.get('Nats-Msg-Id', '').startswith('replay:')]
            assert len(landed) == 1, [e.headers for _s, e in rig.retained_entries('parent', target_run)]
            decoded = decode_envelope(landed[0].body, resolve_blobs = False)
            return {'pid': landed[0].headers['Nats-Msg-Id'], 'headers': dict(landed[0].headers), 'outcomes': ['cli'],
                    'duplicate': False, 'faults': {}, 'lineage': {k: decoded[k] for k in ('flow_id', 'run_id', 'producer_name', 'trace_id', 'seq')}}
        backend = JetStreamMessagingBackend(nats_url, rig.flow_id, target_run, BATCH)
        backend.start()
        try:
            record = _replay_via_backend(backend, rig.flow_id, target_run, letter, source_run, None, drop_receipt)
        finally:
            backend.shutdown()
        schedules.append(record['faults'])
        return record

    try:
        _oracle_pay_016(rig, replay, rig.provision_run, evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'replay_scope.json', evidence)
    assert schedules and schedules[0].get('publish.receipt.before', 0) >= 1, schedules
    if evidence.get('fixtures'):
        record_faults(_Fired(schedules[0]))


class _Fired:
    '''A fired-count carrier for ``record_faults`` when the schedule object lived inside a helper.'''
    def __init__(self, fired : Dict[str, int]) -> None:
        self._fired = dict(fired)

    def fired(self) -> Dict[str, int]:
        return dict(self._fired)

    def unfired(self) -> List[str]:
        return [name for name, count in self._fired.items() if count == 0]


@pytest.mark.case('PAY-016')
@pytest.mark.level('broker')
@pytest.mark.variant('memory')
def test_pay_016_memory_backends_scope_the_replay_to_its_target(evidence_dir, record_faults, monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    rig = MemoryRig(BATCH, specs = _specs_pay_016(), max_retries = 0)
    fired : List[Dict[str, int]] = []

    def replay(target_run : str, letter : DeadLetter, source_run : str, drop_receipt : bool) -> Dict[str, Any]:
        record = _replay_via_backend(rig.backend, rig.flow_id, target_run, letter, source_run, None, drop_receipt)
        if drop_receipt:
            fired.append(record['faults'])
        return record

    try:
        _oracle_pay_016(rig, replay, lambda run: rig.provision(_specs_pay_016(), run_id = run, max_retries = 0), evidence)
    finally:
        rig.close()
    write_evidence(evidence_dir, 'replay_scope.json', evidence)
    record_faults(_Fired(fired[0]))


@pytest.mark.negative_control(of = 'PAY-016')
def test_pay_016_detects_a_replay_that_fans_out_to_every_child(monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    rig = MemoryRig(BATCH, specs = _specs_pay_016(), max_retries = 0)

    def unscoped(target_run : str, letter : DeadLetter, source_run : str, drop_receipt : bool) -> Dict[str, Any]:
        return _replay_via_backend(rig.backend, rig.flow_id, target_run, letter, source_run,
                                   defects_pay.UNSCOPED_REPLAY, drop_receipt)

    try:
        assert defects.detects(_oracle_pay_016, rig, unscoped,
                               lambda run: rig.provision(_specs_pay_016(), run_id = run, max_retries = 0), {})
    finally:
        rig.close()
