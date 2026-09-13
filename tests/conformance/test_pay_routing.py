'''
Conformance cases: PAY-018.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. The oracle is a plain function taking a rig standing the
partitioned flow up on the memory backends or on the compose broker (``_payloads.py``),
so the paired negative control can run it against the reviewed defect
(``defects_pay.py``) and prove it fails.
'''
from __future__ import absolute_import, division, print_function

import collections
import hashlib
import threading
import time
from typing import Any, Dict, Optional

import defects
import defects_pay
import pytest
from _payloads import (
    CountingStore,
    JetStreamRig,
    KeyRecordingStore,
    MemoryRig,
    ReadLog,
    Receiver,
    frame_array,
    object_exists,
    spec,
    wait_until,
    write_evidence,
)

from videoflow.backends import faults
from videoflow.backends.payload import ImmutablePayloadRef
from videoflow.core import constants
from videoflow.core.constants import BATCH
from videoflow.messaging import nats_messenger
from videoflow.wire.serialization import serialized_payload_size

pytestmark = pytest.mark.timeout(180)

REPLICAS = 8
FRAMES = 100
SHAPE = (256, 256)
BLOCKED_REPLICA = 3


def _owner(trace : str) -> int:
    '''The partition owner of a trace id — the messenger's own arithmetic, computed independently.'''
    return int(hashlib.sha256(trace.encode('utf-8')).hexdigest()[:8], 16) % REPLICAS


def _specs() -> list:
    return [spec('parent', [], 'producer', True),
            spec('child', ['parent'], 'consumer', False, nb_tasks = REPLICAS, partition_by = 'trace_id')]


def _oracle_pay_018(rig : Any, evidence : Dict[str, Any], frames : int = FRAMES) -> faults.FaultSchedule:
    log = ReadLog()
    owner_of_key : Dict[str, int] = {}

    def refuse_non_owned(ref : ImmutablePayloadRef) -> Optional[BaseException]:
        owner = owner_of_key.get(ref.key)
        if owner is not None and owner != BLOCKED_REPLICA:
            return ConnectionError(f'GET blocked for replica {BLOCKED_REPLICA}, which does not own {ref.key}')
        return None

    children = []
    for r in range(REPLICAS):
        store = CountingStore(rig.store, f'child/p{r}', log, blocked = refuse_non_owned if r == BLOCKED_REPLICA else None)
        children.append(rig.messenger('child', ['parent'], store = store, replica_id = r, nb_tasks = REPLICAS,
                                      partition_by = 'trace_id'))
    publisher_store = KeyRecordingStore(rig.store, 'parent', log)
    parent = rig.messenger('parent', [], store = publisher_store, blob_reader_ids = [f'child/p{r}' for r in range(REPLICAS)])
    expected = collections.Counter(_owner(f'parent:{i}') for i in range(1, frames + 1))
    assert len(expected) == REPLICAS and expected[BLOCKED_REPLICA] > 0
    receivers = [Receiver(children[r], expected = expected[r], name = f'child/p{r}') for r in range(REPLICAS)]
    # The second hit of the read barrier is the store's, on an owner's first fetch: one transient retry.
    schedule = faults.FaultSchedule({'payload.read.before': faults.Nth(2, faults.RaiseError(
        lambda: ConnectionError('injected transient GET')))})
    started = time.monotonic()
    with schedule:
        for receiver in receivers:
            receiver.start()
        for i in range(1, frames + 1):
            parent.publish_message(frame_array(SHAPE, seed = i))
            owner_of_key[publisher_store.keys[-1]] = _owner(f'parent:{i}')
        for receiver in receivers:
            receiver.join(timeout = 120)
    took = time.monotonic() - started
    for receiver in receivers:
        assert receiver.error is None, f'{receiver.name}: {receiver.error!r}'
        assert not receiver.is_alive(), f'{receiver.name} did not finish: {len(receiver.inputs)} of {receiver.expected}'
    # End of stream: every replica drains what it does not own and reports the parent finished.
    parent.publish_stop_signal()
    drains = [threading.Thread(target = child.receive_message, name = f'drain-p{r}', daemon = True)
              for r, child in enumerate(children)]
    for drain in drains:
        drain.start()
    for drain in drains:
        drain.join(timeout = 60)
    assert not any(d.is_alive() for d in drains), 'a replica did not reach end of stream'
    # Every replica settled every frame — as owner or by ack-and-skip — so every share is released.
    assert wait_until(lambda: not any(object_exists(rig.store, k) for k in publisher_store.keys), 30), \
        f'{sum(object_exists(rig.store, k) for k in publisher_store.keys)} of {frames} objects still owned after ' \
        'every replica settled: a share was never released'
    per_replica = {r: len(receivers[r].inputs) for r in range(REPLICAS)}
    assert per_replica == dict(expected), (per_replica, dict(expected))
    reads = [r for r in log.records() if r.reader != 'parent']
    owner_reads = [r for r in reads if owner_of_key.get(r.key) == int(r.reader.rsplit('p', 1)[1])]
    non_owner_reads = [r for r in reads if r not in owner_reads]
    first_success : Dict[str, int] = {}
    retry_bytes = 0
    for r in owner_reads:
        if r.outcome == 'bytes':
            if r.key in first_success:
                retry_bytes += r.size
            else:
                first_success[r.key] = r.size
    serialized = serialized_payload_size(frame_array(SHAPE, seed = 1))
    useful_bytes = sum(first_success.values())
    evidence.update({
        'frames': frames, 'replicas': REPLICAS, 'frame_serialized_bytes': serialized, 'per_replica': per_replica,
        'owner_get_count': len(owner_reads), 'non_owner_get_count': len(non_owner_reads),
        'non_owner_get_bytes': sum(r.size for r in non_owner_reads), 'useful_fetch_bytes': useful_bytes,
        'retry_fetches': log.count(outcome = 'transient'), 'retry_bytes': retry_bytes,
        'blocked_attempts': log.count(outcome = 'blocked'), 'max_inflight': log.max_inflight,
        'completion_s': took, 'faults': schedule.fired(),
    })
    assert not non_owner_reads, f'{len(non_owner_reads)} non-owner GET(s): {non_owner_reads[:3]}'
    assert useful_bytes == frames * serialized, (useful_bytes, frames * serialized)
    assert len(first_success) == frames
    assert evidence['retry_fetches'] == 1 and retry_bytes == 0, (evidence['retry_fetches'], retry_bytes)
    assert evidence['blocked_attempts'] == 0, 'the blocked replica tried to fetch a frame it does not own'
    for r in range(REPLICAS):
        state = rig.consumer_state('child', 'parent', replica = r)
        assert state.pending == 0 and state.unacked == 0, (r, state)
    return schedule


@pytest.mark.case('PAY-018')
@pytest.mark.level('broker')
def test_pay_018_partition_ownership_is_checked_before_loading_large_image(nats_url, redis_url, evidence_dir,
                                                                           record_faults, monkeypatch) -> None:
    '''
    PAY-018 (P2, integration, broker): Partition ownership is checked before loading large image
    bytes.

    Acceptance: For no-failure single-owner fixture, nonowner frame GET count is zero and useful
    frame fetch bytes are approximately N*S plus declared transport overhead, not replicas*N*S.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, 'MAX_INLINE_PAYLOAD_BYTES', 1024)     # every frame offloads
    evidence : Dict[str, Any] = {}
    rig = JetStreamRig(nats_url, BATCH, _specs(), redis_url = redis_url, ack_wait = 30)
    try:
        schedule = _oracle_pay_018(rig, evidence)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'get_metrics.json', evidence)
    record_faults(schedule)


@pytest.mark.case('PAY-018')
@pytest.mark.level('broker')
@pytest.mark.variant('memory')
def test_pay_018_memory_backends_route_before_hydrating(evidence_dir, record_faults, monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, 'MAX_INLINE_PAYLOAD_BYTES', 1024)
    evidence : Dict[str, Any] = {}
    rig = MemoryRig(BATCH, specs = _specs())
    try:
        with rig.ticking():                                  # the injected retry's NAK delay runs on the fake clock
            schedule = _oracle_pay_018(rig, evidence)
    finally:
        rig.close()
    write_evidence(evidence_dir, 'get_metrics.json', evidence)
    record_faults(schedule)


@pytest.mark.negative_control(of = 'PAY-018')
def test_pay_018_detects_a_replica_that_hydrates_before_deciding_ownership(monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    monkeypatch.setattr(nats_messenger, 'MAX_INLINE_PAYLOAD_BYTES', 1024)
    defects_pay.hydrate_before_ownership(monkeypatch)
    rig = MemoryRig(BATCH, specs = _specs())
    try:
        with rig.ticking():
            assert defects.detects(_oracle_pay_018, rig, {}, 24)
    finally:
        rig.close()
