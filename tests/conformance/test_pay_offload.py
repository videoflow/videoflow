'''
Conformance cases: PAY-001, PAY-020.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. Oracles are plain functions taking the component under
test as an argument, so the paired negative control can run the same oracle against
the reviewed defect (``defects_pay.py``) and prove it fails.
'''
from __future__ import absolute_import, division, print_function

import zlib
from typing import Any, Callable, Dict, List, Optional

import defects
import defects_pay
import numpy as np
import pytest
from _payloads import (
    INLINE_THRESHOLD,
    MEGAFRAME_SHAPE,
    JetStreamRig,
    MemoryRig,
    frame_array,
    sha256,
    spec,
    write_evidence,
)

from videoflow.backends import faults
from videoflow.backends.memory.clock import FakeClock
from videoflow.backends.memory.payload import MemoryPayloadStore
from videoflow.backends.outcomes import Known
from videoflow.backends.payload import RetentionContract
from videoflow.core import constants
from videoflow.core.constants import BATCH
from videoflow.core.errors import ConfigError, ResourceUnavailable
from videoflow.messaging import nats_messenger
from videoflow.wire.serialization import (
    ENVELOPE_OVERHEAD_BYTES,
    MSG_TYPE_DATA,
    BlobStore,
    encode_envelope,
    peek_envelope,
    safe_inline_threshold,
    serialized_payload_size,
)

pytestmark = pytest.mark.timeout(180)

# -- PAY-001 ----------------------------------------------------------------------

#: The catalog's fixture: a 1000x1000 uint8 frame of exactly 1 000 000 bytes.
MEGAFRAME = frame_array(MEGAFRAME_SHAPE, seed = 1)
#: A NATS server's default ``max_payload``; the memory model is given the same limit.
BROKER_MAX_PAYLOAD = 1 << 20
#: An operator's unsafe override: larger than the broker can carry.
UNSAFE_THRESHOLD = 2 << 20
#: A frame the unsafe threshold would inline and the broker would refuse.
OVERSIZE_FRAME = frame_array((1100, 1000), seed = 3)


def _array_with_serialized_size(target : int) -> np.ndarray:
    '''A 1-D uint8 array whose *serialized* (Tensor-framed) size is exactly ``target``.'''
    for n in range(target - 64, target + 1):
        arr = frame_array((n,), seed = n)
        if serialized_payload_size(arr) == target:
            return arr
    raise AssertionError(f'no uint8 array serializes to exactly {target} bytes')


def _publish_or_fail(messenger : Any, payload : Any, what : str) -> None:
    '''A refused or failed publication of a valid payload is the defect, reported as one.'''
    try:
        messenger.publish_message(payload)
    except Exception as e:  # noqa: BLE001 — every failure here is the oracle's verdict
        pytest.fail(f'{what}: the publication failed instead of fitting the broker limit: {type(e).__name__}: {e}')


def _round_trip(rig : Any, child : Any, parent : Any, payload : np.ndarray, max_payload : int,
                expect_offload : bool, what : str) -> Dict[str, Any]:
    '''Publish, inspect the envelope the broker holds, receive it back and compare bytes.'''
    _publish_or_fail(parent, payload, what)
    body = rig.retained('parent')[-1]
    peeked = peek_envelope(body)
    # The store's own record of what it holds, read before the sole reader's ack reclaims it.
    stored = rig.store.ref_for_key(peeked['blob_ref']).size if peeked['blob_ref'] else None
    inputs = child.receive_message()
    received = inputs['parent']['message']
    child.ack_inputs()
    record = {
        'what': what, 'shape': list(payload.shape), 'dtype': str(payload.dtype), 'nbytes': int(payload.nbytes),
        'serialized': serialized_payload_size(payload), 'envelope_bytes': len(body),
        'offloaded': peeked['blob_ref'] is not None, 'stored_bytes': stored,
        'original_sha256': sha256(payload.tobytes()),
        'received_sha256': sha256(np.ascontiguousarray(received).tobytes()),
        'received_shape': list(np.shape(received)),
    }
    assert len(body) <= max_payload, f'{what}: envelope of {len(body)} bytes exceeds max_payload {max_payload}'
    assert record['offloaded'] == expect_offload, record
    assert record['received_sha256'] == record['original_sha256'] and record['received_shape'] == record['shape'], record
    return record


def _oracle_pay_001(rig : Any, max_payload : int, evidence : Dict[str, Any],
                    monkeypatch : pytest.MonkeyPatch) -> faults.FaultSchedule:
    schedule = faults.FaultSchedule({'publish.send.before': faults.Delay(0.0)})
    specimens : List[Dict[str, Any]] = []
    side_effects : Dict[str, Any] = {}
    with schedule:
        child = rig.messenger('child', ['parent'])
        parent = rig.messenger('parent', [], blob_reader_ids = ['child'])
        negotiated = parent._inline_threshold
        assert negotiated == safe_inline_threshold(max_payload, INLINE_THRESHOLD) == INLINE_THRESHOLD, negotiated
        # (a) the megaframe: a compact reference on the wire, exact bytes back.
        record = _round_trip(rig, child, parent, MEGAFRAME, max_payload, True, 'megaframe')
        assert record['envelope_bytes'] < 1024, f'the reference is not compact: {record["envelope_bytes"]} bytes'
        assert record['stored_bytes'] == record['serialized'] == MEGAFRAME.nbytes + record['serialized'] - MEGAFRAME.nbytes
        assert record['stored_bytes'] > MEGAFRAME.nbytes, record   # the store holds the framed bytes, not nbytes
        specimens.append(record)
        # (b) boundary specimens, measured by the serializer, not nbytes.
        threshold = negotiated
        specimens.append(_round_trip(rig, child, parent, _array_with_serialized_size(threshold - 1), max_payload,
                                     False, 'serialized == threshold - 1'))
        specimens.append(_round_trip(rig, child, parent, _array_with_serialized_size(threshold), max_payload,
                                     False, 'serialized == threshold'))
        specimens.append(_round_trip(rig, child, parent, _array_with_serialized_size(threshold + 1), max_payload,
                                     True, 'serialized == threshold + 1'))
        at_nbytes = frame_array((threshold,), seed = 7)
        assert at_nbytes.nbytes == threshold and serialized_payload_size(at_nbytes) > threshold
        specimens.append(_round_trip(rig, child, parent, at_nbytes, max_payload, True,
                                     'nbytes == threshold (serialized above it)'))
        sends_before = schedule.fired().get('publish.send.before', 0)
        # (c) no store: an explicit failure before any send, never a publication.
        bare = rig.messenger('bare', [], store = None)
        bare._payload_store = None
        bare._blob_store = None
        bare._bridge = None
        with pytest.raises(ValueError, match = 'no blob_store'):
            bare.publish_message(MEGAFRAME)
        side_effects['absent_store'] = {'sends': schedule.fired().get('publish.send.before', 0) - sends_before,
                                        'publication_stats': dict(bare.publication_stats)}
        assert side_effects['absent_store']['sends'] == 0 and not bare.publication_stats
        # (d) an unsafe threshold with a store: lowered to what the broker carries.
        monkeypatch.setattr(nats_messenger, 'MAX_INLINE_PAYLOAD_BYTES', UNSAFE_THRESHOLD)
        unsafe_parent = rig.messenger('parent', [], blob_reader_ids = ['child'])
        # The same producer, restarted with the unsafe setting: it continues its
        # sequence, or the broker would deduplicate its publications as retries.
        unsafe_parent._trace_counter = parent._trace_counter
        safe = safe_inline_threshold(max_payload, UNSAFE_THRESHOLD)
        side_effects['unsafe_threshold'] = {'configured': UNSAFE_THRESHOLD, 'negotiated': unsafe_parent._inline_threshold,
                                            'safe': safe, 'max_payload': max_payload}
        assert unsafe_parent._inline_threshold <= max_payload - ENVELOPE_OVERHEAD_BYTES, side_effects
        assert serialized_payload_size(OVERSIZE_FRAME) > max_payload
        specimens.append(_round_trip(rig, child, unsafe_parent, OVERSIZE_FRAME, max_payload, True,
                                     'oversize under an unsafe threshold'))
        specimens.append(_round_trip(rig, child, unsafe_parent, _array_with_serialized_size(safe + 1), max_payload,
                                     True, 'serialized == safe + 1 under an unsafe threshold'))
        specimens.append(_round_trip(rig, child, unsafe_parent, _array_with_serialized_size(safe), max_payload,
                                     False, 'serialized == safe under an unsafe threshold'))
        # (e) an unsafe threshold and no store: a configuration error before anything is published.
        sends_before = schedule.fired().get('publish.send.before', 0)
        try:
            rig.messenger('bare2', [], store = None)   # constructing a producer negotiates the threshold
            raise AssertionError('an unsafe inline threshold with no store was accepted')
        except ConfigError as e:
            side_effects['unsafe_without_store'] = {'error': e.code, 'message': str(e)[:200], 'remedy': e.remedy,
                                                    'sends': schedule.fired().get('publish.send.before', 0) - sends_before}
        assert side_effects['unsafe_without_store']['sends'] == 0
    evidence.update({'negotiated_broker_limit': max_payload, 'inline_threshold': negotiated,
                     'specimens': specimens, 'side_effects': side_effects, 'faults': schedule.fired(),
                     'publication_stats': {'parent': dict(parent.publication_stats),
                                           'unsafe_parent': dict(unsafe_parent.publication_stats)}})
    assert 'rejected' not in parent.publication_stats and 'rejected' not in unsafe_parent.publication_stats
    assert 'dropped' not in parent.publication_stats and 'dropped' not in unsafe_parent.publication_stats
    return schedule


class _MemoryRigWithBareStore(MemoryRig):
    '''A memory rig whose ``store = None`` request yields a messenger with no payload store at all.'''
    def messenger(self, name : str, parents : Any, run_id : Optional[str] = None,
                  store : Any = 'default', **kwargs : Any) -> Any:
        if store is None:
            kwargs['payload_store'] = None
            m = nats_messenger.NATSMessenger(_node(name), list(parents), self.nats_url, self.flow_id, self.flow_type,
                                             run_id or self.run_id, backend = self.backend, **kwargs)
            self._messengers.append(m)
            return m
        return super().messenger(name, parents, run_id, None if store == 'default' else store, **kwargs)


class _JetStreamRigWithBareStore(JetStreamRig):
    def messenger(self, name : str, parents : Any, run_id : Optional[str] = None,
                  store : Any = 'default', nats_url : Optional[str] = None, **kwargs : Any) -> Any:
        if store is None:
            kwargs.setdefault('ack_wait', self.ack_wait)
            kwargs.setdefault('max_retries', self.max_retries)
            m = nats_messenger.NATSMessenger(_node(name), list(parents), nats_url or self.nats_url, self.flow_id,
                                             self.flow_type, run_id or self.run_id, payload_store = None, **kwargs)
            self._messengers.append(m)
            return m
        return super().messenger(name, parents, run_id, None if store == 'default' else store, nats_url, **kwargs)


def _node(name : str) -> Any:
    from _payloads import StubNode
    return StubNode(name)


@pytest.mark.case('PAY-001')
@pytest.mark.level('process')
def test_pay_001_one_megabyte_frames_offload_safely_and_enforce_envelope(evidence_dir, record_faults,
                                                                         monkeypatch) -> None:
    '''
    PAY-001 (P0, payload, process): One-megabyte frames offload safely and enforce envelope
    limits.

    Acceptance: Payload bytes match exactly; transmitted envelope fits negotiated broker limits.
    Absent store produces explicit failure and zero falsely accepted publications.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    rig = _MemoryRigWithBareStore(BATCH, max_payload_bytes = BROKER_MAX_PAYLOAD)
    try:
        schedule = _oracle_pay_001(rig, BROKER_MAX_PAYLOAD, evidence, monkeypatch)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'offload_specimens.json', evidence)
    record_faults(schedule)


@pytest.mark.case('PAY-001')
@pytest.mark.level('broker')
@pytest.mark.variant('jetstream')
def test_pay_001_the_broker_limit_is_read_back_and_honoured(nats_url, redis_url, evidence_dir, record_faults,
                                                           monkeypatch) -> None:
    '''The same specimens against a JetStream server's own ``max_payload`` and a Redis store.'''
    monkeypatch.setattr(constants, 'RFC0006', True)
    evidence : Dict[str, Any] = {}
    specs = [spec('parent', [], 'producer', True), spec('child', ['parent'], 'consumer', False)]
    rig = _JetStreamRigWithBareStore(nats_url, BATCH, specs, redis_url = redis_url)
    try:
        probe = rig.messenger('probe', [])
        observed = probe._backend.capabilities().max_payload_bytes
        assert isinstance(observed, Known), f'max_payload was not read back: {observed}'
        evidence['max_payload_read_back'] = observed.value
        schedule = _oracle_pay_001(rig, observed.value, evidence, monkeypatch)
    finally:
        rig.close()
        write_evidence(evidence_dir, 'offload_specimens.json', evidence)
    record_faults(schedule)


@pytest.mark.negative_control(of = 'PAY-001')
def test_pay_001_detects_an_unnegotiated_inline_threshold(monkeypatch) -> None:
    monkeypatch.setattr(constants, 'RFC0006', True)
    defects_pay.unnegotiated_threshold(monkeypatch)
    rig = _MemoryRigWithBareStore(BATCH, max_payload_bytes = BROKER_MAX_PAYLOAD)
    try:
        assert defects.detects(_oracle_pay_001, rig, BROKER_MAX_PAYLOAD, {}, monkeypatch)
    finally:
        rig.close()


# -- PAY-020 ----------------------------------------------------------------------

#: The catalog's decoded frame: 1920x1080x3 uint8 = 6 220 800 bytes.
HD_SHAPE = (1080, 1920, 3)
HD_BYTES = 6_220_800
TIGHT_BUDGET = 2 << 20
ADEQUATE_BUDGET = 8 << 20


def _compressed_specimen() -> tuple:
    '''
    A 1080p frame of 3x3 flat blocks — content a lossless compressor shrinks to
    about a megabyte — and its compressed source. The frame is *read back through
    the compressed bytes*, as a worker decodes its input, so the estimator is
    handed exactly what a producer would hand it.
    '''
    blocks = np.random.default_rng(20).integers(0, 256, (HD_SHAPE[0] // 3, HD_SHAPE[1] // 3, 3), dtype = np.uint8)
    frame = np.repeat(np.repeat(blocks, 3, axis = 0), 3, axis = 1)
    compressed = zlib.compress(frame.tobytes(), 6)
    decoded = np.frombuffer(zlib.decompress(compressed), dtype = np.uint8).reshape(HD_SHAPE)
    return compressed, decoded


class _RecordingStore(BlobStore):
    '''Records every byte length the encoder hands a store — the instrumented payload write.'''
    def __init__(self) -> None:
        self.sizes : List[int] = []

    def put(self, data : bytes, ttl_seconds : int = 0) -> str:
        self.sizes.append(len(data))
        return 'vf-blob-recorded'

    def put_with_readers(self, data : bytes, readers : int, ttl_seconds : int = 0) -> str:
        return self.put(data, ttl_seconds)


def _real_estimator(frame : np.ndarray, compressed_size : int) -> int:
    '''The framework's sizing: the serializer's own byte count of the decoded array.'''
    return serialized_payload_size(frame)


def _oracle_pay_020(estimator : Callable[[np.ndarray, int], int], evidence : Dict[str, Any],
                    stored_size_of : Optional[Callable[[bytes], Dict[str, int]]] = None) -> None:
    compressed, decoded = _compressed_specimen()
    assert decoded.nbytes == HD_BYTES and decoded.shape == HD_SHAPE
    assert HD_BYTES // 10 < len(compressed) < HD_BYTES // 3, len(compressed)   # near a megabyte, far below raw
    predicted = estimator(decoded, len(compressed))
    # Admission under 2 MB happens on the prediction, before any write.
    tight = MemoryPayloadStore(FakeClock(), max_bytes = TIGHT_BUDGET)
    admitted = predicted <= TIGHT_BUDGET
    evidence.update({'compressed_bytes': len(compressed), 'decoded_bytes': int(decoded.nbytes),
                     'predicted_stored_bytes': predicted, 'tight_budget': TIGHT_BUDGET, 'admitted_under_tight': admitted})
    assert not admitted, f'a {decoded.nbytes}-byte frame was admitted against {TIGHT_BUDGET} bytes (predicted {predicted})'
    # And the store itself refuses the bytes rather than writing over its budget.
    recorder = _RecordingStore()
    encode_envelope('parent', 'f', 'r', 't1', 1, MSG_TYPE_DATA, None, decoded, blob_store = recorder,
                    inline_threshold = INLINE_THRESHOLD)
    measured = recorder.sizes[0]
    with pytest.raises(ResourceUnavailable):
        tight.put(b'\0' * measured, 'cam:t1:1', RetentionContract(60, 60, True, ('det',)))
    assert tight.object_count() == 0 and tight.stored_bytes() == 0
    adequate = MemoryPayloadStore(FakeClock(), max_bytes = ADEQUATE_BUDGET)
    payload_bytes = encode_envelope('parent', 'f', 'r', 't1', 1, MSG_TYPE_DATA, None, decoded,
                                    blob_store = _Capture(), inline_threshold = INLINE_THRESHOLD)
    del payload_bytes
    captured = _Capture.last
    assert captured is not None and len(captured) == measured
    ref = adequate.put(captured, 'cam:t1:1', RetentionContract(60, 60, True, ('det',)))
    framing = measured - decoded.nbytes
    evidence.update({'measured_serialized_bytes': measured, 'stored_bytes': adequate.stored_bytes(),
                     'tensor_framing_bytes': framing, 'envelope_overhead_reserve_bytes': ENVELOPE_OVERHEAD_BYTES,
                     'admitted_under_adequate': True, 'ref_size': ref.size})
    assert predicted == measured == ref.size == adequate.stored_bytes(), evidence
    assert 0 < framing < 64, framing
    if stored_size_of is not None:
        evidence['backend'] = stored_size_of(captured)
        assert evidence['backend']['strlen'] == measured, evidence['backend']
        assert evidence['backend']['memory_usage'] >= measured, evidence['backend']


class _Capture(BlobStore):
    last : Optional[bytes] = None

    def put(self, data : bytes, ttl_seconds : int = 0) -> str:
        _Capture.last = bytes(data)
        return 'vf-blob-captured'

    def put_with_readers(self, data : bytes, readers : int, ttl_seconds : int = 0) -> str:
        return self.put(data, ttl_seconds)


@pytest.mark.case('PAY-020')
@pytest.mark.level('process')
def test_pay_020_raw_decoded_frame_size_drives_capacity_estimates_and(evidence_dir) -> None:
    '''
    PAY-020 (P1, payload, process): Raw decoded frame size drives capacity estimates and
    admission.

    Acceptance: 6220800-byte raw frame is never admitted against only 2MB available capacity;
    predicted stored-byte size agrees with measured serialized size, with separately declared
    allocator overhead.
    '''
    evidence : Dict[str, Any] = {}
    _oracle_pay_020(_real_estimator, evidence)
    write_evidence(evidence_dir, 'capacity_estimate.json', evidence)


@pytest.mark.case('PAY-020')
@pytest.mark.level('process')
@pytest.mark.variant('redis')
def test_pay_020_stored_bytes_match_on_a_redis_server(redis_url, evidence_dir) -> None:
    '''The measured write on a real store: ``STRLEN`` equals the serialized size; ``MEMORY USAGE`` is the allocator's own overhead, declared apart.'''
    from _brokers import redis_client

    def stored_size_of(data : bytes) -> Dict[str, int]:
        with redis_client(redis_url) as client:
            key = 'vf-blob-pay020-' + sha256(data)[:16]
            client.set(key, data, ex = 60)
            try:
                return {'strlen': int(client.strlen(key)), 'memory_usage': int(client.memory_usage(key) or 0)}
            finally:
                client.delete(key)                          # the shared dev instance: only this key

    evidence : Dict[str, Any] = {}
    _oracle_pay_020(_real_estimator, evidence, stored_size_of)
    write_evidence(evidence_dir, 'capacity_estimate.json', evidence)


@pytest.mark.negative_control(of = 'PAY-020')
def test_pay_020_detects_an_estimator_sized_from_the_compressed_source() -> None:
    assert defects.detects(_oracle_pay_020, defects_pay.compressed_size_estimator, {})
