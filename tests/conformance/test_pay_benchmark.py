'''
Conformance cases: PAY-017, PAY-022.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it.

Benchmarks: their workload parameters and SLOs come from the operator's
thresholds (``bench`` gate) and the measurements are always written with the
environment they were taken on. PAY-017 runs on the in-memory backends (the
object inventory is what it measures, not a transport); PAY-022 on the compose
broker and Redis, the real frame path.
'''
from __future__ import absolute_import, division, print_function

import hashlib
import json
import resource
import threading
import time
from typing import Any, Dict, List, Optional

import defects
import defects_pay
import numpy as np
import pytest
from _bench import Verdict, environment_manifest, summary, varz
from _payloads import JetStreamRig, KeyRecordingStore, MemoryRig, ReadLog, frame_array, object_exists, spec
from _status import unsupported

from videoflow.backends.memory.payload import MemoryPayloadStore, StaticLedger
from videoflow.core.constants import BATCH, REALTIME
from videoflow.wire.serialization import peek_envelope


def _chain(stages : int, sinks : int = 1) -> List[Any]:
    specs = [spec('src', [], 'producer', True)]
    previous = 'src'
    for i in range(1, stages + 1):
        specs.append(spec(f's{i}', [previous], 'processor', True))
        previous = f's{i}'
    for j in range(sinks):
        specs.append(spec(f'sink{j}', [previous], 'consumer', False))
    return specs


class _Stage(threading.Thread):
    '''Receive, forward the identical object (or a transform of it), settle — one pass-through stage.'''
    def __init__(self, messenger : Any, expected : int, transform : Optional[Any] = None,
                 on_input : Optional[Any] = None, hold_seconds : float = 0.0, name : str = 'stage') -> None:
        super().__init__(name = name, daemon = True)
        self.messenger = messenger
        self.expected = expected
        self.transform = transform
        self.on_input = on_input
        self.hold_seconds = hold_seconds
        self.seen = 0
        self.error : Optional[BaseException] = None

    def run(self) -> None:
        try:
            while self.seen < self.expected:
                inputs = self.messenger.receive_message()
                if all(v.get('is_stop_signal') for v in inputs.values()):
                    return
                parent = next(iter(inputs.values()))
                message = parent['message']
                if self.on_input is not None:
                    self.on_input(message, parent)
                if callable(self.transform):
                    message = self.transform(message)
                if self.hold_seconds:
                    time.sleep(self.hold_seconds)
                if self.transform is not False:          # False: a sink, publishes nothing
                    self.messenger.publish_message(message)
                self.messenger.ack_inputs()
                self.seen += 1
        except BaseException as e:  # noqa: BLE001 — surfaced to the test thread
            self.error = e


def _digest(frame : np.ndarray) -> str:
    return hashlib.sha256(np.ascontiguousarray(frame).tobytes()).hexdigest()[:16]


def _oracle_pay_017(thresholds : Dict[str, Any], evidence : Dict[str, Any]) -> Dict[str, Any]:
    '''
    ``frames`` unchanged frames through ``stages`` pass-through stages and a
    slow final reader. Measures the objects the store holds against the frames
    that entered, verifies every stage read identical bytes, and that the last
    reader's ownership kept every object until it was done.
    '''
    frames = int(thresholds.get('frames', 100))
    frame_bytes = int(thresholds.get('frame_bytes', 1_000_000))
    stages = int(thresholds.get('stages', 5))
    side = int(np.sqrt(frame_bytes))
    log = ReadLog()
    inner = MemoryPayloadStore(None)
    recording = KeyRecordingStore(inner, 'publisher', log)
    rig = MemoryRig(BATCH, store = recording, max_payload_bytes = 64 * 1024, specs = _chain(stages))
    checksums : Dict[str, List[str]] = {}
    threads : List[_Stage] = []
    try:
        chain = ['src'] + [f's{i}' for i in range(1, stages + 1)]
        messengers = {}
        for i, name in enumerate(chain):
            readers = [chain[i + 1]] if i + 1 < len(chain) else ['sink0']
            messengers[name] = rig.messenger(name, [chain[i - 1]] if i else [], blob_reader_ids = readers)
        messengers['sink0'] = rig.messenger('sink0', [chain[-1]])

        def recorder(stage : str) -> Any:
            def on_input(message : Any, _parent : Any) -> None:
                checksums.setdefault(stage, []).append(_digest(message))
            return on_input
        for name in chain[1:]:
            threads.append(_Stage(messengers[name], frames, on_input = recorder(name), name = name))
        sink = _Stage(messengers['sink0'], frames, transform = False, on_input = recorder('sink0'),
                      hold_seconds = float(thresholds.get('sink_hold_seconds', 0.01)), name = 'sink0')
        threads.append(sink)
        for t in threads:
            t.start()
        originals = []
        t0 = time.monotonic()
        for n in range(frames):
            frame = frame_array((side, side), seed = n)
            originals.append(_digest(frame))
            messengers['src'].publish_message(frame)
        for t in threads:
            t.join(300)
            assert not t.is_alive(), f'{t.name} did not finish'
            assert t.error is None, f'{t.name}: {t.error!r}'
        took = time.monotonic() - t0
        # Every stage and the sink read exactly the bytes that entered.
        for stage, seen in checksums.items():
            assert seen == originals, f'{stage} read different bytes than were published'
        objects = list(recording.keys)
        reclaimed_at = time.monotonic()
        remaining = [k for k in objects if object_exists(inner, k)]
        reclaim_seconds = time.monotonic() - reclaimed_at
        return {'frames': frames, 'stages': stages, 'frame_bytes': frame_bytes, 'objects_created': len(objects),
                'bytes_written': int(inner.metrics['put_bytes']), 'puts': int(inner.metrics['put_count']),
                'amplification': len(objects) / frames, 'gets_by_reader': log.by_reader(), 'took_seconds': took,
                'objects_remaining_after_completion': len(remaining), 'reclaim_seconds': reclaim_seconds,
                'store_declares_reference_forwarding': inner.capabilities().reference_forwarding}
    finally:
        for t in threads:
            if t.is_alive():
                t.messenger.quiesce()
        rig.close()


def _expected_objects(frames : int, stages : int, forwarding_declared : bool) -> int:
    '''What the store's declaration promises: one canonical object per frame under forwarding, one per hop otherwise.'''
    return frames if forwarding_declared else frames * (stages + 1)


def _judge_pay_017(measured : Dict[str, Any], thresholds : Dict[str, Any]) -> Verdict:
    '''The comparison: reclamation within G, and the object count the declared capability promised.'''
    verdict = Verdict()
    verdict.check('reclaimed within G', measured['reclaim_seconds'],
                  measured['objects_remaining_after_completion'] == 0
                  and measured['reclaim_seconds'] <= float(thresholds.get('reclaim_seconds_max', 5.0)), thresholds.get('reclaim_seconds_max'))
    expected = _expected_objects(measured['frames'], measured['stages'], measured['store_declares_reference_forwarding'])
    verdict.check('objects match the declared capability', measured['objects_created'],
                  measured['objects_created'] == expected, expected)
    return verdict


@pytest.mark.case('PAY-017')
@pytest.mark.level('benchmark')
def test_pay_017_unchanged_frames_can_traverse_metadata_stages_without_new(bench, evidence_dir) -> None:
    '''
    PAY-017 (P1, payload, benchmark): Unchanged frames can traverse metadata stages without new
    image copies.

    Acceptance: For the reference-forwarding capability, 100 unchanged frames create 100
    canonical image objects rather than 500; every stage reads identical bytes and final cleanup
    completes within G.

    Neither shipped store declares ``reference_forwarding``: the measurement is
    taken (one object per frame-bearing hop, declared as the amplification), every
    stage's bytes are verified identical and reclamation is timed — and the
    reference-forwarding half is UNSUPPORTED, truthfully, unless a composed store
    declares it.
    '''
    evidence : Dict[str, Any] = {'environment': environment_manifest({'thresholds': bench})}
    measured = _oracle_pay_017(bench, evidence)
    evidence['measured'] = measured
    verdict = _judge_pay_017(measured, bench)
    forwarding = measured['store_declares_reference_forwarding']
    evidence['verdict'] = verdict.checks
    (evidence_dir / 'reference_forwarding.json').write_text(json.dumps(evidence, indent = 2, default = str))
    assert not verdict.failed, verdict.failed
    if not forwarding:
        unsupported(f'the composed payload store declares no reference_forwarding: {measured["objects_created"]} objects '
                    f'for {measured["frames"]} frames over {measured["stages"]} stages (amplification '
                    f'{measured["amplification"]:.0f}x, declared per-hop copies); bytes identical at every stage, '
                    f'reclaimed in {measured["reclaim_seconds"]:.3f}s')


@pytest.mark.negative_control(of = 'PAY-017')
def test_pay_017_detects_a_store_that_declares_forwarding_it_does_not_do(monkeypatch) -> None:
    '''A store advertising reference forwarding while writing one copy per hop: the declared count and the measured one disagree.'''
    defects_pay.false_reference_forwarding(monkeypatch)
    measured = _oracle_pay_017({'frames': 10, 'stages': 2, 'frame_bytes': 100_000}, {})
    assert measured['store_declares_reference_forwarding'] is True and measured['objects_created'] == 30
    assert _judge_pay_017(measured, {}).failed


# -- PAY-022 ------------------------------------------------------------------------------------------------

def _redis_stats(url : str) -> Dict[str, Any]:
    import redis  # optional dep (redis extra)
    client = redis.Redis.from_url(url, socket_timeout = 5)
    try:
        info = client.info()
        return {'used_memory': int(info.get('used_memory', 0)), 'net_input_bytes': int(info.get('total_net_input_bytes', 0)),
                'net_output_bytes': int(info.get('total_net_output_bytes', 0)), 'keys': client.dbsize()}
    finally:
        client.close()


def _broker_bytes() -> Dict[str, int]:
    v = varz() or {}
    return {'in_bytes': int(v.get('in_bytes', 0)), 'out_bytes': int(v.get('out_bytes', 0))}


class _Camera(threading.Thread):
    '''Publishes ``frame_bytes`` frames at ``fps`` for ``duration`` seconds, stamping the send time inside the payload.'''
    def __init__(self, messenger : Any, fps : float, frame_bytes : int, duration : float, name : str) -> None:
        super().__init__(name = name, daemon = True)
        self.messenger = messenger
        self.fps = fps
        self.duration = duration
        side = int(np.sqrt(frame_bytes))
        self.frame = frame_array((side, side), seed = hash(name) & 0xFFFF)
        self.published = 0
        self.error : Optional[BaseException] = None

    def run(self) -> None:
        try:
            period = 1.0 / self.fps
            end = time.monotonic() + self.duration
            next_at = time.monotonic()
            while time.monotonic() < end:
                self.messenger.publish_message({'ts': time.time(), 'camera': self.name, 'n': self.published,
                                                'frame': self.frame})
                self.published += 1
                next_at += period
                delay = next_at - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
        except BaseException as e:  # noqa: BLE001
            self.error = e


class _Reader(threading.Thread):
    '''A consumer measuring frame age at receipt; ``pause`` makes it a slow reader for a while.'''
    def __init__(self, messenger : Any, stop : threading.Event, name : str, pause : Optional[tuple] = None) -> None:
        super().__init__(name = name, daemon = True)
        self.messenger = messenger
        self.stop_event = stop
        self.pause = pause                       # (after_seconds, for_seconds)
        self.ages : List[float] = []
        self.ages_after_pause : List[float] = []
        self.received = 0
        self.error : Optional[BaseException] = None
        self.paused_at : Optional[float] = None

    def run(self) -> None:
        started = time.monotonic()
        try:
            while not self.stop_event.is_set():
                inputs = self.messenger.receive_message()
                if all(v.get('is_stop_signal') for v in inputs.values()):
                    return
                message = next(iter(inputs.values()))['message']
                age = time.time() - float(message['ts'])
                (self.ages_after_pause if self.paused_at is not None else self.ages).append(age)
                self.received += 1
                self.messenger.ack_inputs()
                if self.pause and self.paused_at is None and time.monotonic() - started >= self.pause[0]:
                    self.paused_at = time.monotonic()
                    time.sleep(self.pause[1])
        except BaseException as e:  # noqa: BLE001
            self.error = e


def _drop_ratio(offered : int, received : int) -> Optional[float]:
    '''The fraction of offered frames a reader never got: discarded work, never sustained throughput.'''
    return (1 - received / offered) if offered else None


def _meets_slo(point : Dict[str, Any], thresholds : Dict[str, Any]) -> bool:
    '''A configuration point meets the SLOs only on its unpaused readers' age *and* drops.'''
    return (point['worst_unpaused_age_p95'] <= float(thresholds['max_frame_age_seconds'])
            and point['worst_unpaused_drop_ratio'] <= float(thresholds.get('max_drop_ratio', 0.0)))


def _run_point(nats_url : str, redis_url : str, cameras : int, stages : int, consumers : int,
               thresholds : Dict[str, Any]) -> Dict[str, Any]:
    '''One configuration: C cameras -> K frame-bearing stages -> R consumers (one paused for W), REALTIME.'''
    fps = float(thresholds['fps'])
    frame_bytes = int(thresholds['frame_bytes'])
    duration = float(thresholds['duration_seconds'])
    hold = float(thresholds.get('hold_seconds', 0.0))
    # The cameras are replicas of one source node publishing on one channel — a
    # stage downstream competes for their frames; separate producer nodes would
    # make the stage a join, which is a different graph.
    specs = [spec('cam', [], 'producer', True, nb_tasks = cameras)]
    previous = ['cam']
    for k in range(1, stages + 1):
        specs.append(spec(f'k{k}', previous, 'processor', True))
        previous = [f'k{k}']
    sink_names = [f'r{j}' for j in range(consumers)]
    specs.extend(spec(r, previous, 'consumer', False) for r in sink_names)
    log = ReadLog()
    rig = JetStreamRig(nats_url, REALTIME, specs, redis_url = redis_url)
    stop = threading.Event()
    threads : List[threading.Thread] = []
    redis_before, broker_before = _redis_stats(redis_url), _broker_bytes()
    rss_before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    memory_samples : List[int] = []
    try:
        cams = [_Camera(rig.messenger('cam', [], replica_id = i, nb_tasks = cameras,
                                      blob_reader_ids = ['k1'] if stages else sink_names), fps, frame_bytes, duration, f'cam{i}')
                for i in range(cameras)]
        stage_threads = []
        for k in range(1, stages + 1):
            readers = [f'k{k + 1}'] if k < stages else sink_names
            m = rig.messenger(f'k{k}', ['cam'] if k == 1 else [f'k{k - 1}'], blob_reader_ids = readers)
            stage_threads.append(_Stage(m, 1 << 30, name = f'k{k}'))
        readers = []
        for j, r in enumerate(sink_names):
            m = rig.messenger(r, previous)
            # One slow reader among several is the pause/recovery measurement; a
            # lone reader is never paused (its pause would be the whole path's).
            paused = bool(hold) and consumers > 1 and j == consumers - 1
            readers.append(_Reader(m, stop, r, pause = (duration / 3, hold) if paused else None))
        threads = [*stage_threads, *readers]
        for t in threads:
            t.start()
        for c in cams:
            c.start()
        sampler_end = time.monotonic() + duration
        while time.monotonic() < sampler_end:
            memory_samples.append(_redis_stats(redis_url)['used_memory'])
            time.sleep(0.5)
        for c in cams:
            c.join(duration + 30)
        time.sleep(min(2.0, hold + 1.0))
        stop.set()
        for t in threads:
            t.messenger.quiesce()
        for t in threads:
            t.join(30)
        offered = sum(c.published for c in cams)
        # The inventory after the run: a live path discards frames, and every
        # discarded frame's object is owed to readers that will never see it —
        # the eviction reconciliation (BLOB-14 step 4, PAY-012) cancels those.
        # Reported both ways: what was left, and what reconciliation reclaimed.
        created = rig.created_keys()
        retained_keys : set = set()
        for node in ('cam', *[f'k{k}' for k in range(1, stages + 1)]):
            for body in rig.retained(node):
                key = peek_envelope(body).get('blob_ref')
                if key:
                    retained_keys.add(key)
        keys_left = _redis_stats(redis_url)['keys']
        evicted = {key: () for key in created if key not in retained_keys}
        reconciled = rig.store.reconcile(StaticLedger(evicted), 'pay-022:eviction') if evicted else None
        redis_after, broker_after = _redis_stats(redis_url), _broker_bytes()
        per_reader = {}
        for r in readers:
            per_reader[r.name] = {'received': r.received, 'age': summary(r.ages), 'age_after_pause': summary(r.ages_after_pause),
                                  'paused': r.paused_at is not None, 'drop_ratio': _drop_ratio(offered, r.received)}
        unpaused = [r for r in readers if r.pause is None] or readers
        worst_age = max((summary(r.ages).get('p95', 0.0) for r in unpaused), default = 0.0)
        worst_drop = max((per_reader[r.name]['drop_ratio'] or 0.0 for r in unpaused), default = 0.0)
        return {'cameras': cameras, 'stages': stages, 'consumers': consumers, 'offered': offered,
                'offered_rate_hz': offered / duration, 'readers': per_reader, 'worst_unpaused_age_p95': worst_age,
                'worst_unpaused_drop_ratio': worst_drop,
                'store': {'put_bytes_delta_net_in': redis_after['net_input_bytes'] - redis_before['net_input_bytes'],
                          'get_bytes_delta_net_out': redis_after['net_output_bytes'] - redis_before['net_output_bytes'],
                          'used_memory_peak': max(memory_samples, default = 0), 'used_memory_before': redis_before['used_memory'],
                          'keys_before': redis_before['keys'], 'keys_after_run': keys_left, 'keys_after_reconcile': redis_after['keys'],
                          'objects_created': len(created), 'retained_objects': len(retained_keys),
                          'discarded_objects': len(evicted),
                          'reclaimed_by_reconcile': len(reconciled.reclaimed) if reconciled else 0,
                          'unknown_after_reconcile': len(reconciled.unknown) if reconciled else 0},
                'broker': {'in_bytes': broker_after['in_bytes'] - broker_before['in_bytes'],
                           'out_bytes': broker_after['out_bytes'] - broker_before['out_bytes']},
                'gets_by_reader': log.by_reader(),
                'process_rss_delta_kb': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - rss_before,
                'errors': {t.name: repr(t.error) for t in [*threads, *cams] if t.error is not None}}
    finally:
        stop.set()
        for t in threads:
            try:
                t.messenger.quiesce()
            except Exception:  # noqa: BLE001
                pass
        rig.close()


@pytest.mark.case('PAY-022')
@pytest.mark.level('benchmark')
def test_pay_022_frame_path_capacity_benchmark_measures_useful_throughput(bench, nats_url, redis_url, evidence_dir) -> None:
    '''
    PAY-022 (P2, integration, benchmark): Frame-path capacity benchmark measures useful
    throughput and total byte amplification.

    Acceptance: Publish the maximum load meeting explicit SLOs with full environment and
    amplification factors; admission limit must be no greater than the measured sustainable
    capacity with declared headroom. Passing is workload-specific, not a universal scale claim.
    '''
    evidence : Dict[str, Any] = {'environment': environment_manifest({'thresholds': bench}), 'points': []}
    verdict = Verdict()
    sustainable : Dict[tuple, int] = {}
    try:
        for stages in bench.get('stages', [1]):
            for consumers in bench.get('consumers', [1]):
                for cameras in bench['cameras']:
                    point = _run_point(nats_url, redis_url, int(cameras), int(stages), int(consumers), bench)
                    evidence['points'].append(point)
                    assert not point['errors'], point['errors']
                    ok = _meets_slo(point, bench)
                    point['meets_slo'] = ok
                    if ok:
                        sustainable[(int(stages), int(consumers))] = int(cameras)
                    else:
                        break                    # the first failing camera count ends this series
        evidence['sustainable_cameras'] = {f'stages={k[0]},consumers={k[1]}': v for k, v in sustainable.items()}
        declared = int(bench.get('declared_capacity_cameras', 0))
        headroom = float(bench.get('headroom', 0.0))
        if declared:
            floor = min(sustainable.values(), default = 0)
            allowed = int(floor * (1.0 - headroom))
            verdict.check('declared capacity within measured sustainable capacity minus headroom', declared,
                          declared <= allowed, {'sustainable_min': floor, 'headroom': headroom, 'allowed': allowed})
        verdict.check('at least one configuration met the SLOs', len(sustainable), bool(sustainable), '>= 1')
        # Amplification is reported, never hidden: PUT bytes per offered frame and GET bytes per delivered frame.
        for point in evidence['points']:
            delivered = sum(r['received'] for r in point['readers'].values())
            point['amplification'] = {
                'put_bytes_per_offered_frame': point['store']['put_bytes_delta_net_in'] / max(1, point['offered']),
                'get_bytes_per_delivered_frame': point['store']['get_bytes_delta_net_out'] / max(1, delivered),
                'broker_in_per_offered_frame': point['broker']['in_bytes'] / max(1, point['offered'])}
            # Discarded work is accounted for, never sustaining apparent throughput:
            # after eviction reconciliation only the retained (last) frames' objects remain.
            leftover = point['store']['keys_after_reconcile'] - point['store']['keys_before']
            verdict.check(f'no orphaned objects after the point ({point["cameras"]} cams, {point["stages"]} stages)',
                          leftover, leftover <= 3 * (point['stages'] + 1) and point['store']['unknown_after_reconcile'] == 0,
                          'at most the retained frames (3 keys each) after reconciliation')
        evidence['verdict'] = verdict.checks
    finally:
        (evidence_dir / 'frame_path_capacity.json').write_text(json.dumps(evidence, indent = 2, default = str))
    assert not verdict.failed, verdict.failed


@pytest.mark.negative_control(of = 'PAY-022')
def test_pay_022_detects_discards_counted_as_throughput(monkeypatch) -> None:
    '''An accounting that reads every offered frame as delivered lets an overloaded point meet the SLO.'''
    point = {'worst_unpaused_age_p95': 0.02, 'worst_unpaused_drop_ratio': _drop_ratio(240, 120)}
    thresholds = {'max_frame_age_seconds': 1.0, 'max_drop_ratio': 0.0}
    assert not _meets_slo(point, thresholds)                        # half the frames were discarded
    defects_pay.discards_as_throughput(monkeypatch)
    point = {'worst_unpaused_age_p95': 0.02, 'worst_unpaused_drop_ratio': _drop_ratio(240, 120)}
    assert defects.detects(lambda: _meets_slo(point, thresholds) and pytest.fail('an overloaded point met the SLO'))
