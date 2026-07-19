'''
White-paper experiment driver.

Runs the five experiment families reported in the white paper against a live
local NATS JetStream broker (``docker compose up -d`` in the repo root), using
``LocalProcessEngine`` — the same ``videoflow.worker`` code path Kubernetes uses,
with one OS subprocess per node replica. Results are written as JSON under
``results/`` and raw per-message sink records under ``results/raw/``.

    uv run python white-paper/code/experiments/run_experiments.py [e1 e2 e3 e4 e5 e6]

Experiments:

- **e1**  Wire-format microbenchmark: encoded size and encode/decode time of the
          msgpack (v3) vs protobuf (v4) envelope across payload shapes. In-process,
          no broker.
- **e2**  Horizontal scaling: a 20 ms-per-message stage with ``nb_tasks`` in
          {1,2,4,8} competing-consumer replicas; sink-side throughput vs ideal.
- **e3**  Per-hop overhead: identity chains of depth {1,2,4,8} with a paced
          producer; end-to-end latency vs depth.
- **e4**  Delivery semantics under overload: a 100 msg/s producer into a
          40 msg/s stage, BATCH vs REALTIME; delivered fraction and latency.
- **e5**  Partitioned join scaling: a diamond joined on ``trace_id`` with
          {1,2,4} partitioned replicas; throughput and join correctness.
- **e6**  Time-aligned quorum join: two independent producers (one dropping every
          3rd frame) fused with ``mode='time'``, ``quorum=1``; group completeness.
'''
from __future__ import absolute_import, division, print_function

import json
import os
import statistics
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import expnodes  # noqa: E402

from videoflow.core.constants import BATCH, REALTIME  # noqa: E402
from videoflow.core.flow import Flow  # noqa: E402
from videoflow.core.policies import JoinPolicy  # noqa: E402
from videoflow.engines.local import LocalProcessEngine  # noqa: E402

#: White-paper root (this file lives in ``<root>/code/experiments/``). Every other
#: path is derived from it, so the folder layout is stated once per script.
ROOT = os.path.abspath(os.path.join(HERE, '..', '..'))
RESULTS_DIR = os.path.join(ROOT, 'results')
RAW_DIR = os.path.join(RESULTS_DIR, 'raw')
os.makedirs(RAW_DIR, exist_ok = True)


def run_flow(consumers : list, flow_type : str, timeout_s : float = 120.0,
            engine_kwargs : dict = None, env : dict = None, kill : tuple = None) -> dict:
    '''
    Runs a flow to completion on the local engine, with a watchdog that publishes
    the stop control signal if the run exceeds ``timeout_s``. Returns wall-clock
    timings and any worker failures.

    - Arguments:
        - engine_kwargs: extra ``LocalProcessEngine`` constructor arguments \
            (e.g. ``blob_redis_url`` for the E7 blob-store sweep).
        - env: environment overrides set for the duration of the run. Worker \
            subprocesses inherit the driver's environment, so this is how the \
            E7 sweep varies ``VIDEOFLOW_MAX_INLINE_PAYLOAD_BYTES`` per run.
        - kill: ``(node_name, replica_idx, delay_s)`` — SIGKILL that worker \
            ``delay_s`` seconds after launch (E8 fault injection).
    '''
    flow = Flow(consumers, flow_type = flow_type)
    engine = LocalProcessEngine(**(engine_kwargs or {}))
    timed_out = threading.Event()
    killed : dict = {}

    def _watchdog() -> None:
        timed_out.set()
        try:
            engine.signal_flow_termination()
        except Exception as e:
            print(f'  watchdog stop failed: {e}', file = sys.stderr)

    def _kill_worker() -> None:
        # Reaches into the engine's process table (private API, fine for a test
        # harness): find the named replica and SIGKILL it, simulating a hard
        # worker crash (OOM-kill, node failure) with no chance to ack or clean up.
        name, replica_idx, _delay = kill
        for pname, pidx, proc in engine._procs:
            if pname == name and pidx == replica_idx:
                proc.kill()
                killed['pid'] = proc.pid
                killed['at_s'] = round(time.time() - t_start, 2)
                return

    saved_env = {}
    for key, value in (env or {}).items():
        saved_env[key] = os.environ.get(key)
        os.environ[key] = value
    timer = threading.Timer(timeout_s, _watchdog)
    kill_timer = threading.Timer(kill[2], _kill_worker) if kill else None
    t_start = time.time()
    try:
        flow.run(engine)
        t_alloc = time.time()
        timer.start()
        if kill_timer:
            kill_timer.start()
        try:
            flow.join()
        finally:
            timer.cancel()
            if kill_timer:
                kill_timer.cancel()
    finally:
        for key, value in saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    t_end = time.time()
    return {
        'wall_s': t_end - t_start,
        'alloc_s': t_alloc - t_start,
        't_run_start': t_start,
        'timed_out': timed_out.is_set(),
        'failures': engine.failures(),
        'killed': killed or None,
    }


def read_records(path : str) -> list:
    if not os.path.exists(path):
        return []
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def sink_metrics(records : list, t_run_start : float) -> dict:
    '''Delivered count, sink makespan/throughput, startup delay, latency stats.'''
    out = {'delivered': len(records)}
    if not records:
        return out
    t1s = [r['t1'] for r in records]
    out['startup_s'] = min(t1s) - t_run_start
    span = max(t1s) - min(t1s)
    out['sink_makespan_s'] = span
    if len(records) > 1 and span > 0:
        out['throughput_msg_s'] = (len(records) - 1) / span
    lats = [r['t1'] - r['t0'] for r in records if r.get('t0') is not None]
    if lats:
        out['lat_ms_median'] = statistics.median(lats) * 1000
        out['lat_ms_mean'] = statistics.fmean(lats) * 1000
        out['lat_ms_p95'] = statistics.quantiles(lats, n = 20)[-1] * 1000 if len(lats) >= 20 else max(lats) * 1000
        out['lat_ms_max'] = max(lats) * 1000
    return out


def save(name : str, payload : dict) -> None:
    path = os.path.join(RESULTS_DIR, f'{name}.json')
    with open(path, 'w') as f:
        json.dump(payload, f, indent = 2)
    print(f'== wrote {path}')


def raw_path(tag : str) -> str:
    path = os.path.join(RAW_DIR, f'{tag}.jsonl')
    if os.path.exists(path):
        os.remove(path)
    return path


# -- e1: wire-format microbenchmark ----------------------------------------

def exp_serialization() -> None:
    import numpy as np

    from videoflow.wire import serialization as ser

    rng = np.random.default_rng(7)
    detections = {
        'frame_id': 1234,
        'detections': [
            {'label': 'person', 'score': round(float(rng.random()), 4),
             'box': [float(x) for x in rng.random(4) * 1000]}
            for _ in range(20)
        ],
    }
    payloads = {
        'scalar_dict': {'label': 'person', 'score': 0.98, 'box': [10.0, 20.0, 110.0, 220.0]},
        'detections_20': detections,
        'frame_640x480': rng.integers(0, 255, (480, 640, 3), dtype = np.uint8),
        'frame_1920x1080': rng.integers(0, 255, (1080, 1920, 3), dtype = np.uint8),
    }
    def bench(version : int, name : str, payload, blob_store = None, mode : str = 'inline') -> dict:
        reps = 30 if name.startswith('frame') else 300
        enc_times, dec_times = [], []
        buf = b''
        for _ in range(reps):
            t0 = time.perf_counter()
            buf = ser.encode_envelope('prod', 'flowid', 'runid', 'traceid', 1, 'data',
                                    {}, payload, version = version, blob_store = blob_store)
            enc_times.append(time.perf_counter() - t0)
            t0 = time.perf_counter()
            ser.decode_envelope(buf, blob_store = blob_store)
            dec_times.append(time.perf_counter() - t0)
        entry = {
            'version': version,
            'payload': name,
            'mode': mode,
            'size_bytes': len(buf),
            'encode_ms_median': statistics.median(enc_times) * 1000,
            'decode_ms_median': statistics.median(dec_times) * 1000,
            'reps': reps,
        }
        print(f'  v{version} {name} [{mode}]: {len(buf)} B, '
              f'enc {entry["encode_ms_median"]:.3f} ms, dec {entry["decode_ms_median"]:.3f} ms')
        return entry

    results = []
    # Pure-codec numbers: raise the spill threshold so frames stay inline.
    # (Benchmark-only override of the module constant; default is 512 KiB.)
    original_threshold = ser.MAX_INLINE_PAYLOAD_BYTES
    ser.MAX_INLINE_PAYLOAD_BYTES = 64 * 1024 * 1024
    try:
        for version in (3, 4):
            for name, payload in payloads.items():
                results.append(bench(version, name, payload))
    finally:
        ser.MAX_INLINE_PAYLOAD_BYTES = original_threshold
    # Realistic large-frame path: payload spills to the Redis blob store and the
    # envelope carries only a reference (includes the Redis round trip).
    blob = ser.RedisBlobStore('redis://localhost:6379/0')
    for version in (3, 4):
        for name in ('frame_640x480', 'frame_1920x1080'):
            results.append(bench(version, name, payloads[name], blob_store = blob, mode = 'blob'))
    save('e1_serialization', {'results': results})


# -- e2: horizontal scaling -------------------------------------------------

def exp_scaling(n : int = 240, work_ms : float = 20.0, reps : int = 3) -> None:
    results = []
    for k in (1, 2, 4, 8):
        for rep in range(reps):
            tag = f'e2_k{k}_r{rep}'
            sink_file = raw_path(tag)
            producer = expnodes.StampedProducer(n = n, name = 'producer')
            worker = expnodes.WorkProcessor(work_ms = work_ms, nb_tasks = k, name = 'stage')(producer)
            sink = expnodes.RecordingConsumer(filepath = sink_file, name = 'sink')(worker)
            run = run_flow([sink], BATCH, timeout_s = 120)
            m = sink_metrics(read_records(sink_file), run['t_run_start'])
            results.append({'nb_tasks': k, 'rep': rep, 'n': n, 'work_ms': work_ms, **run, **m})
            print(f'  k={k} rep={rep}: delivered={m.get("delivered")} '
                  f'thr={m.get("throughput_msg_s", 0):.1f} msg/s wall={run["wall_s"]:.1f}s')
    save('e2_scaling', {'n': n, 'work_ms': work_ms, 'results': results})


# -- e3: per-hop overhead ---------------------------------------------------

def exp_depth(n : int = 250, fps : float = 50.0, reps : int = 3) -> None:
    results = []
    for depth in (1, 2, 4, 8):
        for rep in range(reps):
            tag = f'e3_d{depth}_r{rep}'
            sink_file = raw_path(tag)
            node = expnodes.StampedProducer(n = n, fps = fps, name = 'producer')
            for d in range(depth):
                node = expnodes.WorkProcessor(work_ms = 0, name = f'hop{d}')(node)
            sink = expnodes.RecordingConsumer(filepath = sink_file, name = 'sink')(node)
            run = run_flow([sink], BATCH, timeout_s = 120)
            m = sink_metrics(read_records(sink_file), run['t_run_start'])
            results.append({'depth': depth, 'rep': rep, 'n': n, 'fps': fps, **run, **m})
            print(f'  depth={depth} rep={rep}: delivered={m.get("delivered")} '
                  f'lat_med={m.get("lat_ms_median", 0):.1f}ms p95={m.get("lat_ms_p95", 0):.1f}ms')
    save('e3_depth', {'n': n, 'fps': fps, 'results': results})


# -- e4: BATCH vs REALTIME under overload -----------------------------------

def exp_overload(n : int = 300, fps : float = 100.0, work_ms : float = 25.0, reps : int = 3) -> None:
    results = []
    for flow_type in (BATCH, REALTIME):
        for rep in range(reps):
            tag = f'e4_{flow_type}_r{rep}'
            sink_file = raw_path(tag)
            producer = expnodes.StampedProducer(n = n, fps = fps, name = 'producer')
            worker = expnodes.WorkProcessor(work_ms = work_ms, name = 'stage')(producer)
            sink = expnodes.RecordingConsumer(filepath = sink_file, name = 'sink')(worker)
            run = run_flow([sink], flow_type, timeout_s = 90)
            records = read_records(sink_file)
            m = sink_metrics(records, run['t_run_start'])
            entry = {'flow_type': flow_type, 'rep': rep, 'n': n, 'fps': fps,
                     'work_ms': work_ms, **run, **m}
            if rep == 0:    # per-message latency series for the paper's figure
                entry['series'] = [
                    {'i': r['i'], 'lat_ms': (r['t1'] - r['t0']) * 1000}
                    for r in records if 'i' in r and 't0' in r
                ]
            results.append(entry)
            print(f'  {flow_type} rep={rep}: delivered={m.get("delivered")}/{n} '
                  f'lat_med={m.get("lat_ms_median", 0):.0f}ms max={m.get("lat_ms_max", 0):.0f}ms '
                  f'wall={run["wall_s"]:.1f}s timed_out={run["timed_out"]}')
    save('e4_overload', {'n': n, 'fps': fps, 'work_ms': work_ms, 'results': results})


# -- e5: partitioned join scaling -------------------------------------------

def exp_join_scaling(n : int = 240, branch_ms : float = 5.0, join_ms : float = 20.0,
                    reps : int = 3) -> None:
    results = []
    for k in (1, 2, 4):
        for rep in range(reps):
            tag = f'e5_k{k}_r{rep}'
            sink_file = raw_path(tag)
            producer = expnodes.StampedProducer(n = n, name = 'producer')
            left = expnodes.WorkProcessor(work_ms = branch_ms, name = 'left')(producer)
            right = expnodes.WorkProcessor(work_ms = branch_ms, name = 'right')(producer)
            kwargs = {'partition_by': 'trace_id'} if k > 1 else {}
            joiner = expnodes.PairJoiner(work_ms = join_ms, nb_tasks = k,
                                        name = 'joiner', **kwargs)(left, right)
            sink = expnodes.RecordingConsumer(filepath = sink_file, name = 'sink')(joiner)
            run = run_flow([sink], BATCH, timeout_s = 150)
            records = read_records(sink_file)
            m = sink_metrics(records, run['t_run_start'])
            matched = sum(1 for r in records if r.get('b_i') == r.get('i') and r.get('i') is not None)
            results.append({'nb_tasks': k, 'rep': rep, 'n': n, 'matched': matched, **run, **m})
            print(f'  k={k} rep={rep}: delivered={m.get("delivered")} matched={matched}/{n} '
                  f'thr={m.get("throughput_msg_s", 0):.1f} msg/s')
    save('e5_join_scaling', {'n': n, 'branch_ms': branch_ms, 'join_ms': join_ms,
                             'results': results})


# -- e6: time-aligned quorum join -------------------------------------------

def exp_time_join(n : int = 150, fps : float = 50.0, drop_every : int = 3,
                reps : int = 3) -> None:
    results = []
    for rep in range(reps):
        tag = f'e6_r{rep}'
        sink_file = raw_path(tag)
        cam_a = expnodes.StampedProducer(n = n, fps = fps, name = 'cam_a')
        cam_b = expnodes.StampedProducer(n = n, fps = fps, drop_every = drop_every, name = 'cam_b')
        policy = JoinPolicy(mode = 'time', tolerance_ms = 12, timeout_seconds = 0.25, quorum = 1)
        fuse = expnodes.PairJoiner(work_ms = 0, name = 'fuse', join_policy = policy)(cam_a, cam_b)
        sink = expnodes.RecordingConsumer(filepath = sink_file, name = 'sink')(fuse)
        run = run_flow([sink], REALTIME, timeout_s = 60)
        records = read_records(sink_file)
        both = sum(1 for r in records if r.get('has_a') and r.get('has_b'))
        only_a = sum(1 for r in records if r.get('has_a') and not r.get('has_b'))
        only_b = sum(1 for r in records if r.get('has_b') and not r.get('has_a'))
        results.append({'rep': rep, 'n': n, 'drop_every': drop_every,
                        'groups': len(records), 'both': both, 'only_a': only_a,
                        'only_b': only_b, **run})
        print(f'  rep={rep}: groups={len(records)} both={both} only_a={only_a} '
              f'only_b={only_b} wall={run["wall_s"]:.1f}s')
    save('e6_time_join', {'n': n, 'fps': fps, 'drop_every': drop_every, 'results': results})


# -- e7: payload size sweep — when is the blob store needed? -----------------

#: Payload sizes for the E7 sweep. 1000 KB vs 1 MiB brackets the NATS
#: max_payload knife edge: the envelope adds ~300 B of routing/tracing fields,
#: so a 1 MiB payload overflows the broker's 1 MiB message cap while 1000 KB
#: still fits.
E7_SIZES = (1024, 16384, 65536, 262144, 524288, 786432,
            1024000, 1048576, 2097152, 6291456)

REDIS_URL = 'redis://localhost:6379/0'


def exp_payload_sweep() -> None:
    '''
    Producer → sink with padded payloads of increasing size, under three
    payload-placement policies:

    - ``inline``: no blob store, spill threshold raised out of the way — the
      payload rides inside the broker message until the broker's 1 MiB
      ``max_payload`` rejects it (the breaking point).
    - ``blob``: spill threshold forced to ~zero — every payload takes the Redis
      round trip, measuring the blob path's overhead at every size.
    - ``auto``: the shipped default — blob store configured, 512 KiB threshold.
    '''
    modes = (
        ('inline', {'engine_kwargs': {},
                    'env': {'VIDEOFLOW_MAX_INLINE_PAYLOAD_BYTES': str(64 * 1024 * 1024)}}),
        ('blob', {'engine_kwargs': {'blob_redis_url': REDIS_URL},
                  'env': {'VIDEOFLOW_MAX_INLINE_PAYLOAD_BYTES': '1'}}),
        ('auto', {'engine_kwargs': {'blob_redis_url': REDIS_URL}, 'env': {}}),
    )
    results = []
    for mode, cfg in modes:
        for size in E7_SIZES:
            n = 150 if size <= 262144 else (60 if size <= 1048576 else 30)
            tag = f'e7_{mode}_{size}'
            sink_file = raw_path(tag)
            producer = expnodes.StampedProducer(n = n, payload_bytes = size, name = 'producer')
            sink = expnodes.RecordingConsumer(filepath = sink_file, name = 'sink')(producer)
            run = run_flow([sink], BATCH, timeout_s = 30, **cfg)
            records = read_records(sink_file)
            m = sink_metrics(records, run['t_run_start'])
            span = m.get('sink_makespan_s', 0)
            mb_s = (m['delivered'] * size / span / 1e6) if span else 0.0
            results.append({'mode': mode, 'payload_bytes': size, 'n': n,
                            'throughput_mb_s': mb_s, **run, **m})
            print(f'  {mode} {size / 1024:.0f}KB: delivered={m.get("delivered")}/{n} '
                  f'lat_med={m.get("lat_ms_median", 0):.1f}ms {mb_s:.0f}MB/s '
                  f'failures={run["failures"]} timed_out={run["timed_out"]}')
    save('e7_payload_sweep', {'sizes': list(E7_SIZES), 'results': results})


# -- e8: fault injection — SIGKILL a replica mid-flow ------------------------

def exp_fault_injection(n : int = 240, work_ms : float = 20.0, kill_after_s : float = 2.5,
                        reps : int = 2) -> None:
    '''
    The E2 graph with two stage replicas, except one replica is SIGKILLed
    mid-run. BATCH semantics promise zero loss (unacked messages redeliver to
    the survivor after ack_wait) and no duplicates at the sink (content-derived
    publish ids dedup the crash-window republish).
    '''
    results = []
    for rep in range(reps):
        tag = f'e8_r{rep}'
        sink_file = raw_path(tag)
        producer = expnodes.StampedProducer(n = n, name = 'producer')
        stage = expnodes.WorkProcessor(work_ms = work_ms, nb_tasks = 2, name = 'stage')(producer)
        sink = expnodes.RecordingConsumer(filepath = sink_file, name = 'sink')(stage)
        run = run_flow([sink], BATCH, timeout_s = 150, kill = ('stage', 0, kill_after_s))
        records = read_records(sink_file)
        m = sink_metrics(records, run['t_run_start'])
        seen = [r['i'] for r in records if 'i' in r]
        unique = len(set(seen))
        results.append({'rep': rep, 'n': n, 'unique': unique,
                        'duplicates': len(seen) - unique, **run, **m})
        print(f'  rep={rep}: delivered={m.get("delivered")}/{n} unique={unique} '
              f'dups={len(seen) - unique} wall={run["wall_s"]:.1f}s '
              f'killed={run["killed"]} timed_out={run["timed_out"]}')
    save('e8_fault_injection', {'n': n, 'work_ms': work_ms,
                                'kill_after_s': kill_after_s, 'results': results})


EXPERIMENTS = {
    'e1': exp_serialization,
    'e2': exp_scaling,
    'e3': exp_depth,
    'e4': exp_overload,
    'e5': exp_join_scaling,
    'e6': exp_time_join,
    'e7': exp_payload_sweep,
    'e8': exp_fault_injection,
}

if __name__ == '__main__':
    wanted = sys.argv[1:] or list(EXPERIMENTS)
    unknown = [w for w in wanted if w not in EXPERIMENTS]
    if unknown:
        raise SystemExit(f'unknown experiment(s) {unknown}; known: {list(EXPERIMENTS)}')
    for key in wanted:
        print(f'== running {key}')
        t0 = time.time()
        EXPERIMENTS[key]()
        print(f'== {key} done in {time.time() - t0:.1f}s')
