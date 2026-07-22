'''
End-to-end tests for the three toy solutions under ``solutions/``.

These are the widest integration tests in the suite. The other modules here
assemble a graph in-process and run it; these drive the whole *user* path
instead — ``videoflow run-local``, which generates nothing, runs the solution's
``prepare.py`` hook, loads the graph module from disk, compiles it, provisions
streams, spawns one worker subprocess per node replica, and waits for the flow
to drain. What they assert is the solutions' own self-checking artifacts, so a
pass means the distributed run computed the right answer, not merely that it
exited zero.

Two implementation details are load-bearing:

**Each run gets a copy of the solution in a tmpdir.** ``build_flow()`` is called
by ``load_flow`` with no arguments, so a solution always reads the
``config.yaml`` sitting next to its own module — ``--config`` only reaches the
prep hook. Copying the solution is therefore the only way to give a run its own
config and work_dir without writing into the repo, and it keeps the three tests
independent of each other and of any config.yaml a developer left behind.

**Each solution runs in a subprocess, not in-process.** All three ship a
``common.py`` and the graph modules import their glue nodes as top-level modules
(``from toy_router_nodes import ...``) — the layout that lets a worker
reconstruct a node from its ``<module>.<Class>`` path. Importing two solutions
into one interpreter would collide on ``common``; a subprocess per solution is
both the isolation and the honest reproduction of how these actually run.

The configs below are deliberately smaller and faster than the shipped
``config.example.yaml`` (short streams, high pacing): the point here is the
framework path, and a CI run should not spend a minute on arithmetic. Skipped
automatically when NATS is unreachable — see conftest.py.
'''
import json
import os
import pathlib
import shutil
import socket
import subprocess
import sys
from urllib.parse import urlparse

import pytest
import yaml

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')
REDIS_URL = os.environ.get('VF_TEST_REDIS_URL', 'redis://localhost:6379/0')

SOLUTIONS_DIR = pathlib.Path(__file__).resolve().parents[2] / 'solutions'

# A whole flow — provisioning, N worker subprocesses, drain and stream teardown.
# Generous because it bounds a hang, not the expected runtime (~5-10s each).
RUN_TIMEOUT_SECONDS = 300

def _redis_available(url = REDIS_URL) -> bool:
    '''
    True when something is listening on the Redis host/port.

    Only ``toy_router`` cares: its ledger consumer is built with
    ``idempotent=True``, which needs a store to deduplicate against. Without one
    the flow still runs correctly (redelivery just isn't deduplicated), so this
    gates an *extra* argument rather than the test — CI runs a NATS server but no
    Redis, and the solution must pass either way.
    '''
    parsed = urlparse(url)
    try:
        with socket.create_connection((parsed.hostname or 'localhost', parsed.port or 6379),
                                      timeout = 1):
            return True
    except OSError:
        return False

def run_solution(tmp_path : pathlib.Path, name : str, config : dict,
                 blob_redis : bool = False) -> pathlib.Path:
    '''
    Copies solution ``name`` into ``tmp_path``, writes ``config`` as its
    config.yaml, runs it to completion with ``videoflow run-local``, and returns
    the run's work_dir.

    - Arguments:
        - tmp_path: pytest's per-test temp directory; the copy and every \
            artifact live under it.
        - name: the directory name under ``solutions/``.
        - config: the solution's config.yaml as a dict.
        - blob_redis: point the run at Redis for the blob/idempotency store \
            when one is reachable.

    - Returns:
        - the work_dir holding the solution's output artifacts.

    - Raises:
        - ``AssertionError`` with the captured output when the run fails.
    '''
    source = SOLUTIONS_DIR / name
    assert source.is_dir(), f'solution not found: {source}'
    work = tmp_path / name
    work.mkdir()
    # Only the Python modules: the graph, its glue nodes, the config loader and
    # the prep hook. The Dockerfile and config templates play no part in a local
    # run, and copying config.template.yaml would let run-local try to generate a
    # config instead of using the one written below.
    for module in source.glob('*.py'):
        shutil.copy(module, work / module.name)
    (work / 'config.yaml').write_text(yaml.safe_dump(config, sort_keys = False))

    cmd = [sys.executable, '-m', 'videoflow.deploy.cli', 'run-local',
           str(work / f'{name}.py'),
           '--nats', NATS_URL,
           # Never prompt and never start docker containers: the broker is the
           # one this suite already probed for.
           '--non-interactive', '--no-infra']
    if blob_redis and _redis_available():
        cmd += ['--blob-redis-url', REDIS_URL]

    proc = subprocess.run(cmd, cwd = work, capture_output = True, text = True,
                          timeout = RUN_TIMEOUT_SECONDS)
    assert proc.returncode == 0, (
        f'`run-local {name}` exited {proc.returncode}\n'
        f'--- stdout ---\n{proc.stdout[-4000:]}\n'
        f'--- stderr ---\n{proc.stderr[-4000:]}')
    return work / 'out'

def read_artifact(work_dir : pathlib.Path, filename : str) -> dict:
    '''The named JSON artifact, failing with the directory listing when absent.'''
    path = work_dir / filename
    assert path.is_file(), (f'{filename} was not written; work_dir holds: '
                            f'{sorted(p.name for p in work_dir.iterdir()) if work_dir.is_dir() else "nothing"}')
    with open(path) as f:
        return json.load(f)

def test_toy_calculator(tmp_path):
    '''
    BATCH diamond: fan-out, a trace join re-aligning two branches, competing
    replicas, stateful aggregation and a two-parent consumer.

    ``matches_expected`` compares the flow's final statistics against the
    closed-form values prepare.py baked before the run, so it is true only if
    every integer crossed every edge exactly once and the join re-aligned all of
    them — the loss-free guarantee BATCH exists to provide.
    '''
    work_dir = run_solution(tmp_path, 'toy_calculator', {
        'work_dir': './out',
        'start_value': 1,
        'end_value': 40,
        'producer_fps': 200,
        'delay_fps': 150,
        'flow_type': 'batch',
        'square': {'workers': 2},
        'join': {'timeout_s': None, 'missing': 'wait', 'max_pending': 100000},
    })
    report = read_artifact(work_dir, 'report.json')
    assert report['matches_expected'] is True, report
    assert report['pairs_seen'] == 40, report
    assert report['final_stats']['count'] == 40, report

def test_toy_router(tmp_path):
    '''
    Partitioned routing: an async ``process``, ``ctx.set_partition_key``, and a
    stateful counter replicated across three partitions.

    Both assertions matter and they are different claims. ``matches_expected``
    says every event was counted exactly once; ``sticky`` says each sensor was
    owned by exactly one replica — the property that makes replicating a
    stateful node correct at all. A routing bug can easily preserve the totals
    while losing stickiness.
    '''
    work_dir = run_solution(tmp_path, 'toy_router', {
        'work_dir': './out',
        'seed': 7,
        'events': 120,
        'sensors': 6,
        'rate_fps': 400,
        'idempotent_sink': True,
        'enrich': {'threshold': 50.0, 'lookup_ms': 1.0},
        'counter': {'partitions': 3, 'partition_by': '_partition_key'},
    }, blob_redis = True)
    counts = read_artifact(work_dir, 'counts.json')
    assert counts['matches_expected'] is True, counts
    assert counts['sticky'] is True, counts
    assert sum(counts['totals'].values()) == 120, counts

def test_toy_fusion(tmp_path):
    '''
    REALTIME fusion of independent producers by event time.

    The cameras and the IMU share no lineage, so nothing here could be grouped
    by trace id — the moments exist only because each producer stamps an event
    timestamp and the join groups on it. ``duration_s`` bounds what is normally
    an unbounded flow so the run drains and writes its summary.

    The assertions stay above the timing noise a realtime path is allowed to
    have: that moments were fused, that at least one saw every camera (the time
    join really did group them rather than always emitting at quorum), and that
    the collect parent delivered samples.
    '''
    work_dir = run_solution(tmp_path, 'toy_fusion', {
        'work_dir': './out',
        'cameras': 2,
        'camera_fps': 10,
        'phase_step_ms': 3.0,
        'sensor_hz': 100,
        'duration_s': 3,
        'flow_type': 'realtime',
        'fusion': {'tolerance_ms': 15, 'timeout_s': 0.25, 'quorum': 1,
                   'sensor_window_ms': 40},
    })
    summary = read_artifact(work_dir, 'fusion_summary.json')
    assert summary['moments'] > 0, summary
    assert summary['complete_moments'] > 0, summary

    latest = read_artifact(work_dir, 'latest.json')
    assert latest['moment'] > 0, latest
    assert latest['sensor_samples'] > 0, latest

if __name__ == "__main__":
    pytest.main([__file__])
