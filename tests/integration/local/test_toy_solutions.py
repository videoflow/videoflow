'''
The four toy solutions under ``solutions/``, run locally.

These are the widest integration tests in the bucket. The other modules here
assemble a graph in-process and run it; these drive the whole *user* path
instead — ``videoflow run-local``, which generates nothing, runs the solution's
``prepare.py`` hook, loads the graph module from disk, compiles it, provisions
streams, spawns one worker subprocess per node replica, and waits for the flow
to drain. What they assert is the solutions' own self-checking artifacts, so a
pass means the distributed run computed the right answer, not merely that it
exited zero.

The configs and the assertions live in ``support_solutions.py``, shared verbatim
with ``k8s/test_k8s_solutions.py``, which runs these same four solutions as pods
on a kind cluster. That sharing is the point: the two engines are supposed to
produce identical answers, so anything asserted in only one of them is a claim
nobody is checking.

Two implementation details are load-bearing:

**Each run gets a copy of the solution in a tmpdir.** ``build_flow()`` is called
by ``load_flow`` with no arguments, so a solution always reads the
``config.yaml`` sitting next to its own module — ``--config`` only reaches the
prep hook. Copying the solution is therefore the only way to give a run its own
config and work_dir without writing into the repo, and it keeps the tests
independent of each other and of any config.yaml a developer left behind.

**Each solution runs in a subprocess, not in-process.** All four ship a
``common.py`` and the graph modules import their glue nodes as top-level modules
(``from toy_router_nodes import ...``) — the layout that lets a worker
reconstruct a node from its ``<module>.<Class>`` path. Importing two solutions
into one interpreter would collide on ``common``; a subprocess per solution is
both the isolation and the honest reproduction of how these actually run.

Skipped automatically when NATS is unreachable — see ../conftest.py.
'''
import os
import pathlib
import socket
import subprocess
import sys
from urllib.parse import urlparse

import pytest
from support_solutions import (
    SOLUTION_CONFIGS,
    assert_calculator_report,
    assert_fusion_latest,
    assert_recovery_report,
    assert_router_counts,
    read_artifact,
    stage_solution,
)

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')
REDIS_URL = os.environ.get('VF_TEST_REDIS_URL', 'redis://localhost:6379/0')

# A whole flow — provisioning, N worker subprocesses, drain and stream teardown.
# Generous because it bounds a hang, not the expected runtime (~5-10s each).
RUN_TIMEOUT_SECONDS = 300

def _redis_available(url = REDIS_URL) -> bool:
    '''
    True when something is listening on the Redis host/port.

    Only ``toy_router`` cares: its ledger consumer is built with
    ``idempotent=True``, which needs a store to deduplicate against. Without one
    the flow still runs correctly (redelivery just isn't deduplicated), so this
    gates an *extra* argument rather than the test — the solution must pass
    either way.
    '''
    parsed = urlparse(url)
    try:
        with socket.create_connection((parsed.hostname or 'localhost', parsed.port or 6379),
                                      timeout = 1):
            return True
    except OSError:
        return False

def run_solution(tmp_path : pathlib.Path, name : str,
                 blob_redis : bool = False) -> pathlib.Path:
    '''
    Stages solution ``name`` in ``tmp_path``, runs it to completion with
    ``videoflow run-local``, and returns the run's work_dir.

    - Arguments:
        - tmp_path: pytest's per-test temp directory; the copy and every \
            artifact live under it.
        - name: the directory name under ``solutions/``.
        - blob_redis: point the run at Redis for the blob/idempotency store \
            when one is reachable.

    - Returns:
        - the work_dir holding the solution's output artifacts.

    - Raises:
        - ``AssertionError`` with the captured output when the run fails.
    '''
    work = stage_solution(tmp_path, name, SOLUTION_CONFIGS[name])

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

def test_toy_calculator(tmp_path):
    '''
    BATCH diamond: fan-out, a trace join re-aligning two branches, competing
    replicas, stateful aggregation and a two-parent consumer.
    '''
    work_dir = run_solution(tmp_path, 'toy_calculator')
    assert_calculator_report(read_artifact(work_dir, 'report.json'))

def test_toy_router(tmp_path):
    '''
    Partitioned routing: an async ``process``, ``ctx.set_partition_key``, and a
    stateful counter replicated across three partitions.
    '''
    work_dir = run_solution(tmp_path, 'toy_router', blob_redis = True)
    assert_router_counts(read_artifact(work_dir, 'counts.json'))

def test_toy_recovery(tmp_path):
    '''
    Error handling end to end: a bad message and a sick worker, handled
    differently, in one run.
    '''
    work_dir = run_solution(tmp_path, 'toy_recovery')
    assert_recovery_report(read_artifact(work_dir, 'recovery_report.json'))

def test_toy_fusion(tmp_path):
    '''
    REALTIME fusion of independent producers by event time.

    ``duration_s`` bounds what is normally an unbounded flow so the run drains
    and writes its summary — which is why this bucket can assert the close()
    artifact and the k8s one cannot. The assertions stay above the timing noise a
    realtime path is allowed to have: that moments were fused, and that at least
    one saw every camera (the time join really did group them rather than always
    emitting at quorum).
    '''
    work_dir = run_solution(tmp_path, 'toy_fusion')
    summary = read_artifact(work_dir, 'fusion_summary.json')
    assert summary['moments'] > 0, summary
    assert summary['complete_moments'] > 0, summary

    assert_fusion_latest(read_artifact(work_dir, 'latest.json'))

if __name__ == "__main__":
    pytest.main([__file__])
