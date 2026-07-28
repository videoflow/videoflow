'''
Helpers for driving `videoflow deploy` against the kind cluster and reading back
what it left in the cluster and in the broker.

Three things live here rather than in ``../support_broker.py``. The CLI wrapper is
specific to the deploy path (a subprocess, because all four toy solutions ship a
``common.py`` and importing two into one interpreter collides). The kubectl helpers
have no local counterpart at all. And ``dlq_entries`` takes the broker URL as an
argument, where ``support_broker.read_dlq`` binds it at import time to
``VF_TEST_NATS_URL`` — the *local* broker, which is a different server from the one
inside the cluster.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import json
import pathlib
import subprocess
import sys
import uuid
from typing import Any, Dict, List, Optional

from videoflow.messaging import topology

# Bounds a hang, not the expected runtime. A BATCH solution is dominated by pod
# scheduling and the provisioner's 2s poll, not by the flow (~10s of actual work).
DEPLOY_TIMEOUT_SECONDS = 600

# What workers run, all built and side-loaded by scripts/kind-up.sh. The base image
# carries a flow of built-in nodes; the solution images are
# `videoflow-<dirname>:latest`, the same string deploy.build.default_tag computes,
# so a hand-run `videoflow deploy` with no --image would build and find these.
BASE_IMAGE = 'videoflow-base:py3.12'
SOLUTION_IMAGES = {name: f'videoflow-{name}:latest'
                   for name in ('toy_calculator', 'toy_router', 'toy_recovery', 'toy_fusion')}
# videoflow-base plus fixture_nodes.py — see the Dockerfile next to this file.
FIXTURE_IMAGE = 'videoflow-k8s-fixtures:latest'

def videoflow(argv : List[str], cwd : Optional[pathlib.Path] = None,
              timeout : int = DEPLOY_TIMEOUT_SECONDS) -> subprocess.CompletedProcess:
    '''
    Runs the videoflow CLI in a subprocess and returns the completed process.

    Does not assert on the exit code: several tests here are *about* the exit code
    (4 for a flow whose nodes failed, 5 for one that stalled), so the caller
    decides. Use ``assert_ok`` for the common case.
    '''
    cmd = [sys.executable, '-m', 'videoflow.deploy.cli'] + argv
    return subprocess.run(cmd, cwd = str(cwd) if cwd else None,
                          capture_output = True, text = True, timeout = timeout)

def assert_ok(proc : subprocess.CompletedProcess, what : str) -> subprocess.CompletedProcess:
    '''Fails with the tail of both streams — a cluster failure is unreadable without them.'''
    assert proc.returncode == 0, (
        f'`videoflow {what}` exited {proc.returncode}\n'
        f'--- stdout ---\n{proc.stdout[-6000:]}\n'
        f'--- stderr ---\n{proc.stderr[-6000:]}')
    return proc

def deploy_solution(work : pathlib.Path, name : str, namespace : str,
                    flow_id : str, run_id : str, image : str,
                    extra : Optional[List[str]] = None) -> subprocess.CompletedProcess:
    '''
    The staged solution at ``work``, deployed to the cluster.

    ``--flow-id`` and ``--run-id`` are explicit because resource names are
    flow-scoped: a re-apply over a leftover Job of the same name fails on immutable
    fields, and ``toy_recovery`` hard-codes its own flow id in the graph module.

    ``--nats`` is deliberately *omitted*. Without it the deploy runs ``ensure_infra``,
    finds the Services kind-up.sh already created, reports nothing created and owns
    nothing — so no test tears the broker out from under the next one — and derives
    the in-cluster URL the workers need. Passing the URL explicitly would skip that
    path entirely, which is the path a real deploy takes.

    ``--image`` with ``--no-build`` rather than letting deploy autobuild: the build
    context is resolved with ``git rev-parse`` from the graph directory, and a staged
    copy under the work root is not a checkout, so an autobuild would fall back to
    building from the *repo's* files — proving nothing about the copy under test. The
    image still goes through resolve_image, `kind load docker-image`, and the prepare
    hook's `docker run`, and becomes the provision Job's image too (correct here: every
    solution image is FROM videoflow-base).
    '''
    argv = ['deploy', str(work / f'{name}.py'),
            '--namespace', namespace,
            '--flow-id', flow_id,
            '--run-id', run_id,
            '--image', image,
            '--no-build',
            '--non-interactive']
    return videoflow(argv + (extra or []), cwd = work)

def teardown(namespace : str, flow_id : str, run_id : str,
             nats_url : str) -> subprocess.CompletedProcess:
    '''
    Deletes a run's cluster resources and broker streams.

    Needed after a REALTIME deploy, which returns 0 once the rollout looks healthy
    and leaves everything running — there is nothing for it to wait for. A BATCH
    deploy tears itself down and does not need this.
    '''
    return videoflow(['teardown', '--flow-id', flow_id, '--run-id', run_id,
                      '--namespace', namespace, '--nats', nats_url], timeout = 120)

# -- reading the cluster ------------------------------------------------------

def kubectl(*args : str, timeout : int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(['kubectl', *args], capture_output = True, text = True,
                          check = False, timeout = timeout)

def kubectl_json(*args : str) -> Dict[str, Any]:
    '''``kubectl get ... -o json``, parsed. Returns an empty item list on failure.'''
    proc = kubectl(*args, '-o', 'json')
    if proc.returncode != 0 or not proc.stdout.strip():
        return {'items': []}
    return json.loads(proc.stdout)

def resources_for(namespace : str, flow_id : str) -> List[str]:
    '''
    ``<kind>/<name>`` for everything a flow left in the namespace.

    Reads the label every rendered object carries, which is also what
    ``manifests.delete_resources`` deletes by — so an empty list is the same
    statement teardown makes about itself, verified from outside.
    '''
    out = []
    for kind in ('jobs', 'deployments', 'statefulsets', 'configmaps', 'services',
                 'networkpolicies', 'poddisruptionbudgets'):
        got = kubectl_json('get', kind, '-n', namespace, '-l', f'videoflow.io/flow-id={flow_id}')
        out += [f"{item['kind']}/{item['metadata']['name']}" for item in got['items']]
    return sorted(out)

def wait_for(predicate, timeout : float = 60.0, interval : float = 1.0):
    '''Polls ``predicate`` until it returns something truthy; returns it, or None.'''
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return None

# -- reading the broker -------------------------------------------------------

def dlq_entries(nats_url : str, flow_id : str, run_id : Optional[str] = None,
                node : Optional[str] = None) -> List[Dict[str, Any]]:
    '''
    Every dead letter matching the filter, consumed.

    The dead-letter stream is flow-scoped rather than run-scoped and is the one
    thing ``delete_run_streams`` deliberately spares, so this still works after the
    deploy has torn its run down — which is exactly when the contents are wanted.
    '''
    import nats

    async def _go() -> list:
        nc = await nats.connect(nats_url)
        js = nc.jetstream()
        stream = topology.dlq_stream_name(flow_id)
        out : list = []
        try:
            info = await js.stream_info(stream)
        except Exception:
            await nc.drain()
            return out
        if info.state.messages:
            subject = topology.dlq_subject_filter(flow_id, run_id, node)
            sub = await js.pull_subscribe(subject, durable = f'dlqreader{uuid.uuid4().hex[:6]}',
                                          stream = stream)
            try:
                msgs = await sub.fetch(info.state.messages, timeout = 3)
            except Exception:
                msgs = []
            for m in msgs:
                out.append({'headers': dict(m.headers or {}), 'data': m.data,
                            'subject': m.subject})
                await m.ack()
        await nc.drain()
        return out

    return asyncio.run(_go())

def delete_dlq(nats_url : str, flow_id : str) -> None:
    '''Drops a flow's dead-letter stream; it outlives the run, so tests clean it up.'''
    import nats

    async def _go() -> None:
        nc = await nats.connect(nats_url)
        try:
            await nc.jetstream().delete_stream(topology.dlq_stream_name(flow_id))
        except Exception:
            pass
        await nc.drain()

    asyncio.run(_go())
