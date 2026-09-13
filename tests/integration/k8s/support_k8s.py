'''
Helpers for driving `videoflow deploy` against the test cluster and reading back
what it left in the cluster and in the broker.

Three things live here rather than in ``../support_broker.py``. The CLI wrapper is
specific to the deploy path (a subprocess, because all four toy solutions ship a
``common.py`` and importing two into one interpreter collides). The kubectl helpers
have no local counterpart at all. And ``dlq_entries`` takes the broker URL as an
argument, where ``support_broker.read_dlq`` binds it at import time to
``VF_TEST_NATS_URL`` — the *local* broker, which is a different server from the one
inside the cluster.

Two cluster flavors share the bucket, and the facts that differ between them are
resolved once, here, from the environment:

  - **kind** (``VF_KIND_CLUSTER`` set; ``scripts/kind-up.sh``): images are
    side-loaded into the node, the work root is a directory bind-mounted into
    the node at the same path, the broker is published on ``127.0.0.1:4223``.
  - **k3s** (the default; ``scripts/k3s-test-up.sh``): images are pulled from a
    registry, the work root is the backing directory of an RWX claim served over
    NFS — reached in the pods through ``--mount-pvc`` — and the broker is a
    NodePort on this host, which is itself a cluster node. Every pod a deploy
    creates carries ``priorityClassName: cluster-batch`` so it yields to other
    tenants' training work.

``deploy_solution`` adds the flavor's flags itself (``deploy_extra_args``), so the
test modules stay identical between the two — which is the point of the bucket.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import functools
import json
import os
import pathlib
import subprocess
import sys
import uuid
from typing import Any, Dict, List, Optional

from videoflow.deploy.manifests import Mount, parse_mounts, parse_pvc_mounts
from videoflow.messaging import topology

# Bounds a hang, not the expected runtime. A BATCH solution is dominated by pod
# scheduling and the provisioner's 2s poll, not by the flow (~10s of actual work).
DEPLOY_TIMEOUT_SECONDS = 600

# -- which cluster, resolved from the environment --------------------------------

KIND = 'kind'
K3S = 'k3s'

#: Set to use a kind cluster (the context is then ``kind-<name>``); unset means the
#: k3s path, whose context is ``VF_K8S_CONTEXT``. Never switched by the tests.
KIND_CLUSTER = os.environ.get('VF_KIND_CLUSTER')
FLAVOR = KIND if KIND_CLUSTER else K3S
CONTEXT = f'kind-{KIND_CLUSTER}' if KIND_CLUSTER else os.environ.get('VF_K8S_CONTEXT', 'default')
NAMESPACE = os.environ.get('VF_K8S_NAMESPACE', 'videoflow-test')
#: The RWX claim the k3s work root lives on (k8s/test-pvc.yaml).
PVC = os.environ.get('VF_K8S_PVC', 'vf-test-share')
#: This project's dev cluster registry, plain HTTP, reachable from every node.
#: Only a default for the k3s path; kind side-loads and names no registry.
DEFAULT_K3S_REGISTRY = '10.128.81.10:5000'
REGISTRY = os.environ.get('VF_K8S_IMAGE_REGISTRY') or (DEFAULT_K3S_REGISTRY if FLAVOR == K3S else None)
#: The broker as reached from the host: kind's extraPortMapping, or the NodePort
#: k3s-test-up.sh applies (this host is a node, so 127.0.0.1 answers).
NATS_URL = os.environ.get('VF_K8S_NATS_URL',
                          'nats://127.0.0.1:4223' if FLAVOR == KIND else 'nats://127.0.0.1:30422')
#: PriorityClass every k3s test pod carries; empty disables it. Never
#: ``cluster-training`` — that one preempts other tenants.
PRIORITY_CLASS = os.environ.get('VF_K8S_PRIORITY_CLASS', 'cluster-batch' if FLAVOR == K3S else '')
KIND_WORK_ROOT = '/tmp/videoflow-k8s'

def image_ref(name : str) -> str:
    '''``name`` qualified with the registry when one is in use, else as built.'''
    return f'{REGISTRY}/{name}' if REGISTRY else name

# What workers run, all built by the up script (side-loaded on kind, pushed to the
# registry on k3s). The base image carries a flow of built-in nodes; the solution
# images are `videoflow-<dirname>:latest`, the same string deploy.build.default_tag
# computes, so a hand-run `videoflow deploy` with no --image would build and find
# these — under the registry prefix when one is in use.
BASE_IMAGE = image_ref('videoflow-base:py3.12')
SOLUTION_IMAGES = {name: image_ref(f'videoflow-{name}:latest')
                   for name in ('toy_calculator', 'toy_router', 'toy_recovery', 'toy_fusion')}
# videoflow-base plus fixture_nodes.py — see the Dockerfile next to this file.
FIXTURE_IMAGE = image_ref('videoflow-k8s-fixtures:latest')

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

def kubectl_object(*args : str) -> Optional[Dict[str, Any]]:
    '''One object from ``kubectl get ... -o json``, or None when it does not exist.'''
    proc = kubectl(*args, '-o', 'json')
    if proc.returncode != 0 or not proc.stdout.strip():
        return None
    return json.loads(proc.stdout)

@functools.lru_cache(maxsize = 1)
def pvc_host_path() -> Optional[str]:
    '''
    Where the k3s work claim's data lives on the NFS server: ``<share>/<subdir>``
    from the bound PersistentVolume's ``spec.csi.volumeAttributes`` (csi-driver-nfs
    records the export and the per-claim subdirectory there). None when the
    claim is missing, not Bound, or not an NFS volume. Cached: it is asked for by
    the gate, the fixtures and every deploy.
    '''
    claim = kubectl_object('get', 'pvc', PVC, '-n', NAMESPACE)
    if claim is None or claim.get('status', {}).get('phase') != 'Bound':
        return None
    volume = kubectl_object('get', 'pv', claim['spec']['volumeName'])
    attributes = (volume or {}).get('spec', {}).get('csi', {}).get('volumeAttributes', {})
    share, subdir = attributes.get('share'), attributes.get('subdir')
    if not share or not subdir:
        return None
    return os.path.join(share.rstrip('/'), subdir)

def work_root() -> Optional[pathlib.Path]:
    '''
    The directory tests stage solutions in and read artifacts from — the one the
    pods see at the same absolute path. ``VF_K8S_WORK_ROOT`` overrides; on kind it
    defaults to the bind-mounted ``/tmp/videoflow-k8s``, on k3s it is derived from
    the claim (``pvc_host_path``) and is None until the claim is Bound.
    '''
    override = os.environ.get('VF_K8S_WORK_ROOT')
    if FLAVOR == KIND:
        return pathlib.Path(override or KIND_WORK_ROOT)
    if override:
        return pathlib.Path(override)
    host = pvc_host_path()
    return pathlib.Path(host) if host else None

def work_mounts(path : str) -> List[Mount]:
    '''
    The mounts a workload needs to reach ``path`` under the work root: the hostPath
    alone on kind; on k3s the claim as well, which then serves the path in the pods
    (``manifests.pod_mounts`` drops the shadowed hostPath there and keeps it for the
    prep container on this host).
    '''
    mounts = parse_mounts([path])
    if FLAVOR == K3S:
        host = pvc_host_path()
        assert host, f'claim {PVC} is not Bound in {NAMESPACE}; run ./scripts/k3s-test-up.sh'
        mounts += parse_pvc_mounts([f'{PVC}:{host}'])
    return mounts

def deploy_extra_args() -> List[str]:
    '''
    The ``videoflow deploy`` flags the flavor needs and a test should not have to
    know about: on k3s the work claim, the priority class and — because the images
    come from a registry under mutable tags — ``--image-pull-policy Always``, so a
    node that cached last week's ``:latest`` pulls the one just pushed. Empty on kind.
    '''
    if FLAVOR != K3S:
        return []
    host = pvc_host_path()
    assert host, f'claim {PVC} is not Bound in {NAMESPACE}; run ./scripts/k3s-test-up.sh'
    args = ['--mount-pvc', f'{PVC}:{host}']
    if PRIORITY_CLASS:
        args += ['--priority-class', PRIORITY_CLASS]
    if REGISTRY:
        args += ['--image-pull-policy', 'Always']
    return args

# -- driving the CLI ----------------------------------------------------------

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
    finds the Services the up script already created, reports nothing created and
    owns nothing — so no test tears the broker out from under the next one — and
    derives the in-cluster URL the workers need. Passing the URL explicitly would
    skip that path entirely, which is the path a real deploy takes.

    ``--image`` with ``--no-build`` rather than letting deploy autobuild: the build
    context is resolved with ``git rev-parse`` from the graph directory, and a staged
    copy under the work root is not a checkout, so an autobuild would fall back to
    building from the *repo's* files — proving nothing about the copy under test. The
    image still goes through resolve_image, the flavor's image load (a no-op for a
    registry-qualified ref the nodes pull themselves), and the prepare hook's
    `docker run`, and becomes the provision Job's image too (correct here: every
    solution image is FROM videoflow-base).

    The flavor's own flags (``deploy_extra_args``) go first, so ``extra`` can
    override them.
    '''
    argv = ['deploy', str(work / f'{name}.py'),
            '--namespace', namespace,
            '--flow-id', flow_id,
            '--run-id', run_id,
            '--image', image,
            '--no-build',
            '--non-interactive']
    return videoflow(argv + deploy_extra_args() + (extra or []), cwd = work)

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
