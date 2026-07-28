'''
The gate and shared fixtures for the Kubernetes bucket.

Everything here runs whole flows on a real cluster: one Job or Deployment per node,
workers reaching the broker at its in-cluster DNS name, artifacts travelling back
out through a hostPath mount. That needs a cluster, images inside it, a broker, and
a work root that round-trips — so this conftest checks all of it up front and skips
the bucket with the reason when something is missing.

**It gates; it never provisions.** ``scripts/kind-up.sh`` creates the cluster,
builds and side-loads the images, installs the broker and verifies the work root.
A test that quietly built a cluster would take fifteen minutes to fail the first
time something was wrong with it, and a test that switched the kubectl context
would deploy into whatever cluster the operator was actually pointed at. Both are
worse than a skip that names the fix.

The NATS probe is a raw TCP connect for the same reason the parent conftest's is:
nats-py retries a refused connection internally, so a real handshake against a
dead port costs about two minutes regardless of ``connect_timeout``.
'''
from __future__ import absolute_import, division, print_function

import functools
import os
import pathlib
import shutil
import socket
import subprocess
import uuid
from typing import Optional
from urllib.parse import urlparse

import pytest

# Same directory as this conftest, which pytest puts on sys.path before loading it.
from support_k8s import BASE_IMAGE, FIXTURE_IMAGE, SOLUTION_IMAGES

CLUSTER = os.environ.get('VF_KIND_CLUSTER', 'videoflow')
NAMESPACE = os.environ.get('VF_K8S_NAMESPACE', 'videoflow-test')
WORK_ROOT = pathlib.Path(os.environ.get('VF_K8S_WORK_ROOT', '/tmp/videoflow-k8s'))
NATS_URL = os.environ.get('VF_K8S_NATS_URL', 'nats://127.0.0.1:4223')

FIX = 'run ./scripts/kind-up.sh'

def _run(*args : str, timeout : int = 30) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output = True, text = True, check = False,
                          timeout = timeout)

def _nats_listening(url : str = NATS_URL) -> bool:
    parsed = urlparse(url)
    try:
        with socket.create_connection((parsed.hostname or '127.0.0.1', parsed.port or 4222),
                                      timeout = 1):
            return True
    except OSError:
        return False

@functools.lru_cache(maxsize = 1)
def cluster_ready() -> Optional[str]:
    '''
    ``None`` when the bucket can run, otherwise the reason it cannot.

    Ordered cheapest-first and short-circuiting, so the common case (no cluster at
    all) costs a couple of PATH lookups rather than a kubectl timeout.
    '''
    for binary in ('kubectl', 'docker', 'kind'):
        if shutil.which(binary) is None:
            return f'{binary} is not on PATH; {FIX}'

    context = _run('kubectl', 'config', 'current-context')
    name = context.stdout.strip()
    if context.returncode != 0 or not name:
        return f'no current kubectl context; {FIX}'
    if not name.startswith('kind-'):
        # Deliberately not switched for you: which cluster kubectl points at is the
        # operator's call, and silently retargeting it would deploy test workloads
        # into whatever they were actually working on.
        return (f"current kubectl context is '{name}', not a kind cluster; "
                f'{FIX} and `kubectl config use-context kind-{CLUSTER}`')

    if _run('kubectl', 'cluster-info', '--request-timeout=5s').returncode != 0:
        return f"kind context '{name}' is not reachable; {FIX}"

    if _run('kubectl', 'get', 'namespace', NAMESPACE).returncode != 0:
        return f'namespace {NAMESPACE} does not exist; {FIX}'

    missing = [image for image in (BASE_IMAGE, FIXTURE_IMAGE, *SOLUTION_IMAGES.values())
               if _run('docker', 'image', 'inspect', image).returncode != 0]
    if missing:
        return f'images not built ({", ".join(missing)}); {FIX}'

    if not _nats_listening():
        return (f'nothing is listening at {NATS_URL} — the in-cluster broker is not '
                f'published on the host; {FIX}')

    # The work root must resolve to the same absolute path on the host and inside
    # the node, or every artifact assertion fails much later with a missing file
    # instead of here with the cause.
    if not WORK_ROOT.is_dir():
        return f'{WORK_ROOT} does not exist; {FIX}'
    sentinel = WORK_ROOT / f'.vf-probe-{uuid.uuid4().hex[:8]}'
    sentinel.write_text('ok')
    try:
        seen = _run('docker', 'exec', f'{CLUSTER}-control-plane', 'cat', str(sentinel))
    finally:
        sentinel.unlink(missing_ok = True)
    if seen.stdout.strip() != 'ok':
        return (f'{WORK_ROOT} is not visible inside the kind node at the same path, so '
                f'solution artifacts cannot travel back to the host; recreate the '
                f'cluster (./scripts/kind-down.sh && ./scripts/kind-up.sh)')
    return None

def pytest_collection_modifyitems(config, items):
    reason = cluster_ready()
    if reason is None:
        return
    here = pathlib.Path(__file__).parent
    skip = pytest.mark.skip(reason = reason)
    for item in items:
        if here in pathlib.Path(str(item.fspath)).parents:
            item.add_marker(skip)

# -- fixtures ----------------------------------------------------------------

@pytest.fixture(scope = 'session')
def k8s_namespace() -> str:
    '''The namespace kind-up.sh provisioned, holding the shared broker.'''
    return NAMESPACE

@pytest.fixture(scope = 'session')
def k8s_nats_url() -> str:
    '''The broker as reached *from the host*, through the NodePort.

    Workers use the in-cluster name instead; the two are the same server. This one
    exists so a test can read the dead-letter queue and clean up streams.
    '''
    return NATS_URL

@pytest.fixture(scope = 'session')
def k8s_work_root() -> pathlib.Path:
    '''
    A per-session directory under the bind-mounted work root.

    Not pytest's ``tmp_path``: that lives wherever TMPDIR points, which is not the
    path kind bind-mounts into the node, and a hostPath mount of it would resolve
    inside the node's own filesystem and silently swallow every artifact.
    '''
    root = WORK_ROOT / f'run-{uuid.uuid4().hex[:8]}'
    root.mkdir(parents = True)
    yield root
    # Best-effort: a pod running as root may have left files behind, and losing the
    # artifacts of a failed run to a cleanup error would be a poor trade.
    shutil.rmtree(root, ignore_errors = True)

@pytest.fixture
def flow_ids(request) -> tuple:
    '''
    A fresh ``(flow_id, run_id)`` per test.

    Resource names are flow-scoped, not run-scoped (``vf-<flow>-<node>``), so
    re-applying a leftover Job of the same name fails on immutable fields. A unique
    flow id per test also scopes the dead-letter queue, which deliberately outlives
    the run — without it, one test's dead letters would show up in the next one's.
    '''
    stem = request.node.name.replace('test_', '')[:20].replace('_', '-').strip('-')
    unique = uuid.uuid4().hex[:8]
    return f'{stem}-{unique}', unique
