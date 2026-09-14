'''
The gate and shared fixtures for the Kubernetes bucket.

Everything here runs whole flows on a real cluster: one Job or Deployment per node,
workers reaching the broker at its in-cluster DNS name, artifacts travelling back
out through a shared work root. That needs a cluster, images it can run, a broker,
and a work root that round-trips — so this conftest checks all of it up front and
skips the bucket with the reason when something is missing.

**It gates; it never provisions.** ``scripts/k3s-test-up.sh`` (or, for a kind
cluster, ``scripts/kind-up.sh``) prepares the namespace, builds and publishes the
images, installs the broker and verifies the work root. A test that quietly built
a cluster would take fifteen minutes to fail the first time something was wrong
with it, and a test that switched the kubectl context would deploy into whatever
cluster the operator was actually pointed at. Both are worse than a skip that
names the fix. The k3s cluster in particular is shared infrastructure: the gate
only ever *reads* it, and the flavor's own ``support_k8s.deploy_extra_args`` makes
every pod the tests create carry ``priorityClassName: cluster-batch``.

Which cluster is expected is decided from the environment, never from whatever
kubectl happens to point at: ``VF_KIND_CLUSTER`` set means context
``kind-<name>``, otherwise ``VF_K8S_CONTEXT`` (default ``default``, a k3s
install's name). The current context has to *equal* it; a mismatch is a skip that
says which cluster it would have used.

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
import urllib.error
import urllib.request
import uuid
from typing import List, Optional
from urllib.parse import urlparse

import pytest

# Same directory as this conftest, which pytest puts on sys.path before loading it.
from support_k8s import (
    BASE_IMAGE,
    CONTEXT,
    FIXTURE_IMAGE,
    FLAVOR,
    KIND,
    KIND_CLUSTER,
    NAMESPACE,
    NATS_URL,
    PVC,
    REGISTRY,
    SOLUTION_IMAGES,
    deploy_extra_args,
    pvc_host_path,
    work_root,
)

from videoflow.deploy.cluster import detect_cluster

FIX = 'run ./scripts/kind-up.sh' if FLAVOR == KIND else 'run ./scripts/k3s-test-up.sh'
IMAGES = (BASE_IMAGE, FIXTURE_IMAGE, *SOLUTION_IMAGES.values())

# The manifest media types a registry may answer with for a tag pushed by crane
# (docker-style tarballs) or by docker itself.
_MANIFEST_TYPES = ('application/vnd.docker.distribution.manifest.v2+json, '
                   'application/vnd.docker.distribution.manifest.list.v2+json, '
                   'application/vnd.oci.image.manifest.v1+json, '
                   'application/vnd.oci.image.index.v1+json')

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

def _in_registry_over_http(image : str) -> bool:
    '''
    ``HEAD /v2/<repo>/manifests/<tag>`` against the registry, over plain HTTP and
    past any proxy — the read-only check for a registry that speaks no TLS, when
    crane is not installed to ask for us.
    '''
    registry, _, rest = image.partition('/')
    repo, _, tag = rest.rpartition(':')
    request = urllib.request.Request(f'http://{registry}/v2/{repo}/manifests/{tag or "latest"}',
                                     method = 'HEAD', headers = {'Accept': _MANIFEST_TYPES})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(request, timeout = 5) as response:
            return response.status == 200
    except (urllib.error.URLError, OSError, ValueError):
        return False

def _missing_images() -> List[str]:
    '''
    The images the bucket runs that are not where its nodes will look: docker on
    kind (they are side-loaded from there), the registry on k3s (checked with
    ``crane manifest --insecure`` when crane is on PATH, else through the
    registry's own v2 API).
    '''
    if not REGISTRY:
        return [image for image in IMAGES
                if _run('docker', 'image', 'inspect', image).returncode != 0]
    crane = shutil.which('crane')
    missing = []
    for image in IMAGES:
        if crane:
            present = _run(crane, 'manifest', '--insecure', image).returncode == 0
        else:
            present = _in_registry_over_http(image)
        if not present:
            missing.append(image)
    return missing

def _work_root_problem() -> Optional[str]:
    '''
    Why the work root would not round-trip, or None. The work root must resolve to
    the same absolute path on the host and inside the pods, or every artifact
    assertion fails much later with a missing file instead of here with the cause.
    '''
    root = work_root()
    if FLAVOR == KIND:
        assert root is not None
        if not root.is_dir():
            return f'{root} does not exist; {FIX}'
        sentinel = root / f'.vf-probe-{uuid.uuid4().hex[:8]}'
        sentinel.write_text('ok')
        try:
            seen = _run('docker', 'exec', f'{KIND_CLUSTER}-control-plane', 'cat', str(sentinel))
        finally:
            sentinel.unlink(missing_ok = True)
        if seen.stdout.strip() != 'ok':
            return (f'{root} is not visible inside the kind node at the same path, so '
                    f'solution artifacts cannot travel back to the host; recreate the '
                    f'cluster (./scripts/kind-down.sh && ./scripts/kind-up.sh)')
        return None
    host = pvc_host_path()
    if host is None:
        return (f'claim {PVC} in namespace {NAMESPACE} is not Bound to an NFS volume '
                f'(kubectl get pvc -n {NAMESPACE} {PVC}); {FIX}')
    if root is None or not root.is_dir():
        return (f'{root or host} does not exist on this host — the claim is served over NFS '
                f'and the tests read its backing directory directly, so run them on the NFS '
                f'server (or mount the export at the same path); {FIX}')
    if not root.is_relative_to(host):
        return (f'VF_K8S_WORK_ROOT={root} is outside the claim {PVC} ({host}); the pods '
                f'only see the claim, so artifacts written there would never reach the host. '
                f'Unset it, or point it inside {host}')
    if not os.access(root, os.W_OK):
        return f'{root} is not writable by this user; {FIX}'
    return None

@functools.lru_cache(maxsize = 1)
def cluster_ready() -> Optional[str]:
    '''
    ``None`` when the bucket can run, otherwise the reason it cannot.

    Ordered cheapest-first and short-circuiting, so the common case (no cluster at
    all) costs a couple of PATH lookups rather than a kubectl timeout.
    '''
    binaries = ('kubectl', 'docker', 'kind') if FLAVOR == KIND else ('kubectl', 'docker')
    for binary in binaries:
        if shutil.which(binary) is None:
            return f'{binary} is not on PATH; {FIX}'

    kubeconfig = os.environ.get('KUBECONFIG') or os.path.expanduser('~/.kube/config')
    if not os.path.isfile(kubeconfig.split(os.pathsep)[0]):
        return f'no kubeconfig at {kubeconfig}; {FIX}'

    context = _run('kubectl', 'config', 'current-context')
    name = context.stdout.strip()
    if context.returncode != 0 or not name:
        return f'no current kubectl context; {FIX}'
    if name != CONTEXT:
        # Deliberately not switched for you: which cluster kubectl points at is the
        # operator's call, and silently retargeting it would deploy test workloads
        # into whatever they were actually working on.
        hint = (f'export VF_KIND_CLUSTER={name.removeprefix("kind-")}' if name.startswith('kind-')
                else f'export VF_K8S_CONTEXT={name}')
        return (f"current kubectl context is '{name}', not '{CONTEXT}' ({FLAVOR}); either "
                f'`kubectl config use-context {CONTEXT}` yourself or, if that is the cluster '
                f'you mean, {hint}')

    if _run('kubectl', 'cluster-info', '--request-timeout=5s').returncode != 0:
        return f"context '{name}' is not reachable; {FIX}"

    flavor = detect_cluster()
    if flavor != FLAVOR:
        return (f"the cluster at context '{name}' is '{flavor}', not '{FLAVOR}'; "
                f'set VF_KIND_CLUSTER for a kind cluster, VF_K8S_CONTEXT for a k3s one')

    if _run('kubectl', 'get', 'namespace', NAMESPACE).returncode != 0:
        return f'namespace {NAMESPACE} does not exist; {FIX}'

    missing = _missing_images()
    if missing:
        where = f'registry {REGISTRY}' if REGISTRY else 'docker'
        return f'images not in {where} ({", ".join(missing)}); {FIX}'

    if not _nats_listening():
        return (f'nothing is listening at {NATS_URL} — the in-cluster broker is not '
                f'published on the host; {FIX}')

    return _work_root_problem()

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
    '''The namespace the up script provisioned, holding the shared broker.'''
    return NAMESPACE

@pytest.fixture(scope = 'session')
def k8s_nats_url() -> str:
    '''The broker as reached *from the host*, through the NodePort.

    Workers use the in-cluster name instead; the two are the same server. This one
    exists so a test can read the dead-letter queue and clean up streams.
    '''
    return NATS_URL

@pytest.fixture(scope = 'session')
def k8s_deploy_args() -> List[str]:
    '''
    The flags a hand-built ``videoflow deploy`` argv needs on this flavor (the work
    claim, the priority class, the pull policy); ``deploy_solution`` adds them
    itself. Empty on kind.
    '''
    return deploy_extra_args()

@pytest.fixture(scope = 'session')
def k8s_work_root() -> pathlib.Path:
    '''
    A per-session directory under the shared work root: the bind-mounted directory
    on kind, the claim's backing directory on the NFS server on k3s
    (``/opt/data/cluster-share/<pv subdir>/run-<hex>``).

    Not pytest's ``tmp_path``: that lives wherever TMPDIR points, which is neither
    bind-mounted into a kind node nor on the claim, so a mount of it would resolve
    inside the node's own filesystem and silently swallow every artifact.
    '''
    base = work_root()
    assert base is not None, cluster_ready()
    root = base / f'run-{uuid.uuid4().hex[:8]}'
    root.mkdir(parents = True)
    yield root
    # Best-effort: a pod running as root may have left files behind (the NFS export
    # is no_root_squash), and losing the artifacts of a failed run to a cleanup
    # error would be a poor trade.
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
