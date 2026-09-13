'''
The backend conformance suite: one test per case in ``catalog/test_catalog.json``.

What this conftest does, and why it is not the integration conftest:

- **Every case is always collected.** A case whose fixture is absent is reported as
  ``NOT_RUN`` through a skip whose reason carries the ``not_run:`` prefix (see
  ``_status.py``); it is never deselected, so the results file lists all 130 IDs
  with an honest status. The integration buckets skip whole directories; this
  suite skips per resource, because one case may need a broker, another a GPU,
  and a third nothing at all.
- **Levels are markers.** The catalog's primary level is applied as
  ``@pytest.mark.level('broker')``; this hook adds a derived ``level_broker``
  marker so ``-m level_broker`` selects by level (marker expressions cannot match
  marker arguments).
- **Outcomes are written out.** ``pytest_runtest_makereport`` maps each test to
  PASS / FAIL / NOT_RUN / UNSUPPORTED / INVALID_TEST and ``pytest_sessionfinish``
  writes ``run_results.json`` + ``run_manifest.json`` under ``VF_CONF_RESULTS_DIR``
  (default ``tests/conformance/_out``, gitignored).
- **Pending cases** carry ``@pytest.mark.pending('phase N')`` until their phase
  lands; they report ``NOT_RUN: pending phase N``. The lint in
  ``test_catalog_lint.py`` keeps the ID ↔ test mapping 1:1 regardless.
'''
from __future__ import absolute_import, division, print_function

import functools
import json
import os
import pathlib
import sys
from typing import Any, Dict, List, Optional

import pytest

HERE = pathlib.Path(__file__).parent
for _path in (HERE, HERE.parent):          # this directory, and tests/ for the shared support modules
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from _results import RunResults  # noqa: E402
from _status import FAIL, INVALID_TEST, NOT_RUN_PREFIX, PASS, InvalidTest, classify_skip, not_run  # noqa: E402

CATALOG_DIR = HERE / 'catalog'
LEVELS = ('model', 'process', 'broker', 'kubernetes', 'gpu', 'benchmark')
RESULTS_DIR = pathlib.Path(os.environ.get('VF_CONF_RESULTS_DIR', str(HERE / '_out')))
SEED = int(os.environ.get('VF_TEST_SEED', '0'))

@functools.lru_cache(maxsize = 1)
def load_catalog() -> Dict[str, Any]:
    with open(CATALOG_DIR / 'test_catalog.json') as f:
        return json.load(f)

@functools.lru_cache(maxsize = 1)
def cases_by_id() -> Dict[str, Dict[str, Any]]:
    return {case['id']: case for case in load_catalog()['cases']}

def case_id_of(item : pytest.Item) -> Optional[str]:
    marker = item.get_closest_marker('case')
    return marker.args[0] if marker and marker.args else None

def variant_of(item : pytest.Item) -> str:
    marker = item.get_closest_marker('variant')
    if marker and marker.args:
        return str(marker.args[0])
    callspec = getattr(item, 'callspec', None)
    if callspec is not None:
        return callspec.id
    # No variant marker and no parametrization: this is the case's primary test,
    # the one written at the catalog's primary level. Its status is the case's.
    return 'primary'

def pytest_configure(config : pytest.Config) -> None:
    for line in (
        'conformance: a backend conformance case from tests/conformance/catalog.',
        'case(id): the catalog case ID this test implements.',
        'level(name): the level this test runs at (model|process|broker|kubernetes|gpu|benchmark).',
        'variant(name): a named variant of a case (several tests may share one case ID).',
        'negative_control(of): a test that must detect the reviewed defect for case `of`.',
        'pending(phase): the case is written as a skeleton until the named phase lands.',
        'gpu_nodes: needs VF_K8S_GPU_NODES (in-cluster GPU pods).',
    ):
        config.addinivalue_line('markers', line)
    for level in LEVELS:
        config.addinivalue_line('markers', f'level_{level}: derived from level({level!r}).')
    config._vf_results = RunResults(sorted(cases_by_id()))  # type: ignore[attr-defined]

def pytest_collection_modifyitems(config : pytest.Config, items : List[pytest.Item]) -> None:
    for item in items:
        if HERE not in pathlib.Path(str(item.fspath)).parents:
            continue
        item.add_marker(pytest.mark.conformance)
        level = item.get_closest_marker('level')
        if level and level.args:
            item.add_marker(getattr(pytest.mark, f'level_{level.args[0]}'))
        pending = item.get_closest_marker('pending')
        if pending is not None:
            phase = pending.args[0] if pending.args else 'a later phase'
            item.add_marker(pytest.mark.skip(reason = f'{NOT_RUN_PREFIX}pending {phase}'))

@pytest.hookimpl(hookwrapper = True)
def pytest_runtest_makereport(item : pytest.Item, call : pytest.CallInfo):
    outcome = yield
    report = outcome.get_result()
    results : Optional[RunResults] = getattr(item.config, '_vf_results', None)
    case_id = case_id_of(item)
    negative = item.get_closest_marker('negative_control')
    if results is None or (case_id is None and negative is None):
        return
    # One verdict per test: the call phase decides, unless setup already skipped/failed.
    if report.when == 'call' or (report.when == 'setup' and report.outcome != 'passed'):
        if negative is not None:
            target = negative.kwargs.get('of') or (negative.args[0] if negative.args else None)
            if target and report.when == 'call':
                results.record_negative_control(target, detected = report.passed)
            return
        assert case_id is not None
        if report.passed:
            status, reason = PASS, ''
        elif report.skipped:
            longrepr = report.longrepr
            text = longrepr[2] if isinstance(longrepr, tuple) else str(longrepr)
            status, reason = classify_skip(text)
        else:
            exc = call.excinfo.value if call.excinfo is not None else None
            status = INVALID_TEST if isinstance(exc, InvalidTest) else FAIL
            reason = f'{type(exc).__name__}: {exc}'[:500] if exc is not None else 'failed'
        results.record(case_id, variant_of(item), status, reason, item.nodeid)

def pytest_sessionfinish(session : pytest.Session, exitstatus : int) -> None:
    results : Optional[RunResults] = getattr(session.config, '_vf_results', None)
    if results is None:
        return
    ran_any = any(HERE in pathlib.Path(str(item.fspath)).parents for item in session.items)
    if not ran_any:
        return
    path = results.write(RESULTS_DIR, SEED)
    summary = results.summary()['by_status']
    session.config.pluginmanager.get_plugin('terminalreporter').write_line(
        f'conformance: {path} — ' + ', '.join(f'{k}={v}' for k, v in summary.items()))

# -- fixtures shared by every level -----------------------------------------

@pytest.fixture(scope = 'session')
def catalog() -> Dict[str, Any]:
    return load_catalog()

@pytest.fixture
def case_info(request : pytest.FixtureRequest) -> Dict[str, Any]:
    '''The catalog entry of the current test's case (given/fault_schedule/assertions/acceptance).'''
    case_id = case_id_of(request.node)
    assert case_id is not None, 'case_info needs a @pytest.mark.case marker'
    return cases_by_id()[case_id]

@pytest.fixture(scope = 'session')
def seed() -> int:
    '''The deterministic seed for every randomized schedule (``VF_TEST_SEED``, default 0).'''
    return SEED

@pytest.fixture
def record_faults(request : pytest.FixtureRequest):
    '''
    ``record_faults(schedule)`` attaches a ``FaultSchedule``'s fired-barrier counts
    to the current case (``fault_injection_observed`` in the results), and fails
    the test as INVALID_TEST when a scheduled barrier never fired — a fault that
    did not happen proves nothing.
    '''
    case_id = case_id_of(request.node)
    results : Optional[RunResults] = getattr(request.config, '_vf_results', None)

    def record(schedule) -> None:
        fired = schedule.fired()
        if results is not None and case_id is not None:
            results.record_faults(case_id, fired)
        unfired = schedule.unfired()
        if unfired:
            raise InvalidTest(f'scheduled barrier(s) never fired: {unfired}')
    return record

@pytest.fixture
def evidence_dir(request : pytest.FixtureRequest) -> pathlib.Path:
    '''
    A per-test directory under the results dir for observation logs and snapshots.
    Files written here are attached to the case's ``evidence_paths``.
    '''
    case_id = case_id_of(request.node) or 'no-case'
    safe = request.node.name.replace('[', '_').replace(']', '').replace('/', '_')
    path = RESULTS_DIR / 'evidence' / case_id / safe
    path.mkdir(parents = True, exist_ok = True)
    results : Optional[RunResults] = getattr(request.config, '_vf_results', None)
    if results is not None and case_id != 'no-case':
        results.add_evidence(case_id, str(path))
    return path


# -- gates: resources that must already exist; never provisioned here -----------

K3S_DEFAULTS = {'VF_K8S_CONTEXT': 'default', 'VF_K8S_NAMESPACE': 'videoflow-test', 'VF_K8S_PVC': 'vf-test-share'}
#: The kubernetes level runs only when the operator names the namespace explicitly:
#: its cases delete broker pods and scale StatefulSets inside the test namespaces,
#: which a plain `pytest --ignore=tests/integration` on a host that happens to be a
#: cluster node must not do on its own. `scripts/k3s-test-up.sh` prints the export.
K3S_OPT_IN = 'VF_K8S_NAMESPACE'

def _kubectl(*args : str, timeout : float = 20.0) -> tuple[int, str]:
    import subprocess
    try:
        proc = subprocess.run(['kubectl', *args], capture_output = True, text = True, check = False,
                              timeout = timeout)
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        return 1, str(e)
    return proc.returncode, (proc.stdout if proc.returncode == 0 else proc.stderr).strip()

@functools.lru_cache(maxsize = 1)
def k3s_status() -> tuple[Optional[str], Dict[str, str]]:
    '''
    ``(reason_not_ready, facts)``. The gate only *reads*: kubeconfig present, the
    current context is the expected one (never switched here), the cluster is
    k3s, the test namespace exists and the shared PVC is Bound. Anything missing
    names ``scripts/k3s-test-up.sh``, which is where provisioning lives.
    '''
    facts = {k: os.environ.get(k, v) for k, v in K3S_DEFAULTS.items()}
    if not os.environ.get(K3S_OPT_IN):
        return (f'{K3S_OPT_IN} unset — the kubernetes level runs only when the test namespace is named '
                f'explicitly (export the variables scripts/k3s-test-up.sh prints)'), facts
    kubeconfig = os.environ.get('KUBECONFIG') or os.path.expanduser('~/.kube/config')
    if not os.path.exists(kubeconfig):
        return f'no kubeconfig at {kubeconfig}', facts
    rc, context = _kubectl('config', 'current-context')
    if rc != 0 or context != facts['VF_K8S_CONTEXT']:
        return (f'kubectl context is {context!r}, expected VF_K8S_CONTEXT={facts["VF_K8S_CONTEXT"]!r} '
                f'(the harness never switches contexts)'), facts
    from videoflow.deploy.cluster import detect_cluster
    flavor = detect_cluster()
    if flavor != 'k3s':
        return f'cluster flavor is {flavor!r}, not k3s', facts
    rc, _ = _kubectl('get', 'namespace', facts['VF_K8S_NAMESPACE'])
    if rc != 0:
        return f'namespace {facts["VF_K8S_NAMESPACE"]} is missing — run scripts/k3s-test-up.sh', facts
    rc, phase = _kubectl('get', 'pvc', facts['VF_K8S_PVC'], '-n', facts['VF_K8S_NAMESPACE'],
                         '-o', 'jsonpath={.status.phase}')
    if rc != 0 or phase != 'Bound':
        return (f'PVC {facts["VF_K8S_PVC"]} in {facts["VF_K8S_NAMESPACE"]} is not Bound '
                f'({phase or "absent"}) — run scripts/k3s-test-up.sh'), facts
    facts['context'] = context
    return None, facts

@pytest.fixture
def k3s() -> Dict[str, str]:
    '''Facts about the prepared k3s test cluster, or NOT_RUN naming what is missing.'''
    reason, facts = k3s_status()
    if reason is not None:
        not_run(reason)
    return facts

@pytest.fixture
def k3s_admin(k3s : Dict[str, str]) -> Dict[str, str]:
    '''``k3s`` plus the RBAC powers a test that impersonates a restricted user needs.'''
    for verb, resource in (('impersonate', 'users'), ('create', 'clusterroles'), ('create', 'clusterrolebindings')):
        rc, out = _kubectl('auth', 'can-i', verb, resource)
        if rc != 0 or out.strip() != 'yes':
            not_run(f'the kubeconfig cannot {verb} {resource} ({out.strip() or "denied"})')
    return k3s


NATS_URL_DEFAULT = 'nats://localhost:4222'

@functools.lru_cache(maxsize = 1)
def nats_status() -> tuple[Optional[str], str]:
    '''``(reason_not_ready, url)``: a raw socket probe of ``VF_TEST_NATS_URL`` (never ``nats.connect``).'''
    import socket
    from urllib.parse import urlparse
    url = os.environ.get('VF_TEST_NATS_URL', NATS_URL_DEFAULT)
    parsed = urlparse(url)
    try:
        with socket.create_connection((parsed.hostname or 'localhost', parsed.port or 4222), timeout = 1):
            return None, url
    except OSError as e:
        return f'nothing listens at VF_TEST_NATS_URL={url} ({e}); start it with `docker compose up -d`', url

@pytest.fixture
def nats_url() -> str:
    '''A reachable JetStream server for broker-level cases, or NOT_RUN naming how to start one.'''
    reason, url = nats_status()
    if reason is not None:
        not_run(reason)
    return url


def _listening(url : str, default_port : int) -> Optional[str]:
    '''The reason ``url`` is not answering (a raw socket probe), or None when it is.'''
    import socket
    from urllib.parse import urlparse
    parsed = urlparse(url)
    try:
        with socket.create_connection((parsed.hostname or 'localhost', parsed.port or default_port), timeout = 1):
            return None
    except OSError as e:
        return f'{e}'


def _gated_url(env : str, default_port : int, profile : str) -> str:
    '''
    The URL an optional compose profile serves, gated the same way as ``nats_url``:
    the variable must be set *and* something must answer, else NOT_RUN naming the
    ``docker compose --profile`` that starts it. Never a default URL: a profile
    that was not asked for must not be silently assumed to be up.
    '''
    url = os.environ.get(env)
    if not url:
        not_run(f'{env} unset; start the fixture with `docker compose --profile {profile} up -d` and export it '
                f'(see docker-compose.yml)')
    reason = _listening(url, default_port)
    if reason is not None:
        not_run(f'nothing listens at {env}={url} ({reason}); `docker compose --profile {profile} up -d`')
    return url


@pytest.fixture
def redis_url() -> str:
    '''The dev Redis (``docker compose up -d``): ``VF_TEST_REDIS_URL``.'''
    url = os.environ.get('VF_TEST_REDIS_URL', 'redis://localhost:6379/0')
    reason = _listening(url, 6379)
    if reason is not None:
        not_run(f'nothing listens at VF_TEST_REDIS_URL={url} ({reason}); start it with `docker compose up -d`')
    return url


@pytest.fixture
def redis_durable_url() -> str:
    '''An append-only Redis (``--profile redis-durable``): ``VF_TEST_REDIS_DURABLE_URL``.'''
    return _gated_url('VF_TEST_REDIS_DURABLE_URL', 6381, 'redis-durable')


@pytest.fixture
def redis_small_url() -> str:
    '''A 64 MB ``volatile-lru`` Redis (``--profile redis-small``): ``VF_TEST_REDIS_SMALL_URL``.'''
    return _gated_url('VF_TEST_REDIS_SMALL_URL', 6382, 'redis-small')


@pytest.fixture
def toxiproxy_url() -> str:
    '''The toxiproxy control API (``--profile toxiproxy``): ``VF_TEST_TOXIPROXY_URL``.'''
    return _gated_url('VF_TEST_TOXIPROXY_URL', 8474, 'toxiproxy')


@pytest.fixture
def nats_proxied_url(toxiproxy_url : str) -> str:
    '''The NATS URL that goes through toxiproxy's ``nats`` proxy: ``VF_TEST_NATS_PROXIED_URL``.'''
    return _gated_url('VF_TEST_NATS_PROXIED_URL', 4242, 'toxiproxy')


@pytest.fixture
def redis_proxied_url(toxiproxy_url : str) -> str:
    '''The Redis URL that goes through toxiproxy's ``redis`` proxy: ``VF_TEST_REDIS_PROXIED_URL``.'''
    return _gated_url('VF_TEST_REDIS_PROXIED_URL', 6380, 'toxiproxy')


@pytest.fixture
def nats_cluster_urls() -> List[str]:
    '''The three-server JetStream cluster (``--profile cluster``): ``VF_TEST_NATS_CLUSTER_URLS``, comma-separated.'''
    raw = os.environ.get('VF_TEST_NATS_CLUSTER_URLS')
    if not raw:
        not_run('VF_TEST_NATS_CLUSTER_URLS unset; `docker compose --profile cluster up -d` and export '
                'nats://localhost:4231,nats://localhost:4232,nats://localhost:4233')
    urls = [u.strip() for u in raw.split(',') if u.strip()]
    for url in urls:
        reason = _listening(url, 4222)
        if reason is not None:
            not_run(f'nothing listens at {url} ({reason}); `docker compose --profile cluster up -d`')
    return urls


@pytest.fixture
def nats_restricted_url() -> str:
    '''A NATS server with a user denied consumer creation (``--profile restricted``): ``VF_TEST_NATS_RESTRICTED_URL``.'''
    return _gated_url('VF_TEST_NATS_RESTRICTED_URL', 4224, 'restricted')


HA_DEFAULTS = {'VF_K8S_HA_NAMESPACE': 'videoflow-test-ha', 'VF_K8S_HA_NATS_URL': 'nats://127.0.0.1:30423'}

@functools.lru_cache(maxsize = 1)
def k3s_ha_status() -> tuple[Optional[str], Dict[str, str]]:
    '''
    ``(reason_not_ready, facts)`` for the durable broker fixture
    (``scripts/k3s-test-up.sh --durable``): the ``-ha`` namespace, a three-pod
    NATS StatefulSet that is fully ready, the durable Redis, and the NodePort
    that makes the broker reachable from this host. Read-only, like ``k3s``.
    '''
    facts = {k: os.environ.get(k, v) for k, v in HA_DEFAULTS.items()}
    namespace = facts['VF_K8S_HA_NAMESPACE']
    if not os.environ.get('VF_K8S_HA_NAMESPACE'):
        return ('VF_K8S_HA_NAMESPACE unset — the durability cases scale and restart the durable broker, so '
                'they run only when its namespace is named explicitly (scripts/k3s-test-up.sh --durable)'), facts
    rc, _ = _kubectl('get', 'namespace', namespace)
    if rc != 0:
        return f'namespace {namespace} is missing — run scripts/k3s-test-up.sh --durable', facts
    rc, ready = _kubectl('get', 'statefulset', 'nats', '-n', namespace, '-o', 'jsonpath={.status.readyReplicas}')
    if rc != 0 or ready.strip() != '3':
        return f'the durable NATS StatefulSet in {namespace} is not 3/3 ready ({ready.strip() or "absent"})', facts
    rc, avail = _kubectl('get', 'deployment', 'redis', '-n', namespace, '-o', 'jsonpath={.status.availableReplicas}')
    if rc != 0 or avail.strip() != '1':
        return f'the durable Redis in {namespace} is not available ({avail.strip() or "absent"})', facts
    reason = _listening(facts['VF_K8S_HA_NATS_URL'], 4222)
    if reason is not None:
        return (f'nothing listens at VF_K8S_HA_NATS_URL={facts["VF_K8S_HA_NATS_URL"]} ({reason}); apply '
                f'k8s/nats-nodeport.yaml (port 30423) into {namespace} or port-forward svc/nats'), facts
    facts['namespace'] = namespace
    facts['nats_url'] = facts['VF_K8S_HA_NATS_URL']
    return None, facts


@pytest.fixture
def k3s_ha(k3s : Dict[str, str]) -> Dict[str, str]:
    '''The durable (replicated, persistent) broker fixture on the k3s cluster, or NOT_RUN naming what is missing.'''
    reason, facts = k3s_ha_status()
    if reason is not None:
        not_run(reason)
    return facts


@pytest.fixture
def redis_cluster_url() -> str:
    '''
    A Redis Cluster for the cluster-topology variants (PAY-008): ``VF_TEST_REDIS_CLUSTER_URL``.
    No compose profile provides one; point the variable at an existing cluster to run them.
    '''
    url = os.environ.get('VF_TEST_REDIS_CLUSTER_URL')
    if not url:
        not_run('VF_TEST_REDIS_CLUSTER_URL unset; no compose profile provides a Redis Cluster — export the URL '
                'of one (redis://host:port/0) to run the cluster variants')
    reason = _listening(url, 6379)
    if reason is not None:
        not_run(f'nothing listens at VF_TEST_REDIS_CLUSTER_URL={url} ({reason})')
    return url
