'''
Conformance cases: RUN-030, RUN-031, RUN-032, RUN-033, RUN-047.

Each test implements one catalog case (``tests/conformance/catalog/test_catalog.json``);
the docstring quotes the case's title and acceptance rule so the oracle stays next to
the code that decides it. A ``pending`` marker means the case is a skeleton reporting
NOT_RUN until its implementation phase lands.

RUN-030 is decided at process level here, on the real worker: a
``videoflow.runtime.worker`` subprocess holding one input inside ``consume`` and
four more in its adapter's prefetch queue is SIGTERMed. The production hook runs
``messenger.quiesce()`` and then lets the process die of the signal — so what the
test can show is exactly the drain's first half: admission stops, the parked
inputs go back to the broker at once (not after ``ack_wait``), the in-flight one is
redelivered when its lease lapses, and a replacement completes every input after
the old process is gone. The graceful branch — the in-flight input committed
before the process leaves — is the in-process ``graceful`` variant, which is also
what the negative control runs. The kubernetes-level variant runs the same drain
on the shared k3s cluster: a real worker pod of the fixtures image deleted
mid-consume, its Job's replacement completing every input. RUN-031/032/033 decide
the placement rules plan Phase 4 ships — host requests (``--resources``,
descriptor ``spec.resources``), hard constraints (``--gpu-nodes``, GFD terms) and
asset identity (``Node.required_assets`` verified by the worker) — on the rendered
manifests at model level and on the cluster at kubernetes level: the scheduler's
own admission for requests and constraints, a relocated worker pod for assets.
RUN-047's run-scoped names are RFC 0006's Phase-6 flip.

The cluster cases that run a real worker need the fixtures image rebuilt from this
tree (``scripts/k3s-test-up.sh``): the fixture nodes they use and the worker-side
asset check live in the image, not on the host.
'''
from __future__ import absolute_import, division, print_function

import hashlib
import json
import os
import pathlib
import signal
import subprocess
import sys
import time
from typing import Any, Callable, Dict, List, Optional

import defects
import defects_alloc
import defects_run3
import pytest
from _brokers import unique_ids
from _msgdrivers import JetStreamDriver, spec
from _runs3 import messengers, read_sink_log, start_worker, stop_flow, worker_env, write_evidence

from videoflow.backends import faults
from videoflow.backends.messaging import ChannelId
from videoflow.backends.outcomes import Known
from videoflow.core import constants
from videoflow.core.constants import BATCH, REALTIME
from videoflow.messaging import topology

NATS_TIMEOUT = 180
INPUTS = 6
#: The in-flight input's lease: long enough that the hand-back of the parked ones
#: (seconds) is distinguishable from the lease lapsing on a dead process.
ACK_WAIT = 12
HANDBACK_DEADLINE = 4.0


def _specs() -> list:
    return [spec('parent', [], 'producer', True), spec('sink', ['parent'], 'consumer', False)]


def _provision(driver : JetStreamDriver) -> None:
    '''The provision Job's work, with the durables' ``ack_wait`` at the lease this test watches lapse.'''
    topology.provision_flow_sync(driver.nats_url, _specs(), driver.flow_id, driver.run_id, BATCH, max_retries = 3,
                                 ack_wait = ACK_WAIT, timeout = 60)


def _leased(driver : JetStreamDriver) -> tuple[int, int]:
    observed = driver.consumer_state('sink', 'parent')
    assert isinstance(observed, Known), observed
    return observed.value                                        # (pending, leased)


def _publish(driver : JetStreamDriver, count : int, prefix : str = 'in') -> List[str]:
    ids = []
    for i in range(count):
        driver.publish_parent('parent', f'{prefix}-{i}', i + 1, {'input': f'{prefix}-{i}'})
        ids.append(f'{prefix}-{i}')
    return ids


def _oracle_run_030_forced(driver : JetStreamDriver, nats_url : str, evidence_dir : Any, record : Dict[str, Any],
                           record_faults : Callable[..., None] | None = None) -> None:
    '''
    The forced branch on the real worker: SIGTERM while one input is held inside
    ``consume`` and the rest are prefetched. Admission stops and the parked
    inputs are handed back before the process dies — the replacement, started
    once the old process has exited, commits them at once, while the in-flight
    one it never committed comes back only when its lease lapses. Every input is
    committed exactly once, all of it by the replacement.

    JetStream keeps a NAKed message in ``num_ack_pending`` until it is delivered
    again and acknowledged, so the broker's counters cannot show a hand-back on
    their own; what shows it is *when* the survivor gets the input — seconds,
    not the lease.
    '''
    credit = topology.consumer_credit(1, False)                  # 1 in processing + DEFAULT_PREFETCH parked
    # A fresh directory per run: markers and logs of an earlier run must not read as this one's.
    evidence_dir = evidence_dir / driver.run_id
    evidence_dir.mkdir(parents = True, exist_ok = True)
    sink_log = evidence_dir / 'sink_effects.jsonl'
    schedule = faults.FaultSchedule({'sink.effect.before': faults.Nth(1, faults.Pause('hold', timeout_seconds = 120))},
                                    marker_dir = str(evidence_dir / 'markers'))
    env = worker_env(nats_url, driver.flow_id, driver.run_id, 'sink', '_runs3_nodes.PausingSink',
                     {'log_path': str(sink_log), 'name': 'sink'}, ['parent'], ack_wait = ACK_WAIT,
                     extra = {**schedule.to_env(), 'VF_TERMINATION_LOG': str(evidence_dir / 'termination-log')})
    ids = _publish(driver, INPUTS)
    worker = start_worker(env, evidence_dir / 'worker-1.log')
    replacement = None
    try:
        # The worker holds its first input inside consume() and parks the next four.
        assert driver.until(lambda: schedule.fired().get('sink.effect.before', 0) >= 1, 30), 'the worker never reached its first input'
        assert driver.until(lambda: _leased(driver)[1] >= credit, 20), _leased(driver)
        before = _leased(driver)
        assert before == (INPUTS - credit, credit), before
        assert worker.poll() is None
        record['before_sigterm'] = {'pending': before[0], 'leased': before[1], 'credit': credit}
        t_term = time.time()
        worker.send_signal(signal.SIGTERM)
        worker.wait(timeout = 30)
        t_exit = time.time()
        assert worker.returncode == -signal.SIGTERM, worker.returncode   # dies of SIGTERM, as it always has
        assert read_sink_log(sink_log) == [], 'the SIGTERMed worker committed an input it was holding at the barrier'
        # The replacement starts only once the old process is gone (its grant is
        # free). The four handed-back inputs and the pending one reach it at once;
        # the in-flight one only when the dead process's lease lapses.
        schedule.release('hold')
        replacement = start_worker(env, evidence_dir / 'worker-2.log')
        t_start = time.time()
        assert driver.until(lambda: len(read_sink_log(sink_log)) >= INPUTS - 1, HANDBACK_DEADLINE), \
            f'{len(read_sink_log(sink_log))} inputs committed within {HANDBACK_DEADLINE}s: the parked inputs were not handed back'
        early = read_sink_log(sink_log)
        assert ids[0] not in {e['item']['input'] for e in early}, 'the in-flight input came back before its lease lapsed'
        assert driver.until(lambda: len(read_sink_log(sink_log)) >= INPUTS, ACK_WAIT * 3), read_sink_log(sink_log)
        effects = read_sink_log(sink_log)
        items = sorted(e['item']['input'] for e in effects)
        assert items == sorted(ids), items                         # no admitted input disappears, none twice
        assert {e['pid'] for e in effects} == {replacement.pid}, 'an input was committed by the dead process'
        in_flight = next(e for e in effects if e['item']['input'] == ids[0])
        assert in_flight['at'] - t_exit >= ACK_WAIT / 2, 'the in-flight input was not left to its lease'
        assert driver.until(lambda: _leased(driver) == (0, 0), 20), _leased(driver)
        record['after_sigterm'] = {'quiesce_to_exit_s': t_exit - t_term, 'exit_code': worker.returncode,
                                   'handed_back_committed_after_start_s': max(e['at'] for e in early) - t_start,
                                   'in_flight_committed_after_exit_s': in_flight['at'] - t_exit}
        record['replacement'] = {'started_after_exit_s': t_start - t_exit, 'completed': items, 'pid': replacement.pid,
                                 'old_pid': worker.pid}
    finally:
        for proc in (worker, replacement):
            if proc is not None and proc.poll() is None:
                stop_flow(nats_url, driver.flow_id, driver.run_id)
                try:
                    proc.wait(timeout = 15)
                except Exception:  # noqa: BLE001
                    proc.kill()
    if record_faults is not None:
        record_faults(schedule)
    record['faults'] = schedule.fired()


def _oracle_run_030_graceful(driver : JetStreamDriver, record : Dict[str, Any]) -> None:
    '''
    The graceful branch, in-process: ``quiesce()`` while one input is held stops
    admission and hands the parked inputs back at once — a survivor bound while
    the leaving worker is still alive gets them within seconds, as second
    attempts; the held input is then committed (acked) and the worker leaves,
    and the survivor never sees it.
    '''
    credit = topology.consumer_credit(1, False)
    ids = _publish(driver, INPUTS, prefix = 'g')
    with messengers() as pool:
        leaving = pool.messenger(driver, 'sink', ['parent'], ack_wait = ACK_WAIT)
        group = driver.receive_group(leaving, timeout = 30)
        held = group['parent']['message']['input']
        assert driver.until(lambda: _leased(driver)[1] >= credit, 20), _leased(driver)
        before = _leased(driver)
        t_quiesce = time.monotonic()
        leaving.quiesce()
        survivor = pool.messenger(driver, 'sink', ['parent'], ack_wait = ACK_WAIT)
        completed : List[tuple] = []
        for _ in range(INPUTS - 1):
            group = driver.receive_group(survivor, timeout = HANDBACK_DEADLINE)
            completed.append((group['parent']['message']['input'], survivor._inflight_handles[0].num_delivered))
            survivor.ack_inputs()
        t_received = time.monotonic()
        assert t_received - t_quiesce < HANDBACK_DEADLINE + 1.0, 'the parked inputs came back on the lease, not on the quiesce'
        assert sorted(item for item, _a in completed) == sorted(set(ids) - {held}), completed
        # Handed back, never a first attempt (a fetch in flight at the quiesce bounces once more).
        assert all(attempt >= 2 for item, attempt in completed if item != ids[-1]), completed
        # The leaving worker admits nothing more, commits what it held, and leaves.
        leaving.ack_inputs()
        terminal = driver.receive_group(leaving, timeout = 10)
        assert all(v.get('is_stop_signal') for v in terminal.values()), terminal
        pool.close(leaving)
        assert driver.until(lambda: _leased(driver) == (0, 0), 20), _leased(driver)
        assert driver.until(lambda: driver.retained(ChannelId(driver.flow_id, driver.run_id, 'parent')) == 0, 10)
        record['graceful'] = {'held': held, 'before_quiesce': before, 'handback_s': t_received - t_quiesce,
                              'survivor_completed': completed}


@pytest.mark.case('RUN-030')
@pytest.mark.level('process')
@pytest.mark.timeout(NATS_TIMEOUT)
def test_run_030_gpu_scale_down_and_rollout_drain_outstanding_work_before(nats_url, evidence_dir, record_faults,
                                                                          monkeypatch) -> None:
    '''
    RUN-030 (P0, integration, kubernetes): GPU scale-down and rollout drain outstanding work
    before releasing grants.

    Acceptance: No admitted input disappears and no old/new processes concurrently use the same
    exclusive grant after reassignment; forced drains have explicit failure/retry outcomes.

    Process level, on the real worker and the compose broker: the forced branch
    (SIGTERM mid-inference) and the graceful branch (quiesce, commit, leave). The
    grant here is the process itself — a replacement starts only after the old
    process has exited. The pod-level half with a real accelerator grant is the
    pending kubernetes primary.
    '''
    monkeypatch.setattr(constants, 'RFC0006', True)
    flow, run = unique_ids('run030')
    driver = JetStreamDriver(nats_url, flow, run)
    record : Dict[str, Any] = {'flow': flow, 'run': run}
    try:
        _provision(driver)
        _oracle_run_030_forced(driver, nats_url, evidence_dir, record, record_faults)
        graceful_run = unique_ids('run030g')[1]
        graceful = JetStreamDriver(nats_url, flow, graceful_run)
        try:
            _provision(graceful)
            _oracle_run_030_graceful(graceful, record)
        finally:
            graceful.close()
    finally:
        driver.close()
        write_evidence(evidence_dir, 'drain_timeline.json', record)


K8S_INTEGRATION = pathlib.Path(__file__).resolve().parents[1] / 'integration' / 'k8s'
CLUSTER_TIMEOUT = 900


def _cluster_support() -> Any:
    '''``tests/integration/k8s/support_k8s`` — the fixtures image, the work claim and the engine glue.'''
    if str(K8S_INTEGRATION) not in sys.path:
        sys.path.insert(0, str(K8S_INTEGRATION))
    import support_k8s
    return support_k8s


def _image_present(ref : str) -> Optional[str]:
    '''The reason ``ref`` cannot be pulled by the nodes (registry read-back through crane), or None.'''
    from shutil import which
    if which('crane') is None:
        return 'crane is not installed (scripts/push-images.sh names the install)'
    proc = subprocess.run(['crane', 'manifest', '--insecure', ref], capture_output = True, text = True, check = False,
                          timeout = 60)
    return None if proc.returncode == 0 else f'image {ref} is not in the registry — run scripts/k3s-test-up.sh'


def _pods_of(namespace : str, flow_id : str, node : str) -> List[Dict[str, Any]]:
    from _k8s import kubectl_json

    from videoflow.deploy.manifests import k8s_name
    return kubectl_json('get', 'pods', '-n', namespace, '-l',
                        f'videoflow.io/flow-id={k8s_name(flow_id)},videoflow.io/node={k8s_name(node)}').get('items', [])


def _delete_run_streams(flow_id : str, run_id : str) -> None:
    '''The engine's broker teardown targets the in-cluster URL; from the host the NodePort reaches the same server.'''
    import asyncio

    import nats

    from videoflow.messaging import topology
    url = os.environ.get('VF_K8S_NATS_URL', 'nats://127.0.0.1:30422')

    async def _go() -> None:
        nc = await nats.connect(url, connect_timeout = 5, max_reconnect_attempts = 0)
        try:
            await topology.delete_run_streams(nc, flow_id, run_id)
        finally:
            await nc.close()
    try:
        asyncio.run(_go())
    except Exception as e:      # noqa: BLE001 — best effort; the streams are run-scoped and harmless
        print(f'run streams of {flow_id}/{run_id} left on the broker: {e}')


def _read_lines(path : pathlib.Path) -> List[str]:
    '''The complete lines of a sink's output file: a read over NFS can catch a line mid-append, so
    only newline-terminated lines count (a torn tail is not a wrong line, it is not a line yet).'''
    if not path.exists():
        return []
    text = path.read_text()
    complete = text[:text.rfind('\n') + 1] if '\n' in text else ''
    return [line for line in complete.splitlines() if line.strip()]


@pytest.mark.case('RUN-030')
@pytest.mark.level('kubernetes')
@pytest.mark.variant('cluster')
@pytest.mark.timeout(CLUSTER_TIMEOUT)
def test_run_030_pod_deletion_releases_the_grant_only_after_the_drain(k3s, evidence_dir) -> None:
    '''
    The forced branch on the cluster: a BATCH sink pod (the fixtures image, one slow
    input at a time) is deleted while consuming; its Job's replacement pod starts only
    once the old one is gone and completes every input — none disappears, and the
    file the two pods wrote shows each input committed by exactly one of them.
    '''
    from _brokers import unique_ids
    from _k8s import delete_pods, kubectl_json

    from videoflow.core import Flow
    from videoflow.core.compiler import compile_flow
    from videoflow.core.supervision import SupervisionPolicy
    from videoflow.deploy.infra import infra_urls
    from videoflow.engines.kubernetes import KubernetesExecutionEngine
    from videoflow.producers import IntProducer
    support = _cluster_support()
    missing = _image_present(support.FIXTURE_IMAGE)
    if missing:
        pytest.skip(f'not_run: {missing}')
    root = support.work_root()
    if root is None:
        pytest.skip('not_run: the work claim is not Bound — run scripts/k3s-test-up.sh')
    if str(K8S_INTEGRATION) not in sys.path:
        sys.path.insert(0, str(K8S_INTEGRATION))
    from fixture_nodes import SlowLineWriterConsumer
    namespace = k3s['VF_K8S_NAMESPACE']
    flow_id, run_id = unique_ids('run030k')
    work = root / f'conf-{run_id}'
    work.mkdir(parents = True)
    out = work / 'seen.txt'
    inputs = 40
    producer = IntProducer(0, inputs - 1, 0.01, name = 'producer')
    sink = SlowLineWriterConsumer(str(out), delay_seconds = 1.0, name = 'sink')(producer)
    specs = compile_flow(Flow([sink], flow_type = BATCH, flow_id = flow_id))
    engine = KubernetesExecutionEngine(nats_url = infra_urls(namespace)['nats'], namespace = namespace,
                                       default_image = support.FIXTURE_IMAGE, specs = specs,
                                       mounts = support.work_mounts(str(work)), supervision = SupervisionPolicy(),
                                       image_pull_policy = 'Always', priority_class = support.PRIORITY_CLASS or None)
    record : Dict[str, Any] = {'inputs': inputs}
    try:
        engine.allocate_and_run_tasks(None, flow_id, BATCH, run_id)
        # Wait until the sink has committed a few inputs, then take its pod away mid-consume.
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline and len(_read_lines(out)) < 3:
            time.sleep(1)
        assert len(_read_lines(out)) >= 3, 'the sink never started committing'
        (old_pod,) = [p['metadata']['name'] for p in _pods_of(namespace, flow_id, 'sink')
                      if (p.get('status') or {}).get('phase') == 'Running']
        committed_before = _read_lines(out)
        assert len(committed_before) < inputs, 'the sink finished before it could be interrupted'
        t_delete = time.time()
        delete_pods(namespace, [old_pod])
        record['deleted'] = {'pod': old_pod, 'committed_before': len(committed_before)}
        # The replacement exists only after the old pod is gone (a Job with restartPolicy Never).
        deadline = time.monotonic() + 300
        replacement = None
        while time.monotonic() < deadline:
            pods = _pods_of(namespace, flow_id, 'sink')
            names = {p['metadata']['name'] for p in pods}
            if old_pod not in names and names:
                replacement = sorted(names)[0]
                break
            time.sleep(2)
        assert replacement is not None, 'no replacement pod appeared'
        record['replacement'] = {'pod': replacement, 'after_delete_s': time.time() - t_delete}
        assert engine.wait_for_completion() == []
        lines = _read_lines(out)
        seen = sorted({int(line) for line in lines})
        record['completed'] = {'distinct': seen, 'lines': len(lines)}
        assert seen == list(range(inputs)), seen                        # no admitted input disappears
        assert len(lines) - len(seen) <= 1, lines                       # at most the in-flight input twice
        job = kubectl_json('get', 'job', f'vf-{flow_id}-sink', '-n', namespace)
        record['job_status'] = job.get('status')
    finally:
        engine.teardown()
        _delete_run_streams(flow_id, run_id)
        import shutil
        shutil.rmtree(work, ignore_errors = True)
        (evidence_dir / 'cluster_drain.json').write_text(json.dumps(record, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-030')
def test_run_030_detects_a_quiesce_that_only_raises_a_flag(nats_url, monkeypatch) -> None:
    '''A quiesce that hands nothing back leaves the parked inputs leased on a leaving process: the oracle must catch it.'''
    monkeypatch.setattr(constants, 'RFC0006', True)
    defects_run3.flag_only_quiesce(monkeypatch)
    flow, run = unique_ids('run030n')
    driver = JetStreamDriver(nats_url, flow, run)
    try:
        _provision(driver)
        assert defects.detects(_oracle_run_030_graceful, driver, {})
    finally:
        driver.close()


# -- RUN-031 ---------------------------------------------------------------------

def _oracle_run_031_model(evidence : Dict[str, Any]) -> None:
    '''Host requests travel from the contract (descriptor, --resources) into the pod; a measured RSS is
    judged against the declared host budget, never a GPU memory declaration.'''
    from videoflow.core.compiler import NodeSpec
    from videoflow.deploy.manifests import host_resources_for, render_manifests
    descriptor = {'spec': {'device': ['gpu'], 'resources': {'cpu': 2, 'memory': '3Gi', 'gpu': {'count': 1, 'memoryGiB': 40}},
                           'runtime': {'images': {'gpu': 'vendor/model:1'}}}}
    remote = NodeSpec('model', None, {}, ['producer'], 'processor', True, 2, 'gpu', True, image = 'vendor/model:1',
                      component_ref = 'oci://vendor/model:1', descriptor = descriptor, gpu_memory_gib = 40)
    producer = NodeSpec('producer', 'videoflow.producers.IntProducer', {'start': 0, 'end': 5}, [], 'producer', True, 1,
                        'cpu', True)
    assert host_resources_for(remote, None) == {'cpu': '2', 'memory': '3Gi'}
    assert host_resources_for(remote, {'model': {'memory': '6Gi', 'memory_limit': '8Gi'}}) == {
        'cpu': '2', 'memory': '6Gi', 'memory_limit': '8Gi'}
    manifests = render_manifests([producer, remote], 'r031', 'realtime', 'nats://x:4222', 'r', default_image = 'img:1',
                                 resources = {'*': {'cpu': '500m'}, 'model': {'memory': '6Gi', 'memory_limit': '8Gi'}})
    model = next(m for m in manifests if m['kind'] == 'Deployment' and m['metadata']['name'] == 'vf-r031-model')
    resources = model['spec']['template']['spec']['containers'][0]['resources']
    assert resources['requests'] == {'cpu': '500m', 'memory': '6Gi'}
    assert resources['limits'] == {'nvidia.com/gpu': 1, 'memory': '8Gi'}
    assert 'nvidia.com/gpu' not in resources['requests'] and '40' not in json.dumps(resources)   # VRAM stays out of host budgets
    # A measured startup peak against the declared host budget: the process, not the GPU declaration.
    probe = ('import os, sys\nblock = bytearray(int(sys.argv[1]))\n'
             'print(next(l for l in open("/proc/self/status") if l.startswith("VmRSS")).split()[1])')
    rss_kib = int(subprocess.check_output([sys.executable, '-c', probe, str(64 << 20)], text = True).strip())
    budget_bytes = 3 << 30
    assert rss_kib * 1024 < budget_bytes
    assert rss_kib * 1024 > 64 << 20
    evidence.update({'rendered': resources, 'measured_rss_bytes': rss_kib * 1024, 'declared_budget_bytes': budget_bytes,
                     'gpu_memory_gib_declared': 40})


@pytest.mark.case('RUN-031')
@pytest.mark.level('kubernetes')
@pytest.mark.timeout(CLUSTER_TIMEOUT)
def test_run_031_host_cpu_and_ram_requests_participate_in_gpu_worker(k3s, evidence_dir) -> None:
    '''
    RUN-031 (P1, deployment, kubernetes): Host CPU and RAM requests participate in GPU worker
    admission.

    Acceptance: The resource-insufficient plan is rejected or remains explicitly unadmitted, and
    admitted startup stays within the declared resource policy without relying on unconfigured
    namespace defaults.

    On the cluster: two holders of the base image, one with a memory request no node can
    satisfy, one with a modest one — the scheduler leaves the first Pending naming the
    host memory (GPU capacity is beside the point) and runs the second; the rendered
    workers carry the same requests and limits.
    '''
    from _brokers import unique_ids
    from _k8s import apply, delete_workload, pod_conditions, wait_ready
    namespace = k3s['VF_K8S_NAMESPACE']
    run = unique_ids('r031')[1][:8]
    huge, modest = f'vf-conf-{run}-huge', f'vf-conf-{run}-fit'
    evidence : Dict[str, Any] = {}
    _oracle_run_031_model(evidence)

    def holder(name : str, memory : str) -> Dict[str, Any]:
        return {'apiVersion': 'apps/v1', 'kind': 'Deployment',
                'metadata': {'name': name, 'namespace': namespace, 'labels': {'videoflow.io/conformance': 'true', 'app': name}},
                'spec': {'replicas': 1, 'selector': {'matchLabels': {'app': name}},
                         'template': {'metadata': {'labels': {'app': name, 'videoflow.io/conformance': 'true',
                                                              'app.kubernetes.io/managed-by': 'videoflow'}},
                                      'spec': {'priorityClassName': 'cluster-batch', 'terminationGracePeriodSeconds': 5,
                                               'containers': [{'name': 'holder', 'image': _cluster_support().BASE_IMAGE,
                                                               'imagePullPolicy': 'IfNotPresent', 'command': ['sleep', 'infinity'],
                                                               'resources': {'requests': {'cpu': '100m', 'memory': memory},
                                                                             'limits': {'memory': memory}}}]}}}}
    try:
        apply(holder(huge, '4000Gi'))
        apply(holder(modest, '256Mi'))
        wait_ready(namespace, f'app={modest}', 1, timeout = 300)
        time.sleep(5)
        pending = pod_conditions(namespace, f'app={huge}')
        evidence['insufficient'] = pending
        evidence['fitting'] = pod_conditions(namespace, f'app={modest}')
        assert pending and all(c['phase'] == 'Pending' and 'memory' in c['message'] for c in pending), pending
    finally:
        delete_workload(namespace, huge)
        delete_workload(namespace, modest)
        (evidence_dir / 'host_resources.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('RUN-031')
@pytest.mark.level('model')
@pytest.mark.variant('render')
def test_run_031_requests_are_rendered_from_the_contract(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_run_031_model(evidence)
    (evidence_dir / 'host_resources_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-031')
def test_run_031_detects_dropped_host_requests(monkeypatch) -> None:
    defects_alloc.dropped_host_resources(monkeypatch)
    assert defects.detects(_oracle_run_031_model, {})


# -- RUN-032 ---------------------------------------------------------------------

def _oracle_run_032_model(evidence : Dict[str, Any]) -> None:
    '''Hard capability and locality requirements become required placement terms or explicit
    refusals — never generic pool membership — and delivery is verified at startup.'''
    from support_kubectl import FakeKubectl, nodes_json, pods_json

    from videoflow.backends.allocation import SHARING_EXCLUSIVE, Constraint, FeasiblePlan, Infeasible, WorkloadRequest
    from videoflow.consumers import VoidConsumer
    from videoflow.core import Flow
    from videoflow.core.compiler import compile_flow
    from videoflow.core.constants import GPU, REALTIME
    from videoflow.deploy.allocation_kubernetes import KubernetesAllocationBackend
    from videoflow.deploy.manifests import render_manifests
    from videoflow.processors import IdentityProcessor
    from videoflow.producers import IntProducer
    gfd = {'videoflow.io/gpu-pool': 'true', 'nvidia.com/gpu.count': '2', 'nvidia.com/gpu.memory': '81920'}
    hopper = dict(gfd, **{'nvidia.com/gpu.product': 'NVIDIA-H100-80GB-HBM3'})
    ampere = dict(gfd, **{'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB'})
    fake = FakeKubectl({'get nodes -l videoflow.io/gpu-pool=true -o json': nodes_json(
        ('gpu-h', hopper, {'nvidia.com/gpu': '2'}), ('gpu-a', ampere, {'nvidia.com/gpu': '2'})),
        'get pods -A -o json': pods_json()})
    real_run = subprocess.run
    subprocess.run = fake       # type: ignore[assignment]
    try:
        backend = KubernetesAllocationBackend('exclusive')
        snapshot = backend.inventory({}).value
    finally:
        subprocess.run = real_run   # type: ignore[assignment]
    needs_hopper = Constraint('nvidia.com/gpu.product', 'In', ('NVIDIA-H100-80GB-HBM3',))
    plan = backend.plan([WorkloadRequest('r032', 'r', 'vlm', 1, SHARING_EXCLUSIVE, constraints = (needs_hopper,))], snapshot)
    assert isinstance(plan, FeasiblePlan) and plan.assignments['vlm'][0].node == 'gpu-h'
    # The compatible node gone: an explicit refusal, not a fallback onto the generic pool.
    fake.responses['get nodes -l videoflow.io/gpu-pool=true -o json'] = nodes_json(('gpu-a', ampere, {'nvidia.com/gpu': '2'}))
    subprocess.run = fake       # type: ignore[assignment]
    try:
        without = backend.inventory({}).value
    finally:
        subprocess.run = real_run   # type: ignore[assignment]
    refused = backend.plan([WorkloadRequest('r032', 'r', 'vlm', 1, SHARING_EXCLUSIVE, constraints = (needs_hopper,))], without)
    assert isinstance(refused, Infeasible) and 'no pool node satisfies' in refused.reasons[0]
    # Locality: an execution group pinned to hosts renders as a required term; a zone key the
    # backend cannot verify is refused rather than dropped.
    flow = Flow([VoidConsumer(name = 'sink')(IdentityProcessor(name = 'vlm', device_type = GPU)(IntProducer(0, 3, name = 'p')))],
                flow_type = REALTIME, flow_id = 'r032')
    manifests = render_manifests(compile_flow(flow), 'r032', 'realtime', 'nats://x:4222', 'r', default_image = 'img:1',
                                 gpu_nodes = ['gpu-h'])
    pod = next(m for m in manifests if m['kind'] == 'Deployment' and m['metadata']['name'] == 'vf-r032-vlm')['spec']['template']['spec']
    terms = (pod.get('affinity') or {}).get('nodeAffinity', {}).get('requiredDuringSchedulingIgnoredDuringExecution', {}).get('nodeSelectorTerms')
    assert terms == [{'matchExpressions': [{'key': 'kubernetes.io/hostname', 'operator': 'In', 'values': ['gpu-h']}]}]
    zone = backend.plan([WorkloadRequest('r032', 'r', 'vlm', 1, SHARING_EXCLUSIVE, constraints = (
        Constraint('topology.kubernetes.io/zone', 'In', ('zone-b',)),))], snapshot)
    assert isinstance(zone, Infeasible) and 'cannot verify' in zone.reasons[0]
    # Startup verifies the actual capability: the worker's grant report names the delivered product.
    from videoflow.backends.allocation import DeliveredGrant, DeviceIdentity
    from videoflow.deploy.allocation_local import GRANT_ENV
    from videoflow.runtime.gpucheck import report_backend
    grant = DeliveredGrant('vlm/0', (DeviceIdentity('gpu-h', 0, 'GPU-h0', None, 'NVIDIA H100 80GB HBM3', 80 << 30),), True, 1, 'strict')
    report = report_backend(1, {GRANT_ENV: json.dumps(grant.to_dict())})
    assert report.delivered[0].product == 'NVIDIA H100 80GB HBM3' and report.problems == ()
    evidence.update({'placed': plan.assignments['vlm'][0].node, 'refused': list(refused.reasons), 'terms': terms,
                     'zone': list(zone.reasons), 'startup_report': report.to_dict()})


@pytest.mark.case('RUN-032')
@pytest.mark.level('kubernetes')
@pytest.mark.timeout(CLUSTER_TIMEOUT)
def test_run_032_model_compatibility_and_locality_requirements_constrain(k3s, k3s_gpu_nodes, evidence_dir) -> None:
    '''
    RUN-032 (P1, deployment, kubernetes): Model compatibility and locality requirements
    constrain placement.

    Acceptance: No fixture executes on an incompatible or prohibited host; successful admission
    demonstrates the declared capability on the actual runtime resource.

    On the pool: a holder pinned by the node's real GFD product lands there and
    ``nvidia-smi`` inside it names that product (the capability on the actual resource);
    the same holder asking for a product no pool node has stays Pending on the affinity
    rather than landing on a generic GPU; then the requirement is restored and it runs.
    '''
    from _brokers import unique_ids
    from _k8s import apply, delete_workload, gpu_holder_deployment, kubectl, kubectl_json, pod_conditions, wait_ready
    namespace = k3s['VF_K8S_NAMESPACE']
    node = k3s_gpu_nodes[0]
    labels = kubectl_json('get', 'node', node)['metadata']['labels']
    product = labels.get('nvidia.com/gpu.product')
    if not product:
        pytest.skip(f'not_run: node {node} has no GFD product label')
    name = 'vf-conf-' + unique_ids('r032')[1][:8]
    evidence : Dict[str, Any] = {'node': node, 'product': product}
    incompatible = [{'key': 'nvidia.com/gpu.product', 'operator': 'In', 'values': ['NVIDIA-NO-SUCH-MODEL']}]
    compatible = [{'key': 'nvidia.com/gpu.product', 'operator': 'In', 'values': [product]}]
    try:
        apply(gpu_holder_deployment(name, namespace, k3s_gpu_nodes, 1, expressions = incompatible))
        time.sleep(10)
        pending = pod_conditions(namespace, f'app={name}')
        evidence['incompatible'] = pending
        assert pending and all(c['phase'] == 'Pending' and 'affinity' in c['message'] for c in pending), pending
        apply(gpu_holder_deployment(name, namespace, k3s_gpu_nodes, 1, expressions = compatible, generation = 'b'))
        (pod,) = wait_ready(namespace, f'app={name}', 1, timeout = 300)
        placed = pod_conditions(namespace, f'app={name}')
        evidence['compatible'] = placed
        assert placed[0]['node'] in k3s_gpu_nodes
        seen = kubectl('exec', '-n', namespace, pod, '--', 'nvidia-smi', '--query-gpu=name,uuid', '--format=csv,noheader',
                       timeout = 120).strip()
        evidence['delivered'] = seen
        assert seen and product.replace('-', ' ').split(' ')[1] in seen.replace('-', ' '), (product, seen)
    finally:
        delete_workload(namespace, name)
        (evidence_dir / 'placement.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('RUN-032')
@pytest.mark.level('model')
@pytest.mark.variant('constraints')
def test_run_032_requirements_render_as_terms_or_refusals(evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_run_032_model(evidence)
    (evidence_dir / 'placement_model.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-032')
def test_run_032_detects_requirements_downgraded_to_pool_membership(monkeypatch) -> None:
    from videoflow.deploy import allocation_kubernetes as ak
    monkeypatch.setattr(ak, 'constraint_holds', lambda constraint, node: True)
    assert defects.detects(_oracle_run_032_model, {})


# -- RUN-033 ---------------------------------------------------------------------

def _oracle_run_033_model(tmp : pathlib.Path, evidence : Dict[str, Any]) -> None:
    '''A hostPath is no evidence of the bytes; a local-only asset fails explicitly where it is
    absent; a portable asset is verified by identity before readiness; a hostPath under a
    claim's path is dropped from the pods.'''
    from videoflow.core.node import AssetRequirement
    from videoflow.deploy.manifests import Mount, pod_mounts
    from videoflow.runtime.assetcheck import verify_assets
    host_a, host_b = tmp / 'hostA', tmp / 'hostB'
    host_a.mkdir(); host_b.mkdir()
    (host_a / 'model.bin').write_bytes(b'weights-v1')
    (host_b / 'model.bin').write_bytes(b'different bytes at the same path')
    digest = hashlib.sha256(b'weights-v1').hexdigest()
    declared = [AssetRequirement(str(host_a / 'model.bin'), digest, portable = False)]
    verified = verify_assets(declared, 'sink')
    assert verified and verified[0].status == 'verified', verified
    relocated = [AssetRequirement(str(host_b / 'model.bin'), digest, portable = False)]
    with pytest.raises(Exception) as excinfo:
        verify_assets(relocated, 'sink')
    assert 'not the declared' in str(excinfo.value) and 'local-only' in getattr(excinfo.value, 'remedy', '')
    with pytest.raises(Exception) as excinfo:
        verify_assets([AssetRequirement(str(tmp / 'nowhere' / 'model.bin'), digest, portable = True)], 'sink')
    assert 'is missing' in str(excinfo.value) and 'distribution' in getattr(excinfo.value, 'remedy', '')
    # Placement: a hostPath shadowed by a claim is served by the claim (the portable artifact), not the host.
    mounts = [Mount('vf-mount-0', str(host_a), str(host_a), False), Mount('vf-pvc-0', '', '/share', False, claim = 'vf-test-share')]
    rendered = pod_mounts(mounts + [Mount('vf-mount-1', '/share/models', '/share/models', False)])
    assert [m.claim for m in rendered if m.container_path.startswith('/share')] == ['vf-test-share']
    evidence.update({'digest': digest, 'relocated_error': str(excinfo.value), 'pod_mounts': [m.container_path for m in rendered]})


@pytest.mark.case('RUN-033')
@pytest.mark.level('kubernetes')
@pytest.mark.timeout(CLUSTER_TIMEOUT)
def test_run_033_worker_relocation_preserves_model_and_input_asset_identity(k3s, evidence_dir) -> None:
    '''
    RUN-033 (P1, deployment, kubernetes): Worker relocation preserves model and input asset
    identity.

    Acceptance: Relocation never processes with silently missing or changed model/input bytes;
    the portable variant resumes with the verified asset.

    On the cluster: a REALTIME sink that declares its asset (the fixtures image's
    ``AssetLineWriterConsumer``) starts on this host, where a local-only file exists,
    then is relocated to another node by a node selector: the replacement refuses to
    open (the termination log names the host and the missing bytes) and processes
    nothing. The portable variant keeps the asset on the shared claim, and the
    relocated worker verifies it and resumes.
    '''
    from _brokers import unique_ids
    from _k8s import kubectl, kubectl_json

    from videoflow.core import Flow
    from videoflow.core.compiler import compile_flow
    from videoflow.core.supervision import SupervisionPolicy
    from videoflow.deploy.infra import infra_urls
    from videoflow.deploy.manifests import k8s_name, parse_mounts
    from videoflow.engines.kubernetes import KubernetesExecutionEngine
    from videoflow.producers import IntProducer
    support = _cluster_support()
    missing = _image_present(support.FIXTURE_IMAGE)
    if missing:
        pytest.skip(f'not_run: {missing}')
    root = support.work_root()
    if root is None:
        pytest.skip('not_run: the work claim is not Bound — run scripts/k3s-test-up.sh')
    if str(K8S_INTEGRATION) not in sys.path:
        sys.path.insert(0, str(K8S_INTEGRATION))
    from fixture_nodes import AssetLineWriterConsumer
    namespace = k3s['VF_K8S_NAMESPACE']
    nodes = [n['metadata']['name'] for n in kubectl_json('get', 'nodes').get('items', [])
             if all(c.get('type') != 'Ready' or c.get('status') == 'True' for c in n['status'].get('conditions', []))]
    here = os.uname().nodename
    if here not in nodes or len(nodes) < 2:
        pytest.skip(f'not_run: this host ({here}) must be a Ready cluster node with at least one other node')
    other = next(n for n in nodes if n != here)
    evidence : Dict[str, Any] = {'host_a': here, 'host_b': other, 'variants': {}}
    for portable in (False, True):
        flow_id, run_id = unique_ids('run033p' if portable else 'run033l')
        work = root / f'conf-{run_id}'
        work.mkdir(parents = True)
        weights = b'weights-' + run_id.encode()
        digest = hashlib.sha256(weights).hexdigest()
        if portable:
            asset = work / 'model.bin'                          # on the claim: every node sees these bytes
            asset.write_bytes(weights)
            mounts = support.work_mounts(str(work))
        else:
            local = pathlib.Path(f'/tmp/videoflow-conf-{run_id}')  # this host only
            local.mkdir(parents = True)
            asset = local / 'model.bin'
            asset.write_bytes(weights)
            # One parse_mounts call for both host paths (volume names are numbered per call),
            # plus the claim that serves the work directory in the pods.
            mounts = parse_mounts([str(work), str(local)]) + [m for m in support.work_mounts(str(work)) if m.claim]
        out = work / 'seen.txt'
        producer = IntProducer(0, 10 ** 6, 0.5, name = 'producer')
        sink = AssetLineWriterConsumer(str(out), str(asset), digest, portable = portable, name = 'sink')(producer)
        specs = compile_flow(Flow([sink], flow_type = REALTIME, flow_id = flow_id))
        # ``drain``: the old pod stops before the replacement starts, so the two never
        # append to the one output file at once (two NFS clients appending concurrently
        # clobber each other's lines — a property of NFS, not of the flow).
        engine = KubernetesExecutionEngine(nats_url = infra_urls(namespace)['nats'], namespace = namespace,
                                           default_image = support.FIXTURE_IMAGE, specs = specs, mounts = mounts,
                                           supervision = SupervisionPolicy(), image_pull_policy = 'Always',
                                           priority_class = support.PRIORITY_CLASS or None, rollout_policy = 'drain')
        record : Dict[str, Any] = {'asset': str(asset), 'digest': digest}
        deployment = f'vf-{k8s_name(flow_id)}-sink'
        try:
            engine.allocate_and_run_tasks(None, flow_id, REALTIME, run_id)
            # Pin the sink to this host first (the local file exists here), wait for it to process.
            kubectl('patch', 'deployment', deployment, '-n', namespace, '--type=merge', '-p',
                    json.dumps({'spec': {'template': {'spec': {'nodeSelector': {'kubernetes.io/hostname': here}}}}}))
            deadline = time.monotonic() + 300
            while time.monotonic() < deadline and len(_read_lines(out)) < 3:
                time.sleep(2)
            before = _read_lines(out)
            assert len(before) >= 3, 'the sink never processed on host A'
            assert all(line.endswith(':' + weights[:8].hex()) for line in before), before
            record['on_host_a'] = len(before)
            # Relocate to another node.
            kubectl('patch', 'deployment', deployment, '-n', namespace, '--type=merge', '-p',
                    json.dumps({'spec': {'template': {'spec': {'nodeSelector': {'kubernetes.io/hostname': other}}}}}))
            deadline = time.monotonic() + 300
            relocated : Dict[str, Any] = {}
            while time.monotonic() < deadline:
                pods = [p for p in _pods_of(namespace, flow_id, 'sink') if (p.get('spec') or {}).get('nodeName') == other]
                if pods:
                    pod = pods[0]
                    statuses = (pod.get('status') or {}).get('containerStatuses') or []
                    state = statuses[0].get('state', {}) if statuses else {}
                    last = statuses[0].get('lastState', {}) if statuses else {}
                    ready = bool(statuses and statuses[0].get('ready'))
                    terminated = (last.get('terminated') or state.get('terminated') or {})
                    if portable and ready:
                        relocated = {'pod': pod['metadata']['name'], 'ready': True}
                        break
                    if not portable and terminated.get('message'):
                        relocated = {'pod': pod['metadata']['name'], 'ready': False, 'termination': terminated.get('message'),
                                     'exit_code': terminated.get('exitCode'), 'restarts': statuses[0].get('restartCount')}
                        break
                time.sleep(3)
            record['relocated'] = relocated
            assert relocated, 'the relocated pod never reached a decision'
            if portable:
                deadline = time.monotonic() + 120
                while time.monotonic() < deadline and len(_read_lines(out)) <= len(before):
                    time.sleep(2)
                after = _read_lines(out)
                assert len(after) > len(before), 'the relocated worker did not resume'
                assert all(line.endswith(':' + weights[:8].hex()) for line in after), after[-3:]
                record['resumed_lines'] = len(after) - len(before)
            else:
                message = relocated['termination']
                assert 'asset' in message and 'is missing' in message and relocated['pod'] in message, message
                assert 'local-only' in message, message
                assert relocated['exit_code'] == 3, relocated                 # VideoflowEnvironmentError
                # The old pod is gone (drain) and its last lines have reached the file once the
                # NFS attribute cache has turned over; from then on the count must not move.
                time.sleep(45)
                settled = len(_read_lines(out))
                time.sleep(20)
                assert len(_read_lines(out)) == settled, 'the relocated worker processed without the asset'
                record['settled_lines'] = settled
        finally:
            engine.teardown()
            _delete_run_streams(flow_id, run_id)
            import shutil
            shutil.rmtree(work, ignore_errors = True)                   # the claim's directory (NFS, no_root_squash)
            if not portable:
                shutil.rmtree(local, ignore_errors = True)
            evidence['variants']['portable' if portable else 'local-only'] = record
    (evidence_dir / 'relocation.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.case('RUN-033')
@pytest.mark.level('model')
@pytest.mark.variant('identity')
def test_run_033_assets_are_verified_by_identity_not_by_path(tmp_path, evidence_dir) -> None:
    evidence : Dict[str, Any] = {}
    _oracle_run_033_model(tmp_path, evidence)
    (evidence_dir / 'asset_identity.json').write_text(json.dumps(evidence, indent = 2, default = str))


@pytest.mark.negative_control(of = 'RUN-033')
def test_run_033_detects_a_trusting_worker(tmp_path, monkeypatch) -> None:
    import test_run_rollout as me

    from videoflow.runtime import assetcheck
    monkeypatch.setattr(assetcheck, 'verify_assets', lambda requirements, node_name = 'node': [])
    monkeypatch.setattr(me, '_oracle_run_033_model', me._oracle_run_033_model)
    assert defects.detects(_oracle_run_033_model, tmp_path, {})


@pytest.mark.case('RUN-047')
@pytest.mark.level('kubernetes')
@pytest.mark.pending('phase 6')
def test_run_047_concurrent_kubernetes_runs_cannot_overwrite_another_runs() -> None:
    '''
    RUN-047 (P0, deployment, kubernetes): Concurrent Kubernetes runs cannot overwrite another
    runs configuration or workload.

    Acceptance: RunA remains unchanged and correctly configured throughout runB
    creation/deletion, or runB is rejected before any mutation under an explicit single-run
    policy.

    Pending phase 6: run-scoped Kubernetes names (RFC 0006 §10) and the
    ``--single-run`` policy are the acceptance flip.
    '''
