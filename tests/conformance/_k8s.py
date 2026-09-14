'''
Kubernetes plumbing for the kubernetes-level conformance cases: read-only
``kubectl`` queries, pod deletion inside the test namespaces (the only mutation),
readiness waits, a ``kubectl port-forward`` context manager for services with no
NodePort, and JetStream cluster facts (the stream leader) read through nats-py.

Everything here is scoped to the namespaces ``scripts/k3s-test-up.sh`` prepared;
nothing labels nodes, changes cluster policy or switches the kubectl context.
'''
from __future__ import absolute_import, division, print_function

import asyncio
import contextlib
import json
import os
import pathlib
import re
import subprocess
import time
from typing import Any, Dict, Iterator, List, Optional


def kubectl(*args : str, timeout : float = 60.0) -> str:
    '''Run ``kubectl`` and return stdout; a non-zero exit is a ``RuntimeError`` naming the command.'''
    proc = subprocess.run(['kubectl', *args], capture_output = True, text = True, check = False, timeout = timeout)
    if proc.returncode != 0:
        raise RuntimeError(f'kubectl {" ".join(args)} failed ({proc.returncode}): {proc.stderr.strip()}')
    return proc.stdout


def kubectl_json(*args : str) -> Any:
    return json.loads(kubectl(*args, '-o', 'json'))


def pods(namespace : str, selector : str) -> List[Dict[str, Any]]:
    return list(kubectl_json('get', 'pods', '-n', namespace, '-l', selector)['items'])


def pod_names(namespace : str, selector : str) -> List[str]:
    return sorted(p['metadata']['name'] for p in pods(namespace, selector))


def pod_ready(pod : Dict[str, Any]) -> bool:
    if pod.get('status', {}).get('phase') != 'Running' or pod.get('metadata', {}).get('deletionTimestamp'):
        return False
    statuses = pod.get('status', {}).get('containerStatuses') or []
    return bool(statuses) and all(s.get('ready') for s in statuses)


def volume_kinds(pod : Dict[str, Any]) -> set:
    '''The kinds of volume a pod mounts (``emptyDir``, ``persistentVolumeClaim``, ``configMap`` ...).'''
    kinds = set()
    for volume in pod.get('spec', {}).get('volumes') or []:
        kinds.update(k for k in volume if k != 'name')
    return kinds


def wait_ready(namespace : str, selector : str, count : int, timeout : float = 300.0) -> List[str]:
    '''Block until exactly ``count`` pods matching ``selector`` are Running and ready; return their names.'''
    deadline = time.monotonic() + timeout
    while True:
        current = pods(namespace, selector)
        ready = sorted(p['metadata']['name'] for p in current if pod_ready(p))
        if len(ready) == count and len(current) == count:
            return ready
        if time.monotonic() >= deadline:
            raise TimeoutError(f'{namespace}: {len(ready)}/{count} pods ready for {selector!r} after {timeout:.0f}s')
        time.sleep(2)


def delete_pods(namespace : str, names : List[str]) -> None:
    '''Delete pods by name (the workload's controller recreates them); returns as soon as the API accepted it.'''
    kubectl('delete', 'pod', '-n', namespace, *names, '--wait=false', timeout = 120)


def scale_statefulset(namespace : str, name : str, replicas : int) -> None:
    '''Scale a StatefulSet; a scale-down terminates its highest-ordinal pods and keeps their claims.'''
    kubectl('scale', 'statefulset', name, '-n', namespace, f'--replicas={replicas}', timeout = 60)


def free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return int(s.getsockname()[1])


@contextlib.contextmanager
def port_forward(namespace : str, target : str, remote_port : int, timeout : float = 30.0) -> Iterator[str]:
    '''
    ``kubectl port-forward`` to ``target`` (``deploy/redis``, ``pod/nats-0`` ...) on a
    free local port; yields ``127.0.0.1:<port>`` once the forward answers and kills
    the forward on exit. A forward is bound to one pod: after that pod is deleted
    a *new* forward is needed, which is exactly what a restart test wants.
    '''
    local = free_port()
    proc = subprocess.Popen(['kubectl', '-n', namespace, 'port-forward', target, f'{local}:{remote_port}'],
                            stdout = subprocess.PIPE, stderr = subprocess.STDOUT, text = True)
    try:
        deadline = time.monotonic() + timeout
        ready = False
        assert proc.stdout is not None
        while time.monotonic() < deadline and proc.poll() is None:
            line = proc.stdout.readline()
            if re.search(r'Forwarding from 127\.0\.0\.1:\d+', line):
                ready = True
                break
        if not ready:
            raise RuntimeError(f'port-forward to {target} in {namespace} did not come up: {proc.stdout.read()[:400]}')
        yield f'127.0.0.1:{local}'
    finally:
        proc.kill()
        with contextlib.suppress(Exception):
            proc.wait(timeout = 10)


def stream_facts(nats_url : str, stream : str, timeout : float = 10.0) -> Dict[str, Any]:
    '''
    ``stream_info`` reduced to what the durability cases assert on: the effective
    ``num_replicas`` and storage, the message count and first/last sequence, the
    Raft leader and its peers (nats-py 2.15.0 ``nats/js/api.py``: ``StreamInfo.cluster``
    is a ``ClusterInfo(name, leader, replicas)``; ``replicas`` are ``PeerInfo``).
    '''
    import nats  # optional dep (distributed extras)

    async def _go() -> Dict[str, Any]:
        nc = await nats.connect(nats_url, connect_timeout = timeout, allow_reconnect = False)
        try:
            info = await nc.jetstream().stream_info(stream)
        finally:
            await nc.drain()
        cluster = info.cluster
        return {
            'num_replicas': info.config.num_replicas, 'storage': str(info.config.storage or ''),
            'messages': info.state.messages, 'first_seq': info.state.first_seq, 'last_seq': info.state.last_seq,
            'leader': cluster.leader if cluster is not None else None,
            'peers': sorted(p.name for p in (cluster.replicas or [])) if cluster is not None else [],
        }

    async def _bounded() -> Dict[str, Any]:
        return await asyncio.wait_for(_go(), timeout = timeout + 5)
    return asyncio.run(_bounded())


def wait_for_leader(nats_url : str, stream : str, timeout : float = 180.0) -> Dict[str, Any]:
    '''Poll until the stream reports a leader again (a fresh election after a pod loss).'''
    deadline = time.monotonic() + timeout
    last : Optional[BaseException] = None
    while time.monotonic() < deadline:
        try:
            facts = stream_facts(nats_url, stream)
            if facts['leader']:
                return facts
        except Exception as e:  # noqa: BLE001 — the broker is mid-failover; the deadline is the bound
            last = e
        time.sleep(2)
    raise TimeoutError(f'stream {stream} has no leader after {timeout:.0f}s (last error: {last!r})')


# -- GPU pool workloads for the capacity/rollout cases (plan Phase 4) ----------------

TEST_LABEL = 'videoflow.io/conformance'
DEFAULT_REGISTRY = '10.128.81.10:5000'


def base_image() -> str:
    '''The pushed videoflow base image (``scripts/k3s-test-up.sh``), used as an inert GPU holder.'''
    return f'{os.environ.get("VF_K8S_IMAGE_REGISTRY", DEFAULT_REGISTRY)}/videoflow-base:py3.12'


def gpu_holder_deployment(name : str, namespace : str, nodes : List[str], gpus : int, replicas : int = 1,
                          strategy : Optional[Dict[str, Any]] = None, generation : str = 'a',
                          resources : Optional[Dict[str, Dict[str, str]]] = None,
                          expressions : Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    '''
    A Deployment whose pods hold ``gpus`` devices each and do nothing (``sleep``),
    pinned to ``nodes`` with the same affinity/tolerations/runtime class the
    renderer puts on a GPU worker, at the ``cluster-batch`` priority so it yields
    to training work. ``generation`` is an env value whose change forces a rollout.
    '''
    terms = [{'key': 'kubernetes.io/hostname', 'operator': 'In', 'values': list(nodes)}] + list(expressions or [])
    limits : Dict[str, Any] = {'nvidia.com/gpu': gpus}
    container_resources : Dict[str, Any] = {'limits': dict(limits, **(resources or {}).get('limits', {}))}
    if (resources or {}).get('requests'):
        container_resources['requests'] = dict((resources or {})['requests'])
    spec : Dict[str, Any] = {
        'replicas': replicas,
        'selector': {'matchLabels': {'app': name}},
        'template': {
            'metadata': {'labels': {'app': name, TEST_LABEL: 'true', 'app.kubernetes.io/managed-by': 'videoflow'}},
            'spec': {
                'priorityClassName': 'cluster-batch',
                'runtimeClassName': 'nvidia',
                'terminationGracePeriodSeconds': 5,
                'tolerations': [{'key': 'nvidia.com/gpu', 'operator': 'Exists', 'effect': 'NoSchedule'}],
                'affinity': {'nodeAffinity': {'requiredDuringSchedulingIgnoredDuringExecution': {
                    'nodeSelectorTerms': [{'matchExpressions': terms}]}}},
                'containers': [{
                    'name': 'holder', 'image': base_image(), 'imagePullPolicy': 'IfNotPresent',
                    'command': ['sleep', 'infinity'],
                    'env': [{'name': 'VF_CONFORMANCE_GENERATION', 'value': generation}],
                    'resources': container_resources,
                }],
            },
        },
    }
    if strategy:
        spec['strategy'] = strategy
    return {'apiVersion': 'apps/v1', 'kind': 'Deployment',
            'metadata': {'name': name, 'namespace': namespace, 'labels': {TEST_LABEL: 'true', 'app': name}},
            'spec': spec}


def apply(manifest : Dict[str, Any]) -> None:
    proc = subprocess.run(['kubectl', 'apply', '-f', '-'], input = json.dumps(manifest), capture_output = True,
                          text = True, check = False, timeout = 60)
    if proc.returncode != 0:
        raise RuntimeError(f'kubectl apply failed: {proc.stderr.strip()}')


def delete_workload(namespace : str, name : str, timeout : float = 180.0) -> None:
    '''Delete a holder Deployment and wait until its pods are gone: a Terminating pod still holds
    its devices in the API, and the next case reads free capacity from the pods it sees.'''
    subprocess.run(['kubectl', 'delete', 'deployment', name, '-n', namespace, '--ignore-not-found', '--wait=true',
                    '--timeout=120s'], capture_output = True, text = True, check = False, timeout = 150)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline and pods(namespace, f'app={name}'):
        time.sleep(2)


def rollout_status(namespace : str, name : str, timeout : float) -> tuple[bool, str]:
    '''``kubectl rollout status`` bounded by ``timeout``: (completed, last output).'''
    proc = subprocess.run(['kubectl', 'rollout', 'status', f'deployment/{name}', '-n', namespace,
                           f'--timeout={int(timeout)}s'], capture_output = True, text = True, check = False,
                          timeout = timeout + 30)
    return proc.returncode == 0, (proc.stdout + proc.stderr).strip()[-800:]


def pod_conditions(namespace : str, selector : str) -> List[Dict[str, Any]]:
    '''Phase, node and the scheduling condition message of every pod matching ``selector``.'''
    out = []
    for pod in pods(namespace, selector):
        scheduled = next((c for c in (pod.get('status') or {}).get('conditions', []) if c.get('type') == 'PodScheduled'), {})
        out.append({'name': pod['metadata']['name'], 'phase': (pod.get('status') or {}).get('phase'),
                    'node': (pod.get('spec') or {}).get('nodeName'), 'ready': pod_ready(pod),
                    'scheduled': scheduled.get('status'), 'reason': scheduled.get('reason'),
                    'message': scheduled.get('message', '')[:300]})
    return out


def node_allocatable(node : str) -> Dict[str, str]:
    return kubectl_json('get', 'node', node).get('status', {}).get('allocatable', {})


def mig_slices_advertised(node_doc : Dict[str, Any]) -> Dict[str, int]:
    '''
    The ``nvidia.com/mig-*`` resources a node advertises with a positive count.
    A kubelet keeps a resource the device plugin stopped advertising as a ``0``
    entry, so key presence says a slice profile once existed, not that one is
    offered now.
    '''
    return {k: int(v) for k, v in (node_doc.get('status') or {}).get('allocatable', {}).items()
            if k.startswith('nvidia.com/mig-') and str(v).isdigit() and int(v) > 0}


def whole_cards_back(node : str, before : Dict[str, str], timeout : float = 300.0) -> Dict[str, Any]:
    '''
    The node document once the device plugin advertises the whole cards it had
    ``before`` a managed-MIG case and no slice with a positive count — or the
    last document read when ``timeout`` runs out, for the caller's assertion to
    fail on. The MIG manager reports ``success`` while the plugin it bounced is
    still down, when every count reads 0 (see ``gpu._wait_for_restore``).
    '''
    deadline = time.monotonic() + timeout
    while True:
        doc = kubectl_json('get', 'node', node)
        if ((doc.get('status') or {}).get('allocatable', {}).get('nvidia.com/gpu') == before.get('nvidia.com/gpu')
                and not mig_slices_advertised(doc)) or time.monotonic() > deadline:
            return doc
        time.sleep(5)


def gpu_pods_on(node : str) -> List[Dict[str, Any]]:
    '''Every non-terminal pod on ``node`` whose containers request a ``nvidia.com/*`` resource.'''
    out = []
    for pod in kubectl_json('get', 'pods', '-A', '--field-selector', f'spec.nodeName={node}').get('items', []):
        if (pod.get('status') or {}).get('phase') in ('Succeeded', 'Failed'):
            continue
        if any('nvidia.com/' in k for c in (pod.get('spec') or {}).get('containers', [])
               for k in ((c.get('resources') or {}).get('limits') or {})):
            out.append(pod)
    return out


def free_gpus_on(node : str, settle : float = 120.0) -> int:
    '''
    Whole GPUs no pod holds on ``node`` — after waiting for pods still winding down
    (a Terminating pod holds its devices until it is gone, and a case that counts
    them as free would plan against capacity that is not there yet).
    '''
    deadline = time.monotonic() + settle
    while time.monotonic() < deadline and any(p['metadata'].get('deletionTimestamp') for p in gpu_pods_on(node)):
        time.sleep(2)
    allocatable = int(node_allocatable(node).get('nvidia.com/gpu', '0'))
    held = 0
    for pod in gpu_pods_on(node):
        for c in (pod.get('spec') or {}).get('containers', []):
            held += int(((c.get('resources') or {}).get('limits') or {}).get('nvidia.com/gpu', 0))
    return allocatable - held


# -- deploying a real flow on the shared cluster (the engine path) ----------------------------

K8S_INTEGRATION = pathlib.Path(__file__).resolve().parents[1] / 'integration' / 'k8s'


def cluster_support() -> Any:
    '''``tests/integration/k8s/support_k8s`` — the fixtures image, the work claim and the engine glue.'''
    import sys
    if str(K8S_INTEGRATION) not in sys.path:
        sys.path.insert(0, str(K8S_INTEGRATION))
    import support_k8s
    return support_k8s


def fixture_nodes() -> Any:
    '''The cluster fixture nodes module (``tests/integration/k8s/fixture_nodes``), importable in the fixtures image.'''
    import sys
    if str(K8S_INTEGRATION) not in sys.path:
        sys.path.insert(0, str(K8S_INTEGRATION))
    import fixture_nodes
    return fixture_nodes


def image_present(ref : str) -> Optional[str]:
    '''The reason ``ref`` cannot be pulled by the nodes (registry read-back through crane), or None.'''
    from shutil import which
    if which('crane') is None:
        return 'crane is not installed (scripts/push-images.sh names the install)'
    proc = subprocess.run(['crane', 'manifest', '--insecure', ref], capture_output = True, text = True, check = False,
                          timeout = 60)
    return None if proc.returncode == 0 else f'image {ref} is not in the registry — run scripts/k3s-test-up.sh'


def pods_of(namespace : str, flow_id : str, node : str) -> List[Dict[str, Any]]:
    '''Every pod of one node of a flow (any phase), by the labels the manifests render.'''
    from videoflow.deploy.manifests import k8s_name
    return kubectl_json('get', 'pods', '-n', namespace, '-l',
                        f'videoflow.io/flow-id={k8s_name(flow_id)},videoflow.io/node={k8s_name(node)}').get('items', [])


def termination_messages(pods : List[Dict[str, Any]]) -> Dict[str, List[str]]:
    '''``pod name -> [termination messages]`` of every terminated container state (current and last).'''
    out : Dict[str, List[str]] = {}
    for pod in pods:
        messages = []
        for status in (pod.get('status') or {}).get('containerStatuses', []) or []:
            for state in (status.get('state') or {}, status.get('lastState') or {}):
                terminated = state.get('terminated') or {}
                if terminated.get('message'):
                    messages.append(str(terminated['message']))
        out[pod['metadata']['name']] = messages
    return out


def delete_run_streams_from_host(nats_url : str, flow_id : str, run_id : str) -> None:
    '''The engine's broker teardown targets the in-cluster URL; from the host the NodePort reaches the same server.'''
    import nats

    from videoflow.messaging import topology

    async def _go() -> None:
        nc = await nats.connect(nats_url, connect_timeout = 5, max_reconnect_attempts = 0)
        try:
            await topology.delete_run_streams(nc, flow_id, run_id)
        finally:
            await nc.close()
    try:
        asyncio.run(_go())
    except Exception as e:      # noqa: BLE001 — best effort; the streams are run-scoped and harmless
        print(f'run streams of {flow_id}/{run_id} left on the broker: {e}')


def read_lines(path : pathlib.Path) -> List[str]:
    '''Complete lines of a sink's output file over NFS: only newline-terminated lines count.'''
    if not path.exists():
        return []
    data = path.read_bytes()
    text = data.decode('utf-8', 'replace')
    lines = text.split('\n')
    return [line for line in lines[:-1] if line] if not text.endswith('\n') else [line for line in lines if line]
