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
