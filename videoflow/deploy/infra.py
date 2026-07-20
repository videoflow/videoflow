'''
Auto-provisioning of the dev broker infrastructure ``videoflow deploy`` needs
when the user doesn't bring their own: an in-cluster NATS JetStream server and,
when the blob store is wanted, a Redis. Built as plain dicts (same convention as
``manifests``) so they ship inside the package, parametrize by namespace, and
carry an ownership label for selective teardown.

Everything here is dev/test-grade (single replica, emptyDir/no persistence) — a
faithful port of ``k8s/nats.yaml``. For production, bring your own broker (the
official NATS Helm chart, a managed Redis) and pass ``--nats``/``--blob-redis-url``.

Ownership rule: a pre-existing ``nats``/``redis`` Service in the namespace is
reused as-is and never owned; only components this module applied are returned
as "created" and later torn down.
'''
from __future__ import absolute_import, division, print_function

import subprocess
from typing import List

from .manifests import LABEL_MANAGED_BY, dump_manifests

LABEL_INFRA = 'videoflow.io/infra'

_NATS_CONF = '''\
port: 4222
http: 8222
max_payload: 8MB
jetstream {
  store_dir: "/data/jetstream"
  max_memory_store: 1GB
  max_file_store: 10GB
}
'''

def _infra_labels(component : str) -> dict:
    return {'app': component, LABEL_INFRA: component, LABEL_MANAGED_BY: 'videoflow'}

def nats_manifests(namespace : str) -> list:
    '''Single-replica NATS JetStream (port of k8s/nats.yaml) + infra labels.'''
    labels = _infra_labels('nats')
    return [
        {
            'apiVersion': 'v1',
            'kind': 'ConfigMap',
            'metadata': {'name': 'nats-config', 'namespace': namespace, 'labels': labels},
            'data': {'nats.conf': _NATS_CONF},
        },
        {
            'apiVersion': 'apps/v1',
            'kind': 'Deployment',
            'metadata': {'name': 'nats', 'namespace': namespace, 'labels': labels},
            'spec': {
                'replicas': 1,
                'selector': {'matchLabels': {'app': 'nats'}},
                'template': {
                    'metadata': {'labels': labels},
                    'spec': {
                        'containers': [{
                            'name': 'nats',
                            'image': 'nats:2.10',
                            'args': ['-c', '/etc/nats/nats.conf'],
                            'ports': [
                                {'containerPort': 4222, 'name': 'client'},
                                {'containerPort': 8222, 'name': 'monitor'},
                            ],
                            'volumeMounts': [
                                {'name': 'config', 'mountPath': '/etc/nats'},
                                {'name': 'data', 'mountPath': '/data'},
                            ],
                        }],
                        'volumes': [
                            {'name': 'config', 'configMap': {'name': 'nats-config'}},
                            {'name': 'data', 'emptyDir': {}},
                        ],
                    },
                },
            },
        },
        {
            'apiVersion': 'v1',
            'kind': 'Service',
            'metadata': {'name': 'nats', 'namespace': namespace, 'labels': labels},
            'spec': {
                'selector': {'app': 'nats'},
                'ports': [
                    {'port': 4222, 'targetPort': 4222, 'name': 'client'},
                    {'port': 8222, 'targetPort': 8222, 'name': 'monitor'},
                ],
            },
        },
    ]

def redis_manifests(namespace : str) -> list:
    '''
    Single-replica Redis for the large-payload blob store. Persistence off: it is
    transport, not storage. Memory is capped with volatile-lru eviction — every key
    videoflow writes carries a TTL (PROTOCOL.md BLOB-7), so under pressure Redis
    evicts our oldest blobs instead of growing until the node OOMs (the redis:7
    default is unlimited memory with noeviction). The container limit sits above
    maxmemory to leave headroom for allocator fragmentation.
    '''
    labels = _infra_labels('redis')
    return [
        {
            'apiVersion': 'apps/v1',
            'kind': 'Deployment',
            'metadata': {'name': 'redis', 'namespace': namespace, 'labels': labels},
            'spec': {
                'replicas': 1,
                'selector': {'matchLabels': {'app': 'redis'}},
                'template': {
                    'metadata': {'labels': labels},
                    'spec': {
                        'containers': [{
                            'name': 'redis',
                            'image': 'redis:7-alpine',
                            'args': ['--save', '', '--appendonly', 'no',
                                     '--maxmemory', '4gb', '--maxmemory-policy', 'volatile-lru'],
                            'ports': [{'containerPort': 6379, 'name': 'client'}],
                            'resources': {
                                'requests': {'memory': '512Mi', 'cpu': '100m'},
                                'limits': {'memory': '5Gi'},
                            },
                        }],
                    },
                },
            },
        },
        {
            'apiVersion': 'v1',
            'kind': 'Service',
            'metadata': {'name': 'redis', 'namespace': namespace, 'labels': labels},
            'spec': {
                'selector': {'app': 'redis'},
                'ports': [{'port': 6379, 'targetPort': 6379, 'name': 'client'}],
            },
        },
    ]

def infra_urls(namespace : str) -> dict:
    '''The in-cluster URLs workers use once the dev infra is up.'''
    return {'nats': f'nats://nats.{namespace}.svc:4222',
            'redis': f'redis://redis.{namespace}.svc:6379/0'}

def ensure_namespace(kubectl : str, namespace : str) -> None:
    proc = subprocess.run([kubectl, 'get', 'namespace', namespace],
                          capture_output = True, check = False)
    if proc.returncode == 0:
        return
    proc = subprocess.run([kubectl, 'create', 'namespace', namespace],
                          capture_output = True, check = False)
    if proc.returncode != 0:
        raise RuntimeError(f'could not create namespace {namespace}: '
                           f'{proc.stderr.decode("utf-8", "replace")}')

def service_exists(kubectl : str, namespace : str, name : str) -> bool:
    proc = subprocess.run([kubectl, 'get', 'svc', name, '-n', namespace],
                          capture_output = True, check = False)
    return proc.returncode == 0

def ensure_infra(kubectl : str, namespace : str, need_redis : bool) -> tuple:
    '''
    Applies the dev NATS (and, when ``need_redis``, Redis) unless a Service of the
    same name already exists in the namespace (bring-your-own is reused, not owned).

    - Returns:
        - ``(urls, created)`` where ``urls`` maps ``nats``/``redis`` to in-cluster \
            URLs (``redis`` is None when not needed) and ``created`` lists only \
            the components THIS call applied (what teardown may later delete).
    '''
    urls = infra_urls(namespace)
    if not need_redis:
        urls['redis'] = None
    created = []
    to_apply = []
    if not service_exists(kubectl, namespace, 'nats'):
        to_apply += nats_manifests(namespace)
        created.append('nats')
    if need_redis and not service_exists(kubectl, namespace, 'redis'):
        to_apply += redis_manifests(namespace)
        created.append('redis')
    if to_apply:
        proc = subprocess.run([kubectl, 'apply', '-n', namespace, '-f', '-'],
                              input = dump_manifests(to_apply).encode('utf-8'),
                              capture_output = True, check = False)
        if proc.returncode != 0:
            raise RuntimeError(f'could not provision {"/".join(created)}: '
                               f'{proc.stderr.decode("utf-8", "replace")}')
    return urls, created

def wait_infra_ready(kubectl : str, namespace : str, created : List[str],
                     timeout_secs : int = 120) -> None:
    '''Blocks until each freshly created infra Deployment rolls out; raises on timeout.'''
    for component in created:
        proc = subprocess.run(
            [kubectl, 'rollout', 'status', f'deployment/{component}', '-n', namespace,
             f'--timeout={timeout_secs}s'],
            check = False,
        )
        if proc.returncode != 0:
            raise RuntimeError(f'{component} did not become ready within {timeout_secs}s — '
                               f'check `kubectl get pods -n {namespace} -l app={component}`.')

def teardown_infra(kubectl : str, namespace : str, components : List[str]) -> None:
    '''Deletes the given auto-provisioned components by ownership label. Best-effort (never raises).'''
    if not components:
        return
    selector = f'{LABEL_INFRA} in ({",".join(components)})'
    subprocess.run(
        [kubectl, 'delete', '-n', namespace, 'deployment,service,configmap', '-l', selector],
        check = False,
    )
