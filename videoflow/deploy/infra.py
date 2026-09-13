'''
Auto-provisioning of the dev broker infrastructure ``videoflow deploy`` needs
when the user doesn't bring their own: an in-cluster NATS JetStream server and,
when the blob store is wanted, a Redis. Built as plain dicts (same convention as
``manifests``) so they ship inside the package, parametrize by namespace, and
carry an ownership label for selective teardown.

Two shapes, chosen by a ``deploy.broker_profiles`` profile:

  - the **dev** profile (``profile = None``): single replica, emptyDir, no
    persistence — a faithful port of ``k8s/nats.yaml``, rendered byte-for-byte
    as it always was. For production, bring your own broker (the official NATS
    Helm chart, a managed Redis) and pass ``--nats``/``--blob-redis-url``.
  - the **durable** profile: a NATS StatefulSet whose pods route to each other
    through a headless Service (``cluster { routes }``) and keep the JetStream
    file store on a PersistentVolumeClaim each, plus an append-only Redis on a
    claim of its own. Enough for a stream to survive a pod, a node, or a
    rollout; still not a tuned production broker.

Ownership rule: a pre-existing ``nats``/``redis`` Service in the namespace is
reused as-is and never owned; only components this module applied are returned
as "created" and later torn down. Persistent claims a durable profile created
are deliberately *not* torn down with the workloads — the data is the point —
so a redeploy finds it; reclaim them by hand with
``kubectl delete pvc -n <namespace> -l videoflow.io/infra``.
'''
from __future__ import absolute_import, division, print_function

import subprocess
from typing import List, Optional

from .broker_profiles import BrokerProfile, RedisProfile
from .manifests import LABEL_MANAGED_BY, dump_manifests

LABEL_INFRA = 'videoflow.io/infra'

#: The client Service every profile exposes (what ``infra_urls`` names) and the
#: headless one a StatefulSet's pods address each other through.
NATS_SERVICE = 'nats'
NATS_HEADLESS_SERVICE = 'nats-headless'
#: The claim a durable Redis keeps its append-only file on.
REDIS_CLAIM = 'redis-data'

#: The JetStream cluster name every route peer must agree on.
_NATS_CLUSTER_NAME = 'videoflow'
_NATS_ROUTE_PORT = 6222

def _infra_labels(component : str) -> dict:
    return {'app': component, LABEL_INFRA: component, LABEL_MANAGED_BY: 'videoflow'}

def nats_conf(namespace : str, profile : Optional[BrokerProfile] = None) -> str:
    '''
    The ``nats.conf`` the server pods run with.

    The dev profile's text is the one ``k8s/nats.yaml`` carries. A stateful
    profile adds ``server_name`` (JetStream clustering requires a unique name per
    server; ``$POD_NAME`` is substituted by the server from the pod's downward-API
    env) and, for more than one replica, a ``cluster`` block whose routes name
    every peer through the headless Service — the seed list JetStream's Raft
    groups form over.
    '''
    profile = profile or BrokerProfile.dev()
    conf = ''
    if profile.stateful:
        conf += 'server_name: $POD_NAME\n'
    conf += (
        'port: 4222\n'
        'http: 8222\n'
        'max_payload: 8MB\n'
        'jetstream {\n'
        '  store_dir: "/data/jetstream"\n'
        '  max_memory_store: 1GB\n'
        f'  max_file_store: {profile.max_file_store}\n'
        '}\n'
    )
    if profile.replicas > 1:
        routes = ',\n'.join(
            f'    nats://{NATS_SERVICE}-{i}.{NATS_HEADLESS_SERVICE}.{namespace}.svc:{_NATS_ROUTE_PORT}'
            for i in range(profile.replicas))
        conf += (
            'cluster {\n'
            f'  name: {_NATS_CLUSTER_NAME}\n'
            f'  port: {_NATS_ROUTE_PORT}\n'
            '  routes: [\n'
            f'{routes}\n'
            '  ]\n'
            '}\n'
        )
    return conf

# Kept as the literal the dev profile has always rendered; the golden test pins
# nats_conf() against it so a template slip cannot change the default silently.
_NATS_CONF = nats_conf('default')

def _nats_pod_spec(profile : BrokerProfile) -> dict:
    container : dict = {
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
    }
    volumes : list = [{'name': 'config', 'configMap': {'name': 'nats-config'}}]
    if profile.stateful:
        # server_name: $POD_NAME in the config reads this.
        container['env'] = [{'name': 'POD_NAME',
                             'valueFrom': {'fieldRef': {'fieldPath': 'metadata.name'}}}]
    if profile.replicas > 1:
        container['ports'].append({'containerPort': _NATS_ROUTE_PORT, 'name': 'cluster'})
    if not profile.persistence:
        volumes.append({'name': 'data', 'emptyDir': {}})
    pod_spec : dict = {'containers': [container], 'volumes': volumes}
    if profile.priority_class:
        pod_spec['priorityClassName'] = profile.priority_class
    return pod_spec

def _nats_workload(namespace : str, profile : BrokerProfile, labels : dict) -> dict:
    template = {'metadata': {'labels': labels}, 'spec': _nats_pod_spec(profile)}
    if not profile.stateful:
        return {
            'apiVersion': 'apps/v1',
            'kind': 'Deployment',
            'metadata': {'name': NATS_SERVICE, 'namespace': namespace, 'labels': labels},
            'spec': {
                'replicas': 1,
                'selector': {'matchLabels': {'app': 'nats'}},
                'template': template,
            },
        }
    spec : dict = {
        # The headless Service gives pod N the stable name nats-N.nats-headless
        # the routes above point at (kubectl explain statefulset.spec.serviceName).
        'serviceName': NATS_HEADLESS_SERVICE,
        'replicas': profile.replicas,
        # All pods at once: a Raft group needs a majority up before it elects a
        # leader, and OrderedReady would wait for each pod in turn.
        'podManagementPolicy': 'Parallel',
        'selector': {'matchLabels': {'app': 'nats'}},
        'template': template,
    }
    if profile.persistence:
        # One claim per pod, named data-nats-N, retained across a delete of the
        # StatefulSet by default (kubectl explain
        # statefulset.spec.persistentVolumeClaimRetentionPolicy) — so the file
        # store outlives the workload, which is what "durable" promises.
        claim_spec : dict = {
            'accessModes': ['ReadWriteOnce'],
            'resources': {'requests': {'storage': profile.storage_size}},
        }
        if profile.storage_class is not None:
            claim_spec['storageClassName'] = profile.storage_class
        spec['volumeClaimTemplates'] = [{
            'metadata': {'name': 'data', 'labels': labels},
            'spec': claim_spec,
        }]
    return {
        'apiVersion': 'apps/v1',
        'kind': 'StatefulSet',
        'metadata': {'name': NATS_SERVICE, 'namespace': namespace, 'labels': labels},
        'spec': spec,
    }

def nats_manifests(namespace : str, profile : Optional[BrokerProfile] = None) -> list:
    '''
    The NATS JetStream server for ``namespace`` + infra labels: the single-replica
    Deployment of ``k8s/nats.yaml`` for the dev profile (``None``), a StatefulSet
    with a headless Service, route peers and one claim per pod for a durable one.
    The client Service is named ``nats`` in every profile, which is what
    ``infra_urls`` and the reuse rule key on.
    '''
    profile = profile or BrokerProfile.dev()
    labels = _infra_labels('nats')
    manifests = [
        {
            'apiVersion': 'v1',
            'kind': 'ConfigMap',
            'metadata': {'name': 'nats-config', 'namespace': namespace, 'labels': labels},
            'data': {'nats.conf': nats_conf(namespace, profile)},
        },
        _nats_workload(namespace, profile, labels),
        {
            'apiVersion': 'v1',
            'kind': 'Service',
            'metadata': {'name': NATS_SERVICE, 'namespace': namespace, 'labels': labels},
            'spec': {
                'selector': {'app': 'nats'},
                'ports': [
                    {'port': 4222, 'targetPort': 4222, 'name': 'client'},
                    {'port': 8222, 'targetPort': 8222, 'name': 'monitor'},
                ],
            },
        },
    ]
    if profile.stateful:
        manifests.append({
            'apiVersion': 'v1',
            'kind': 'Service',
            'metadata': {'name': NATS_HEADLESS_SERVICE, 'namespace': namespace, 'labels': labels},
            'spec': {
                'clusterIP': 'None',
                'selector': {'app': 'nats'},
                # Peers must resolve each other before any of them is Ready, or
                # the cluster never forms (kubectl explain
                # service.spec.publishNotReadyAddresses).
                'publishNotReadyAddresses': True,
                'ports': [
                    {'port': 4222, 'targetPort': 4222, 'name': 'client'},
                    {'port': _NATS_ROUTE_PORT, 'targetPort': _NATS_ROUTE_PORT, 'name': 'cluster'},
                    {'port': 8222, 'targetPort': 8222, 'name': 'monitor'},
                ],
            },
        })
    return manifests

def redis_manifests(namespace : str, profile : Optional[RedisProfile] = None) -> list:
    '''
    Single-replica Redis for the large-payload blob store.

    Dev profile (``None``): persistence off — it is transport, not storage.
    Memory is capped with volatile-lru eviction — every key videoflow writes
    carries a TTL (PROTOCOL.md BLOB-7), so under pressure Redis evicts our oldest
    blobs instead of growing until the node OOMs (the redis:7 default is
    unlimited memory with noeviction). The container limit sits above maxmemory
    to leave headroom for allocator fragmentation.

    Durable profile: an append-only file under ``/data`` on a
    PersistentVolumeClaim, ``noeviction`` so a full store refuses a write rather
    than dropping a blob, and a ``Recreate`` strategy, since a ReadWriteOnce
    claim cannot be held by the old and the new pod at once during a rollout.
    '''
    profile = profile or RedisProfile.dev()
    labels = _infra_labels('redis')
    args = ['--save', '', '--appendonly', 'yes' if profile.stateful else 'no',
            '--maxmemory', '4gb', '--maxmemory-policy', profile.eviction]
    container : dict = {
        'name': 'redis',
        'image': 'redis:7-alpine',
        'args': args,
        'ports': [{'containerPort': 6379, 'name': 'client'}],
        'resources': {
            'requests': {'memory': '512Mi', 'cpu': '100m'},
            'limits': {'memory': '5Gi'},
        },
    }
    pod_spec : dict = {'containers': [container]}
    deployment_spec : dict = {
        'replicas': 1,
        'selector': {'matchLabels': {'app': 'redis'}},
        'template': {'metadata': {'labels': labels}, 'spec': pod_spec},
    }
    manifests : list = []
    if profile.stateful:
        args += ['--dir', '/data']
        container['volumeMounts'] = [{'name': 'data', 'mountPath': '/data'}]
        pod_spec['volumes'] = [{'name': 'data',
                                'persistentVolumeClaim': {'claimName': REDIS_CLAIM}}]
        deployment_spec['strategy'] = {'type': 'Recreate'}
        claim_spec : dict = {
            'accessModes': ['ReadWriteOnce'],
            'resources': {'requests': {'storage': profile.storage_size}},
        }
        if profile.storage_class is not None:
            claim_spec['storageClassName'] = profile.storage_class
        manifests.append({
            'apiVersion': 'v1',
            'kind': 'PersistentVolumeClaim',
            'metadata': {'name': REDIS_CLAIM, 'namespace': namespace, 'labels': labels},
            'spec': claim_spec,
        })
    if profile.priority_class:
        pod_spec['priorityClassName'] = profile.priority_class
    manifests += [
        {
            'apiVersion': 'apps/v1',
            'kind': 'Deployment',
            'metadata': {'name': 'redis', 'namespace': namespace, 'labels': labels},
            'spec': deployment_spec,
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
    return manifests

def infra_urls(namespace : str) -> dict:
    '''The in-cluster URLs workers use once the dev infra is up.'''
    return {'nats': f'nats://{NATS_SERVICE}.{namespace}.svc:4222',
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

def ensure_infra(kubectl : str, namespace : str, need_redis : bool,
                 profile : Optional[BrokerProfile] = None,
                 redis_profile : Optional[RedisProfile] = None) -> tuple:
    '''
    Applies the NATS (and, when ``need_redis``, Redis) of the given profiles
    unless a Service of the same name already exists in the namespace
    (bring-your-own is reused, not owned).

    - Arguments:
        - profile: how to shape NATS; ``None`` is the dev profile.
        - redis_profile: how to shape Redis; ``None`` is the dev profile.

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
    if not service_exists(kubectl, namespace, NATS_SERVICE):
        to_apply += nats_manifests(namespace, profile)
        created.append('nats')
    if need_redis and not service_exists(kubectl, namespace, 'redis'):
        to_apply += redis_manifests(namespace, redis_profile)
        created.append('redis')
    if to_apply:
        proc = subprocess.run([kubectl, 'apply', '-n', namespace, '-f', '-'],
                              input = dump_manifests(to_apply).encode('utf-8'),
                              capture_output = True, check = False)
        if proc.returncode != 0:
            raise RuntimeError(f'could not provision {"/".join(created)}: '
                               f'{proc.stderr.decode("utf-8", "replace")}')
    return urls, created

def _workload_ref(component : str, profile : Optional[BrokerProfile]) -> str:
    '''``kind/name`` of a component's workload — the StatefulSet for a stateful NATS profile.'''
    if component == 'nats' and profile is not None and profile.stateful:
        return f'statefulset/{NATS_SERVICE}'
    return f'deployment/{component}'

def wait_infra_ready(kubectl : str, namespace : str, created : List[str],
                     timeout_secs : int = 120,
                     profile : Optional[BrokerProfile] = None) -> None:
    '''
    Blocks until each freshly created infra workload rolls out; raises on timeout.
    Pass the same ``profile`` as ``ensure_infra`` so a stateful NATS is awaited as
    the StatefulSet it is.
    '''
    for component in created:
        proc = subprocess.run(
            [kubectl, 'rollout', 'status', _workload_ref(component, profile), '-n', namespace,
             f'--timeout={timeout_secs}s'],
            check = False,
        )
        if proc.returncode != 0:
            raise RuntimeError(f'{component} did not become ready within {timeout_secs}s — '
                               f'check `kubectl get pods -n {namespace} -l app={component}`.')

def teardown_infra(kubectl : str, namespace : str, components : List[str],
                   profile : Optional[BrokerProfile] = None) -> None:
    '''
    Deletes the given auto-provisioned components by ownership label. Best-effort
    (never raises). A stateful ``profile`` adds the StatefulSet to the kinds
    deleted; the PersistentVolumeClaims either profile created are left in place
    (see the module docstring).
    '''
    if not components:
        return
    kinds = 'deployment,service,configmap'
    if profile is not None and profile.stateful:
        kinds = 'statefulset,' + kinds
    selector = f'{LABEL_INFRA} in ({",".join(components)})'
    subprocess.run(
        [kubectl, 'delete', '-n', namespace, kinds, '-l', selector],
        check = False,
    )
