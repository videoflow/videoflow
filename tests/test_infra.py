'''
Dev-infra auto-provisioning: manifest shape + ownership labels, the
reuse-if-exists rule (a pre-existing Service is never owned), and the
label-scoped teardown selector.

Pure/unit: subprocess is monkeypatched — no cluster.
'''
import subprocess

from videoflow.deploy import infra
from videoflow.deploy.manifests import LABEL_MANAGED_BY


class _Proc:
    def __init__(self, returncode = 0, stdout = b'', stderr = b''):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def test_manifest_shapes_and_labels():
    nats = {(m['kind'], m['metadata']['name']): m for m in infra.nats_manifests('ns1')}
    assert set(nats) == {('ConfigMap', 'nats-config'), ('Deployment', 'nats'), ('Service', 'nats')}
    redis = {(m['kind'], m['metadata']['name']): m for m in infra.redis_manifests('ns1')}
    assert set(redis) == {('Deployment', 'redis'), ('Service', 'redis')}
    for m in list(nats.values()) + list(redis.values()):
        assert m['metadata']['namespace'] == 'ns1'
        assert m['metadata']['labels'][infra.LABEL_INFRA] in ('nats', 'redis')
        assert m['metadata']['labels'][LABEL_MANAGED_BY] == 'videoflow'
    # Redis runs as a cache/transport: persistence explicitly off, memory capped
    # with volatile-lru so a long run evicts TTL'd blobs instead of OOMing the node.
    container = redis[('Deployment', 'redis')]['spec']['template']['spec']['containers'][0]
    assert container['args'] == ['--save', '', '--appendonly', 'no',
                                 '--maxmemory', '4gb', '--maxmemory-policy', 'volatile-lru']
    # The container limit sits above maxmemory (fragmentation headroom); without a
    # limit there is no cgroup boundary and the *node* absorbs any overrun.
    assert container['resources']['limits']['memory'] == '5Gi'
    conf = nats[('ConfigMap', 'nats-config')]['data']['nats.conf']
    assert 'max_payload: 8MB' in conf and 'jetstream' in conf


def test_ensure_infra_applies_missing_components(monkeypatch):
    calls = []
    def run(cmd, **kwargs):
        calls.append((cmd, kwargs.get('input')))
        if cmd[:3] == ['kubectl', 'get', 'svc']:
            return _Proc(returncode = 1)          # neither service exists
        return _Proc()
    monkeypatch.setattr(subprocess, 'run', run)
    urls, created = infra.ensure_infra('kubectl', 'ns1', need_redis = True)
    assert created == ['nats', 'redis']
    assert urls == {'nats': 'nats://nats.ns1.svc:4222', 'redis': 'redis://redis.ns1.svc:6379/0'}
    applied = [inp for cmd, inp in calls if 'apply' in cmd][0].decode()
    assert 'kind: Deployment' in applied and 'name: redis' in applied and 'name: nats' in applied


def test_ensure_infra_reuses_existing_services(monkeypatch):
    def run(cmd, **kwargs):
        if cmd[:3] == ['kubectl', 'get', 'svc']:
            return _Proc(returncode = 0)          # both already exist
        raise AssertionError(f'unexpected call: {cmd}')
    monkeypatch.setattr(subprocess, 'run', run)
    urls, created = infra.ensure_infra('kubectl', 'ns1', need_redis = True)
    assert created == []                          # pre-existing infra is never owned
    assert urls['nats'] == 'nats://nats.ns1.svc:4222'


def test_ensure_infra_skips_redis_when_not_needed(monkeypatch):
    def run(cmd, **kwargs):
        if cmd[:3] == ['kubectl', 'get', 'svc']:
            return _Proc(returncode = 1)
        return _Proc()
    monkeypatch.setattr(subprocess, 'run', run)
    urls, created = infra.ensure_infra('kubectl', 'ns1', need_redis = False)
    assert created == ['nats']
    assert urls['redis'] is None


def test_teardown_scoped_to_created_components(monkeypatch):
    calls = []
    monkeypatch.setattr(subprocess, 'run', lambda cmd, **kw: calls.append(cmd) or _Proc())
    infra.teardown_infra('kubectl', 'ns1', ['nats'])
    assert calls == [['kubectl', 'delete', '-n', 'ns1', 'deployment,service,configmap',
                      '-l', 'videoflow.io/infra in (nats)']]
    calls.clear()
    infra.teardown_infra('kubectl', 'ns1', [])    # nothing owned → no kubectl call
    assert calls == []


# -- broker profiles -----------------------------------------------------------
#
# The dev profile is what every existing cluster runs and what the k8s tests
# deploy against, so its render is pinned byte-for-byte; the durable profile is
# checked for the shape that makes it durable — replicas, routes, a claim per pod,
# the file-store limit — rather than for every key.

import pytest  # noqa: E402

from videoflow.core.errors import ConfigError  # noqa: E402
from videoflow.deploy.broker_profiles import (  # noqa: E402
    BrokerProfile,
    RedisProfile,
    broker_profiles,
)
from videoflow.deploy.manifests import dump_manifests  # noqa: E402

_DEV_NATS_CONF = '''\
port: 4222
http: 8222
max_payload: 8MB
jetstream {
  store_dir: "/data/jetstream"
  max_memory_store: 1GB
  max_file_store: 10GB
}
'''


def test_dev_profile_is_the_default_and_renders_todays_manifests():
    for ns in ('ns1', 'videoflow-test'):
        assert dump_manifests(infra.nats_manifests(ns)) == \
            dump_manifests(infra.nats_manifests(ns, BrokerProfile.dev()))
        assert dump_manifests(infra.nats_manifests(ns)) == \
            dump_manifests(infra.nats_manifests(ns, BrokerProfile()))
        assert dump_manifests(infra.redis_manifests(ns)) == \
            dump_manifests(infra.redis_manifests(ns, RedisProfile.dev()))
    assert infra.nats_conf('ns1') == _DEV_NATS_CONF
    assert infra._NATS_CONF == _DEV_NATS_CONF
    dev = {(m['kind'], m['metadata']['name']): m for m in infra.nats_manifests('ns1')}
    assert set(dev) == {('ConfigMap', 'nats-config'), ('Deployment', 'nats'), ('Service', 'nats')}
    pod = dev[('Deployment', 'nats')]['spec']['template']['spec']
    assert 'priorityClassName' not in pod and pod['volumes'][1] == {'name': 'data', 'emptyDir': {}}
    assert not BrokerProfile.dev().stateful and not RedisProfile.dev().stateful


def test_durable_profile_renders_a_statefulset_with_routes_and_claims():
    profile = BrokerProfile.durable(replicas = 3, storage_class = 'local-path',
                                    priority_class = 'cluster-batch')
    assert profile.stateful and profile.persistence and profile.jetstream_replicas == 3
    nats = {(m['kind'], m['metadata']['name']): m for m in infra.nats_manifests('ns1', profile)}
    assert set(nats) == {('ConfigMap', 'nats-config'), ('StatefulSet', 'nats'),
                         ('Service', 'nats'), ('Service', 'nats-headless')}
    for m in nats.values():
        assert m['metadata']['namespace'] == 'ns1'
        assert m['metadata']['labels'][infra.LABEL_INFRA] == 'nats'
        assert m['metadata']['labels'][LABEL_MANAGED_BY] == 'videoflow'

    sts = nats[('StatefulSet', 'nats')]['spec']
    assert sts['replicas'] == 3
    assert sts['serviceName'] == 'nats-headless'
    assert sts['podManagementPolicy'] == 'Parallel'
    # One ReadWriteOnce claim per pod from the requested class, labelled so it can
    # be found (and reclaimed by hand) by the ownership label.
    [claim] = sts['volumeClaimTemplates']
    assert claim['metadata']['name'] == 'data'
    assert claim['metadata']['labels'][infra.LABEL_INFRA] == 'nats'
    assert claim['spec'] == {'accessModes': ['ReadWriteOnce'],
                             'resources': {'requests': {'storage': '10Gi'}},
                             'storageClassName': 'local-path'}
    pod = sts['template']['spec']
    assert pod['priorityClassName'] == 'cluster-batch'
    assert pod['volumes'] == [{'name': 'config', 'configMap': {'name': 'nats-config'}}]
    container = pod['containers'][0]
    assert container['env'] == [{'name': 'POD_NAME',
                                 'valueFrom': {'fieldRef': {'fieldPath': 'metadata.name'}}}]
    assert {p['name']: p['containerPort'] for p in container['ports']} == \
        {'client': 4222, 'monitor': 8222, 'cluster': 6222}
    assert [v['mountPath'] for v in container['volumeMounts']] == ['/etc/nats', '/data']

    # The config: a unique server name per pod, the file-store limit, and a
    # cluster block whose routes name every peer through the headless Service.
    conf = nats[('ConfigMap', 'nats-config')]['data']['nats.conf']
    assert conf.startswith('server_name: $POD_NAME\n')
    assert 'max_file_store: 10GB' in conf and 'max_payload: 8MB' in conf
    assert 'cluster {\n  name: videoflow\n  port: 6222\n' in conf
    for i in range(3):
        assert f'nats://nats-{i}.nats-headless.ns1.svc:6222' in conf
    assert 'nats-3.' not in conf

    headless = nats[('Service', 'nats-headless')]['spec']
    assert headless['clusterIP'] == 'None' and headless['publishNotReadyAddresses'] is True
    assert {p['name']: p['port'] for p in headless['ports']} == \
        {'client': 4222, 'cluster': 6222, 'monitor': 8222}
    # The client Service keeps its name and shape, so infra_urls and the reuse
    # rule are the same in every profile.
    assert nats[('Service', 'nats')]['spec'] == \
        {m['kind']: m for m in infra.nats_manifests('ns1')}['Service']['spec']


def test_durable_profile_without_persistence_keeps_emptydir_but_still_routes():
    profile = BrokerProfile(replicas = 3, jetstream_replicas = 3)
    sts = {m['kind']: m for m in infra.nats_manifests('ns1', profile)}['StatefulSet']['spec']
    assert 'volumeClaimTemplates' not in sts
    assert sts['template']['spec']['volumes'][1] == {'name': 'data', 'emptyDir': {}}
    assert 'routes' in infra.nats_conf('ns1', profile)


def test_single_persistent_replica_is_a_statefulset_without_a_cluster_block():
    profile = BrokerProfile(persistence = True, storage_class = None)
    docs = {m['kind']: m for m in infra.nats_manifests('ns1', profile)}
    assert 'StatefulSet' in docs and 'Deployment' not in docs
    # No storage class → the cluster default; kubectl explain pvc.spec:
    # storageClassName is optional.
    assert 'storageClassName' not in docs['StatefulSet']['spec']['volumeClaimTemplates'][0]['spec']
    conf = docs['ConfigMap']['data']['nats.conf']
    assert conf.startswith('server_name: $POD_NAME\n') and 'cluster {' not in conf


def test_durable_redis_is_append_only_on_a_claim_and_never_evicts():
    profile = RedisProfile.durable(storage_class = 'local-path', priority_class = 'cluster-batch')
    assert profile.stateful
    redis = {(m['kind'], m['metadata']['name']): m for m in infra.redis_manifests('ns1', profile)}
    assert set(redis) == {('PersistentVolumeClaim', 'redis-data'), ('Deployment', 'redis'),
                          ('Service', 'redis')}
    claim = redis[('PersistentVolumeClaim', 'redis-data')]
    assert claim['metadata']['labels'][infra.LABEL_INFRA] == 'redis'
    assert claim['spec'] == {'accessModes': ['ReadWriteOnce'],
                             'resources': {'requests': {'storage': '10Gi'}},
                             'storageClassName': 'local-path'}
    spec = redis[('Deployment', 'redis')]['spec']
    # A ReadWriteOnce claim cannot be held by the old and the new pod at once.
    assert spec['strategy'] == {'type': 'Recreate'}
    pod = spec['template']['spec']
    assert pod['priorityClassName'] == 'cluster-batch'
    assert pod['volumes'] == [{'name': 'data', 'persistentVolumeClaim': {'claimName': 'redis-data'}}]
    container = pod['containers'][0]
    assert container['args'] == ['--save', '', '--appendonly', 'yes', '--maxmemory', '4gb',
                                 '--maxmemory-policy', 'noeviction', '--dir', '/data']
    assert container['volumeMounts'] == [{'name': 'data', 'mountPath': '/data'}]


def test_profiles_reject_what_cannot_elect_or_persist():
    with pytest.raises(ConfigError, match = 'at least one'):
        BrokerProfile(replicas = 0)
    with pytest.raises(ConfigError, match = 'never more than there are servers'):
        BrokerProfile(replicas = 1, jetstream_replicas = 3)
    with pytest.raises(ConfigError, match = 'no majority quorum'):
        BrokerProfile.durable(replicas = 2)
    with pytest.raises(ConfigError, match = 'unknown Redis persistence mode'):
        RedisProfile(persistence = 'rdb')
    with pytest.raises(ConfigError, match = 'unknown Redis eviction policy'):
        RedisProfile(eviction = 'lru')
    # Every one of those carries a remedy, because the CLI renders it.
    for make in (lambda: BrokerProfile(replicas = 0), lambda: RedisProfile(persistence = 'rdb')):
        with pytest.raises(ConfigError) as info:
            make()
        assert info.value.remedy


def test_broker_profiles_lookup_mirrors_the_cli_flags():
    nats, redis = broker_profiles('dev', priority_class = 'p')
    assert nats == BrokerProfile.dev('p') and redis == RedisProfile.dev('p')
    nats, redis = broker_profiles('durable')
    assert nats == BrokerProfile.durable() and redis == RedisProfile.durable()
    nats, redis = broker_profiles('durable', replicas = 5, storage_class = 'nfs-shared')
    assert (nats.replicas, nats.jetstream_replicas, nats.storage_class) == (5, 3, 'nfs-shared')
    assert redis.storage_class == 'nfs-shared'
    with pytest.raises(ConfigError, match = 'only apply to the durable'):
        broker_profiles('dev', replicas = 3)
    with pytest.raises(ConfigError, match = 'unknown broker profile'):
        broker_profiles('prod')


def test_durable_profile_flows_through_ensure_wait_and_teardown(monkeypatch):
    profile = BrokerProfile.durable()
    calls = []

    def run(cmd, **kwargs):
        calls.append((cmd, kwargs.get('input')))
        if cmd[:3] == ['kubectl', 'get', 'svc']:
            return _Proc(returncode = 1)
        return _Proc()

    monkeypatch.setattr(subprocess, 'run', run)
    urls, created = infra.ensure_infra('kubectl', 'ns1', need_redis = True, profile = profile,
                                       redis_profile = RedisProfile.durable())
    assert created == ['nats', 'redis'] and urls['nats'] == 'nats://nats.ns1.svc:4222'
    applied = [inp for cmd, inp in calls if 'apply' in cmd][0].decode()
    assert 'kind: StatefulSet' in applied and 'kind: PersistentVolumeClaim' in applied
    assert 'kind: Deployment\nmetadata:\n  name: nats' not in applied

    calls.clear()
    infra.wait_infra_ready('kubectl', 'ns1', created, profile = profile)
    assert [cmd[3] for cmd, _ in calls] == ['statefulset/nats', 'deployment/redis']
    calls.clear()
    infra.wait_infra_ready('kubectl', 'ns1', created)          # dev: as before
    assert [cmd[3] for cmd, _ in calls] == ['deployment/nats', 'deployment/redis']

    calls.clear()
    infra.teardown_infra('kubectl', 'ns1', created, profile = profile)
    assert calls == [(['kubectl', 'delete', '-n', 'ns1', 'statefulset,deployment,service,configmap',
                       '-l', 'videoflow.io/infra in (nats,redis)'], None)]
