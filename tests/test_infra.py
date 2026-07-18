'''
Dev-infra auto-provisioning: manifest shape + ownership labels, the
reuse-if-exists rule (a pre-existing Service is never owned), and the
label-scoped teardown selector.

Pure/unit: subprocess is monkeypatched — no cluster.
'''
import subprocess

from videoflow import infra
from videoflow.manifests import LABEL_MANAGED_BY


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
    # Redis runs as a cache/transport: persistence explicitly off.
    args = redis[('Deployment', 'redis')]['spec']['template']['spec']['containers'][0]['args']
    assert args == ['--save', '', '--appendonly', 'no']
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
