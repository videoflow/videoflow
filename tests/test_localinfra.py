'''
Local dev-broker auto-provisioning: the reuse-if-already-listening ownership
rule, per-component docker argv, and the teardown scoped to what we started.

Pure/unit: subprocess and the port probe are monkeypatched — no docker, no NATS.
'''
import subprocess

import pytest

from videoflow import localinfra


class _Proc:
    def __init__(self, returncode = 0, stdout = b'', stderr = b''):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


def _fake_docker(monkeypatch, fail = False):
    calls = []
    def run(cmd, **kwargs):
        calls.append(cmd)
        if cmd[:2] == ['docker', 'info']:
            return _Proc(returncode = 1 if fail else 0)
        return _Proc(returncode = 1 if fail else 0, stderr = b'boom')
    monkeypatch.setattr(subprocess, 'run', run)
    return calls


def _listening(monkeypatch, nats = False, redis = False):
    monkeypatch.setattr(localinfra, 'nats_running', lambda url = None: nats)
    monkeypatch.setattr(localinfra, 'redis_running', lambda url = None: redis)


def test_reuses_broker_already_listening(monkeypatch):
    '''The ownership rule: a broker someone else started is used and never owned.'''
    calls = _fake_docker(monkeypatch)
    _listening(monkeypatch, nats = True, redis = True)
    urls, created = localinfra.ensure_local_infra(need_redis = True)
    assert created == []
    assert urls['nats'] == localinfra.DEFAULT_NATS_URL
    assert not [c for c in calls if c[:2] == ['docker', 'run']]


def test_starts_missing_components_with_labels_and_ports(monkeypatch):
    calls = _fake_docker(monkeypatch)
    _listening(monkeypatch, nats = False, redis = False)
    urls, created = localinfra.ensure_local_infra(need_redis = True)
    assert created == ['nats', 'redis']
    runs = [c for c in calls if c[:2] == ['docker', 'run']]
    assert len(runs) == 2
    nats_run, redis_run = runs
    assert '--label' in nats_run and f'{localinfra.LABEL_INFRA}=nats' in nats_run
    assert '4222:4222' in nats_run and '8222:8222' in nats_run
    assert localinfra.NATS_IMAGE in nats_run and '-js' in nats_run
    assert f'{localinfra.LABEL_INFRA}=redis' in redis_run
    assert '6379:6379' in redis_run and localinfra.REDIS_IMAGE in redis_run
    # Persistence off — the blob store is transport, not storage.
    assert '--appendonly' in redis_run and 'no' in redis_run
    assert urls['redis'] == localinfra.DEFAULT_REDIS_URL


def test_mixed_ownership_starts_only_what_is_missing(monkeypatch):
    calls = _fake_docker(monkeypatch)
    _listening(monkeypatch, nats = True, redis = False)
    _urls, created = localinfra.ensure_local_infra(need_redis = True)
    assert created == ['redis']
    runs = [c for c in calls if c[:2] == ['docker', 'run']]
    assert len(runs) == 1 and localinfra.REDIS_IMAGE in runs[0]


def test_skips_redis_when_not_needed(monkeypatch):
    _fake_docker(monkeypatch)
    _listening(monkeypatch, nats = False, redis = False)
    urls, created = localinfra.ensure_local_infra(need_redis = False)
    assert created == ['nats']
    assert urls['redis'] is None


def test_stale_container_removed_before_start(monkeypatch):
    calls = _fake_docker(monkeypatch)
    _listening(monkeypatch, nats = False, redis = False)
    localinfra.ensure_local_infra(need_redis = False)
    kinds = [c[:3] for c in calls if c[0] == 'docker' and c[1] in ('rm', 'run')]
    assert kinds[0] == ['docker', 'rm', '-f']
    assert kinds[1][:2] == ['docker', 'run']


def test_no_docker_and_nothing_listening_is_actionable(monkeypatch):
    _fake_docker(monkeypatch, fail = True)
    _listening(monkeypatch, nats = False, redis = False)
    with pytest.raises(RuntimeError) as exc:
        localinfra.ensure_local_infra(need_redis = False)
    message = str(exc.value)
    assert '--nats' in message and 'nats-server -js' in message


def test_teardown_scoped_to_created(monkeypatch):
    calls = _fake_docker(monkeypatch)
    localinfra.teardown_local_infra(['nats'])
    assert calls == [['docker', 'rm', '-f', localinfra.NATS_CONTAINER]]
    calls.clear()
    localinfra.teardown_local_infra([])          # nothing owned → no docker calls
    assert calls == []


def test_wait_ready_timeout_dumps_logs(monkeypatch):
    calls = _fake_docker(monkeypatch)
    monkeypatch.setattr(localinfra, 'port_open', lambda *a, **kw: False)
    with pytest.raises(RuntimeError, match = 'did not become ready'):
        localinfra.wait_local_infra_ready(['nats'], localinfra.local_infra_urls(), timeout_secs = 0)
    assert any(c[:2] == ['docker', 'logs'] for c in calls)


def test_port_probe_parses_custom_urls(monkeypatch):
    seen = []
    monkeypatch.setattr(localinfra, 'port_open',
                        lambda host, port, timeout = 1.0: seen.append((host, port)) or True)
    localinfra.nats_running('nats://broker.example:5222')
    localinfra.redis_running('redis://cache.example:6380/1')
    assert seen == [('broker.example', 5222), ('cache.example', 6380)]
