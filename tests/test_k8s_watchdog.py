'''
The unschedulable-pod watchdog in the Kubernetes engine's wait paths.

Historically an unschedulable pod (e.g. ``Insufficient nvidia.com/gpu`` on an
under-provisioned cluster) never ran, never consumed its Job backoffLimit, and
left ``wait_for_completion`` — and the whole BATCH flow, via backpressure —
hanging forever. These tests pin the fail-fast behavior with a canned kubectl
and a fake clock; no cluster required.
'''
import pytest

from videoflow.engines import kubernetes as k8s_engine
from videoflow.engines.kubernetes import KubernetesExecutionEngine

INSUFFICIENT = '0/1 nodes are available: 1 Insufficient nvidia.com/gpu.'


class _Proc:
    def __init__(self, stdout = ''):
        self.stdout = stdout
        self.stderr = ''
        self.returncode = 0


class _FakeCluster:
    '''subprocess.run stand-in serving canned kubectl output keyed by verb.'''
    def __init__(self, jobs = '', pods = '', events = ''):
        self.jobs, self.pods, self.events = jobs, pods, events

    def __call__(self, cmd, **kwargs):
        joined = ' '.join(cmd)
        if 'get jobs' in joined:
            return _Proc(self.jobs)
        if 'get pods' in joined:
            return _Proc(self.pods)
        if 'get events' in joined:
            return _Proc(self.events)
        return _Proc()


class _FakeClock:
    def __init__(self):
        self.now = 0.0

    def time(self):
        return self.now

    def sleep(self, secs):
        # A zero poll interval must still advance the clock or the loop never
        # reaches the grace deadline.
        self.now += max(secs, 1.0)


@pytest.fixture
def engine(monkeypatch):
    clock = _FakeClock()
    monkeypatch.setattr(k8s_engine.time, 'time', clock.time)
    monkeypatch.setattr(k8s_engine.time, 'sleep', clock.sleep)
    eng = KubernetesExecutionEngine(nats_url = 'nats://x:4222', namespace = 'ns')
    eng._flow_id, eng._run_id = 'f', 'run1'
    return eng


def _install(monkeypatch, cluster):
    monkeypatch.setattr(k8s_engine.subprocess, 'run', cluster)


def test_wait_aborts_on_unschedulable_pod_after_grace(monkeypatch, engine):
    _install(monkeypatch, _FakeCluster(
        jobs = 'vf-f-g|||g\n',                                    # pending forever
        pods = f'vf-f-g-abc12|Pending|Unschedulable|{INSUFFICIENT}\n',
    ))
    with pytest.raises(RuntimeError) as e:
        engine.wait_for_completion(poll_secs = 0, unschedulable_grace_secs = 60)
    assert 'Insufficient nvidia.com/gpu' in str(e.value)
    assert 'cannot be scheduled' in str(e.value)
    # The abort names the remedies rather than just the symptom.
    assert '--gpu-mode shared' in str(e.value)


def test_wait_does_not_abort_before_grace_or_during_scaleup(monkeypatch, engine):
    # Same stuck pod, but a cluster-autoscaler scale-up is in flight: the watchdog
    # must keep waiting (the pod is expected to schedule once the node joins).
    # The job completing then ends the wait cleanly.
    cluster = _FakeCluster(
        jobs = 'vf-f-g|||g\n',
        pods = f'vf-f-g-abc12|Pending|Unschedulable|{INSUFFICIENT}\n',
        events = 'event/triggered-scale-up\n',
    )
    _install(monkeypatch, cluster)

    original_jobs = cluster.jobs
    def complete_later(cmd, **kwargs):
        if k8s_engine.time.time() > 200:
            cluster.jobs = 'vf-f-g|1||g\n'      # job succeeded
            cluster.pods = ''
        return _FakeCluster(cluster.jobs, cluster.pods, cluster.events)(cmd, **kwargs)
    monkeypatch.setattr(k8s_engine.subprocess, 'run', complete_later)

    assert engine.wait_for_completion(poll_secs = 0, unschedulable_grace_secs = 60) == []
    assert original_jobs != cluster.jobs  # sanity: the transition actually happened


def test_wait_still_fails_fast_on_job_failure(monkeypatch, engine):
    _install(monkeypatch, _FakeCluster(jobs = 'vf-f-g||1|g\n'))
    assert engine.wait_for_completion(poll_secs = 0) == ['g']


def test_schedulability_report_for_realtime(monkeypatch, engine):
    _install(monkeypatch, _FakeCluster(
        pods = f'vf-f-g-abc12|Pending|Unschedulable|{INSUFFICIENT}\n',
    ))
    problems = engine.schedulability_report(grace_secs = 10, poll_secs = 0)
    assert len(problems) == 1
    assert 'vf-f-g-abc12' in problems[0]
    assert 'Insufficient nvidia.com/gpu' in problems[0]
    # A clean run reports nothing.
    _install(monkeypatch, _FakeCluster(pods = 'vf-f-g-abc12|Running||\n'))
    assert engine.schedulability_report(grace_secs = 10, poll_secs = 0) == []


def test_schedulability_report_is_not_fooled_by_absent_pods(monkeypatch, engine):
    # Right after apply the controllers may not have created pods yet; an empty
    # snapshot must not count as success (the original first-poll race).
    _install(monkeypatch, _FakeCluster(pods = ''))
    problems = engine.schedulability_report(grace_secs = 5, poll_secs = 0)
    assert len(problems) == 1
    assert 'no pods appeared' in problems[0]

    # Pods appearing late and unschedulable are still caught.
    cluster = _FakeCluster(pods = '')
    def pods_appear_late(cmd, **kwargs):
        if k8s_engine.time.time() > 2:
            cluster.pods = f'vf-f-g-abc12|Pending|Unschedulable|{INSUFFICIENT}\n'
        return cluster(cmd, **kwargs)
    monkeypatch.setattr(k8s_engine.subprocess, 'run', pods_appear_late)
    problems = engine.schedulability_report(grace_secs = 10, poll_secs = 0)
    assert len(problems) == 1
    assert 'Insufficient nvidia.com/gpu' in problems[0]


def test_pod_states_parses_message_with_pipes(monkeypatch, engine):
    # The condition message is split with maxsplit so a '|' inside it survives.
    _install(monkeypatch, _FakeCluster(pods = 'p|Pending|Unschedulable|a|b|c\n'))
    assert engine._pod_states('x') == [('p', 'Pending', 'Unschedulable', 'a|b|c')]
