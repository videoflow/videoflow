'''
The pod watchdogs in the Kubernetes engine's wait paths.

Historically an unschedulable pod (e.g. ``Insufficient nvidia.com/gpu`` on an
under-provisioned cluster) never ran, never consumed its Job backoffLimit, and
left ``wait_for_completion`` — and the whole BATCH flow, via backpressure —
hanging forever. And a REALTIME pod that scheduled but then crash-looped
(CUDA OOM in open(), a startup-probe kill of a slow model load, a bad image)
was reported as a clean deploy: ``rollout_report``'s predecessor only looked
for Unschedulable. These tests pin the fail-fast behavior of both watchdogs
with a canned kubectl and a fake clock; no cluster required.
'''
import pytest

from videoflow.core.errors import EXIT_FLOW_STALLED, FlowStalled
from videoflow.engines import kubernetes as k8s_engine
from videoflow.engines.kubernetes import KubernetesExecutionEngine

INSUFFICIENT = '0/1 nodes are available: 1 Insufficient nvidia.com/gpu.'


class _Proc:
    def __init__(self, stdout = ''):
        self.stdout = stdout
        self.stderr = ''
        self.returncode = 0


class _FakeCluster:
    '''
    subprocess.run stand-in serving canned kubectl output keyed by verb.
    ``pods`` is the ``_pod_states`` jsonpath view; ``container_pods`` the
    ``_container_states`` one — the two queries are told apart by whether the
    jsonpath mentions containerStatuses.
    '''
    def __init__(self, jobs = '', pods = '', events = '', container_pods = ''):
        self.jobs, self.pods, self.events = jobs, pods, events
        self.container_pods = container_pods

    def __call__(self, cmd, **kwargs):
        joined = ' '.join(cmd)
        if 'get jobs' in joined:
            return _Proc(self.jobs)
        if 'get pods' in joined:
            if 'containerStatuses' in joined:
                return _Proc(self.container_pods)
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
    with pytest.raises(FlowStalled) as e:
        engine.wait_for_completion(poll_secs = 0, unschedulable_grace_secs = 60)
    assert 'Insufficient nvidia.com/gpu' in str(e.value)
    assert 'cannot be scheduled' in str(e.value)
    # The abort names the remedies rather than just the symptom.
    assert 'time-slicing' in str(e.value)
    # A stall gets its own exit code so CI can tell it from a flow that ran and failed.
    assert e.value.exit_code == EXIT_FLOW_STALLED


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
            cluster.jobs = 'vf-f-g|True||g\n'   # job Complete
            cluster.pods = ''
        return _FakeCluster(cluster.jobs, cluster.pods, cluster.events)(cmd, **kwargs)
    monkeypatch.setattr(k8s_engine.subprocess, 'run', complete_later)

    assert engine.wait_for_completion(poll_secs = 0, unschedulable_grace_secs = 60) == []
    assert original_jobs != cluster.jobs  # sanity: the transition actually happened


def test_wait_still_fails_fast_on_job_failure(monkeypatch, engine):
    _install(monkeypatch, _FakeCluster(jobs = 'vf-f-g||True|g\n'))
    assert engine.wait_for_completion(poll_secs = 0) == ['g']


def test_a_retrying_job_is_not_reported_as_failed(monkeypatch, engine):
    # A node Job renders with restartPolicy Never and backoffLimit = max_restarts,
    # so the controller sets .status.failed = 1 on the FIRST pod failure and then
    # creates a replacement pod. Reading that counter called a node dead while it
    # was still retrying — which is exactly the recoverable crash that a worker
    # restart is supposed to absorb (solutions/toy_recovery is built on it). Only
    # the terminal Failed condition counts.
    cluster = _FakeCluster(jobs = 'vf-f-g||\n')      # a pod died; no condition yet
    _install(monkeypatch, cluster)

    def succeeds_on_retry(cmd, **kwargs):
        if k8s_engine.time.time() > 20:
            cluster.jobs = 'vf-f-g|True||g\n'
        return cluster(cmd, **kwargs)
    monkeypatch.setattr(k8s_engine.subprocess, 'run', succeeds_on_retry)

    assert engine.wait_for_completion(poll_secs = 0) == []


def test_a_provision_job_retry_does_not_abort_the_deploy(monkeypatch, engine):
    # Same trap on the other wait path: the provision Job is OnFailure with
    # backoffLimit 6, so a single failed attempt must not be read as "the broker
    # streams were not created".
    cluster = _FakeCluster(jobs = 'vf-f-provision||\n')
    _install(monkeypatch, cluster)

    def succeeds_on_retry(cmd, **kwargs):
        if k8s_engine.time.time() > 20:
            cluster.jobs = 'vf-f-provision|True||\n'
        return cluster(cmd, **kwargs)
    monkeypatch.setattr(k8s_engine.subprocess, 'run', succeeds_on_retry)

    engine._wait_provision('f')       # returns rather than raising BrokerUnavailable


# Scheduler-clean _pod_states view for a pod that found a node.
CLEAN_SCHED = 'vf-f-g-abc12|Running||\n'


def test_rollout_report_flags_crashloop(monkeypatch, engine):
    # The pod scheduled fine — the scheduler view is clean — but the container
    # crash-loops. The old schedulability check called this a healthy deploy.
    _install(monkeypatch, _FakeCluster(
        pods = CLEAN_SCHED,
        container_pods = 'vf-f-g-abc12|g|Running|false|3|CrashLoopBackOff|Error\n',
    ))
    report = engine.rollout_report(deadline_secs = 200, poll_secs = 0)
    assert [node for node, _ in report.failing] == ['g']
    assert 'CrashLoopBackOff' in report.failing[0][1]
    assert 'startup-probe' in report.failing[0][1]
    # Confirmed on the second poll — nowhere near the 200s deadline.
    assert k8s_engine.time.time() < 10


def test_rollout_report_names_oomkill(monkeypatch, engine):
    _install(monkeypatch, _FakeCluster(
        pods = CLEAN_SCHED,
        container_pods = 'vf-f-g-abc12|g|Running|false|3|CrashLoopBackOff|OOMKilled\n',
    ))
    report = engine.rollout_report(deadline_secs = 200, poll_secs = 0)
    assert 'OOM-killed' in report.failing[0][1]
    assert 'memory' in report.failing[0][1]


def test_rollout_report_all_ready_early_exit(monkeypatch, engine):
    _install(monkeypatch, _FakeCluster(
        pods = CLEAN_SCHED,
        container_pods = 'vf-f-g-abc12|g|Running|true|0||\n',
    ))
    report = engine.rollout_report(deadline_secs = 200, poll_secs = 0)
    assert report.failing == [] and report.warnings == []
    assert k8s_engine.time.time() < 10       # two clean polls, not the deadline


def test_rollout_report_tolerates_single_restart(monkeypatch, engine):
    # One rocky-startup restart that recovered must not fail the deploy.
    _install(monkeypatch, _FakeCluster(
        pods = CLEAN_SCHED,
        container_pods = 'vf-f-g-abc12|g|Running|true|1||\n',
    ))
    report = engine.rollout_report(deadline_secs = 200, poll_secs = 0)
    assert report.failing == [] and report.warnings == []


def test_rollout_report_succeeded_producer_is_clean(monkeypatch, engine):
    # A REALTIME finite-producer Job pod finishes and goes Succeeded (never
    # Ready again) — it must count as satisfied, not hold the report open.
    _install(monkeypatch, _FakeCluster(
        pods = 'vf-f-prod-9zzz9|Succeeded||\n' + CLEAN_SCHED,
        container_pods = ('vf-f-prod-9zzz9|prod|Succeeded|false|0||Completed\n'
                          'vf-f-g-abc12|g|Running|true|0||\n'),
    ))
    report = engine.rollout_report(deadline_secs = 200, poll_secs = 0)
    assert report.failing == [] and report.warnings == []


def test_rollout_report_transient_reason_needs_two_polls(monkeypatch, engine):
    # A momentary ImagePullBackOff that resolves on the next poll (registry
    # blip) must not abort the deploy: failure needs two consecutive polls.
    cluster = _FakeCluster(
        pods = CLEAN_SCHED,
        container_pods = 'vf-f-g-abc12|g|Pending|false|0|ImagePullBackOff|\n',
    )
    def recovers(cmd, **kwargs):
        if k8s_engine.time.time() >= 1:
            cluster.container_pods = 'vf-f-g-abc12|g|Running|true|0||\n'
        return cluster(cmd, **kwargs)
    monkeypatch.setattr(k8s_engine.subprocess, 'run', recovers)
    report = engine.rollout_report(deadline_secs = 200, poll_secs = 0)
    assert report.failing == [] and report.warnings == []


def test_rollout_report_not_ready_by_deadline_warns(monkeypatch, engine):
    # Never failing, never Ready (e.g. a very slow model load still inside the
    # startup window at the deadline): a warning, not a failure.
    _install(monkeypatch, _FakeCluster(
        pods = CLEAN_SCHED,
        container_pods = 'vf-f-g-abc12|g|Pending|false|0|ContainerCreating|\n',
    ))
    report = engine.rollout_report(deadline_secs = 5, poll_secs = 0)
    assert report.failing == []
    assert len(report.warnings) == 1
    assert 'vf-f-g-abc12' in report.warnings[0]
    assert 'ContainerCreating' in report.warnings[0]


def test_rollout_report_flags_unschedulable_after_grace(monkeypatch, engine):
    # Unschedulable is now a confirmed failure (it was a warning): the pod never
    # starts, so the flow silently stalls exactly like a crash-loop.
    _install(monkeypatch, _FakeCluster(
        pods = f'vf-f-g-abc12|Pending|Unschedulable|{INSUFFICIENT}\n',
        container_pods = 'vf-f-g-abc12|g|Pending|false|0||\n',
    ))
    report = engine.rollout_report(deadline_secs = 30, poll_secs = 0,
                                   unschedulable_grace_secs = 5)
    assert [node for node, _ in report.failing] == ['g']
    assert 'Insufficient nvidia.com/gpu' in report.failing[0][1]


def test_rollout_report_unschedulable_during_scaleup_stays_warning(monkeypatch, engine):
    # Same stuck pod, but a cluster-autoscaler scale-up is in flight: expected
    # to schedule once the node joins, so the deploy must not exit non-zero.
    _install(monkeypatch, _FakeCluster(
        pods = f'vf-f-g-abc12|Pending|Unschedulable|{INSUFFICIENT}\n',
        container_pods = 'vf-f-g-abc12|g|Pending|false|0||\n',
        events = 'event/triggered-scale-up\n',
    ))
    report = engine.rollout_report(deadline_secs = 10, poll_secs = 0,
                                   unschedulable_grace_secs = 5)
    assert report.failing == []
    assert any('scale-up' in w for w in report.warnings)


def test_rollout_report_is_not_fooled_by_absent_pods(monkeypatch, engine):
    # Right after apply the controllers may not have created pods yet; an empty
    # snapshot must not count as success (the original first-poll race).
    _install(monkeypatch, _FakeCluster())
    report = engine.rollout_report(deadline_secs = 5, poll_secs = 0)
    assert report.failing == []
    assert len(report.warnings) == 1
    assert 'no pods appeared' in report.warnings[0]

    # Pods appearing late and unschedulable are still caught.
    cluster = _FakeCluster()
    def pods_appear_late(cmd, **kwargs):
        if k8s_engine.time.time() > 2:
            cluster.pods = f'vf-f-g-abc12|Pending|Unschedulable|{INSUFFICIENT}\n'
            cluster.container_pods = 'vf-f-g-abc12|g|Pending|false|0||\n'
        return cluster(cmd, **kwargs)
    monkeypatch.setattr(k8s_engine.subprocess, 'run', pods_appear_late)
    report = engine.rollout_report(deadline_secs = 30, poll_secs = 0,
                                   unschedulable_grace_secs = 5)
    assert 'Insufficient nvidia.com/gpu' in report.failing[0][1]


def test_container_states_parses_missing_statuses(monkeypatch, engine):
    # A not-yet-scheduled pod has no containerStatuses; the empty jsonpath
    # fields must parse to safe defaults rather than raise.
    _install(monkeypatch, _FakeCluster(container_pods = 'p|g|Pending||||\n'))
    (state,) = engine._container_states('x')
    assert (state.ready, state.restart_count, state.waiting_reason) == (False, 0, '')


def test_pod_states_parses_message_with_pipes(monkeypatch, engine):
    # The condition message is split with maxsplit so a '|' inside it survives.
    _install(monkeypatch, _FakeCluster(pods = 'p|Pending|Unschedulable|a|b|c\n'))
    assert engine._pod_states('x') == [('p', 'Pending', 'Unschedulable', 'a|b|c')]
