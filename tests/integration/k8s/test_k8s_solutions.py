'''
The toy solutions again — this time as pods on a kind cluster.

Every config dict and every assertion in this module is imported verbatim from
``support_solutions.py``, the same ones ``local/test_toy_solutions.py`` uses. That
is the whole design: the framework's central promise is that a graph built on one
machine runs unchanged on many, so the two engines must produce the same answer
from the same input. A solution that passes locally and fails here is a framework
bug, and the diff between the two files is the list of things that legitimately
differ — an image, a namespace, a flow id.

What this adds over the local run, beyond "it works in a cluster":

- The compiled node parameters travel through a ConfigMap into a pod that has
  never seen the graph module, and rebuild there.
- ``prepare.py`` runs in a container and its output has to be visible to a
  different container later — the ground truth ``toy_calculator`` compares against.
- The work dir is a hostPath mount, so the artifacts these tests read were written
  by a pod, on a node, and arrived back on the host.
- Worker restart is the kubelet's and the Job controller's, not a supervisor
  thread's. ``toy_recovery`` is the same claim about a different mechanism.

``toy_fusion`` is the one that cannot be a straight parity run — see its test.
'''
from __future__ import absolute_import, division, print_function

import pytest
from support_k8s import (
    SOLUTION_IMAGES,
    assert_ok,
    delete_dlq,
    deploy_solution,
    dlq_entries,
    resources_for,
    teardown,
    videoflow,
    wait_for,
)
from support_solutions import (
    K8S_FUSION_CONFIG,
    SOLUTION_CONFIGS,
    SOLUTIONS_DIR,
    assert_calculator_report,
    assert_fusion_latest,
    assert_recovery_report,
    assert_router_counts,
    read_artifact,
    stage_solution,
)

# Bounds a hang. A BATCH solution takes 60-120s here, dominated by image pulls,
# pod scheduling and the provisioner's poll interval rather than by the flow.
pytestmark = pytest.mark.timeout(900)

def run_solution(work_root, name, flow_ids, namespace, config = None):
    '''
    Stages solution ``name``, deploys it, and returns its work_dir.

    ``with_template = True`` is not optional: ``_cmd_deploy`` only collects the
    hostPath mounts a solution declares in ``x-mounts`` when it can find
    ``config.template.yaml`` next to the graph. Without it the deploy renders no
    volume at all, every pod writes its artifacts into a container-local ``out/``,
    and the test fails with a missing file for a reason nowhere near the cause.
    (It is safe to copy: ``ensure_config`` returns the config.yaml written here
    before it ever reads the template.)
    '''
    flow_id, run_id = flow_ids
    work = stage_solution(work_root / flow_id, name,
                          config or SOLUTION_CONFIGS[name], with_template = True)
    proc = deploy_solution(work, name, namespace, flow_id, run_id, SOLUTION_IMAGES[name])
    assert_ok(proc, f'deploy {name}')
    assert 'completed.' in proc.stdout, proc.stdout
    return work / 'out'

def test_toy_calculator(k8s_work_root, k8s_namespace, flow_ids):
    '''
    BATCH diamond: fan-out, a trace join re-aligning two branches, competing
    replicas, stateful aggregation and a two-parent consumer — as six Jobs.

    Also the narrowest test of the prepare hook: ``matches_expected`` compares the
    flow's statistics against ground truth ``prepare.py`` wrote before the run, in
    a container, into the mounted work dir. If the mount were wrong in either
    direction this assertion is the one that notices.
    '''
    work_dir = run_solution(k8s_work_root, 'toy_calculator', flow_ids, k8s_namespace)
    assert_calculator_report(read_artifact(work_dir, 'report.json'))
    # A BATCH deploy tears itself down when the flow completes.
    assert resources_for(k8s_namespace, flow_ids[0]) == []

def test_toy_router(k8s_work_root, k8s_namespace, flow_ids):
    '''
    Partitioned routing across three replicas — which on Kubernetes is a
    StatefulSet-shaped claim rather than a process-shaped one: ``sticky`` says each
    sensor was owned by exactly one replica, and replica identity here comes from
    the pod's own name via the downward API, not from a constructor argument.
    '''
    work_dir = run_solution(k8s_work_root, 'toy_router', flow_ids, k8s_namespace)
    assert_router_counts(read_artifact(work_dir, 'counts.json'))

def test_toy_recovery(k8s_work_root, k8s_namespace, k8s_nats_url, flow_ids):
    '''
    The error taxonomy, enforced by Kubernetes rather than by a supervisor thread.

    Locally a worker-fatal error stops a subprocess and the engine's supervisor
    starts another. Here the pod exits non-zero, the Job controller notices, and
    the replacement pod picks the un-acked message back up off the same durable.
    Same conservation claim, entirely different machinery — and this is the test
    that catches the engine reading a retrying Job as a dead one, which would fail
    the whole deploy with exit code 4 instead of recovering.
    '''
    try:
        work_dir = run_solution(k8s_work_root, 'toy_recovery', flow_ids, k8s_namespace)
        assert_recovery_report(read_artifact(work_dir, 'recovery_report.json'))
    finally:
        # The DLQ outlives the run by design, so it is the test's to clean up.
        delete_dlq(k8s_nats_url, flow_ids[0])

def test_the_poison_messages_are_in_the_dead_letter_queue(
        k8s_work_root, k8s_namespace, k8s_nats_url, flow_ids):
    '''
    The other half of the recovery claim, read from the broker instead of from the
    solution's own ledger: the two unusable events are *in* the DLQ, attributed to
    the node that rejected them.

    Readable after the run because the dead-letter stream is flow-scoped and is the
    one thing ``delete_run_streams`` deliberately spares — a run's evidence is
    wanted precisely when the run is over.
    '''
    flow_id, run_id = flow_ids
    try:
        run_solution(k8s_work_root, 'toy_recovery', flow_ids, k8s_namespace)
        entries = dlq_entries(k8s_nats_url, flow_id, run_id = run_id)
        assert len(entries) == 2, entries
        assert {e['headers'].get('VF-Origin-Node') for e in entries} == {'fragile'}, entries
        assert all(e['headers'].get('VF-Disposition') == 'poison' for e in entries), entries
        # The remedy travels with the dead letter: the DLQ inspector renders it.
        assert all(e['headers'].get('VF-Remedy') for e in entries), entries
    finally:
        delete_dlq(k8s_nats_url, flow_id)

def test_toy_fusion_rolls_out_and_fuses(k8s_work_root, k8s_namespace, k8s_nats_url, flow_ids):
    '''
    REALTIME fusion of independent producers by event time.

    Two things differ from the local run, both forced by what REALTIME *means* on a
    cluster. ``duration_s`` is 0, because a bounded producer renders as a Job, the
    consumer Deployment reaches end-of-stream and exits, and the kubelet restarts
    it — which the rollout check correctly calls a failing deploy. And the artifact
    asserted is ``latest.json``, rewritten on every fused moment, rather than
    ``fusion_summary.json``, which is only written from ``close()``: nothing here
    ever closes, and teardown deletes the pods rather than draining them.

    A REALTIME deploy returns as soon as the rollout looks healthy and leaves
    everything running, so the teardown is this test's job.
    '''
    flow_id, run_id = flow_ids
    work_dir = None
    try:
        work = stage_solution(k8s_work_root / flow_id, 'toy_fusion', K8S_FUSION_CONFIG,
                              with_template = True)
        proc = deploy_solution(work, 'toy_fusion', k8s_namespace, flow_id, run_id,
                               SOLUTION_IMAGES['toy_fusion'])
        assert_ok(proc, 'deploy toy_fusion')
        assert 'REALTIME flow is running' in proc.stdout, proc.stdout

        work_dir = work / 'out'
        latest = wait_for(lambda: (work_dir / 'latest.json').is_file(), timeout = 90)
        assert latest, (f'no fused moment reached the host within 90s; work_dir holds: '
                        f'{sorted(p.name for p in work_dir.iterdir())}')
        assert_fusion_latest(read_artifact(work_dir, 'latest.json'))
    finally:
        teardown(k8s_namespace, flow_id, run_id, k8s_nats_url)

    assert resources_for(k8s_namespace, flow_id) == []

def test_a_solution_image_builds_from_its_own_dockerfile(k8s_work_root, flow_ids, tmp_path):
    '''
    The one path the tests above deliberately skip: letting ``deploy`` build the
    image instead of handing it one.

    They pass ``--image`` because the build context is resolved with ``git
    rev-parse`` from the graph directory, and a staged copy is not a checkout — so
    an autobuild there would silently build from the *repo's* files and prove
    nothing about the copy under test. Here the context is explicit, and
    ``--render-only`` returns after the build, the prepare hook and the compile but
    before anything touches the cluster, which makes this cheap enough to keep.
    '''
    flow_id, run_id = flow_ids
    work = stage_solution(k8s_work_root / flow_id, 'toy_calculator',
                          SOLUTION_CONFIGS['toy_calculator'],
                          with_template = True, with_dockerfile = True)
    proc = videoflow(['deploy', str(work / 'toy_calculator.py'),
                      '--flow-id', flow_id, '--run-id', run_id,
                      '--build-context', str(SOLUTIONS_DIR.parent),
                      '--render-only', '--output', str(tmp_path / 'manifests'),
                      '--non-interactive'], cwd = work)
    assert_ok(proc, 'deploy --render-only')
    rendered = sorted(p.name for p in (tmp_path / 'manifests').iterdir())
    assert rendered, 'nothing was rendered'

if __name__ == '__main__':
    pytest.main([__file__])
