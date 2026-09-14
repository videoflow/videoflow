'''
BATCH flows render every node as a Job so the whole flow runs to completion and
gets torn down, instead of processors/consumers becoming long-running Deployments
that CrashLoopBackOff when they exit on end-of-stream. Also covers the run-id
labelling, provision/worker phase split, and the CRD-safe teardown kind list.

Pure/unit: only inspects rendered manifest dicts (no cluster, no broker).
'''
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.compiler import compile_flow
from videoflow.core.constants import BATCH, REALTIME
from videoflow.deploy.manifests import (
    _CORE_DELETABLE_KINDS,
    _CRD_DELETABLE_KINDS,
    LABEL_RUN_ID,
    render_manifests,
    split_provision_manifests,
)
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer

IMG = 'ghcr.io/acme/app:v1'


def _flow(flow_type, partitioned = False, competing = False):
    p = IntProducer(0, 5, name = 'producer')
    if partitioned:
        a = IdentityProcessor(name = 'work', nb_tasks = 3, partition_by = 'trace_id')(p)
    elif competing:
        a = IdentityProcessor(name = 'work', nb_tasks = 3)(p)
    else:
        a = IdentityProcessor(name = 'work')(p)
    out = CommandlineConsumer(name = 'printer')(a)
    return Flow([out], flow_type = flow_type, flow_id = 'demo')


def _by_kind(flow_type, **kw):
    manifests = render_manifests(compile_flow(_flow(flow_type, **kw)), 'demo', flow_type,
                                 'nats://x:4222', 'run1', default_image = IMG)
    return manifests, {(m['kind'], m['metadata']['name']): m for m in manifests}


def test_batch_renders_every_node_as_a_completing_job():
    _, by = _by_kind(BATCH)
    # producer, work, printer are ALL Jobs (not Deployments) in a batch flow.
    for node in ('producer', 'work', 'printer'):
        job = by.get(('Job', f'vf-demo-run1-{node}'))
        assert job is not None, f'{node} should be a Job in a BATCH flow'
        # 'Never', not 'OnFailure': an OnFailure Job deletes its failed pod when the
        # backoffLimit is exhausted, destroying the logs dump_failed_logs prints.
        assert job['spec']['template']['spec']['restartPolicy'] == 'Never'
        assert job['spec']['ttlSecondsAfterFinished'] > 0
    # No Deployments/StatefulSets remain to CrashLoopBackOff.
    assert not any(kind in ('Deployment', 'StatefulSet') for kind, _ in by)


def test_realtime_keeps_deployments_and_finite_producer_job():
    _, by = _by_kind(REALTIME)
    assert ('Job', 'vf-demo-run1-producer') in by          # finite producer → Job
    assert ('Deployment', 'vf-demo-run1-work') in by       # processor stays up
    assert ('Deployment', 'vf-demo-run1-printer') in by    # consumer stays up
    assert 'ttlSecondsAfterFinished' not in by[('Job', 'vf-demo-run1-producer')]['spec']


def test_partitioned_batch_node_is_an_indexed_job_with_replica_id():
    _, by = _by_kind(BATCH, partitioned = True)
    job = by[('Job', 'vf-demo-run1-work')]
    assert job['spec']['completionMode'] == 'Indexed'
    assert job['spec']['completions'] == 3
    assert job['spec']['parallelism'] == 3
    env = job['spec']['template']['spec']['containers'][0]['env']
    names = {e['name'] for e in env}
    assert 'VF_REPLICA_ID' in names                   # from the completion index
    # REALTIME partitioned node keeps the StatefulSet + POD_NAME ordinal path.
    _, rt = _by_kind(REALTIME, partitioned = True)
    assert ('StatefulSet', 'vf-demo-run1-work') in rt


def test_competing_batch_replicas_are_an_indexed_job_too():
    # EOS-7 records terminators (and the partition lease) per replica, so two
    # competing pods must not both call themselves replica 0: the completion
    # index is the id, and a retried index resumes that replica's ledger.
    _, by = _by_kind(BATCH, competing = True)
    job = by[('Job', 'vf-demo-run1-work')]
    assert job['spec']['completionMode'] == 'Indexed'
    assert job['spec']['completions'] == 3 and job['spec']['parallelism'] == 3
    env = {e['name']: e for e in job['spec']['template']['spec']['containers'][0]['env']}
    assert env['VF_REPLICA_ID']['valueFrom']['fieldRef']['fieldPath'] == \
        "metadata.annotations['batch.kubernetes.io/job-completion-index']"
    # A single-replica Job stays NonIndexed, with no replica env at all.
    _, single = _by_kind(BATCH)
    assert 'completionMode' not in single[('Job', 'vf-demo-run1-work')]['spec']
    assert not single[('Job', 'vf-demo-run1-work')]['spec']['template']['spec']['containers'][0].get('env')
    # A competing REALTIME node is a Deployment whose pods carry neither: they
    # claim a replica slot through the run ledger at start (ENV-5 step 3) —
    # among nb_tasks slots, or up to the scaler's ceiling when one drives the node.
    _, rt = _by_kind(REALTIME, competing = True)
    dep = rt[('Deployment', 'vf-demo-run1-work')]
    assert dep['spec']['replicas'] == 3
    assert not dep['spec']['template']['spec']['containers'][0].get('env')
    assert 'VF_REPLICA_SLOTS' not in rt[('ConfigMap', 'vf-demo-run1-work-env')]['data']
    scaled = render_manifests(compile_flow(_flow(REALTIME, competing = True)), 'demo', REALTIME, 'nats://x:4222',
                              'run1', default_image = IMG, autoscaling = True, max_replicas = 8)
    by_scaled = {(m['kind'], m['metadata']['name']): m for m in scaled}
    assert ('ScaledObject', 'vf-demo-run1-work-scaler') in by_scaled
    assert by_scaled[('ConfigMap', 'vf-demo-run1-work-env')]['data']['VF_REPLICA_SLOTS'] == '8'
    assert 'VF_REPLICA_SLOTS' not in by_scaled[('ConfigMap', 'vf-demo-run1-printer-env')]['data']


def test_every_resource_is_run_scoped_by_label():
    manifests, _ = _by_kind(BATCH)
    for m in manifests:
        assert m['metadata']['labels'].get(LABEL_RUN_ID) == 'run1'


def test_split_provision_isolates_broker_from_workers():
    manifests, _ = _by_kind(BATCH)
    phases = split_provision_manifests(manifests, 'demo', 'run1')
    p1 = {m['metadata']['name'] for m in phases.provision}
    assert p1 == {'vf-demo-run1-broker', 'vf-demo-run1-specs', 'vf-demo-netpol', 'vf-demo-run1-provision'}
    # No worker Job leaks into the provision phase.
    assert not any(m['kind'] == 'Job' and m['metadata']['name'] != 'vf-demo-run1-provision'
                   for m in phases.provision)
    assert any(m['metadata']['name'] == 'vf-demo-run1-work' for m in phases.worker)
    # Still a plain 2-tuple underneath, so positional unpacking keeps working.
    phase1, phase2 = split_provision_manifests(manifests, 'demo', 'run1')
    assert (phase1, phase2) == (phases.provision, phases.worker)


def test_batch_jobs_carry_the_outer_stall_backstop():
    '''
    A BATCH pod is a Job and Job pods have their probes stripped, so
    activeDeadlineSeconds is the last line against a run that would otherwise hang
    forever. The worker's own progress deadline is the precise instrument; this is
    the blunt one behind it.
    '''
    _, by = _by_kind(BATCH)
    job = by[('Job', 'vf-demo-run1-work')]
    assert job['spec']['activeDeadlineSeconds'] > 0
    # A REALTIME finite-producer Job completes on its own schedule and gets none.
    _, rt = _by_kind(REALTIME)
    assert 'activeDeadlineSeconds' not in rt[('Job', 'vf-demo-run1-producer')]['spec']


def test_the_backoff_limit_comes_from_the_supervision_policy():
    '''
    The same object the local engine hands to its supervisor thread. One policy,
    two mechanisms — which is what keeps the two engines' failure behaviour
    identical rather than merely similar.
    '''
    from videoflow.core.supervision import SupervisionPolicy

    specs = compile_flow(_flow(BATCH))
    manifests = render_manifests(specs, 'demo', BATCH, 'nats://x:4222', 'run1',
                                 default_image = IMG,
                                 supervision = SupervisionPolicy(max_restarts = 7))
    job = [m for m in manifests if m['kind'] == 'Job'
           and m['metadata']['name'] == 'vf-demo-run1-work'][0]
    assert job['spec']['backoffLimit'] == 7


def test_every_container_reports_why_it_died():
    '''
    The worker writes a structured reason to /dev/termination-log; this policy is
    what makes a death too abrupt to write anything still surface its last log
    lines through the same channel.
    '''
    manifests, _ = _by_kind(BATCH)
    containers = [m['spec']['template']['spec']['containers'][0]
                  for m in manifests if m['kind'] in ('Job', 'Deployment', 'StatefulSet')
                  and 'template' in m['spec']]
    assert containers
    for container in containers:
        assert container['terminationMessagePolicy'] == 'FallbackToLogsOnError'


def test_error_handling_overrides_reach_the_worker_env():
    '''
    ``delivery``/``on_error`` are lifted out of params like partition_by, because
    provisioning needs the delivery mode before any worker exists — it decides the
    durables' max_deliver.
    '''
    p = IntProducer(0, 5, name = 'producer')
    a = IdentityProcessor(name = 'work', on_error = 'poison')(p)
    out = CommandlineConsumer(name = 'printer', delivery = 'at-least-once')(a)
    specs = compile_flow(Flow([out], flow_type = BATCH, flow_id = 'demo'))
    manifests = render_manifests(specs, 'demo', BATCH, 'nats://x:4222', 'run1',
                                 default_image = IMG)
    env = {m['metadata']['name']: m['data'] for m in manifests if m['kind'] == 'ConfigMap'}
    assert env['vf-demo-run1-work-env']['VF_ON_ERROR'] == 'poison'
    assert 'VF_DELIVERY' not in env['vf-demo-run1-work-env']
    assert env['vf-demo-run1-printer-env']['VF_DELIVERY'] == 'at-least-once'
    # A node that overrides nothing renders exactly the env it always did.
    assert 'VF_ON_ERROR' not in env['vf-demo-run1-producer-env']
    assert 'VF_DELIVERY' not in env['vf-demo-run1-producer-env']


def test_scaledobject_is_a_separate_crd_delete_kind():
    # scaledobject (KEDA CRD) is deleted separately so a missing CRD can't abort the
    # core delete and leak resources.
    assert 'scaledobject' in _CRD_DELETABLE_KINDS
    assert 'scaledobject' not in _CORE_DELETABLE_KINDS
    assert 'job' in _CORE_DELETABLE_KINDS
