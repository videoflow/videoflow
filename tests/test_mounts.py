'''
--mount support: hostPath volumes threaded into every node workload (Job,
Deployment, StatefulSet) but never the provision Job, plus parse_mounts
validation of the HOST:CONTAINER[:ro] and single-path shorthand forms.

Pure/unit: only inspects rendered manifest dicts (no cluster, no broker).
'''
import pytest

from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.compiler import compile_flow
from videoflow.core.constants import BATCH, REALTIME
from videoflow.deploy.manifests import Mount, parse_mounts, render_manifests
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer

IMG = 'ghcr.io/acme/app:v1'


def _flow(flow_type, partitioned = False):
    p = IntProducer(0, 5, name = 'producer')
    if partitioned:
        a = IdentityProcessor(name = 'work', nb_tasks = 3, partition_by = 'trace_id')(p)
    else:
        a = IdentityProcessor(name = 'work')(p)
    out = CommandlineConsumer(name = 'printer')(a)
    return Flow([out], flow_type = flow_type, flow_id = 'demo')


def _by_kind(flow_type, mounts, **kw):
    manifests = render_manifests(compile_flow(_flow(flow_type, **kw)), 'demo', flow_type,
                                 'nats://x:4222', 'run1', default_image = IMG, mounts = mounts)
    return manifests, {(m['kind'], m['metadata']['name']): m for m in manifests}


def test_parse_mounts_forms():
    mounts = parse_mounts(['/data/in:/data/in:ro', '/work', '/cache:ro', '/a:/b'])
    assert mounts[0] == Mount(name = 'vf-mount-0', host_path = '/data/in',
                              container_path = '/data/in', read_only = True)
    # Single-path shorthand mounts the same path on both sides.
    assert (mounts[1].host_path, mounts[1].container_path, mounts[1].read_only) == \
        ('/work', '/work', False)
    assert (mounts[2].host_path, mounts[2].container_path, mounts[2].read_only) == \
        ('/cache', '/cache', True)
    assert (mounts[3].host_path, mounts[3].container_path) == ('/a', '/b')
    assert parse_mounts(None) == []
    assert parse_mounts([]) == []


@pytest.mark.parametrize('bad', ['relative/path', '/abs:relative', 'rel:/abs',
                                 '/a:/b:rw', '/a:/b:/c:ro', ''])
def test_parse_mounts_rejects_malformed(bad):
    with pytest.raises(ValueError):
        parse_mounts([bad])


def _volumes_and_mounts(workload):
    pod = workload['spec']['template']['spec']
    return pod.get('volumes'), pod['containers'][0].get('volumeMounts')


def test_batch_jobs_get_hostpath_volumes_but_provision_does_not():
    mounts = parse_mounts(['/data:/data:ro'])
    _, by = _by_kind(BATCH, mounts)
    for node in ('producer', 'work', 'printer'):
        volumes, vmounts = _volumes_and_mounts(by[('Job', f'vf-demo-{node}')])
        assert volumes == [{'name': 'vf-mount-0', 'hostPath': {'path': '/data'}}]
        assert vmounts == [{'name': 'vf-mount-0', 'mountPath': '/data', 'readOnly': True}]
    volumes, vmounts = _volumes_and_mounts(by[('Job', 'vf-demo-provision')])
    # The provision Job keeps only its specs ConfigMap volume — no hostPath.
    assert all(v.get('hostPath') is None for v in volumes)
    assert all(m['mountPath'] != '/data' for m in vmounts)


def test_realtime_deployment_and_statefulset_get_volumes():
    mounts = parse_mounts(['/data'])
    _, by = _by_kind(REALTIME, mounts, partitioned = True)
    for key in (('Deployment', 'vf-demo-printer'), ('StatefulSet', 'vf-demo-work')):
        volumes, vmounts = _volumes_and_mounts(by[key])
        assert volumes[0]['hostPath'] == {'path': '/data'}
        assert vmounts[0] == {'name': 'vf-mount-0', 'mountPath': '/data', 'readOnly': False}


def test_no_mounts_leaves_manifests_unchanged():
    _, by = _by_kind(BATCH, None)
    volumes, vmounts = _volumes_and_mounts(by[('Job', 'vf-demo-work')])
    assert volumes is None and vmounts is None


def test_parse_mounts_numbers_from_zero_per_call():
    '''
    Names are unique within one call but NOT across calls — each starts at
    vf-mount-0. Concatenating two results is what ``cli.py`` does for the
    prepare/compile containers, and it is only safe because ``run_in_image``
    addresses mounts by path. Pinning the numbering so the collision below
    stays reproducible rather than accidental.
    '''
    assert [m.name for m in parse_mounts(['/a', '/b'])] == ['vf-mount-0', 'vf-mount-1']
    concatenated = parse_mounts(['/graph']) + parse_mounts(['/a', '/b'])
    assert [m.name for m in concatenated] == ['vf-mount-0', 'vf-mount-0', 'vf-mount-1']


def test_render_rejects_duplicate_mount_names():
    '''
    A pod may not declare two volumes with the same name — the API server rejects
    it. Routing cli.py's concatenated ``container_mounts`` into render_manifests
    (instead of the single-call ``mounts``) would do exactly that, so fail with a
    message naming the fix rather than emitting a manifest the cluster refuses.
    '''
    concatenated = parse_mounts(['/graph']) + parse_mounts(['/data:/data:ro'])
    with pytest.raises(ValueError, match = 'duplicate mount volume names'):
        _by_kind(BATCH, concatenated)
    # The single-call list this is easily confused with renders fine.
    _by_kind(BATCH, parse_mounts(['/graph', '/data:/data:ro']))


# -- claim mounts (--mount-pvc) and the priority class ------------------------
#
# A PersistentVolumeClaim is the multi-node answer to the hostPath work root: on a
# cluster where no node's own filesystem holds the directory, a hostPath silently
# mounts an empty root-owned directory on every node but one. These pin the
# claim:/path[:ro] form, its rendering as a persistentVolumeClaim volume, the rule
# that drops a shadowed hostPath from the pods (and only the pods), and that none
# of it changes a render that uses neither.

from videoflow.deploy import build  # noqa: E402
from videoflow.deploy.manifests import parse_pvc_mounts, pod_mounts  # noqa: E402


def test_parse_pvc_mounts_forms():
    mounts = parse_pvc_mounts(['models:/models:ro', 'vf-test-share:/opt/data/share'])
    assert mounts[0] == Mount(name = 'vf-pvc-0', host_path = '', container_path = '/models',
                              read_only = True, claim = 'models')
    assert mounts[1] == Mount(name = 'vf-pvc-1', host_path = '', container_path = '/opt/data/share',
                              read_only = False, claim = 'vf-test-share')
    assert parse_pvc_mounts(None) == []
    assert parse_pvc_mounts([]) == []
    # Host-path mounts carry no claim, so the two kinds are told apart by the field.
    assert all(m.claim is None for m in parse_mounts(['/data']))


@pytest.mark.parametrize('bad', ['/models', 'models', 'Models:/m', 'm:relative', 'm:/p:rw',
                                 'm:/p:/q:ro', 'my_claim:/p', '', ':/p'])
def test_parse_pvc_mounts_rejects_malformed(bad):
    with pytest.raises(ValueError, match = 'claim:/container/path'):
        parse_pvc_mounts([bad])


def test_claim_names_never_collide_with_hostpath_names():
    '''The two parsers number from disjoint prefixes, so one pod takes both lists.'''
    combined = parse_mounts(['/a', '/b']) + parse_pvc_mounts(['c:/c'])
    assert [m.name for m in combined] == ['vf-mount-0', 'vf-mount-1', 'vf-pvc-0']
    _by_kind(BATCH, combined)     # renders without the duplicate-name rejection


def test_claim_mounts_render_as_persistent_volume_claim_volumes():
    mounts = parse_pvc_mounts(['models:/models:ro', 'share:/share'])
    _, by = _by_kind(BATCH, mounts)
    for node in ('producer', 'work', 'printer'):
        volumes, vmounts = _volumes_and_mounts(by[('Job', f'vf-demo-{node}')])
        # kubectl explain pod.spec.volumes.persistentVolumeClaim: claimName is
        # required, readOnly optional and only emitted when set.
        assert volumes == [
            {'name': 'vf-pvc-0', 'persistentVolumeClaim': {'claimName': 'models', 'readOnly': True}},
            {'name': 'vf-pvc-1', 'persistentVolumeClaim': {'claimName': 'share'}}]
        assert vmounts == [
            {'name': 'vf-pvc-0', 'mountPath': '/models', 'readOnly': True},
            {'name': 'vf-pvc-1', 'mountPath': '/share', 'readOnly': False}]
    # The provision Job still gets only its specs ConfigMap.
    volumes, _ = _volumes_and_mounts(by[('Job', 'vf-demo-provision')])
    assert all('persistentVolumeClaim' not in v for v in volumes)
    # REALTIME workloads too, Deployment and StatefulSet alike.
    _, by = _by_kind(REALTIME, mounts, partitioned = True)
    for key in (('Deployment', 'vf-demo-printer'), ('StatefulSet', 'vf-demo-work')):
        volumes, _ = _volumes_and_mounts(by[key])
        assert volumes[1] == {'name': 'vf-pvc-1', 'persistentVolumeClaim': {'claimName': 'share'}}


def test_hostpath_mounts_under_a_claim_path_are_dropped_from_the_pods():
    '''
    The one rule, stated once: a hostPath at or under a claim's container path is
    served by the claim in the pods, so it is dropped there — kept for the
    prep/compile containers, which run on the host where it resolves.
    '''
    mounts = parse_mounts(['/share/run/out', '/share', '/share-2:ro', '/other:/other:ro']) \
        + parse_pvc_mounts(['vf-test-share:/share'])
    kept = pod_mounts(mounts)
    assert [(m.name, m.container_path) for m in kept] == [
        ('vf-mount-2', '/share-2'),      # a sibling that merely shares the prefix string
        ('vf-mount-3', '/other'),
        ('vf-pvc-0', '/share')]
    # Order and identity are preserved for a list without claims.
    plain = parse_mounts(['/b', '/a'])
    assert pod_mounts(plain) == plain
    # And that is exactly what the pods render.
    _, by = _by_kind(BATCH, mounts)
    volumes, vmounts = _volumes_and_mounts(by[('Job', 'vf-demo-work')])
    assert volumes == [
        {'name': 'vf-mount-2', 'hostPath': {'path': '/share-2'}},
        {'name': 'vf-mount-3', 'hostPath': {'path': '/other'}},
        {'name': 'vf-pvc-0', 'persistentVolumeClaim': {'claimName': 'vf-test-share'}}]
    assert [m['mountPath'] for m in vmounts] == ['/share-2', '/other', '/share']


def test_a_claim_at_root_shadows_every_hostpath():
    mounts = parse_mounts(['/data', '/x:ro']) + parse_pvc_mounts(['everything:/'])
    assert [m.name for m in pod_mounts(mounts)] == ['vf-pvc-0']


def test_run_in_image_skips_claim_mounts(monkeypatch):
    '''The prep/compile container runs on the host, where a claim does not exist.'''
    import subprocess

    seen = {}

    class _Proc:
        returncode = 0
        stdout = ''
        stderr = ''

    def run(cmd, **kwargs):
        seen['cmd'] = cmd
        return _Proc()

    monkeypatch.setattr(subprocess, 'run', run)
    mounts = parse_mounts(['/graph', '/graph/out']) + parse_pvc_mounts(['share:/share', 'm:/models:ro'])
    build.run_in_image('img:1', ['python', '-c', 'pass'], mounts = mounts)
    flags = [seen['cmd'][i + 1] for i, a in enumerate(seen['cmd']) if a == '-v']
    assert flags == ['/graph:/graph', '/graph/out:/graph/out']


def _pod_specs(manifests):
    '''Every pod template spec in a render, keyed by workload kind/name.'''
    return {(m['kind'], m['metadata']['name']): m['spec']['template']['spec']
            for m in manifests if 'template' in m.get('spec', {})}


def test_priority_class_lands_on_every_pod_and_defaults_to_none():
    manifests = render_manifests(compile_flow(_flow(REALTIME, partitioned = True)), 'demo', REALTIME,
                                 'nats://x:4222', 'run1', default_image = IMG,
                                 priority_class = 'cluster-batch')
    pods = _pod_specs(manifests)
    # Workers of every kind and the provision Job alike — a flow that yields must
    # yield everywhere, or its provision pod is the one that cannot schedule.
    assert {kind for kind, _ in pods} == {'Job', 'Deployment', 'StatefulSet'}
    assert all(spec['priorityClassName'] == 'cluster-batch' for spec in pods.values()), pods
    assert pods[('Job', 'vf-demo-provision')]['priorityClassName'] == 'cluster-batch'
    # Absent by default: kubectl explain pod.spec.priorityClassName — unset means
    # the cluster's default priority, and the goldens pin that nothing is emitted.
    manifests = render_manifests(compile_flow(_flow(REALTIME, partitioned = True)), 'demo', REALTIME,
                                 'nats://x:4222', 'run1', default_image = IMG)
    assert all('priorityClassName' not in spec for spec in _pod_specs(manifests).values())
