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
