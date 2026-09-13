'''
``videoflow deploy --render-only`` / ``--dry-run``: the manifest-generation escape
hatch, which returns before anything touches a cluster.

The property under test is that it renders **the same manifests the real deploy
would apply**. It stopped doing that in one specific way: the renderer read the
``--image`` flag while the deploy path read the *resolved* image, so a plain
``videoflow deploy graph.py --render-only`` built the image and then rendered
manifests referencing none, dying with "node 'x' has no container image" — for a
command that deploys perfectly well without the flag. A rendered manifest set that
disagrees with the deploy is worse than no rendering at all, since its whole purpose
is being applied later by hand.

Pure/unit: docker, the graph loader and the cluster are all monkeypatched.
'''
from __future__ import absolute_import, division, print_function

import yaml

from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.constants import BATCH
from videoflow.deploy import build, cli
from videoflow.producers import IntProducer

BUILT = 'videoflow-mygraph:latest'

def _flow():
    producer = IntProducer(0, 3, name = 'numbers')
    return Flow([CommandlineConsumer(name = 'printer')(producer)], flow_type = BATCH,
                flow_id = 'render')

def _render(tmp_path, monkeypatch, argv_extra = ()):
    '''Runs `deploy --render-only` with the build and the graph load faked out.'''
    graph = tmp_path / 'mygraph.py'
    graph.write_text('# staged by the test; _load_flow is monkeypatched\n')
    out = tmp_path / 'manifests'

    monkeypatch.setattr(cli, '_load_flow', lambda target: _flow())
    monkeypatch.setattr(build, 'docker_gpus_available', lambda: False)
    monkeypatch.setattr(cli, 'autobuild', lambda *a, **kw: BUILT)
    monkeypatch.setattr(cli, 'docker_gpus_available', lambda: False)

    code = cli.main(['deploy', str(graph), '--render-only', '--output', str(out),
                     '--non-interactive', *argv_extra])
    return code, out

def _images(out):
    '''Every container image referenced by the rendered workloads.'''
    found = set()
    for path in sorted(out.glob('*.yaml')):
        for doc in yaml.safe_load_all(path.read_text()):
            if not doc:
                continue
            template = doc.get('spec', {}).get('template') or \
                doc.get('spec', {}).get('jobTemplate', {}).get('spec', {}).get('template')
            for container in (template or {}).get('spec', {}).get('containers', []):
                found.add(container['image'])
    return found

def test_render_only_uses_the_image_it_just_built(tmp_path, monkeypatch):
    code, out = _render(tmp_path, monkeypatch)
    assert code == 0
    assert BUILT in _images(out)

def test_an_explicit_image_still_wins(tmp_path, monkeypatch):
    code, out = _render(tmp_path, monkeypatch,
                        ['--image', 'ghcr.io/acme/app:v1', '--no-build'])
    assert code == 0
    images = _images(out)
    assert 'ghcr.io/acme/app:v1' in images
    assert BUILT not in images


# -- claim mounts, priority class and broker profiles --------------------------

def _docs(out):
    '''Every rendered document, keyed by (kind, name).'''
    found = {}
    for path in sorted(out.glob('*.yaml')):
        for doc in yaml.safe_load_all(path.read_text()):
            if doc and doc.get('kind') != 'Kustomization':
                found[(doc['kind'], doc['metadata']['name'])] = doc
    return found

def _pod_spec(doc):
    return doc['spec']['template']['spec']

def test_mount_pvc_and_priority_class_reach_the_rendered_pods(tmp_path, monkeypatch):
    '''
    ``--mount-pvc`` renders a persistentVolumeClaim volume, a ``--mount`` under
    its path is dropped from the pods (the claim serves it there), and
    ``--priority-class`` lands on every pod — workers, provision Job and the
    dev broker the render includes.
    '''
    code, out = _render(tmp_path, monkeypatch,
                        ['--mount-pvc', 'vf-test-share:/opt/data/share',
                         '--mount', '/opt/data/share/run-1/out',
                         '--mount', '/models:ro',
                         '--priority-class', 'cluster-batch'])
    assert code == 0
    docs = _docs(out)
    worker = _pod_spec(docs[('Job', 'vf-render-numbers')])
    assert worker['volumes'] == [
        {'name': 'vf-mount-1', 'hostPath': {'path': '/models'}},
        {'name': 'vf-pvc-0', 'persistentVolumeClaim': {'claimName': 'vf-test-share'}}]
    assert [m['mountPath'] for m in worker['containers'][0]['volumeMounts']] == \
        ['/models', '/opt/data/share']
    for key in (('Job', 'vf-render-numbers'), ('Job', 'vf-render-printer'),
                ('Job', 'vf-render-provision'), ('Deployment', 'nats'), ('Deployment', 'redis')):
        assert _pod_spec(docs[key])['priorityClassName'] == 'cluster-batch', key

def test_the_default_render_carries_no_priority_or_claim(tmp_path, monkeypatch):
    code, out = _render(tmp_path, monkeypatch)
    assert code == 0
    for doc in _docs(out).values():
        if 'template' in doc.get('spec', {}):
            assert 'priorityClassName' not in _pod_spec(doc)
            assert 'volumes' not in _pod_spec(doc) or all(
                'persistentVolumeClaim' not in v for v in _pod_spec(doc)['volumes'])

def test_broker_profile_durable_renders_a_statefulset(tmp_path, monkeypatch):
    code, out = _render(tmp_path, monkeypatch,
                        ['--broker-profile', 'durable', '--broker-replicas', '3',
                         '--broker-storage-class', 'nfs-shared'])
    assert code == 0
    docs = _docs(out)
    assert ('Deployment', 'nats') not in docs
    nats = docs[('StatefulSet', 'nats')]
    assert nats['spec']['replicas'] == 3
    assert nats['spec']['serviceName'] == 'nats-headless'
    assert nats['spec']['volumeClaimTemplates'][0]['spec']['storageClassName'] == 'nfs-shared'
    assert ('Service', 'nats-headless') in docs and ('Service', 'nats') in docs
    conf = docs[('ConfigMap', 'nats-config')]['data']['nats.conf']
    assert 'nats://nats-2.nats-headless.default.svc:6222' in conf
    # The durable Redis: append-only on its own claim.
    redis = docs[('Deployment', 'redis')]
    assert ('PersistentVolumeClaim', 'redis-data') in docs
    assert '--appendonly' in redis['spec']['template']['spec']['containers'][0]['args']
    assert redis['spec']['template']['spec']['containers'][0]['args'][3] == 'yes'
    # The kustomization lists the new kinds too.
    listed = yaml.safe_load((out / 'kustomization.yaml').read_text())['resources']
    assert 'statefulset-nats.yaml' in listed and 'persistentvolumeclaim-redis-data.yaml' in listed

def test_durable_only_flags_are_refused_with_the_dev_profile(tmp_path, monkeypatch):
    '''A ConfigError (exit 2) before any image is built — the flag has no target.'''
    from videoflow.core.errors import EXIT_USER

    code, _out = _render(tmp_path, monkeypatch, ['--broker-replicas', '3'])
    assert code == EXIT_USER
    code, _out = _render(tmp_path, monkeypatch, ['--broker-profile', 'durable',
                                                 '--broker-replicas', '2'])
    assert code == EXIT_USER       # an even cluster has no quorum
