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
