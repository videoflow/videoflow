'''
Tests for OCI component distribution (videoflow/oci.py): ref parsing, cache
resolution, push validation, cosign gating, and oci:// resolution through
load_descriptor. The registry round-trip itself is mocked (a live registry needs
docker); the ORAS calls are exercised via a fake client so the caching/validation
logic around them is covered.
'''
from __future__ import absolute_import, division, print_function

import os

import pytest

from videoflow import oci

_VALID_DESCRIPTOR = '''\
apiVersion: videoflow.io/v1
kind: Component
metadata: {name: acme/sort, version: 1.2.0}
spec:
  role: processor
  protocol: 1
  runtime:
    pythonClass: acme.sort.Tracker
    images: {cpu: ghcr.io/acme/sort:1.2.0}
  device: [cpu]
'''

def test_is_and_parse_oci_ref():
    assert oci.is_oci_ref('oci://ghcr.io/acme/sort:1.2.0')
    assert not oci.is_oci_ref('/local/path')
    assert oci.parse_oci_ref('oci://ghcr.io/acme/sort:1.2.0') == 'ghcr.io/acme/sort:1.2.0'
    with pytest.raises(ValueError):
        oci.parse_oci_ref('/local/path')
    with pytest.raises(ValueError):
        oci.parse_oci_ref('oci://nohostslash')

def test_cache_dir_for_is_sanitized(tmp_path):
    d = oci.cache_dir_for('oci://ghcr.io/acme/sort:1.2.0', cache_root = str(tmp_path))
    assert str(tmp_path) in d
    assert '/' not in os.path.basename(d) and ':' not in os.path.basename(d)


class _FakeClient:
    def __init__(self):
        self.push_calls = []
        self.pull_count = 0

    def push(self, target, files = None, manifest_annotations = None, **kw):
        self.push_calls.append({'target': target, 'files': files, 'annotations': manifest_annotations})

    def pull(self, target, outdir = None, allowed_media_type = None, **kw):
        self.pull_count += 1
        path = os.path.join(outdir, 'component.yaml')
        with open(path, 'w') as f:
            f.write(_VALID_DESCRIPTOR)
        return [path]


def test_push_validates_and_tags_media_type(tmp_path, monkeypatch):
    fake = _FakeClient()
    monkeypatch.setattr(oci, '_client', lambda: fake)
    p = tmp_path / 'component.yaml'
    p.write_text(_VALID_DESCRIPTOR)

    target = oci.push_component(str(p), 'oci://ghcr.io/acme/sort:1.2.0')
    assert target == 'ghcr.io/acme/sort:1.2.0'
    call = fake.push_calls[0]
    assert call['files'] == [f'{p}:{oci.COMPONENT_MEDIA_TYPE}']
    assert call['annotations']['org.opencontainers.image.title'] == 'acme/sort'
    assert call['annotations']['io.videoflow.component.protocol'] == '1'


def test_push_rejects_broken_descriptor(tmp_path, monkeypatch):
    fake = _FakeClient()
    monkeypatch.setattr(oci, '_client', lambda: fake)
    p = tmp_path / 'component.yaml'
    p.write_text('apiVersion: wrong\nkind: Component\n')
    with pytest.raises(ValueError):
        oci.push_component(str(p), 'oci://ghcr.io/acme/sort:1.2.0')
    assert fake.push_calls == []   # never pushed a broken descriptor


def test_pull_caches_after_first_fetch(tmp_path, monkeypatch):
    fake = _FakeClient()
    monkeypatch.setattr(oci, '_client', lambda: fake)
    ref = 'oci://ghcr.io/acme/sort:1.2.0'

    path1 = oci.pull_component(ref, cache_root = str(tmp_path))
    assert os.path.isfile(path1) and fake.pull_count == 1
    # Second resolve is served from cache — no second network pull.
    path2 = oci.pull_component(ref, cache_root = str(tmp_path))
    assert path2 == path1 and fake.pull_count == 1
    # ...unless forced.
    oci.pull_component(ref, cache_root = str(tmp_path), force = True)
    assert fake.pull_count == 2


def test_pull_verify_requires_cosign(tmp_path, monkeypatch):
    fake = _FakeClient()
    monkeypatch.setattr(oci, '_client', lambda: fake)
    monkeypatch.setattr(oci.shutil, 'which', lambda _name: None)   # cosign absent
    with pytest.raises(RuntimeError, match = 'cosign'):
        oci.pull_component('oci://ghcr.io/acme/sort:1.2.0', cache_root = str(tmp_path), verify = True)


def test_load_descriptor_resolves_oci(tmp_path, monkeypatch):
    from videoflow import component as component_mod

    cached = tmp_path / 'component.yaml'
    cached.write_text(_VALID_DESCRIPTOR)
    monkeypatch.setattr('videoflow.oci.pull_component', lambda ref, **kw: str(cached))

    desc = component_mod.load_descriptor('oci://ghcr.io/acme/sort:1.2.0')
    assert desc.name == 'acme/sort' and desc.python_class == 'acme.sort.Tracker'


def test_inspect_returns_descriptor(tmp_path, monkeypatch):
    fake = _FakeClient()
    monkeypatch.setattr(oci, '_client', lambda: fake)
    monkeypatch.setattr(oci, 'default_cache_root', lambda: str(tmp_path))
    desc = oci.inspect_component('oci://ghcr.io/acme/sort:1.2.0')
    assert desc.name == 'acme/sort' and desc.role == 'processor'


if __name__ == '__main__':
    pytest.main([__file__])
