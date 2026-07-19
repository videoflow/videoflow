'''
Distribution of component descriptors as OCI artifacts (marketplace foundation).

A component's ``component.yaml`` is pushed to any OCI registry as an artifact with
media type ``application/vnd.videoflow.component.v1+yaml``, referencing its cpu/gpu
container images (which are pushed separately as normal images). Users then pull a
component by reference — ``oci://ghcr.io/vendor/name:1.2.0`` — and videoflow resolves
the descriptor (and, through it, the image) without the component's source.

This module is intentionally thin: it wraps ORAS-py for push/pull and shells out to
``cosign`` for optional signature verification, rather than reimplementing either.
Pulled descriptors are cached under ``~/.videoflow/components/`` so a repeated
``component('oci://...')`` resolve is offline after the first fetch.
'''
from __future__ import absolute_import, division, print_function

import os
import re
import shutil
import subprocess
from typing import List, Optional

#: OCI media type for a videoflow component descriptor artifact.
COMPONENT_MEDIA_TYPE = 'application/vnd.videoflow.component.v1+yaml'
#: artifactType set on the artifact manifest (helps registries/tools classify it).
COMPONENT_ARTIFACT_TYPE = 'application/vnd.videoflow.component.v1'

OCI_PREFIX = 'oci://'

def is_oci_ref(ref : str) -> bool:
    return isinstance(ref, str) and ref.startswith(OCI_PREFIX)

def parse_oci_ref(ref : str) -> str:
    '''``oci://ghcr.io/vendor/name:1.2.0`` -> ``ghcr.io/vendor/name:1.2.0`` (the registry target).'''
    if not is_oci_ref(ref):
        raise ValueError(f'Not an OCI ref (expected {OCI_PREFIX}...): {ref!r}')
    target = ref[len(OCI_PREFIX):]
    if '/' not in target:
        raise ValueError(f'Malformed OCI ref {ref!r}: expected {OCI_PREFIX}<registry>/<repo>[:<tag>]')
    return target

def default_cache_root() -> str:
    return os.environ.get('VIDEOFLOW_COMPONENT_CACHE',
                        os.path.join(os.path.expanduser('~'), '.videoflow', 'components'))

def cache_dir_for(ref : str, cache_root : str = None) -> str:
    '''A stable per-ref cache directory (the ref sanitized into a filesystem-safe key).'''
    target = parse_oci_ref(ref)
    key = re.sub(r'[^A-Za-z0-9_.-]+', '_', target)
    return os.path.join(cache_root or default_cache_root(), key)

def _client():
    try:
        from oras.client import OrasClient
    except ImportError as e:
        raise RuntimeError(
            'OCI component distribution needs the "oras" package: pip install oras '
            '(or install videoflow[deploy]).') from e
    return OrasClient()

def push_component(descriptor_path : str, ref : str, annotations : dict = None) -> str:
    '''
    Push a ``component.yaml`` to ``ref`` as an OCI artifact. The descriptor is
    validated first (a broken descriptor is never published). Returns the target.
    '''
    from .descriptor import load_descriptor  # validates on load
    if os.path.isdir(descriptor_path):
        descriptor_path = os.path.join(descriptor_path, 'component.yaml')
    desc = load_descriptor(descriptor_path)

    target = parse_oci_ref(ref)
    ann = {
        'org.opencontainers.image.title': desc.name,
        'org.opencontainers.image.version': desc.version,
        'io.videoflow.component.role': desc.role,
        'io.videoflow.component.protocol': str(desc.protocol),
    }
    ann.update(annotations or {})

    client = _client()
    # ORAS-py tags a file's media type via a "path:mediatype" spec.
    client.push(
        target = target,
        files = [f'{descriptor_path}:{COMPONENT_MEDIA_TYPE}'],
        manifest_annotations = ann,
    )
    return target

def pull_component(ref : str, cache_root : str = None, force : bool = False,
                verify : bool = False, cosign_args : List[str] = None) -> str:
    '''
    Resolve ``ref`` to a local ``component.yaml`` path, pulling + caching it on first
    use. With ``verify``, the artifact's signature is checked via ``cosign`` before
    the descriptor is trusted. Returns the path to the cached descriptor.
    '''
    outdir = cache_dir_for(ref, cache_root)
    cached = os.path.join(outdir, 'component.yaml')
    if os.path.isfile(cached) and not force:
        return cached

    if verify:
        cosign_verify(parse_oci_ref(ref), extra_args = cosign_args)

    os.makedirs(outdir, exist_ok = True)
    client = _client()
    pulled = client.pull(target = parse_oci_ref(ref), outdir = outdir,
                        allowed_media_type = [COMPONENT_MEDIA_TYPE])
    # Normalize whatever filename the artifact used to component.yaml in the cache.
    yaml_path = _first_yaml(pulled) or _first_yaml_in_dir(outdir)
    if yaml_path is None:
        raise RuntimeError(f'Pulled artifact {ref} contained no component descriptor (.yaml).')
    if os.path.abspath(yaml_path) != os.path.abspath(cached):
        shutil.copyfile(yaml_path, cached)
    return cached

def inspect_component(ref : str, **pull_kwargs):
    '''Pull (cached) and return the parsed ``ComponentDescriptor`` for ``ref``.'''
    from .descriptor import load_descriptor
    return load_descriptor(pull_component(ref, **pull_kwargs))

def cosign_verify(target : str, extra_args : List[str] = None) -> None:
    '''
    Verify an OCI artifact/image signature with cosign. ``extra_args`` carries the
    trust policy (e.g. ``--key cosign.pub`` or
    ``--certificate-identity=... --certificate-oidc-issuer=...``). Raises on failure.
    '''
    if shutil.which('cosign') is None:
        raise RuntimeError('cosign is required for --verify but was not found on PATH. '
                        'Install cosign (https://docs.sigstore.dev/cosign/) or drop --verify.')
    cmd = ['cosign', 'verify', *(extra_args or []), target]
    result = subprocess.run(cmd, capture_output = True, text = True)
    if result.returncode != 0:
        raise RuntimeError(f'cosign verify failed for {target}:\n{result.stderr.strip()}')

def _first_yaml(paths) -> Optional[str]:
    for p in paths or []:
        if str(p).endswith(('.yaml', '.yml')):
            return p
    return None

def _first_yaml_in_dir(outdir : str) -> Optional[str]:
    for root, _dirs, files in os.walk(outdir):
        for f in files:
            if f.endswith(('.yaml', '.yml')):
                return os.path.join(root, f)
    return None
