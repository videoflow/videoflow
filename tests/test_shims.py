'''
The compatibility shims left at the package root by the module reorganization.

These five paths are frozen public contract: ``videoflow.worker`` is the
ENTRYPOINT of the published base images (inherited by every contrib component
image), ``videoflow.provision`` is rendered into the init Job of every deployed
flow, ``videoflow.compile`` is spawned inside solution images by the host CLI,
``videoflow.cli`` backs the console script, and ``videoflow.serialization`` is a
frozen re-export of the wire codec that external importers may have pinned. None
can be retargeted retroactively, so this file exists to make deleting or hollowing
one a test failure rather than a field incident.
'''
from __future__ import absolute_import, division, print_function

import subprocess
import sys

import pytest

# (shim module path, module it forwards to)
SHIMS = [
    ('videoflow.worker', 'videoflow.runtime.worker'),
    ('videoflow.provision', 'videoflow.runtime.provision'),
    ('videoflow.compile', 'videoflow.deploy.compile'),
    ('videoflow.cli', 'videoflow.deploy.cli'),
    ('videoflow.serialization', 'videoflow.wire.serialization'),
]

# Shims that must stay runnable as ``python -m <module>``. Each is baked into an
# artifact we cannot retroactively edit -- see this module's docstring.
RUNNABLE = ['videoflow.worker', 'videoflow.provision', 'videoflow.compile', 'videoflow.cli']


def _import(path):
    return __import__(path, fromlist = ['__name__'])


@pytest.mark.parametrize('shim_path,target_path', SHIMS)
def test_shim_imports(shim_path, target_path):
    assert _import(shim_path) is not None
    assert _import(target_path) is not None


@pytest.mark.parametrize('shim_path,target_path', SHIMS)
def test_shim_reexports_are_the_same_objects(shim_path, target_path):
    '''
    A star-import re-export must alias the target's objects, not copy them:
    ``isinstance`` checks and monkeypatching across the two paths have to agree.
    '''
    shim, target = _import(shim_path), _import(target_path)
    public = [n for n in dir(target) if not n.startswith('_')]
    assert public, f'{target_path} exports nothing public'
    shared = 0
    for name in public:
        value = getattr(target, name)
        # Star-import skips modules imported by the target, not just underscored
        # names; only assert on what the shim actually re-exported.
        if hasattr(shim, name):
            assert getattr(shim, name) is value, f'{shim_path}.{name} is not {target_path}.{name}'
            shared += 1
    assert shared, f'{shim_path} re-exported nothing from {target_path}'


@pytest.mark.parametrize('shim_path', RUNNABLE)
def test_shim_defines_main(shim_path):
    assert callable(_import(shim_path).main)


@pytest.mark.parametrize('shim_path', RUNNABLE)
def test_shim_is_runnable_as_module(shim_path):
    '''
    ``python -m <shim>`` must reach the real code. A non-zero exit is fine (these
    entrypoints need env/config we do not supply here) -- an import error is not.
    '''
    proc = subprocess.run([sys.executable, '-m', shim_path, '--help'],
                          capture_output = True, text = True, timeout = 60)
    combined = proc.stdout + proc.stderr
    assert 'ModuleNotFoundError' not in combined, combined
    assert 'ImportError' not in combined, combined


def test_provision_command_literal_is_unchanged():
    '''
    The rendered init-Job command must keep naming the shim: manifests already
    applied in a cluster reference it.
    '''
    from videoflow.deploy.manifests import provision_init_job

    job = provision_init_job('flow', 'run', 'batch', 'img:1', 'cm')
    container = job['spec']['template']['spec']['containers'][0]
    assert container['command'] == ['python', '-m', 'videoflow.provision']


if __name__ == '__main__':
    pytest.main([__file__])
