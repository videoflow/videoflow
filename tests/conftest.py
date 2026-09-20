'''
Suite-wide fixtures for the unit tests (the integration and conformance buckets
have their own conftests alongside this one).
'''
import os
import subprocess

import pytest

#: Binaries the unit suite must never run for real: each one means a stub is
#: missing, and the answer then depends on the developer's machine rather than on
#: the code under test.
EXTERNAL_TOOLS = frozenset({'docker', 'kubectl', 'crane', 'oras', 'nvidia-smi', 'helm', 'kind'})


@pytest.fixture(autouse = True)
def _no_cluster_profile(tmp_path, monkeypatch):
    '''
    A developer's own ``~/.config/videoflow/clusters.yaml`` (matched by the live
    kubectl context) must never feed a test's ``deploy``; every test starts from an
    absent profiles file and a clean docker-args environment. ``test_profiles.py``
    sets the variable again for the file it stages. The variable also reaches the
    ``videoflow`` subprocesses the integration buckets spawn.
    '''
    monkeypatch.setenv('VF_CLUSTERS_FILE', str(tmp_path / 'no-clusters.yaml'))
    monkeypatch.delenv('VF_DOCKER_BUILD_ARGS', raising = False)
    monkeypatch.delenv('VF_DOCKER_RUN_ARGS', raising = False)


@pytest.fixture(autouse = True)
def _no_external_tools(request, monkeypatch):
    '''
    Fails a unit test that reaches a docker daemon, a cluster or a GPU driver for
    real. The ``run-local`` CLI tests once paid a real ``docker info`` each (a
    third of a second, and a daemon dependency), and a ``deploy`` test asked the
    developer's current cluster what it would reuse. The integration and
    conformance buckets gate on real ``kubectl`` and run a fake ``nvidia-smi``
    from PATH, so they are exempt.
    '''
    path = str(request.node.fspath)
    if f'{os.sep}integration{os.sep}' in path or f'{os.sep}conformance{os.sep}' in path:
        return
    real_run, real_popen = subprocess.run, subprocess.Popen

    def check(args):
        argv = list(args) if isinstance(args, (list, tuple)) else [args]
        tool = os.path.basename(str(argv[0])) if argv else ''
        if tool in EXTERNAL_TOOLS:
            pytest.fail(f'unit test ran {tool!r} for real ({argv[:4]}); stub the call instead')

    def run(args, *a, **kw):
        check(args)
        return real_run(args, *a, **kw)

    class Popen(real_popen):  # type: ignore[valid-type, misc]
        def __init__(self, args, *a, **kw):
            check(args)
            super().__init__(args, *a, **kw)

    monkeypatch.setattr(subprocess, 'run', run)
    monkeypatch.setattr(subprocess, 'Popen', Popen)
