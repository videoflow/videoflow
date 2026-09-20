'''
Suite-wide fixtures for the unit tests (the integration and conformance buckets
have their own conftests alongside this one).
'''
import pytest


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
