'''
The dev-infrastructure lifecycle, against a real cluster.

``videoflow deploy`` installs NATS and Redis into the target namespace when no
``--nats`` is given, and the contract that makes that safe is ownership: it
installs only what is missing, reports exactly what it created, and tears down only
that. Unit tests pin the manifest shapes and the kubectl argv (tests/test_infra.py,
with subprocess.run monkeypatched); what they cannot show is whether the workloads
that come out actually become ready, or whether a second deploy into a namespace
that already has a broker correctly claims nothing.

Runs in its own throwaway namespace rather than the shared one, so a failure here
cannot take the broker down for every other test in the bucket.
'''
from __future__ import absolute_import, division, print_function

import uuid

import pytest
from support_k8s import kubectl

from videoflow.deploy.infra import (
    ensure_infra,
    ensure_namespace,
    infra_urls,
    service_exists,
    teardown_infra,
    wait_infra_ready,
)

pytestmark = pytest.mark.timeout(300)

@pytest.fixture
def scratch_namespace():
    '''A namespace that exists only for one test.'''
    name = f'vf-it-infra-{uuid.uuid4().hex[:8]}'
    ensure_namespace('kubectl', name)
    yield name
    kubectl('delete', 'namespace', name, '--wait=false', timeout = 60)

def test_infra_installs_becomes_ready_and_is_reused(scratch_namespace):
    urls, created = ensure_infra('kubectl', scratch_namespace, need_redis = True)

    # It reports what it installed, and the URLs it hands workers are the
    # in-cluster DNS names — not anything the deploying host could reach.
    assert sorted(created) == ['nats', 'redis'], created
    assert urls == infra_urls(scratch_namespace)
    assert urls['nats'] == f'nats://nats.{scratch_namespace}.svc:4222'

    # The part no mock can assert: the Deployments actually roll out.
    wait_infra_ready('kubectl', scratch_namespace, created)
    assert service_exists('kubectl', scratch_namespace, 'nats')
    assert service_exists('kubectl', scratch_namespace, 'redis')

    # The reuse contract. A second deploy into a namespace that already has a
    # broker must claim nothing — otherwise it would later tear down infra it did
    # not install, taking every other flow in the namespace with it.
    urls_again, created_again = ensure_infra('kubectl', scratch_namespace, need_redis = True)
    assert created_again == [], created_again
    assert urls_again == urls

    # Teardown is scoped to what was created, by ownership label.
    teardown_infra('kubectl', scratch_namespace, created)
    assert not service_exists('kubectl', scratch_namespace, 'nats')
    assert not service_exists('kubectl', scratch_namespace, 'redis')

def test_redis_is_optional(scratch_namespace):
    '''A flow with no blob store gets a broker and nothing else.'''
    _urls, created = ensure_infra('kubectl', scratch_namespace, need_redis = False)
    assert created == ['nats'], created
    wait_infra_ready('kubectl', scratch_namespace, created)
    assert not service_exists('kubectl', scratch_namespace, 'redis')
    teardown_infra('kubectl', scratch_namespace, created)

if __name__ == '__main__':
    pytest.main([__file__])
