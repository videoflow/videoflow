'''
Opt-in placement rendering (plan Phase 4): ``--rollout-policy``, ``--gpu-nodes``,
``--resources`` and the ``dra`` GPU mode's claim fragments in
``videoflow.deploy.manifests``. Every option is off by default — the goldens in
``test_render_goldens.py`` are the byte-identity check — and each renders exactly
the Kubernetes fields it names (RUN-029/031/032, ALLOC-019/029/031).
'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.consumers import VoidConsumer
from videoflow.core import Flow
from videoflow.core.compiler import NodeSpec, compile_flow
from videoflow.core.constants import GPU, REALTIME
from videoflow.deploy.cli import parse_resources
from videoflow.deploy.manifests import host_resources_for, render_manifests, rollout_strategy
from videoflow.processors import IdentityProcessor
from videoflow.producers import IntProducer

IMG = 'ghcr.io/acme/app:v1'


def _flow():
    p = IntProducer(0, 5, name = 'producer')
    a = IdentityProcessor(name = 'detector', nb_tasks = 2, device_type = GPU)(p)
    out = VoidConsumer(name = 'sink')(a)
    return Flow([out], flow_type = REALTIME, flow_id = 'demo')


def _render(**options):
    return render_manifests(compile_flow(_flow()), 'demo', 'realtime', 'nats://x:4222', 'run1',
                            default_image = IMG, **options)


def _workload(manifests, node, kind = 'Deployment'):
    return next(m for m in manifests if m['kind'] == kind and m['metadata']['name'] == f'vf-demo-run1-{node}')


def test_rollout_policy_renders_the_deployment_strategy():
    assert rollout_strategy('drain') == {'type': 'Recreate'}
    assert rollout_strategy('surge') == {'type': 'RollingUpdate', 'rollingUpdate': {'maxSurge': 1, 'maxUnavailable': 0}}
    with pytest.raises(ValueError):
        rollout_strategy('yolo')
    assert 'strategy' not in _workload(_render(), 'detector')['spec']
    assert _workload(_render(rollout_policy = 'drain'), 'detector')['spec']['strategy'] == {'type': 'Recreate'}
    # Every Deployment of the flow, GPU or not.
    assert _workload(_render(rollout_policy = 'surge'), 'sink')['spec']['strategy']['type'] == 'RollingUpdate'


def test_gpu_nodes_pin_gpu_pods_by_hostname_and_leave_cpu_pods_alone():
    pod = _workload(_render(gpu_nodes = ['gpu-01', 'gpu-03']), 'detector')['spec']['template']['spec']
    terms = pod['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms']
    assert terms == [{'matchExpressions': [{'key': 'kubernetes.io/hostname', 'operator': 'In',
                                            'values': ['gpu-01', 'gpu-03']}]}]
    assert pod['nodeSelector'] == {'videoflow.io/gpu-pool': 'true'}          # the pool label still applies
    assert 'affinity' not in _workload(_render(gpu_nodes = ['gpu-01']), 'sink')['spec']['template']['spec']


def test_gpu_nodes_are_anded_into_the_mix_modes_ownership_terms():
    pod = _workload(_render(gpu_nodes = ['gpu-01'], gpu_mode = 'mix'), 'detector')['spec']['template']['spec']
    terms = pod['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms']
    assert len(terms) == 2                                                   # unowned OR owned-by-us ...
    for term in terms:
        assert term['matchExpressions'][-1] == {'key': 'kubernetes.io/hostname', 'operator': 'In', 'values': ['gpu-01']}


def test_resources_render_requests_and_limits_beside_the_gpu_limit():
    resources = {'*': {'cpu': '500m', 'memory': '1Gi'}, 'detector': {'memory': '4Gi', 'memory_limit': '8Gi'}}
    manifests = _render(resources = resources)
    detector = _workload(manifests, 'detector')['spec']['template']['spec']['containers'][0]['resources']
    assert detector == {'limits': {'nvidia.com/gpu': 1, 'memory': '8Gi'}, 'requests': {'cpu': '500m', 'memory': '4Gi'}}
    sink = _workload(manifests, 'sink')['spec']['template']['spec']['containers'][0]['resources']
    assert sink == {'requests': {'cpu': '500m', 'memory': '1Gi'}}
    assert 'resources' not in _workload(_render(), 'sink')['spec']['template']['spec']['containers'][0]


def test_descriptor_host_resources_are_the_defaults_under_the_operators():
    spec = NodeSpec('det', None, {}, [], 'processor', True, 1, 'cpu', True,
                    descriptor = {'spec': {'resources': {'cpu': 2, 'memory': '2Gi'}}})
    assert host_resources_for(spec, None) == {'cpu': '2', 'memory': '2Gi'}
    assert host_resources_for(spec, {'*': {'cpu': '1'}}) == {'cpu': '1', 'memory': '2Gi'}
    assert host_resources_for(spec, {'*': {'cpu': '1'}, 'det': {'cpu': '4', 'cpu_limit': '4'}}) == {
        'cpu': '4', 'memory': '2Gi', 'cpu_limit': '4'}


def test_parse_resources_names_the_fix():
    assert parse_resources(['*=cpu:500m,memory:1Gi', 'detector=memory_limit:4Gi']) == {
        '*': {'cpu': '500m', 'memory': '1Gi'}, 'detector': {'memory_limit': '4Gi'}}
    assert parse_resources(None) == {}
    for bad in ('cpu:1', 'det=', 'det=gpu:1', 'det=cpu'):
        with pytest.raises(Exception) as excinfo:
            parse_resources([bad])
        assert 'resources' in str(excinfo.value)


def test_dra_mode_renders_claim_templates_and_pod_fragments():
    manifests = _render(gpu_mode = 'dra')
    template = next(m for m in manifests if m['kind'] == 'ResourceClaimTemplate')
    assert template['apiVersion'] == 'resource.k8s.io/v1'
    assert template['metadata'] == {'name': 'vf-gpu-detector-template', 'namespace': 'default',
                                    'labels': {'videoflow.io/flow-id': 'demo', 'app.kubernetes.io/managed-by': 'videoflow',
                                               'videoflow.io/node': 'detector', 'videoflow.io/run-id': 'run1'}}
    assert template['spec']['spec']['devices']['requests'] == [
        {'name': 'gpu', 'exactly': {'deviceClassName': 'gpu.nvidia.com', 'allocationMode': 'ExactCount', 'count': 1}}]
    pod = _workload(manifests, 'detector')['spec']['template']['spec']
    assert pod['resourceClaims'] == [{'name': 'vf-gpu-detector', 'resourceClaimTemplateName': 'vf-gpu-detector-template'}]
    assert pod['containers'][0]['resources'] == {'claims': [{'name': 'vf-gpu-detector'}]}
    # The template precedes the workload that instantiates it, and CPU nodes get none.
    kinds = [m['kind'] for m in manifests]
    assert kinds.index('ResourceClaimTemplate') < kinds.index('Deployment')
    assert kinds.count('ResourceClaimTemplate') == 1
    assert 'resourceClaims' not in _workload(manifests, 'sink')['spec']['template']['spec']


def test_rollout_admission_refuses_surge_without_spare_devices():
    from videoflow.backends.outcomes import known, unknown
    from videoflow.deploy.admission import rollout_problems
    specs = compile_flow(_flow())
    assert rollout_problems('surge', specs, 'realtime', known(1)) == []
    (problem,) = rollout_problems('surge', specs, 'realtime', known(0))
    assert 'needs 1 spare GPU device' in problem and 'drain' in problem
    (problem,) = rollout_problems('surge', specs, 'realtime', unknown('timeout', 'slow'))
    assert 'could not be observed' in problem
    assert rollout_problems('drain', specs, 'realtime', known(0)) == []
    assert 'no --rollout-policy declared' in rollout_problems(None, specs, 'realtime', known(0))[0]
    assert rollout_problems(None, specs, 'realtime', known(3)) == []
    assert rollout_problems('surge', specs, 'batch', known(0)) == []      # a BATCH GPU processor is a Job: no rollout


def test_replica_admission_keeps_desired_admitted_and_ready_apart():
    from videoflow.deploy.admission import replica_admission
    fits_two = lambda n: [] if n <= 2 else [f'{n} replicas need {n} devices; 2 free']   # noqa: E731
    decision = replica_admission(10, fits_two, ready = 1)
    assert (decision.desired, decision.admitted, decision.ready, decision.unadmitted) == (10, 2, 1, 8)
    assert '10 replicas need 10 devices' in decision.reasons[0]
    assert replica_admission(2, fits_two, ready = 5).ready == 2                 # never more ready than admitted
    assert replica_admission(3, lambda n: ['none free'], ready = 0).admitted == 0
