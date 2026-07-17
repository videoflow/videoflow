'''
Unit tests for the distributed compile + manifest-generation path. No broker or
cluster required — these exercise pure transformation logic.
'''
import json

import pytest
import yaml

from videoflow.compiler import NODE_KIND_CONSUMER, NODE_KIND_PROCESSOR, NODE_KIND_PRODUCER, compile_flow
from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.constants import GPU, REALTIME
from videoflow.images import parse_override, resolve_image
from videoflow.manifests import dump_manifests, render_manifests
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.producers import IntProducer
from videoflow.producers.video import VideoFileReader

IMG = 'ghcr.io/acme/app:v1'  # a default image for manifest-render tests

def _demo_flow(flow_id = 'demo'):
    producer = IntProducer(0, 40, 0.1, name = 'producer')
    identity = IdentityProcessor(name = 'identity', nb_tasks = 2)(producer)
    identity1 = IdentityProcessor(name = 'identity1')(identity)
    joined = JoinerProcessor(name = 'joined')(identity, identity1)
    printer = CommandlineConsumer(name = 'printer')(joined)
    return Flow([printer], flow_type = REALTIME, flow_id = flow_id)

def test_compile_flow_specs():
    specs = {s.name: s for s in compile_flow(_demo_flow())}
    assert set(specs) == {'producer', 'identity', 'identity1', 'joined', 'printer'}
    assert specs['producer'].kind == NODE_KIND_PRODUCER
    assert specs['identity'].kind == NODE_KIND_PROCESSOR
    assert specs['printer'].kind == NODE_KIND_CONSUMER
    assert specs['identity'].nb_tasks == 2
    assert specs['joined'].parents == ['identity', 'identity1']
    assert specs['printer'].has_children is False
    assert specs['producer'].has_children is True

def test_spec_params_are_json_serializable():
    for s in compile_flow(_demo_flow()):
        json.dumps(s.params)  # must not raise

def test_resolve_image_order():
    # override > node image= > --image default; raises if none.
    assert resolve_image('n', 'node-img', 'default-img', {'n': 'ovr'}) == 'ovr'
    assert resolve_image('n', 'node-img', 'default-img', {}) == 'node-img'
    assert resolve_image('n', None, 'default-img', {}) == 'default-img'
    with pytest.raises(ValueError, match = 'no container image'):
        resolve_image('n', None, None, {})

def test_parse_override():
    assert parse_override('det=ghcr.io/me/gpu:v1') == ('det', 'ghcr.io/me/gpu:v1')
    with pytest.raises(ValueError):
        parse_override('no-equals')

def test_node_declared_image_flows_to_spec():
    p = IntProducer(0, 5, name = 'producer', image = 'ghcr.io/me/prod:v1')
    printer = CommandlineConsumer(name = 'printer')(p)
    specs = {s.name: s for s in compile_flow(Flow([printer], flow_id = 'img'))}
    assert specs['producer'].image == 'ghcr.io/me/prod:v1'
    assert specs['printer'].image is None  # no declared image → uses the default at deploy

def test_render_requires_an_image():
    # No node image and no --image default → actionable error, no partial manifests.
    specs = compile_flow(_demo_flow())
    with pytest.raises(ValueError, match = 'no container image'):
        render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1')

def test_default_image_and_override_applied_to_pods():
    p = IntProducer(0, 5, name = 'producer')
    a = IdentityProcessor(name = 'identity')(p)
    printer = CommandlineConsumer(name = 'printer')(a)
    specs = compile_flow(Flow([printer], flow_type = REALTIME, flow_id = 'demo'))
    manifests = render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1',
                                default_image = IMG, image_overrides = {'identity': 'ghcr.io/acme/gpu:v1'})
    images = {}
    for m in manifests:
        if m['kind'] in ('Deployment', 'Job') and m['metadata']['name'].startswith('vf-demo-') \
                and m['metadata']['name'] != 'vf-demo-provision':
            images[m['metadata']['name']] = m['spec']['template']['spec']['containers'][0]['image']
    assert images['vf-demo-producer'] == IMG            # default
    assert images['vf-demo-printer'] == IMG             # default
    assert images['vf-demo-identity'] == 'ghcr.io/acme/gpu:v1'  # override wins

def test_finite_producer_is_job_infinite_is_deployment():
    # finite producer
    specs = compile_flow(_demo_flow())
    manifests = render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1',
                                namespace = 'ns', default_image = IMG)
    by_name = {(m['kind'], m['metadata']['name']): m for m in manifests}
    assert ('Job', 'vf-demo-producer') in by_name
    # processors and consumer are Deployments
    assert ('Deployment', 'vf-demo-identity') in by_name
    assert ('Deployment', 'vf-demo-printer') in by_name

def test_nb_tasks_maps_to_replicas():
    specs = compile_flow(_demo_flow())
    manifests = render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1', default_image = IMG)
    dep = [m for m in manifests if m['kind'] == 'Deployment' and m['metadata']['name'] == 'vf-demo-identity'][0]
    assert dep['spec']['replicas'] == 2

def test_gpu_node_resources():
    producer = IntProducer(name = 'p')
    gpu = IdentityProcessor(name = 'g', device_type = GPU)(producer)
    printer = CommandlineConsumer(name = 'c')(gpu)
    flow = Flow([printer], flow_type = REALTIME, flow_id = 'g')
    manifests = render_manifests(compile_flow(flow), 'g', 'realtime', 'nats://x:4222', 'run1', default_image = IMG)
    dep = [m for m in manifests if m['kind'] == 'Deployment' and m['metadata']['name'] == 'vf-g-g'][0]
    container = dep['spec']['template']['spec']['containers'][0]
    assert container['resources']['limits']['nvidia.com/gpu'] == 1
    assert dep['spec']['template']['spec']['nodeSelector'] == {'videoflow.io/gpu-pool': 'true'}

def test_manifests_are_valid_yaml():
    specs = compile_flow(_demo_flow())
    manifests = render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1',
                                default_image = IMG, autoscaling = True)
    ystr = dump_manifests(manifests)
    parsed = list(yaml.safe_load_all(ystr))
    assert len(parsed) == len(manifests)
    scaled = [m for m in parsed if m['kind'] == 'ScaledObject']
    assert len(scaled) == 3  # identity, identity1, joined

def test_video_file_reader_is_finite():
    reader = VideoFileReader('/tmp/x.mp4', name = 'reader')
    printer = CommandlineConsumer(name = 'printer')(reader)
    flow = Flow([printer], flow_type = REALTIME, flow_id = 'v')
    specs = {s.name: s for s in compile_flow(flow)}
    assert specs['reader'].is_finite is True
    assert specs['reader'].image is None  # image is chosen at deploy, not inferred

if __name__ == "__main__":
    pytest.main([__file__])
