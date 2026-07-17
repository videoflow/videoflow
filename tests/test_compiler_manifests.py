'''
Unit tests for the distributed compile + manifest-generation path. No broker or
cluster required — these exercise pure transformation logic.
'''
import json

import pytest
import yaml

from videoflow.core import Flow
from videoflow.core.constants import REALTIME, GPU
from videoflow.producers import IntProducer
from videoflow.producers.video import VideoFileReader
from videoflow.processors import IdentityProcessor, JoinerProcessor
from videoflow.consumers import CommandlineConsumer
from videoflow.compiler import compile_flow, NODE_KIND_PRODUCER, NODE_KIND_PROCESSOR, NODE_KIND_CONSUMER
from videoflow.image_registry import image_family_for, set_override
from videoflow.manifests import render_manifests, dump_manifests

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

def test_image_family_resolution():
    assert image_family_for('videoflow.processors.vision.detectors.ObjectDetector') == 'vision'
    assert image_family_for('videoflow.producers.video.VideoFileReader') == 'video-io'
    assert image_family_for('videoflow.processors.basic.IdentityProcessor') == 'basic'
    assert image_family_for('some.unknown.CustomNode') == 'basic'  # default fallback

def test_image_family_override():
    set_override('mynode', 'vision')
    assert image_family_for('some.unknown.CustomNode', 'mynode') == 'vision'

def test_finite_producer_is_job_infinite_is_deployment():
    # finite producer
    specs = compile_flow(_demo_flow())
    manifests = render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1', namespace = 'ns')
    by_name = {(m['kind'], m['metadata']['name']): m for m in manifests}
    assert ('Job', 'vf-demo-producer') in by_name
    # processors and consumer are Deployments
    assert ('Deployment', 'vf-demo-identity') in by_name
    assert ('Deployment', 'vf-demo-printer') in by_name

def test_nb_tasks_maps_to_replicas():
    specs = compile_flow(_demo_flow())
    manifests = render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1')
    dep = [m for m in manifests if m['kind'] == 'Deployment' and m['metadata']['name'] == 'vf-demo-identity'][0]
    assert dep['spec']['replicas'] == 2

def test_gpu_node_resources():
    producer = IntProducer(name = 'p')
    gpu = IdentityProcessor(name = 'g', device_type = GPU)(producer)
    printer = CommandlineConsumer(name = 'c')(gpu)
    flow = Flow([printer], flow_type = REALTIME, flow_id = 'g')
    manifests = render_manifests(compile_flow(flow), 'g', 'realtime', 'nats://x:4222', 'run1')
    dep = [m for m in manifests if m['kind'] == 'Deployment' and m['metadata']['name'] == 'vf-g-g'][0]
    container = dep['spec']['template']['spec']['containers'][0]
    assert container['resources']['limits']['nvidia.com/gpu'] == 1
    assert dep['spec']['template']['spec']['nodeSelector'] == {'videoflow.io/gpu-pool': 'true'}

def test_manifests_are_valid_yaml():
    specs = compile_flow(_demo_flow())
    manifests = render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1', autoscaling = True)
    ystr = dump_manifests(manifests)
    parsed = list(yaml.safe_load_all(ystr))
    assert len(parsed) == len(manifests)
    scaled = [m for m in parsed if m['kind'] == 'ScaledObject']
    assert len(scaled) == 3  # identity, identity1, joined

def test_video_file_reader_is_finite_job():
    reader = VideoFileReader('/tmp/x.mp4', name = 'reader')
    printer = CommandlineConsumer(name = 'printer')(reader)
    flow = Flow([printer], flow_type = REALTIME, flow_id = 'v')
    specs = {s.name: s for s in compile_flow(flow)}
    assert specs['reader'].is_finite is True
    assert specs['reader'].image_family == 'video-io'

if __name__ == "__main__":
    pytest.main([__file__])
