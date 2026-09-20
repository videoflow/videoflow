'''
Unit tests for the distributed compile + manifest-generation path. No broker or
cluster required — these exercise pure transformation logic.
'''
import json

import pytest

from videoflow.consumers import CommandlineConsumer
from videoflow.core import Flow
from videoflow.core.compiler import NODE_KIND_CONSUMER, NODE_KIND_PROCESSOR, NODE_KIND_PRODUCER, compile_flow
from videoflow.core.constants import BATCH, GPU, REALTIME
from videoflow.core.policies import MISSING_DROP, MISSING_WAIT, JoinPolicy
from videoflow.deploy.images import parse_override, resolve_image
from videoflow.deploy.manifests import dump_manifests, render_manifests
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

def test_blob_readers_counts_downstream_broker_consumers():
    # Mirrors provision_flow's durable arithmetic (PROTOCOL.md BLOB-5): one durable
    # per non-partitioned child (replicas compete), one per replica of a
    # partitioned child (each replica decodes every message).
    specs = {s.name: s for s in compile_flow(_demo_flow())}
    # producer → identity (nb_tasks=2 but NOT partitioned: replicas compete) = 1
    assert specs['producer'].blob_readers == 1
    # identity → identity1 + joined = 2 children, neither partitioned
    assert specs['identity'].blob_readers == 2
    assert specs['identity1'].blob_readers == 1
    assert specs['joined'].blob_readers == 1
    # leaf publishes nothing
    assert specs['printer'].blob_readers == 0

def test_blob_readers_counts_each_partitioned_replica():
    p = IntProducer(0, 5, name = 'p')
    fan = IdentityProcessor(name = 'fan', nb_tasks = 3, partition_by = 'trace_id')(p)
    plain = IdentityProcessor(name = 'plain')(p)
    printer = CommandlineConsumer(name = 'printer')(fan)
    printer2 = CommandlineConsumer(name = 'printer2')(plain)
    flow = Flow([printer, printer2], flow_type = REALTIME, flow_id = 'demo')
    specs = {s.name: s for s in compile_flow(flow)}
    # partitioned child contributes nb_tasks reads; plain sibling contributes 1.
    assert specs['p'].blob_readers == 4

@pytest.mark.parametrize('field, value, legacy_default', [
    # A spec serialized before RFC 0002 has no blob_readers key: reclamation off.
    ('blob_readers', 2, None),
    # Old serialized specs (no GPU fields) load with the defaults.
    ('gpu_count', 4, 1),
    ('gpu_resource_name', 'amd.com/gpu', None),     # strategy-set, so stamped post-compile
    ('gpu_memory_gib', 10, None),
])
def test_spec_fields_round_trip_and_legacy_specs_take_the_default(field, value, legacy_default):
    # The flow-spec ConfigMap round-trips specs as JSON; every field must survive,
    # and a spec written before the field existed must load with its default.
    from videoflow.core.compiler import NodeSpec
    spec = compile_flow(_demo_flow())[1]
    setattr(spec, field, value)
    clone = NodeSpec.from_dict(json.loads(json.dumps(spec.to_dict())))
    assert getattr(clone, field) == value
    old = {k: v for k, v in spec.to_dict().items() if k != field}
    assert getattr(NodeSpec.from_dict(old), field) == legacy_default

def test_blob_ttl_override_reaches_the_broker_configmap():
    # The per-node reader counts and ids are pinned by the manifest goldens; the
    # TTL override is the one thing no golden passes.
    specs = compile_flow(_demo_flow())
    manifests = render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1',
                                default_image = IMG, blob_redis_url = 'redis://r:6379/0',
                                blob_ttl_seconds = 300)
    # The shared broker ConfigMap carries the TTL override (BLOB-7).
    broker_cm = next(m for m in manifests if m['kind'] == 'ConfigMap'
                     and 'VF_NATS_URL' in m.get('data', {}))
    assert broker_cm['data']['VF_BLOB_TTL_SECONDS'] == '300'
    # Without an override the var is absent — workers use the flow-type default.
    manifests_no_ttl = render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1',
                                        default_image = IMG)
    broker_cm2 = next(m for m in manifests_no_ttl if m['kind'] == 'ConfigMap'
                      and 'VF_NATS_URL' in m.get('data', {}))
    assert 'VF_BLOB_TTL_SECONDS' not in broker_cm2['data']

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
        if m['kind'] in ('Deployment', 'Job') and m['metadata']['name'].startswith('vf-demo-run1-') \
                and m['metadata']['name'] != 'vf-demo-run1-provision':
            images[m['metadata']['name']] = m['spec']['template']['spec']['containers'][0]['image']
    assert images['vf-demo-run1-producer'] == IMG            # default
    assert images['vf-demo-run1-printer'] == IMG             # default
    assert images['vf-demo-run1-identity'] == 'ghcr.io/acme/gpu:v1'  # override wins

def _pull_policies(manifests):
    '''Every container's imagePullPolicy, keyed by resource name (workers + provision Job).'''
    policies = {}
    for m in manifests:
        if m['kind'] in ('Deployment', 'Job', 'StatefulSet'):
            container = m['spec']['template']['spec']['containers'][0]
            policies[m['metadata']['name']] = container.get('imagePullPolicy')
    return policies

def test_provision_job_pull_policy_matches_the_workers():
    # The provision Job runs first: if it alone cannot pull, the only symptom is the
    # provision-wait timeout, with no worker ever started.
    specs = compile_flow(_demo_flow())
    manifests = render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1',
                                default_image = IMG, image_pull_policy = 'Always')
    policies = _pull_policies(manifests)
    assert policies['vf-demo-run1-provision'] == 'Always'
    assert set(policies.values()) == {'Always'}

def test_invalid_image_pull_policy_names_the_valid_values():
    specs = compile_flow(_demo_flow())
    with pytest.raises(ValueError) as e:
        render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1',
                        default_image = IMG, image_pull_policy = 'ifnotpresent')
    assert 'IfNotPresent' in str(e.value)

def test_gpu_pods_get_no_runtime_class_unless_asked():
    # The GPU limits and pool selector are pinned by the manifest goldens, which all
    # pass a runtime class; what none of them shows is the default.
    producer = IntProducer(name = 'p')
    gpu = IdentityProcessor(name = 'g', device_type = GPU)(producer)
    printer = CommandlineConsumer(name = 'c')(gpu)
    flow = Flow([printer], flow_type = REALTIME, flow_id = 'g')
    manifests = render_manifests(compile_flow(flow), 'g', 'realtime', 'nats://x:4222', 'run1', default_image = IMG)
    dep = [m for m in manifests if m['kind'] == 'Deployment' and m['metadata']['name'] == 'vf-g-run1-g'][0]
    # No runtimeClassName unless asked for: on a cluster whose node default already is
    # the NVIDIA runtime, naming a class that doesn't exist would fail the pod.
    assert 'runtimeClassName' not in dep['spec']['template']['spec']


def _gpu_flow(**gpu_kwargs):
    producer = IntProducer(name = 'p')
    gpu = IdentityProcessor(name = 'g', device_type = GPU, **gpu_kwargs)(producer)
    printer = CommandlineConsumer(name = 'c')(gpu)
    return Flow([printer], flow_type = REALTIME, flow_id = 'g')

def test_strategy_resolved_resource_name_wins_over_the_deploy_default():
    # NodeSpec.gpu_resource_name is internal: only a GPU strategy sets it (the mix
    # solver's chosen MIG profile). When set, it wins over --gpu-resource-name.
    specs = compile_flow(_gpu_flow())
    next(s for s in specs if s.name == 'g').gpu_resource_name = 'nvidia.com/mig-1g.10gb'
    manifests = render_manifests(specs, 'g', 'realtime', 'nats://x:4222', 'run1',
                                default_image = IMG, gpu_resource_name = 'amd.com/gpu')
    dep = [m for m in manifests if m['kind'] == 'Deployment' and m['metadata']['name'] == 'vf-g-run1-g'][0]
    limits = dep['spec']['template']['spec']['containers'][0]['resources']['limits']
    assert limits == {'nvidia.com/mig-1g.10gb': 1}
    manifests = render_manifests(compile_flow(_gpu_flow()), 'g', 'realtime', 'nats://x:4222', 'run1',
                                default_image = IMG, gpu_resource_name = 'amd.com/gpu')
    dep = [m for m in manifests if m['kind'] == 'Deployment' and m['metadata']['name'] == 'vf-g-run1-g'][0]
    limits = dep['spec']['template']['spec']['containers'][0]['resources']['limits']
    assert limits == {'amd.com/gpu': 1}

def test_render_rejects_a_multi_gpu_grant_against_a_mig_resource():
    # Bug 2 regression: impossible by construction (a model cannot span MIG
    # slices), so it is a hard render error — not a skippable preflight warning.
    with pytest.raises(ValueError, match = 'MIG'):
        render_manifests(compile_flow(_gpu_flow(gpu_count = 2)), 'g', 'realtime',
                        'nats://x:4222', 'run1', default_image = IMG,
                        gpu_resource_name = 'nvidia.com/mig-3g.40gb')

def test_gpu_max_per_pod_takes_the_max_not_the_sum():
    from videoflow.deploy.manifests import gpu_demand, gpu_max_per_pod
    producer = IntProducer(name = 'p')
    small = IdentityProcessor(name = 'small', device_type = GPU, nb_tasks = 3)(producer)
    big = IdentityProcessor(name = 'big', device_type = GPU, gpu_count = 4)(small)
    printer = CommandlineConsumer(name = 'c')(big)
    specs = compile_flow(Flow([printer], flow_type = REALTIME, flow_id = 'g'))
    # Demand sums every replica's claim; max-per-pod is the biggest single claim.
    assert gpu_demand(specs) == {'nvidia.com/gpu': 3 + 4}
    assert gpu_max_per_pod(specs) == {'nvidia.com/gpu': 4}
    # Both group by resolved resource name, honoring the deploy default.
    assert gpu_max_per_pod(specs, default_resource = 'amd.com/gpu') == {'amd.com/gpu': 4}


def test_env_pairs_carry_gpu_grant_for_gpu_nodes():
    from videoflow.deploy.manifests import _env_pairs
    specs = {s.name: s for s in compile_flow(_gpu_flow(gpu_count = 2))}
    specs['g'].gpu_resource_name = 'amd.com/gpu'   # strategy-resolved (internal)
    env = _env_pairs(specs['g'], 'g', 'realtime', 'run1', 4)
    assert env['VF_GPU_COUNT'] == '2'
    assert env['VF_GPU_RESOURCE_NAME'] == 'amd.com/gpu'
    # CPU nodes carry neither; a GPU node without a resource name only the count.
    cpu_env = _env_pairs(specs['c'], 'g', 'realtime', 'run1', 4)
    assert 'VF_GPU_COUNT' not in cpu_env and 'VF_GPU_RESOURCE_NAME' not in cpu_env
    plain = {s.name: s for s in compile_flow(_gpu_flow())}
    plain_env = _env_pairs(plain['g'], 'g', 'realtime', 'run1', 4)
    assert plain_env['VF_GPU_COUNT'] == '1'
    assert 'VF_GPU_RESOURCE_NAME' not in plain_env


def test_invalid_gpu_mode_is_rejected():
    with pytest.raises(ValueError, match = 'gpu_mode'):
        render_manifests(compile_flow(_gpu_flow()), 'g', 'realtime', 'nats://x:4222', 'run1',
                        default_image = IMG, gpu_mode = 'fractional')

def test_gpu_memory_gib_is_validated_and_reaches_the_spec():
    # RFC 0004: a sharer declares memory; the compiler carries it to the spec.
    flow = _gpu_flow(gpu_memory_gib = 10)
    spec = next(s for s in compile_flow(flow) if s.name == 'g')
    assert spec.gpu_memory_gib == 10
    # Validation: positive number, GPU-only, and never combined with a span.
    with pytest.raises(ValueError, match = 'gpu_memory_gib'):
        IdentityProcessor(name = 'g', device_type = GPU, gpu_memory_gib = 0)
    with pytest.raises(ValueError, match = 'device_type=GPU'):
        IdentityProcessor(name = 'g', gpu_memory_gib = 10)
    with pytest.raises(ValueError, match = 'mutually exclusive'):
        IdentityProcessor(name = 'g', device_type = GPU, gpu_count = 2, gpu_memory_gib = 10)
    node = IdentityProcessor(name = 'g', device_type = GPU, gpu_memory_gib = 10)
    with pytest.raises(ValueError, match = 'gpu_memory_gib'):
        node.change_device('cpu')

def test_gpu_kwargs_are_validated():
    with pytest.raises(ValueError, match = 'gpu_count'):
        IdentityProcessor(name = 'g', device_type = GPU, gpu_count = 0)
    # Bug 7 regression: a GPU grant on a CPU node is a build error, not a silent no-op.
    with pytest.raises(ValueError, match = 'device_type=GPU'):
        IdentityProcessor(name = 'g', gpu_count = 4)
    # ...including via change_device after construction.
    node = IdentityProcessor(name = 'g', device_type = GPU, gpu_count = 2)
    with pytest.raises(ValueError, match = 'gpu_count=2'):
        node.change_device('cpu')

def test_autoscaling_skips_gpu_nodes_by_default():
    producer = IntProducer(name = 'p')
    gpu = IdentityProcessor(name = 'g', device_type = GPU)(producer)
    cpu = IdentityProcessor(name = 'k')(gpu)
    printer = CommandlineConsumer(name = 'c')(cpu)
    flow = Flow([printer], flow_type = REALTIME, flow_id = 'g')
    manifests = render_manifests(compile_flow(flow), 'g', 'realtime', 'nats://x:4222', 'run1',
                                default_image = IMG, autoscaling = True)
    scaled = {m['metadata']['name'] for m in manifests if m['kind'] == 'ScaledObject'}
    # The CPU processor autoscales; the GPU one does not (each extra replica would
    # claim its own whole GPU and can strand the flow Pending).
    assert scaled == {'vf-g-run1-k-scaler'}
    manifests = render_manifests(compile_flow(flow), 'g', 'realtime', 'nats://x:4222', 'run1',
                                default_image = IMG, autoscaling = True, gpu_autoscaling = True)
    scaled = {m['metadata']['name'] for m in manifests if m['kind'] == 'ScaledObject'}
    assert scaled == {'vf-g-run1-g-scaler', 'vf-g-run1-k-scaler'}

# -- partitioning: policy, compiled specs, rendered workload -----------------
#
# A partitioned node is the one shape whose identity has to survive all three
# hops: JoinPolicy round-tripping through get_params() (the worker rebuilds the
# node from them), compile_flow carrying partition_by/nb_tasks into the spec, and
# render_manifests turning that into a StatefulSet with a stable replica id. The
# end-to-end "each message handled by exactly one replica" check needs a broker
# and lives in tests/integration/local/test_partitioning.py.

def _partitioned_flow():
    p = IntProducer(0, 5, name = 'producer')
    a = IdentityProcessor(name = 'a')(p)
    joined = JoinerProcessor(name = 'joined', nb_tasks = 3, partition_by = 'trace_id')(p, a)
    out = CommandlineConsumer(name = 'out')(joined)
    return Flow([out], flow_type = REALTIME, flow_id = 'part')

def test_join_policy_round_trips_through_get_params():
    j = JoinerProcessor(name = 'j', nb_tasks = 2, partition_by = 'trace_id',
                        join_policy = JoinPolicy(timeout_seconds = 5, missing = MISSING_DROP))
    params = j.get_params()
    json.dumps(params)  # must be JSON-serializable
    j2 = JoinerProcessor(**params)
    assert j2.partition_by == 'trace_id'
    assert j2.join_policy.timeout_seconds == 5
    assert j2.join_policy.missing == MISSING_DROP

def test_join_policy_defaults_per_flow_type():
    assert JoinPolicy.default_for(BATCH).missing == MISSING_WAIT
    assert JoinPolicy.default_for(REALTIME).timeout_seconds == 10.0

def test_compiler_carries_partition_and_join_policy():
    specs = {s.name: s for s in compile_flow(_partitioned_flow())}
    assert specs['joined'].partition_by == 'trace_id'
    assert specs['joined'].nb_tasks == 3

def test_video_file_reader_is_finite():
    reader = VideoFileReader('/tmp/x.mp4', name = 'reader')
    printer = CommandlineConsumer(name = 'printer')(reader)
    flow = Flow([printer], flow_type = REALTIME, flow_id = 'v')
    specs = {s.name: s for s in compile_flow(flow)}
    assert specs['reader'].is_finite is True
    assert specs['reader'].image is None  # image is chosen at deploy, not inferred

if __name__ == "__main__":
    pytest.main([__file__])


def _provision_env(manifests):
    job = next(m for m in manifests if m['kind'] == 'Job' and m['metadata']['name'].endswith('-provision'))
    return {e['name']: e['value'] for e in job['spec']['template']['spec']['containers'][0]['env']}

def test_stream_replicas_reach_the_provision_job_only_for_a_replicated_profile():
    specs = compile_flow(_demo_flow())
    plain = render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1', default_image = IMG)
    assert 'VF_STREAM_REPLICAS' not in _provision_env(plain)
    replicated = render_manifests(specs, 'demo', 'realtime', 'nats://x:4222', 'run1', default_image = IMG,
                                  stream_replicas = 3)
    assert _provision_env(replicated)['VF_STREAM_REPLICAS'] == '3'
    # Everything else is byte-identical: only the provision Job's env differs.
    assert dump_manifests(plain) != dump_manifests(replicated)
    assert [m for m in plain if m['kind'] != 'Job'] == [m for m in replicated if m['kind'] != 'Job']
