'''
Renders Kubernetes manifests for a compiled flow (a list of ``NodeSpec``). One
workload per node:

  - finite producers      -> a Job (runs to completion when the source is exhausted)
  - everything else       -> a Deployment (stays up until the control-channel stop)

plus a per-node ConfigMap holding the node's env, a shared ConfigMap for the NATS
URL, and a default-deny-except-broker NetworkPolicy.

Manifests are built as plain dicts and serialized with ``yaml.dump`` rather than
text-templated, so the output is always structurally valid YAML.
'''
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import json
import re

import yaml

from .compiler import NODE_KIND_PRODUCER

LABEL_FLOW_ID = 'videoflow.io/flow-id'
LABEL_NODE = 'videoflow.io/node'
LABEL_MANAGED_BY = 'app.kubernetes.io/managed-by'

_DNS1123_RE = re.compile(r'[^a-z0-9-]+')

def k8s_name(*parts) -> str:
    '''Joins parts into a DNS-1123-safe Kubernetes resource name.'''
    raw = '-'.join(str(p) for p in parts).lower()
    name = _DNS1123_RE.sub('-', raw).strip('-')
    return name[:63].strip('-')

def _image_ref(image_family : str, registry : str, tag : str) -> str:
    prefix = registry.rstrip('/') + '/' if registry else ''
    return f'{prefix}videoflow-{image_family}:{tag}'

def _env_pairs(spec, flow_id, flow_type):
    return {
        'VF_NODE_CLASS': spec.node_class,
        'VF_NODE_PARAMS_JSON': json.dumps(spec.params),
        'VF_NODE_KIND': spec.kind,
        'VF_NODE_NAME': spec.name,
        'VF_PARENT_NAMES': ','.join(spec.parents),
        'VF_HAS_CHILDREN': '1' if spec.has_children else '0',
        'VF_FLOW_ID': flow_id,
        'VF_FLOW_TYPE': flow_type,
        # In-cluster each pod has its own IP, so a fixed health port is safe and
        # matches the probe/containerPort below.
        'VF_HEALTH_PORT': '8080',
    }

def _labels(flow_id, node_name = None):
    labels = {LABEL_FLOW_ID: k8s_name(flow_id), LABEL_MANAGED_BY: 'videoflow'}
    if node_name is not None:
        labels[LABEL_NODE] = k8s_name(node_name)
    return labels

def _pod_spec(spec, flow_id, flow_type, registry, tag, nats_configmap):
    container = {
        'name': 'worker',
        'image': _image_ref(spec.image_family, registry, tag),
        'envFrom': [
            {'configMapRef': {'name': k8s_name('vf', flow_id, spec.name, 'env')}},
            {'configMapRef': {'name': nats_configmap}},
        ],
    }

    resources = {}
    node_selector = None
    tolerations = None
    if spec.device_type == 'gpu':
        resources['limits'] = {'nvidia.com/gpu': 1}
        node_selector = {'videoflow.io/gpu-pool': 'true'}
        tolerations = [{
            'key': 'nvidia.com/gpu', 'operator': 'Exists', 'effect': 'NoSchedule',
        }]
    if resources:
        container['resources'] = resources

    # Readiness/liveness probes hit the worker's health server (Phase 6): ready
    # only after node.open() completes, live while the run loop keeps beating.
    container['readinessProbe'] = {
        'httpGet': {'path': '/readyz', 'port': 8080},
        'initialDelaySeconds': 2, 'periodSeconds': 5,
    }
    container['livenessProbe'] = {
        'httpGet': {'path': '/healthz', 'port': 8080},
        'initialDelaySeconds': 10, 'periodSeconds': 15,
    }
    container['ports'] = [{'containerPort': 8080, 'name': 'health'}]

    pod_spec = {'containers': [container]}
    if node_selector:
        pod_spec['nodeSelector'] = node_selector
    if tolerations:
        pod_spec['tolerations'] = tolerations
    return pod_spec

def node_configmap(spec, flow_id, flow_type):
    return {
        'apiVersion': 'v1',
        'kind': 'ConfigMap',
        'metadata': {
            'name': k8s_name('vf', flow_id, spec.name, 'env'),
            'labels': _labels(flow_id, spec.name),
        },
        'data': _env_pairs(spec, flow_id, flow_type),
    }

def nats_configmap(flow_id, nats_url, blob_redis_url = None):
    data = {'VF_NATS_URL': nats_url}
    if blob_redis_url:
        data['VF_BLOB_REDIS_URL'] = blob_redis_url
    return {
        'apiVersion': 'v1',
        'kind': 'ConfigMap',
        'metadata': {
            'name': k8s_name('vf', flow_id, 'broker'),
            'labels': _labels(flow_id),
        },
        'data': data,
    }

def workload(spec, flow_id, flow_type, registry, tag, nats_cm_name):
    labels = _labels(flow_id, spec.name)
    pod_template = {
        'metadata': {'labels': labels},
        'spec': _pod_spec(spec, flow_id, flow_type, registry, tag, nats_cm_name),
    }

    is_job = spec.kind == NODE_KIND_PRODUCER and spec.is_finite
    if is_job:
        # A finite producer completes; its worker pod should not be restarted on
        # a clean exit, only on failure.
        pod_template['spec']['restartPolicy'] = 'OnFailure'
        # Probes make no sense for a short-lived Job pod that has no long-running
        # health server contract; drop them.
        pod_template['spec']['containers'][0].pop('readinessProbe', None)
        pod_template['spec']['containers'][0].pop('livenessProbe', None)
        return {
            'apiVersion': 'batch/v1',
            'kind': 'Job',
            'metadata': {'name': k8s_name('vf', flow_id, spec.name), 'labels': labels},
            'spec': {
                'backoffLimit': 3,
                'template': pod_template,
            },
        }

    return {
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'metadata': {'name': k8s_name('vf', flow_id, spec.name), 'labels': labels},
        'spec': {
            'replicas': spec.nb_tasks,
            'selector': {'matchLabels': {LABEL_NODE: k8s_name(spec.name), LABEL_FLOW_ID: k8s_name(flow_id)}},
            'template': pod_template,
        },
    }

def scaled_object(spec, flow_id, nats_monitoring_endpoint, max_replicas):
    '''
    A KEDA ScaledObject that scales a processor Deployment on NATS JetStream
    consumer lag. ``nb_tasks`` becomes the floor (minReplicaCount); KEDA scales up
    toward ``max_replicas`` when the node's input stream backs up. Returns None for
    node kinds that aren't autoscaled (producers, consumers).

    Requires KEDA installed in the cluster and the NATS monitoring endpoint
    (port 8222) reachable at ``nats_monitoring_endpoint``.
    '''
    from .compiler import NODE_KIND_PROCESSOR
    from .messaging.nats_messenger import stream_name_for, durable_name_for
    if spec.kind != NODE_KIND_PROCESSOR or not spec.parents:
        return None

    # Scale on the lag of this node's consumer against its first parent's stream.
    parent = spec.parents[0]
    triggers = [{
        'type': 'nats-jetstream',
        'metadata': {
            'natsServerMonitoringEndpoint': nats_monitoring_endpoint,
            'account': '$G',
            'stream': stream_name_for(flow_id, parent),
            'consumer': durable_name_for(spec.name, parent),
            'lagThreshold': '10',
        },
    }]
    return {
        'apiVersion': 'keda.sh/v1alpha1',
        'kind': 'ScaledObject',
        'metadata': {'name': k8s_name('vf', flow_id, spec.name, 'scaler'), 'labels': _labels(flow_id, spec.name)},
        'spec': {
            'scaleTargetRef': {'name': k8s_name('vf', flow_id, spec.name)},
            'minReplicaCount': spec.nb_tasks,
            'maxReplicaCount': max(max_replicas, spec.nb_tasks),
            'triggers': triggers,
        },
    }

def network_policy(flow_id):
    '''
    Allows worker pods to talk to each other and out to the broker/DNS, and denies
    everything else ingress by default. Kept intentionally permissive on egress
    since the NATS/Redis services may live in another namespace.
    '''
    return {
        'apiVersion': 'networking.k8s.io/v1',
        'kind': 'NetworkPolicy',
        'metadata': {'name': k8s_name('vf', flow_id, 'netpol'), 'labels': _labels(flow_id)},
        'spec': {
            'podSelector': {'matchLabels': {LABEL_FLOW_ID: k8s_name(flow_id)}},
            'policyTypes': ['Ingress'],
            'ingress': [{
                'from': [{'podSelector': {'matchLabels': {LABEL_FLOW_ID: k8s_name(flow_id)}}}],
            }],
        },
    }

def render_manifests(specs, flow_id, flow_type, nats_url, namespace = 'default',
                    registry = '', image_tag = 'latest', blob_redis_url = None,
                    autoscaling = False, max_replicas = 10,
                    nats_monitoring_endpoint = None):
    '''
    Returns a list of manifest dicts for the whole flow. The caller decides whether
    to ``yaml.dump`` them to files (CLI) or apply them via the API (engine).

    - autoscaling: if True, emit a KEDA ScaledObject per processor node.
    - max_replicas: upper bound for autoscaled processors.
    - nats_monitoring_endpoint: NATS monitoring host:port for KEDA (defaults to \
        ``nats.<namespace>.svc:8222``).
    '''
    nats_cm = nats_configmap(flow_id, nats_url, blob_redis_url)
    nats_cm_name = nats_cm['metadata']['name']

    manifests = [nats_cm, network_policy(flow_id)]
    for spec in specs:
        manifests.append(node_configmap(spec, flow_id, flow_type))
        manifests.append(workload(spec, flow_id, flow_type, registry, image_tag, nats_cm_name))

    if autoscaling:
        endpoint = nats_monitoring_endpoint or f'nats.{namespace}.svc:8222'
        for spec in specs:
            so = scaled_object(spec, flow_id, endpoint, max_replicas)
            if so is not None:
                manifests.append(so)

    for m in manifests:
        m['metadata']['namespace'] = namespace
    return manifests

def dump_manifests(manifests) -> str:
    '''Serializes a list of manifest dicts to a single multi-document YAML string.'''
    return yaml.dump_all(manifests, default_flow_style = False, sort_keys = False)
