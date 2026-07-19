'''
Renders Kubernetes manifests for a compiled flow (a list of ``NodeSpec``). One
workload per node, chosen by flow type:

  - BATCH flow     -> every node is a Job (each worker exits 0 when its upstream
                      end-of-stream drains, so the whole flow runs to completion).
  - REALTIME flow  -> a finite producer is a Job; every other node is a Deployment
                      (or a StatefulSet if partitioned) that stays up until the
                      control-channel stop.

plus a per-node ConfigMap holding the node's env, a shared ConfigMap for the NATS
URL, and a default-deny-except-broker NetworkPolicy.

Manifests are built as plain dicts and serialized with ``yaml.dump`` rather than
text-templated, so the output is always structurally valid YAML.
'''
from __future__ import absolute_import, division, print_function

import json
import re
import subprocess
from typing import Optional

import yaml

from ..core.compiler import NODE_KIND_PRODUCER
from ..core.constants import BATCH

# GPU allocation lives in .gpu; these four are used below.
# Re-export only: DEFAULT_GPU_RESOURCE was a public name of this module before the
# GPU strategies were extracted, so external callers may still import it from here.
from .gpu import (
    DEFAULT_GPU_RESOURCE,  # noqa: F401
    GPU_POOL_LABEL,
    GPU_TAINT_KEY,
    get_gpu_mode,
    resolve_gpu_resource,
)

LABEL_FLOW_ID = 'videoflow.io/flow-id'
LABEL_RUN_ID = 'videoflow.io/run-id'
LABEL_NODE = 'videoflow.io/node'
LABEL_MANAGED_BY = 'app.kubernetes.io/managed-by'

# Built-in k8s kinds videoflow creates for a flow — always resolvable, deleted in one
# call. The single source of truth for label-selector teardown.
_CORE_DELETABLE_KINDS = ('deployment', 'statefulset', 'job', 'configmap', 'service',
                         'networkpolicy', 'poddisruptionbudget')
# Optional CRD kinds (only present if their operator is installed, e.g. KEDA). Deleted
# separately so a missing CRD type doesn't make kubectl reject the whole delete and
# leave the core resources behind.
_CRD_DELETABLE_KINDS = ('scaledobject',)
DELETABLE_KINDS = _CORE_DELETABLE_KINDS + _CRD_DELETABLE_KINDS

# GC safety net on batch Jobs: even if the launching client dies mid-run, k8s reaps
# completed Job objects (and their pods) after this many seconds.
_BATCH_JOB_TTL_SECONDS = 600

_DNS1123_RE = re.compile(r'[^a-z0-9-]+')

def k8s_name(*parts) -> str:
    '''Joins parts into a DNS-1123-safe Kubernetes resource name.'''
    raw = '-'.join(str(p) for p in parts).lower()
    name = _DNS1123_RE.sub('-', raw).strip('-')
    return name[:63].strip('-')

def parse_mounts(values) -> list:
    '''
    Parses repeatable ``--mount`` specs into mount dicts consumed by ``_pod_spec``.

    - Arguments:
        - values: list of ``/host/path:/container/path[:ro]`` strings. The \
            single-path shorthand ``/path[:ro]`` mounts the same absolute path on \
            both sides (what a flow compiled against local files needs, since the \
            paths baked into node params must resolve identically in the pods).

    - Raises:
        - ``ValueError`` on a relative path or a malformed suffix.
    '''
    mounts = []
    for i, value in enumerate(values or []):
        parts = value.split(':')
        read_only = False
        if parts and parts[-1] == 'ro':
            read_only = True
            parts = parts[:-1]
        if len(parts) == 1:
            parts = parts * 2
        if len(parts) != 2 or not parts[0].startswith('/') or not parts[1].startswith('/'):
            raise ValueError(f'--mount must be /host/path[:/container/path][:ro] with '
                             f'absolute paths, got: {value!r}')
        mounts.append({'name': f'vf-mount-{i}', 'host_path': parts[0],
                       'container_path': parts[1], 'read_only': read_only})
    return mounts

def _env_pairs(spec, flow_id, flow_type, run_id, envelope_version, allow_pickle = False) -> dict:
    env = {
        'VF_NODE_PARAMS_JSON': json.dumps(spec.params),
        'VF_NODE_KIND': spec.kind,
        'VF_NODE_NAME': spec.name,
        'VF_PARENT_NAMES': ','.join(spec.parents),
        'VF_HAS_CHILDREN': '1' if spec.has_children else '0',
        'VF_FLOW_ID': flow_id,
        'VF_FLOW_TYPE': flow_type,
        'VF_RUN_ID': run_id,
        'VF_NB_TASKS': str(spec.nb_tasks),
        # In-cluster each pod has its own IP, so a fixed health port is safe and
        # matches the probe/containerPort below.
        'VF_HEALTH_PORT': '8080',
        # Whole-run wire version (homogeneous: a mixed flow forces v4 so a native
        # producer and a remote consumer agree on the protobuf wire).
        'VF_ENVELOPE_VERSION': str(envelope_version),
    }
    # A Python node/component names the class the worker imports; a native component
    # has none and runs its own image entrypoint. A component (either kind) also
    # carries its ref + protocol version.
    if spec.node_class:
        env['VF_NODE_CLASS'] = spec.node_class
    if spec.component_ref:
        env['VF_COMPONENT_REF'] = spec.component_ref
        if spec.protocol_version is not None:
            env['VF_PROTOCOL_VERSION'] = str(spec.protocol_version)
    if spec.partition_by:
        env['VF_PARTITION_BY'] = spec.partition_by
    if spec.join_policy:
        env['VF_JOIN_POLICY_JSON'] = json.dumps(spec.join_policy)
    if allow_pickle:
        env['VF_ALLOW_PICKLE'] = '1'
    return env

def _is_partitioned(spec) -> bool:
    return bool(getattr(spec, 'partition_by', None)) and spec.nb_tasks > 1

def _labels(flow_id, node_name = None) -> dict:
    labels = {LABEL_FLOW_ID: k8s_name(flow_id), LABEL_MANAGED_BY: 'videoflow'}
    if node_name is not None:
        labels[LABEL_NODE] = k8s_name(node_name)
    return labels

def gpu_demand(specs, default_resource = None) -> dict:
    '''
    Whole-flow GPU demand per extended-resource name: every replica of a GPU node
    claims its own ``gpu_count`` devices exclusively, so this is the allocatable
    capacity the cluster must have for the flow to fully schedule. The single
    source of truth for deploy's preflight and ``videoflow explain`` — the two
    must never disagree.
    '''
    demand : dict = {}
    for spec in specs:
        if spec.device_type != 'gpu':
            continue
        resource = resolve_gpu_resource(spec, default_resource)
        demand[resource] = demand.get(resource, 0) + spec.nb_tasks * spec.gpu_count
    return demand

def _pod_spec(spec, flow_id, flow_type, image, nats_configmap, mounts = None,
              gpu_runtime_class = None, gpu_mode = 'exclusive',
              gpu_resource_name = None) -> dict:
    container = {
        'name': 'worker',
        'image': image,
        'envFrom': [
            {'configMapRef': {'name': k8s_name('vf', flow_id, spec.name, 'env')}},
            {'configMapRef': {'name': nats_configmap}},
        ],
    }
    # A remote component may override the container command; native nodes and
    # vendor images with a videoflow entrypoint leave it to the image.
    if getattr(spec, 'command', None):
        container['command'] = list(spec.command)
    if _is_partitioned(spec):
        if flow_type == BATCH:
            # Partitioned BATCH node runs as an Indexed Job; the completion index is
            # the stable 0..N-1 replica id. Feed it straight into VF_REPLICA_ID (the
            # worker reads that first) — an indexed-Job pod name has a random suffix,
            # so the POD_NAME-ordinal path would not recover it.
            container['env'] = [{
                'name': 'VF_REPLICA_ID',
                'valueFrom': {'fieldRef': {
                    'fieldPath': "metadata.annotations['batch.kubernetes.io/job-completion-index']"}},
            }]
        else:
            # Partitioned REALTIME node runs as a StatefulSet; the ordinal in the pod
            # name is the replica id (the worker parses it off POD_NAME).
            container['env'] = [{
                'name': 'POD_NAME',
                'valueFrom': {'fieldRef': {'fieldPath': 'metadata.name'}},
            }]

    resources = {}
    node_selector = None
    tolerations = None
    runtime_class = None
    if spec.device_type == 'gpu':
        # Resolved here, not only in render_manifests: a direct workload()/_pod_spec
        # caller with a typo'd mode must get an error, not a silent fall-through to
        # the no-limit shared branch.
        resources.update(get_gpu_mode(gpu_mode).pod_resources(spec, gpu_resource_name))
        node_selector = {GPU_POOL_LABEL: 'true'}
        tolerations = [{
            'key': GPU_TAINT_KEY, 'operator': 'Exists', 'effect': 'NoSchedule',
        }]
        # Requesting the resource only makes the pod *schedulable* onto a GPU node;
        # injecting the device into the container is the NVIDIA container runtime's
        # job. That is automatic only where it is the node's default runtime. On
        # distros that register it as an opt-in RuntimeClass instead (k3s ships an
        # 'nvidia' handler but leaves runc default), a pod without runtimeClassName
        # starts with no device — so --gpu-runtime-class names the handler to use.
        # In shared mode it is the only thing that grants device access at all.
        runtime_class = gpu_runtime_class
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
    # Startup probe covers slow model loading in open() (a GPU detector can take
    # tens of seconds) without letting the liveness probe kill the pod meanwhile.
    container['startupProbe'] = {
        'httpGet': {'path': '/readyz', 'port': 8080},
        'periodSeconds': 2, 'failureThreshold': 60,
    }
    container['ports'] = [{'containerPort': 8080, 'name': 'health'}]

    pod_spec: dict = {'containers': [container]}
    if mounts:
        # hostPath type left unset on purpose: the mounted path may be a directory
        # or a single file, and an unset type skips kubelet existence-kind checks.
        container['volumeMounts'] = [
            {'name': m['name'], 'mountPath': m['container_path'], 'readOnly': m['read_only']}
            for m in mounts]
        pod_spec['volumes'] = [
            {'name': m['name'], 'hostPath': {'path': m['host_path']}} for m in mounts]
    if node_selector:
        pod_spec['nodeSelector'] = node_selector
    if tolerations:
        pod_spec['tolerations'] = tolerations
    if runtime_class:
        pod_spec['runtimeClassName'] = runtime_class
    return pod_spec

def node_configmap(spec, flow_id, flow_type, run_id, envelope_version, allow_pickle = False) -> dict:
    return {
        'apiVersion': 'v1',
        'kind': 'ConfigMap',
        'metadata': {
            'name': k8s_name('vf', flow_id, spec.name, 'env'),
            'labels': _labels(flow_id, spec.name),
        },
        'data': _env_pairs(spec, flow_id, flow_type, run_id, envelope_version, allow_pickle),
    }

def nats_configmap(flow_id, nats_url, blob_redis_url = None) -> dict:
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

def workload(spec, flow_id, flow_type, image, nats_cm_name, mounts = None,
             gpu_runtime_class = None, gpu_mode = 'exclusive', gpu_resource_name = None) -> dict:
    labels = _labels(flow_id, spec.name)
    pod_template = {
        'metadata': {'labels': labels},
        'spec': _pod_spec(spec, flow_id, flow_type, image, nats_cm_name, mounts = mounts,
                          gpu_runtime_class = gpu_runtime_class, gpu_mode = gpu_mode,
                          gpu_resource_name = gpu_resource_name),
    }

    batch = (flow_type == BATCH)
    partitioned = _is_partitioned(spec)
    # In a BATCH flow every node terminates: each worker exits 0 once its upstream
    # end-of-stream drains. So every node is a Job — a Deployment (restartPolicy
    # Always) would restart the finished pod into CrashLoopBackOff. In a REALTIME
    # flow only a finite producer completes; the rest run until the control stop.
    is_job = batch or (spec.kind == NODE_KIND_PRODUCER and spec.is_finite)
    if is_job:
        # A completing worker pod should not be restarted on a clean exit, only on
        # failure.
        pod_template['spec']['restartPolicy'] = 'OnFailure'
        # Probes make no sense for a short-lived Job pod that has no long-running
        # health server contract; drop them.
        for probe in ('readinessProbe', 'livenessProbe', 'startupProbe'):
            pod_template['spec']['containers'][0].pop(probe, None)
        job_spec: dict = {'backoffLimit': 3, 'template': pod_template}
        if batch:
            # Reap completed Jobs (and pods) even if the launching client dies.
            job_spec['ttlSecondsAfterFinished'] = _BATCH_JOB_TTL_SECONDS
            if partitioned:
                # N distinct, stable replica ids → Indexed Job (completion index is
                # the replica id, see _pod_spec).
                job_spec['completionMode'] = 'Indexed'
                job_spec['completions'] = spec.nb_tasks
                job_spec['parallelism'] = spec.nb_tasks
            elif spec.nb_tasks > 1:
                # Competing consumers sharing one durable: N interchangeable pods,
                # each with its own per-process EOS durable, so each independently
                # sees EOS and exits 0. Require all N to complete.
                job_spec['completions'] = spec.nb_tasks
                job_spec['parallelism'] = spec.nb_tasks
        return {
            'apiVersion': 'batch/v1',
            'kind': 'Job',
            'metadata': {'name': k8s_name('vf', flow_id, spec.name), 'labels': labels},
            'spec': job_spec,
        }

    selector = {'matchLabels': {LABEL_NODE: k8s_name(spec.name), LABEL_FLOW_ID: k8s_name(flow_id)}}

    if _is_partitioned(spec):
        # Partitioned node → StatefulSet, so pod ordinals give stable replica ids
        # (changing the replica count rehashes ownership, so these are NOT
        # autoscaled — see scaled_object).
        return {
            'apiVersion': 'apps/v1',
            'kind': 'StatefulSet',
            'metadata': {'name': k8s_name('vf', flow_id, spec.name), 'labels': labels},
            'spec': {
                'serviceName': k8s_name('vf', flow_id, spec.name, 'hl'),
                'replicas': spec.nb_tasks,
                'selector': selector,
                'template': pod_template,
            },
        }

    return {
        'apiVersion': 'apps/v1',
        'kind': 'Deployment',
        'metadata': {'name': k8s_name('vf', flow_id, spec.name), 'labels': labels},
        'spec': {
            'replicas': spec.nb_tasks,
            'selector': selector,
            'template': pod_template,
        },
    }

def headless_service(spec, flow_id) -> dict:
    '''Headless Service backing a partitioned node's StatefulSet (required for stable pod network identity/ordinals).'''
    labels = _labels(flow_id, spec.name)
    return {
        'apiVersion': 'v1',
        'kind': 'Service',
        'metadata': {'name': k8s_name('vf', flow_id, spec.name, 'hl'), 'labels': labels},
        'spec': {
            'clusterIP': 'None',
            'selector': {LABEL_NODE: k8s_name(spec.name), LABEL_FLOW_ID: k8s_name(flow_id)},
            'ports': [{'port': 8080, 'name': 'health'}],
        },
    }

def pod_disruption_budget(spec, flow_id) -> dict:
    '''Keeps at least one replica of a multi-replica node available during voluntary disruptions (node drains, upgrades).'''
    labels = _labels(flow_id, spec.name)
    return {
        'apiVersion': 'policy/v1',
        'kind': 'PodDisruptionBudget',
        'metadata': {'name': k8s_name('vf', flow_id, spec.name, 'pdb'), 'labels': labels},
        'spec': {
            'minAvailable': 1,
            'selector': {'matchLabels': {LABEL_NODE: k8s_name(spec.name), LABEL_FLOW_ID: k8s_name(flow_id)}},
        },
    }

def flow_spec_configmap(specs, flow_id, run_id) -> dict:
    '''ConfigMap holding the compiled specs as JSON, mounted by the provisioning init Job.'''
    return {
        'apiVersion': 'v1',
        'kind': 'ConfigMap',
        'metadata': {'name': k8s_name('vf', flow_id, 'specs'), 'labels': _labels(flow_id)},
        'data': {'specs.json': json.dumps([s.to_dict() for s in specs])},
    }

def provision_init_job(flow_id, run_id, flow_type, image, nats_cm_name) -> dict:
    '''
    A one-shot Job that runs ``videoflow.provision`` to create all streams/durables
    before workers start (required so BATCH interest-retention streams don't drop
    early messages). Runs on ``image`` — any worker image has the framework + broker
    client installed.
    '''
    labels = _labels(flow_id)
    spec_cm = k8s_name('vf', flow_id, 'specs')
    container = {
        'name': 'provision',
        'image': image,
        'command': ['python', '-m', 'videoflow.provision'],
        'envFrom': [{'configMapRef': {'name': nats_cm_name}}],
        'env': [
            {'name': 'VF_FLOW_ID', 'value': flow_id},
            {'name': 'VF_RUN_ID', 'value': run_id},
            {'name': 'VF_FLOW_TYPE', 'value': flow_type},
            {'name': 'VF_FLOW_SPECS_PATH', 'value': '/etc/videoflow/specs.json'},
        ],
        'volumeMounts': [{'name': 'specs', 'mountPath': '/etc/videoflow', 'readOnly': True}],
    }
    return {
        'apiVersion': 'batch/v1',
        'kind': 'Job',
        'metadata': {'name': k8s_name('vf', flow_id, 'provision'), 'labels': labels},
        'spec': {
            'backoffLimit': 6,
            'template': {
                'metadata': {'labels': labels},
                'spec': {
                    'restartPolicy': 'OnFailure',
                    'containers': [container],
                    'volumes': [{'name': 'specs', 'configMap': {'name': spec_cm}}],
                },
            },
        },
    }

def scaled_object(spec, flow_id, run_id, nats_monitoring_endpoint, max_replicas) -> Optional[dict]:
    '''
    A KEDA ScaledObject that scales a processor Deployment on NATS JetStream
    consumer lag. ``nb_tasks`` becomes the floor (minReplicaCount); KEDA scales up
    toward ``max_replicas`` when the node's input stream backs up. Returns None for
    node kinds that aren't autoscaled (producers, consumers).

    Requires KEDA installed in the cluster and the NATS monitoring endpoint
    (port 8222) reachable at ``nats_monitoring_endpoint``.
    '''
    from ..core.compiler import NODE_KIND_PROCESSOR
    from ..messaging.topology import durable_name_for, stream_name_for
    if spec.kind != NODE_KIND_PROCESSOR or not spec.parents:
        return None
    if _is_partitioned(spec):
        # Rehashing ownership on a replica-count change would double- or zero-process
        # messages, so partitioned nodes run at a fixed scale (no KEDA).
        return None

    # Scale on the lag of this node's consumer against its first parent's stream.
    parent = spec.parents[0]
    triggers = [{
        'type': 'nats-jetstream',
        'metadata': {
            'natsServerMonitoringEndpoint': nats_monitoring_endpoint,
            'account': '$G',
            'stream': stream_name_for(flow_id, run_id, parent),
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

def network_policy(flow_id) -> dict:
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

def render_manifests(specs, flow_id, flow_type, nats_url, run_id, namespace = 'default',
                    default_image = None, image_overrides = None, blob_redis_url = None,
                    autoscaling = False, max_replicas = 10,
                    nats_monitoring_endpoint = None, envelope_version = None,
                    allow_pickle = False, provision_image = None, mounts = None,
                    gpu_runtime_class = None, gpu_mode = 'exclusive',
                    gpu_resource_name = None, gpu_autoscaling = False) -> list:
    '''
    Returns a list of manifest dicts for the whole flow. The caller decides whether
    to ``yaml.dump`` them to files (CLI) or apply them via the API (engine).

    - run_id: per-run identifier stamped into each node's env (scopes broker streams).
    - default_image: image ref used for any node that didn't declare its own (``--image``).
    - image_overrides: mapping of node name to image ref (``--image-override``).
    - autoscaling: if True, emit a KEDA ScaledObject per processor node.
    - max_replicas: upper bound for autoscaled processors.
    - nats_monitoring_endpoint: NATS monitoring host:port for KEDA (defaults to \
        ``nats.<namespace>.svc:8222``).
    - envelope_version: wire version for the whole run; a flow with any remote \
        component is forced to v4 (the protobuf wire non-Python SDKs speak). \
        Defaults to the ambient ``DEFAULT_ENVELOPE_VERSION`` for pure-Python flows.
    - allow_pickle: permit the legacy Python-only pickle codec (rejected for flows \
        with remote components).
    - provision_image: image the one-shot provision Job runs on — must have \
        videoflow + the broker client (i.e. a Python image), which a vendor \
        ``default_image`` may not be. Defaults to ``default_image``; set it \
        explicitly (``--provision-image``) for flows whose default image is a \
        non-Python vendor image.
    - mounts: mount dicts from ``parse_mounts`` — each becomes a hostPath \
        volume + volumeMount on every node workload (not the provision Job, \
        which never touches node data).
    - gpu_runtime_class: ``runtimeClassName`` to put on GPU pods (``--gpu-runtime-class``). \
        Needed where the NVIDIA container runtime is registered as an opt-in \
        RuntimeClass rather than the node default — on k3s, ``nvidia``. Without it \
        such a pod schedules onto a GPU node and then runs with no device visible.
    - gpu_mode: ``'exclusive'`` (default — each GPU replica claims whole devices \
        via the extended resource) or ``'shared'`` (no resource limit; all GPU pods \
        co-schedule onto the pool and share the physical GPUs through the NVIDIA \
        runtime — LocalProcessEngine semantics, dev clusters only).
    - gpu_resource_name: deploy-level default extended-resource name for GPU claims \
        (``--gpu-resource-name``); a node's own ``gpu_resource_name`` wins. Defaults \
        to ``nvidia.com/gpu``.
    - gpu_autoscaling: emit KEDA ScaledObjects for GPU nodes too. Off by default: \
        each autoscaled replica claims its own GPUs, so scaling to ``max_replicas`` \
        can demand more devices than the cluster has and strand pods Pending.

    - Raises:
        - ``ValueError`` if a node has no resolvable image (see ``videoflow.deploy.images``), \
            or if the wire settings are incompatible with the flow's components.
    '''
    from ..core.compiler import has_native_components, validate_wire_compatibility
    from ..wire.serialization import DEFAULT_ENVELOPE_VERSION
    from .images import resolve_image

    # Fail before any manifest is built, rather than at the first GPU node.
    get_gpu_mode(gpu_mode)

    # Resolve the whole-run wire version: a flow with any native component must use
    # the protobuf wire (v4+) so every node — native or not — speaks it.
    base_version = DEFAULT_ENVELOPE_VERSION if envelope_version is None else envelope_version
    resolved_version = 4 if (has_native_components(specs) and base_version < 4) else base_version
    validate_wire_compatibility(specs, resolved_version, allow_pickle)

    # Resolve every node's image up front (raises with an actionable message if any
    # node has none), so a misconfiguration fails before partial manifests are built.
    images_by_name = {
        spec.name: resolve_image(spec.name, spec.image, default_image, image_overrides)
        for spec in specs
    }
    # The provision init Job needs a videoflow-capable (Python) image. Prefer an
    # explicit --provision-image; else the default image; else any node's image
    # (correct only when that node is a Python worker — hence the override exists).
    init_image = provision_image or default_image or images_by_name[specs[0].name]

    nats_cm = nats_configmap(flow_id, nats_url, blob_redis_url)
    nats_cm_name = nats_cm['metadata']['name']

    manifests = [nats_cm, network_policy(flow_id)]
    # Provision streams/durables up front via a one-shot init Job (BATCH interest
    # retention drops messages published before their consumers exist).
    manifests.append(flow_spec_configmap(specs, flow_id, run_id))
    manifests.append(provision_init_job(flow_id, run_id, flow_type, init_image, nats_cm_name))
    for spec in specs:
        manifests.append(node_configmap(spec, flow_id, flow_type, run_id, resolved_version, allow_pickle))
        if _is_partitioned(spec):
            manifests.append(headless_service(spec, flow_id))
        manifests.append(workload(spec, flow_id, flow_type, images_by_name[spec.name], nats_cm_name,
                                  mounts = mounts, gpu_runtime_class = gpu_runtime_class,
                                  gpu_mode = gpu_mode, gpu_resource_name = gpu_resource_name))
        if spec.nb_tasks > 1:
            manifests.append(pod_disruption_budget(spec, flow_id))

    if autoscaling:
        endpoint = nats_monitoring_endpoint or f'nats.{namespace}.svc:8222'
        for spec in specs:
            # GPU nodes are excluded from autoscaling unless explicitly opted in:
            # every extra replica claims its own whole GPUs, so scaling on broker lag
            # can demand max_replicas x gpu_count devices and strand pods Pending
            # (and in shared mode it multiplies VRAM pressure instead).
            if spec.device_type == 'gpu' and not gpu_autoscaling:
                continue
            so = scaled_object(spec, flow_id, run_id, endpoint, max_replicas)
            if so is not None:
                manifests.append(so)

    # Stamp namespace + the run-id label on every resource (and on workload pod
    # templates, so `kubectl logs`/wait can select a single run). Done centrally here
    # rather than threaded through every builder. The run-id label makes teardown
    # run-scoped, so a redeploy under a new run-id never nukes a concurrent run.
    run_label = k8s_name(run_id)
    for m in manifests:
        m['metadata']['namespace'] = namespace
        m['metadata'].setdefault('labels', {})[LABEL_RUN_ID] = run_label
        template = m.get('spec', {}).get('template')
        if isinstance(template, dict):
            template.setdefault('metadata', {}).setdefault('labels', {})[LABEL_RUN_ID] = run_label
    return manifests

def split_provision_manifests(manifests, flow_id):
    '''
    Partitions rendered manifests into ``(provision_phase, worker_phase)`` so the
    caller can apply provisioning first and wait for it before starting workers.

    The provision phase is everything the provision Job needs to create the broker
    streams/durables (including its EOS interest anchors) before any worker
    publishes: the broker + specs ConfigMaps, the NetworkPolicy, and the provision
    Job itself. The worker phase is every node's ConfigMap/workload/service/PDB.
    '''
    provision_names = {
        k8s_name('vf', flow_id, 'broker'),
        k8s_name('vf', flow_id, 'specs'),
        k8s_name('vf', flow_id, 'netpol'),
        k8s_name('vf', flow_id, 'provision'),
    }
    phase1, phase2 = [], []
    for m in manifests:
        (phase1 if m['metadata']['name'] in provision_names else phase2).append(m)
    return phase1, phase2

def delete_resources(kubectl, namespace, flow_id, run_id = None) -> None:
    '''
    Single source of truth for tearing down a flow's Kubernetes resources: deletes
    every kind in ``DELETABLE_KINDS`` matching the flow-id label, scoped to one run
    when ``run_id`` is given (else every run of the flow). Best-effort (never raises).
    '''
    selector = f'{LABEL_FLOW_ID}={k8s_name(flow_id)}'
    if run_id is not None:
        selector += f',{LABEL_RUN_ID}={k8s_name(run_id)}'
    subprocess.run(
        [kubectl, 'delete', '-n', namespace, ','.join(_CORE_DELETABLE_KINDS), '-l', selector],
        check = False,
    )
    # CRD kinds one at a time: if the CRD isn't installed, kubectl errors on just that
    # kind instead of aborting the whole delete. Output suppressed (the "no such
    # resource type" message is expected when the operator is absent).
    for kind in _CRD_DELETABLE_KINDS:
        subprocess.run(
            [kubectl, 'delete', '-n', namespace, kind, '-l', selector],
            check = False, capture_output = True,
        )

def dump_manifests(manifests) -> str:
    '''Serializes a list of manifest dicts to a single multi-document YAML string.'''
    return yaml.dump_all(manifests, default_flow_style = False, sort_keys = False)
