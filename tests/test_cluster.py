'''
Cluster auto-detection and image loading: classification from the kubectl
context / node labels, the per-flavor load command, the remote-cluster error,
and the GPU preflight messages.

Pure/unit: subprocess is monkeypatched — no cluster, no docker.
'''
import json
import subprocess

import pytest

from videoflow.deploy import cluster, gpu


def _nodes_json(*nodes):
    '''A `kubectl get nodes -o json` body: nodes as (labels, allocatable) pairs.'''
    return json.dumps({'items': [
        {'metadata': {'labels': labels}, 'status': {'allocatable': allocatable}}
        for labels, allocatable in nodes
    ]})


#: GFD labels of a plain physical-GPU node (no sharing).
_PHYSICAL_LABELS = {'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB',
                    'nvidia.com/gpu.count': '2'}


class _Proc:
    def __init__(self, stdout = '', returncode = 0):
        self.stdout = stdout
        self.returncode = returncode


def _fake_run(responses):
    '''Returns a subprocess.run stand-in serving canned stdout keyed by a substring of the command.'''
    calls = []
    def run(cmd, **kwargs):
        calls.append(cmd)
        joined = ' '.join(cmd)
        for key, out in responses.items():
            if key in joined:
                return _Proc(stdout = out)
        return _Proc()
    return run, calls


@pytest.mark.parametrize('ctx,expected', [
    ('kind-dev', cluster.KIND),
    ('minikube', cluster.MINIKUBE),
    ('docker-desktop', cluster.DOCKER_DESKTOP),
])
def test_detect_by_context_name(monkeypatch, ctx, expected):
    run, _ = _fake_run({'current-context': ctx})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.detect_cluster() == expected


def test_detect_k3s_by_node_labels(monkeypatch):
    run, _ = _fake_run({'current-context': 'default', 'get nodes': 'k3s \n'})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.detect_cluster() == cluster.K3S


def test_detect_falls_back_to_remote(monkeypatch):
    run, _ = _fake_run({'current-context': 'gke_my-proj_us-east1_prod'})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.detect_cluster() == cluster.GENERIC_REMOTE


def test_load_images_kind_uses_context_cluster_name(monkeypatch):
    run, calls = _fake_run({'current-context': 'kind-dev'})
    monkeypatch.setattr(subprocess, 'run', run)
    cluster.load_images(cluster.KIND, ['img:1', 'img:2'])
    load = [c for c in calls if c[0] == 'kind'][0]
    assert load == ['kind', 'load', 'docker-image', 'img:1', 'img:2', '--name', 'dev']


def test_load_images_minikube_one_per_image(monkeypatch):
    run, calls = _fake_run({})
    monkeypatch.setattr(subprocess, 'run', run)
    cluster.load_images(cluster.MINIKUBE, ['img:1', 'img:2'])
    loads = [c for c in calls if c[0] == 'minikube']
    assert loads == [['minikube', 'image', 'load', 'img:1'],
                     ['minikube', 'image', 'load', 'img:2']]


def test_load_images_remote_raises_with_push_hint(monkeypatch):
    run, _ = _fake_run({})
    monkeypatch.setattr(subprocess, 'run', run)
    with pytest.raises(RuntimeError, match = 'docker push'):
        cluster.load_images(cluster.GENERIC_REMOTE, ['img:1'])


def test_load_images_docker_desktop_is_a_noop(monkeypatch, capsys):
    def boom(cmd, **kwargs):
        raise AssertionError('no subprocess expected')
    monkeypatch.setattr(subprocess, 'run', boom)
    cluster.load_images(cluster.DOCKER_DESKTOP, ['img:1'])
    assert 'no loading' in capsys.readouterr().out


def test_hostpath_warning_only_for_vm_backed_clusters():
    assert 'extraMounts' in cluster.hostpath_warning(cluster.KIND)
    assert 'minikube mount' in cluster.hostpath_warning(cluster.MINIKUBE)
    assert cluster.hostpath_warning(cluster.K3S) is None
    assert cluster.hostpath_warning(cluster.DOCKER_DESKTOP) is None
    assert cluster.hostpath_warning(cluster.GENERIC_REMOTE) is None


def test_gpu_preflight_reports_both_problems_with_fixes(monkeypatch):
    run, _ = _fake_run({'version': '{}',
                        'get nodes -l videoflow.io/gpu-pool=true': '',
                        'get nodes -o name': 'node/gpu-box\n'})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight()
    assert len(problems) == 2
    assert 'kubectl label node gpu-box videoflow.io/gpu-pool=true' in problems[0]
    assert 'nvidia-device-plugin' in problems[1]


def test_gpu_preflight_ok(monkeypatch):
    run, _ = _fake_run({'version': '{}', 'gpu-pool=true -o name': 'node/gpu-box', 'allocatable': '1'})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.gpu_preflight() == []


def test_gpu_preflight_unreachable_cluster_says_so(monkeypatch):
    '''An unreachable cluster answers every query with '', which must not be
    misreported as a missing label + missing device plugin.'''
    run, _ = _fake_run({})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight()
    assert len(problems) == 1
    assert 'cannot reach the cluster' in problems[0]


def test_gpu_preflight_flags_opt_in_nvidia_runtimeclass(monkeypatch):
    '''k3s registers an 'nvidia' RuntimeClass but leaves runc the node default, so a
    GPU pod without runtimeClassName schedules and then runs with no device.'''
    run, _ = _fake_run({'version': '{}', 'gpu-pool=true -o name': 'node/gpu-box', 'allocatable': '1',
                        'get runtimeclass': 'crun nvidia nvidia-experimental'})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight()
    assert len(problems) == 1
    assert '--gpu-runtime-class nvidia' in problems[0]
    # ...and passing it silences the warning.
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.gpu_preflight(gpu_runtime_class = 'nvidia') == []


def test_gpu_preflight_reports_demand_over_capacity(monkeypatch):
    '''The offside case: 9 exclusive GPU claims against 1 allocatable device. The
    scheduler would bind one pod and strand the rest Pending — preflight must say
    so with the exact numbers, before anything is applied.'''
    run, _ = _fake_run({'version': '{}', 'gpu-pool=true -o name': 'node/gpu-box',
                        'allocatable': '1'})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia',
                                     demand = {'nvidia.com/gpu': 9})
    assert len(problems) == 1
    assert 'demands 9 x nvidia.com/gpu' in problems[0]
    assert 'only 1 allocatable' in problems[0]
    assert '8 pod(s) will stay Pending' in problems[0]
    # Demand within capacity is clean.
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.gpu_preflight(gpu_runtime_class = 'nvidia',
                                 demand = {'nvidia.com/gpu': 1}) == []


def test_gpu_preflight_checks_each_requested_resource(monkeypatch):
    '''A node that requests a MIG profile the cluster does not expose must be
    reported against that resource name, not nvidia.com/gpu.'''
    run, _ = _fake_run({'version': '{}', 'gpu-pool=true -o name': 'node/gpu-box',
                        'nvidia\\.com/gpu}': '2'})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia',
                                     demand = {'nvidia.com/gpu': 2,
                                               'nvidia.com/mig-1g.10gb': 1})
    assert len(problems) == 1
    assert 'nvidia.com/mig-1g.10gb' in problems[0]
    assert 'does not expose it' in problems[0]


def test_allocatable_gpus_sums_across_nodes(monkeypatch):
    run, _ = _fake_run({'version': '{}', 'allocatable': '1 4'})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.allocatable_gpus() == 5


def test_max_allocatable_gpus_per_node_takes_the_largest_node(monkeypatch):
    run, _ = _fake_run({'version': '{}', 'allocatable': '1 4'})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.max_allocatable_gpus_per_node() == 4
    run_empty, _ = _fake_run({'version': '{}'})
    monkeypatch.setattr(subprocess, 'run', run_empty)
    assert cluster.max_allocatable_gpus_per_node() == 0


def test_gpu_preflight_flags_gpu_count_exceeding_largest_node(monkeypatch):
    '''A 3-GPU pod against two 2-GPU nodes: total capacity (4) satisfies the demand
    (3), but no single host can bind the pod — it would stay Pending forever, and
    only the per-pod check can say why.'''
    physical = _nodes_json((_PHYSICAL_LABELS, {'nvidia.com/gpu': '2'}),
                           (_PHYSICAL_LABELS, {'nvidia.com/gpu': '2'}))
    run, _ = _fake_run({'version': '{}', 'gpu-pool=true -o name': 'node/gpu-box',
                        'allocatable': '2 2', 'nodes -o json': physical})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia',
                                     demand = {'nvidia.com/gpu': 3},
                                     max_per_pod = {'nvidia.com/gpu': 3})
    assert len(problems) == 1
    assert '3 x nvidia.com/gpu in a single pod' in problems[0]
    assert 'largest cluster node has only 2' in problems[0]
    # A big-enough single node is clean.
    big = _nodes_json(({'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB',
                        'nvidia.com/gpu.count': '4'}, {'nvidia.com/gpu': '4'}))
    run_big, _ = _fake_run({'version': '{}', 'gpu-pool=true -o name': 'node/gpu-box',
                            'allocatable': '4', 'nodes -o json': big})
    monkeypatch.setattr(subprocess, 'run', run_big)
    assert cluster.gpu_preflight(gpu_runtime_class = 'nvidia',
                                 demand = {'nvidia.com/gpu': 3},
                                 max_per_pod = {'nvidia.com/gpu': 3}) == []


def test_gpu_preflight_rejects_combining_mig_slices(monkeypatch):
    '''Bug 2 regression: MIG slices are hardware-isolated — a model cannot span two
    of them, so a multi-slice single-pod claim is impossible no matter what the
    cluster has, and the problem carries the fatal marker the CLI hard-errors on.'''
    run, _ = _fake_run({'version': '{}', 'gpu-pool=true -o name': 'node/gpu-box',
                        'allocatable': '4'})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia',
                                     demand = {'nvidia.com/mig-1g.10gb': 2},
                                     max_per_pod = {'nvidia.com/mig-1g.10gb': 2})
    assert len(problems) == 1
    assert problems[0].startswith(gpu.IMPOSSIBLE_GPU_REQUEST)
    assert 'MIG slices' in problems[0]


def test_gpu_preflight_rejects_multi_gpu_on_a_time_sliced_pool(monkeypatch):
    '''Bug 1 regression: a time-sliced cluster advertises inflated units, so the
    numeric checks pass — but gpu_count > 1 would be rejected at admission or
    granted slices of the same card. GFD labels are what make this detectable.'''
    sliced = _nodes_json(({'nvidia.com/gpu.product': 'NVIDIA-GeForce-RTX-3090-SHARED',
                           'nvidia.com/gpu.replicas': '4',
                           'nvidia.com/gpu.sharing-strategy': 'time-slicing'},
                          {'nvidia.com/gpu': '4'}))
    run, _ = _fake_run({'version': '{}', 'gpu-pool=true -o name': 'node/gpu-box',
                        'allocatable': '4', 'nodes -o json': sliced})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia',
                                     demand = {'nvidia.com/gpu': 2},
                                     max_per_pod = {'nvidia.com/gpu': 2})
    assert len(problems) == 1
    assert problems[0].startswith(gpu.IMPOSSIBLE_GPU_REQUEST)
    assert 'time-sliced' in problems[0]
    # gpu_count == 1 on the same pool stays the supported dev workflow.
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.gpu_preflight(gpu_runtime_class = 'nvidia',
                                 demand = {'nvidia.com/gpu': 2},
                                 max_per_pod = {'nvidia.com/gpu': 1}) == []


def test_gpu_preflight_notes_an_unclassifiable_resource(monkeypatch):
    '''No GFD labels (or a non-NVIDIA resource): preflight cannot prove the units
    are whole devices, so it keeps the numeric checks and says what it assumed.'''
    run, _ = _fake_run({'version': '{}', 'gpu-pool=true -o name': 'node/gpu-box',
                        'allocatable': '4'})
    monkeypatch.setattr(subprocess, 'run', run)
    problems = cluster.gpu_preflight(gpu_runtime_class = 'nvidia',
                                     demand = {'amd.com/gpu': 2},
                                     max_per_pod = {'amd.com/gpu': 2})
    assert len(problems) == 1
    assert 'cannot classify amd.com/gpu' in problems[0]
    assert not problems[0].startswith(gpu.IMPOSSIBLE_GPU_REQUEST)


def test_gpu_inventory_reads_gfd_labels(monkeypatch):
    nodes = _nodes_json(({'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB',
                          'nvidia.com/gpu.count': '2',
                          'nvidia.com/gpu.memory': '81920'}, {'nvidia.com/gpu': '2'}),
                        ({}, {}),                        # CPU node contributes nothing
                        ({'nvidia.com/gpu.product': 'NVIDIA-A30',
                          'nvidia.com/gpu.count': '1'}, {'nvidia.com/gpu': '1'}))
    run, _ = _fake_run({'gpu-pool=true -o json': nodes})
    monkeypatch.setattr(subprocess, 'run', run)
    inventory = cluster.gpu_inventory()
    assert [(n.product, n.card_count, n.memory_gib_per_card) for n in inventory] == [
        ('NVIDIA-A100-SXM4-80GB', 2, 80.0), ('NVIDIA-A30', 1, 0.0)]
    run_empty, _ = _fake_run({})
    monkeypatch.setattr(subprocess, 'run', run_empty)
    assert cluster.gpu_inventory() == []


def _named_nodes_json(*nodes):
    '''A `kubectl get nodes -o json` body: nodes as (name, labels, allocatable) triples.'''
    return json.dumps({'items': [
        {'metadata': {'name': name, 'labels': labels}, 'status': {'allocatable': allocatable}}
        for name, labels, allocatable in nodes
    ]})


def _pods_json(*pods):
    '''A `kubectl get pods -A -o json` body: pods as (node, phase, [limits, ...]) triples.'''
    return json.dumps({'items': [
        {'spec': {'nodeName': node,
                  'containers': [{'resources': {'limits': limits}} for limits in containers]},
         'status': {'phase': phase}}
        for node, phase, containers in pods
    ]})


def test_gpu_inventory_is_scoped_to_the_pool(monkeypatch):
    '''The solver must never plan geometry on a node videoflow was not given: the
    inventory read carries the pool selector, so out-of-pool GPU nodes are invisible.'''
    run, calls = _fake_run({})
    monkeypatch.setattr(subprocess, 'run', run)
    cluster.gpu_inventory()
    node_reads = [c for c in calls if 'get nodes' in ' '.join(c)]
    assert node_reads and all('-l videoflow.io/gpu-pool=true' in ' '.join(c) for c in node_reads)


def test_allocatable_gpus_is_scoped_to_the_pool(monkeypatch):
    '''Capacity outside the pool is capacity the pods can never use — counting it
    made preflight pass while the flow deadlocked Pending.'''
    run, calls = _fake_run({'allocatable': '1 4'})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.allocatable_gpus() == 5
    assert cluster.max_allocatable_gpus_per_node() == 4
    assert all('-l videoflow.io/gpu-pool=true' in ' '.join(c) for c in calls)


def test_gpu_inventory_reads_sharing_and_mig_state(monkeypatch):
    '''The enrichment fields are facts for MixGpu's exclusion rules: time-slicing
    signals, existing MIG geometry under either strategy, the owner stamp, and
    units held by running pods.'''
    nodes = _named_nodes_json(
        ('sliced', {'nvidia.com/gpu.product': 'NVIDIA-GeForce-RTX-3090-SHARED',
                    'nvidia.com/gpu.count': '1', 'nvidia.com/gpu.replicas': '4'},
         {'nvidia.com/gpu': '4'}),
        ('migd', {'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB',
                  'nvidia.com/gpu.count': '1', 'nvidia.com/mig.config': 'all-1g.10gb'},
         {'nvidia.com/mig-1g.10gb': '7'}),
        ('single-migd', {'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB-MIG-1g.10gb',
                         'nvidia.com/gpu.count': '7'}, {'nvidia.com/gpu': '7'}),
        ('owned', {'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB',
                   'nvidia.com/gpu.count': '2', 'videoflow.io/gpu-owner': 'flowa'},
         {'nvidia.com/gpu': '2'}),
        ('pristine', {'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB',
                      'nvidia.com/gpu.count': '2'}, {'nvidia.com/gpu': '2'}))
    pods = _pods_json(('pristine', 'Running', [{'nvidia.com/gpu': '1'}]))
    run, _ = _fake_run({'gpu-pool=true -o json': nodes, 'pods -A': pods})
    monkeypatch.setattr(subprocess, 'run', run)
    by_name = {n.name: n for n in cluster.gpu_inventory()}

    assert by_name['sliced'].time_sliced
    assert by_name['migd'].mig_partitioned and by_name['migd'].mig_config == 'all-1g.10gb'
    assert by_name['single-migd'].mig_partitioned
    assert by_name['owned'].owner == 'flowa'
    pristine = by_name['pristine']
    assert (pristine.time_sliced, pristine.mig_config, pristine.mig_partitioned,
            pristine.owner) == (False, None, False, None)
    assert pristine.used_units == {'nvidia.com/gpu': 1}
    assert pristine.mig_allowed    # a fact for MixGpu to clear, defaulting open


def test_gpu_units_in_use_sums_extended_resource_limits(monkeypatch):
    pods = _pods_json(
        ('gpu-a', 'Running', [{'nvidia.com/gpu': '1', 'cpu': '2', 'memory': '1Gi'},
                              {'nvidia.com/gpu': '1'}]),     # multi-container pod sums
        ('gpu-a', 'Running', [{'nvidia.com/mig-1g.10gb': '2'}]),
        ('gpu-a', 'Succeeded', [{'nvidia.com/gpu': '5'}]),   # terminated: releases devices
        ('gpu-b', 'Pending', [{'amd.com/gpu': '1'}]),        # any domain-qualified resource
        (None, 'Pending', [{'nvidia.com/gpu': '3'}]))        # unscheduled: holds nothing yet
    run, _ = _fake_run({'pods -A': pods})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.gpu_units_in_use() == {
        'gpu-a': {'nvidia.com/gpu': 2, 'nvidia.com/mig-1g.10gb': 2},
        'gpu-b': {'amd.com/gpu': 1}}
    run_empty, _ = _fake_run({})
    monkeypatch.setattr(subprocess, 'run', run_empty)
    assert cluster.gpu_units_in_use() == {}


def test_gpu_availability_subtracts_usage(monkeypatch):
    nodes = _named_nodes_json(
        ('gpu-a', {}, {'nvidia.com/gpu': '4'}),
        ('gpu-b', {}, {'nvidia.com/gpu': '2'}),
        ('cpu-1', {}, {}))
    run, _ = _fake_run({'gpu-pool=true -o json': nodes})
    monkeypatch.setattr(subprocess, 'run', run)
    in_use = {'gpu-a': {'nvidia.com/gpu': 3}, 'elsewhere': {'nvidia.com/gpu': 9}}
    avail = cluster.gpu_availability(in_use = in_use)
    assert (avail.allocatable, avail.in_use, avail.free) == (6, 3, 3)
    # Per-pod bound: gpu-b's 2 untouched cards beat gpu-a's 1 free one.
    assert avail.max_free_on_node == 2
    # Excluded nodes contribute nothing to any number.
    monkeypatch.setattr(subprocess, 'run', run)
    narrowed = cluster.gpu_availability(in_use = in_use, exclude_nodes = {'gpu-b'})
    assert (narrowed.allocatable, narrowed.free, narrowed.max_free_on_node) == (4, 1, 1)


def test_classify_gpu_resource(monkeypatch):
    '''The classification table: name says MIG; labels say time-sliced or
    single-strategy MIG; GFD present and quiet says physical; nothing says unknown.'''
    assert cluster.classify_gpu_resource(resource = 'nvidia.com/mig-3g.40gb') == 'mig'

    single = _nodes_json(({'nvidia.com/mig.capable': 'true',
                           'nvidia.com/mig.strategy': 'single',
                           'nvidia.com/gpu.product': 'NVIDIA-A100-SXM4-80GB'},
                          {'nvidia.com/gpu': '14'}))
    run, _ = _fake_run({'nodes -o json': single})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.classify_gpu_resource() == 'mig'

    sliced = _nodes_json(({'nvidia.com/gpu.replicas': '4'}, {'nvidia.com/gpu': '4'}))
    run, _ = _fake_run({'nodes -o json': sliced})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.classify_gpu_resource() == 'time-sliced'

    physical = _nodes_json((_PHYSICAL_LABELS, {'nvidia.com/gpu': '2'}))
    run, _ = _fake_run({'nodes -o json': physical})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.classify_gpu_resource() == 'physical'

    # Worst case wins across nodes: one sliced advertiser taints the pool.
    mixed = _nodes_json((_PHYSICAL_LABELS, {'nvidia.com/gpu': '2'}),
                        ({'nvidia.com/gpu.replicas': '4'}, {'nvidia.com/gpu': '4'}))
    run, _ = _fake_run({'nodes -o json': mixed})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.classify_gpu_resource() == 'time-sliced'

    unlabeled = _nodes_json(({}, {'nvidia.com/gpu': '2'}))
    run, _ = _fake_run({'nodes -o json': unlabeled})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.classify_gpu_resource() == 'unknown'


# -- flavor registry -------------------------------------------------------

def test_detect_reuses_one_node_label_query(monkeypatch):
    '''
    Node labels are fetched lazily and at most once per detection, so adding
    label-probing flavors does not multiply kubectl calls.
    '''
    run, calls = _fake_run({'current-context': 'default', 'jsonpath={range': 'k3s'})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.detect_cluster() == cluster.K3S
    label_queries = [c for c in calls if 'instance-type' in ' '.join(c)]
    assert len(label_queries) == 1


@pytest.mark.parametrize('ctx,expected', [
    ('kind-dev', cluster.KIND),
    ('minikube', cluster.MINIKUBE),
    ('docker-desktop', cluster.DOCKER_DESKTOP),
])
def test_context_named_flavors_need_no_node_query(monkeypatch, ctx, expected):
    '''
    kind/minikube/docker-desktop are decided from the context name alone. This is
    why they are registered ahead of the label-only k3s handler: put k3s first and
    a context-named minikube pays for k3s's node-label probe before its own check
    ever runs. Parametrized over all three, since testing only kind would pass
    while minikube regressed.
    '''
    run, calls = _fake_run({'current-context': ctx})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.detect_cluster() == expected
    assert not any('instance-type' in ' '.join(c) for c in calls)


def test_registering_a_flavor_covers_all_three_behaviors(monkeypatch):
    '''
    The point of the registry: one class teaches detection, image loading and the
    hostPath warning at once, instead of three separate ladder edits.
    '''
    loaded = []

    class _Colima(cluster.ClusterFlavorHandler):
        name = 'colima'

        def matches(self, context_name, node_labels):
            return context_name == 'colima'

        def load_images(self, images, kubectl = 'kubectl'):
            loaded.extend(images)

        def hostpath_warning(self):
            return 'colima runs in a VM'

    monkeypatch.setattr(cluster, '_FLAVORS', list(cluster._FLAVORS))
    cluster.register_cluster_flavor(_Colima())

    run, _ = _fake_run({'current-context': 'colima'})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.detect_cluster() == 'colima'
    cluster.load_images('colima', ['img:1'])
    assert loaded == ['img:1']
    assert cluster.hostpath_warning('colima') == 'colima runs in a VM'


def test_generic_remote_stays_the_terminal_fallback(monkeypatch):
    '''A registered flavor must not shadow the catch-all that matches everything.'''
    class _Never(cluster.ClusterFlavorHandler):
        name = 'never'

        def matches(self, context_name, node_labels):
            return False

    monkeypatch.setattr(cluster, '_FLAVORS', list(cluster._FLAVORS))
    cluster.register_cluster_flavor(_Never())
    assert cluster._FLAVORS[-1].name == cluster.GENERIC_REMOTE

    run, _ = _fake_run({'current-context': 'some-eks-cluster'})
    monkeypatch.setattr(subprocess, 'run', run)
    assert cluster.detect_cluster() == cluster.GENERIC_REMOTE


def test_registering_before_an_unknown_flavor_is_rejected():
    with pytest.raises(ValueError, match = 'no registered cluster flavor'):
        cluster.register_cluster_flavor(cluster.ClusterFlavorHandler(), before = 'nope')


def test_unknown_flavor_raises_on_load_but_not_on_warning():
    with pytest.raises(RuntimeError, match = 'unknown cluster flavor'):
        cluster.load_images('nope', ['img:1'])
    # Advisory: a missing warning must not fail a deploy.
    assert cluster.hostpath_warning('nope') is None
