'''
Best-effort detection of what kind of Kubernetes cluster kubectl points at, and
the cluster-flavor-specific mechanics that depend on it: how to load a locally
built image into it, whether hostPath mounts see the local filesystem, and
whether GPU pods are schedulable. Everything here is advisory — deploy still
works with explicit flags when detection gets it wrong.

Each flavor is one ``ClusterFlavorHandler`` registered at import. Supporting a
new one (microk8s, colima, k0s) is a class plus a ``register_cluster_flavor``
call rather than an edit to three parallel if-ladders, which is what this used to
be — and what made it easy to teach detection about a flavor while forgetting to
teach image loading about it.
'''
from __future__ import absolute_import, division, print_function

import json
import subprocess
from dataclasses import dataclass, field
from typing import AbstractSet, Callable, Dict, Iterable, List, Mapping, Optional

from ..backends.outcomes import Observation, Unknown, is_known, known, unknown, value_or
from .gpu import GPU_OWNER_LABEL, GPU_POOL_LABEL, get_gpu_mode
from .mig import NodeInventory

#: Selector for the nodes GPU pods can actually schedule on (they carry the
#: matching nodeSelector — see ``manifests._pod_spec``). Every capacity and
#: inventory read below scopes to it: counting a GPU outside the pool would
#: report capacity the flow's pods can never use.
_POOL_SELECTOR : str = GPU_POOL_LABEL + '=true'

K3S = 'k3s'
KIND = 'kind'
MINIKUBE = 'minikube'
DOCKER_DESKTOP = 'docker-desktop'
GENERIC_REMOTE = 'generic-remote'

def _kubectl_out(kubectl : str, *args : str) -> str:
    '''Runs kubectl and returns stdout, or '' on any failure (detection is best-effort).'''
    try:
        proc = subprocess.run([kubectl, *args], capture_output = True, text = True, check = False)
    except FileNotFoundError:
        return ''
    return proc.stdout.strip() if proc.returncode == 0 else ''

def _kubectl_observed(kubectl : str, *args : str) -> Observation[str]:
    '''
    Runs kubectl and says what happened: ``Known(stdout)`` on success, otherwise
    ``Unknown`` with the reason (``missing`` binary, ``failed`` command). The
    observed counterpart of ``_kubectl_out`` for reads that gate a decision — an
    occupancy the API refused to list is unknown, not zero.
    '''
    try:
        proc = subprocess.run([kubectl, *args], capture_output = True, text = True, check = False)
    except FileNotFoundError:
        return unknown('missing', f'{kubectl!r} is not on PATH')
    if proc.returncode != 0:
        detail = (proc.stderr or proc.stdout).strip().splitlines()
        return unknown('failed', (detail[-1] if detail else f'{kubectl} exited {proc.returncode}')[:300])
    return known(proc.stdout.strip())

def current_context(kubectl : str = 'kubectl') -> str:
    return _kubectl_out(kubectl, 'config', 'current-context')

def _node_labels(kubectl : str = 'kubectl') -> str:
    '''
    Node labels used to recognize flavors whose context name is generic (a k3s
    install's context is just 'default').
    '''
    return _kubectl_out(kubectl, 'get', 'nodes', '-o',
        'jsonpath={range .items[*]}{.metadata.labels.node\\.kubernetes\\.io/instance-type}'
        '{" "}{.metadata.labels.minikube\\.k8s\\.io/name}{"\\n"}{end}')

# -- flavor handlers -------------------------------------------------------

class ClusterFlavorHandler:
    '''
    One local-cluster flavor: how to recognize it, how to get a locally built
    image into it, and whether its hostPath mounts see the host filesystem.

    Subclasses set ``name`` and implement ``matches``. ``load_images`` defaults to
    refusing, which is the right answer for anything remote.
    '''
    #: Flavor identifier, returned by ``detect_cluster`` and passed back to
    #: ``load_images``/``hostpath_warning``.
    name : str = ''

    def matches(self, context_name : str, node_labels : Callable[[], str]) -> bool:
        '''
        Whether the current cluster is this flavor. ``node_labels`` is a callable
        so a handler that can decide from the context name alone costs no kubectl
        call; the result is shared between handlers that do need it.
        '''
        raise NotImplementedError('ClusterFlavorHandler subclass must implement matches()')

    def load_images(self, images : List[str], kubectl : str = 'kubectl') -> None:
        '''
        Side-loads locally built images so pods can run them without a registry.

        - Raises:
            - ``RuntimeError`` when the images cannot be loaded (remote cluster, \
                missing tool, failed command). The message must name the fix.
        '''
        refs = ' '.join(images)
        raise RuntimeError(
            f'cluster looks remote — pods there cannot see locally built images. '
            f'Push them to a registry the cluster can reach and pass --image, e.g.:\n'
            f'  docker tag <local> <registry>/<name>:<tag> && docker push <registry>/<name>:<tag>\n'
            f'(locally built: {refs})')

    def hostpath_warning(self) -> Optional[str]:
        '''Message when hostPath will not resolve against the local filesystem, else None.'''
        return None

class _KindFlavor(ClusterFlavorHandler):
    name = KIND

    def matches(self, context_name : str, node_labels : Callable[[], str]) -> bool:
        return context_name.startswith('kind-')

    def load_images(self, images : List[str], kubectl : str = 'kubectl') -> None:
        name = current_context(kubectl).removeprefix('kind-') or 'kind'
        _run_load(['kind', 'load', 'docker-image', *images, '--name', name])

    def hostpath_warning(self) -> Optional[str]:
        return ('this is a kind cluster: hostPath resolves inside the kind node '
                'container, not on your host. Recreate the cluster with extraMounts '
                'covering each mounted path (https://kind.sigs.k8s.io/docs/user/configuration/#extra-mounts).')

class _MinikubeFlavor(ClusterFlavorHandler):
    name = MINIKUBE

    def matches(self, context_name : str, node_labels : Callable[[], str]) -> bool:
        return context_name == 'minikube' or 'minikube' in node_labels()

    def load_images(self, images : List[str], kubectl : str = 'kubectl') -> None:
        for image in images:
            _run_load(['minikube', 'image', 'load', image])

    def hostpath_warning(self) -> Optional[str]:
        return ('this is a minikube cluster: hostPath resolves inside the minikube '
                'VM. Expose each mounted path with `minikube mount /path:/path` or '
                'start minikube with --mount --mount-string=/path:/path.')

class _DockerDesktopFlavor(ClusterFlavorHandler):
    name = DOCKER_DESKTOP

    def matches(self, context_name : str, node_labels : Callable[[], str]) -> bool:
        return context_name == 'docker-desktop'

    def load_images(self, images : List[str], kubectl : str = 'kubectl') -> None:
        print('docker-desktop shares the local docker daemon; images need no loading.')

def is_registry_qualified(image : str) -> bool:
    '''
    Whether an image reference names a registry — its first path component has
    a dot or a port, or is ``localhost`` (the Docker reference grammar's rule
    for telling ``registry.example/ns/img`` from the implicit ``docker.io``
    ``ns/img``). Such an image was pushed somewhere the cluster pulls from;
    a bare local tag was only ever built here.
    '''
    first = image.split('/', 1)[0]
    return '/' in image and ('.' in first or ':' in first or first == 'localhost')

class _K3sFlavor(ClusterFlavorHandler):
    name = K3S

    def matches(self, context_name : str, node_labels : Callable[[], str]) -> bool:
        # k3s uses a generic context name ('default'), so node facts decide.
        return 'k3s' in node_labels()

    def load_images(self, images : List[str], kubectl : str = 'kubectl') -> None:
        for image in images:
            if is_registry_qualified(image):
                # Pushed to a registry the nodes pull from (containerd's
                # registries.yaml): importing it into this node's containerd would
                # be redundant, needs sudo, and reaches only one of the nodes.
                print(f'Image {image} is registry-qualified; the nodes pull it, no import needed.')
                continue
            _k3s_import(image)

class _GenericRemoteFlavor(ClusterFlavorHandler):
    '''Terminal fallback: inherits the base's refuse-to-load behavior.'''
    name = GENERIC_REMOTE

    def matches(self, context_name : str, node_labels : Callable[[], str]) -> bool:
        return True

#: Handlers in detection order. Flavors that can be recognized from the context
#: name alone (kind, docker-desktop, minikube) are registered before the label-only
#: one (k3s), so a cluster identifiable by its context name is detected without any
#: `kubectl get nodes` call — and GENERIC_REMOTE stays last as the terminal
#: fallback. Registering k3s ahead of minikube would make a context-named minikube
#: cluster pay for k3s's label probe first, which is the regression this order
#: avoids.
_FLAVORS : List[ClusterFlavorHandler] = []

def register_cluster_flavor(handler : ClusterFlavorHandler,
                            before : Optional[str] = GENERIC_REMOTE) -> None:
    '''
    Registers a cluster flavor. Detection tries handlers in registration order, so
    a new handler is inserted before ``before`` (by default the generic-remote
    fallback, which matches everything and must stay last).

    - Arguments:
        - handler: the flavor handler instance.
        - before: name of the handler to insert ahead of, or None to append.

    - Raises:
        - ValueError: ``before`` names no registered flavor.
    '''
    if before is None:
        _FLAVORS.append(handler)
        return
    for i, existing in enumerate(_FLAVORS):
        if existing.name == before:
            _FLAVORS.insert(i, handler)
            return
    raise ValueError(f'no registered cluster flavor named {before!r}; '
                     f'known: {", ".join(f.name for f in _FLAVORS)}')

for _flavor in (_KindFlavor(), _DockerDesktopFlavor(), _MinikubeFlavor(),
                _K3sFlavor(), _GenericRemoteFlavor()):
    register_cluster_flavor(_flavor, before = None)

def get_cluster_flavor(cluster : str) -> ClusterFlavorHandler:
    '''
    The handler registered under ``cluster``.

    - Raises:
        - ``RuntimeError`` when no flavor is registered under that name.
    '''
    for handler in _FLAVORS:
        if handler.name == cluster:
            return handler
    # RuntimeError, not the ValueError the other registries raise: load_images has
    # always raised RuntimeError for this and callers catch that type. The message
    # still names the known values and the fix, as the seam convention requires.
    raise RuntimeError(f'unknown cluster flavor: {cluster}. Known flavors: '
                       f'{", ".join(f.name for f in _FLAVORS)}. Register another with '
                       f'videoflow.deploy.cluster.register_cluster_flavor.')

def detect_cluster(kubectl : str = 'kubectl') -> str:
    '''
    Classifies the cluster kubectl currently points at, by asking each registered
    flavor in order. Context-name conventions identify kind/minikube/docker-desktop;
    k3s installs use a generic context name ('default'), so they are confirmed via
    node facts instead — fetched at most once per call, and only if some handler
    asks for them.
    '''
    ctx = current_context(kubectl)
    cached : List[str] = []

    def node_labels() -> str:
        if not cached:
            cached.append(_node_labels(kubectl))
        return cached[0]

    for handler in _FLAVORS:
        if handler.matches(ctx, node_labels):
            return handler.name
    return GENERIC_REMOTE

def load_images(cluster : str, images : List[str], kubectl : str = 'kubectl') -> None:
    '''
    Loads locally built docker images into the detected cluster so pods can pull
    them without a registry.

    - Raises:
        - ``RuntimeError`` when the cluster is remote (push to a registry instead), \
            when a required tool is missing, or when a load command fails.
    '''
    get_cluster_flavor(cluster).load_images(images, kubectl)

def _run_load(cmd : List[str]) -> None:
    try:
        proc = subprocess.run(cmd, check = False)
    except FileNotFoundError as e:
        raise RuntimeError(f'{cmd[0]!r} not found on PATH — install it or load the '
                           f'image manually: {" ".join(cmd)}') from e
    if proc.returncode != 0:
        raise RuntimeError(f'image load failed: {" ".join(cmd)}')

def _k3s_import(image : str) -> None:
    '''docker save | sudo k3s ctr images import - (k3s containerd socket is root-owned).'''
    pipeline = f'docker save {image} | sudo k3s ctr images import -'
    try:
        save = subprocess.Popen(['docker', 'save', image], stdout = subprocess.PIPE)
        imp = subprocess.run(['sudo', 'k3s', 'ctr', 'images', 'import', '-'],
                             stdin = save.stdout, check = False)
        if save.stdout is not None:
            save.stdout.close()
        save.wait()
    except FileNotFoundError as e:
        raise RuntimeError(f'{e.filename!r} not found on PATH — load the image '
                           f'manually: {pipeline}') from e
    if save.returncode != 0 or imp.returncode != 0:
        raise RuntimeError(f'image load into k3s failed — retry manually: {pipeline}')

def hostpath_warning(cluster : str) -> Optional[str]:
    '''
    A message when hostPath mounts will NOT resolve against the local filesystem
    (the cluster "node" is a VM/container with its own filesystem), else None.
    An unregistered flavor warns nothing rather than raising: this is advisory,
    and deploy should not fail over a missing warning.
    '''
    try:
        return get_cluster_flavor(cluster).hostpath_warning()
    except RuntimeError:
        return None

def allocatable_gpus(kubectl : str = 'kubectl', resource : str = 'nvidia.com/gpu') -> int:
    '''
    Total allocatable units of one GPU extended resource across the pool's nodes
    (0 when no pool node advertises it or the cluster is unreachable).
    '''
    # jsonpath needs the dots inside the key escaped: nvidia.com/gpu -> nvidia\.com/gpu
    path = resource.replace('.', '\\.')
    out = _kubectl_out(kubectl, 'get', 'nodes', '-l', _POOL_SELECTOR, '-o',
                       'jsonpath={.items[*].status.allocatable.' + path + '}')
    return sum(int(v) for v in out.split() if v.isdigit())

def max_allocatable_gpus_per_node(kubectl : str = 'kubectl',
                                resource : str = 'nvidia.com/gpu') -> int:
    '''
    The largest allocatable count of one GPU extended resource on any single pool
    node (0 when no pool node advertises it or the cluster is unreachable). The
    per-pod schedulability bound, complementing ``allocatable_gpus``: total capacity
    answers "will the whole flow schedule", this answers "can any single node
    host the biggest pod" — all of a pod's ``gpu_count`` devices must come from
    one node, so a flow can pass the total-capacity check and still never schedule.
    '''
    path = resource.replace('.', '\\.')
    out = _kubectl_out(kubectl, 'get', 'nodes', '-l', _POOL_SELECTOR, '-o',
                       'jsonpath={.items[*].status.allocatable.' + path + '}')
    return max((int(v) for v in out.split() if v.isdigit()), default = 0)

def gpu_units_in_use_observed(kubectl : str = 'kubectl') -> Observation[Dict[str, Dict[str, int]]]:
    '''
    Extended-resource units currently claimed by pods, as node name ->
    resource -> units, summed over the ``resources.limits`` of every
    non-terminated pod in the cluster (all namespaces — a foreign workload's
    claim occupies a device just as much as ours). Extended resources always
    carry a domain, so keys containing ``/`` are counted and native resources
    (cpu, memory, ``hugepages-*``) are not.

    ``Unknown`` when the pod listing could not be read or parsed. Callers that
    decide anything destructive on occupancy must branch on that: a failed read
    does not prove a device idle.
    '''
    out = _kubectl_observed(kubectl, 'get', 'pods', '-A', '-o', 'json')
    if isinstance(out, Unknown):
        return out
    try:
        pods = json.loads(out.value).get('items', [])
    except ValueError:
        return unknown('malformed', 'pod listing was not JSON')
    used : Dict[str, Dict[str, int]] = {}
    for pod in pods:
        if (pod.get('status') or {}).get('phase') in ('Succeeded', 'Failed'):
            continue
        node = (pod.get('spec') or {}).get('nodeName')
        if not node:
            continue
        for container in (pod.get('spec') or {}).get('containers') or []:
            limits = (container.get('resources') or {}).get('limits') or {}
            for name, value in limits.items():
                if '/' not in name or not str(value).isdigit():
                    continue
                per_node = used.setdefault(node, {})
                per_node[name] = per_node.get(name, 0) + int(value)
    return known(used, out.generation)

def gpu_units_in_use(kubectl : str = 'kubectl') -> Dict[str, Dict[str, int]]:
    '''
    ``gpu_units_in_use_observed`` for display-only callers: ``{}`` when unknown.
    Anything that plans or mutates on occupancy must use the observed form.
    '''
    return value_or(gpu_units_in_use_observed(kubectl), {})

@dataclass
class GpuAvailability:
    '''
    One GPU extended resource's pool capacity with occupancy subtracted:
    what is allocatable per node, what running pods already claim, and the
    derived free numbers preflight compares demand against. Raw allocatable
    alone lies in a shared cluster — the scheduler will not grant units that
    other workloads hold.
    '''
    per_node_allocatable : Dict[str, int] = field(default_factory = dict)
    per_node_in_use : Dict[str, int] = field(default_factory = dict)
    #: False when the pod listing behind ``per_node_in_use`` could not be read:
    #: ``free`` is then an upper bound, not a fact, and preflight says so.
    occupancy_known : bool = True

    @property
    def allocatable(self) -> int:
        return sum(self.per_node_allocatable.values())

    @property
    def in_use(self) -> int:
        return sum(self.per_node_in_use.get(n, 0) for n in self.per_node_allocatable)

    @property
    def free(self) -> int:
        return max(0, self.allocatable - self.in_use)

    @property
    def max_free_on_node(self) -> int:
        '''The per-pod bound: the most units any single node can still grant.'''
        return max((count - self.per_node_in_use.get(node, 0)
                    for node, count in self.per_node_allocatable.items()), default = 0)

def gpu_availability(kubectl : str = 'kubectl', resource : str = 'nvidia.com/gpu',
                     in_use : Optional[Dict[str, Dict[str, int]]] = None,
                     exclude_nodes : AbstractSet[str] = frozenset()) -> GpuAvailability:
    '''
    Pool-scoped availability of one GPU extended resource. ``in_use`` accepts a
    pre-fetched ``gpu_units_in_use()`` result so a caller checking several
    resources fetches the pod list once; None fetches it here. ``exclude_nodes``
    drops nodes the caller has already ruled out (e.g. mix planning excluded
    them as another flow's), so capacity math matches what will be planned.
    '''
    out = _kubectl_out(kubectl, 'get', 'nodes', '-l', _POOL_SELECTOR, '-o', 'json')
    if not out:
        return GpuAvailability()
    try:
        nodes = json.loads(out).get('items', [])
    except ValueError:
        return GpuAvailability()
    availability = GpuAvailability()
    if in_use is None:
        observed = gpu_units_in_use_observed(kubectl)
        availability.occupancy_known = is_known(observed)
        in_use = value_or(observed, {})
    for node in nodes:
        name = (node.get('metadata') or {}).get('name', '')
        if not name or name in exclude_nodes:
            continue
        count = str(((node.get('status') or {}).get('allocatable') or {}).get(resource, '')).strip()
        if not count.isdigit() or int(count) < 1:
            continue
        availability.per_node_allocatable[name] = int(count)
        availability.per_node_in_use[name] = in_use.get(name, {}).get(resource, 0)
    return availability

GPU_KINDS = ('physical', 'mig', 'time-sliced', 'mps', 'unknown')

def classify_gfd_labels(labels : Mapping[str, str], advertised_units : object = None) -> str:
    '''
    What one node's GPU units are, from its GPU Feature Discovery labels:
    ``'physical'`` (one unit = one whole device), ``'mig'`` (hardware-isolated
    slices), ``'time-sliced'`` or ``'mps'`` (shares of a device — MPS is
    recognised from its explicit strategy label alone, with or without the
    replica/``-SHARED`` auxiliaries), or ``'unknown'`` (no GFD labels to judge
    by). The pure rule behind ``classify_gpu_resource``; the reference
    allocator's node fixtures use the same one so both agree on every fixture.

    - Arguments:
        - advertised_units: the node's allocatable count of the resource, when \
            known. Under the ``single`` MIG strategy slices are advertised under \
            the plain resource name, so more units than ``gpu.count`` cards is \
            geometry evidence; a MIG-*capable* node with MIG disabled advertises \
            whole cards and is physical.
    '''
    product = str(labels.get('nvidia.com/gpu.product', ''))
    replicas = str(labels.get('nvidia.com/gpu.replicas', '')).strip()
    card_count = str(labels.get('nvidia.com/gpu.count', '')).strip()
    advertised = '' if advertised_units is None else str(advertised_units).strip()
    more_units_than_cards = (card_count.isdigit() and advertised.isdigit()
                             and int(advertised) > int(card_count))
    if labels.get('nvidia.com/gpu.sharing-strategy') == 'mps':
        return 'mps'
    if (labels.get('nvidia.com/gpu.sharing-strategy') == 'time-slicing'
            or product.endswith('-SHARED')
            or (replicas.isdigit() and int(replicas) > 1)):
        return 'time-sliced'
    if (labels.get('nvidia.com/mig.strategy') == 'single'
            and str(labels.get('nvidia.com/mig.capable', '')).lower() == 'true'
            and ('-MIG-' in product or more_units_than_cards
                 or (labels.get('nvidia.com/mig.config') not in (None, '', 'all-disabled')
                     and labels.get('nvidia.com/mig.config.state') == 'success'))):
        return 'mig'
    if any(key.startswith('nvidia.com/gpu') for key in labels):
        return 'physical'
    return 'unknown'

def combine_classifications(kinds : Iterable[str]) -> str:
    '''
    One answer for a pool: worst case wins across advertisers. A share kind
    anywhere makes multi-unit claims impossible everywhere (the scheduler may
    place the pod on any advertising node), MIG likewise; and an unlabeled
    advertiser next to a physical one is not "physical" — nothing proves its
    units are whole devices — it is unresolved.
    '''
    present = set(kinds)
    for kind in ('time-sliced', 'mps', 'mig', 'unknown', 'physical'):
        if kind in present:
            return kind
    return 'unknown'

def _pool_nodes(kubectl : str) -> Optional[list]:
    '''
    The pool's node objects (``videoflow.io/gpu-pool=true``), or None when the
    listing could not be read or parsed. One listing serves both classification
    and capacity, so a sharing label on a node *outside* the pool — which the
    scheduler will never pick for a pool workload — cannot taint the pool's
    classification, and a label on a pool node is seen by both.
    '''
    out = _kubectl_out(kubectl, 'get', 'nodes', '-l', _POOL_SELECTOR, '-o', 'json')
    if not out:
        return None
    try:
        return json.loads(out).get('items', [])
    except ValueError:
        return None

def classify_gpu_resource(kubectl : str = 'kubectl',
                        resource : str = 'nvidia.com/gpu',
                        exclude_nodes : AbstractSet[str] = frozenset()) -> str:
    '''
    What one advertised GPU extended resource's units actually are, from GPU
    Feature Discovery node labels: ``'physical'`` (one unit = one whole device),
    ``'mig'`` (units are hardware-isolated MIG slices), ``'time-sliced'`` (units
    are shares of a device), or ``'unknown'`` (no GFD labels to judge by — e.g. a
    non-NVIDIA resource, or a cluster without GFD).

    This is what makes ``gpu_count > 1`` checkable: the scheduler happily grants N
    units of any integer resource, but only whole physical devices can be spanned
    by one model. Classification looks only at **pool** nodes (the same
    ``videoflow.io/gpu-pool=true`` snapshot the capacity math reads, minus
    ``exclude_nodes``) that advertise ``resource`` — a time-sliced node outside the
    pool advertising the same name is not somewhere a pool workload can land:

    - a ``mig-`` final path segment, or ``nvidia.com/mig.strategy=single`` on a
      MIG-capable advertising node (slices renamed to ``nvidia.com/gpu``) → mig;
    - ``nvidia.com/gpu.sharing-strategy=time-slicing``, a ``-SHARED`` product
      suffix, or ``nvidia.com/gpu.replicas`` > 1 → time-sliced;
    - GFD labels present and none of the above → physical.

    Worst case wins across nodes (any time-sliced advertiser taints the answer):
    the scheduler may place the pod on any advertising node, so the safe claim is
    the weakest one.
    '''
    # The name is definitive on its own: nvidia.com/mig-<profile> is always MIG.
    if resource.rsplit('/', 1)[-1].startswith('mig-'):
        return 'mig'
    listed = _pool_nodes(kubectl)
    if listed is None:
        return 'unknown'
    nodes = [n for n in listed if (n.get('metadata') or {}).get('name') not in exclude_nodes]
    kinds : set[str] = set()
    for node in nodes:
        allocatable = (node.get('status') or {}).get('allocatable') or {}
        if resource not in allocatable:
            continue
        labels = (node.get('metadata') or {}).get('labels') or {}
        kinds.add(classify_gfd_labels(labels, allocatable.get(resource)))
    return combine_classifications(kinds)

def gpu_inventory_observed(kubectl : str = 'kubectl') -> Observation[List[NodeInventory]]:
    '''
    The videoflow pool's GPU inventory as ``NodeInventory`` records — only nodes
    labeled ``videoflow.io/gpu-pool=true``, because that is the only place the
    rendered pods can schedule and the only nodes mix mode may repartition.
    Physical facts come off GPU Feature Discovery labels (``nvidia.com/gpu.count``,
    ``.product``, ``.memory`` — the last in MiB); nodes without those labels
    contribute nothing: without GFD there is no inventory to lay out, and the mix
    strategy reports that as its own preflight problem rather than guessing.

    Each record also carries the node's sharing/ownership state (time-slicing
    signals, existing MIG geometry, the ``videoflow.io/gpu-owner`` stamp, units
    in use by running pods). These are facts, not decisions: the cluster is
    multi-tenant, and ``MixGpu`` decides which nodes are usable.

    ``Unknown`` when the node listing itself failed; when only the pod listing
    failed, every record carries ``occupancy_known=False`` so the mix strategy
    refuses to plan on it rather than treat the pool as idle.
    '''
    out = _kubectl_observed(kubectl, 'get', 'nodes', '-l', _POOL_SELECTOR, '-o', 'json')
    if isinstance(out, Unknown):
        return out
    try:
        nodes = json.loads(out.value).get('items', [])
    except ValueError:
        return unknown('malformed', 'node listing was not JSON')
    used_obs = gpu_units_in_use_observed(kubectl)
    used = value_or(used_obs, {})
    inventory = []
    for node in nodes:
        labels = (node.get('metadata') or {}).get('labels') or {}
        product = labels.get('nvidia.com/gpu.product')
        count = str(labels.get('nvidia.com/gpu.count', '')).strip()
        memory_mib = str(labels.get('nvidia.com/gpu.memory', '')).strip()
        if not product or not count.isdigit() or int(count) < 1:
            continue
        name = (node.get('metadata') or {}).get('name', '')
        memory_gib = round(int(memory_mib) / 1024, 1) if memory_mib.isdigit() else 0.0
        # Same three time-slicing signals classify_gpu_resource keys on.
        replicas = str(labels.get('nvidia.com/gpu.replicas', '')).strip()
        time_sliced = (labels.get('nvidia.com/gpu.sharing-strategy') == 'time-slicing'
                       or str(product).endswith('-SHARED')
                       or (replicas.isdigit() and int(replicas) > 1))
        allocatable = (node.get('status') or {}).get('allocatable') or {}
        # Carved geometry shows as mixed-strategy nvidia.com/mig-* resources, or —
        # under single strategy — as a -MIG-<profile> suffix in the GFD product.
        mig_partitioned = (any(key.startswith('nvidia.com/mig-') for key in allocatable)
                           or '-MIG-' in str(product))
        inventory.append(NodeInventory(name = name, product = product,
                                       card_count = int(count),
                                       memory_gib_per_card = memory_gib,
                                       time_sliced = time_sliced,
                                       mig_config = labels.get('nvidia.com/mig.config'),
                                       mig_partitioned = mig_partitioned,
                                       owner = labels.get(GPU_OWNER_LABEL),
                                       used_units = used.get(name, {}),
                                       occupancy_known = is_known(used_obs)))
    return known(sorted(inventory, key = lambda n: n.name), out.generation)

def gpu_inventory(kubectl : str = 'kubectl') -> List[NodeInventory]:
    '''``gpu_inventory_observed`` for display-only callers: ``[]`` when unknown.'''
    return value_or(gpu_inventory_observed(kubectl), [])

def nvidia_runtimeclass(kubectl : str = 'kubectl') -> Optional[str]:
    '''
    The NVIDIA RuntimeClass name when the cluster registers one, else None. Prefers
    the conventional ``nvidia`` but also recognizes variant names (e.g. a distro
    registering ``nvidia-container-runtime``) so the shared-mode escalation cannot
    silently no-op on them.
    '''
    handlers = _kubectl_out(kubectl, 'get', 'runtimeclass', '-o',
                            'jsonpath={.items[*].metadata.name}').split()
    if 'nvidia' in handlers:
        return 'nvidia'
    return next((h for h in handlers if h.startswith('nvidia')), None)

def gpu_preflight(kubectl : str = 'kubectl', gpu_runtime_class : Optional[str] = None,
                  demand : Optional[dict] = None,
                  gpu_mode : str = 'exclusive',
                  max_per_pod : Optional[dict] = None,
                  pod_claims : Optional[dict] = None) -> List[str]:
    '''
    Checks what a GPU node workload needs (see ``manifests._pod_spec``): a node
    labeled ``videoflow.io/gpu-pool=true``; enough allocatable units of each
    requested extended resource to satisfy the flow's whole demand (an
    under-provisioned flow schedules partially and stalls with the rest of its
    pods Pending); and — where the NVIDIA container runtime is an opt-in
    RuntimeClass rather than the node default — a ``--gpu-runtime-class``, without
    which the pod schedules and then runs with no device. Returns problem strings
    with copy-pasteable fixes (empty list = OK). A problem prefixed with
    ``gpu.IMPOSSIBLE_GPU_REQUEST`` is fatal regardless of ``--strict-preflight``
    (the request cannot work by construction).

    - Arguments:
        - demand: dict of extended-resource name -> total units the flow requests \
            (sum over GPU nodes of ``nb_tasks * gpu_count``), or None to skip the \
            capacity comparison and only check that the resource exists.
        - gpu_mode: the ``--gpu-mode`` strategy name whose ``preflight_problems`` \
            runs the mode-specific checks.
        - max_per_pod: dict of extended-resource name -> largest single-pod claim \
            (``manifests.gpu_max_per_pod``), or None to skip the per-node capacity \
            and resource-classification checks (RFC 0003).
        - pod_claims: dict of extended-resource name -> one claim per pod replica \
            (``manifests.gpu_pod_claims``), or None to skip the per-host packing \
            check — the one that catches a pool whose free devices are fragmented \
            across hosts although the total and the largest node both suffice.
    '''
    problems = []
    # An unreachable cluster makes every check below come back empty, which would
    # otherwise be reported as "no GPU nodes" and send the operator chasing a
    # device-plugin install that was never the problem.
    if _kubectl_out(kubectl, 'version', '-o', 'json') == '':
        return ['cannot reach the cluster — check that it is running and that '
                'kubectl is pointed at the right context (kubectl config current-context)']
    labeled = _kubectl_out(kubectl, 'get', 'nodes', '-l', _POOL_SELECTOR, '-o', 'name')
    if not labeled:
        nodes = _kubectl_out(kubectl, 'get', 'nodes', '-o', 'name').splitlines()
        example = nodes[0].removeprefix('node/') if nodes else '<node-name>'
        problems.append(f'no node labeled {_POOL_SELECTOR} — GPU pods will stay '
                        f'Pending. Fix: kubectl label node {example} {_POOL_SELECTOR}')
    # Everything past this point depends on how the mode claims devices — capacity
    # math only means something with a resource limit, and the RuntimeClass check is
    # merely advisory in exclusive mode but fatal in shared. The strategy owns both.
    problems.extend(get_gpu_mode(gpu_mode).preflight_problems(
        kubectl = kubectl, demand = demand, gpu_runtime_class = gpu_runtime_class,
        max_per_pod = max_per_pod, pod_claims = pod_claims))
    return problems
