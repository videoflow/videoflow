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

import subprocess
from typing import Callable, List, Optional

# SHARED_NEEDS_RUNTIME_CLASS lived here before the GPU strategies were extracted;
# cli.py still imports it from this module to detect the fatal shared-mode problem.
from .gpu import SHARED_NEEDS_RUNTIME_CLASS, get_gpu_mode  # noqa: F401

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

class _K3sFlavor(ClusterFlavorHandler):
    name = K3S

    def matches(self, context_name : str, node_labels : Callable[[], str]) -> bool:
        # k3s uses a generic context name ('default'), so node facts decide.
        return 'k3s' in node_labels()

    def load_images(self, images : List[str], kubectl : str = 'kubectl') -> None:
        for image in images:
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
    Total allocatable units of one GPU extended resource across all nodes
    (0 when no node advertises it or the cluster is unreachable).
    '''
    # jsonpath needs the dots inside the key escaped: nvidia.com/gpu -> nvidia\.com/gpu
    path = resource.replace('.', '\\.')
    out = _kubectl_out(kubectl, 'get', 'nodes', '-o',
                       'jsonpath={.items[*].status.allocatable.' + path + '}')
    return sum(int(v) for v in out.split() if v.isdigit())

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
                  gpu_mode : str = 'exclusive') -> List[str]:
    '''
    Checks what a GPU node workload needs (see ``manifests._pod_spec``): a node
    labeled ``videoflow.io/gpu-pool=true``; in exclusive mode, enough allocatable
    units of each requested extended resource to satisfy the flow's whole demand
    (an under-provisioned flow schedules partially and stalls with the rest of its
    pods Pending); and — where the NVIDIA container runtime is an opt-in
    RuntimeClass rather than the node default — a ``--gpu-runtime-class``, without
    which the pod schedules and then runs with no device. Returns problem strings
    with copy-pasteable fixes (empty list = OK).

    - Arguments:
        - demand: dict of extended-resource name -> total units the flow requests \
            (sum over GPU nodes of ``nb_tasks * gpu_count``), or None to skip the \
            capacity comparison and only check that the resource exists.
        - gpu_mode: ``'exclusive'`` or ``'shared'``. Shared pods carry no resource \
            limit, so the capacity/device-plugin checks are skipped; the RuntimeClass \
            check is escalated instead, because in shared mode the runtime class is \
            the only thing granting device access at all.
    '''
    problems = []
    # An unreachable cluster makes every check below come back empty, which would
    # otherwise be reported as "no GPU nodes" and send the operator chasing a
    # device-plugin install that was never the problem.
    if _kubectl_out(kubectl, 'version', '-o', 'json') == '':
        return ['cannot reach the cluster — check that it is running and that '
                'kubectl is pointed at the right context (kubectl config current-context)']
    labeled = _kubectl_out(kubectl, 'get', 'nodes', '-l', 'videoflow.io/gpu-pool=true', '-o', 'name')
    if not labeled:
        nodes = _kubectl_out(kubectl, 'get', 'nodes', '-o', 'name').splitlines()
        example = nodes[0].removeprefix('node/') if nodes else '<node-name>'
        problems.append(f'no node labeled videoflow.io/gpu-pool=true — GPU pods will stay '
                        f'Pending. Fix: kubectl label node {example} videoflow.io/gpu-pool=true')
    # Everything past this point depends on how the mode claims devices — capacity
    # math only means something with a resource limit, and the RuntimeClass check is
    # merely advisory in exclusive mode but fatal in shared. The strategy owns both.
    problems.extend(get_gpu_mode(gpu_mode).preflight_problems(
        kubectl = kubectl, demand = demand, gpu_runtime_class = gpu_runtime_class))
    return problems
