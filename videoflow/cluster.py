'''
Best-effort detection of what kind of Kubernetes cluster kubectl points at, and
the cluster-flavor-specific mechanics that depend on it: how to load a locally
built image into it, whether hostPath mounts see the local filesystem, and
whether GPU pods are schedulable. Everything here is advisory — deploy still
works with explicit flags when detection gets it wrong.
'''
from __future__ import absolute_import, division, print_function

import subprocess
from typing import List, Optional

K3S = 'k3s'
KIND = 'kind'
MINIKUBE = 'minikube'
DOCKER_DESKTOP = 'docker-desktop'
GENERIC_REMOTE = 'generic-remote'

def _kubectl_out(kubectl, *args) -> str:
    '''Runs kubectl and returns stdout, or '' on any failure (detection is best-effort).'''
    try:
        proc = subprocess.run([kubectl, *args], capture_output = True, text = True, check = False)
    except FileNotFoundError:
        return ''
    return proc.stdout.strip() if proc.returncode == 0 else ''

def current_context(kubectl = 'kubectl') -> str:
    return _kubectl_out(kubectl, 'config', 'current-context')

def detect_cluster(kubectl = 'kubectl') -> str:
    '''
    Classifies the cluster kubectl currently points at. Context-name conventions
    identify kind/minikube/docker-desktop; k3s installs use a generic context name
    ('default'), so it is confirmed via node facts instead.
    '''
    ctx = current_context(kubectl)
    if ctx.startswith('kind-'):
        return KIND
    if ctx == 'minikube':
        return MINIKUBE
    if ctx == 'docker-desktop':
        return DOCKER_DESKTOP
    labels = _kubectl_out(kubectl, 'get', 'nodes', '-o',
        'jsonpath={range .items[*]}{.metadata.labels.node\\.kubernetes\\.io/instance-type}'
        '{" "}{.metadata.labels.minikube\\.k8s\\.io/name}{"\\n"}{end}')
    if 'k3s' in labels:
        return K3S
    if 'minikube' in labels:
        return MINIKUBE
    return GENERIC_REMOTE

def load_images(cluster, images, kubectl = 'kubectl') -> None:
    '''
    Loads locally built docker images into the detected cluster so pods can pull
    them without a registry.

    - Raises:
        - ``RuntimeError`` when the cluster is remote (push to a registry instead), \
            when a required tool is missing, or when a load command fails.
    '''
    if cluster == DOCKER_DESKTOP:
        print('docker-desktop shares the local docker daemon; images need no loading.')
        return
    if cluster == GENERIC_REMOTE:
        refs = ' '.join(images)
        raise RuntimeError(
            f'cluster looks remote — pods there cannot see locally built images. '
            f'Push them to a registry the cluster can reach and pass --image, e.g.:\n'
            f'  docker tag <local> <registry>/<name>:<tag> && docker push <registry>/<name>:<tag>\n'
            f'(locally built: {refs})')
    if cluster == KIND:
        name = current_context(kubectl).removeprefix('kind-') or 'kind'
        _run_load(['kind', 'load', 'docker-image', *images, '--name', name])
        return
    if cluster == MINIKUBE:
        for image in images:
            _run_load(['minikube', 'image', 'load', image])
        return
    if cluster == K3S:
        for image in images:
            _k3s_import(image)
        return
    raise RuntimeError(f'unknown cluster flavor: {cluster}')

def _run_load(cmd) -> None:
    try:
        proc = subprocess.run(cmd, check = False)
    except FileNotFoundError as e:
        raise RuntimeError(f'{cmd[0]!r} not found on PATH — install it or load the '
                           f'image manually: {" ".join(cmd)}') from e
    if proc.returncode != 0:
        raise RuntimeError(f'image load failed: {" ".join(cmd)}')

def _k3s_import(image) -> None:
    '''docker save | sudo k3s ctr images import - (k3s containerd socket is root-owned).'''
    pipeline = f'docker save {image} | sudo k3s ctr images import -'
    try:
        save = subprocess.Popen(['docker', 'save', image], stdout = subprocess.PIPE)
        imp = subprocess.run(['sudo', 'k3s', 'ctr', 'images', 'import', '-'],
                             stdin = save.stdout, check = False)
        save.stdout.close()
        save.wait()
    except FileNotFoundError as e:
        raise RuntimeError(f'{e.filename!r} not found on PATH — load the image '
                           f'manually: {pipeline}') from e
    if save.returncode != 0 or imp.returncode != 0:
        raise RuntimeError(f'image load into k3s failed — retry manually: {pipeline}')

def hostpath_warning(cluster) -> Optional[str]:
    '''
    A message when hostPath mounts will NOT resolve against the local filesystem
    (the cluster "node" is a VM/container with its own filesystem), else None.
    '''
    if cluster == KIND:
        return ('this is a kind cluster: hostPath resolves inside the kind node '
                'container, not on your host. Recreate the cluster with extraMounts '
                'covering each mounted path (https://kind.sigs.k8s.io/docs/user/configuration/#extra-mounts).')
    if cluster == MINIKUBE:
        return ('this is a minikube cluster: hostPath resolves inside the minikube '
                'VM. Expose each mounted path with `minikube mount /path:/path` or '
                'start minikube with --mount --mount-string=/path:/path.')
    return None

def gpu_preflight(kubectl = 'kubectl') -> List[str]:
    '''
    Checks the two things a GPU node workload needs to schedule (see
    ``manifests._pod_spec``): a node labeled ``videoflow.io/gpu-pool=true`` and a
    node advertising allocatable ``nvidia.com/gpu``. Returns problem strings with
    copy-pasteable fixes (empty list = OK).
    '''
    problems = []
    labeled = _kubectl_out(kubectl, 'get', 'nodes', '-l', 'videoflow.io/gpu-pool=true', '-o', 'name')
    if not labeled:
        nodes = _kubectl_out(kubectl, 'get', 'nodes', '-o', 'name').splitlines()
        example = nodes[0].removeprefix('node/') if nodes else '<node-name>'
        problems.append(f'no node labeled videoflow.io/gpu-pool=true — GPU pods will stay '
                        f'Pending. Fix: kubectl label node {example} videoflow.io/gpu-pool=true')
    allocatable = _kubectl_out(kubectl, 'get', 'nodes', '-o',
                               'jsonpath={.items[*].status.allocatable.nvidia\\.com/gpu}')
    if not allocatable.strip():
        problems.append('no node advertises nvidia.com/gpu — install the NVIDIA device plugin: '
                        'kubectl apply -f https://raw.githubusercontent.com/NVIDIA/k8s-device-plugin/'
                        'v0.16.2/deployments/static/nvidia-device-plugin.yml')
    return problems
