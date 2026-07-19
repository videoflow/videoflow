'''
Image auto-build for ``videoflow deploy``: when no ``--image`` is given, find the
Dockerfile next to the graph module (``gpu.Dockerfile`` for a flow with GPU
nodes), build the ``videoflow-base`` image it is FROM if missing, and build the
solution image from the enclosing git root (solution Dockerfiles COPY sibling
packages, so the repo root is the context).

The base images can only be auto-built from a videoflow *source checkout*
(``docker/base/Dockerfile`` COPYs the source tree); a wheel-only install gets a
precise error with the manual commands instead.
'''
from __future__ import absolute_import, division, print_function

import os
import re
import subprocess
import sys
from typing import List, Optional

import videoflow

_BASE_ARG_RE = re.compile(r'^ARG\s+BASE_IMAGE\s*=\s*(\S+)\s*$', re.MULTILINE)

def find_dockerfile(graph_dir : str, needs_gpu : bool) -> Optional[str]:
    '''
    The Dockerfile deploy builds the node image from: ``gpu.Dockerfile`` when the
    flow has GPU nodes and one exists, else ``Dockerfile``. None when the solution
    ships neither (the caller falls back to requiring ``--image``).
    '''
    gpu = os.path.join(graph_dir, 'gpu.Dockerfile')
    plain = os.path.join(graph_dir, 'Dockerfile')
    for candidate in ([gpu, plain] if needs_gpu else [plain, gpu]):
        if os.path.isfile(candidate):
            return candidate
    return None

def build_context_for(graph_dir : str, override : Optional[str] = None) -> str:
    '''
    The docker build context: an explicit ``--build-context``, else the git root
    enclosing the graph (solution Dockerfiles COPY sibling packages from the repo
    root — e.g. the offside solution copies nine of them), else the graph dir.
    '''
    if override:
        return os.path.abspath(override)
    proc = subprocess.run(['git', '-C', graph_dir, 'rev-parse', '--show-toplevel'],
                          capture_output = True, text = True, check = False)
    if proc.returncode == 0 and proc.stdout.strip():
        return proc.stdout.strip()
    return graph_dir

def base_image_for(dockerfile_path : str) -> Optional[str]:
    '''The default of the Dockerfile's ``ARG BASE_IMAGE=`` line, or None if it has none.'''
    with open(dockerfile_path) as f:
        match = _BASE_ARG_RE.search(f.read())
    return match.group(1) if match else None

def image_exists(ref : str) -> bool:
    proc = subprocess.run(['docker', 'image', 'inspect', ref],
                          capture_output = True, check = False)
    return proc.returncode == 0

def ensure_base_image(base_ref : str) -> None:
    '''
    Makes sure ``base_ref`` (a ``videoflow-base:*`` image) exists locally,
    building it from the videoflow source checkout if missing.

    - Raises:
        - ``RuntimeError`` when the image is missing and videoflow is not an \
            editable/source install (the base Dockerfile COPYs the source tree, \
            so there is nothing to build from).
    '''
    if image_exists(base_ref):
        return
    source_root = os.path.dirname(os.path.dirname(os.path.abspath(videoflow.__file__)))
    gpu = base_ref.endswith('-cuda')
    dockerfile = os.path.join(source_root, 'docker', 'base',
                              'Dockerfile.gpu' if gpu else 'Dockerfile')
    if not os.path.isfile(dockerfile):
        raise RuntimeError(
            f'base image {base_ref} is not built and videoflow is not installed from '
            f'source, so it cannot be built automatically. Build it once with:\n'
            f'  git clone https://github.com/videoflow/videoflow && cd videoflow\n'
            f'  ./docker/build-images.sh')
    print(f'Building base image {base_ref} (one-time)...')
    cmd = ['docker', 'build', '-f', dockerfile, '-t', base_ref]
    if not gpu:
        cmd += ['--build-arg', 'PYTHON_VERSION=3.12']
    cmd.append(source_root)
    _docker_build(cmd)

def build_image(dockerfile : str, context : str, tag : str) -> None:
    '''Builds the solution image, streaming docker output (layer cache makes unchanged rebuilds fast).'''
    print(f'Building {tag} from {dockerfile}...')
    _docker_build(['docker', 'build', '-f', dockerfile, '-t', tag, context])

def _docker_build(cmd : List[str]) -> None:
    try:
        proc = subprocess.run(cmd, check = False)
    except FileNotFoundError as e:
        raise RuntimeError('docker not found on PATH — install docker or pass a '
                           'prebuilt image with --image.') from e
    if proc.returncode != 0:
        raise RuntimeError(f'docker build failed: {" ".join(cmd)}')

def default_tag(graph_dir : str) -> str:
    '''Deterministic human-readable tag for the auto-built solution image, e.g. ``videoflow-offside:latest``.'''
    return f'videoflow-{os.path.basename(os.path.abspath(graph_dir))}:latest'

def docker_gpus_available() -> bool:
    '''Whether the local docker daemon has the NVIDIA runtime (for --gpus all).'''
    proc = subprocess.run(['docker', 'info', '--format', '{{json .Runtimes}}'],
                          capture_output = True, text = True, check = False)
    return proc.returncode == 0 and 'nvidia' in proc.stdout

def run_in_image(image : str, command : List[str], mounts : Optional[List[dict]] = None,
                 workdir : Optional[str] = None, gpus : bool = False,
                 capture : bool = False, interactive : bool = False) -> Optional[str]:
    '''
    Runs a command in the solution image with the given hostPath-style mounts
    (dicts from ``manifests.parse_mounts``) — how deploy executes the prepare
    hook and the graph compile without the graph's deps on the host.

    - Returns:
        - the command's stdout when ``capture``, else None.

    - Raises:
        - ``RuntimeError`` on a non-zero exit (with stderr when captured).
    '''
    cmd = ['docker', 'run', '--rm']
    if interactive and sys.stdin.isatty():
        cmd.append('-i')
    if gpus:
        cmd += ['--gpus', 'all']
    for m in mounts or []:
        suffix = ':ro' if m['read_only'] else ''
        cmd += ['-v', f"{m['host_path']}:{m['container_path']}{suffix}"]
    if workdir:
        cmd += ['-w', workdir]
    # The worker entrypoint is baked into videoflow-base images; override it to
    # run an arbitrary command.
    cmd += ['--entrypoint', command[0], image, *command[1:]]
    try:
        proc = subprocess.run(cmd, capture_output = capture, text = True, check = False)
    except FileNotFoundError as e:
        raise RuntimeError('docker not found on PATH.') from e
    if proc.returncode != 0:
        detail = f':\n{proc.stderr}' if capture else ''
        raise RuntimeError(f'command failed in {image}: {" ".join(command)}{detail}')
    return proc.stdout if capture else None

def autobuild(graph_dir : str, needs_gpu : bool,
              context_override : Optional[str] = None) -> Optional[str]:
    '''
    The whole auto-build path: find the Dockerfile, ensure its base image, build
    the solution image. Returns the built tag, or None when the solution ships no
    Dockerfile (caller falls back to explicit-image resolution).
    '''
    dockerfile = find_dockerfile(graph_dir, needs_gpu)
    if dockerfile is None:
        return None
    base = base_image_for(dockerfile)
    if base and base.startswith('videoflow-base:'):
        ensure_base_image(base)
    tag = default_tag(graph_dir)
    build_image(dockerfile, build_context_for(graph_dir, context_override), tag)
    return tag
