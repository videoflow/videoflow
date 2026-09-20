'''
Image auto-build for ``videoflow deploy`` and ``run-local``: when no ``--image``
is given, find the Dockerfile next to the graph module (``gpu.Dockerfile`` when
the flow needs a GPU — decided by the template's ``x-gpu`` block or the graph's
device placement, see ``resolve_needs_gpu``), build the ``videoflow-base`` image
it is FROM if missing, and build the solution image from the enclosing git root
(solution Dockerfiles COPY sibling packages, so the repo root is the context).

The base images can only be auto-built from a videoflow *source checkout*
(``docker/base/Dockerfile`` COPYs the source tree); a wheel-only install gets a
precise error with the manual commands instead.

Two environment variables reach every docker invocation made here (a machine's
concern, never a deploy flag): ``VF_DOCKER_BUILD_ARGS`` is spliced into each
``docker build`` and ``VF_DOCKER_RUN_ARGS`` into each ``docker run`` — the way
to pass a corporate proxy as ``--build-arg http_proxy=...``, say.
'''
from __future__ import absolute_import, division, print_function

import os
import re
import shlex
import subprocess
import sys
from typing import TYPE_CHECKING, List, Mapping, Optional

import videoflow

if TYPE_CHECKING:
    # Type-only: .manifests imports yaml at module scope and this module must stay
    # importable without the optional deploy extras.
    from .manifests import Mount

_BASE_ARG_RE = re.compile(r'^ARG\s+BASE_IMAGE\s*=\s*(\S+)\s*$', re.MULTILINE)

BUILD_ARGS_ENV = 'VF_DOCKER_BUILD_ARGS'
RUN_ARGS_ENV = 'VF_DOCKER_RUN_ARGS'

def docker_build_extra_args() -> List[str]:
    '''Extra ``docker build`` arguments from ``VF_DOCKER_BUILD_ARGS`` (shell-split; empty when unset).'''
    return shlex.split(os.environ.get(BUILD_ARGS_ENV, ''))

def docker_run_extra_args() -> List[str]:
    '''Extra ``docker run`` arguments from ``VF_DOCKER_RUN_ARGS`` (shell-split; empty when unset).'''
    return shlex.split(os.environ.get(RUN_ARGS_ENV, ''))

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

def resolve_needs_gpu(graph_dir : str, declared : Optional[bool],
                      specs : Optional[list]) -> tuple[bool, Optional[str]]:
    '''
    Whether the solution's GPU image is the one to build, decided from the flow
    rather than from the docker daemon (a daemon with the nvidia runtime says
    nothing about where the flow's nodes run, and one without it must still build
    a CUDA image for a cluster that has GPUs). In order:

    1. the template's ``x-gpu`` declaration (``solution.resolve_gpu``), when present;
    2. the only Dockerfile the solution ships, when it ships just one;
    3. the compiled graph's device placement, when the graph could be compiled here;
    4. the CPU image, with a note recommending ``x-gpu``.

    - Returns:
        - ``(needs_gpu, note)`` — ``note`` is a message for the operator in case 4, else None.
    '''
    if declared is not None:
        return bool(declared), None
    has_cpu = os.path.isfile(os.path.join(graph_dir, 'Dockerfile'))
    has_gpu = os.path.isfile(os.path.join(graph_dir, 'gpu.Dockerfile'))
    if has_cpu != has_gpu:
        return has_gpu, None
    if not has_cpu:
        return False, None                      # no Dockerfile at all: nothing to choose
    if specs is not None:
        return any(s.device_type == 'gpu' for s in specs), None
    return False, ('both Dockerfile and gpu.Dockerfile exist, config.template.yaml declares no x-gpu, and '
                   'the graph cannot be compiled on this host to read its device placement — building the '
                   "CPU image. Add `x-gpu: ['{device}']` (the config keys that select gpu) to the template, "
                   'or pass --image.')

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
    # ``docker build [VF_DOCKER_BUILD_ARGS...] -f ...`` — the machine's build flags
    # (a proxy, a builder) ride on every build, base and solution alike.
    cmd = cmd[:2] + docker_build_extra_args() + cmd[2:]
    try:
        proc = subprocess.run(cmd, check = False)
    except FileNotFoundError as e:
        raise RuntimeError('docker not found on PATH — install docker or pass a '
                           'prebuilt image with --image.') from e
    if proc.returncode != 0:
        raise RuntimeError(f'docker build failed: {" ".join(cmd)}')

def default_tag(graph_dir : str) -> str:
    '''Deterministic human-readable tag for the auto-built solution image, e.g. ``videoflow-human_tracking:latest``.'''
    return f'videoflow-{os.path.basename(os.path.abspath(graph_dir))}:latest'

def image_id(ref : str) -> Optional[str]:
    '''The local image's content id (``sha256:...``), or None when docker cannot read it.'''
    proc = subprocess.run(['docker', 'image', 'inspect', '--format', '{{.Id}}', ref],
                          capture_output = True, text = True, check = False)
    out = proc.stdout.strip() if proc.returncode == 0 else ''
    return out or None

def _repository(ref : str) -> str:
    '''``ref`` without its tag (a ``:`` after the last ``/`` is a tag, not a port).'''
    name = ref.rsplit('/', 1)[-1]
    return ref[:-(len(name) - name.index(':'))] if ':' in name else ref

def content_tag(ref : str) -> str:
    '''
    ``<repository>:<first 12 hex of the image id>`` — a tag that changes exactly
    when the image does, tagged onto the local image beside ``ref``. It is what a
    deploy uses: ``imagePullPolicy: IfNotPresent`` is then always right, on a
    registry as much as for a side-loaded image (a node that cached last week's
    ``:latest`` never runs it by mistake), while ``ref`` keeps docker's layer cache
    warm for the next build. Falls back to ``ref`` (with a warning) when the id
    cannot be read or the tag cannot be applied.
    '''
    ident = image_id(ref)
    if not ident:
        print(f'WARNING: could not read the image id of {ref}; deploying it under that mutable tag.',
              file = sys.stderr)
        return ref
    tagged = f'{_repository(ref)}:{ident.split(":", 1)[-1][:12]}'
    proc = subprocess.run(['docker', 'tag', ref, tagged], capture_output = True, check = False)
    if proc.returncode != 0:
        print(f'WARNING: could not tag {ref} as {tagged}; deploying it under that mutable tag.',
              file = sys.stderr)
        return ref
    return tagged

def registry_ref(registry : str, local_ref : str) -> str:
    '''``local_ref`` under ``registry`` (``host[:port][/prefix]``), e.g. ``10.0.0.1:5000/videoflow-x:abc``.'''
    return f'{registry.rstrip("/")}/{local_ref}'

#: The push tools ``push_image`` knows: docker's own push, or crane from user space.
PUSH_TOOLS = ('docker', 'crane')

CRANE_INSTALL_HINT = '''crane is not on PATH. Install it into ~/.local/bin — user space, no sudo, no
change to docker (release asset name checked against go-containerregistry v0.22.1;
pin a version by replacing 'latest/download' with 'download/vX.Y.Z'):

  mkdir -p ~/.local/bin
  curl -sSL https://github.com/google/go-containerregistry/releases/latest/download/go-containerregistry_Linux_x86_64.tar.gz \\
    | tar -xz -C ~/.local/bin crane
  export PATH="$HOME/.local/bin:$PATH"
  crane version'''

def _run_tool(cmd : List[str], capture : bool = False, missing_hint : Optional[str] = None) -> None:
    '''Runs a push step, streaming its output to stderr; a missing binary or a non-zero exit is a RuntimeError naming the fix.'''
    try:
        if capture:
            proc = subprocess.run(cmd, capture_output = True, text = True, check = False)
        else:
            proc = subprocess.run(cmd, stdout = sys.stderr, text = True, check = False)
    except FileNotFoundError as e:
        raise RuntimeError(missing_hint or f'{cmd[0]!r} not found on PATH.') from e
    if proc.returncode != 0:
        detail = f':\n{proc.stderr}' if capture and proc.stderr else ''
        raise RuntimeError(f'command failed: {" ".join(cmd)}{detail}')

def _image_tmpdir() -> str:
    '''Where crane's tarballs go: ``VF_IMAGE_TMPDIR``, else the user cache (images are gigabytes; never ``/tmp`` on a small root).'''
    return os.environ.get('VF_IMAGE_TMPDIR') or os.path.join(
        os.environ.get('XDG_CACHE_HOME') or os.path.expanduser('~/.cache'), 'videoflow', 'images')

def push_image(local_ref : str, registry : str, tool : str = 'docker') -> str:
    '''
    Pushes a locally built image to ``registry`` and returns the registry-qualified
    ref the pods pull — the multi-node answer to side-loading, which reaches one
    node only.

    - ``docker``: ``docker tag`` + ``docker push`` (the daemon must trust the registry).
    - ``crane``: ``docker save`` to a tarball, ``crane push --insecure`` (a plain-HTTP \
        registry the daemon does not trust — nothing about the daemon changes, no \
        sudo), a ``crane manifest`` read-back, then the same local ``docker tag`` so \
        the prepare hook's ``docker run <ref>`` finds the image without pulling.

    - Raises:
        - ``RuntimeError`` naming the failed step or the missing tool (with the install recipe for crane).
        - ``ValueError`` for an unknown tool.
    '''
    if tool not in PUSH_TOOLS:
        raise ValueError(f'unknown push tool {tool!r}; use one of: {", ".join(PUSH_TOOLS)}.')
    ref = registry_ref(registry, local_ref)
    print(f'Pushing {local_ref} -> {ref} ({tool})...', file = sys.stderr)
    if tool == 'docker':
        _run_tool(['docker', 'tag', local_ref, ref])
        _run_tool(['docker', 'push', ref])
        return ref
    tmpdir = _image_tmpdir()
    os.makedirs(tmpdir, exist_ok = True)
    tarball = os.path.join(tmpdir, re.sub(r'[:/]', '-', local_ref) + '.tar')
    try:
        _run_tool(['docker', 'save', local_ref, '-o', tarball])
        _run_tool(['crane', 'push', '--insecure', tarball, ref], missing_hint = CRANE_INSTALL_HINT)
        _run_tool(['crane', 'manifest', '--insecure', ref], capture = True)   # pushed is not the same as present
    finally:
        if os.path.exists(tarball):
            os.remove(tarball)
    _run_tool(['docker', 'tag', local_ref, ref])
    return ref

def docker_gpus_available() -> bool:
    '''Whether the local docker daemon has the NVIDIA runtime (for --gpus all).'''
    proc = subprocess.run(['docker', 'info', '--format', '{{json .Runtimes}}'],
                          capture_output = True, text = True, check = False)
    return proc.returncode == 0 and 'nvidia' in proc.stdout

def run_in_image(image : str, command : List[str], mounts : Optional[List['Mount']] = None,
                 workdir : Optional[str] = None, gpus : bool = False,
                 capture : bool = False, interactive : bool = False,
                 env : Optional[Mapping[str, str]] = None) -> Optional[str]:
    '''
    Runs a command in the solution image with the given hostPath-style mounts
    (``Mount`` records from ``manifests.parse_mounts``) — how deploy executes the
    prepare hook and the graph compile without the graph's deps on the host.

    Claim mounts (``manifests.parse_pvc_mounts``, ``Mount.claim`` set) are skipped:
    a PersistentVolumeClaim exists only inside the cluster, and this container runs
    on the operator's host, where the same directory is reached by the hostPath
    the claim shadows in the pods (see ``manifests.pod_mounts``).

    - Arguments:
        - env: environment variables set in the container (``-e`` pairs), e.g. the \
            resolved config path for ``build_flow``.

    - Returns:
        - the command's stdout when ``capture``, else None.

    - Raises:
        - ``RuntimeError`` on a non-zero exit (with stderr when captured).
    '''
    cmd = ['docker', 'run', '--rm', *docker_run_extra_args()]
    if interactive and sys.stdin.isatty():
        cmd.append('-i')
    if gpus:
        cmd += ['--gpus', 'all']
    for key, value in (env or {}).items():
        cmd += ['-e', f'{key}={value}']
    for m in mounts or []:
        if m.claim is not None:
            continue
        suffix = ':ro' if m.read_only else ''
        cmd += ['-v', f'{m.host_path}:{m.container_path}{suffix}']
    if workdir:
        cmd += ['-w', workdir]
    # The worker entrypoint is baked into videoflow-base images; override it to
    # run an arbitrary command.
    cmd += ['--entrypoint', command[0], image, *command[1:]]
    try:
        if capture:
            proc = subprocess.run(cmd, capture_output = True, text = True, check = False)
        else:
            # Uncaptured child output goes to stderr, not stdout: deploy's stdout is
            # machine-readable (--dry-run streams the rendered manifests there), so a
            # prepare hook printing progress must not interleave with it.
            proc = subprocess.run(cmd, stdout = sys.stderr, text = True, check = False)
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
    the solution image. Returns the image's content-addressed ref
    (``content_tag``), or None when the solution ships no Dockerfile (caller falls
    back to explicit-image resolution).
    '''
    dockerfile = find_dockerfile(graph_dir, needs_gpu)
    if dockerfile is None:
        return None
    base = base_image_for(dockerfile)
    if base and base.startswith('videoflow-base:'):
        ensure_base_image(base)
    tag = default_tag(graph_dir)
    build_image(dockerfile, build_context_for(graph_dir, context_override), tag)
    deployed = content_tag(tag)
    if deployed != tag:
        print(f'Built {tag}; deploying it as {deployed}.', file = sys.stderr)
    return deployed
