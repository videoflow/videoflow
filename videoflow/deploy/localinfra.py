'''
Auto-provisioning of the dev broker ``videoflow run-local`` needs when the user
doesn't bring their own: a NATS JetStream container and, for the large-payload
blob store, a Redis container, both published on localhost. The local analogue of
``videoflow.infra``, which does the same job in-cluster for ``videoflow deploy``.

Ownership rule, identical to ``infra``: if something is already listening on the
port — ``docker compose up -d``, a bare ``nats-server -js``, a previous
``--keep-infra`` run — it is reused as-is and **never** torn down. Only containers
this module started are returned as "created", and only those are stopped later.

Dev-grade only: no persistence, no auth, host-published ports. Point ``--nats`` /
``--blob-redis-url`` at a real broker for anything else.
'''
from __future__ import absolute_import, division, print_function

import socket
import subprocess
import time
from typing import List, Optional, Tuple
from urllib.parse import urlparse

from .infra import LABEL_INFRA

NATS_CONTAINER = 'videoflow-nats'
REDIS_CONTAINER = 'videoflow-redis'
NATS_IMAGE = 'nats:2.10'
REDIS_IMAGE = 'redis:7-alpine'
DEFAULT_NATS_URL = 'nats://localhost:4222'
DEFAULT_REDIS_URL = 'redis://localhost:6379/0'

CONTAINERS = {'nats': NATS_CONTAINER, 'redis': REDIS_CONTAINER}

def local_infra_urls() -> dict:
    '''The localhost URLs workers use once the dev containers are up.'''
    return {'nats': DEFAULT_NATS_URL, 'redis': DEFAULT_REDIS_URL}

def _host_port(url : str, default_port : int) -> tuple:
    parsed = urlparse(url)
    return (parsed.hostname or 'localhost', parsed.port or default_port)

def port_open(host : str, port : int, timeout : float = 1.0) -> bool:
    '''True when something accepts a TCP connection — the "is it already up?" probe.'''
    try:
        with socket.create_connection((host, port), timeout = timeout):
            return True
    except OSError:
        return False

def nats_running(url : str = DEFAULT_NATS_URL) -> bool:
    return port_open(*_host_port(url, 4222))

def redis_running(url : str = DEFAULT_REDIS_URL) -> bool:
    return port_open(*_host_port(url, 6379))

def docker_available() -> bool:
    '''True when a docker daemon is reachable.'''
    try:
        proc = subprocess.run(['docker', 'info'], capture_output = True, check = False)
    except FileNotFoundError:
        return False
    return proc.returncode == 0

def _run_docker(cmd : List[str], what : str) -> None:
    try:
        proc = subprocess.run(cmd, capture_output = True, check = False)
    except FileNotFoundError as e:
        raise RuntimeError('docker not found on PATH.') from e
    if proc.returncode != 0:
        raise RuntimeError(f'could not {what}: {proc.stderr.decode("utf-8", "replace").strip()}')

def _remove_container(name : str) -> None:
    '''Best-effort removal of a stale same-named container (never touches a live port).'''
    subprocess.run(['docker', 'rm', '-f', name], capture_output = True, check = False)

def _docker_run_argv(component : str) -> list:
    name = CONTAINERS[component]
    argv = ['docker', 'run', '-d', '--rm', '--name', name,
            '--label', f'{LABEL_INFRA}={component}']
    if component == 'nats':
        # Same server the repo's docker-compose.yml starts, so reusing a
        # compose-managed broker and starting our own are equivalent.
        argv += ['-p', '4222:4222', '-p', '8222:8222', NATS_IMAGE, '-js', '-m', '8222']
    else:
        # Persistence off: the blob store is transport, not storage.
        argv += ['-p', '6379:6379', REDIS_IMAGE, '--save', '', '--appendonly', 'no']
    return argv

def _start(component : str) -> None:
    _remove_container(CONTAINERS[component])
    _run_docker(_docker_run_argv(component), f'start dev {component}')

def ensure_local_infra(need_redis : bool = True, nats_url : Optional[str] = None,
                       redis_url : Optional[str] = None) -> Tuple[dict, List[str]]:
    '''
    Starts a dev NATS (and, when ``need_redis``, Redis) container unless something
    is already listening on the port.

    - Returns:
        - ``(urls, created)`` where ``urls`` maps ``nats``/``redis`` to localhost \
            URLs (``redis`` is None when not needed) and ``created`` lists only the \
            components THIS call started — the only ones teardown may stop.

    - Raises:
        - ``RuntimeError`` when a container must be started but docker is \
            unavailable, or when ``docker run`` fails.
    '''
    urls = local_infra_urls()
    urls['nats'] = nats_url or urls['nats']
    urls['redis'] = (redis_url or urls['redis']) if need_redis else None

    wanted = []
    if not nats_running(urls['nats']):
        wanted.append('nats')
    if need_redis and not redis_running(urls['redis']):
        wanted.append('redis')
    if wanted and not docker_available():
        missing = ' and '.join(wanted)
        raise RuntimeError(
            f'nothing is listening for {missing} on localhost and docker is not available, '
            f'so a dev broker cannot be started. Either start one yourself '
            f'(`nats-server -js`, or `docker compose up -d` from a videoflow checkout) '
            f'or point --nats at a running broker.')
    for component in wanted:
        _start(component)
    return urls, wanted

def wait_local_infra_ready(created : Optional[List[str]], urls : dict,
                           timeout_secs : int = 45) -> None:
    '''
    Polls each freshly started component's port until it accepts connections.

    - Raises:
        - ``RuntimeError`` (after dumping the container's logs) on timeout.
    '''
    probes = {'nats': (urls.get('nats') or DEFAULT_NATS_URL, 4222),
              'redis': (urls.get('redis') or DEFAULT_REDIS_URL, 6379)}
    for component in created or []:
        url, default_port = probes[component]
        host, port = _host_port(url, default_port)
        deadline = time.time() + timeout_secs
        while time.time() < deadline:
            if port_open(host, port, timeout = 0.5):
                break
            time.sleep(0.25)
        else:
            subprocess.run(['docker', 'logs', '--tail', '40', CONTAINERS[component]], check = False)
            raise RuntimeError(f'dev {component} did not become ready within {timeout_secs}s.')

def teardown_local_infra(components : Optional[List[str]]) -> None:
    '''Removes the given auto-started containers. Best-effort; never raises.'''
    for component in components or []:
        _remove_container(CONTAINERS[component])
