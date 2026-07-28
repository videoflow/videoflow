'''
Shared setup for the integration buckets.

``broker/`` and ``local/`` need a reachable NATS JetStream server (start one with
``docker compose up -d`` or ``nats-server -js``); they are skipped automatically
when none is up. ``k8s/`` talks to the broker running *inside* a kind cluster, so
it is exempt from that gate and brings its own in ``k8s/conftest.py`` — a
developer with no local nats-server must still be able to run it, and someone
with no cluster must not have the other two silently disappear.

The reachability probe deliberately uses a plain TCP connect rather than
``nats.connect``: nats-py retries a refused connection internally, so a full
handshake against a dead port costs ~2 minutes regardless of ``connect_timeout``.
Each module used to run that probe at import time, which made merely *collecting*
this suite take ~14 minutes with no server running. A socket check settles it in
well under a second, and the result is cached so the cost is paid once per run.
'''
import functools
import os
import pathlib
import socket
import sys
from urllib.parse import urlparse

import pytest

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')

HERE = pathlib.Path(__file__).parent
K8S_DIR = HERE / 'k8s'

# The support_* modules live here, next to this conftest, while the tests that
# import them live one directory deeper. pytest prepends a test module's own
# directory to sys.path, not its ancestors', so make the seam explicit rather
# than relying on the insertion pytest happens to do for this conftest itself.
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

@functools.lru_cache(maxsize = 1)
def nats_available(url = NATS_URL) -> bool:
    '''True when something is listening on the NATS host/port.'''
    parsed = urlparse(url)
    try:
        with socket.create_connection((parsed.hostname or 'localhost', parsed.port or 4222),
                                      timeout = 1):
            return True
    except OSError:
        return False

def pytest_collection_modifyitems(config, items):
    '''
    Mark everything in this directory as integration; skip what needs the local
    broker when it is down.

    pytest hands this hook every collected item in the session, not just the ones
    under the conftest that defines it — so the directory check is what keeps a
    down broker from skipping the whole (unit) suite and passing vacuously.
    '''
    skip = pytest.mark.skip(reason = f'NATS not reachable at {NATS_URL}')
    available = nats_available()
    for item in items:
        path = pathlib.Path(str(item.fspath))
        if HERE not in path.parents:
            continue
        item.add_marker(pytest.mark.integration)
        if K8S_DIR in path.parents:
            # Gated by k8s/conftest.py against the cluster instead: its broker is
            # the one inside kind, reached through VF_K8S_NATS_URL.
            item.add_marker(pytest.mark.k8s)
            continue
        if not available:
            item.add_marker(skip)
