'''
Shared setup for the NATS-backed integration tests.

These need a reachable NATS JetStream server (start one with `nats-server -js` or
`docker compose up -d nats`); they are skipped automatically when none is up.

The reachability probe deliberately uses a plain TCP connect rather than
``nats.connect``: nats-py retries a refused connection internally, so a full
handshake against a dead port costs ~2 minutes regardless of ``connect_timeout``.
Each module used to run that probe at import time, which made merely *collecting*
this suite take ~14 minutes with no server running. A socket check settles it in
well under a second, and the result is cached so the cost is paid once per run.
'''
import functools
import os
import socket
from urllib.parse import urlparse

import pytest

NATS_URL = os.environ.get('VF_TEST_NATS_URL', 'nats://localhost:4222')

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
    '''Mark everything in this directory as integration; skip it when NATS is down.'''
    skip = pytest.mark.skip(reason = f'NATS not reachable at {NATS_URL}')
    available = nats_available()
    for item in items:
        item.add_marker(pytest.mark.integration)
        if not available:
            item.add_marker(skip)
