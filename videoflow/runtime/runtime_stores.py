'''
Runtime-store selection by URL scheme (RFC 0006 ``ENV-10``, ``VF_RUNTIME_STORE_URL``).

Three schemes are built in — ``memory://`` (per-process, never durable: tests and
the pre-RFC default), ``file://<dir>`` (one host, durable across restarts: the
local engine's default under the switch) and ``redis://`` / ``rediss://``
(cross-host; durable only when the server's persistence is read back on). The
registry follows the blob-store one: a module-level table seeded with the
built-ins, an explicit ``register_runtime_store``, and a lookup that names the
known schemes and the fix. What each store *is* — durable or not, shared or
not — is the store's own ``capabilities()``, which the planner reads; nothing
here promises more than the store reports.
'''
from __future__ import absolute_import, division, print_function

from typing import Callable, Dict
from urllib.parse import urlparse

from ..backends.memory.runtime_store import FileRuntimeStore, MemoryRuntimeStore
from ..backends.runtime import RuntimeStore
from .redis_runtime_store import runtime_store_from_url

#: The default when ``VF_RUNTIME_STORE_URL`` is unset: no durable state, nothing shared.
DEFAULT_RUNTIME_STORE_URL = 'memory://'


def _memory(url : str) -> RuntimeStore:
    return MemoryRuntimeStore()


def _file(url : str) -> RuntimeStore:
    parsed = urlparse(url)
    root = (parsed.netloc + parsed.path) if parsed.netloc and parsed.netloc != 'localhost' else parsed.path
    if not root:
        raise ValueError(f'file:// runtime store URL {url!r} names no directory. Use file:///path/to/ledger.')
    return FileRuntimeStore(root)


_RUNTIME_STORES : Dict[str, Callable[[str], RuntimeStore]] = {
    'memory': _memory,
    'file': _file,
    'redis': runtime_store_from_url,
    'rediss': runtime_store_from_url,
}


def register_runtime_store(scheme : str, factory : Callable[[str], RuntimeStore]) -> None:
    '''Register a ``RuntimeStore`` factory for a URL scheme (case-insensitive); ``factory`` receives the full URL.'''
    _RUNTIME_STORES[scheme.lower()] = factory


def known_runtime_store_schemes() -> list[str]:
    return sorted(_RUNTIME_STORES)


def make_runtime_store(url : str | None) -> RuntimeStore:
    '''
    - Arguments:
        - url: ``VF_RUNTIME_STORE_URL``; ``None`` or empty means ``memory://``.

    - Raises:
        - ValueError: an unknown scheme, naming the known ones and ``register_runtime_store``.
    '''
    url = url or DEFAULT_RUNTIME_STORE_URL
    scheme = urlparse(url).scheme.lower()
    factory = _RUNTIME_STORES.get(scheme)
    if factory is None:
        raise ValueError(f'No runtime store is registered for scheme {scheme!r} (URL {url!r}). Known schemes: '
                         f'{", ".join(known_runtime_store_schemes())}. Register one with '
                         'videoflow.runtime.runtime_stores.register_runtime_store.')
    return factory(url)
