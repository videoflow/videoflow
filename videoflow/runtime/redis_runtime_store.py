'''
The Redis ``RuntimeStore``: the run ledger for workers spread across hosts
(RFC 0006 ``CTRL-4``, ``ENV-10``). One Redis hash per key holds the value and its
version; every mutation is an optimistic ``WATCH`` / ``MULTI`` / ``EXEC``
transaction on that one key, so a compare-and-swap is exactly one round of the
same primitive the payload store uses — no Lua, no cross-slot commands, and a
key layout a Redis Cluster can host (``vfrt:{...}`` keys hash individually; a
ledger never needs two keys in one transaction).

Durability is *read back*, never declared: the store is ``durable = Known(True)``
only when ``CONFIG GET`` shows persistence on (``appendonly yes`` or an RDB
``save`` schedule) and ``maxmemory-policy noeviction``, ``Known(False)`` when
persistence is off, and ``Unknown('auth')`` when ``CONFIG`` is denied (a managed
Redis). The planner admits ``restart_safe`` and ``durable_control`` only on
``Known(True)``; an unobservable ledger is not a durable one.

redis-py 8.0.1 facts relied on (``.venv/lib/python3.12/site-packages/redis/``):
``Pipeline.watch`` executes commands immediately until ``multi()``
(``client.py:1763``); a ``nil`` ``EXEC`` raises ``WatchError`` (``client.py:1879``);
``execute()`` always resets the pipeline (``client.py:2039``); ``hset(mapping=)``
(``commands/core.py:9702``); ``rpush`` returns the new length; ``scan_iter`` pages
with ``SCAN`` (never ``KEYS``).
'''
from __future__ import absolute_import, division, print_function

import logging
import random
import time
from typing import Any, Optional

from ..backends.capabilities import RuntimeCapabilities
from ..backends.outcomes import Unknown, known
from ..backends.runtime import RuntimeStore
from ..core.errors import TransientFailure
from ..wire.redis_payload_store import redis_capabilities_observed

logger = logging.getLogger(__package__)

#: Bounded ``WATCH`` retries on a conflict that is not the caller's (a lost reply,
#: a concurrent touch of an unrelated field): a genuine version mismatch is
#: reported after the first round, never retried.
TRANSACTION_RETRIES = 8
KEY_PREFIX = 'vfrt:'


def _is_transport(error : BaseException) -> bool:
    name = type(error).__name__
    return any(marker in name for marker in ('ConnectionError', 'TimeoutError', 'BusyLoading'))


class RedisRuntimeStore(RuntimeStore):
    '''
    - Arguments:
        - url: ``redis://`` / ``rediss://`` URL; ignored when ``client`` is given.
        - client: a ready ``redis.Redis`` (tests pass a fake with ``pipeline``, \
            ``hgetall``, ``rpush``, ``llen``, ``scan_iter``, ``config_get``, ``info``).
        - prefix: what every key is prefixed with on the server, so a ledger never \
            collides with the payload store sharing the instance.
    '''
    def __init__(self, url : str | None = None, client : Any = None, prefix : str = KEY_PREFIX) -> None:
        if client is None:
            if not url:
                raise ValueError('RedisRuntimeStore needs a url or a client')
            # Deferred: `redis` is an optional dependency (the `blob` extra).
            import redis
            client = redis.Redis.from_url(url)
        self._client = client
        self._prefix = prefix

    def _key(self, key : str) -> str:
        return self._prefix + key

    def _log_key(self, log : str) -> str:
        return self._prefix + 'log:' + log

    @staticmethod
    def _decode(fields : dict) -> tuple[bytes | None, str | None]:
        if not fields:
            return None, None
        value = fields.get(b'value', fields.get('value'))
        version = fields.get(b'version', fields.get('version'))
        if value is None or version is None:
            return None, None
        return (value if isinstance(value, bytes) else str(value).encode()), str(_text(version))

    def get(self, key : str) -> tuple[bytes | None, str | None]:
        try:
            return self._decode(self._client.hgetall(self._key(key)))
        except Exception as e:  # noqa: BLE001 — every store failure is the same transient case
            if _is_transport(e):
                raise TransientFailure(f'runtime store unreachable reading {key!r}: {e}',
                                       remedy = 'Restore the Redis behind VF_RUNTIME_STORE_URL.') from e
            raise

    def cas(self, key : str, expected_version : str | None, value : bytes) -> bool:
        server_key = self._key(key)
        for attempt in range(TRANSACTION_RETRIES):
            try:
                with self._client.pipeline() as pipe:
                    pipe.watch(server_key)
                    _current, version = self._decode(pipe.hgetall(server_key))
                    if version != expected_version:
                        pipe.unwatch()
                        return False
                    next_version = 1 if version is None else int(version) + 1
                    pipe.multi()
                    pipe.hset(server_key, mapping = {'value': value, 'version': str(next_version)})
                    pipe.execute()
                    return True
            except Exception as e:  # noqa: BLE001 — a WatchError or a transport error; the rest re-raise
                if type(e).__name__ == 'WatchError':
                    time.sleep(random.uniform(0.0, 0.002) * (attempt + 1))
                    continue
                if _is_transport(e):
                    raise TransientFailure(f'runtime store unreachable writing {key!r}: {e}',
                                           remedy = 'Restore the Redis behind VF_RUNTIME_STORE_URL.') from e
                raise
        # Every round saw the key change under us: the caller re-reads and retries at its level.
        return False

    def append(self, log : str, record : bytes) -> int:
        try:
            return int(self._client.rpush(self._log_key(log), record))
        except Exception as e:  # noqa: BLE001
            if _is_transport(e):
                raise TransientFailure(f'runtime store unreachable appending to {log!r}: {e}',
                                       remedy = 'Restore the Redis behind VF_RUNTIME_STORE_URL.') from e
            raise

    def log_entries(self, log : str) -> list[bytes]:
        return [v if isinstance(v, bytes) else str(v).encode() for v in self._client.lrange(self._log_key(log), 0, -1)]

    def scan(self, prefix : str) -> list[tuple[str, bytes, str]]:
        rows : list[tuple[str, bytes, str]] = []
        pattern = self._key(prefix).replace('*', '\\*').replace('?', '\\?').replace('[', '\\[') + '*'
        try:
            for raw in self._client.scan_iter(match = pattern, count = 500):
                server_key = _text(raw)
                key = server_key[len(self._prefix):]
                value, version = self._decode(self._client.hgetall(server_key))
                if value is not None and version is not None:
                    rows.append((key, value, version))
        except Exception as e:  # noqa: BLE001
            if _is_transport(e):
                raise TransientFailure(f'runtime store unreachable scanning {prefix!r}: {e}',
                                       remedy = 'Restore the Redis behind VF_RUNTIME_STORE_URL.') from e
            raise
        rows.sort(key = lambda row: row[0])
        return rows

    def delete(self, key : str, expected_version : str | None) -> bool:
        server_key = self._key(key)
        for attempt in range(TRANSACTION_RETRIES):
            try:
                with self._client.pipeline() as pipe:
                    pipe.watch(server_key)
                    _current, version = self._decode(pipe.hgetall(server_key))
                    if version is None:
                        pipe.unwatch()
                        return expected_version is None
                    if expected_version is not None and version != expected_version:
                        pipe.unwatch()
                        return False
                    pipe.multi()
                    pipe.delete(server_key)
                    pipe.execute()
                    return True
            except Exception as e:  # noqa: BLE001
                if type(e).__name__ == 'WatchError':
                    time.sleep(random.uniform(0.0, 0.002) * (attempt + 1))
                    continue
                if _is_transport(e):
                    raise TransientFailure(f'runtime store unreachable deleting {key!r}: {e}',
                                           remedy = 'Restore the Redis behind VF_RUNTIME_STORE_URL.') from e
                raise
        return False

    def capabilities(self) -> RuntimeCapabilities:
        '''Read back: persistence on and ``noeviction`` ⇒ durable; denied ``CONFIG`` ⇒ Unknown.'''
        observed = redis_capabilities_observed(self._client)
        durable = observed.durable
        if isinstance(durable, Unknown):
            return RuntimeCapabilities('redis', durable = durable, shared_across_processes = True,
                                       restart_safe_joins = False, elastic_state = False)
        return RuntimeCapabilities('redis', durable = known(bool(durable.value)), shared_across_processes = True,
                                   restart_safe_joins = bool(durable.value), elastic_state = False)


def _text(value : Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def runtime_store_from_url(url : str, client : Optional[Any] = None) -> RedisRuntimeStore:
    '''The registry factory for ``redis://`` and ``rediss://`` (``runtime_stores.make_runtime_store``).'''
    return RedisRuntimeStore(url, client = client)
