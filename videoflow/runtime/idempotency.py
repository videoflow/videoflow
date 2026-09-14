'''
Optional sink-side idempotency: dedupe the *effects* of a consumer across message
redelivery/restart, giving "exactly-once-ish" side effects on top of the broker's
at-least-once delivery. A consumer opts in with ``ConsumerNode(idempotent=True)``
and the flow must be given a Redis URL (reuses the blob-store Redis).

Consumers are single sinks (not replicated), so a plain check-then-mark against a
shared store is race-free.
'''
from __future__ import absolute_import, division, print_function

import hashlib
from typing import Any

#: How long a sink's effect marker is kept (the reviewed 86400 s default): shorter
#: than the dead-letter replay horizon, which is why a marker alone never certifies
#: exactly-once (RUN-017) — the planner compares the two.
EFFECT_RETENTION_SECONDS = 86400.0


class IdempotencyStore:
    def seen(self, key : str) -> bool:
        raise NotImplementedError

    def mark(self, key : str) -> None:
        raise NotImplementedError

class RedisIdempotencyStore(IdempotencyStore):
    def __init__(self, url : str, ttl_seconds : int = 86400) -> None:
        # Deferred: `redis` is an optional dependency (the `blob` extra).
        import redis
        self._client = redis.Redis.from_url(url)
        self._ttl = ttl_seconds

    def seen(self, key : str) -> bool:
        return self._client.exists(key) == 1

    def mark(self, key : str) -> None:
        self._client.set(key, b'1', ex = self._ttl)

def idempotency_key(flow_id : str, node_name : str, message_id : str) -> str:
    raw = f'{flow_id}:{node_name}:{message_id}'
    return 'vf-idem-' + hashlib.sha256(raw.encode('utf-8')).hexdigest()


class LedgerIdempotencyStore(IdempotencyStore):
    '''
    Effect markers in the run ledger (RFC 0006 ``CTRL-4``; RUN-017): the same
    ``seen``/``mark`` contract as the Redis store, kept where the node's other
    durable facts are, with the retention the planner admitted
    (``effect_retention_seconds``). A marker is evidence that *this runtime*
    applied the effect; it never certifies exactly-once on its own — only a sink
    that keys its external effect (``effect_guarantee = 'idempotent_key'``) does.
    '''
    def __init__(self, runtime : Any, retention_seconds : float = 86400.0) -> None:
        self._runtime = runtime
        self._retention = retention_seconds

    def seen(self, key : str) -> bool:
        return bool(self._runtime.effect_seen(key))

    def mark(self, key : str) -> None:
        self._runtime.mark_effect(key, self._retention)
