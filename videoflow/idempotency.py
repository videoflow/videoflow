'''
Optional sink-side idempotency: dedupe the *effects* of a consumer across message
redelivery/restart, giving "exactly-once-ish" side effects on top of the broker's
at-least-once delivery. A consumer opts in with ``ConsumerNode(idempotent=True)``
and the flow must be given a Redis URL (reuses the blob-store Redis).

Consumers are single sinks (not replicated), so a plain check-then-mark against a
shared store is race-free.
'''
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import hashlib

class IdempotencyStore:
    def seen(self, key : str) -> bool:
        raise NotImplementedError

    def mark(self, key : str):
        raise NotImplementedError

class RedisIdempotencyStore(IdempotencyStore):
    def __init__(self, url : str, ttl_seconds : int = 86400):
        import redis
        self._client = redis.Redis.from_url(url)
        self._ttl = ttl_seconds

    def seen(self, key : str) -> bool:
        return self._client.exists(key) == 1

    def mark(self, key : str):
        self._client.set(key, b'1', ex = self._ttl)

def idempotency_key(flow_id : str, node_name : str, message_id : str) -> str:
    raw = f'{flow_id}:{node_name}:{message_id}'
    return 'vf-idem-' + hashlib.sha256(raw.encode('utf-8')).hexdigest()
