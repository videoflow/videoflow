'''
A fake ``redis.Redis`` for the Redis payload store's unit tests — and for the
conformance suite's process-level PAY cases, which need to interleave two
"processes" around one transaction without a server.

It models the commands ``RedisPayloadStore`` and ``RedisBlobStore`` issue *with the
server semantics they rely on*, not canned answers:

- strings ``SET`` (EX/PX/NX/XX/KEEPTTL), ``GET``, ``STRLEN``, ``DECR``; keys ``EXISTS``,
  ``UNLINK``/``DEL``, ``EXPIRE`` (NX/XX/GT/LT), ``TTL``, ``PERSIST``, ``SCAN`` (MATCH/COUNT);
- sets ``SADD``, ``SREM``, ``SCARD``, ``SMEMBERS`` — an empty set does not exist:
  ``SREM`` of the last member deletes the key, exactly as Redis;
- hashes ``HSET`` (field/value, ``mapping``, ``items``), ``HGET``, ``HGETALL``;
- server ``CONFIG GET`` (``deny_config`` makes it the ``NOPERM`` a locked-down ACL
  gives), ``INFO`` (``cluster_enabled``), ``CLUSTER KEYSLOT`` (real CRC16-XMODEM slots
  with hash-tag extraction when ``cluster_enabled``; otherwise the "cluster support
  disabled" ``ResponseError`` a standalone server gives), ``PING``;
- transactions: ``pipeline()`` returns a pipeline with redis-py 8.0.1's shape —
  ``watch()``, immediate commands while watching, ``multi()``, queued commands,
  ``execute()`` raising ``WatchError`` when a watched key was modified (or expired)
  since the ``WATCH``, ``unwatch()``, ``discard()``, ``reset()`` on context exit;
- expiry: a fake clock (``now``, ``advance(seconds)``); keys expire lazily on access,
  and an expiry counts as a modification for ``WATCH`` (Redis ≥ 6.0.9), except for a
  key that was already expired when watched (Redis ≥ 7.0);
- faults: ``fail(command, exc)`` raises ``exc`` when that command is next issued,
  *before* it applies (the command never reached the server); ``lose_response(command)``
  applies it and then raises a ``ConnectionError`` (the lost-receipt model). Inside a
  watching pipeline both surface the way redis-py surfaces them — a ``WatchError``
  chained on the transport error (client.py:1801-1805 and 2033-2037);
- interleaving: ``hook(command, fn)`` runs ``fn(fake)`` just before that command is
  processed, so a test can have "another process" release, acquire or expire between
  a ``WATCH`` and its ``EXEC``;
- evidence: ``log`` lists every processed command as ``(NAME, *args)`` in order.

Replies are ``bytes`` (``decode_responses = False``), as the real client returns them.
'''
from __future__ import absolute_import, division, print_function

import fnmatch
import math
from typing import Any, Callable, Optional

from redis import exceptions as rexc

#: Wire names of the commands the fake models, keyed by the redis-py method name.
COMMANDS = {
    'set': 'SET', 'get': 'GET', 'strlen': 'STRLEN', 'decr': 'DECR', 'exists': 'EXISTS', 'unlink': 'UNLINK',
    'delete': 'DEL', 'expire': 'EXPIRE', 'ttl': 'TTL', 'persist': 'PERSIST', 'scan': 'SCAN', 'sadd': 'SADD',
    'srem': 'SREM', 'scard': 'SCARD', 'smembers': 'SMEMBERS', 'hset': 'HSET', 'hget': 'HGET',
    'hgetall': 'HGETALL', 'config_get': 'CONFIG GET', 'info': 'INFO', 'cluster': 'CLUSTER', 'ping': 'PING',
}

WRONGTYPE = 'WRONGTYPE Operation against a key holding the wrong kind of value'


def crc16(data : bytes) -> int:
    '''CRC16-XMODEM (poly 0x1021, init 0), the checksum Redis Cluster hashes keys with.'''
    crc = 0
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) if crc & 0x8000 else (crc << 1)
            crc &= 0xFFFF
    return crc


def keyslot(key : str) -> int:
    '''``CLUSTER KEYSLOT``: the slot of the hash tag (text between the first ``{`` and the next ``}``), else of the key.'''
    start = key.find('{')
    if start != -1:
        end = key.find('}', start + 1)
        if end > start + 1:
            key = key[start + 1:end]
    return crc16(key.encode('utf-8')) % 16384


def _encode(value : Any) -> bytes:
    '''redis-py's ``Encoder``: bytes pass through, str is UTF-8, int/float are their text, bool is refused.'''
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, bool):
        raise rexc.DataError('Invalid input of type bool')
    if isinstance(value, float):
        return repr(value).encode('utf-8')
    if isinstance(value, int):
        return str(value).encode('utf-8')
    if isinstance(value, str):
        return value.encode('utf-8')
    raise rexc.DataError(f'Invalid input of type {type(value).__name__}')


def _arg(value : Any) -> str:
    return value.decode('utf-8', 'replace') if isinstance(value, bytes) else str(value)


class FakeRedis:
    '''
    - Arguments:
        - cluster_enabled: what ``INFO cluster`` reports and whether ``CLUSTER KEYSLOT`` answers.
        - config: the ``CONFIG GET`` table; defaults to the dev compose server's shape \
            (no persistence, ``volatile-lru``).
        - deny_config: make ``CONFIG GET`` raise ``NoPermissionError``.
        - now: the fake clock's start (epoch seconds).
    '''
    def __init__(self, cluster_enabled : bool = False, config : Optional[dict] = None,
                 deny_config : bool = False, now : float = 1_700_000_000.0) -> None:
        self.now = float(now)
        self.cluster_enabled = cluster_enabled
        self.deny_config = deny_config
        self.config = dict(config) if config is not None else {
            'appendonly': 'no', 'save': '', 'maxmemory-policy': 'volatile-lru'}
        self.log : list[tuple[str, ...]] = []
        self._data : dict[str, Any] = {}
        self._expires : dict[str, float] = {}
        self._versions : dict[str, int] = {}
        self._faults : dict[str, list[BaseException]] = {}
        self._lost : dict[str, int] = {}
        self._hooks : dict[str, list[list]] = {}

    # -- test seams ------------------------------------------------------------------

    def time(self) -> float:
        return self.now

    def advance(self, seconds : float) -> None:
        self.now += float(seconds)

    def fail(self, command : str, exc : BaseException, times : int = 1) -> None:
        '''Raise ``exc`` the next ``times`` times ``command`` (wire name) is issued, before it applies.'''
        self._faults.setdefault(command, []).extend([exc] * times)

    def lose_response(self, command : str, times : int = 1) -> None:
        '''Apply ``command`` normally, then raise ``ConnectionError`` — the reply was lost.'''
        self._lost[command] = self._lost.get(command, 0) + times

    def hook(self, command : str, fn : Callable[['FakeRedis'], None], times : Optional[int] = 1) -> None:
        '''Run ``fn(fake)`` just before ``command`` is processed, ``times`` times (None: always).'''
        self._hooks.setdefault(command, []).append([fn, times])

    def expire_now(self, key : str) -> None:
        '''The server expiring ``key`` at this instant (a modification for WATCH purposes).'''
        if key in self._data:
            self._drop(key)

    def live_keys(self, pattern : str = '*') -> list[str]:
        return sorted(k for k in list(self._data) if self._alive(k) and fnmatch.fnmatchcase(k, pattern))

    def value(self, key : str) -> Any:
        '''The live value (bytes, set of bytes, or dict of bytes) or None.'''
        return self._data.get(key) if self._alive(key) else None

    def commands(self, name : Optional[str] = None) -> list[tuple[str, ...]]:
        return [entry for entry in self.log if name is None or entry[0] == name]

    def pipeline(self, transaction : bool = True, shard_hint : Any = None) -> 'FakePipeline':
        return FakePipeline(self, transaction)

    # -- the client surface ----------------------------------------------------------

    def set(self, name : str, value : Any, **kwargs : Any) -> Any:
        return self._execute('set', (name, value), kwargs)

    def get(self, name : str) -> Any:
        return self._execute('get', (name,), {})

    def strlen(self, name : str) -> Any:
        return self._execute('strlen', (name,), {})

    def decr(self, name : str, amount : int = 1) -> Any:
        return self._execute('decr', (name, amount), {})

    def exists(self, *names : str) -> Any:
        return self._execute('exists', names, {})

    def unlink(self, *names : str) -> Any:
        return self._execute('unlink', names, {})

    def delete(self, *names : str) -> Any:
        return self._execute('delete', names, {})

    def expire(self, name : str, time : Any, **kwargs : Any) -> Any:
        return self._execute('expire', (name, time), kwargs)

    def ttl(self, name : str) -> Any:
        return self._execute('ttl', (name,), {})

    def persist(self, name : str) -> Any:
        return self._execute('persist', (name,), {})

    def scan(self, cursor : int = 0, match : Optional[str] = None, count : Optional[int] = None,
             **kwargs : Any) -> Any:
        return self._execute('scan', (cursor,), {'match': match, 'count': count})

    def sadd(self, name : str, *values : Any) -> Any:
        return self._execute('sadd', (name,) + values, {})

    def srem(self, name : str, *values : Any) -> Any:
        return self._execute('srem', (name,) + values, {})

    def scard(self, name : str) -> Any:
        return self._execute('scard', (name,), {})

    def smembers(self, name : str) -> Any:
        return self._execute('smembers', (name,), {})

    def hset(self, name : str, key : Any = None, value : Any = None, mapping : Any = None,
             items : Any = None) -> Any:
        return self._execute('hset', (name,), {'key': key, 'value': value, 'mapping': mapping, 'items': items})

    def hget(self, name : str, key : Any) -> Any:
        return self._execute('hget', (name, key), {})

    def hgetall(self, name : str) -> Any:
        return self._execute('hgetall', (name,), {})

    def config_get(self, pattern : str = '*', *args : str, **kwargs : Any) -> Any:
        return self._execute('config_get', (pattern,) + args, {})

    def info(self, section : Optional[str] = None, *args : str, **kwargs : Any) -> Any:
        return self._execute('info', (section,) + args, {})

    def cluster(self, cluster_arg : str, *args : Any, **kwargs : Any) -> Any:
        return self._execute('cluster', (cluster_arg,) + args, {})

    def ping(self) -> Any:
        return self._execute('ping', (), {})

    # -- server internals ------------------------------------------------------------------

    def _execute(self, method : str, args : tuple, kwargs : dict) -> Any:
        wire = COMMANDS[method]
        self._before(wire, args)
        result = self._apply(method, args, kwargs)
        self._after(wire)
        return result

    def _before(self, wire : str, args : tuple) -> None:
        for entry in list(self._hooks.get(wire, ())):
            fn, times = entry
            if times is not None:
                if times <= 0:
                    continue
                entry[1] = times - 1
            fn(self)
        pending = self._faults.get(wire)
        if pending:
            raise pending.pop(0)

    def _after(self, wire : str) -> None:
        if self._lost.get(wire, 0) > 0:
            self._lost[wire] -= 1
            raise rexc.ConnectionError('Connection closed by server.')

    def _apply(self, method : str, args : tuple, kwargs : dict) -> Any:
        self.log.append((COMMANDS[method],) + tuple(_arg(a) for a in args))
        return getattr(self, '_cmd_' + method)(*args, **kwargs)

    def _alive(self, key : str) -> bool:
        expires = self._expires.get(key)
        if expires is not None and expires <= self.now:
            self._drop(key)
            return False
        return key in self._data

    def _drop(self, key : str) -> None:
        self._data.pop(key, None)
        self._expires.pop(key, None)
        self._touch(key)

    def _touch(self, key : str) -> None:
        self._versions[key] = self._versions.get(key, 0) + 1

    def _modified_since(self, key : str, version : int) -> bool:
        self._alive(key)
        return self._versions.get(key, 0) != version

    def _typed(self, name : str, kind : type, create : bool) -> Any:
        if self._alive(name):
            value = self._data[name]
            if not isinstance(value, kind):
                raise rexc.ResponseError(WRONGTYPE)
            return value
        if not create:
            return kind()
        value = kind()
        self._data[name] = value
        self._expires.pop(name, None)
        return value

    # -- commands -----------------------------------------------------------------------------

    def _cmd_set(self, name : str, value : Any, ex : Any = None, px : Any = None, nx : bool = False,
                 xx : bool = False, keepttl : bool = False, **kwargs : Any) -> Any:
        exists = self._alive(name)
        if (nx and exists) or (xx and not exists):
            return None
        self._data[name] = _encode(value)
        if keepttl and exists:
            pass
        elif ex is not None:
            self._expires[name] = self.now + float(ex)
        elif px is not None:
            self._expires[name] = self.now + float(px) / 1000.0
        else:
            self._expires.pop(name, None)
        self._touch(name)
        return True

    def _cmd_get(self, name : str) -> Optional[bytes]:
        return self._typed(name, bytes, False) if self._alive(name) else None

    def _cmd_strlen(self, name : str) -> int:
        return len(self._typed(name, bytes, False))

    def _cmd_decr(self, name : str, amount : int = 1) -> int:
        current = int(self._typed(name, bytes, False)) if self._alive(name) else 0
        current -= amount
        self._data[name] = str(current).encode('utf-8')
        self._touch(name)
        return current

    def _cmd_exists(self, *names : str) -> int:
        return sum(1 for name in names if self._alive(name))

    def _cmd_unlink(self, *names : str) -> int:
        removed = 0
        for name in names:
            if self._alive(name):
                self._drop(name)
                removed += 1
        return removed

    _cmd_delete = _cmd_unlink

    def _cmd_expire(self, name : str, time : Any, nx : bool = False, xx : bool = False, gt : bool = False,
                    lt : bool = False) -> bool:
        if not self._alive(name):
            return False
        current = self._expires.get(name)
        new = self.now + float(time)
        if (nx and current is not None) or (xx and current is None):
            return False
        if gt and (current is None or new <= current):   # a key without a TTL counts as infinite
            return False
        if lt and current is not None and new >= current:
            return False
        self._expires[name] = new
        self._touch(name)
        return True

    def _cmd_ttl(self, name : str) -> int:
        if not self._alive(name):
            return -2
        expires = self._expires.get(name)
        if expires is None:
            return -1
        return max(0, int(math.ceil(expires - self.now)))

    def _cmd_persist(self, name : str) -> bool:
        if not self._alive(name) or name not in self._expires:
            return False
        del self._expires[name]
        self._touch(name)
        return True

    def _cmd_scan(self, cursor : int = 0, match : Optional[str] = None, count : Optional[int] = None) -> Any:
        keys = self.live_keys(match or '*')
        start, size = int(cursor), int(count or 10)
        batch = keys[start:start + size]
        following = start + size if start + size < len(keys) else 0
        return following, [k.encode('utf-8') for k in batch]

    def _cmd_sadd(self, name : str, *values : Any) -> int:
        members = self._typed(name, set, True)
        added = 0
        for value in values:
            encoded = _encode(value)
            if encoded not in members:
                members.add(encoded)
                added += 1
        if not members:
            self._data.pop(name, None)
        if added:
            self._touch(name)
        return added

    def _cmd_srem(self, name : str, *values : Any) -> int:
        if not self._alive(name):
            return 0
        members = self._typed(name, set, False)
        removed = 0
        for value in values:
            encoded = _encode(value)
            if encoded in members:
                members.remove(encoded)
                removed += 1
        if not members:   # an empty set does not exist
            self._data.pop(name, None)
            self._expires.pop(name, None)
        if removed:
            self._touch(name)
        return removed

    def _cmd_scard(self, name : str) -> int:
        return len(self._typed(name, set, False)) if self._alive(name) else 0

    def _cmd_smembers(self, name : str) -> set:
        return set(self._typed(name, set, False)) if self._alive(name) else set()

    def _cmd_hset(self, name : str, key : Any = None, value : Any = None, mapping : Any = None,
                  items : Any = None) -> int:
        pairs : list[tuple[Any, Any]] = []
        if items:
            pairs.extend(zip(items[::2], items[1::2]))
        if key is not None:
            pairs.append((key, value))
        if mapping:
            pairs.extend(mapping.items())
        if not pairs:
            raise rexc.DataError("'hset' with no key value pairs")
        fields = self._typed(name, dict, True)
        added = 0
        for field, item in pairs:
            encoded = _encode(field)
            if encoded not in fields:
                added += 1
            fields[encoded] = _encode(item)
        self._touch(name)
        return added

    def _cmd_hget(self, name : str, key : Any) -> Optional[bytes]:
        return self._typed(name, dict, False).get(_encode(key)) if self._alive(name) else None

    def _cmd_hgetall(self, name : str) -> dict:
        return dict(self._typed(name, dict, False)) if self._alive(name) else {}

    def _cmd_config_get(self, pattern : str = '*', *patterns : str) -> dict:
        if self.deny_config:
            raise rexc.NoPermissionError("NOPERM this user has no permissions to run the 'config|get' command")
        wanted = (pattern,) + patterns
        return {k: v for k, v in self.config.items() if any(fnmatch.fnmatchcase(k, p) for p in wanted)}

    def _cmd_info(self, section : Optional[str] = None, *args : str) -> dict:
        return {'redis_version': '7.4.11', 'cluster_enabled': 1 if self.cluster_enabled else 0}

    def _cmd_cluster(self, cluster_arg : str, *args : Any) -> Any:
        if not self.cluster_enabled:
            raise rexc.ResponseError('ERR This instance has cluster support disabled')
        if cluster_arg.upper() == 'KEYSLOT':
            return keyslot(_arg(args[0]))
        raise rexc.ResponseError(f'ERR unknown subcommand {cluster_arg!r}')

    def _cmd_ping(self) -> bool:
        return True


class FakePipeline:
    '''
    redis-py 8.0.1's ``Pipeline`` shape over a ``FakeRedis``: immediate commands
    while watching, queued commands after ``multi()``, and ``execute()`` that
    aborts with ``WatchError`` when a watched key changed.
    '''
    def __init__(self, server : FakeRedis, transaction : bool = True) -> None:
        self._server = server
        self.transaction = transaction
        self.watching = False
        self.explicit_transaction = False
        self.command_stack : list[tuple[str, tuple, dict]] = []
        self._watched : dict[str, int] = {}

    def __enter__(self) -> 'FakePipeline':
        return self

    def __exit__(self, *exc : Any) -> None:
        self.reset()

    def __len__(self) -> int:
        return len(self.command_stack)

    def __bool__(self) -> bool:
        return True

    def reset(self) -> None:
        self.command_stack = []
        if self.watching:
            self._server.log.append(('UNWATCH',))
        self.watching = False
        self.explicit_transaction = False
        self._watched = {}

    def close(self) -> None:
        self.reset()

    def watch(self, *names : str) -> bool:
        if self.explicit_transaction:
            raise rexc.RedisError('Cannot issue a WATCH after a MULTI')
        # Not watching yet, so a transport fault here is raw (retries = 0, connection.py:894).
        self._server._before('WATCH', names)
        self._server.log.append(('WATCH',) + tuple(names))
        for name in names:
            self._server._alive(name)   # already expired at WATCH time: no abort later (Redis 7.0)
            self._watched[name] = self._server._versions.get(name, 0)
        self.watching = True
        return True

    def unwatch(self) -> bool:
        if self.watching:
            self._server.log.append(('UNWATCH',))
            self.watching = False
            self._watched = {}
        return True

    def multi(self) -> None:
        if self.explicit_transaction:
            raise rexc.RedisError('Cannot issue nested calls to MULTI')
        if self.command_stack:
            raise rexc.RedisError('Commands without an initial WATCH have already been issued')
        self.explicit_transaction = True

    def discard(self) -> None:
        self.command_stack = []
        self.explicit_transaction = False
        self.unwatch()

    def execute(self, raise_on_error : bool = True) -> list:
        stack, watching = self.command_stack, self.watching
        if not stack and not watching:
            return []
        try:
            try:
                self._server._before('EXEC', ())
            except (rexc.ConnectionError, rexc.TimeoutError) as e:
                self._as_redis_py_would(e, watching)
                raise
            dirty = any(self._server._modified_since(k, v) for k, v in self._watched.items())
            self.watching = False
            if dirty:
                raise rexc.WatchError('Watched variable changed.')
            self._server.log.append(('MULTI',))
            results = [self._server._apply(method, args, kwargs) for method, args, kwargs in stack]
            self._server.log.append(('EXEC',))
            try:
                self._server._after('EXEC')
            except (rexc.ConnectionError, rexc.TimeoutError) as e:
                self._as_redis_py_would(e, watching)
                raise
            return results
        finally:
            self.reset()

    @staticmethod
    def _as_redis_py_would(error : BaseException, watching : bool) -> None:
        '''client.py:2033-2037 (8.0.1): while watching, a transport error becomes a WatchError chained on it.'''
        if watching:
            raise rexc.WatchError(f'A {type(error).__name__} occurred while watching one or more keys')

    def _issue(self, method : str, args : tuple, kwargs : dict) -> Any:
        if self.watching and not self.explicit_transaction:
            try:
                return self._server._execute(method, args, kwargs)
            except (rexc.ConnectionError, rexc.TimeoutError) as e:
                self.reset()   # client.py:1801-1805: reset, then a WatchError chained on the transport error
                raise rexc.WatchError(f'A {type(e).__name__} occurred while watching one or more keys') from e
        self.command_stack.append((method, args, kwargs))
        return self

    def __getattr__(self, name : str) -> Any:
        if name.startswith('_') or name not in COMMANDS:
            raise AttributeError(name)

        def call(*args : Any, **kwargs : Any) -> Any:
            return self._issue(name, args, kwargs)
        return call
