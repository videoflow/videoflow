'''
The Redis ``PayloadStore``: named, generation-fenced obligations instead of a
decrement-only counter, on the keys RFC 0006 ``BLOB-13`` names, with every
multi-key step an optimistic ``WATCH``/``MULTI``/``EXEC`` transaction and no Lua.

Why this module exists
----------------------
``RedisBlobStore`` (RFC 0002, ``serialization.py``) reclaims an offloaded payload
with one counter: ``EXISTS`` then ``DECR`` then ``UNLINK`` at zero. Reviewed against
the payload conformance cases that shape has three holes. Two deliveries of one
message to the same reader decrement twice (PAY-005). A counter that expires
between the ``EXISTS`` and the ``DECR`` is re-created at ``-1`` and the blob another
reader still needs is deleted (PAY-008). A worker that dies after its ack but
before its release leaks the blob until its TTL with nothing able to tell that
the reader finished (PAY-006). This store replaces the counter with *obligations*:
a SET of reader ids, released by id (idempotent), fenced by the generation
minted at put (a stale release touches nothing), reconcilable from a ledger.

Keys (``BLOB-13``)
------------------
=================================  ======  ==============================================
``vf-blob-<hex>``                  string  the bytes; the wire ``BlobRef.ref``, unchanged
``vf-blobmeta-{vf-blob-<hex>}``    HASH    ``size``, ``digest``, ``generation``,
                                           ``content_id``, ``created_at``
``vf-blobobl-{vf-blob-<hex>}``     SET     obligation ids; EXPIRE = latest deadline
=================================  ======  ==============================================

The companion keys are *hash-tagged* onto the blob key: on Redis Cluster a key's
slot is computed from the text between its first ``{`` and ``}``, so all three
hash to the slot of ``vf-blob-<hex>`` and one ``UNLINK`` / one transaction can
cover them (decision D10). The blob key itself keeps its RFC 0002 spelling, so
nothing on the wire changes. RFC 0002's ``vf-blobrc-<hex>`` counter is *not*
hash-tagged (it predates this layout); the fallback below keeps treating it as a
single-key operation for that reason.

Lifecycle (``BLOB-14``)
-----------------------
1. *Put* writes the blob, then the metadata, then the obligation set. An
   interrupted put therefore degrades to a TTL-only blob — the safe direction
   (``BLOB-5``): nothing can ever be reclaimed early because a record is missing.
2. *Release* is ``WATCH obl meta`` → read the set and the stored generation →
   ``MULTI`` ``SREM obl id`` [+ ``UNLINK`` of all three keys when the set would
   become empty and the generation matches] → ``EXEC``. A nil ``EXEC`` (something
   touched the keys since the ``WATCH``) retries, bounded by ``TRANSACTION_RETRIES``
   with a jittered pause between attempts — eight replicas of a partitioned child
   release one object within milliseconds of each other, and a bound below the
   reader count strands the last contenders with an ``unknown`` receipt the
   messenger does not retry (the object then leaks until its TTL). A release naming an
   older generation is ``stale`` and touches nothing. A missing obligation set is
   never created by a release: the reader applies RFC 0002 counter semantics if
   ``vf-blobrc-<hex>`` exists (``BLOB-6``) and otherwise leaves the blob to its TTL.
3. *Acquire* (``dlq/<flow>``, ``archive/<flow>``, replay readers) adds the id and
   extends every key's life to the deadline — never shortens it — so a dead
   letter's bytes outlive the run (PAY-014).
4. *Reconcile* scans the store (``SCAN``, never ``KEYS``) and reclaims objects
   whose obligation set is empty, objects that never received their set (a put
   interrupted after the metadata, older than ``orphan_grace_seconds``, with no
   RFC 0002 counter) and objects whose only obligations are ``intent/*`` ids the
   ledger no longer requires. Anything it could not read lands in ``unknown``.
5. *TTL backstop* (``BLOB-7``): every key carries the contract's TTL, extended to
   the horizon when obligations pin the object.

Typed reads (``BLOB-15``)
-------------------------
``read`` never returns ``None`` and never raises for a store that is merely
unreachable: a transport failure is ``TransientFailure`` (retry — never "malformed
bytes"), a nil is ``Missing``, a digest mismatch against the metadata is
``Corrupt``. A key with no metadata (an RFC 0002 publisher, or metadata that
expired) reads unverified, and says so.

redis-py facts this module relies on (redis 8.0.1, ``.venv/.../redis/``)
--------------------------------------------------------------------------
- ``client.py:1763-1766`` — a ``Pipeline`` that is ``watching`` runs each command
  immediately and returns its real reply until ``multi()`` is called; after
  ``multi()`` commands are queued for ``execute()``.
- ``client.py:1879-1921`` — ``execute()`` sends ``MULTI … EXEC`` as one packet; a
  nil ``EXEC`` reply (a watched key changed) raises ``WatchError('Watched variable
  changed.')``. ``client.py:2039-2100`` — ``execute()`` always ``reset()``s in
  ``finally``, which returns the connection to the pool.
- ``client.py:2003-2037`` and ``1768-1805`` — a ``ConnectionError``/``TimeoutError``
  while watching is re-raised as a ``WatchError`` whose ``__context__`` is the
  transport error. That is how a *lost response* to ``EXEC`` reaches us, and why
  the release code inspects ``__context__`` to report ``unknown`` instead of
  retrying blindly.
- ``client.py:1703-1707`` — ``with client.pipeline() as pipe`` calls ``reset()`` on
  exit (an ``UNWATCH`` is sent if still watching). ``client.py:2112-2120`` —
  ``watch()`` refuses to run after ``multi()``; ``unwatch()`` only sends when watching.
- ``exceptions.py:23-56`` — ``BusyLoadingError`` and ``AuthenticationError`` are
  ``ConnectionError`` subclasses; ``TimeoutError`` is separate; ``WatchError``
  (``:69``), ``ResponseError`` (``:57``), ``NoPermissionError(ResponseError)``
  (``:97``), ``OutOfMemoryError(ResponseError)`` (``:77``). None of them derive from
  the builtins of the same name.
- ``connection.py:884-894`` — the default retry policy is ``Retry(NoBackoff(), 0)``:
  a transport error surfaces once, redis-py does not retry it for us.
- ``_parsers/helpers.py:35`` — ``INFO`` is parsed into a dict with ints
  (``cluster_enabled`` → ``0``/``1``); ``:911`` — ``CONFIG GET`` values are ``str``
  even with ``decode_responses = False``; ``:933`` — ``SCAN`` returns
  ``(int cursor, [raw keys])``. Every other reply here is raw ``bytes``.
- ``commands/core.py:2997-3033`` — ``expire(name, secs, gt = True)`` sends
  ``EXPIRE key secs GT`` (Redis ≥ 7.0): set only when longer than the current
  expiry, and a key without a TTL counts as infinite, so it is never shortened.
- ``commands/core.py:11815`` — ``client.cluster('KEYSLOT', key)``; a standalone
  server answers with a ``ResponseError`` (cluster support disabled), which is why
  the probe first reads ``INFO cluster``.

Redis server facts: an empty SET does not exist (``SREM`` of the last member
deletes the key, so "set absent" and "set empty" are one state); a watched key
that *expires* before ``EXEC`` aborts the transaction (since 6.0.9; one already
expired at ``WATCH`` time does not, since 7.0); ``SCAN`` may return a key twice
and never blocks the server.
'''
from __future__ import absolute_import, division, print_function

import hashlib
import logging
import math
import os
import random
import time
import uuid
from typing import Any, Callable, Mapping, Optional

from ..backends import faults
from ..backends.capabilities import PayloadCapabilities
from ..backends.outcomes import Observation, known, unknown
from ..backends.payload import (
    Corrupt,
    DurableReceipt,
    ImmutablePayloadRef,
    Missing,
    ObligationLedger,
    PayloadBytes,
    PayloadStore,
    ReadOutcome,
    ReclamationObservation,
    ReleaseReceipt,
    RetentionContract,
    TransientFailure,
)
from ..core.errors import ResourceUnavailable
from ..core.errors import TransientFailure as TransientError

logger = logging.getLogger(__package__)

#: The store identity written into every ref (``ImmutablePayloadRef.store``).
STORE_ID = 'redis'

BLOB_KEY_PREFIX = 'vf-blob-'
OBLIGATION_KEY_PREFIX = 'vf-blobobl-'
METADATA_KEY_PREFIX = 'vf-blobmeta-'
#: RFC 0002's reclamation counter (``BLOB-5``): the fallback contract when no obligation set exists.
COUNTER_KEY_PREFIX = 'vf-blobrc-'
#: Publisher intents (``intent/<publication_id>``): the only obligations reconcile may cancel on its own.
INTENT_PREFIX = 'intent/'

#: Bound on optimistic-transaction retries (``BLOB-14`` step 2); past it the outcome is
#: ``unknown``. Every contender on one object costs the others a round — eight replicas
#: of a partitioned child release the same key within milliseconds of each other —
#: so the bound must exceed the largest reader set a payload carries, and each retry
#: yields briefly (``_RETRY_BACKOFF_SECONDS``, jittered) so the contenders do not
#: re-collide in lockstep.
TRANSACTION_RETRIES = 32
_RETRY_BACKOFF_SECONDS = 0.002
#: How long a blob with metadata but no obligation set is presumed to be a put still in progress.
DEFAULT_ORPHAN_GRACE_SECONDS = 60.0
#: Redis' ``proto-max-bulk-len`` default: the largest string value a stock server accepts.
MAX_OBJECT_BYTES = 512 << 20
#: Keys per ``SCAN`` round trip.
SCAN_COUNT = 200

REDIS_URL_ENV = 'VIDEOFLOW_BLOB_REDIS_URL'
DEFAULT_REDIS_URL = 'redis://localhost:6379/0'

def obligation_key(key : str) -> str:
    '''``vf-blobobl-{vf-blob-<hex>}`` — hash-tagged onto the blob key (``BLOB-13``).'''
    return f'{OBLIGATION_KEY_PREFIX}{{{key}}}'

def metadata_key(key : str) -> str:
    '''``vf-blobmeta-{vf-blob-<hex>}`` — hash-tagged onto the blob key (``BLOB-13``).'''
    return f'{METADATA_KEY_PREFIX}{{{key}}}'

def counter_key(key : str) -> str:
    '''``vf-blobrc-<hex>`` — RFC 0002's counter for the blob (``BLOB-5``), *not* hash-tagged.'''
    return COUNTER_KEY_PREFIX + key.removeprefix(BLOB_KEY_PREFIX)

def blob_key_of(companion : str) -> Optional[str]:
    '''The blob key a companion key is hash-tagged onto, or None when ``companion`` is not one.'''
    for prefix in (OBLIGATION_KEY_PREFIX, METADATA_KEY_PREFIX):
        if companion.startswith(prefix + '{') and companion.endswith('}'):
            return companion[len(prefix) + 1:-1]
    return None

def _text(value : Any) -> str:
    '''A reply as text: raw ``bytes`` by default, ``str`` when the client was built with ``decode_responses``.'''
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).decode('utf-8')
    return str(value)

def _redis_exceptions() -> Any:
    '''
    The ``redis.exceptions`` module, imported on demand so this module imports
    cleanly without the extra (the core runs with no blob store at all).
    '''
    import redis.exceptions  # optional dependency (extra): only the Redis stores need it
    return redis.exceptions

# -- capabilities ---------------------------------------------------------------------

def redis_capabilities_observed(client : Any, max_object_bytes : int = MAX_OBJECT_BYTES) -> PayloadCapabilities:
    '''
    What this Redis, *as configured*, can guarantee — read back live, never assumed.

    - Arguments:
        - client: a ``redis.Redis`` (or a fake modelling ``config_get``, ``info`` and \
            ``cluster``).
        - max_object_bytes: the largest object the server accepts (``proto-max-bulk-len``).

    - Returns: a ``PayloadCapabilities`` whose ``durable`` is ``Known(True)`` only when \
        persistence is on (``appendonly yes`` or a ``save`` schedule) *and* the \
        eviction policy is ``noeviction``; ``evictable`` is ``Known(policy != \
        'noeviction')``; both are ``Unknown('auth', …)`` when ``CONFIG GET`` is denied \
        (ACL ``NOPERM``, or the command renamed/disabled on a managed offering). \
        ``atomic_multikey`` is ``Known(True)`` on a standalone server, and on a cluster \
        only when ``CLUSTER KEYSLOT`` agrees for the three keys of a probe blob; any \
        failure to observe it is ``Unknown`` — the planner treats that as "not offered".
    '''
    errors = _redis_exceptions()

    def setting(name : str) -> str:
        value = client.config_get(name).get(name)
        return '' if value is None else _text(value)

    durable : Observation[bool]
    evictable : Observation[bool]
    try:
        appendonly, save, policy = setting('appendonly'), setting('save'), setting('maxmemory-policy')
    except errors.TimeoutError as e:
        durable = evictable = unknown('timeout', f'CONFIG GET timed out: {e}')
    except errors.ConnectionError as e:
        durable = evictable = unknown('unreachable', f'CONFIG GET failed: {type(e).__name__}: {e}')
    except errors.ResponseError as e:
        # NOPERM (an ACL without CONFIG) and "unknown command" (CONFIG renamed or
        # disabled, the managed-Redis norm) both arrive as ResponseError; either way
        # the server is refusing to tell us, which is an access problem, not a timeout.
        durable = evictable = unknown('auth', f'CONFIG GET refused: {e}')
    else:
        persistent = appendonly.strip().lower() == 'yes' or save.strip() != ''
        durable = known(persistent and policy.strip() == 'noeviction')
        evictable = known(policy.strip() != 'noeviction')

    atomic : Observation[bool]
    try:
        enabled = int(client.info('cluster').get('cluster_enabled', 0) or 0)
        if not enabled:
            atomic = known(True)
        else:
            probe = f'{BLOB_KEY_PREFIX}{uuid.uuid4().hex}'
            slots = {int(client.cluster('KEYSLOT', name))
                     for name in (probe, obligation_key(probe), metadata_key(probe))}
            atomic = known(len(slots) == 1)
    except errors.TimeoutError as e:
        atomic = unknown('timeout', f'cluster probe timed out: {e}')
    except errors.ConnectionError as e:
        atomic = unknown('unreachable', f'cluster probe failed: {type(e).__name__}: {e}')
    except errors.RedisError as e:
        atomic = unknown('unsupported', f'cluster probe refused: {type(e).__name__}: {e}')
    except (TypeError, ValueError) as e:
        atomic = unknown('malformed', f'cluster probe reply not understood: {e}')

    return PayloadCapabilities(STORE_ID, durable = durable, evictable = evictable, atomic_multikey = atomic,
                               max_object_bytes = max_object_bytes, reader_identities = True)

# -- the store ---------------------------------------------------------------------------

class RedisPayloadStore(PayloadStore):
    '''
    - Arguments:
        - url: the Redis URL; defaults to ``VIDEOFLOW_BLOB_REDIS_URL`` or the local \
            dev server, exactly as ``RedisBlobStore``.
        - client: an already-built client (a test's fake). When given, ``url`` is ignored.
        - orphan_grace_seconds: how long a blob that has metadata but no obligation set \
            is left alone by ``reconcile`` — a put interrupted before its set was \
            written must not be reclaimed while its publisher may still be about to \
            reference it.
        - clock: epoch-seconds source. Obligation deadlines and ``created_at`` are in \
            this domain, because they are compared across processes.

    Thread-safe: every transaction takes its own pooled connection.
    '''
    def __init__(self, url : Optional[str] = None, client : Any = None,
                 orphan_grace_seconds : float = DEFAULT_ORPHAN_GRACE_SECONDS,
                 clock : Callable[[], float] = time.time) -> None:
        import redis  # optional dependency (extra): only the Redis stores need it
        self._errors = redis.exceptions
        #: The transport failures a caller retries: the bytes may well still exist.
        self._transient : tuple[type[BaseException], ...] = (
            redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, redis.exceptions.BusyLoadingError)
        if client is None:
            client = redis.Redis.from_url(url or os.environ.get(REDIS_URL_ENV, DEFAULT_REDIS_URL))
        self._client : Any = client
        self._grace = float(orphan_grace_seconds)
        self._clock = clock

    @property
    def client(self) -> Any:
        return self._client

    def capabilities(self) -> PayloadCapabilities:
        return redis_capabilities_observed(self._client)

    # -- put -------------------------------------------------------------------------------

    def put(self, data : bytes, content_id : str, contract : RetentionContract) -> ImmutablePayloadRef:
        '''
        Blob, then metadata, then the obligation set (``BLOB-14`` step 1). Every key
        gets the contract's TTL, extended to the horizon when obligations pin the
        object (``BLOB-14`` step 5); a TTL of 0 or less means no expiry.
        ``payload.write.after`` fires once the bytes and their metadata are on the
        server and before the set is written; ``obligation.acquire.after`` once the
        set is — so a fault schedule can stop a put at exactly the point where it
        leaves a counterless, TTL-only object (PAY-007).

        - Raises:
            - ResourceUnavailable: the server refused the write for memory (``OOM`` \
                under ``noeviction``) — backpressure, never silent eviction.
            - TransientFailure (core): the server was unreachable or slow. Whatever \
                was written before the failure is TTL-only and harmless.
        '''
        digest = hashlib.sha256(data).hexdigest()
        faults.barrier('payload.write.before', content_id = content_id, size = len(data))
        key = f'{BLOB_KEY_PREFIX}{uuid.uuid4().hex}'
        generation = uuid.uuid4().hex[:8]
        life = self._life_seconds(contract)
        record = {'size': str(len(data)), 'digest': digest, 'generation': generation,
                  'content_id': content_id, 'created_at': repr(float(self._clock()))}
        try:
            # 1. the bytes — a crash from here on leaves at worst a TTL-only blob.
            self._client.set(key, bytes(data), ex = life)
            # 2. the metadata that lets a reader verify and fence.
            self._client.hset(metadata_key(key), mapping = record)
            if life is not None:
                self._client.expire(metadata_key(key), life)
        except self._errors.OutOfMemoryError as e:
            raise self._refused(e, content_id, key, len(data)) from e
        except self._transient as e:
            raise self._unavailable(e, content_id, key) from e
        faults.barrier('payload.write.after', key = key, content_id = content_id)
        if contract.obligations:
            try:
                # 3. the obligations, last: their presence is what makes the object reclaimable.
                self._client.sadd(obligation_key(key), *contract.obligations)
                if life is not None:
                    self._client.expire(obligation_key(key), life)
            except self._errors.OutOfMemoryError as e:
                raise self._refused(e, content_id, key, len(data)) from e
            except self._transient as e:
                raise self._unavailable(e, content_id, key) from e
            faults.barrier('obligation.acquire.after', key = key, obligation_id = ','.join(contract.obligations))
        return ImmutablePayloadRef(STORE_ID, key, len(data), digest, generation, content_id)

    @staticmethod
    def _refused(error : BaseException, content_id : str, key : str, size : int) -> ResourceUnavailable:
        '''The server refused a write for memory (``OOM`` under ``noeviction``): backpressure, never silent eviction.'''
        return ResourceUnavailable(
            f'the Redis payload store refused {size} bytes for {content_id}: {error}',
            remedy = 'Backpressure the producer, raise maxmemory, or release obligations so '
                     'reclaimable objects free memory; do not switch to an evicting policy for '
                     'reliable work.', key = key, size = size)

    @staticmethod
    def _unavailable(error : BaseException, content_id : str, key : str) -> TransientError:
        '''The server was unreachable or slow mid-put: whatever was written before is TTL-only and harmless.'''
        return TransientError(
            f'the Redis payload store was unavailable while storing {content_id}: '
            f'{type(error).__name__}: {error}',
            remedy = 'Retry the publish; any partially written object is TTL-only.', key = key)

    @staticmethod
    def _life_seconds(contract : RetentionContract) -> Optional[int]:
        '''The TTL every key gets at put: the contract's, or the horizon when obligations pin it longer.'''
        ttl = int(contract.ttl_seconds)
        if ttl <= 0:
            return None
        if contract.obligations and int(contract.horizon_seconds) > ttl:
            return int(contract.horizon_seconds)
        return ttl

    # -- obligations --------------------------------------------------------------------------

    def acquire_obligation(self, ref : ImmutablePayloadRef, obligation_id : str, deadline : float) -> DurableReceipt:
        '''
        ``SADD`` the id and extend every key's life to ``deadline`` (epoch seconds),
        never shortening it, in one transaction fenced on the stored generation.

        - Raises:
            - LookupError: the object is not stored (expired, evicted, reclaimed or \
                never written), or is a different generation now — as the reference \
                store.
            - TransientFailure (core): the server was unreachable, or the record kept \
                changing under every attempt.
        '''
        key, obl, meta = ref.key, obligation_key(ref.key), metadata_key(ref.key)
        seconds = int(math.ceil(deadline - self._clock()))
        for _ in range(TRANSACTION_RETRIES):
            try:
                with self._client.pipeline() as pipe:
                    pipe.watch(key, obl, meta)
                    stored = pipe.hget(meta, 'generation')
                    blob_ttl = int(pipe.ttl(key))
                    set_ttl = int(pipe.ttl(obl))
                    if stored is None or blob_ttl == -2:
                        pipe.unwatch()
                        raise LookupError(f'{key} is not stored (expired, evicted or never written)')
                    if ref.generation and _text(stored) != ref.generation:
                        pipe.unwatch()
                        raise LookupError(f'{key} is a different generation now ({_text(stored)} != {ref.generation})')
                    pipe.multi()
                    pipe.sadd(obl, obligation_id)
                    self._queue_pin(pipe, key, seconds, blob_ttl, set_ttl)
                    pipe.execute()
                break
            except self._errors.WatchError:
                self._yield()   # a release, acquire or reconcile touched the keys: re-read and retry
                continue
            except self._transient as e:
                raise TransientError(
                    f'could not acquire {obligation_id} on {key}: {type(e).__name__}: {e}',
                    remedy = 'Retry; the payload store was unreachable or slow.', key = key,
                    obligation_id = obligation_id) from e
        else:
            raise TransientError(
                f'could not acquire {obligation_id} on {key}: its records changed under all '
                f'{TRANSACTION_RETRIES} attempts',
                remedy = 'Retry; the object is being released or reconciled concurrently.',
                key = key, obligation_id = obligation_id)
        faults.barrier('obligation.acquire.after', key = key, obligation_id = obligation_id)
        return DurableReceipt(ref, obligation_id, deadline)

    def renew_obligation(self, ref : ImmutablePayloadRef, obligation_id : str, deadline : float) -> DurableReceipt:
        return self.acquire_obligation(ref, obligation_id, deadline)

    @staticmethod
    def _queue_pin(pipe : Any, key : str, seconds : int, blob_ttl : int, set_ttl : int) -> None:
        '''
        Queue the EXPIREs that pin an object to a deadline ``seconds`` away, inside
        an open ``MULTI``. ``EXPIRE … GT`` (Redis ≥ 7.0) only ever lengthens a TTL
        and leaves a key without one alone. A set the ``SADD`` is creating has no
        TTL yet, so ``GT`` would leave it immortal: it gets the object's own life
        (or the deadline, if longer) outright instead.
        '''
        if seconds > 0:
            pipe.expire(key, seconds, gt = True)
            pipe.expire(metadata_key(key), seconds, gt = True)
        if set_ttl == -2:
            if blob_ttl >= 0:
                pipe.expire(obligation_key(key), max(seconds, blob_ttl))
        elif seconds > 0:
            pipe.expire(obligation_key(key), seconds, gt = True)

    def release_obligation(self, ref : ImmutablePayloadRef, obligation_id : str,
                           completion_receipt : str) -> ReleaseReceipt:
        '''
        The ``BLOB-14`` step 2 transaction. Idempotent by ``(obligation_id, generation)``:
        releasing an id that is not in the set changes nothing and reports the
        truthful remaining count; a release after the object is gone finds no set
        and applies the RFC 0002 fallback, which creates nothing.

        - Returns: a receipt whose ``stale`` means the generation did not match and \
            nothing was touched; ``unknown`` means the transaction was sent but its \
            reply was lost, or the keys changed under every attempt — retry, it is \
            safe; ``remaining`` is None on the fallback path (counter semantics keep \
            no per-reader record).
        '''
        faults.barrier('obligation.release.before', key = ref.key, obligation_id = obligation_id)
        receipt = self._release(ref, obligation_id)
        hit = faults.barrier('obligation.release.after', key = ref.key, obligation_id = obligation_id)
        if hit.drop_response:
            return ReleaseReceipt(ref, obligation_id, None, False, False, unknown = True)
        return receipt

    def _release(self, ref : ImmutablePayloadRef, obligation_id : str) -> ReleaseReceipt:
        key, obl, meta = ref.key, obligation_key(ref.key), metadata_key(ref.key)
        for _ in range(TRANSACTION_RETRIES):
            sent = False
            try:
                with self._client.pipeline() as pipe:
                    pipe.watch(obl, meta)
                    members = {_text(m) for m in pipe.smembers(obl)}
                    stored = pipe.hget(meta, 'generation')
                    generation = _text(stored) if stored is not None else None
                    if ref.generation and generation is not None and generation != ref.generation:
                        pipe.unwatch()
                        logger.info('stale release of %s on %s: generation %s != stored %s',
                                    obligation_id, key, ref.generation, generation)
                        return ReleaseReceipt(ref, obligation_id, len(members), False, True)
                    if not members:
                        # No obligation set (expired, evicted, reclaimed, or an RFC 0002
                        # publisher): never create one, never delete on its absence.
                        pipe.unwatch()
                        return self._release_counter(ref, obligation_id)
                    remaining = members - {obligation_id}
                    # The blob is deleted only on a *verified* generation match. With the
                    # metadata gone the set is still honoured, but the bytes are left to
                    # their TTL rather than deleted on an identity nobody can confirm.
                    reclaim = not remaining and generation is not None
                    pipe.multi()
                    pipe.srem(obl, obligation_id)
                    if reclaim:
                        pipe.unlink(key, meta, obl)
                    sent = True
                    pipe.execute()
                if reclaim:
                    logger.info('reclaimed %s: last obligation %s released', key, obligation_id)
                return ReleaseReceipt(ref, obligation_id, len(remaining), reclaim, False)
            except self._errors.WatchError as e:
                if sent and self._is_transport(e):
                    return self._unknown(ref, obligation_id, f'reply to EXEC lost: {e.__context__!r}')
                self._yield()   # a watched key changed (or expired): re-read and retry
                continue
            except self._transient as e:
                if sent:
                    return self._unknown(ref, obligation_id, f'{type(e).__name__}: {e}')
                continue
        return self._unknown(ref, obligation_id, f'the records of {key} changed under all '
                                                 f'{TRANSACTION_RETRIES} attempts')

    def _release_counter(self, ref : ImmutablePayloadRef, obligation_id : str) -> ReleaseReceipt:
        '''
        RFC 0002 ``BLOB-6`` counter semantics, as ``RedisBlobStore.release`` but
        without its hole: ``WATCH`` on the counter makes "the counter expired between
        the existence check and the decrement" abort the transaction instead of
        re-creating the counter at ``-1`` and deleting a blob another reader needs.
        No counter means a TTL-only blob, and nothing is created or deleted.
        '''
        key, counter = ref.key, counter_key(ref.key)
        for _ in range(TRANSACTION_RETRIES):
            sent = False
            try:
                with self._client.pipeline() as pipe:
                    pipe.watch(counter)
                    current = pipe.get(counter)
                    if current is None:
                        pipe.unwatch()
                        return ReleaseReceipt(ref, obligation_id, None, False, False)
                    pipe.multi()
                    pipe.decr(counter)
                    sent = True
                    left = int(pipe.execute()[0])
                if left > 0:
                    return ReleaseReceipt(ref, obligation_id, None, False, False)
                # Two single-key UNLINKs, as RedisBlobStore.release: the counter is not
                # hash-tagged onto the blob, so one multi-key call would be cross-slot
                # on Redis Cluster.
                self._client.unlink(key)
                self._client.unlink(counter)
                logger.info('reclaimed %s: RFC 0002 counter reached %d', key, left)
                return ReleaseReceipt(ref, obligation_id, None, True, False)
            except self._errors.WatchError as e:
                if sent and self._is_transport(e):
                    return self._unknown(ref, obligation_id, f'reply to EXEC lost: {e.__context__!r}')
                continue
            except self._transient as e:
                if sent:
                    return self._unknown(ref, obligation_id, f'{type(e).__name__}: {e}')
                continue
        return self._unknown(ref, obligation_id, f'the counter of {key} changed under all '
                                                 f'{TRANSACTION_RETRIES} attempts')

    @staticmethod
    def _yield() -> None:
        '''A short, jittered pause between optimistic-transaction attempts, so contenders on one key spread out.'''
        time.sleep(random.uniform(0.0, _RETRY_BACKOFF_SECONDS))

    def _is_transport(self, error : BaseException) -> bool:
        '''
        True when a ``WatchError`` stands in for a transport failure: redis-py raises
        it from inside the ``except`` that caught the ``ConnectionError``/``TimeoutError``
        (client.py:2033-2037 and 1801-1805, 8.0.1), so the original is its ``__context__``.
        A genuine "watched variable changed" has none.
        '''
        return isinstance(error.__context__, self._transient)

    @staticmethod
    def _unknown(ref : ImmutablePayloadRef, obligation_id : str, reason : str) -> ReleaseReceipt:
        logger.warning('release of %s on %s has an unknown outcome (%s); the caller retries',
                       obligation_id, ref.key, reason)
        return ReleaseReceipt(ref, obligation_id, None, False, False, unknown = True)

    # -- read ---------------------------------------------------------------------------------

    def read(self, ref : ImmutablePayloadRef, reader : Optional[str] = None) -> ReadOutcome:
        '''
        ``GET`` the bytes and verify them against the ref's digest, or the metadata's
        when the ref carries none (``BLOB-15``). ``reader`` only labels diagnostics.
        '''
        try:
            faults.barrier('payload.read.before', key = ref.key, reader = reader)
        except (TransientError, ConnectionError, TimeoutError) as e:
            return TransientFailure(ref, f'{type(e).__name__}: {e}')
        try:
            data = self._client.get(ref.key)
            expected = ref.digest
            if not expected and data is not None:
                stored = self._client.hget(metadata_key(ref.key), 'digest')
                expected = _text(stored) if stored is not None else ''
        except self._transient as e:
            return TransientFailure(ref, f'{type(e).__name__}: {e}')
        if data is None:
            return Missing(ref)
        payload = bytes(data)
        if expected:
            actual = hashlib.sha256(payload).hexdigest()
            if actual != expected:
                logger.warning('payload %s is corrupt: digest %s, expected %s', ref.key, actual, expected)
                return Corrupt(ref, expected, actual)
        faults.barrier('payload.read.after', key = ref.key, reader = reader)
        return PayloadBytes(ref, payload, bool(expected))

    # -- reconciliation / inventory ---------------------------------------------------------------

    def reconcile(self, ledger : ObligationLedger, operation_id : str) -> ReclamationObservation:
        '''
        ``BLOB-14`` step 4. For a key the ledger lists, its tuple is the whole truth:
        every other obligation is cancelled. For a key it does not list only
        ``intent/*`` obligations are cancelled — reader obligations belong to
        readers this ledger may not see, and leaking until TTL is the safe error.
        An object is reclaimed when nothing remains; one with no set at all is
        reclaimed only past ``orphan_grace_seconds`` and only if it has metadata to
        date it and no RFC 0002 counter. A scan that failed reports its pattern in
        ``unknown``; a key that could not be read or kept changing is reported there
        by name.
        '''
        required = {key: tuple(ids) for key, ids in ledger.required_obligations().items()}
        now = self._clock()
        reclaimed : list[str] = []
        retained : list[str] = []
        unknown_keys : list[str] = []
        try:
            candidates = self._candidate_keys()
        except self._transient as e:
            logger.warning('reconcile %s: inventory scan failed (%s: %s)', operation_id, type(e).__name__, e)
            return ReclamationObservation((), (), (f'{BLOB_KEY_PREFIX}*',))
        for key in candidates:
            try:
                fate = self._reconcile_one(key, required, now)
            except self._transient as e:
                logger.warning('reconcile %s: %s unreadable (%s: %s)', operation_id, key, type(e).__name__, e)
                unknown_keys.append(key)
            except TransientError as e:
                logger.warning('reconcile %s: %s (%s)', operation_id, key, e)
                unknown_keys.append(key)
            else:
                if fate == 'reclaimed':
                    reclaimed.append(key)
                elif fate == 'retained':
                    retained.append(key)
        logger.info('reconcile %s: reclaimed=%d retained=%d unknown=%d', operation_id,
                    len(reclaimed), len(retained), len(unknown_keys))
        return ReclamationObservation(tuple(reclaimed), tuple(retained), tuple(unknown_keys))

    def _reconcile_one(self, key : str, required : Mapping[str, tuple[str, ...]], now : float) -> str:
        '''
        One object's fate — ``'reclaimed'``, ``'retained'`` or ``'gone'`` (already
        absent) — decided and applied under ``WATCH`` so a concurrent acquire or
        release aborts the decision instead of racing it.
        '''
        obl, meta, counter = obligation_key(key), metadata_key(key), counter_key(key)
        for _ in range(TRANSACTION_RETRIES):
            try:
                with self._client.pipeline() as pipe:
                    pipe.watch(obl, meta)
                    members = {_text(m) for m in pipe.smembers(obl)}
                    record = {_text(k): _text(v) for k, v in pipe.hgetall(meta).items()}
                    if not pipe.exists(key):
                        pipe.unwatch()
                        return 'gone'
                    if key in required:
                        cancel = members - set(required[key])
                    else:
                        cancel = {m for m in members if m.startswith(INTENT_PREFIX)}
                    remaining = members - cancel
                    if remaining:
                        if cancel:
                            pipe.multi()
                            pipe.srem(obl, *sorted(cancel))
                            pipe.execute()
                            logger.info('reconcile: cancelled %s on %s', sorted(cancel), key)
                        else:
                            pipe.unwatch()
                        return 'retained'
                    if not members and not self._orphan(record, counter, now, pipe):
                        pipe.unwatch()
                        return 'retained'
                    pipe.multi()
                    if cancel:
                        pipe.srem(obl, *sorted(cancel))
                    pipe.unlink(key, meta, obl)
                    pipe.execute()
                    logger.info('reconcile: reclaimed %s (cancelled %s)', key, sorted(cancel))
                    return 'reclaimed'
            except self._errors.WatchError:
                self._yield()
                continue
        raise TransientError(f'{key} changed under all {TRANSACTION_RETRIES} reconciliation attempts',
                             remedy = 'Re-run reconcile; the object stays until then.', key = key)

    def _orphan(self, record : Mapping[str, str], counter : str, now : float, pipe : Any) -> bool:
        '''
        Whether a blob with no obligation set may be reclaimed. Without metadata its
        age is unknowable and it is TTL-only by contract (an RFC 0002 publisher, or a
        put that never got its metadata); with an RFC 0002 counter its readers own it;
        otherwise it is an interrupted put once older than the grace.
        '''
        if not record:
            return False
        if pipe.exists(counter):
            return False
        try:
            age = now - float(record['created_at'])
        except (KeyError, TypeError, ValueError):
            return False
        return age >= self._grace

    def _candidate_keys(self) -> list[str]:
        '''Every blob key that has bytes or an obligation set, from two ``SCAN``s (never ``KEYS``).'''
        keys : set[str] = set()
        for pattern in (f'{BLOB_KEY_PREFIX}*', f'{OBLIGATION_KEY_PREFIX}*'):
            for name in self._scan(pattern):
                inner = blob_key_of(name)
                keys.add(name if inner is None else inner)
        return sorted(keys)

    def _scan(self, pattern : str) -> list[str]:
        found : set[str] = set()   # SCAN may return a key more than once
        cursor = 0
        while True:
            cursor, batch = self._client.scan(cursor = cursor, match = pattern, count = SCAN_COUNT)
            found.update(_text(raw) for raw in batch)
            if int(cursor) == 0:
                return sorted(found)

    def inventory(self) -> Observation[tuple[ImmutablePayloadRef, ...]]:
        try:
            refs = tuple(self.ref_for_key(key) for key in self._scan(f'{BLOB_KEY_PREFIX}*'))
        except self._transient as e:
            return unknown('api', f'{type(e).__name__}: {e}')
        except TransientError as e:
            return unknown('api', str(e))
        return known(refs)

    def ref_for_key(self, key : str) -> ImmutablePayloadRef:
        '''
        The ref for a wire key, completed from ``vf-blobmeta-{key}``; the ABC default
        (unverifiable, unfenced) when there is no metadata.

        - Raises:
            - TransientFailure (core): the metadata could not be read; the caller retries.
        '''
        try:
            raw = self._client.hgetall(metadata_key(key))
        except self._transient as e:
            raise TransientError(f'metadata of {key} could not be read: {type(e).__name__}: {e}',
                                 remedy = 'Retry; the payload store was unreachable or slow.', key = key) from e
        if not raw:
            return super().ref_for_key(key)
        record = {_text(k): _text(v) for k, v in raw.items()}
        try:
            size = int(record.get('size') or 0)
        except ValueError:
            size = 0
        return ImmutablePayloadRef(STORE_ID, key, size, record.get('digest', ''), record.get('generation', ''),
                                   record.get('content_id', ''))
