'''
The in-memory ``PayloadStore``: objects with digests and generations, named
obligations instead of a counter, TTL and byte-budget physics under a fake clock.

Two tiers are modelled because the design package insists they be told apart:

- ``tier='durable'``: an object under obligation is never expired or evicted;
  acquiring an obligation extends the object's life to the obligation deadline;
  a put over budget is refused (backpressure) rather than admitted by eviction.
- ``tier='evictable'``: the Redis ``volatile-lru`` shape — every object has a TTL
  that fires regardless of obligations, and under memory pressure the
  least-recently-used TTL-bearing object is evicted *even if a reader still
  needs it*. That is the finding the reliable profile must reject; this model
  makes it observable.

Release is atomic and idempotent by ``(obligation_id, generation)``; a stale
generation cannot delete a newer object at the same key. An object whose
obligation *set* is absent — a put that died before attaching it, or a set that
expired — is never reclaimed by a release (``BLOB-14`` step 2: the missing set is
neither created nor treated as "everyone finished"); ``reconcile`` reclaims such
an orphan only once it is older than ``orphan_grace_seconds``, as the Redis store
does, so a put still in progress is not mistaken for a leak.

Barriers, in ``put``: ``payload.write.after`` fires once the bytes are stored and
before any obligation is attached; ``obligation.acquire.after`` fires once the
contract's obligations are attached. A crash between them leaves exactly the
counterless, TTL-only object the lifecycle promises (PAY-007).
'''
from __future__ import absolute_import, division, print_function

import hashlib
import threading
import uuid
from dataclasses import dataclass, field
from typing import Any, Mapping

from ...core.errors import ResourceUnavailable
from ...core.errors import TransientFailure as TransientError
from .. import faults
from ..capabilities import PayloadCapabilities
from ..observation import ObservationLog
from ..outcomes import Observation, known, unknown
from ..payload import (
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
from .clock import FakeClock

TIER_DURABLE = 'durable'
TIER_EVICTABLE = 'evictable'

@dataclass
class _Object:
    key : str
    data : bytes
    digest : str
    size : int
    generation : str
    content_id : str
    created_at : float
    expires_at : float | None
    obligations : dict[str, float] = field(default_factory = dict)
    last_access : float = 0.0
    corrupted : bool = False
    #: Whether an obligation set exists at all (the Redis SET key): False for a
    #: put that never attached one and after the set expired; an empty set does
    #: not exist, exactly as on the server.
    has_set : bool = False

class MemoryPayloadStore(PayloadStore):
    '''
    - Arguments:
        - tier: ``'durable'`` or ``'evictable'`` (see the module docstring).
        - max_bytes: byte budget; None for unlimited.
        - forward_unchanged: when True, ``put`` of bytes whose ``content_id`` and \
            digest already exist returns the existing reference (reference \
            forwarding: an unchanged frame traverses metadata stages as one object).
        - orphan_grace_seconds: how long an object with no obligation set is left \
            alone by ``reconcile`` — a put interrupted before its obligations were \
            attached must not be reclaimed while its publisher may still be about \
            to reference it.
    '''
    def __init__(self, clock : FakeClock | None = None, tier : str = TIER_DURABLE,
                 max_bytes : int | None = None, forward_unchanged : bool = True,
                 log : ObservationLog | None = None, store_id : str = 'memory',
                 orphan_grace_seconds : float = 60.0) -> None:
        if tier not in (TIER_DURABLE, TIER_EVICTABLE):
            raise ValueError(f'tier must be durable or evictable, got {tier!r}')
        self._clock = clock or FakeClock()
        self._tier = tier
        self._max_bytes = max_bytes
        self._forward = forward_unchanged
        self._log = log
        self._id = store_id
        self._grace = float(orphan_grace_seconds)
        self._objects : dict[str, _Object] = {}
        self._by_content : dict[tuple[str, str], str] = {}
        self._lock = threading.RLock()
        self._inventory_failure : str | None = None
        self._read_failures : list[str] = []
        self.metrics : dict[str, Any] = {'put_count': 0, 'put_bytes': 0, 'get_count': {}, 'get_bytes': {},
                                         'evicted': 0, 'expired': 0}

    # -- capabilities ---------------------------------------------------------------

    def capabilities(self) -> PayloadCapabilities:
        return PayloadCapabilities(self._id, durable = known(self._tier == TIER_DURABLE),
                                   evictable = known(self._tier == TIER_EVICTABLE),
                                   atomic_multikey = known(True), max_object_bytes = self._max_bytes,
                                   reader_identities = True)

    # -- put -------------------------------------------------------------------------

    def put(self, data : bytes, content_id : str, contract : RetentionContract) -> ImmutablePayloadRef:
        digest = hashlib.sha256(data).hexdigest()
        faults.barrier('payload.write.before', content_id = content_id, size = len(data))
        with self._lock:
            now = self._clock.now()
            self._sweep(now)
            existing_key = self._by_content.get((content_id, digest)) if self._forward else None
            if existing_key is not None and existing_key in self._objects:
                obj = self._objects[existing_key]
                for obligation in contract.obligations:
                    obj.obligations[obligation] = max(obj.obligations.get(obligation, 0.0),
                                                      now + contract.horizon_seconds)
                if contract.obligations:
                    obj.has_set = True
                    if self._tier == TIER_DURABLE and obj.expires_at is not None:
                        obj.expires_at = max(obj.expires_at, max(obj.obligations.values()))
                self._emit('put', op_id = content_id, status = 'forwarded', key = obj.key)
                return self._ref(obj)
            if self._max_bytes is not None and self._bytes() + len(data) > self._max_bytes:
                if self._tier == TIER_EVICTABLE:
                    self._evict_until(len(data))
                if self._bytes() + len(data) > self._max_bytes:
                    raise ResourceUnavailable(
                        f'payload store budget of {self._max_bytes} bytes cannot admit {len(data)} more bytes',
                        remedy = 'Backpressure the producer, raise the budget, or release obligations.',
                        stored_bytes = self._bytes(), requested = len(data))
            key = f'vf-blob-{uuid.uuid4().hex}'
            expires = now + contract.ttl_seconds if contract.ttl_seconds > 0 else None
            obj = _Object(key, bytes(data), digest, len(data), uuid.uuid4().hex[:8], content_id, now, expires,
                          {}, now)
            self._objects[key] = obj
            self._by_content[(content_id, digest)] = key
            self.metrics['put_count'] += 1
            self.metrics['put_bytes'] += len(data)
            self._emit('put', op_id = content_id, key = key, payload_digest = digest, size = len(data))
        # The bytes are stored; the obligations are not yet. A crash here leaves a
        # counterless, TTL-only object (BLOB-14 step 1), which reconcile reclaims
        # past the orphan grace.
        faults.barrier('payload.write.after', key = key, content_id = content_id)
        if contract.obligations:
            with self._lock:
                deadline = now + contract.horizon_seconds
                obj.obligations = dict.fromkeys(contract.obligations, deadline)
                obj.has_set = True
                if self._tier == TIER_DURABLE and obj.expires_at is not None:
                    obj.expires_at = max(obj.expires_at, deadline)
            faults.barrier('obligation.acquire.after', key = key, obligation_id = ','.join(contract.obligations))
        return self._ref(obj)

    # -- obligations -----------------------------------------------------------------------

    def acquire_obligation(self, ref : ImmutablePayloadRef, obligation_id : str, deadline : float) -> DurableReceipt:
        with self._lock:
            obj = self._live(ref)
            if obj is None:
                raise LookupError(f'{ref.key} is not stored (expired, evicted or never written)')
            if obj.generation != ref.generation:
                raise LookupError(f'{ref.key} is a different generation now ({obj.generation} != {ref.generation})')
            obj.obligations[obligation_id] = max(obj.obligations.get(obligation_id, 0.0), deadline)
            obj.has_set = True
            if self._tier == TIER_DURABLE and obj.expires_at is not None and obj.expires_at < deadline:
                obj.expires_at = deadline
            self._emit('obligation', op_id = obligation_id, key = ref.key, status = 'acquired')
        faults.barrier('obligation.acquire.after', key = ref.key, obligation_id = obligation_id)
        return DurableReceipt(ref, obligation_id, deadline)

    def renew_obligation(self, ref : ImmutablePayloadRef, obligation_id : str, deadline : float) -> DurableReceipt:
        return self.acquire_obligation(ref, obligation_id, deadline)

    def release_obligation(self, ref : ImmutablePayloadRef, obligation_id : str,
                           completion_receipt : str) -> ReleaseReceipt:
        faults.barrier('obligation.release.before', key = ref.key, obligation_id = obligation_id)
        with self._lock:
            obj = self._objects.get(ref.key)
            if obj is None:
                receipt = ReleaseReceipt(ref, obligation_id, None, False, False)
            elif obj.generation != ref.generation:
                self._emit('obligation', op_id = obligation_id, key = ref.key, status = 'stale')
                receipt = ReleaseReceipt(ref, obligation_id, len(obj.obligations), False, True)
            elif not obj.has_set:
                # No obligation set (never attached, or expired): never create one,
                # never delete on its absence — the object is TTL-only (BLOB-14 step 2).
                self._emit('obligation', op_id = obligation_id, key = ref.key, status = 'no-set')
                receipt = ReleaseReceipt(ref, obligation_id, None, False, False)
            else:
                obj.obligations.pop(obligation_id, None)
                remaining = len(obj.obligations)
                reclaimed = False
                if remaining == 0:
                    del self._objects[ref.key]
                    self._by_content.pop((obj.content_id, obj.digest), None)
                    reclaimed = True
                self._emit('obligation', op_id = obligation_id, key = ref.key, status = 'released',
                           remaining = remaining, reclaimed = reclaimed)
                receipt = ReleaseReceipt(ref, obligation_id, remaining, reclaimed, False)
        hit = faults.barrier('obligation.release.after', key = ref.key, obligation_id = obligation_id)
        if hit.drop_response:
            return ReleaseReceipt(ref, obligation_id, None, False, False, unknown = True)
        return receipt

    # -- read ----------------------------------------------------------------------------

    def read(self, ref : ImmutablePayloadRef, reader : str | None = None) -> ReadOutcome:
        try:
            faults.barrier('payload.read.before', key = ref.key, reader = reader)
        except (TransientError, ConnectionError, TimeoutError) as e:
            return TransientFailure(ref, f'{type(e).__name__}: {e}')
        with self._lock:
            if self._read_failures:
                return TransientFailure(ref, self._read_failures.pop(0))
            obj = self._live(ref)
            if obj is None:
                self._emit('read', key = ref.key, reader = reader, status = 'missing')
                return Missing(ref)
            obj.last_access = self._clock.now()
            self.metrics['get_count'][reader] = self.metrics['get_count'].get(reader, 0) + 1
            self.metrics['get_bytes'][reader] = self.metrics['get_bytes'].get(reader, 0) + obj.size
            actual = hashlib.sha256(obj.data).hexdigest()
            if obj.corrupted or actual != ref.digest:
                self._emit('read', key = ref.key, reader = reader, status = 'corrupt')
                return Corrupt(ref, ref.digest, actual)
            data = obj.data
        faults.barrier('payload.read.after', key = ref.key, reader = reader)
        return PayloadBytes(ref, data, True)

    # -- reconciliation / inventory ----------------------------------------------------------

    def reconcile(self, ledger : ObligationLedger, operation_id : str) -> ReclamationObservation:
        '''
        For a key the ledger lists, its tuple is the whole truth: every other
        obligation is cancelled. For a key it does not list only ``intent/*``
        obligations are cancelled — reader obligations belong to readers this
        ledger may not see. An object is reclaimed when nothing remains of a set
        it had; one that never had a set (or whose set expired) is reclaimed only
        past ``orphan_grace_seconds``, as the Redis store does.
        '''
        required = ledger.required_obligations()
        reclaimed : list[str] = []
        retained : list[str] = []
        with self._lock:
            now = self._clock.now()
            self._sweep(now)
            for key, obj in list(self._objects.items()):
                if key in required:
                    cancel = {o for o in obj.obligations if o not in set(required[key])}
                else:
                    cancel = {o for o in obj.obligations if o.startswith('intent/')}
                for obligation in cancel:
                    del obj.obligations[obligation]
                if obj.obligations:
                    retained.append(key)
                    continue
                if not obj.has_set and now - obj.created_at < self._grace:
                    retained.append(key)         # a put that may still be in progress
                    continue
                del self._objects[key]
                self._by_content.pop((obj.content_id, obj.digest), None)
                reclaimed.append(key)
        self._emit('reconcile', op_id = operation_id, reclaimed = len(reclaimed), retained = len(retained))
        return ReclamationObservation(tuple(reclaimed), tuple(retained), ())

    def ref_for_key(self, key : str) -> ImmutablePayloadRef:
        with self._lock:
            obj = self._objects.get(key)
        if obj is None:
            return super().ref_for_key(key)
        return ImmutablePayloadRef('memory', obj.key, obj.size, obj.digest, obj.generation, obj.content_id)

    def inventory(self) -> Observation[tuple[ImmutablePayloadRef, ...]]:
        with self._lock:
            if self._inventory_failure:
                return unknown(self._inventory_failure, 'inventory injected to fail')
            self._sweep(self._clock.now())
            return known(tuple(self._ref(o) for o in self._objects.values()))

    # -- test hooks ---------------------------------------------------------------------------

    def corrupt(self, key : str) -> None:
        with self._lock:
            obj = self._objects[key]
            obj.data = obj.data[:-1] + bytes([obj.data[-1] ^ 0xFF]) if obj.data else b'\x00'
            obj.corrupted = True

    def delete(self, key : str) -> None:
        with self._lock:
            obj = self._objects.pop(key)
            self._by_content.pop((obj.content_id, obj.digest), None)

    def expire_obligation(self, key : str, obligation_id : str) -> None:
        '''Model one obligation record vanishing; an emptied set no longer exists, as on the server.'''
        with self._lock:
            obj = self._objects[key]
            obj.obligations.pop(obligation_id, None)
            if not obj.obligations:
                obj.has_set = False

    def expire_obligation_set(self, key : str) -> None:
        '''Model the whole obligation set expiring or being evicted while the bytes remain (PAY-008).'''
        with self._lock:
            obj = self._objects[key]
            obj.obligations = {}
            obj.has_set = False

    def truncate(self, key : str, size : int) -> None:
        '''Model a truncated object: the bytes are cut short, the metadata still names the original.'''
        with self._lock:
            obj = self._objects[key]
            obj.data = obj.data[:size]
            obj.corrupted = True

    def fail_reads(self, reasons : list[str]) -> None:
        with self._lock:
            self._read_failures.extend(reasons)

    def fail_inventory(self, reason : str | None) -> None:
        with self._lock:
            self._inventory_failure = reason

    def obligations(self, key : str) -> dict[str, float]:
        with self._lock:
            obj = self._objects.get(key)
            return {} if obj is None else dict(obj.obligations)

    def stored_bytes(self) -> int:
        with self._lock:
            return self._bytes()

    def object_count(self) -> int:
        with self._lock:
            return len(self._objects)

    # -- internals ------------------------------------------------------------------------------

    def _ref(self, obj : _Object) -> ImmutablePayloadRef:
        return ImmutablePayloadRef(self._id, obj.key, obj.size, obj.digest, obj.generation, obj.content_id)

    def _live(self, ref : ImmutablePayloadRef) -> _Object | None:
        self._sweep(self._clock.now())
        return self._objects.get(ref.key)

    def _bytes(self) -> int:
        return sum(o.size for o in self._objects.values())

    def _sweep(self, now : float) -> None:
        for key, obj in list(self._objects.items()):
            # The obligation set lapses as a whole at its latest deadline (the Redis
            # SET's EXPIRE): a dead-letter pin outlives its horizon by nothing.
            if obj.obligations and max(obj.obligations.values()) <= now:
                obj.obligations = {}
                obj.has_set = False
            if obj.expires_at is None or obj.expires_at > now:
                continue
            if self._tier == TIER_DURABLE and obj.obligations:
                continue
            del self._objects[key]
            self._by_content.pop((obj.content_id, obj.digest), None)
            self.metrics['expired'] += 1
            self._emit('expire', key = key, obligations = sorted(obj.obligations))

    def _evict_until(self, needed : int) -> None:
        candidates = sorted((o for o in self._objects.values() if o.expires_at is not None),
                            key = lambda o: o.last_access)
        for obj in candidates:
            if self._bytes() + needed <= (self._max_bytes or 0):
                break
            del self._objects[obj.key]
            self._by_content.pop((obj.content_id, obj.digest), None)
            self.metrics['evicted'] += 1
            self._emit('evict', key = obj.key, obligations = sorted(obj.obligations), reason = 'memory pressure')

    def _emit(self, kind : str, **fields : Any) -> None:
        if self._log is not None:
            self._log.emit(kind, **fields)

class StaticLedger(ObligationLedger):
    '''A ledger built from a mapping, for tests and simple runtimes.'''
    def __init__(self, required : Mapping[str, tuple[str, ...]]) -> None:
        self._required = dict(required)

    def required_obligations(self) -> Mapping[str, tuple[str, ...]]:
        return dict(self._required)
