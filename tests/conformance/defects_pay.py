'''
Negative controls for the PAY family: one reproduction per reviewed defect,
applied as a monkeypatch (or handed to an oracle as the defective collaborator),
so the paired conformance oracle can be shown to *fail* against it. A case is
only evidence if its oracle would have caught the bug it was written for; the
``negative_control(of = ID)`` test in each ``test_pay_*`` module asserts exactly
that through ``defects.detects``.

Kept apart from ``defects.py`` (the MSG/ALLOC/RUN reproductions) so the two can
grow independently; ``defects.detects`` is the shared verdict.
'''
from __future__ import absolute_import, division, print_function

from typing import Any, Optional

import pytest

from videoflow.backends.memory.payload import MemoryPayloadStore
from videoflow.backends.payload import ReleaseReceipt
from videoflow.messaging import nats_messenger
from videoflow.messaging.nats_messenger import NATSMessenger, _AckHandle
from videoflow.wire.serialization import peek_envelope

# -- PAY-002: every store failure is poison ---------------------------------------------------

def poison_on_any_store_failure(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The reviewed messenger caught every exception on decode and terminated the
    delivery: a ConnectionError from the store looked exactly like malformed bytes.'''
    monkeypatch.setattr(nats_messenger, '_is_transient_store_failure', lambda error: False)


# -- PAY-003: undecodable payloads terminated without a record -----------------------------------

def silent_terminal_discard(monkeypatch : pytest.MonkeyPatch) -> None:
    '''Before ``DELIV-15`` a missing or corrupt payload was terminated against nothing:
    no dead letter, no drop count — its only trace was its own disappearance.'''
    def discard(self : NATSMessenger, parent_name : str, delivery : Any, error : BaseException,
                handle : Optional[_AckHandle] = None) -> None:
        handle = handle or _AckHandle(delivery.token, self, raw = delivery.envelope_bytes)
        handle.term(f'ledger:{self._node.name}/terminal:silent')
    monkeypatch.setattr(NATSMessenger, '_discard_undecodable', discard)


# -- PAY-019: the payload fetched inside the broker's delivery loop ---------------------------------

def hydrate_on_the_loop(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The reviewed pull loop decoded — and so fetched — the payload on the NATS
    loop thread before ownership was decided: one slow GET starved every
    heartbeat, keepalive and control message the client owed the broker.'''
    real = NATSMessenger._admit_on_loop

    def admit(self : NATSMessenger, delivery : Any) -> bool:
        try:
            ref = peek_envelope(delivery.envelope_bytes).get('blob_ref')
        except Exception:  # noqa: BLE001 — classified on the receiving side
            return real(self, delivery)
        if ref is not None and self._blob_store is not None:
            try:
                self._blob_store.get(ref)                    # blocking I/O on the loop thread
            except Exception:  # noqa: BLE001 — the reviewed loop swallowed it too
                pass
        return real(self, delivery)
    monkeypatch.setattr(NATSMessenger, '_admit_on_loop', admit)


# -- PAY-001: an inline threshold that never meets the broker ----------------------------------------

def unnegotiated_threshold(monkeypatch : pytest.MonkeyPatch) -> None:
    '''``VIDEOFLOW_MAX_INLINE_PAYLOAD_BYTES`` used as configured: a threshold above
    the broker's ``max_payload`` inlines envelopes the broker then refuses.'''
    monkeypatch.setattr(NATSMessenger, '_negotiate_inline_threshold', lambda self, max_payload: None)


# -- PAY-020: capacity estimated from the compressed source ---------------------------------------------

def compressed_size_estimator(frame : Any, compressed_size : int) -> int:
    '''The reviewed sizing assumed a frame costs what its JPEG cost: the 1 MB the
    reader ingested, not the 6.2 MB the decoded array occupies on the wire.'''
    return compressed_size


# -- PAY-004: release on any settlement --------------------------------------------------------------

def release_on_any_settlement(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The reviewed handle: ``msg.ack()`` returned, so the blob was released — a
    client-side send result taken for a confirmed durable settlement.'''
    from videoflow.backends.messaging import Completed

    def ack(self : _AckHandle) -> None:
        if self._resolved:
            return
        self._resolved = True
        self._m._forget_handle(self)
        self._m._backend.settle(self._token, Completed(), f'ack:{self._token.message_id}:{self._token.attempt}')
        self._m._release_after_settlement(self._blob_ref, f'ack:{self._token.message_id}:{self._token.attempt}')
    monkeypatch.setattr(_AckHandle, 'ack', ack)


# -- PAY-005: one decrement-only counter ----------------------------------------------------------------

def decrementing_release(monkeypatch : pytest.MonkeyPatch) -> None:
    '''RFC 0002's counter as a release: every call takes *one* share, whoever's it
    is, and deletes at zero — so X's redelivered handle spends Y's share.'''
    def release(self : MemoryPayloadStore, ref : Any, obligation_id : str, completion_receipt : str) -> ReleaseReceipt:
        with self._lock:
            obj = self._objects.get(ref.key)
            if obj is None:
                return ReleaseReceipt(ref, obligation_id, None, False, False)
            if obj.obligations:
                obj.obligations.pop(next(iter(obj.obligations)))
            remaining = len(obj.obligations)
            reclaimed = remaining == 0
            if reclaimed:
                del self._objects[ref.key]
                self._by_content.pop((obj.content_id, obj.digest), None)
            return ReleaseReceipt(ref, obligation_id, remaining, reclaimed, False)
    monkeypatch.setattr(MemoryPayloadStore, 'release_obligation', release)


# -- PAY-008: EXISTS, then a blind decrement --------------------------------------------------------------

def exists_then_decrement(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The reviewed release: check that the record exists, then (after whatever
    happened in between) decrement it — a record that vanished in the gap is
    re-created below zero and the object still owned by another reader deleted.'''
    from videoflow.backends import faults

    def release(self : MemoryPayloadStore, ref : Any, obligation_id : str, completion_receipt : str) -> ReleaseReceipt:
        with self._lock:
            obj = self._objects.get(ref.key)
            existed = obj is not None and obj.has_set                    # EXISTS(refcount)
        faults.barrier('obligation.release.before', key = ref.key, obligation_id = obligation_id)
        with self._lock:
            obj = self._objects.get(ref.key)
            if obj is None or not existed:
                return ReleaseReceipt(ref, obligation_id, None, False, False)
            obj.obligations.pop(obligation_id, None)
            count = len(obj.obligations) - (0 if obj.has_set else 1)     # DECR on a vanished record: -1
            reclaimed = count <= 0
            if reclaimed:
                del self._objects[ref.key]
                self._by_content.pop((obj.content_id, obj.digest), None)
        faults.barrier('obligation.release.after', key = ref.key, obligation_id = obligation_id)
        return ReleaseReceipt(ref, obligation_id, count, reclaimed, False)
    monkeypatch.setattr(MemoryPayloadStore, 'release_obligation', release)


# -- PAY-021: shares from the initial replica count ---------------------------------------------------------

def count_based_membership(spec : Any, specs : Any) -> list:
    '''The reviewed compiler: one share per *replica* of every child, competing or
    not — a scale-down leaves shares no worker will ever release, and a
    replacement worker's release names nothing in the set.'''
    ids = []
    for child in specs:
        if spec.name not in child.parents:
            continue
        ids.extend(f'{child.name}/p{i}' for i in range(child.nb_tasks))
    return ids


# -- PAY-006 / PAY-012 / PAY-013: a reconciler that never cancels a reader's obligation ----------------------

def leaking_reconciler(monkeypatch : pytest.MonkeyPatch) -> None:
    '''No reconciliation record: an obligation is released by its reader or never
    — a crashed reader's share, an evicted message's share and a rejected
    publication's share all leak until the TTL backstop.'''
    from videoflow.backends.payload import ReclamationObservation

    def reconcile(self : MemoryPayloadStore, ledger : Any, operation_id : str) -> ReclamationObservation:
        with self._lock:
            self._sweep(self._clock.now())
            return ReclamationObservation((), tuple(self._objects), ())
    monkeypatch.setattr(MemoryPayloadStore, 'reconcile', reconcile)


def intent_only_resolution(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The messenger before the fix released only ``intent/<id>`` when a send
    resolved: a definitely refused publication, or a retry the broker deduplicated
    against an earlier attempt, left its reader shares on an object no message
    names — reclaimable only by a reconciler that knew the publication's fate.'''
    from videoflow.messaging.nats_messenger import NATSMessenger
    real = NATSMessenger._resolve_intent

    def resolve(self : NATSMessenger, publication_id : str, ref : Any, outcome : Any,
                orphaned : bool = False) -> None:
        real(self, publication_id, ref, outcome, False)
    monkeypatch.setattr(NATSMessenger, '_resolve_intent', resolve)


# -- PAY-007: a publisher whose objects carry no intent ------------------------------------------------

def intent_less_bridge(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The reviewed publisher: serialize, offload, publish — nothing ties the
    object to the publication, so an object whose publish never happened is
    indistinguishable from one a reader is about to fetch.'''
    from videoflow.backends.payload import RetentionContract
    from videoflow.backends.payload_bridge import PayloadStoreBlobBridge

    def put(self : PayloadStoreBlobBridge, data : bytes, readers : Any, ttl_seconds : int) -> str:
        contract = RetentionContract(ttl_seconds = ttl_seconds, horizon_seconds = self._horizon,
                                     durable_required = self._durable, obligations = tuple(readers))
        ref = self._store.put(data, self._content_id(), contract)
        self.last_ref = ref
        return ref.key
    monkeypatch.setattr(PayloadStoreBlobBridge, '_put', put)


# -- PAY-015: routing that hydrates ---------------------------------------------------------------------

def hydrating_router(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The reviewed ``dlq replay`` decoded each dead letter *with* its payload to
    learn the producer, so an offloaded entry failed to decode without a store
    and was skipped as undecodable.'''
    from videoflow.wire import serialization
    real = serialization.decode_envelope

    def decode(buf : bytes, blob_store : Any = None, resolve_blobs : bool = True) -> Any:
        return real(buf, blob_store = blob_store, resolve_blobs = True)
    monkeypatch.setattr(serialization, 'decode_envelope', decode)


# -- PAY-016: a replay addressed to nobody in particular ---------------------------------------------------

UNSCOPED_REPLAY = 'unscoped'
'''Passed to the PAY-016 replayer: publish on the parent's subject with a fresh id
and no ``VF-Replay-Target`` — every child of the parent reprocesses it.'''


# -- PAY-018: the payload fetched before ownership is decided ----------------------------------------------

def hydrate_before_ownership(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The reviewed pull loop decoded the whole envelope — payload included —
    before asking ``_owns``: every replica fetched every frame to discard 7/8.'''
    real = NATSMessenger._owns

    def owns(self : NATSMessenger, entry : Any) -> bool:
        if entry.blob_ref is not None and self._blob_store is not None:
            self._blob_store.get(entry.blob_ref)
        return real(self, entry)
    monkeypatch.setattr(NATSMessenger, '_owns', owns)


# -- PAY-009: a fixed TTL, whatever is outstanding ---------------------------------------------------------

def fixed_ttl_store(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The reviewed retention: 3600 s realtime / 86400 s batch, minted at put and
    never looked at again — no pin, no extension, no admission against the
    horizon the flow promised.'''
    from videoflow.backends import payload, payload_bridge

    def sweep(self : MemoryPayloadStore, now : float) -> None:
        for key, obj in list(self._objects.items()):
            if obj.expires_at is not None and obj.expires_at <= now:
                del self._objects[key]
                self._by_content.pop((obj.content_id, obj.digest), None)
    monkeypatch.setattr(MemoryPayloadStore, '_sweep', sweep)
    real_put = MemoryPayloadStore.put

    def put(self : MemoryPayloadStore, data : bytes, content_id : str, contract : Any) -> Any:
        ref = real_put(self, data, content_id, contract)
        with self._lock:
            obj = self._objects.get(ref.key)
            if obj is not None and contract.ttl_seconds > 0:
                obj.expires_at = obj.created_at + contract.ttl_seconds          # the TTL alone decides
        return ref
    monkeypatch.setattr(MemoryPayloadStore, 'put', put)
    monkeypatch.setattr(payload, 'admit_retention', lambda contract: None)
    monkeypatch.setattr(payload_bridge, 'admit_retention', lambda contract: None)


# -- PAY-010: a cache that says it is durable -----------------------------------------------------------------

def lying_capabilities(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The shipped compose/infra Redis as the reviewed deploy path described it:
    a "store" — durable in name, ``volatile-lru`` with persistence off in fact.'''
    from videoflow.backends.capabilities import PayloadCapabilities
    from videoflow.backends.outcomes import known

    def capabilities(self : MemoryPayloadStore) -> PayloadCapabilities:
        return PayloadCapabilities(self._id, durable = known(True), evictable = known(False),
                                   atomic_multikey = known(True), max_object_bytes = self._max_bytes,
                                   reader_identities = True)
    monkeypatch.setattr(MemoryPayloadStore, 'capabilities', capabilities)


# -- PAY-014: a dead letter that pins nothing --------------------------------------------------------------------

def unpinned_dead_letter(monkeypatch : pytest.MonkeyPatch) -> None:
    '''The reviewed DLQ kept the raw envelope for seven days and its 1 h / 24 h
    blob for nothing: the pin is acknowledged and never recorded.'''
    from videoflow.backends.payload import DurableReceipt

    def acquire(self : MemoryPayloadStore, ref : Any, obligation_id : str, deadline : float) -> DurableReceipt:
        return DurableReceipt(ref, obligation_id, deadline)
    monkeypatch.setattr(MemoryPayloadStore, 'acquire_obligation', acquire)
