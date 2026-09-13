'''
A ``BlobStore`` façade over a ``PayloadStore``, so the wire codec's offload path
(``encode_envelope(blob_store = ...)`` / ``hydrate_message(decoded, blob_store)``)
can drive an obligation-keeping store without the codec learning about
obligations, contracts or typed read outcomes.

The codec sees the RFC 0002 interface it always has: ``put_with_readers`` returns
the wire key, ``get`` returns bytes or raises. Behind it the bridge translates:

- a put becomes ``PayloadStore.put`` under a ``RetentionContract`` naming the
  reader obligations the publisher acquires up front (``BLOB-14`` step 1) plus
  the publisher's own ``intent/<publication_id>`` for the send in flight; the
  ref of the last put is kept so the messenger can release that intent once the
  publication's outcome is known. The reader set is the bridge's
  (``VF_BLOB_READER_IDS``), whichever of the codec's two put paths is taken:
  the RFC 0002 reader *count* chooses between them and means nothing here. With
  no reader ids at all the object is TTL-only — no obligations, not even the
  intent — because a lone intent released on the PubAck would reclaim an object
  its (unidentified) readers have yet to fetch (``ENV-12``: ids unset ⇒ count
  semantics, else TTL-only);
- a read's typed outcome becomes the error the messenger's ladder already
  understands: ``TransientFailure`` (retry — never malformed bytes, ``BLOB-15``),
  and ``DecodeError`` for ``Missing``/``Corrupt`` (terminal for that delivery,
  dead-lettered with a ``VF-Error`` naming the ref and the reason);
- ``release`` is a no-op — obligations are released *by reader id* through the
  store, after a confirmed settlement, which is the messenger's decision.
'''
from __future__ import absolute_import, division, print_function

from typing import Callable, Optional, Sequence

from ..core.errors import DecodeError, TransientFailure
from ..wire.serialization import DEFAULT_BLOB_TTL_SECONDS, BlobStore
from .payload import (
    Corrupt,
    ImmutablePayloadRef,
    Missing,
    PayloadBytes,
    PayloadStore,
    RetentionContract,
    admit_retention,
)
from .payload import TransientFailure as TransientRead


class PayloadStoreBlobBridge(BlobStore):
    '''
    - Arguments:
        - store: the obligation-keeping store.
        - obligations: the reader obligation ids every put acquires up front \\
            (``VF_BLOB_READER_IDS``: ``<child>`` per competing child, \\
            ``<child>/p<i>`` per partitioned replica).
        - horizon_seconds: the recovery horizon the flow promised; a shorter \\
            TTL is the admission's problem, recorded on the contract here.
        - durable_required: whether the channel's profile needs the bytes to \\
            survive the store's own failure model.
        - content_id: the logical content identity of the message being \\
            published, evaluated at put time.
        - intent: the ``intent/<publication_id>`` obligation for the send in \\
            flight, evaluated at put time; None when there is none.
    '''
    def __init__(self, store : PayloadStore, obligations : Sequence[str], horizon_seconds : int,
                 durable_required : bool, content_id : Callable[[], str],
                 intent : Callable[[], Optional[str]] = lambda: None) -> None:
        self._store = store
        self._obligations = tuple(obligations)
        self._horizon = horizon_seconds
        self._durable = durable_required
        self._content_id = content_id
        self._intent = intent
        #: The ref of the most recent put — what the publisher releases its intent on.
        self.last_ref : Optional[ImmutablePayloadRef] = None

    @property
    def store(self) -> PayloadStore:
        return self._store

    def put(self, data : bytes, ttl_seconds : int = DEFAULT_BLOB_TTL_SECONDS) -> str:
        return self._put(data, self._obligations, ttl_seconds)

    def put_with_readers(self, data : bytes, readers : int, ttl_seconds : int = DEFAULT_BLOB_TTL_SECONDS) -> str:
        return self._put(data, self._obligations, ttl_seconds)

    def _put(self, data : bytes, readers : Sequence[str], ttl_seconds : int) -> str:
        intent = self._intent()
        # No identified readers: TTL-only, exactly as a count-less RFC 0002 put. An
        # intent alone would be released on the PubAck and reclaim the object first.
        obligations = tuple(readers) + ((intent,) if intent and readers else ())
        contract = RetentionContract(ttl_seconds = ttl_seconds, horizon_seconds = self._horizon,
                                     durable_required = self._durable, obligations = obligations)
        # Before a byte is written: a horizon the TTL cannot cover, and nothing
        # pinning the object, is a configuration error, not a publication (PAY-009).
        admit_retention(contract)
        ref = self._store.put(data, self._content_id(), contract)
        self.last_ref = ref
        return ref.key

    def get(self, ref : str) -> bytes:
        outcome = self._store.read(self._store.ref_for_key(ref))
        if isinstance(outcome, PayloadBytes):
            return outcome.data
        if isinstance(outcome, TransientRead):
            raise TransientFailure(f'payload {ref} could not be read: {outcome.reason}',
                                   remedy = 'The store is unreachable or slow; the delivery is retried.',
                                   ref = ref)
        if isinstance(outcome, Missing):
            raise DecodeError(f'payload {ref} is missing from the store',
                              remedy = 'The object expired or was evicted before its reader finished; check '
                                       'the store retention against the flow\'s recovery horizon.',
                              ref = ref, reason = 'missing')
        assert isinstance(outcome, Corrupt), outcome
        raise DecodeError(f'payload {ref} is corrupt: digest {outcome.actual_digest[:12]} != '
                          f'{outcome.expected_digest[:12]}',
                          remedy = 'The stored bytes do not match what was published; the object cannot be '
                                   'handed to a model as the original frame.',
                          ref = ref, reason = 'corrupt')

    def release(self, ref : str) -> None:
        return None
