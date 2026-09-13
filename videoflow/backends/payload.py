'''
The ``PayloadStore`` contract: where the image bytes are, and how long they must
survive.

The reviewed implementation kept one decrement-only counter per blob and deleted
the blob when it reached zero. Two deliveries of the same message to the same
reader decremented twice; a counter that expired between ``EXISTS`` and ``DECR``
deleted a blob another reader still needed; a crash after the acknowledgment but
before the release leaked the blob until its TTL. The contract here replaces the
counter with *obligations*: named, idempotent, generation-fenced claims by
logical readers. A payload is reclaimable only when no obligation names it; a
release is idempotent by ``(obligation_id, generation)``; a TTL is an expiry
policy or a final safety net, never proof that every consumer finished.

Read outcomes are typed: transient failures retry, a missing object is a
durable data-loss outcome, a digest mismatch is corruption. None of them is a
silent ``None``.
'''
from __future__ import absolute_import, division, print_function

import abc
from dataclasses import dataclass
from typing import Mapping, Union

from ..core.errors import IncompatibleProfile
from .capabilities import PayloadCapabilities
from .outcomes import Observation


@dataclass(frozen = True)
class ImmutablePayloadRef:
    '''
    - Arguments:
        - store: the store identity the ref belongs to (``'redis'``, ``'memory'``).
        - key: the store-local key (the wire ``BlobRef.ref``).
        - size: byte length of the object.
        - digest: hex SHA-256 of the object, verified on read when the store keeps it.
        - generation: the ownership generation minted at ``put``; a release from an \
            older generation is stale and cannot delete a newer object at the same key.
        - content_id: the logical content identity (event id + producer), so an \
            unchanged frame forwarded through metadata stages can share one object.
    '''
    store : str
    key : str
    size : int
    digest : str
    generation : str
    content_id : str

@dataclass(frozen = True)
class RetentionContract:
    '''
    - Arguments:
        - ttl_seconds: the store-side expiry backstop.
        - horizon_seconds: the recovery/replay horizon the flow promised; a TTL \
            shorter than it is rejected at admission unless obligations pin the \
            object for the whole horizon.
        - durable_required: whether the profile needs the object to survive the \
            store's own failure model (reliable work) or tolerates loss (live).
        - obligations: the logical readers that will claim the object up front.
    '''
    ttl_seconds : int
    horizon_seconds : int
    durable_required : bool
    obligations : tuple[str, ...] = ()

def admit_retention(contract : RetentionContract) -> None:
    '''
    Admit a retention contract before a byte is written (``BLOB-14`` step 5,
    PAY-009): a ``durable_required`` contract whose TTL is shorter than the
    recovery horizon it promises is rejected unless obligations pin the object
    for the whole horizon. Both reference stores extend a pinned object's life
    to the horizon, so with obligations the TTL is a backstop rather than the
    bound; without them it *is* the bound, and a horizon it cannot cover is a
    promise nobody keeps.

    - Raises:
        - IncompatibleProfile: the contract promises a horizon its TTL cannot \
            cover and no obligation pins the object.
    '''
    if not contract.durable_required or contract.obligations:
        return
    if 0 < contract.ttl_seconds < contract.horizon_seconds:
        raise IncompatibleProfile(
            f'a durable payload contract promises a {contract.horizon_seconds}s recovery horizon but its '
            f'objects expire after {contract.ttl_seconds}s with no reader obligation to pin them',
            remedy = 'Raise VF_BLOB_TTL_SECONDS to at least the recovery horizon, or publish with reader '
                     'obligations (VF_BLOB_READER_IDS) so every object is pinned for the horizon.',
            ttl_seconds = contract.ttl_seconds, horizon_seconds = contract.horizon_seconds)

@dataclass(frozen = True)
class DurableReceipt:
    ref : ImmutablePayloadRef
    obligation_id : str
    deadline : float

@dataclass(frozen = True)
class PayloadBytes:
    ref : ImmutablePayloadRef
    data : bytes
    digest_verified : bool

@dataclass(frozen = True)
class TransientFailure:
    '''The store was unreachable or slow; the bytes may still exist. Retry. (An outcome, not the core error.)'''
    ref : ImmutablePayloadRef
    reason : str

@dataclass(frozen = True)
class Missing:
    ref : ImmutablePayloadRef

@dataclass(frozen = True)
class Corrupt:
    ref : ImmutablePayloadRef
    expected_digest : str
    actual_digest : str

ReadOutcome = Union[PayloadBytes, TransientFailure, Missing, Corrupt]

@dataclass(frozen = True)
class ReleaseReceipt:
    '''
    ``stale`` means the release named an older generation and touched nothing;
    ``unknown`` means the store applied (or may have applied) the release but the
    response was lost — the caller retries, which is safe because a release is
    idempotent by ``(obligation_id, generation)``.
    '''
    ref : ImmutablePayloadRef
    obligation_id : str
    remaining : int | None
    reclaimed : bool
    stale : bool
    unknown : bool = False

@dataclass(frozen = True)
class ReclamationObservation:
    reclaimed : tuple[str, ...]
    retained : tuple[str, ...]
    unknown : tuple[str, ...]

class ObligationLedger(abc.ABC):
    '''
    What the runtime knows about obligations, for reconciliation: which readers
    are still required for which refs. A store reconciles its objects against it.
    '''
    @abc.abstractmethod
    def required_obligations(self) -> Mapping[str, tuple[str, ...]]:
        '''ref key -> obligation ids still required.'''

class PayloadStore(abc.ABC):
    @abc.abstractmethod
    def capabilities(self) -> PayloadCapabilities:
        ...

    @abc.abstractmethod
    def put(self, data : bytes, content_id : str, contract : RetentionContract) -> ImmutablePayloadRef:
        ...

    @abc.abstractmethod
    def acquire_obligation(self, ref : ImmutablePayloadRef, obligation_id : str,
                           deadline : float) -> DurableReceipt:
        ...

    @abc.abstractmethod
    def renew_obligation(self, ref : ImmutablePayloadRef, obligation_id : str,
                         deadline : float) -> DurableReceipt:
        ...

    @abc.abstractmethod
    def read(self, ref : ImmutablePayloadRef) -> ReadOutcome:
        ...

    @abc.abstractmethod
    def release_obligation(self, ref : ImmutablePayloadRef, obligation_id : str,
                           completion_receipt : str) -> ReleaseReceipt:
        ...

    @abc.abstractmethod
    def reconcile(self, ledger : ObligationLedger, operation_id : str) -> ReclamationObservation:
        ...

    @abc.abstractmethod
    def inventory(self) -> Observation[tuple[ImmutablePayloadRef, ...]]:
        ...

    def ref_for_key(self, key : str) -> ImmutablePayloadRef:
        '''
        The ref for a wire key (``BlobRef.ref``), completed from the store's own
        metadata when it keeps any: size, digest and generation as recorded at
        ``put``, so a reader that only has the key can still verify what it reads
        and release against the right generation. A store without metadata for
        the key (an RFC 0002 publisher, or metadata that expired) returns a ref
        with empty digest and generation — readable, unverifiable, and released
        against nothing.
        '''
        return ImmutablePayloadRef(store = 'unknown', key = key, size = 0, digest = '', generation = '',
                                   content_id = '')
