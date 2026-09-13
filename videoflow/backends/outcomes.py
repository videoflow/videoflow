'''
Truthful outcome types shared by every backend contract.

The failure that motivates this module is quiet: a broker query that raised was
reported as "zero pending", a lost acknowledgment as "acknowledged", a node list
the API refused as "no GPUs in use". Each of those turned an unobservable state
into a confident answer, and the code above it then made a destructive decision
(declared a drain complete, reclaimed a payload, repartitioned a card). The types
here make the distinction unavoidable in the type system:

- ``Known[T]`` / ``Unknown``: an observation either carries a value with the time
  and generation it was observed at, or says why it could not be made. Nothing
  coerces ``Unknown`` to a default.
- ``Accepted`` / ``Rejected`` / ``PublicationUnknown`` / ``PublicationUnresolvable``:
  what happened to a publication. ``Unknown`` means the side effect may have
  happened (a lost receipt); ``Unresolvable`` means the backend has no ledger to
  ask, so the runtime must reconcile through its own durable intent.
- ``SettleConfirmed`` / ``SettleUnknown`` / ``SettleStale``: what happened to a
  settlement. A stale settlement is one issued from a delivery attempt that a
  newer attempt has superseded; it is refused before it reaches the broker.
- ``CleanupObservation``: what a teardown actually removed, and what it could not
  — never a silent return on a failed listing.
'''
from __future__ import absolute_import, division, print_function

import time
from dataclasses import dataclass
from typing import TYPE_CHECKING, Generic, TypeGuard, TypeVar, Union

if TYPE_CHECKING:
    from .messaging import DeliveryToken

T = TypeVar('T')

#: Durability boundaries an ``Accepted`` publication can report having crossed.
DURABILITY_MEMORY = 'memory'
DURABILITY_STREAM = 'stream'
DURABILITY_QUORUM = 'stream-quorum'

@dataclass(frozen = True)
class Known(Generic[T]):
    '''
    A successful observation.

    - Arguments:
        - value: what was observed.
        - observed_at: monotonic time of the observation, so two observations can \
            be ordered without trusting wall clocks.
        - generation: the provider's version of the observed state when it has one \
            (a Kubernetes ``resourceVersion``, a consumer info sequence, a ledger \
            version). ``None`` when the provider offers nothing comparable.
    '''
    value : T
    observed_at : float
    generation : str | None = None

@dataclass(frozen = True)
class Unknown:
    '''
    An observation that could not be made. ``reason`` is a short machine-readable
    category (``'timeout'``, ``'auth'``, ``'malformed'``, ``'unreachable'``,
    ``'unsupported'``); ``detail`` is the human-readable why.
    '''
    reason : str
    observed_at : float
    detail : str = ''

Observation = Union[Known[T], Unknown]

def known(value : T, generation : str | None = None) -> Known[T]:
    return Known(value, time.monotonic(), generation)

def unknown(reason : str, detail : str = '') -> Unknown:
    return Unknown(reason, time.monotonic(), detail)

def is_known(observation : 'Observation[T]') -> TypeGuard[Known[T]]:
    return isinstance(observation, Known)

def value_or(observation : 'Observation[T]', default : T) -> T:
    '''
    The observed value, or ``default`` when unknown. Only for diagnostics and
    display: a decision that would act on ``default`` must branch on ``is_known``
    instead, which is the whole point of the type.
    '''
    return observation.value if isinstance(observation, Known) else default

# -- publication outcomes --------------------------------------------------------

@dataclass(frozen = True)
class Accepted:
    '''The backend durably took the envelope. ``duplicate`` is True when it deduplicated a retry.'''
    publication_id : str
    sequence : int | None
    duplicate : bool
    durability_boundary : str

@dataclass(frozen = True)
class Rejected:
    '''The backend definitely did not take the envelope. ``retryable`` says whether waiting can help.'''
    publication_id : str
    reason : str
    retryable : bool

@dataclass(frozen = True)
class PublicationUnknown:
    '''A response was lost or a deadline expired after the send; the envelope may be stored.'''
    publication_id : str
    reason : str

@dataclass(frozen = True)
class PublicationUnresolvable:
    '''The backend keeps no queryable publication history for this id; only the runtime's own intent can reconcile it.'''
    publication_id : str
    reason : str

PublicationOutcome = Union[Accepted, Rejected, PublicationUnknown, PublicationUnresolvable]

# -- settlement outcomes ---------------------------------------------------------

@dataclass(frozen = True)
class SettleConfirmed:
    token : 'DeliveryToken'
    settlement_id : str

@dataclass(frozen = True)
class SettleUnknown:
    '''The settlement was sent but not confirmed; the delivery may still be leased or may redeliver.'''
    token : 'DeliveryToken'
    reason : str

@dataclass(frozen = True)
class SettleStale:
    '''Refused before reaching the broker: a newer attempt owns this logical message.'''
    token : 'DeliveryToken'
    current_attempt : int
    current_generation : str

SettlementOutcome = Union[SettleConfirmed, SettleUnknown, SettleStale]

# -- cleanup ---------------------------------------------------------------------

@dataclass(frozen = True)
class CleanupObservation:
    '''
    What a teardown did. ``complete`` is False when anything owned could not be
    confirmed removed — including when the inventory itself could not be read,
    which is not "nothing to remove".
    '''
    complete : bool
    removed : tuple[str, ...]
    remaining : tuple[str, ...]
    reason : str = ''
