'''
The runtime's own obligation ledger (RFC 0006 ``BLOB-14`` step 4): what a
worker derives, at start and periodically, from durable records it already
keeps and from the broker's own facts — no test-built ledger, no counter.

For every publication a parent of this node made (the parent's *outbox* in the
run ledger: the payload refs, the reader obligations the put acquired, the
accepted stream sequence), the ledger decides which readers still owe a release:

- a publication the channel **evicted** (its sequence is below the channel's
  first retained sequence, ``observe_channel``) will never reach a reader that
  has not taken it — nobody owes it anything (PAY-012);
- otherwise a reader owes its obligation while its **ack floor** on that channel
  is below the sequence (``observe_ack_floor``); a floor at or past it means the
  reader settled the message, and a release that never happened (a crash after
  a confirmed ack, PAY-006) is what reconciliation cancels;
- an ``intent/<publication_id>`` is owed while the publication is unresolved;
  a definitely refused one owes nothing (PAY-007, PAY-013).

Anything that could not be observed — a floor, a channel — is *required*: an
unobservable release is not a release. The ledger is authoritative only for
the reader ids it knows and for publishers' intents; an archive's or a
dead-letter pin's obligation (``archive/*``, ``dlq/*``) is never cancelled by it.
'''
from __future__ import absolute_import, division, print_function

import logging
from typing import Any, Mapping, Sequence

from ..backends.messaging import SUBSCRIPTION_DATA, ChannelId, MessagingBackend, SubscriptionId
from ..backends.outcomes import Unknown
from ..backends.payload import ObligationLedger
from ..backends.runtime import OUTCOME_ACCEPTED, OUTCOME_DUPLICATE, OUTCOME_REJECTED, FlowRuntime

logger = logging.getLogger(__package__)

INTENT_PREFIX = 'intent/'


class RuntimeObligationLedger(ObligationLedger):
    '''
    - Arguments:
        - runtime: the node's ledger (its store is shared with its parents' outboxes).
        - backend: the transport, for channel retention and ack floors.
        - channels: the publishing nodes whose obligations to judge — this node's \\
            parents, and itself for its own intents.
    '''
    def __init__(self, runtime : FlowRuntime, backend : MessagingBackend, channels : Sequence[str]) -> None:
        self._runtime = runtime
        self._backend = backend
        self._channels = list(channels)
        self._known_readers : set[str] | None = None
        self._floors : dict[SubscriptionId, Any] = {}

    def _floor(self, subscription : SubscriptionId) -> Any:
        if subscription not in self._floors:
            self._floors[subscription] = self._backend.observe_ack_floor(subscription)
        return self._floors[subscription]

    def required_obligations(self) -> Mapping[str, tuple[str, ...]]:
        runtime = self._runtime
        required : dict[str, list[str]] = {}
        self._known_readers = set()
        flow_id, run_id = runtime.flow_id, runtime.run_id
        for node in self._channels:
            channel = ChannelId(flow_id, run_id, node)
            retention = self._backend.observe_channel(channel)
            first_seq = None if isinstance(retention, Unknown) else int(retention.value.first_seq)
            for doc in runtime.outbox_of(node):
                refs = [str(r) for r in doc.get('refs') or ()]
                if not refs:
                    continue
                readers = [str(r) for r in doc.get('readers') or ()]
                self._known_readers.update(readers)
                outcome = str(doc.get('outcome', ''))
                seq = doc.get('seq')
                owed : list[str] = []
                if outcome not in (OUTCOME_ACCEPTED, OUTCOME_DUPLICATE, OUTCOME_REJECTED):
                    owed.append(INTENT_PREFIX + str(doc['publication_id']))      # unresolved: keep the intent
                if outcome in (OUTCOME_ACCEPTED, OUTCOME_DUPLICATE):
                    evicted = (first_seq is not None and seq is not None and int(seq) < first_seq)
                    if not evicted:
                        for reader in readers:
                            if seq is None or first_seq is None or self._still_owed(reader, node, channel, int(seq)):
                                owed.append(reader)
                elif outcome != OUTCOME_REJECTED:
                    owed.extend(readers)                                          # unresolved: every reader still may take it
                for key in refs:
                    required.setdefault(key, []).extend(o for o in owed if o not in required.get(key, []))
        return {key: tuple(ids) for key, ids in required.items()}

    def _still_owed(self, reader : str, node : str, channel : ChannelId, seq : int) -> bool:
        consumer, _, partition = reader.partition('/p')
        subscription = SubscriptionId(channel, consumer, int(partition) if partition.isdigit() else None,
                                      SUBSCRIPTION_DATA)
        floor = self._floor(subscription)
        if isinstance(floor, Unknown):
            return True                                                          # unobservable: still owed
        return int(floor.value) < seq

    def authoritative(self, obligation_id : str) -> bool:
        if self._known_readers is None:
            self.required_obligations()                                         # the scan collects the readers
        assert self._known_readers is not None
        return obligation_id.startswith(INTENT_PREFIX) or obligation_id in self._known_readers
