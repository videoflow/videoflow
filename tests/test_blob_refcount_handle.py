'''
The messenger's delivery handle over a backend token: a payload obligation is
released exactly once and only on a *confirmed* settlement; nak/term/supersede
never release; a stale token settles nothing.
'''
from __future__ import absolute_import, division, print_function

from videoflow.backends.messaging import ChannelId, Completed, DeliveryToken, Retry, SubscriptionId, Terminal
from videoflow.backends.outcomes import SettleConfirmed, SettleStale, SettleUnknown
from videoflow.messaging.nats_messenger import _AckHandle


class _StubBackend:
    def __init__(self, outcome = 'confirmed'):
        self.outcome = outcome
        self.settled = []
        self.superseded = []

    def settle(self, token, outcome, settlement_id):
        self.settled.append((token, outcome, settlement_id))
        if self.outcome == 'unknown':
            return SettleUnknown(token, 'ack not confirmed')
        if self.outcome == 'stale':
            return SettleStale(token, token.attempt + 1, token.generation)
        return SettleConfirmed(token, settlement_id)

    def supersede(self, token):
        self.superseded.append(token)
        return True


class _StubMessenger:
    '''Just enough of NATSMessenger for _AckHandle: forget + release recording.'''
    def __init__(self, backend, obligations = False):
        self._backend = backend
        self.released = []
        self.forgotten = []
        # Whether this messenger keeps identity obligations (RFC 0006) or the legacy refcount.
        self.obligations = obligations

    def _forget_handle(self, handle):
        self.forgotten.append(handle)

    def _release_after_settlement(self, blob_ref, settlement_id):
        if blob_ref is not None:
            self.released.append((blob_ref, settlement_id))

    def _release_after_terminal(self, blob_ref, settlement_id):
        # The obligation model releases the reader after a confirmed terminal
        # settlement; a legacy refcount messenger leaves the blob to its TTL.
        if self.obligations:
            self._release_after_settlement(blob_ref, settlement_id)


def _token(attempt = 1):
    sub = SubscriptionId(ChannelId('f', 'r', 'p'), 'c', None)
    return DeliveryToken(sub, 'm1', 7, attempt, 'g1')


def test_ack_releases_exactly_once():
    backend = _StubBackend()
    m = _StubMessenger(backend)
    h = _AckHandle(_token(), m, blob_ref = 'vf-blob-abc')
    h.ack()
    h.ack()                                                     # idempotent
    assert [o for _t, o, _s in backend.settled] == [Completed()]
    assert [ref for ref, _s in m.released] == ['vf-blob-abc']
    assert m.forgotten == [h]


def test_unconfirmed_ack_does_not_release():
    backend = _StubBackend('unknown')
    m = _StubMessenger(backend)
    h = _AckHandle(_token(), m, blob_ref = 'vf-blob-abc')
    h.ack()
    assert m.released == []                                     # PAY-004: unknown is not confirmed


def test_stale_token_settles_nothing():
    backend = _StubBackend('stale')
    m = _StubMessenger(backend)
    h = _AckHandle(_token(), m, blob_ref = 'vf-blob-abc')
    h.term('dlq:1')
    assert m.released == []


def test_nak_and_supersede_never_release_and_term_needs_a_record():
    backend = _StubBackend()
    m = _StubMessenger(backend)
    h = _AckHandle(_token(), m, blob_ref = 'vf-blob-abc')
    h.nak(delay = 2)
    assert backend.settled[-1][1] == Retry(2) and m.released == []
    h2 = _AckHandle(_token(2), m, blob_ref = 'vf-blob-abc')
    h2.supersede()
    assert backend.superseded == [_token(2)] and m.released == []
    h3 = _AckHandle(_token(3), m, blob_ref = 'vf-blob-abc')
    h3.term('ledger:c/terminal:1')
    assert backend.settled[-1][1] == Terminal('ledger:c/terminal:1')
    # Legacy refcount: a dead letter re-reads the blob, so a termination leaves it to its TTL (BLOB-6).
    assert m.released == []
    assert h.num_delivered == 1 and h3.stream_seq == 7


def test_term_releases_the_reader_only_under_the_obligation_model():
    '''BLOB-14 step 3: the dead letter has pinned the payload, so the reader's own obligation goes.'''
    backend = _StubBackend()
    m = _StubMessenger(backend, obligations = True)
    h = _AckHandle(_token(), m, blob_ref = 'vf-blob-abc')
    h.term('dlq:f:r:c:1')
    assert [ref for ref, _s in m.released] == ['vf-blob-abc']
    stale = _AckHandle(_token(), _StubMessenger(_StubBackend('stale'), obligations = True), blob_ref = 'vf-blob-abc')
    stale.term('dlq:f:r:c:1')
    assert stale._m.released == []
