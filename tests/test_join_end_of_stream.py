'''
A join at end of stream: once every parent has ended and nothing is left to
deliver, an incomplete group can never complete. The messenger settles it per
the missing policy instead of waiting forever (the drain used to hang on
``in_groups=True`` with every parent at EOS).

Pure/unit: the messenger is built bare, with a fake backend and consumer probe.
'''
from types import SimpleNamespace

from videoflow.backends.outcomes import known, unknown
from videoflow.core.constants import BATCH
from videoflow.core.policies import JoinPolicy
from videoflow.messaging.grouping import EnvelopeEntry, TraceGroupAssembler
from videoflow.messaging.nats_messenger import NATSMessenger


class _Handle:
    def __init__(self):
        self.state = None

    def ack(self):
        self.state = 'acked'

    def nak(self, delay = None):
        self.state = 'naked'

    def supersede(self):
        pass


def _entry(trace_id, seq):
    return EnvelopeEntry(trace_id = trace_id, seq = seq, event_ts = None, message = seq, metadata = None,
                         is_stop_signal = False)


def _join(missing = 'wait', pending = (0, 0), prefetched = 0, eos = ('a', 'b'), stopped = ()):
    m = NATSMessenger.__new__(NATSMessenger)
    m._node = SimpleNamespace(name = 'join')
    m._parent_names = ['a', 'b']
    m._join_policy = JoinPolicy(missing = missing)
    m._assembler = TraceGroupAssembler('join', ['a', 'b'], m._join_policy)
    m._eos_seen = set(eos)
    m._stopped_parents = set(stopped)
    m._data_subs = {'a': 'sub-a', 'b': 'sub-b'}
    m._backend = SimpleNamespace(prefetched = lambda sub: prefetched)
    m._consumer_pending = lambda parent: pending if isinstance(pending, str) and pending == 'unknown' else known(pending)
    if pending == 'unknown':
        m._consumer_pending = lambda parent: unknown('failed', 'no api')
    handle = _Handle()
    m._assembler.add('a', _entry('t1', 1), handle)      # a half that b will never match
    return m, handle


def test_a_group_no_half_can_still_complete_is_settled_at_end_of_stream(caplog):
    m, handle = _join(missing = 'wait')
    assert m._flush_unfinishable_groups() is True
    assert handle.state == 'acked' and m._assembler.pending_count() == 0
    assert 'end of stream' in caplog.text
    assert m._flush_unfinishable_groups() is False        # nothing left: the stall logger takes over


def test_error_policy_hands_the_halves_back():
    m, handle = _join(missing = 'error')
    assert m._flush_unfinishable_groups() is True
    assert handle.state == 'naked'


def test_nothing_is_settled_while_a_half_could_still_arrive():
    m, handle = _join(eos = ('a',))                         # b has not ended
    assert m._flush_unfinishable_groups() is False and handle.state is None
    m, handle = _join(pending = (3, 0))                     # b still has messages on the broker
    assert m._flush_unfinishable_groups() is False and handle.state is None
    m, handle = _join(prefetched = 1)                       # ...or already fetched, not yet folded in
    assert m._flush_unfinishable_groups() is False and handle.state is None
    m, handle = _join(pending = 'unknown')                  # an unreadable broker is never "empty"
    assert m._flush_unfinishable_groups() is False and handle.state is None


def test_a_stopped_parent_is_not_asked_again():
    m, handle = _join(stopped = ('b',))
    m._consumer_pending = lambda parent: (_ for _ in ()).throw(AssertionError(f'{parent} asked'))
    m._consumer_pending = lambda parent: known((0, 0)) if parent == 'a' else (_ for _ in ()).throw(AssertionError('b asked'))
    assert m._flush_unfinishable_groups() is True and handle.state == 'acked'


def test_the_default_batch_policy_settles_as_drop():
    m, handle = _join(missing = JoinPolicy.default_for(BATCH).missing)
    assert m._flush_unfinishable_groups() is True and handle.state == 'acked'
