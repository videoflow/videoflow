'''
The ABORT envelope's edges: what happens at the boundaries the golden vectors
don't reach.

``test_golden_vectors.py`` pins the round trip against frozen bytes — the
cross-SDK contract. This pins the behaviour a *decoder* must have when the input
is not what it expected, which is where an abort marker matters most: it is the
message that ends a flow, and it must never be the message that fails to encode.
'''
from __future__ import absolute_import, division, print_function

import pytest

from videoflow.core.errors import DeviceError, error_to_dict
from videoflow.v1 import error_pb2
from videoflow.wire import serialization as s


def _abort(error):
    return s.decode_envelope(
        s.encode_envelope('n', 'f', 'r', 'abort-r0', 1, s.MSG_TYPE_ABORT, None, None,
                        error = error))


def test_an_abort_carries_its_error_and_the_other_types_carry_none():
    for msg_type, payload in ((s.MSG_TYPE_DATA, {'v': 1}), (s.MSG_TYPE_EOS, None)):
        decoded = s.decode_envelope(
            s.encode_envelope('n', 'f', 'r', 't', 1, msg_type, None, payload))
        assert decoded['error'] is None
        assert decoded['is_abort'] is False
    assert _abort({'code': 'VF_DEVICE', 'message': 'gone'})['error']['code'] == 'VF_DEVICE'


def test_both_terminators_stop_a_reader_that_only_knows_the_older_one():
    '''
    The compatibility property RFC 0005 turns on. A reader predating
    MSG_TYPE_ABORT sees an unknown enum value; because ``is_stop_signal`` covers
    *any* terminator, it stops rather than waiting forever for an end-of-stream
    that already happened. Degrading to the old behaviour is acceptable; degrading
    to a hang is not.
    '''
    for msg_type in (s.MSG_TYPE_EOS, s.MSG_TYPE_ABORT):
        decoded = s.decode_envelope(
            s.encode_envelope('n', 'f', 'r', 't', 1, msg_type, None, None,
                            error = {'code': 'VF_X', 'message': 'x'}))
        assert decoded['is_stop_signal'] is True
        assert decoded['message'] is None            # neither carries a payload


def test_an_abort_encodes_even_when_the_error_is_empty_or_absent():
    '''
    An abort's job is to end a flow. It must not be the thing that fails to
    encode, so a missing or malformed error degrades to an empty record rather
    than raising.
    '''
    assert _abort(None)['error']['code'] == 'VF_UNKNOWN'
    assert _abort({})['error']['code'] == 'VF_UNKNOWN'


def test_an_unknown_disposition_degrades_instead_of_raising():
    decoded = _abort({'code': 'VF_X', 'message': 'x', 'disposition': 'sideways'})
    # UNSPECIFIED round-trips as absent, and the protocol says to read it as
    # transient — the safe default.
    assert 'disposition' not in decoded['error']


def test_long_text_is_truncated_rather_than_carried_whole():
    '''
    An abort rides the terminator path, where an unbounded string from a node's
    exception text could outgrow a broker message and take the marker down with it.
    '''
    huge = 'x' * (s.MAX_ERROR_TEXT_BYTES * 3)
    decoded = _abort({'code': 'VF_X', 'message': huge, 'remedy': huge,
                    'context': {'k': huge}})
    assert len(decoded['error']['message']) == s.MAX_ERROR_TEXT_BYTES
    assert len(decoded['error']['remedy']) == s.MAX_ERROR_TEXT_BYTES
    assert len(decoded['error']['context']['k']) == s.MAX_ERROR_TEXT_BYTES


def test_a_typed_error_round_trips_through_the_wire_unchanged():
    original = DeviceError('CUDA out of memory', remedy = 'Lower the batch size.',
                        node = 'detector', replica = 2)
    restored = _abort(error_to_dict(original))['error']
    assert restored['code'] == 'VF_DEVICE'
    assert restored['message'] == 'CUDA out of memory'
    assert restored['remedy'] == 'Lower the batch size.'
    assert restored['disposition'] == 'worker_fatal'
    # Context values become strings on the wire: every SDK has those, and not
    # every SDK has a variant type.
    assert restored['context'] == {'node': 'detector', 'replica': '2'}


def test_empty_optional_fields_stay_absent_rather_than_becoming_empty_strings():
    '''
    "No remedy" and "an empty remedy" are different facts, and an SDK that cannot
    tell them apart will render a blank line under every error.
    '''
    restored = _abort({'code': 'VF_X', 'message': 'x'})['error']
    assert set(restored) == {'code', 'message'}


def test_the_proto_is_the_normative_form():
    '''
    The taxonomy is a proto, not one SDK's class tree — a component may be written
    in any language, and a Python exception hierarchy is invisible to the rest of
    them.
    '''
    proto = s.error_to_proto({'code': 'VF_DEVICE', 'message': 'gone',
                            'disposition': 'worker_fatal'})
    assert isinstance(proto, error_pb2.Error)
    assert proto.disposition == error_pb2.DISPOSITION_WORKER_FATAL
    assert s.error_from_proto(proto)['disposition'] == 'worker_fatal'


if __name__ == '__main__':
    pytest.main([__file__])
