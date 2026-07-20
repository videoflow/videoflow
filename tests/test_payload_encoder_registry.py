'''
Vendor payload codecs on the v4 wire (videoflow.wire.serialization).

``register_payload_type`` already made *decoding* a vendor FQN pluggable;
encoding was a hard isinstance ladder, so a vendor type had no neutral wire
encoding at all. These tests pin the encode-side registry and, importantly, the
ordering guarantee: registering an encoder must not be able to change how a
built-in payload encodes.
'''
from __future__ import absolute_import, division, print_function

import numpy as np
import pytest

from videoflow.utils import plugins
from videoflow.wire import serialization as s


class _Widget:
    '''A vendor type that is neither ndarray, proto, nor JSON-like.'''
    def __init__(self, x):
        self.x = x


def _encode_widget(w):
    return 'vendor.acme.Widget', str(w.x).encode()


@pytest.fixture
def registry_sandbox(monkeypatch):
    monkeypatch.setattr(s, '_PAYLOAD_ENCODERS', list(s._PAYLOAD_ENCODERS))
    monkeypatch.setattr(s, '_PAYLOAD_REGISTRY', dict(s._PAYLOAD_REGISTRY))
    monkeypatch.setattr(plugins, '_loaded', set(plugins._loaded))
    return s


def test_unregistered_vendor_type_is_rejected_and_names_the_fix():
    with pytest.raises(TypeError) as excinfo:
        s.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {}, _Widget(1), version = 4)
    assert 'register_payload_encoder' in str(excinfo.value)


def test_registered_encoder_gives_a_vendor_type_a_real_wire_type(registry_sandbox):
    registry_sandbox.register_payload_encoder(_Widget, _encode_widget)
    buf = registry_sandbox.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {},
                                           _Widget(42), version = 4)
    decoded = registry_sandbox.decode_envelope(buf)
    # Without a matching decoder the payload comes back opaque (never deserialized).
    assert isinstance(decoded['message'], s.RawPayload)
    assert decoded['message'].payload_type == 'vendor.acme.Widget'
    assert decoded['message'].data == b'42'


@pytest.mark.parametrize('payload,expected_type', [
    (np.zeros(3, dtype = np.float32), s.PAYLOAD_TENSOR),
    ({'a': 1}, s.PAYLOAD_VALUE),
    (7, s.PAYLOAD_VALUE),
    ('text', s.PAYLOAD_VALUE),
    (None, s.PAYLOAD_VALUE),
])
def test_encoder_cannot_hijack_a_builtin_payload(registry_sandbox, payload, expected_type):
    '''
    Rules are consulted after the built-in checks, so a greedy registration
    (here: object, matching everything) must not change built-in encoding. The
    built-in mappings are fixed by PROTOCOL.md 4.4.
    '''
    registry_sandbox.register_payload_encoder(object, lambda p: ('vendor.hijack', b'x'))
    buf = registry_sandbox.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {},
                                           payload, version = 4)
    from videoflow.v1 import envelope_pb2
    env = envelope_pb2.Envelope()
    env.ParseFromString(buf)
    assert env.payload_type == expected_type


def test_already_encodable_type_keeps_its_builtin_encoding(registry_sandbox):
    '''
    The corollary of the ordering guarantee: a type that is already encodable
    (here a dict subclass) travels as the built-in it resembles, and its
    registered rule never runs. Documented so the behavior is a decision rather
    than a surprise.
    '''
    class _DictLike(dict):
        pass

    registry_sandbox.register_payload_encoder(_DictLike, lambda p: ('vendor.dictlike', b'x'))
    buf = registry_sandbox.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {},
                                           _DictLike(a = 1), version = 4)
    assert registry_sandbox.decode_envelope(buf)['message'] == {'a': 1}


def test_rules_are_checked_in_registration_order(registry_sandbox):
    class _Sub(_Widget):
        pass

    registry_sandbox.register_payload_encoder(_Widget, lambda p: ('first', b'1'))
    registry_sandbox.register_payload_encoder(_Sub, lambda p: ('second', b'2'))
    buf = registry_sandbox.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {},
                                           _Sub(0), version = 4)
    assert registry_sandbox.decode_envelope(buf)['message'].payload_type == 'first'


def test_decode_consults_entry_points_for_an_unknown_fqn(registry_sandbox, monkeypatch):
    '''
    Host-side tools (videoflow debug decode) never import the component that
    registers a vendor payload, so an unknown FQN triggers one entry-point scan.
    '''
    from videoflow.v1 import payloads_pb2

    calls = []

    def fake_load(group):
        calls.append(group)
        registry_sandbox.register_payload_type(payloads_pb2.Detections)

    monkeypatch.setattr(registry_sandbox, '_PAYLOAD_REGISTRY', {})
    monkeypatch.setattr(plugins, 'load_plugin_group', fake_load)

    det = payloads_pb2.Detections(class_names = ['car'])
    buf = registry_sandbox.encode_envelope('n', 'f', 'r', 't', 1, s.MSG_TYPE_DATA, {},
                                           det, version = 4)
    decoded = registry_sandbox.decode_envelope(buf)
    assert calls == [registry_sandbox.PAYLOAD_ENTRY_POINT_GROUP]
    assert isinstance(decoded['message'], payloads_pb2.Detections)
    assert list(decoded['message'].class_names) == ['car']


if __name__ == '__main__':
    pytest.main([__file__])
