from videoflow.v1 import value_pb2 as _value_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MsgType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    MSG_TYPE_UNSPECIFIED: _ClassVar[MsgType]
    MSG_TYPE_DATA: _ClassVar[MsgType]
    MSG_TYPE_EOS: _ClassVar[MsgType]
MSG_TYPE_UNSPECIFIED: MsgType
MSG_TYPE_DATA: MsgType
MSG_TYPE_EOS: MsgType

class Envelope(_message.Message):
    __slots__ = ("v", "type", "producer_name", "flow_id", "run_id", "trace_id", "seq", "event_ts", "span_id", "parent_span_id", "replica_id", "metadata", "payload_type", "payload")
    class MetadataEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: _value_pb2.Value
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[_value_pb2.Value, _Mapping]] = ...) -> None: ...
    V_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    PRODUCER_NAME_FIELD_NUMBER: _ClassVar[int]
    FLOW_ID_FIELD_NUMBER: _ClassVar[int]
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    EVENT_TS_FIELD_NUMBER: _ClassVar[int]
    SPAN_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_SPAN_ID_FIELD_NUMBER: _ClassVar[int]
    REPLICA_ID_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_TYPE_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    v: int
    type: MsgType
    producer_name: str
    flow_id: str
    run_id: str
    trace_id: str
    seq: int
    event_ts: float
    span_id: str
    parent_span_id: str
    replica_id: int
    metadata: _containers.MessageMap[str, _value_pb2.Value]
    payload_type: str
    payload: bytes
    def __init__(self, v: _Optional[int] = ..., type: _Optional[_Union[MsgType, str]] = ..., producer_name: _Optional[str] = ..., flow_id: _Optional[str] = ..., run_id: _Optional[str] = ..., trace_id: _Optional[str] = ..., seq: _Optional[int] = ..., event_ts: _Optional[float] = ..., span_id: _Optional[str] = ..., parent_span_id: _Optional[str] = ..., replica_id: _Optional[int] = ..., metadata: _Optional[_Mapping[str, _value_pb2.Value]] = ..., payload_type: _Optional[str] = ..., payload: _Optional[bytes] = ...) -> None: ...
