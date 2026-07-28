from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Disposition(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    DISPOSITION_UNSPECIFIED: _ClassVar[Disposition]
    DISPOSITION_POISON: _ClassVar[Disposition]
    DISPOSITION_TRANSIENT: _ClassVar[Disposition]
    DISPOSITION_WORKER_FATAL: _ClassVar[Disposition]
DISPOSITION_UNSPECIFIED: Disposition
DISPOSITION_POISON: Disposition
DISPOSITION_TRANSIENT: Disposition
DISPOSITION_WORKER_FATAL: Disposition

class Error(_message.Message):
    __slots__ = ("code", "message", "remedy", "disposition", "node", "trace_id", "num_delivered", "context")
    class ContextEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    REMEDY_FIELD_NUMBER: _ClassVar[int]
    DISPOSITION_FIELD_NUMBER: _ClassVar[int]
    NODE_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    NUM_DELIVERED_FIELD_NUMBER: _ClassVar[int]
    CONTEXT_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    remedy: str
    disposition: Disposition
    node: str
    trace_id: str
    num_delivered: int
    context: _containers.ScalarMap[str, str]
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ..., remedy: _Optional[str] = ..., disposition: _Optional[_Union[Disposition, str]] = ..., node: _Optional[str] = ..., trace_id: _Optional[str] = ..., num_delivered: _Optional[int] = ..., context: _Optional[_Mapping[str, str]] = ...) -> None: ...
