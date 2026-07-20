from videoflow.v1 import payloads_pb2 as _payloads_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class NullValue(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    NULL_VALUE: _ClassVar[NullValue]
NULL_VALUE: NullValue

class Value(_message.Message):
    __slots__ = ("null_value", "double_value", "int_value", "string_value", "bytes_value", "bool_value", "list_value", "map_value", "tensor_value")
    NULL_VALUE_FIELD_NUMBER: _ClassVar[int]
    DOUBLE_VALUE_FIELD_NUMBER: _ClassVar[int]
    INT_VALUE_FIELD_NUMBER: _ClassVar[int]
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    BYTES_VALUE_FIELD_NUMBER: _ClassVar[int]
    BOOL_VALUE_FIELD_NUMBER: _ClassVar[int]
    LIST_VALUE_FIELD_NUMBER: _ClassVar[int]
    MAP_VALUE_FIELD_NUMBER: _ClassVar[int]
    TENSOR_VALUE_FIELD_NUMBER: _ClassVar[int]
    null_value: NullValue
    double_value: float
    int_value: int
    string_value: str
    bytes_value: bytes
    bool_value: bool
    list_value: ListValue
    map_value: MapValue
    tensor_value: _payloads_pb2.Tensor
    def __init__(self, null_value: _Optional[_Union[NullValue, str]] = ..., double_value: _Optional[float] = ..., int_value: _Optional[int] = ..., string_value: _Optional[str] = ..., bytes_value: _Optional[bytes] = ..., bool_value: _Optional[bool] = ..., list_value: _Optional[_Union[ListValue, _Mapping]] = ..., map_value: _Optional[_Union[MapValue, _Mapping]] = ..., tensor_value: _Optional[_Union[_payloads_pb2.Tensor, _Mapping]] = ...) -> None: ...

class ListValue(_message.Message):
    __slots__ = ("values",)
    VALUES_FIELD_NUMBER: _ClassVar[int]
    values: _containers.RepeatedCompositeFieldContainer[Value]
    def __init__(self, values: _Optional[_Iterable[_Union[Value, _Mapping]]] = ...) -> None: ...

class MapValue(_message.Message):
    __slots__ = ("fields",)
    class FieldsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: Value
        def __init__(self, key: _Optional[str] = ..., value: _Optional[_Union[Value, _Mapping]] = ...) -> None: ...
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    fields: _containers.MessageMap[str, Value]
    def __init__(self, fields: _Optional[_Mapping[str, Value]] = ...) -> None: ...
