from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Tensor(_message.Message):
    __slots__ = ("shape", "dtype", "data")
    SHAPE_FIELD_NUMBER: _ClassVar[int]
    DTYPE_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    shape: _containers.RepeatedScalarFieldContainer[int]
    dtype: str
    data: bytes
    def __init__(self, shape: _Optional[_Iterable[int]] = ..., dtype: _Optional[str] = ..., data: _Optional[bytes] = ...) -> None: ...

class Frame(_message.Message):
    __slots__ = ("pixels", "pixel_format", "capture_ts")
    PIXELS_FIELD_NUMBER: _ClassVar[int]
    PIXEL_FORMAT_FIELD_NUMBER: _ClassVar[int]
    CAPTURE_TS_FIELD_NUMBER: _ClassVar[int]
    pixels: Tensor
    pixel_format: str
    capture_ts: float
    def __init__(self, pixels: _Optional[_Union[Tensor, _Mapping]] = ..., pixel_format: _Optional[str] = ..., capture_ts: _Optional[float] = ...) -> None: ...

class Detections(_message.Message):
    __slots__ = ("boxes", "class_names")
    BOXES_FIELD_NUMBER: _ClassVar[int]
    CLASS_NAMES_FIELD_NUMBER: _ClassVar[int]
    boxes: Tensor
    class_names: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, boxes: _Optional[_Union[Tensor, _Mapping]] = ..., class_names: _Optional[_Iterable[str]] = ...) -> None: ...

class Tracks(_message.Message):
    __slots__ = ("tracks",)
    TRACKS_FIELD_NUMBER: _ClassVar[int]
    tracks: Tensor
    def __init__(self, tracks: _Optional[_Union[Tensor, _Mapping]] = ...) -> None: ...

class BlobRef(_message.Message):
    __slots__ = ("ref", "inner_payload_type", "size")
    REF_FIELD_NUMBER: _ClassVar[int]
    INNER_PAYLOAD_TYPE_FIELD_NUMBER: _ClassVar[int]
    SIZE_FIELD_NUMBER: _ClassVar[int]
    ref: str
    inner_payload_type: str
    size: int
    def __init__(self, ref: _Optional[str] = ..., inner_payload_type: _Optional[str] = ..., size: _Optional[int] = ...) -> None: ...
