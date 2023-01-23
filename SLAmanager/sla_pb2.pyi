from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class MetricValue(_message.Message):
    __slots__ = ["max", "metric_name", "min"]
    MAX_FIELD_NUMBER: _ClassVar[int]
    METRIC_NAME_FIELD_NUMBER: _ClassVar[int]
    MIN_FIELD_NUMBER: _ClassVar[int]
    max: int
    metric_name: str
    min: int
    def __init__(self, metric_name: _Optional[str] = ..., min: _Optional[int] = ..., max: _Optional[int] = ...) -> None: ...

class SlaReply(_message.Message):
    __slots__ = ["msg"]
    MSG_FIELD_NUMBER: _ClassVar[int]
    msg: str
    def __init__(self, msg: _Optional[str] = ...) -> None: ...

class SlaRequest(_message.Message):
    __slots__ = ["hour", "name"]
    HOUR_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    hour: int
    name: str
    def __init__(self, name: _Optional[str] = ..., hour: _Optional[int] = ...) -> None: ...

class Violation(_message.Message):
    __slots__ = ["metric_name", "num", "value"]
    METRIC_NAME_FIELD_NUMBER: _ClassVar[int]
    NUM_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    metric_name: str
    num: int
    value: float
    def __init__(self, metric_name: _Optional[str] = ..., value: _Optional[float] = ..., num: _Optional[int] = ...) -> None: ...
