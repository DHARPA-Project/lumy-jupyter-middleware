import base64
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, Generic, List, TypeVar, Union

import pyarrow as pa
from lumy_middleware.types.generated import DataType, DataValueContainer

logger = logging.getLogger(__name__)

T = TypeVar('T')
W = TypeVar('W')


class Codec(Generic[T, W], ABC):
    @abstractmethod
    def serialize(self, value: T) -> W:
        ...

    @abstractmethod
    def deserialize(self, value: W) -> T:
        ...

    @abstractmethod
    def supports(self, value: Any) -> bool:
        ...


class TableCodec(Codec[pa.Table, str]):
    def serialize(self, value: pa.Table) -> str:
        sink = pa.BufferOutputStream()

        writer = pa.ipc.new_stream(sink, value.schema)
        writer.write(value)
        writer.close()

        val = sink.getvalue().to_pybytes()
        return base64.b64encode(val).decode('ascii')

    def deserialize(self, value: str) -> pa.Table:
        table_bytes = base64.b64decode(value)
        src = pa.BufferReader(table_bytes)
        reader = pa.ipc.open_stream(src)
        return reader.read_all()

    def supports(self, value: Any) -> bool:
        return isinstance(value, pa.Table)


SupportedSimpleTypes = Union[Dict, List, float, int, str, bool]
SupportedSimpleTypesClasses = (list, dict, float, int, str, bool)


class SimpleValueCodec(Codec[Any, SupportedSimpleTypes]):
    def serialize(self, value: Any) -> SupportedSimpleTypes:
        return value

    def deserialize(self, value: Any) -> SupportedSimpleTypes:
        return value

    def supports(self, value: Any) -> bool:
        return isinstance(value, SupportedSimpleTypesClasses) or value is None


CODECS: Dict[DataType, Codec] = {
    DataType.TABLE: TableCodec(),
    DataType.SIMPLE: SimpleValueCodec()
}


def serialize(value: Any) -> DataValueContainer:
    '''
    Serialize any value to wire format.
    '''
    for data_type, codec in CODECS.items():
        if codec.supports(value):
            return DataValueContainer(data_type, codec.serialize(value))
    raise Exception(f'No codec found that supports {value} ({type(value)})')


def deserialize(container: DataValueContainer) -> Any:
    '''
    Deserialize any value from wire format.
    '''
    codec = CODECS.get(container.data_type, None)
    if codec is None:
        raise Exception(f'No codec found for type {container.data_type.value}')
    return codec.deserialize(container.value)
