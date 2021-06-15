import base64
from typing import Any, Tuple

import pyarrow as pa
from lumy_middleware.types.generated import DataType, DataValueContainer


def serialize_table(table: pa.Table) -> str:
    sink = pa.BufferOutputStream()

    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write(table)
    writer.close()

    val = sink.getvalue().to_pybytes()
    return base64.b64encode(val).decode('ascii')


def deserialize_table(serialized_table: str) -> pa.Table:
    table_bytes = base64.b64decode(serialized_table)
    src = pa.BufferReader(table_bytes)
    reader = pa.ipc.open_stream(src)
    return reader.read_all()


def serialize(value: Any) -> Tuple[Any, DataType]:
    '''
    Serialize any value to wire format.
    '''
    if isinstance(value, pa.Table):
        return (serialize_table(value), DataType.TABLE)
    return (value, DataType.SIMPLE)


def deserialize(container: DataValueContainer) -> Any:
    '''
    Deserialize any value from wire format.
    '''
    if container.data_type == DataType.TABLE:
        return deserialize_table(container.value)
    return container.value
