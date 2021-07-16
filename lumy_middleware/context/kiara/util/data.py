import logging
from typing import Any, Callable, Dict, Optional, Tuple, TypeVar
from kiara.data.values import Value
from lumy_middleware.context.kiara.table_utils import filter_table, sort_table
from lumy_middleware.types.generated import DataTabularDataFilter, TableStats
from pyarrow import Table

logger = logging.getLogger(__name__)

LUMY_SUPPORTED_VALUE_TYPES = [
    'table',
    'string',
    'boolean',
    'integer',
    'float',
    'dict',
    'list',
    'any'
]


def is_lumy_supported_type(type_name: str) -> bool:
    return type_name in LUMY_SUPPORTED_VALUE_TYPES


def filter_table_fn(
    table: Table,
    filter: Optional[DataTabularDataFilter]
) -> Tuple[Optional[Table], Optional[TableStats]]:
    '''
    TODO: Perform filtering using a Kiara pipeline
    '''
    if table is None:
        return (None, None)
    if filter is None or filter.full_value:
        return (table, TableStats(rows_count=table.num_rows))

    filtered_table = filter_table(table, filter.condition)
    sorted_table = sort_table(filtered_table, filter.sorting)

    offset = filter.offset or 0
    page_size = filter.page_size or 5
    table_page = sorted_table.slice(offset, page_size)
    return (table_page, TableStats(rows_count=sorted_table.num_rows))


V = TypeVar('V')
F = TypeVar('F')

FilterFn = Callable[[V, Optional[F]], V]

FILTERS: Dict[str, FilterFn] = {
    'table': filter_table_fn
}


def get_value_data(
    value: Value,
    filter: Optional[DataTabularDataFilter]
) -> Tuple[Any, Any]:
    filter_fn = FILTERS.get(value.type_name, None)

    if filter_fn is None:
        return (value.get_value_data(), None)

    return filter_fn(value.get_value_data(), filter)
