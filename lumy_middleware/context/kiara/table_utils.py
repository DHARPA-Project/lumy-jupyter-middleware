from typing import List, Optional

import pyarrow.compute as pc
from lumy_middleware.types.generated import (DataTabularDataFilter,
                                             DataTabularDataFilterCondition,
                                             DataTabularDataFilterItem,
                                             DataTabularDataSortingMethod,
                                             Direction)
from pyarrow import Table


def row_matches(
    table: Table,
    idx: int,
    filter_items: List[DataTabularDataFilterItem]
) -> bool:
    matches = [
        str(item.value) in str(table[item.column][idx])
        for item in filter_items
    ]
    return all(matches)


def filter_table(
    table: Table,
    condition: Optional[DataTabularDataFilterCondition]
) -> Table:
    '''
    NOTE: This method is here while filtering workflow and data registry
    are not ready in kiara. It only supports 'contains' filter for strings
    for demonstration purposes.
    '''
    if condition is None:
        return table

    filter_items = [
        item
        for item in condition.items
        if item.operator == 'contains'
    ]

    if len(filter_items) == 0:
        return table

    mask = [
        row_matches(table, idx, filter_items)
        for idx in range(0, len(table))
    ]
    return pc.filter(table, mask)


def sort_table(
    table: Table,
    sorting: Optional[DataTabularDataSortingMethod]
) -> Table:
    if sorting is None \
        or sorting.direction == Direction.DEFAULT \
            or sorting.direction is None:
        return table

    sort_opts = (
        sorting.column,
        'ascending' if sorting.direction == Direction.ASC else 'descending'
    )

    indices = pc.sort_indices(
        table,
        sort_keys=[sort_opts]
    )
    return table.take(indices)


def filter_table_with_pagination(
    table: Table,
    filter: Optional[DataTabularDataFilter]
) -> Table:
    if filter is None:
        return table
    if filter.full_value:
        return table

    filtered_table = filter_table(table, filter.condition)

    return filtered_table.slice(filter.offset or 0, filter.page_size or 5)
