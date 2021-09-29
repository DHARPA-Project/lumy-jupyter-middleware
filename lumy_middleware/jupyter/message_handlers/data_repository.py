import logging
from typing import Any, Dict, List, cast

import pyarrow as pa
from kiara.data.values import Value
from lumy_middleware.context.dataregistry import DataRegistryItem, IsIn
from lumy_middleware.context.kiara.table_utils import \
    filter_table_with_pagination
from lumy_middleware.jupyter.base import MessageHandler
from lumy_middleware.types.generated import (MsgDataRepositoryFindItems,
                                             MsgDataRepositoryGetItemValue,
                                             MsgDataRepositoryItems,
                                             MsgDataRepositoryItemValue,
                                             TableStats)
from lumy_middleware.utils.codec import serialize
from lumy_middleware.utils.dataclasses import to_dict

logger = logging.getLogger(__name__)


def get_column_names(metadata: Dict[str, Any]) -> List[str]:
    return metadata.get('table', {}).get('column_names', [])


def get_column_types(metadata: Dict[str, Any]) -> List[str]:
    columns = get_column_names(metadata)
    schema = metadata.get('table', {}).get('schema', {})
    return [
        schema.get(c, {}).get('arrow_type_name', 'unknown')
        for c in columns
    ]


def as_table(items: List[DataRegistryItem]) -> pa.Table:
    '''
    TODO: This is a basic implementation. Need to allow
    the method caller to select which metadata columns
    are included.
    '''
    return pa.Table.from_pydict({
        'id': [i.id for i in items],
        'label': [i.label for i in items],
        'type': [i.type for i in items],
        'columnNames': [get_column_names(i.metadata) for i in items],
        'columnTypes': [get_column_types(i.metadata) for i in items],
    })


class DataRepositoryHandler(MessageHandler):
    def _handle_FindItems(self, msg: MsgDataRepositoryFindItems):
        if msg.filter.types is not None and len(msg.filter.types) > 0:
            batch = self.context.data_registry.find(
                type=IsIn(msg.filter.types)
            )
        else:
            batch = self.context.data_registry.find()

        offset = msg.filter.offset or 0
        page_size = msg.filter.page_size or 5

        filtered_items: List[DataRegistryItem] = batch[offset:offset+page_size]
        filtered_items_table = as_table(filtered_items)

        serialized_filtered_items = serialize(filtered_items_table)
        stats = TableStats(rows_count=len(batch))

        return MsgDataRepositoryItems(
            filter=msg.filter,
            items=serialized_filtered_items.value,
            stats=cast(Any, to_dict(stats)))

    def _handle_GetItemValue(self, msg: MsgDataRepositoryGetItemValue):
        value: Value = self.context.data_registry.get_item_value(msg.item_id)
        # TODO: This will be updated when abstract filtering is implemented
        # For now we just support original "table" values
        if value.type_name == 'table':
            table: pa.Table = value.get_value_data()
            data = filter_table_with_pagination(table, msg.filter)

            return MsgDataRepositoryItemValue(
                item_id=msg.item_id,
                type='table',
                value=serialize(data).value,
                filter=msg.filter,
                metadata=cast(Any, to_dict(TableStats(rows_count=len(table))))
            )
