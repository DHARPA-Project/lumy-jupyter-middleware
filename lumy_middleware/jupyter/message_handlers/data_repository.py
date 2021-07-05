import logging
from typing import Any, cast

import pyarrow as pa
from lumy_middleware.context.dataregistry import DataRegistryItem, IsIn
from lumy_middleware.jupyter.base import MessageHandler
from lumy_middleware.types.generated import (MsgDataRepositoryFindItems,
                                             MsgDataRepositoryItems,
                                             TableStats)
from lumy_middleware.utils.codec import serialize
from lumy_middleware.utils.dataclasses import to_dict

logger = logging.getLogger(__name__)


def get_column_names(metadata: dict[str, Any]) -> list[str]:
    return metadata.get('table', {}).get('column_names', [])


def get_column_types(metadata: dict[str, Any]) -> list[str]:
    columns = get_column_names(metadata)
    schema = metadata.get('table', {}).get('schema', {})
    return [
        schema.get(c, {}).get('arrow_type_name', 'unknown')
        for c in columns
    ]


def as_table(items: list[DataRegistryItem]) -> pa.Table:
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
        self.context.data_registry

        if msg.filter.types is not None and len(msg.filter.types) > 0:
            batch = self.context.data_registry.find(
                type=IsIn(msg.filter.types)
            )
        else:
            batch = self.context.data_registry.find()

        offset = msg.filter.offset or 0
        page_size = msg.filter.page_size or 5

        filtered_items: list[DataRegistryItem] = batch[offset:offset+page_size]
        filtered_items_table = as_table(filtered_items)

        serialized_filtered_items, _ = serialize(filtered_items_table)
        stats = TableStats(rows_count=len(batch))

        return MsgDataRepositoryItems(
            filter=msg.filter,
            items=serialized_filtered_items,
            stats=cast(Any, to_dict(stats)))
