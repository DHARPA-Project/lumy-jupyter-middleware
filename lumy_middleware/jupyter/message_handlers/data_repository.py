import logging
from typing import Any, cast

import pyarrow as pa
from lumy_middleware.context.dataregistry import DataRegistryItem, IsIn
from lumy_middleware.dev.data_registry.mock import MockDataRegistry
from lumy_middleware.jupyter.base import MessageHandler
from lumy_middleware.types.generated import (MsgDataRepositoryFindItems,
                                             MsgDataRepositoryItems,
                                             TableStats)
from lumy_middleware.utils.codec import serialize
from lumy_middleware.utils.dataclasses import to_dict

logger = logging.getLogger(__name__)


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
        'columnNames': [i.metadata.get('columnNames', []) for i in items],
        'columnTypes': [i.metadata.get('columnTypes', []) for i in items],
    })


class DataRepositoryHandler(MessageHandler):
    def _handle_FindItems(self, msg: MsgDataRepositoryFindItems):
        registry = MockDataRegistry.get_instance()

        if msg.filter.types is not None and len(msg.filter.types) > 0:
            batch = registry.find(type=IsIn(msg.filter.types))
        else:
            batch = registry.find()

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
