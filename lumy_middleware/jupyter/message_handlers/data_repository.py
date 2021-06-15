import logging
from typing import Any, cast

from lumy_middleware.dev.data_registry.mock import MockDataRegistry
from lumy_middleware.jupyter.base import MessageHandler
from lumy_middleware.types.generated import (MsgDataRepositoryFindItems,
                                             MsgDataRepositoryItems,
                                             TableStats)
from lumy_middleware.utils.codec import serialize
from lumy_middleware.utils.dataclasses import to_dict

logger = logging.getLogger(__name__)


class DataRepositoryHandler(MessageHandler):
    def _handle_FindItems(self, msg: MsgDataRepositoryFindItems):
        filtered_items = MockDataRegistry.get_instance().filter_items(
            msg.filter.offset or 0,
            msg.filter.page_size or 5,
            msg.filter.types)

        serialized_filtered_items, _ = serialize(filtered_items)
        stats = TableStats(
            rows_count=MockDataRegistry.get_instance().get_total_items())

        return MsgDataRepositoryItems(
            filter=msg.filter,
            items=serialized_filtered_items,
            stats=cast(Any, to_dict(stats)))
