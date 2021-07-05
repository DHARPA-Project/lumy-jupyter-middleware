import logging
from typing import Mapping
import pyarrow as pa

from kiara.data.values import ValueSchema
from kiara.module import KiaraModule, ValueSet
from lumy_middleware.dev.data_registry.mock import MockDataRegistry

logger = logging.getLogger(__name__)


class DataSelectionModule(KiaraModule):

    def create_input_schema(self) -> Mapping[str, ValueSchema]:
        return {
            "selectedItemsIds": ValueSchema(
                type="any", doc="URIs of selected items.", default=[]
            ),
            "metadataFields": ValueSchema(
                type="any", doc="Metadata field.", default=[]
            )
        }

    def create_output_schema(self) -> Mapping[str, ValueSchema]:
        return {
            "selectedItems": ValueSchema(
                type="table",
                doc="Selected corpus.",
            )
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
        selected_items_ids = inputs.get_value_data(
            'selectedItemsIds') or []
        registry = MockDataRegistry.get_instance()
        fields = ['id', 'label', 'type', 'columnNames', 'columnTypes']
        selected_items = [
            {
                'id': id,
                'label': id,
                'type': registry.get_item_value(id).type_name,
                'columnNames': registry.get_item_value(id)
                .metadata['table']['column_names'],
                'columnTypes': [
                    v['arrow_type_name']
                    for v in registry.get_item_value(id)
                    .metadata['table']['schema'].values()
                ]
            }
            for id in selected_items_ids
        ]

        selected_items = pa.Table.from_pydict({
            k: [i[k] for i in selected_items]
            for k in fields
        }, pa.schema({
            'id': pa.utf8(),
            'label': pa.utf8(),
            'type': pa.utf8(),
            'columnNames': pa.list_(pa.utf8()),
            'columnTypes': pa.list_(pa.utf8())
        }))
        outputs.set_value('selectedItems', selected_items)
