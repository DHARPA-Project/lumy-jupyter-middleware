from typing import Mapping

from kiara.data.values import ValueSchema
from kiara.module import KiaraModule, ValueSet
from lumy_middleware.dev.data_registry.mock import MockDataRegistry


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
        selected_items_ids = inputs.get_value_data('selectedItemsIds') or []
        registry = MockDataRegistry.get_instance()
        selected_items = [
            registry.get_item_value(id).get_value_data()
            for id in selected_items_ids
        ]
        outputs.set_value('selectedItems', selected_items)
