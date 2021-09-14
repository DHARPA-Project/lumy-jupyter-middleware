import logging
from typing import List, Mapping, cast

import pandas as pd
import pyarrow as pa
from kiara import Kiara
from kiara.data.values import ValueSchema
from kiara.module import KiaraModule, ValueSet
from lumy_middleware.context.kiara.dataregistry import get_value_label

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

        def get_value(id: str):
            '''
            TODO: This will go away when workflow is refactored.
            registry should not be accessible from modules code.
            '''
            return self._kiara.data_store.get_value_obj(id)

        fields = ['id', 'label', 'type', 'columnNames', 'columnTypes']
        selected_items = [
            {
                'id': id,
                'label': get_value_label(get_value(id), self._kiara),
                'type': get_value(id).type_name,
                'columnNames': get_value(id)
                .get_metadata('table').get('table', {})
                .get('column_names', []),
                'columnTypes': [
                    v['arrow_type_name']
                    for v in get_value(id)
                    .get_metadata('table').get('table', {}).get('schema', {})
                    .values()
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


# Value of any column in the mapping table
MappingItemStruct = pa.struct([
    # ID of the tabular data item
    pa.field(name='id', type=pa.utf8(), nullable=False),
    # Column name from the data item
    pa.field(name='column', type=pa.utf8(), nullable=False)
])


def get_value_data(id: str, kiara: Kiara):
    '''
    TODO: This will go away when workflow is refactored.
    registry should not be accessible from modules code.
    '''
    # NOTE: If using mock data registry - the commented
    # code below should be used instead.
    # registry = IpythonKernelController.get_instance() \
    #     ._context.data_registry
    # return registry.get_item_value(id)
    return kiara.data_store.get_value_data(id)


def build_table_from_mapping(
    mapping_table: pa.Table,
    column_names: List[str],
    kiara: Kiara
) -> pa.Table:
    mapping = mapping_table.to_pydict()

    table = pd.DataFrame()

    def get_data_item_for_column_mapping(column_mapping):
        data_item = cast(pa.Table, get_value_data(column_mapping['id'], kiara))
        data_item_df = data_item.to_pandas()
        if column_mapping['column'] not in data_item_df:
            return None
        else:
            return data_item_df[column_mapping['column']]

    for column_name in column_names:
        column_mappings = mapping.get(column_name, [])
        if len(column_mappings) > 0:
            items = [
                get_data_item_for_column_mapping(m)
                for m in column_mappings
            ]
            non_null_items = [
                item
                for item in items
                if item is not None
            ]
            if len(non_null_items) > 0:
                table[column_name] = pd.concat(non_null_items)

    if len(table) == 0:
        table = pd.DataFrame(columns=column_names)

    # NOTE: this is probably not right, but ok to start with
    # Converting mixed type columns to string.
    # Otherwise pyarrow will throw an exception
    for column in table:
        if table[column].dtype.name == 'object':
            table[column] = table[column].astype('str')

    return pa.Table.from_pandas(table, preserve_index=False)


class NetworkAnalysisDataMappingModule(KiaraModule):
    _module_type_name = 'data_mapping'

    def create_input_schema(self) -> Mapping[str, ValueSchema]:
        return {
            "corpus": ValueSchema(
                type="table", doc="Corpus items."
            ),
            "nodesMappingTable": ValueSchema(
                type="table", doc="Nodes mapping table.",
                optional=True
            ),
            "edgesMappingTable": ValueSchema(
                type="table", doc="Edges mapping table.",
                optional=True
            )
        }

    def create_output_schema(self) -> Mapping[str, ValueSchema]:
        return {
            "nodes": ValueSchema(
                type="table",
                doc="Nodes table.",
            ),
            "edges": ValueSchema(
                type="table",
                doc="Edges table.",
            )
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
        nodes_mapping_table = inputs.get_value_data(
            'nodesMappingTable') or pa.Table.from_pydict({})
        nodes = build_table_from_mapping(
            nodes_mapping_table,
            ['id', 'label', 'group'],
            self._kiara
        )

        edges_mapping_table = inputs.get_value_data(
            'edgesMappingTable') or pa.Table.from_pydict({})
        edges = build_table_from_mapping(
            edges_mapping_table,
            ['source', 'target', 'weight'],
            self._kiara
        )
        outputs.set_values(nodes=nodes, edges=edges)
