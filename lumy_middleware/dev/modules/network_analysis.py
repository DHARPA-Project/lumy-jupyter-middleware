import logging
from typing import List, Mapping, cast

import pandas as pd
import pyarrow as pa
from kiara import Kiara
from kiara.data.values import ValueSchema
from kiara.module import KiaraModule, StepInputs, StepOutputs

logger = logging.getLogger(__name__)

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

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:
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
