import logging
from typing import Mapping

from kiara import KiaraModule
from kiara.module import StepInputs, StepOutputs
from kiara.data.values import ValueSchema
from networkx import Graph, to_pandas_edgelist
from pandas import DataFrame
from pyarrow import Table

logger = logging.getLogger(__name__)


class GraphToNodesTableTransformationModule(KiaraModule):
    _module_type_name = 'to_nodes_table'

    def create_input_schema(self) -> Mapping[str, ValueSchema]:
        return {
            'source': ValueSchema(type='network_graph'),
            'node_id_column': ValueSchema(type='string', default='id')
        }

    def create_output_schema(self) -> Mapping[str, ValueSchema]:
        return {
            'target': ValueSchema(type='table')
        }

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:
        graph: Graph = inputs.get_value_data('source')
        node_id_column: str = inputs.get_value_data('node_id_column')

        nodes = [
            {**node_attrs, node_id_column: node_id}
            for node_id, node_attrs in graph.nodes.data()
        ]
        df = DataFrame.from_records(nodes)
        table = Table.from_pandas(df)
        outputs.set_value('target', table)


class GraphToEdgesTableTransformationModule(KiaraModule):
    _module_type_name = 'to_edges_table'

    def create_input_schema(self) -> Mapping[str, ValueSchema]:
        return {
            'source': ValueSchema(type='network_graph')
        }

    def create_output_schema(self) -> Mapping[str, ValueSchema]:
        return {
            'target': ValueSchema(type='table')
        }

    def process(self, inputs: StepInputs, outputs: StepOutputs) -> None:
        graph: Graph = inputs.get_value_data('source')
        df = to_pandas_edgelist(graph, "source", "target")
        table = Table.from_pandas(df)
        outputs.set_value('target', table)
