import logging
from typing import List, Mapping, cast

import networkx as nx
import numpy as np
import pandas as pd
import pyarrow as pa
from kiara.data.values import ValueSchema
from kiara.module import KiaraModule, ValueSet
from lumy_middleware.dev.data_registry.mock import MockDataRegistry

logger = logging.getLogger(__name__)

# Value of any column in the mapping table
MappingItemStruct = pa.struct([
    # ID of the tabular data item
    pa.field(name='id', type=pa.utf8(), nullable=False),
    # Column name from the data item
    pa.field(name='column', type=pa.utf8(), nullable=False)
])


def build_table_from_mapping(
    mapping_table: pa.Table,
    column_names: List[str]
) -> pa.Table:
    mapping = mapping_table.to_pydict()

    table = pd.DataFrame()

    def get_data_item_for_column_mapping(column_mapping):
        data_item = cast(pa.Table, MockDataRegistry
                         .get_instance()
                         .get_file_content(column_mapping['id']))
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

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
        nodes_mapping_table = inputs.get_value_data(
            'nodesMappingTable') or pa.Table.from_pydict({})
        nodes = build_table_from_mapping(
            nodes_mapping_table,
            ['id', 'label', 'group']
        )
        outputs.set_value('nodes', nodes)

        edges_mapping_table = inputs.get_value_data(
            'edgesMappingTable') or pa.Table.from_pydict({})
        edges = build_table_from_mapping(
            edges_mapping_table,
            ['srcId', 'tgtId', 'weight']
        )
        outputs.set_value('edges', edges)


class NetworkAnalysisDataVisModule(KiaraModule):

    def create_input_schema(self) -> Mapping[str, ValueSchema]:
        return {
            "nodes": ValueSchema(
                type="table",
                doc="Nodes table.",
            ),
            "edges": ValueSchema(
                type="table",
                doc="Edges table.",
            ),
            "shortestPathSource": ValueSchema(
                type="any",
                doc="ID of the start node for the shortest path calculations",
                optional=True,
                default=None
            ),
            "shortestPathTarget": ValueSchema(
                type="any",
                doc="ID of the end node for the shortest path calculations",
                optional=True,
                default=None
            ),
            "selectedNodeId": ValueSchema(
                type="any",
                doc="ID of the selected node to calculate direct neighbours",
                optional=True,
                default=None
            ),
        }

    def create_output_schema(self) -> Mapping[str, ValueSchema]:
        return {
            "graphData": ValueSchema(
                type="table",
                doc="Nodes table.",
            ),
            "shortestPath": ValueSchema(
                type="any",
                doc="Shortest path array.",
            ),
            "directConnections": ValueSchema(
                type="any",
                doc="Ids of the direct connections of the 'selectedNodeId'",
            ),
            "graphStats": ValueSchema(
                type="dict",
                doc="Graph stats",
            ),
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
        edges = inputs.get_value_data('edges').to_pandas()

        graph: nx.Graph = nx.from_pandas_edgelist(
            edges,
            "srcId", "tgtId",
            edge_attr=True,
            create_using=nx.DiGraph()
        )

        nodes = inputs.get_value_data('nodes').to_pandas()
        graph.add_nodes_from(nodes.set_index(
            'id').to_dict('index').items())

        if len(nodes) > 0:
            degree_dict = dict(graph.degree(graph.nodes()))
            betweenness_dict = nx.betweenness_centrality(graph)
            eigenvector_dict = nx.eigenvector_centrality(graph)
        else:
            degree_dict = {}
            betweenness_dict = {}
            eigenvector_dict = {}

        isolated_nodes_ids = list(nx.isolates(graph))

        ids = inputs.get_value_data('nodes')['id'].to_numpy()

        graph_data = pa.Table.from_pydict({
            'degree': [degree_dict[i] for i in ids],
            'eigenvector': [eigenvector_dict[i] for i in ids],
            'betweenness': [betweenness_dict[i] for i in ids],
            'isIsolated': [i in isolated_nodes_ids for i in ids],
            # TODO: what was "isLarge" exactly? It is not
            # currently used in the visualisation.
            'isLarge': np.random.rand(*ids.shape) > 0.5
        })
        outputs.set_value('graphData', graph_data)

        # shortest path
        shortest_path_source = inputs.get_value_data('shortestPathSource')
        shortest_path_target = inputs.get_value_data('shortestPathTarget')

        if shortest_path_source in ids \
                and shortest_path_target in ids:
            shortest_path = nx.shortest_path(
                graph,
                source=shortest_path_source,
                target=shortest_path_target
            )
            outputs.set_value('shortestPath', shortest_path)
        else:
            outputs.set_value('shortestPath', [])

        # direct connections
        selected_node_id = inputs.get_value_data('selectedNodeId')
        if selected_node_id is not None and selected_node_id in graph:
            direct_connections = list(graph.neighbors(id))
            outputs.set_value('directConnections', direct_connections)
        else:
            outputs.set_value('directConnections', [])

        # stats
        num_nodes = graph.number_of_nodes()
        if num_nodes == 0:
            num_nodes = 1  # to avoid division by zero down below
        # TODO: This sometimes throws this error:
        # networkx.exception.NetworkXError: Graph is not weakly connected.
        # Look into it when there is a moment
        try:
            avg_shortest_path_len = nx.average_shortest_path_length(graph)
        except Exception:
            avg_shortest_path_len = 0

        graph_stats = {
            'nodesCount': nx.number_of_nodes(graph),
            'edgesCount': nx.number_of_edges(graph),
            'averageInDegree':
            sum(d for _, d in graph.in_degree()) / float(num_nodes),
            'averageOutDegree':
            sum(d for _, d in graph.out_degree()) / float(num_nodes),
            'density': nx.density(graph),
            'averageShortestPathLength': avg_shortest_path_len
        }
        outputs.set_value('graphStats', graph_stats)
