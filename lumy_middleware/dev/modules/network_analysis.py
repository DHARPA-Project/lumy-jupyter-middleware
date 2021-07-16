import copy
import logging
from typing import List, Mapping, Optional, cast

import networkx as nx
import numpy as np
import pandas as pd
import pyarrow as pa
from kiara import Kiara
from kiara.data.values import ValueSchema
from kiara.module import KiaraModule, ValueSet
from kiara.module_config import KiaraModuleConfig
from networkx import (DiGraph, Graph, density, is_directed, isolates,
                      set_node_attributes)
from pydantic.fields import Field

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
    return kiara.data_registry.get_value_item(id).get_value_data()


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


class NetworkAnalysisDataVisModuleConfig(KiaraModuleConfig):
    use_graph: Optional[bool] = Field(
        description='''
        If set to true, the module expects a graph object in 'graph'
        input. Otherwise it will construct the graph from two
        table: 'nodes' and 'edges'.
        ''',
        default=False,
    )


class NetworkAnalysisDataVisModule(KiaraModule):
    _config_cls = NetworkAnalysisDataVisModuleConfig

    def create_input_schema(self) -> Mapping[str, ValueSchema]:
        schema = {
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

        if self.get_config_value("use_graph"):
            schema['graph'] = ValueSchema(
                type="network_graph",
                doc="Graph",
            )
        else:
            schema['nodes'] = ValueSchema(
                type="table",
                doc="Nodes table.",
            )
            schema['edges'] = ValueSchema(
                type="table",
                doc="Edges table.",
            )

        return schema

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
        if self.get_config_value("use_graph"):
            graph: nx.Graph = inputs.get_value_data('graph')
        else:
            edges = inputs.get_value_data('edges').to_pandas()

            graph = nx.from_pandas_edgelist(
                edges,
                "source", "target",
                edge_attr=True,
                create_using=nx.DiGraph()
            )

            nodes = inputs.get_value_data('nodes').to_pandas()
            graph.add_nodes_from(nodes.set_index(
                'id').to_dict('index').items())

        ids = list(graph.nodes())
        if len(ids) > 0:
            degree_dict = dict(graph.degree(graph.nodes()))
            betweenness_dict = nx.betweenness_centrality(graph)
            eigenvector_dict = nx.eigenvector_centrality(graph)
        else:
            degree_dict = {}
            betweenness_dict = {}
            eigenvector_dict = {}

        isolated_nodes_ids = list(nx.isolates(graph))

        graph_data = pa.Table.from_pydict({
            'degree': [degree_dict[i] for i in ids],
            'eigenvector': [eigenvector_dict[i] for i in ids],
            'betweenness': [betweenness_dict[i] for i in ids],
            'isIsolated': [i in isolated_nodes_ids for i in ids],
            # TODO: what was "isLarge" exactly? It is not
            # currently used in the visualisation.
            'isLarge': np.random.rand(*[len(ids)]) > 0.5
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

    class AddCentralityCalculationsModule(KiaraModule):

        def create_input_schema(self) -> Mapping[str, ValueSchema]:
            return {
                'graph': ValueSchema(type='network_graph'),
                'degree_property_name': ValueSchema(
                    type='string',
                    default='degree'
                ),
                'indegree_property_name': ValueSchema(
                    type='string',
                    default='indegree'
                ),
                'outdegree_property_name': ValueSchema(
                    type='string',
                    default='outdegree'
                ),
                'isolated_property_name': ValueSchema(
                    type='string',
                    default='isolated'
                ),
                'betweenness_property_name': ValueSchema(
                    type='string',
                    default='betweenness'
                ),
                'eigenvector_property_name': ValueSchema(
                    type='string',
                    default='eigenvector'
                ),
            }

        def create_output_schema(self) -> Mapping[str, ValueSchema]:
            return {
                'graph': ValueSchema(type='network_graph'),
            }

        def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
            graph: Graph = inputs.get_value_data("graph")
            graph = copy.deepcopy(graph)

            # degree
            degree_dict = graph.degree()
            set_node_attributes(
                graph,
                dict(degree_dict),
                inputs.get_value_data('degree_property_name')
            )

            # isolated
            isolated_flag_dict = {id: True for id in isolates(graph)}
            set_node_attributes(
                graph,
                isolated_flag_dict,
                inputs.get_value_data('isolated_property_name')
            )

            if is_directed(graph):
                graph = cast(DiGraph, graph)

                # indegree
                indegree_dict = graph.in_degree()
                set_node_attributes(
                    graph,
                    dict(indegree_dict),
                    inputs.get_value_data('indegree_property_name')
                )

                # outdegree
                outdegree_dict = graph.out_degree()
                set_node_attributes(
                    graph,
                    dict(outdegree_dict),
                    inputs.get_value_data('outdegree_property_name')
                )

            # eigenvector
            # betweenness
            betweenness_dict = nx.betweenness_centrality(graph)
            eigenvector_dict = nx.eigenvector_centrality(graph)

            set_node_attributes(
                graph,
                betweenness_dict,
                inputs.get_value_data('betweenness_property_name')
            )
            set_node_attributes(
                graph,
                eigenvector_dict,
                inputs.get_value_data('eigenvector_property_name')
            )

            outputs.set_value('graph', graph)


class ExtractGraphPropertiesModule(KiaraModule):
    """Extract inherent properties of a network graph."""

    _module_type_name = "graph_properties"

    def create_input_schema(self) -> Mapping[str, ValueSchema]:
        return {
            'graph': ValueSchema(type='network_graph'),
        }

    def create_output_schema(self) -> Mapping[str, ValueSchema]:
        return {
            'nodesCount': ValueSchema(type='integer'),
            'edgesCount': ValueSchema(type='integer'),
            'density': ValueSchema(type='float'),
            'averageDegree': ValueSchema(type='float', optional=True),
            'averageInDegree': ValueSchema(type='float', optional=True),
            'averageOutDegree': ValueSchema(type='float', optional=True),
            'averageShortestPathLength': ValueSchema(type='float',
                                                     optional=True),
        }

    def process(self, inputs: ValueSet, outputs: ValueSet) -> None:
        graph: Graph = inputs.get_value_data("graph")

        nodes_count = graph.number_of_nodes()

        output_values = {
            'nodesCount': nodes_count,
            'edgesCount': graph.number_of_edges(),
            'density': density(graph)
        }

        if nodes_count > 0:
            if is_directed(graph):
                graph = cast(DiGraph, graph)
                output_values['averageInDegree'] = sum(
                    d for _, d in graph.in_degree()) / float(nodes_count)
                output_values['averageOutDegree'] = sum(
                    d for _, d in graph.out_degree()) / float(nodes_count)
            else:
                output_values['averageDegree'] = sum(
                    d for _, d in graph.degree()) / float(nodes_count)

        try:
            output_values['averageShortestPathLength'] = \
                nx.average_shortest_path_length(graph)
        except Exception:
            # TODO: This sometimes throws this error:
            # networkx.exception.NetworkXError: Graph is not weakly connected.
            # Look into it when there is a moment
            pass

        outputs.set_values(**output_values)
