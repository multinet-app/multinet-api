from typing import List, Optional

from django.shortcuts import get_object_or_404
from drf_yasg.utils import swagger_auto_schema
from rest_framework import serializers, status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework_extensions.mixins import DetailSerializerMixin

from multinet.api.auth.decorators import require_workspace_permission
from multinet.api.models import Network, Table, Workspace, WorkspaceRoleChoice
from multinet.api.tasks.upload.csv import create_csv_network
from multinet.api.utils.arango import ArangoQuery
from multinet.api.views.serializers import (
    CSVNetworkCreateSerializer,
    LimitOffsetSerializer,
    NetworkCreateSerializer,
    NetworkReturnDetailSerializer,
    NetworkReturnSerializer,
    NetworkSerializer,
    NetworkTablesSerializer,
    PaginatedResultSerializer,
    TableReturnSerializer,
    NodeStatsSerializer,
    NodeStatsQuerySerializer,
    NodeAndEdgeFilteredQuerySerializer,
    NodeStatsAllFieldsSerializer,
)

from .common import ArangoPagination, MultinetPagination, WorkspaceChildMixin

# added for graph library exploration
import networkx as nx
import arrow
from django.http import JsonResponse
import json


## -------------------- CRL added supporting routines for graph manipulation using networkX

# develop accessor function to return node index from _key. Be tolerant of
# table names preceding the unique node name
def nodeKeyToNumber(g,namestring):
    if '/' in namestring:
        # chop off the table name
        namestring = namestring.split('/')[1]
    names = nx.get_node_attributes(g,'_key')
    for name in names:
        # chop of the table name
        nodename = names[name]
        if '/' in nodename:
            nodename = nodename.split('/')[1]
        if str(nodename) == str(namestring):
            return name

def buildGraph_netX(node_list,edge_list):
    # create empty directed graph.  All ArangoDB graphs are directed by default
    g = nx.DiGraph()
    
    # insert nodes
    node_index = 1
    for node in node_list:
        for attrib in node:
            g.add_node(node_index)
            if attrib not in ['gt_object','used_by_edge','index']:
                g.nodes[node_index][attrib] = node[attrib]
        node_index+=1
        
    # insert edges
    for edge in edge_list:
        sourceNode = nodeKeyToNumber(g,edge['_from'])
        destNode = nodeKeyToNumber(g,edge['_to'])
        g.add_edge(sourceNode,destNode)
        for attrib in edge:
            if attrib not in ['_from','_to']:
                g[sourceNode][destNode][attrib] = edge[attrib]
    # return the nx graph object
    return g

# return a subset of a graph by filtering by node in or out degree
def subsetGraph_netX(g,algorithm,threshold):
    def in_node_filter(index):
        if algorithm == 'in_degree':
            return (g.in_degree(index)>threshold)
    def out_node_filter(index):
            return (g.out_degree(index)>threshold)
    if algorithm == 'in_degree':
        view = nx.subgraph_view(g, filter_node=in_node_filter)
    else:
        view = nx.subgraph_view(g, filter_node=out_node_filter)
    return view


def calculateNodeCentrality_netX(g):
    returnValues = nx.degree_centrality(g)
    return returnValues

def calculateBetweenness_netX(g):
    returnValues = nx.betweenness_centrality(g)
    return returnValues
    

# --------------- end of graph support routines




class NetworkCreationErrorSerializer(serializers.Serializer):
    missing_node_tables = serializers.ListField(child=serializers.CharField())
    missing_table_keys = serializers.DictField(
        child=serializers.ListField(child=serializers.CharField())
    )


# TODO: Use for validation once it's added
def validate_edge_table(
    workspace: Workspace, edge_table: Table, node_tables: List[str]
) -> Optional[Response]:
    """If there is a validation error, this method returns the error response."""
    missing_node_tables = []
    missing_table_keys = {}
    for table, keys in node_tables.items():
        query = Table.objects.filter(workspace=workspace, name=table)
        if query.count() == 0:
            missing_node_tables.append(table)
            continue

        nt: Table = query[0]
        for key in keys:
            row = nt.get_row({'_key': key})
            if row.count() == 0:
                if missing_table_keys.get(table):
                    missing_table_keys[table].add(key)
                else:
                    missing_table_keys[table] = {key}

    if missing_node_tables or missing_table_keys:
        serialized_resp = NetworkCreationErrorSerializer(
            data={
                'missing_node_tables': missing_node_tables,
                'missing_table_keys': missing_table_keys,
            }
        )

        serialized_resp.is_valid(raise_exception=True)
        return Response(serialized_resp.data, status=status.HTTP_400_BAD_REQUEST)


class NetworkViewSet(WorkspaceChildMixin, DetailSerializerMixin, ReadOnlyModelViewSet):
    queryset = Network.objects.all().select_related('workspace')
    lookup_field = 'name'

    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = NetworkReturnSerializer
    serializer_detail_class = NetworkReturnDetailSerializer

    pagination_class = MultinetPagination

    # Categorize entire ViewSet
    swagger_tags = ['networks']

    @swagger_auto_schema(
        request_body=NetworkCreateSerializer(),
        responses={200: NetworkReturnSerializer()},
    )
    @require_workspace_permission(WorkspaceRoleChoice.WRITER)
    def create(self, request, parent_lookup_workspace__name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        edge_table: Table = get_object_or_404(
            Table, workspace=workspace, name=request.data.get('edge_table')
        )

        serializer = NetworkSerializer(
            data={
                'name': request.data.get('name'),
                'workspace': workspace.pk,
            }
        )
        serializer.is_valid(raise_exception=True)

        node_tables = edge_table.find_referenced_node_tables()
        if not node_tables:
            return Response(
                'Cannot create network with empty edge table', status=status.HTTP_400_BAD_REQUEST
            )

        network = Network.create_with_edge_definition(
            name=serializer.validated_data['name'],
            workspace=workspace,
            edge_table=edge_table.name,
            node_tables=list(node_tables.keys()),
        )

        return Response(NetworkReturnDetailSerializer(network).data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        request_body=CSVNetworkCreateSerializer(),
        responses={200: NetworkReturnSerializer()},
    )
    @action(detail=False, methods=['POST'])
    @require_workspace_permission(WorkspaceRoleChoice.WRITER)
    def from_tables(self, request, parent_lookup_workspace__name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)

        serializer = CSVNetworkCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        # Ensure network doesn't already exist
        network_name = serializer.validated_data['name']
        if Network.objects.filter(workspace=workspace, name=network_name).first():
            return Response('Network already exists', status=status.HTTP_400_BAD_REQUEST)

        network = create_csv_network(workspace, serializer)
        return Response(NetworkReturnDetailSerializer(network).data, status=status.HTTP_200_OK)

    @require_workspace_permission(WorkspaceRoleChoice.WRITER)
    def destroy(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        network: Network = get_object_or_404(Network, workspace=workspace, name=name)
        network.delete()

        return Response(None, status=status.HTTP_204_NO_CONTENT)



    @swagger_auto_schema(
        query_serializer=LimitOffsetSerializer(),
        responses={200: PaginatedResultSerializer()},
    )
    @action(detail=True, url_path='nodes')
    @require_workspace_permission(WorkspaceRoleChoice.READER)
    def nodes(self, request, parent_lookup_workspace__name: str, name: str):
        # Doesn't use the Network.nodes method, in order to do proper pagination.

        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        network: Network = get_object_or_404(Network, workspace=workspace, name=name)

        pagination = ArangoPagination()
        query = ArangoQuery.from_collections(workspace.get_arango_db(), network.node_tables())
        paginated_query = pagination.paginate_queryset(query, request)

        return pagination.get_paginated_response(paginated_query)

#-------- add algorithm calls

    @swagger_auto_schema(
        query_serializer=NodeStatsQuerySerializer(),
        responses={200: NodeStatsSerializer()},
    )
    @action(detail=True, url_path='nodes_stats')
    @require_workspace_permission(WorkspaceRoleChoice.READER)
    def nodes_stats(self, request, parent_lookup_workspace__name: str, name: str):

        # algorithm value selects which algorithm to run
        # options are: in-degree, out-degree, pagerank, node-centrality, betweenness

        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        network: Network = get_object_or_404(Network, workspace=workspace, name=name)

        # get the algorithm option from the query serializer
        serializer = NodeStatsQuerySerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        algorithm = serializer.validated_data['algorithm']

        #print('received algorithm choice:',algorithm)

        # traverse through arango cursors to get data into lists
        timestamps = []
        timestamps.append(('before cursor read',arrow.now()))

        # iterate through Arango cursors to extract the node and edge data
        # arango queries are buffered by default pagination of 
        # 1000 entries, so loop through all the batches
        edge_list = []
        node_list = []

        edges = network.edges()
        # loop until there are no more batches waiting
        if edges.has_more():
            while edges.has_more():
                while not edges.empty():
                    edge = edges.next()
                    edge_list.append(edge)
                # advance to the last partial batch
                edges.fetch()
        # get the last partial batch
        while not edges.empty():
            edge = edges.next()
            edge_list.append(edge)

        nodes = network.nodes()
        # loop until there are no more batches waiting
        if nodes.has_more():
            while nodes.has_more():
                while not nodes.empty():
                    node = nodes.next()
                    node_list.append(node)
                nodes.fetch()
        # get the last partial batch
        while not nodes.empty():
            node = nodes.next()
            node_list.append(node)



        timestamps.append(('after cursor read',arrow.now()))
        edgecount = network.edge_count

        #print('network has:',edgecount,'edges')
        #print('found',len(node_list),'nodes and ',len(edge_list),'edges')   
        # print a sample to observe we are traversing correctly
        #print('node[2]:',node_list[2])   
        #print('edge[2]:',edge_list[2])   

        timestamps.append(('before graph creation',arrow.now()))
        nxNetwork = buildGraph_netX(node_list,edge_list)
        timestamps.append(('after graph creation',arrow.now()))

        if algorithm == 'node_centrality':
            algorithmReturn = calculateNodeCentrality_netX(nxNetwork)
        elif algorithm == 'betweenness':
            algorithmReturn = calculateBetweenness_netX(nxNetwork)
        elif algorithm == 'pagerank':
            algorithmReturn = nx.pagerank(nxNetwork)
        elif algorithm == 'all':
            nodeCentrality_result = calculateNodeCentrality_netX(nxNetwork)
            betweenness_result = calculateBetweenness_netX(nxNetwork)
            pagerank_result = nx.pagerank(nxNetwork)
        
        timestamps.append(('after algorithm completed',arrow.now()))


        node_stat_list = []
        for index,node in enumerate(node_list):
            entry = {}
            # add in the node _key for reference
            entry['_key'] = node['_key']
            # fill in the result value according to what algorithm was selected
            if algorithm in ['node_centrality','betweenness']:
                entry['result'] = algorithmReturn[index+1]
            elif algorithm == 'in_degree':
                entry['result'] = nxNetwork.in_degree(index+1)
            elif algorithm == 'out_degree':
                entry['result'] = nxNetwork.out_degree(index+1)
            elif algorithm == 'node_centrality':
                entry['result'] = algorithmReturn[index+1]
            elif algorithm == 'pagerank':
                entry['result'] = algorithmReturn[index+1]
            elif algorithm == 'all':
                entry['in_degree'] = nxNetwork.in_degree(index+1)
                entry['out_degree'] = nxNetwork.out_degree(index+1)
                entry['node_centrality'] = nodeCentrality_result[index+1]
                entry['betweenness'] = betweenness_result[index+1]
                entry['pagerank'] = pagerank_result[index+1]
            else:
                # return out-degree as a default
                entry['result'] = nxNetwork.out_degree(index+1)
            node_stat_list.append(entry)

        #for stamp in range(len(timestamps)-1):
        #    diff = timestamps[stamp+1][1]-timestamps[stamp][1]
        #    print(timestamps[stamp+1][0],diff)

        if algorithm == 'all':
            serializer = NodeStatsAllFieldsSerializer(node_stat_list, many=True)
        else:
            serializer = NodeStatsSerializer(node_stat_list, many=True)
        return Response(serializer.data,status=status.HTTP_200_OK)

    # If the stored graph is large, we may want to request a subset of the graph containing
    # only high degree nodes and their corresponding edges.  The nodes_filtered and edges_filtered
    # endpoints expect a parameter ('in-degree' or 'out-degree') and a threshold to be over (e.g. 10)
    # in order to return the subgraph containing nodes only above the threshold degree

    @swagger_auto_schema(
        query_serializer=NodeAndEdgeFilteredQuerySerializer(),
        responses={200: TableReturnSerializer(many=True)},
    )
    @action(detail=True, url_path='nodes_filtered')
    @require_workspace_permission(WorkspaceRoleChoice.READER)
    def nodes_filtered(self, request, parent_lookup_workspace__name: str, name: str):

        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        network: Network = get_object_or_404(Network, workspace=workspace, name=name)

        # get the algorithm option from the query serializer
        serializer = NodeAndEdgeFilteredQuerySerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        algorithm = serializer.validated_data['algorithm']
        threshold = serializer.validated_data['threshold']

        # traverse through arango cursors to get data into lists
        timestamps = []
        timestamps.append(('before cursor read',arrow.now()))
     
        # iterate through Arango cursors to extract the node and edge data
        # arango queries are buffered by default pagination of 
        # 1000 entries, so loop through all the batches
        edge_list = []
        node_list = []

        edges = network.edges()
        # loop until there are no more batches waiting
        if edges.has_more():
            while edges.has_more():
                while not edges.empty():
                    edge = edges.next()
                    edge_list.append(edge)
                # advance to the last partial batch
                edges.fetch()
        # get the last partial batch
        while not edges.empty():
            edge = edges.next()
            edge_list.append(edge)

        nodes = network.nodes()
        # loop until there are no more batches waiting
        if nodes.has_more():
            while nodes.has_more():
                while not nodes.empty():
                    node = nodes.next()
                    node_list.append(node)
                nodes.fetch()
        # get the last partial batch
        while not nodes.empty():
            node = nodes.next()
            node_list.append(node)

        timestamps.append(('before graph creation',arrow.now()))
        nxNetwork = buildGraph_netX(node_list,edge_list)
        timestamps.append(('after graph creation',arrow.now()))
        smallNetwork = subsetGraph_netX(nxNetwork,algorithm,threshold)
        timestamps.append(('after graph subset',arrow.now()))
        print('timestamps:')
        #print(timestamps)
        for stamp in range(len(timestamps)-1):
            diff = timestamps[stamp+1][1]-timestamps[stamp][1]
            print(timestamps[stamp+1][0],diff)

        print('reduced graph has ',nx.number_of_nodes(smallNetwork),'nodes')
        print('reduced graph has ',nx.number_of_edges(smallNetwork),'edges')

        # iterate over the nodes returned and build a json structure that
        # contains all the node information
        node_iter = list(smallNetwork.nodes)
        out_list = []
        for index in node_iter:
            out_list.append(node_list[index-1])
        return JsonResponse(out_list,safe=False)


    @swagger_auto_schema(
        query_serializer=NodeAndEdgeFilteredQuerySerializer(),
        responses={200: TableReturnSerializer(many=True)},
    )
    @action(detail=True, url_path='edges_filtered')
    @require_workspace_permission(WorkspaceRoleChoice.READER)
    def edges_filtered(self, request, parent_lookup_workspace__name: str, name: str):

        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        network: Network = get_object_or_404(Network, workspace=workspace, name=name)

        # get the algorithm option from the query serializer
        serializer = NodeAndEdgeFilteredQuerySerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        algorithm = serializer.validated_data['algorithm']
        threshold = serializer.validated_data['threshold']

        # traverse through arango cursors to get data into lists
        timestamps = []
        timestamps.append(('before cursor read',arrow.now()))

        # iterate through Arango cursors to extract the node and edge data
        # arango queries are buffered by default pagination of 
        # 1000 entries, so loop through all the batches
        edge_list = []
        node_list = []

        edges = network.edges()
        # loop until there are no more batches waiting
        if edges.has_more():
            while edges.has_more():
                while not edges.empty():
                    edge = edges.next()
                    edge_list.append(edge)
                # advance to the last partial batch
                edges.fetch()
        # get the last partial batch
        while not edges.empty():
            edge = edges.next()
            edge_list.append(edge)

        nodes = network.nodes()
        # loop until there are no more batches waiting
        if nodes.has_more():
            while nodes.has_more():
                while not nodes.empty():
                    node = nodes.next()
                    node_list.append(node)
                nodes.fetch()
        # get the last partial batch
        while not nodes.empty():
            node = nodes.next()
            node_list.append(node)     


        timestamps.append(('before graph creation',arrow.now()))
        nxNetwork = buildGraph_netX(node_list,edge_list)
        timestamps.append(('after graph creation',arrow.now()))
        smallNetwork = subsetGraph_netX(nxNetwork,algorithm,threshold)
        timestamps.append(('after graph subset',arrow.now()))
      

        print('reduced graph has ',nx.number_of_nodes(smallNetwork),'nodes')
        print('reduced graph has ',nx.number_of_edges(smallNetwork),'edges')

        # now we need to build a lookup table that maps each edge from its
        # node indices, e.g. (2,4), to its full data so we can output the edges
        # of a reduced graph
        edgeIndexToData = {}
        for edge in edge_list:
            fromIndex = nodeKeyToNumber(nxNetwork,edge['_from'])
            toIndex = nodeKeyToNumber(nxNetwork,edge['_to'])
            edgeIndexToData[(fromIndex,toIndex)] = edge

        print('timestamps:')
        for stamp in range(len(timestamps)-1):
            diff = timestamps[stamp+1][1]-timestamps[stamp][1]
            print(timestamps[stamp+1][0],diff)

        # iterate over the edges returned and build a json structure that
        # contains all the edge information.  This is complicated because networkX returns
        # only a index tuple, do we use the index we built during the inital traversal to 
        # recover the edge metadata
        edge_iter = list(smallNetwork.edges)
        out_list = []
        for edgeTuple in edge_iter:
            edgeMetadata = edgeIndexToData[edgeTuple]
            out_list.append(edgeMetadata)
        return JsonResponse(out_list,safe=False)




#---------------- end of graph algorithm additions

    @swagger_auto_schema(
        query_serializer=LimitOffsetSerializer(),
        responses={200: PaginatedResultSerializer()},
    )
    @action(detail=True, url_path='edges')
    @require_workspace_permission(WorkspaceRoleChoice.READER)
    def edges(self, request, parent_lookup_workspace__name: str, name: str):
        # Doesn't use the Network.edges method, in order to do proper pagination.

        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        network: Network = get_object_or_404(Network, workspace=workspace, name=name)

        pagination = ArangoPagination()
        query = ArangoQuery.from_collections(workspace.get_arango_db(), network.edge_tables())
        paginated_query = pagination.paginate_queryset(query, request)

        return pagination.get_paginated_response(paginated_query)

    @swagger_auto_schema(
        query_serializer=NetworkTablesSerializer(),
        responses={200: TableReturnSerializer(many=True)},
    )
    @action(detail=True, url_path='tables')
    @require_workspace_permission(WorkspaceRoleChoice.READER)
    def tables(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        network: Network = get_object_or_404(Network, workspace=workspace, name=name)

        serializer = NetworkTablesSerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        table_type = serializer.validated_data.get('type', None)
        if table_type == 'node':
            table_names = network.node_tables()
        elif table_type == 'edge':
            table_names = network.edge_tables()
        else:
            table_names = network.node_tables() + network.edge_tables()

        network_tables = Table.objects.filter(name__in=table_names)
        serializer = TableReturnSerializer(network_tables, many=True)

        return Response(serializer.data, status=status.HTTP_200_OK)
 