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
from graph_tool.all import *
from graph_tool.centrality import pagerank
from graph_tool.centrality import betweenness
from graph_tool.centrality import katz
import arrow
from django.http import JsonResponse
import json
import pandas as pd


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
def subsetGraph_gtool(g,algorithm,lowerBound,upperBound):
    if algorithm == 'in_degree':
        view = GraphView(g, vfilt=lambda v: (v.in_degree() >= lowerBound and v.in_degree() <= upperBound))
    elif algorithm == 'out_degree':
        view = GraphView(g, vfilt=lambda v: (v.out_degree() >= lowerBound and v.out_degree() <= upperBound))
        # vcount = 0
        # for v in view.vertices():
        #     if (vcount<5):
        #         print('vert info:',v)
        #     vcount += 1

    else:
        # or of two conditions gives 'degree' (either in or out conditions match)
        view = GraphView(g, vfilt=lambda v: ((v.out_degree() >= lowerBound and v.out_degree() <= upperBound) or 
                                             (v.in_degree() >= lowerBound and v.in_degree() <= upperBound))
                                             )
    return view



# given a list of nodes and edges with arbitrary attributes, create graph_tool attribute structures
def BuildNodeAndEdgeAttributeStructure_gtool(g,node_list,edge_list):
    # get dataframe version so we can use pandas type analysis API
    node_df = pd.DataFrame(node_list)
    edge_df = pd.DataFrame(edge_list)
    # find all the attributes on the nodes and edges
    node_attrs = {}
    edge_attrs = {}
    infer_type = lambda x: pd.api.types.infer_dtype(x, skipna=True)
    
    # find the node properties
    for column in node_df.columns:
        if column not in ['gt_object','used_by_edge','index']:
            #print(column)
            # return node attribute types as a pandas series
            node_types = node_df.apply(infer_type, axis=0)
            # create a dictionary that includes type and value keys. The
            # value will be an instance of a graph-tool vertex_properties object
            node_attrs[column] = {}
            node_attrs[column]['type'] = node_types[column]       
            if node_types[column] == 'string':
                vert_id_prop = g.new_vertex_property("string")
            elif node_types[column] == 'integer':
                vert_id_prop = g.new_vertex_property("int")
            else:
                vert_id_prop = g.new_vertex_property("float")           
            node_attrs[column]['value'] = vert_id_prop
            g.vertex_properties[column] = vert_id_prop
            #print('id of inside graph:',id(g))
    
            
    # now fill in all the node property values
    for node in node_list:
        for attrib in node.keys():
            if attrib not in ['gt_object','used_by_edge','index']:
                #print(node['id'],attrib)
                # assign the attribute value to the proper place in the vertex_properties object
                # the node['gt_object'] is the index into the property object, which is shared across all nodes
                node_attrs[attrib]['value'][node['gt_object']] = node[attrib]

    # find the edge properties
    for column in edge_df.columns:
        if column not in []:
            #print(column)
            # return node attribute types as a pandas series
            edge_types = edge_df.apply(infer_type, axis=0)
            # create a dictionary that includes type and value keys. The
            # value will be an instance of a graph-tool vertex_properties object
            edge_attrs[column] = {}
            edge_attrs[column]['type'] = edge_types[column]       
            if edge_types[column] == 'string':
                edge_id_prop = g.new_edge_property("string")
            elif edge_types[column] == 'integer':
                edge_id_prop = g.new_edge_property("int")
            else:
                edge_id_prop = g.new_edge_property("float")           
            edge_attrs[column]['value'] = edge_id_prop
            g.edge_properties[column] = edge_id_prop
            
    # make a dict of all nodes indexed by '_id' to help lookup faster when we are adding edges
    node_dict = {}
    for node in node_list:
        node_dict[node['_id']] = node
            
    # now fill in the edge values. We have to look up the proper edge by its start and end nodes
    for edge in edge_list:
        sourceNode = node_dict[edge['_from']]['gt_object']
        destNode   = node_dict[edge['_to']]['gt_object']
        thisEdge = g.edge(sourceNode,destNode)
        for attrib in edge:
            if attrib not in ['_from','_to']:
                #print('edge attrib:',attrib)
                edge_attrs[attrib]['value'][thisEdge] = edge[attrib]
        
    # return attribute structures
    return (node_attrs,edge_attrs)

# build a graph-tool network based on the nodes and edges passed in dataframes. This routine supports
# arbitrary node and edge attributes 
def buildGraph_gtool(node_list,edge_list):   
    #print('found ',len(edge_list), 'edges')
    #print('found ',len(node_list), 'nodes')
    edge_df = pd.DataFrame(edge_list)
    node_df = pd.DataFrame(node_list)
    #print(node_list[0])
    g = Graph(directed=True)
    #print('id of graph:',id(g))
    
    # first we put the nodes in a dictionary, indexed by their primary key, so we can retrieve the node 
    potential_nodes = {}
    index = 0
    duplicate_nodes = []
    #print('node sample',node_list[:1])
    for node in node_list:
        node['index'] = index
        #node['gt_object'] = g.add_vertex()
        if node['_id'] in potential_nodes.keys():
            duplicate_nodes.append(node)
        potential_nodes[node['_id']] = node
        index += 1
        # debug, we shouldn't have extra, but indicate if dups exist
        if len(duplicate_nodes)>0:
            print('found ',len(duplicate_nodes),'duplicate nodes')

    # now go through the edges and see which ones connect nodes in the potential_nodes dictionary
    edge_count = 0
    #print('edge sample',edge_list[:1])
    for edge in edge_list:
        if (edge['_to'] in potential_nodes.keys()) and (edge['_from'] in potential_nodes.keys()):
            # record a record in the node, so we remember which nodes are needed
            potential_nodes[edge['_to']]['used_by_edge'] = 1
            potential_nodes[edge['_from']]['used_by_edge']  = 1
            edge_count += 1
    #print('found ',edge_count, 'edges between known nodes')   
    # add all the nodes that were used by edges
    count = 0
    used_nodes = {}
    used_node_ids_in_order = []
    used_node_list = []
    for node in potential_nodes.keys():
        #print(potential_nodes[node]['id'])
        if 'used_by_edge' in potential_nodes[node]:
            count += 1
            # add the node to the graph and keep record of the record we create
            potential_nodes[node]['gt_object'] = g.add_vertex()
            used_node_ids_in_order.append(node)
            #used_nodes[node] = potential_nodes[node]
            used_node_list.append(potential_nodes[node])
    #print('unnecessary used_nodes dictionary here?')
    #print('used ',count,'nodes')
    # we have added the nodes to the graph, now add the edges and retrieve the node records from the potential_nodes dictionry
    used_edge_list = []
    for edge in edge_list:
        if (edge['_from'] in potential_nodes.keys()) and (edge['_to'] in potential_nodes.keys()):
            newEdge = g.add_edge((potential_nodes[edge['_from']])['gt_object'],(potential_nodes[edge['_to']])['gt_object'])
            used_edge_list.append(edge)
    #print('really used ',len(used_edge_list),'edges')
    # now that we know what node and edge records are used, lets add attributes to the graph. the used_nodes and used_edges
    # are lists of dicts that contain all the attributes
    node_attrs,edge_attrs = BuildNodeAndEdgeAttributeStructure_gtool(g,used_node_list,used_edge_list)
    # return the graph-tool graph object 
    return (g, node_attrs, edge_attrs)



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

        timestamps.append(('before graph creation',arrow.now()))
        gtoolNetwork, node_attrs, edge_attrs  = buildGraph_gtool(node_list,edge_list)
        timestamps.append(('after graph creation',arrow.now()))

        if algorithm == 'node_centrality':
            algorithmReturn = katz(gtoolNetwork)
        elif algorithm == 'betweenness':
            algorithmReturn, edge_betweenness = betweenness(gtoolNetwork)
        elif algorithm == 'pagerank':
            algorithmReturn = pagerank(gtoolNetwork)
        elif algorithm == 'all':
            nodeCentrality_result = katz(gtoolNetwork)
            betweenness_result, edge_betweenness = betweenness(gtoolNetwork)
            pagerank_result = pagerank(gtoolNetwork)
        
        timestamps.append(('after algorithm completed',arrow.now()))


        node_stat_list = []
        for index,node in enumerate(node_list):
            entry = {}
            # add in the node _key for reference
            entry['_key'] = node['_key']
            entry['_id'] = node['_id']
            # fill in the result value according to what algorithm was selected
            if algorithm in ['node_centrality','betweenness']:
                try:
                    entry['result'] = algorithmReturn[index]
                except:
                    # there was no value for this vertex index, so return -1 flag
                    print('returning -1 for',node['_key'])
                    entry['result'] = -1
            elif algorithm == 'in_degree':
                entry['result'] = gtoolNetwork.vertex(index).in_degree()
            elif algorithm == 'out_degree':
                entry['result'] = gtoolNetwork.vertex(index).out_degree()
            elif algorithm == 'degree':
                entry['result'] = gtoolNetwork.vertex(index).in_degree()+gtoolNetwork.vertex(index).out_degree()
            elif algorithm == 'pagerank':
                entry['result'] = algorithmReturn[index]
            elif algorithm == 'all':
                entry['degree'] = gtoolNetwork.vertex(index).out_degree() + gtoolNetwork.vertex(index).in_degree()
                entry['in_degree'] = gtoolNetwork.vertex(index).in_degree()
                entry['out_degree'] = gtoolNetwork.vertex(index).out_degree()
                entry['node_centrality'] = nodeCentrality_result[index]
                entry['betweenness'] = betweenness_result[index]
                entry['pagerank'] = pagerank_result[index]
            else:
                # return out-degree as a default
                entry['result'] = gtoolNetwork.out_degree(index)
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
        lowerBound = serializer.validated_data['lowerBound']
        upperBound = serializer.validated_data['upperBound']

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
        gtoolNetwork, node_attrs, edge_attrs  = buildGraph_gtool(node_list,edge_list)
        timestamps.append(('after graph creation',arrow.now()))
        smallNetwork = subsetGraph_gtool(gtoolNetwork,algorithm,lowerBound, upperBound)
        timestamps.append(('after graph subset',arrow.now()))
        for stamp in range(len(timestamps)-1):
            diff = timestamps[stamp+1][1]-timestamps[stamp][1]
            print(timestamps[stamp+1][0],diff)

        # iterate over the nodes returned and build a json structure that
        # contains all the node information
        vertcount = 0
        out_list = []
        for v in smallNetwork.vertices():
            # vertex object attributes are stored in a graph_tool node_attr structure for the
            # the entire graph.  traverse all attributes, then get the value for this attribute
            # from the structure.  Build out records a node at a time.  This should work for an arbitrary
            # number and type of attributes. 
            out_record = {}
            for attribute in node_attrs:
                out_record[attribute] = node_attrs[attribute]['value'][int(v)]
            out_list.append(out_record)
            vertcount += 1
        print('resulting subgraph had',vertcount,'nodes')
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
        lowerBound = serializer.validated_data['lowerBound']
        upperBound = serializer.validated_data['upperBound']
        
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
        gtoolNetwork, node_attrs,edge_attrs = buildGraph_gtool(node_list,edge_list)
        timestamps.append(('after graph creation',arrow.now()))
        smallNetwork = subsetGraph_gtool(gtoolNetwork,algorithm,lowerBound,upperBound)
        timestamps.append(('after graph subset',arrow.now()))

        vertcount = edgecount = 0
        for vert in smallNetwork.vertices():
            vertcount += 1
        for edge in smallNetwork.edges():
            edgecount += 1
        print('reduced network has',vertcount,' nodes and',edgecount,'edges')

        # print('timestamps:')
        # for stamp in range(len(timestamps)-1):
        #     diff = timestamps[stamp+1][1]-timestamps[stamp][1]
        #     print(timestamps[stamp+1][0],diff)

        # iterate over the edges returned and build a json structure that
        # contains all the edge information.  Iterating over the edge_attrs returns
        # all attributes except the _from and _to nodes, so we pick them out of the node_attrs
        out_list = []
        for edge in smallNetwork.edges():
            out_record = {}
            for attribute in edge_attrs:
                out_record[attribute] = edge_attrs[attribute]['value'][edge]
            out_record['_from'] = node_attrs['_key']['value'][edge.source()]
            out_record['_to'] = node_attrs['_key']['value'][edge.target()]
            out_list.append(out_record)
        
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
 