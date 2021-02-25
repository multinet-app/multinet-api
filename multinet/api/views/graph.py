from django.shortcuts import get_object_or_404
from django_filters import rest_framework as filters
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from guardian.utils import get_40x_or_None
from rest_framework import status

from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from multinet.api.models import Graph, Table, Workspace
from multinet.api.views.serializers import (
    GraphCreateSerializer,
    GraphReturnSerializer,
    GraphSerializer,
)

from multinet.api.utils.arango import get_or_create_db

from .common import MultinetPagination, OPENAPI_ROWS_SCHEMA


EDGE_DEFINITION_CREATE_SCHEMA = openapi.Schema(
    type=openapi.TYPE_OBJECT,
    properties={
        'edge_table': openapi.Schema(type=openapi.TYPE_STRING),
        'node_tables': openapi.Schema(
            type=openapi.TYPE_ARRAY, items=openapi.Schema(type=openapi.TYPE_STRING)
        ),
    },
)


class GraphViewSet(ReadOnlyModelViewSet):
    queryset = Graph.objects.all().select_related('workspace')
    lookup_field = 'name'

    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = GraphReturnSerializer

    filter_backends = [filters.DjangoFilterBackend]
    filterset_fields = ['name']

    pagination_class = MultinetPagination

    @swagger_auto_schema(
        request_body=GraphCreateSerializer(),
        responses={200: GraphReturnSerializer()},
    )
    def create(self, request, parent_lookup_workspace__name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)

        # TODO @permission_required doesn't work on methods
        # https://github.com/django-guardian/django-guardian/issues/723
        response = get_40x_or_None(request, ['owner'], workspace, return_403=True)
        if response:
            return response

        edge_table: Table = get_object_or_404(
            Table, workspace=workspace, name=request.data.get('edge_table')
        )

        serializer = GraphSerializer(
            data={
                'name': request.data.get('name'),
                'workspace': workspace.pk,
            }
        )
        serializer.is_valid(raise_exception=True)

        # Create graph in arango before creating the Graph object here
        # TODO: Below call doesn't exist yet, it would analyze all rows and return the associations
        # node_tables = edge_table.find_node_tables()

        # get_or_create_db(workspace.name).create_graph(
        #     serializer.validated_data['name'],
        #     edge_definitions=[
        #         {
        #             'edge_collection': edge_table.name,
        #             'from_vertex_collections': node_tables,
        #             'to_vertex_collections': node_tables,
        #         }
        #     ],
        # )

        table, created = Graph.objects.get_or_create(
            name=serializer.validated_data['name'],
            workspace=workspace,
        )

        if created:
            table.save()

        return Response(GraphReturnSerializer(table).data, status=status.HTTP_200_OK)

    # @permission_required_or_403('owner', (Workspace, 'dandiset__pk'))
    def destroy(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)

        # TODO @permission_required doesn't work on methods
        # https://github.com/django-guardian/django-guardian/issues/723
        response = get_40x_or_None(request, ['owner'], workspace, return_403=True)
        if response:
            return response

        graph: Graph = get_object_or_404(Graph, name=name)

        # TODO @permission_required doesn't work on methods
        # https://github.com/django-guardian/django-guardian/issues/723
        response = get_40x_or_None(request, ['owner'], graph, return_403=True)
        if response:
            return response

        graph.delete()
        return Response(None, status=status.HTTP_204_NO_CONTENT)

    # @swagger_auto_schema(
    #     request_body=EDGE_DEFINITION_CREATE_SCHEMA,
    #     responses={200: OPENAPI_ROWS_SCHEMA},
    # )
    # @action(detail=True, methods=['POST'], url_path='edge_definition')
    # def add_edge_definition(self, request, parent_lookup_workspace__name: str, name: str):
    #     workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
    #     graph: Graph = get_object_or_404(Graph, workspace=workspace, name=name)

    #     edge_table = get_object_or_404(
    #         Table, workspace=workspace, name=request.data.get('edge_table')
    #     )

    #     node_tables = []
    #     for node_table in request.data.get('node_tables', []):
    #         node_tables.append(get_object_or_404(Table, workspace=workspace, name=node_table))

    #     print(edge_table, node_tables)

    #     edge_collection = edge_table.name
    #     from_vertex_collections = to_vertex_collections = [table.name for table in node_tables]
    #     arango_graph = get_or_create_db(workspace.name).graph(graph.name)

    #     print(get_or_create_db(workspace.name).has_graph(graph.name))
    #     print(get_or_create_db(workspace.name).graphs())
    #     if arango_graph.has_edge_collection(edge_table.name):
    #         arango_graph.replace_edge_definition(
    #             edge_collection, from_vertex_collections, to_vertex_collections
    #         )
    #     else:
    #         arango_graph.create_edge_definition(
    #             edge_collection, from_vertex_collections, to_vertex_collections
    #         )

    #     return Response(arango_graph.edge_definitions(), status=status.HTTP_200_OK)

    # # Add a table to this graph
    # def add_tables(self, request, parent_lookup_workspace__name: str, name: str):
    #     pass

    # # Remove a table from this graph
    # def remove_table(self, request, parent_lookup_workspace__name: str, name: str):
    #     pass

    # # List tables in this graph
    # def tables(self, request, parent_lookup_workspace__name: str, name: str):
    #     pass

    # # List graph nodes
    # def nodes(self, request, parent_lookup_workspace__name: str, name: str):
    #     pass

    # # List the graph edges
    # def edges(self, request, parent_lookup_workspace__name: str, name: str):
    #     pass
