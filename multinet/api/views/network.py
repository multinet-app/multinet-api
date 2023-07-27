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
from multinet.api.models import Network, NetworkSession, Table, Workspace, WorkspaceRoleChoice
from multinet.api.tasks.upload.csv import create_csv_network
from multinet.api.utils.arango import ArangoQuery
from multinet.api.views.serializers import (
    CSVNetworkCreateSerializer,
    LimitOffsetSerializer,
    NetworkCreateSerializer,
    NetworkReturnDetailSerializer,
    NetworkReturnSerializer,
    NetworkSerializer,
    NetworkSessionSerializer,
    NetworkTablesSerializer,
    PaginatedResultSerializer,
    TableReturnSerializer,
)

from .common import ArangoPagination, MultinetPagination, WorkspaceChildMixin


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

    @swagger_auto_schema(
        responses={200: NetworkSessionSerializer(many=True)},
    )
    @action(detail=True, url_path='sessions')
    @require_workspace_permission(WorkspaceRoleChoice.READER)
    def sessions(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        network: Network = get_object_or_404(Network, workspace=workspace, name=name)

        sessions = NetworkSession.objects.filter(network=network.id)
        serializer = NetworkSessionSerializer(sessions, many=True)

        return Response(serializer.data, status=status.HTTP_200_OK)
