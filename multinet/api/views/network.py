from multinet.auth.decorators import require_workspace_permission
from typing import List, Optional
from django.http.response import Http404

from django.shortcuts import get_object_or_404
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import serializers, status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework_extensions.mixins import DetailSerializerMixin, NestedViewSetMixin

from multinet.api.models import Network, Table, Workspace
from multinet.api.utils.arango import ArangoQuery
from multinet.api.views.serializers import (
    NetworkCreateSerializer,
    NetworkReturnDetailSerializer,
    NetworkReturnSerializer,
    NetworkSerializer,
)
from multinet.api.utils.workspace_permissions import WorkspacePermission

from .common import (
    LIMIT_OFFSET_QUERY_PARAMS,
    PAGINATED_RESULTS_SCHEMA,
    ArangoPagination,
    MultinetPagination,
)

EDGE_DEFINITION_CREATE_SCHEMA = openapi.Schema(
    type=openapi.TYPE_OBJECT,
    properties={
        'edge_table': openapi.Schema(type=openapi.TYPE_STRING),
        'node_tables': openapi.Schema(
            type=openapi.TYPE_ARRAY, items=openapi.Schema(type=openapi.TYPE_STRING)
        ),
    },
)


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


class NetworkViewSet(NestedViewSetMixin, DetailSerializerMixin, ReadOnlyModelViewSet):
    queryset = Network.objects.all().select_related('workspace')
    lookup_field = 'name'

    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = NetworkReturnSerializer
    serializer_detail_class = NetworkReturnDetailSerializer

    pagination_class = MultinetPagination

    # Categorize entire ViewSet
    swagger_tags = ['networks']

    def get_queryset(self):
        """
        Get the queryset for network endpoints. Check that the requeting user has
        appropriate permissions for the associated workspace.
        """

        # prevent warning for schema generation incompatibility
        if getattr(self, "swagger_fake_view", False):
            return Network.objects.none()

        networks = super().get_queryset()

        parent_query_dict = self.get_parents_query_dict()
        workspace = get_object_or_404(Workspace, name=parent_query_dict['workspace__name'])

        if workspace.get_user_permission(self.request.user) is not None or workspace.public:
            return networks
        raise Http404

    @swagger_auto_schema(
        request_body=NetworkCreateSerializer(),
        responses={200: NetworkReturnSerializer()},
    )
    @require_workspace_permission(WorkspacePermission.writer)
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

    @require_workspace_permission(WorkspacePermission.writer)
    def destroy(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        network: Network = get_object_or_404(Network, workspace=workspace, name=name)
        network.delete()

        return Response(None, status=status.HTTP_204_NO_CONTENT)

    @swagger_auto_schema(
        manual_parameters=LIMIT_OFFSET_QUERY_PARAMS,
        responses={200: PAGINATED_RESULTS_SCHEMA},
    )
    @action(detail=True, url_path='nodes')
    @require_workspace_permission(WorkspacePermission.reader, allow_public=True)
    def nodes(self, request, parent_lookup_workspace__name: str, name: str):
        # Doesn't use the Network.nodes method, in order to do proper pagination.

        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        network: Network = get_object_or_404(Network, workspace=workspace, name=name)

        pagination = ArangoPagination()
        query = ArangoQuery.from_collections(workspace.get_arango_db(), network.node_tables())
        paginated_query = pagination.paginate_queryset(query, request)

        return pagination.get_paginated_response(paginated_query)

    @swagger_auto_schema(
        manual_parameters=LIMIT_OFFSET_QUERY_PARAMS,
        responses={200: PAGINATED_RESULTS_SCHEMA},
    )
    @action(detail=True, url_path='edges')
    @require_workspace_permission(WorkspacePermission.reader, allow_public=True)
    def edges(self, request, parent_lookup_workspace__name: str, name: str):
        # Doesn't use the Network.edges method, in order to do proper pagination.

        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        network: Network = get_object_or_404(Network, workspace=workspace, name=name)

        pagination = ArangoPagination()
        query = ArangoQuery.from_collections(workspace.get_arango_db(), network.edge_tables())
        paginated_query = pagination.paginate_queryset(query, request)

        return pagination.get_paginated_response(paginated_query)
