from dataclasses import asdict

from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django_filters import rest_framework as filters
from drf_yasg.utils import swagger_auto_schema
from guardian.decorators import permission_required_or_403
from rest_framework import serializers, status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework_extensions.mixins import NestedViewSetMixin

from multinet.api.models import Table, Workspace
from multinet.api.utils.arango import ArangoQuery
from multinet.api.views.serializers import (
    TableCreateSerializer,
    TableReturnSerializer,
    TableSerializer,
)

from .common import (
    ARRAY_OF_OBJECTS_SCHEMA,
    LIMIT_OFFSET_QUERY_PARAMS,
    PAGINATED_RESULTS_SCHEMA,
    ArangoPagination,
    MultinetPagination,
)


class RowInsertResponseSerializer(serializers.Serializer):
    inserted = serializers.IntegerField()
    errors = serializers.ListField(child=serializers.JSONField())


class RowDeleteResponseSerializer(serializers.Serializer):
    deleted = serializers.IntegerField()
    errors = serializers.ListField(child=serializers.JSONField())


class TableViewSet(NestedViewSetMixin, ReadOnlyModelViewSet):
    queryset = Table.objects.all().select_related('workspace')
    lookup_field = 'name'

    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = TableReturnSerializer

    filter_backends = [filters.DjangoFilterBackend]
    filterset_fields = ['name']

    pagination_class = MultinetPagination

    # Categorize entire ViewSet
    swagger_tags = ['tables']

    @swagger_auto_schema(
        request_body=TableCreateSerializer(),
        responses={200: TableReturnSerializer()},
    )
    @method_decorator(
        permission_required_or_403('owner', (Workspace, 'name', 'parent_lookup_workspace__name'))
    )
    def create(self, request, parent_lookup_workspace__name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        serializer = TableSerializer(
            data={
                **request.data,
                'workspace': workspace.pk,
            }
        )
        serializer.is_valid(raise_exception=True)

        table, created = Table.objects.get_or_create(
            name=serializer.validated_data['name'],
            edge=serializer.validated_data['edge'],
            workspace=workspace,
        )

        if created:
            table.save()

        return Response(TableReturnSerializer(table).data, status=status.HTTP_200_OK)

    @method_decorator(
        permission_required_or_403('owner', (Workspace, 'name', 'parent_lookup_workspace__name'))
    )
    def destroy(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        table: Table = get_object_or_404(Table, workspace=workspace, name=name)
        table.delete()

        return Response(None, status=status.HTTP_204_NO_CONTENT)

    @swagger_auto_schema(
        manual_parameters=LIMIT_OFFSET_QUERY_PARAMS,
        responses={200: PAGINATED_RESULTS_SCHEMA},
    )
    @action(detail=True, url_path='rows')
    def get_rows(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        table: Table = get_object_or_404(Table, workspace=workspace, name=name)

        pagination = ArangoPagination()
        query = ArangoQuery.from_collections(workspace.get_arango_db(), [table.name])
        paginated_query = pagination.paginate_queryset(query, request)

        return pagination.get_paginated_response(paginated_query)

    @swagger_auto_schema(
        request_body=ARRAY_OF_OBJECTS_SCHEMA,
        responses={200: RowInsertResponseSerializer()},
    )
    @get_rows.mapping.put
    def put_rows(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        table: Table = get_object_or_404(Table, workspace=workspace, name=name)

        insert_res = table.put_rows(request.data)
        serializer = RowInsertResponseSerializer(data=asdict(insert_res))
        serializer.is_valid(raise_exception=True)

        return Response(serializer.data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        request_body=ARRAY_OF_OBJECTS_SCHEMA,
        responses={200: RowDeleteResponseSerializer()},
    )
    @get_rows.mapping.delete
    def delete_rows(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        table: Table = get_object_or_404(Table, workspace=workspace, name=name)

        delete_res = table.delete_rows(request.data)
        serializer = RowDeleteResponseSerializer(data=asdict(delete_res))
        serializer.is_valid(raise_exception=True)

        return Response(serializer.data, status=status.HTTP_200_OK)
