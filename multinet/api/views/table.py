from collections import OrderedDict

from django.shortcuts import get_object_or_404
from django_filters import rest_framework as filters
from drf_yasg.utils import swagger_auto_schema
from guardian.utils import get_40x_or_None
from rest_framework import serializers, status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.utils.urls import remove_query_param, replace_query_param
from rest_framework.viewsets import ReadOnlyModelViewSet

from multinet.api.models import Table, Workspace
from multinet.api.views.serializers import (
    TableCreateSerializer,
    TableReturnSerializer,
    TableSerializer,
)

from .common import (
    ARRAY_OF_OBJECTS_SCHEMA,
    PAGINATED_RESULTS_SCHEMA,
    PAGINATION_QUERY_PARAMS,
    MultinetPagination,
)


class RowInsertResponseSerializer(serializers.Serializer):
    inserted = serializers.ListField(child=serializers.JSONField())
    errors = serializers.ListField(child=serializers.JSONField())


class TableViewSet(ReadOnlyModelViewSet):
    queryset = Table.objects.all().select_related('workspace')
    lookup_field = 'name'

    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = TableReturnSerializer

    filter_backends = [filters.DjangoFilterBackend]
    filterset_fields = ['name']

    pagination_class = MultinetPagination

    @swagger_auto_schema(
        request_body=TableCreateSerializer(),
        responses={200: TableReturnSerializer()},
    )
    def create(self, request, parent_lookup_workspace__name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)

        # TODO @permission_required doesn't work on methods
        # https://github.com/django-guardian/django-guardian/issues/723
        response = get_40x_or_None(request, ['owner'], workspace, return_403=True)
        if response:
            return response

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

    # @permission_required_or_403('owner', (Workspace, 'dandiset__pk'))
    def destroy(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)

        # TODO @permission_required doesn't work on methods
        # https://github.com/django-guardian/django-guardian/issues/723
        response = get_40x_or_None(request, ['owner'], workspace, return_403=True)
        if response:
            return response

        table: Table = get_object_or_404(Table, name=name)

        # TODO @permission_required doesn't work on methods
        # https://github.com/django-guardian/django-guardian/issues/723
        response = get_40x_or_None(request, ['owner'], table, return_403=True)
        if response:
            return response

        table.delete()
        return Response(None, status=status.HTTP_204_NO_CONTENT)

    @swagger_auto_schema(
        manual_parameters=PAGINATION_QUERY_PARAMS,
        responses={200: PAGINATED_RESULTS_SCHEMA},
    )
    @action(detail=True, url_path='rows')
    def get_rows(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        table: Table = get_object_or_404(Table, workspace=workspace, name=name)

        # NOTE: Below is done to emulate pagination, since arango cursors aren't querysets
        # If there is a way to do with firsthand with Django Pagination, this should be replaced

        try:
            page = int(request.GET.get('page'))
        except (TypeError, ValueError):
            page = 1

        try:
            page_size = int(request.GET.get('page_size')) or 1
        except (TypeError, ValueError):
            page_size = self.pagination_class.page_size

        rows = table.get_rows(page=page, page_size=page_size)
        count = table.count()

        url = request.build_absolute_uri()
        next_url = replace_query_param(url, 'page', page + 1) if count > page * page_size else None
        prev_url = None

        if page > 1:
            if page == 2:
                prev_url = remove_query_param(url, 'page')
            else:
                prev_url = replace_query_param(url, 'page', page - 1)

        return Response(
            OrderedDict(
                [
                    ('count', count),
                    ('next', next_url),
                    ('previous', prev_url),
                    ('results', rows),
                ]
            )
        )

    @swagger_auto_schema(
        request_body=ARRAY_OF_OBJECTS_SCHEMA,
        responses={200: PAGINATED_RESULTS_SCHEMA},
    )
    @get_rows.mapping.put
    def put_rows(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        table: Table = get_object_or_404(Table, workspace=workspace, name=name)

        inserted, errors = table.put_rows(request.data)
        serializer = RowInsertResponseSerializer(data={'inserted': inserted, 'errors': errors})
        serializer.is_valid(raise_exception=True)

        return Response(serializer.data)

    @swagger_auto_schema(
        request_body=ARRAY_OF_OBJECTS_SCHEMA,
        responses={200: PAGINATED_RESULTS_SCHEMA},
    )
    @get_rows.mapping.delete
    def delete_rows(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        table: Table = get_object_or_404(Table, workspace=workspace, name=name)

        table.delete_rows(request.data)
        return Response(None, status=status.HTTP_200_OK)
