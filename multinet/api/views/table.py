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

from multinet.api.models import Table, Workspace
from multinet.api.views.serializers import (
    TableCreateSerializer,
    TableReturnSerializer,
    TableSerializer,
)

from .common import MultinetPagination

OPENAPI_ROWS_SCHEMA = openapi.Schema(
    type=openapi.TYPE_ARRAY, items=openapi.Schema(type=openapi.TYPE_OBJECT)
)


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
        manual_parameters=[
            openapi.Parameter('page', 'query', type='integer'),
            openapi.Parameter('page_size', 'query', type='integer'),
        ],
        responses={200: OPENAPI_ROWS_SCHEMA},
    )
    @action(detail=True, url_path='rows')
    def get_rows(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        table: Table = get_object_or_404(Table, workspace=workspace, name=name)

        # NOTE
        # Below is done to emulate pagination, since arango cursors aren't querysets
        # If there is a way to do with firsthand with Django Pagination, this should be replaced

        try:
            page = int(request.GET.get('page'))
        except (TypeError, ValueError):
            page = 1

        try:
            page_size = int(request.GET.get('page_size')) or 1
        except (TypeError, ValueError):
            page_size = self.pagination_class.page_size

        rows, count = table.get_rows(page=page, page_size=page_size)
        base_url = request.build_absolute_uri().split('?')[0]

        next_url = None
        if count > page * page_size:
            next_url = f'{base_url}?page={page+1}'

        prev_url = None
        if page > 1:
            prev_url = f'{base_url}?page={page-1}'

        return Response(
            {
                'count': count,
                'next': next_url,
                'previous': prev_url,
                'results': rows,
            }
        )

    @swagger_auto_schema(
        request_body=OPENAPI_ROWS_SCHEMA,
        responses={200: OPENAPI_ROWS_SCHEMA},
    )
    @get_rows.mapping.put
    def put_rows(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        table: Table = get_object_or_404(Table, workspace=workspace, name=name)

        return Response(table.put_rows(request.data))

    @swagger_auto_schema(
        request_body=OPENAPI_ROWS_SCHEMA,
        responses={200: OPENAPI_ROWS_SCHEMA},
    )
    @get_rows.mapping.delete
    def delete_rows(self, request, parent_lookup_workspace__name: str, name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        table: Table = get_object_or_404(Table, workspace=workspace, name=name)

        table.delete_rows(request.data)
        return Response(None, status=status.HTTP_200_OK)
