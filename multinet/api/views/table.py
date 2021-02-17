# from django.http import HttpResponseRedirect
from django.shortcuts import get_object_or_404
from django_filters import rest_framework as filters
from drf_yasg.utils import swagger_auto_schema
from guardian.utils import get_40x_or_None
from rest_framework import status

from rest_framework.pagination import PageNumberPagination
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from multinet.api.models import Table, Workspace
from multinet.api.views.serializers import TableSerializer


class TableViewSet(ReadOnlyModelViewSet):
    queryset = Table.objects.all().select_related('workspace')
    lookup_field = 'name'

    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = TableSerializer

    filter_backends = [filters.DjangoFilterBackend]
    filterset_fields = ['name']

    pagination_class = PageNumberPagination

    @swagger_auto_schema(
        request_body=TableSerializer(),
        responses={200: TableSerializer()},
    )
    def create(self, request, parent_lookup_workspace__name: str):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)

        # TODO @permission_required doesn't work on methods
        # https://github.com/django-guardian/django-guardian/issues/723
        response = get_40x_or_None(request, ['owner'], workspace, return_403=True)
        if response:
            return response

        serializer = TableSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        table, created = Table.objects.get_or_create(
            name=serializer.validated_data['name'],
            edge=serializer.validated_data['edge'],
            workspace=workspace,
        )

        if created:
            table.save()

        return Response(TableSerializer(table).data, status=status.HTTP_200_OK)

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
