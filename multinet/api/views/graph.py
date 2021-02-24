from django.shortcuts import get_object_or_404
from django_filters import rest_framework as filters
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from guardian.utils import get_40x_or_None
from rest_framework import status

# from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from multinet.api.models import Graph, Workspace
from multinet.api.views.serializers import (
    GraphCreateSerializer,
    GraphReturnSerializer,
    GraphSerializer,
)

from .common import MultinetPagination

OPENAPI_ROWS_SCHEMA = openapi.Schema(
    type=openapi.TYPE_ARRAY, items=openapi.Schema(type=openapi.TYPE_OBJECT)
)


class GraphViewSet(ReadOnlyModelViewSet):
    queryset = Graph.objects.all().select_related('workspace')
    lookup_field = 'name'

    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = GraphSerializer

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

        serializer = GraphSerializer(
            data={
                **request.data,
                'workspace': workspace.pk,
            }
        )
        serializer.is_valid(raise_exception=True)

        table, created = Graph.objects.get_or_create(
            name=serializer.validated_data['name'],
            workspace=workspace,
        )

        if created:
            table.save()

        return Response(GraphSerializer(table).data, status=status.HTTP_200_OK)

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
