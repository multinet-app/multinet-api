from django.shortcuts import get_object_or_404
from drf_yasg.utils import swagger_auto_schema
from rest_framework import status
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from multinet.api.auth.decorators import require_workspace_permission
from multinet.api.models import AqlQuery, Workspace, WorkspaceRoleChoice
from multinet.api.tasks.aql import execute_query

from .common import WorkspaceChildMixin
from .serializers import AqlQuerySerializer, AqlQueryTaskSerializer


class AqlQueryViewSet(WorkspaceChildMixin, ReadOnlyModelViewSet):
    queryset = AqlQuery.objects.all().select_related('workspace')
    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = AqlQueryTaskSerializer
    swagger_tags = ['queries']

    @swagger_auto_schema(
        request_body=AqlQuerySerializer(), responses={200: AqlQueryTaskSerializer()}
    )
    @require_workspace_permission(WorkspaceRoleChoice.READER)
    def create(self, request, parent_lookup_workspace__name: str):
        """Create an AQL query task."""
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        serializer = AqlQuerySerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        query_str = serializer.validated_data['query']

        query: AqlQuery = AqlQuery.objects.create(
            workspace=workspace, user=request.user, query=query_str
        )

        execute_query.delay(task_id=query.pk)

        return Response(AqlQueryTaskSerializer(query).data, status=status.HTTP_200_OK)
