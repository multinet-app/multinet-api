from django.shortcuts import get_object_or_404
from drf_yasg.utils import swagger_auto_schema
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from multinet.api.auth.decorators import require_workspace_permission
from multinet.api.models import AqlQuery, Workspace, WorkspaceRoleChoice
from multinet.api.tasks.aql import execute_query

from .common import WorkspaceChildMixin
from .serializers import AqlQueryResultsSerializer, AqlQuerySerializer, AqlQueryTaskSerializer


class AqlQueryViewSet(WorkspaceChildMixin(), ReadOnlyModelViewSet):
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
        query: AqlQuery = AqlQuery.objects.create(
            workspace=workspace,
            user=request.user,
            query=serializer.validated_data['query'],
            bind_vars=serializer.validated_data['bind_vars'],
        )

        execute_query.delay(task_id=query.pk)
        return Response(AqlQueryTaskSerializer(query).data, status=status.HTTP_200_OK)

    @swagger_auto_schema(responses={200: AqlQueryResultsSerializer()})
    @action(detail=True, url_path='results')
    @require_workspace_permission(WorkspaceRoleChoice.READER)
    def results(self, request, parent_lookup_workspace__name: str, pk):
        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        aql_task: AqlQuery = get_object_or_404(AqlQuery, workspace=workspace, pk=pk)
        if aql_task.status == AqlQuery.Status.FINISHED:
            return Response(AqlQueryResultsSerializer(aql_task).data, status=status.HTTP_200_OK)
        elif aql_task.status in [AqlQuery.Status.STARTED, AqlQuery.Status.PENDING]:
            return Response(
                'The given query has not finished executing', status=status.HTTP_400_BAD_REQUEST
            )
        elif aql_task.status == AqlQuery.Status.FAILED:
            return Response(
                'The given query could not be executed, and has no results',
                status=status.HTTP_400_BAD_REQUEST,
            )
