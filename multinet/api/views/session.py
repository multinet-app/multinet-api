from django.http.response import Http404
from django.shortcuts import get_object_or_404
from drf_yasg.utils import swagger_auto_schema
from rest_framework import serializers, status
from rest_framework.decorators import action
from rest_framework.mixins import (
    CreateModelMixin,
    DestroyModelMixin,
    ListModelMixin,
    RetrieveModelMixin,
)
from rest_framework.response import Response
from rest_framework.viewsets import GenericViewSet

from ..auth.decorators import require_workspace_permission
from ..models import NetworkSession, TableSession, Workspace, WorkspaceRoleChoice
from .common import NetworkWorkspaceChildMixin, TableWorkspaceChildMixin
from .serializers import NetworkSessionSerializer, TableSessionSerializer


class SessionCreateSerializer(serializers.Serializer):
    workspace = serializers.CharField()
    network = serializers.CharField(required=False)
    table = serializers.CharField(required=False)

    visapp = serializers.CharField()
    name = serializers.CharField()

    def validate(self, data):
        if not bool(data.get('network')) ^ bool(data.get('table')):
            raise serializers.ValidationError('exactly one of `network` or `table` is required')

        return data


class SessionStatePatchSerializer(serializers.Serializer):
    state = serializers.JSONField()


class SessionNamePatchSerializer(serializers.Serializer):
    name = serializers.CharField(max_length=300)


class SessionViewSet(
    CreateModelMixin, RetrieveModelMixin, DestroyModelMixin, ListModelMixin, GenericViewSet
):
    swagger_tags = ['sessions']

    @swagger_auto_schema(request_body=SessionStatePatchSerializer)
    @action(detail=True, methods=['patch'])
    @require_workspace_permission(WorkspaceRoleChoice.WRITER)
    def state(self, request, parent_lookup_workspace__name: str, pk=None):
        session = self.get_object()

        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        session_ws = (
            session.table.workspace if hasattr(session, 'table') else session.network.workspace
        )
        if workspace.id != session_ws.id:
            raise Http404

        serializer = SessionStatePatchSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data['state']

        session.state = data
        session.save()

        return Response(status=status.HTTP_204_NO_CONTENT)

    @swagger_auto_schema(request_body=SessionNamePatchSerializer)
    @action(detail=True, methods=['patch'], url_path='name')
    @require_workspace_permission(WorkspaceRoleChoice.WRITER)
    def set_name(self, request, parent_lookup_workspace__name: str, pk=None):
        session = self.get_object()

        workspace: Workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        session_ws = (
            session.table.workspace if hasattr(session, 'table') else session.network.workspace
        )
        if workspace.id != session_ws.id:
            raise Http404

        serializer = SessionNamePatchSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        name = serializer.validated_data['name']

        session.name = name
        session.save()

        return Response(status=status.HTTP_204_NO_CONTENT)


class NetworkSessionViewSet(NetworkWorkspaceChildMixin, SessionViewSet):
    queryset = NetworkSession.objects.all().select_related('network__workspace')
    serializer_class = NetworkSessionSerializer


class TableSessionViewSet(TableWorkspaceChildMixin, SessionViewSet):
    queryset = TableSession.objects.all().select_related('table__workspace')
    serializer_class = TableSessionSerializer
