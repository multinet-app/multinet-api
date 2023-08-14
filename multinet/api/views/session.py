from django.http.response import Http404, HttpResponseBadRequest, HttpResponseForbidden
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
from .serializers import (
    NetworkSessionCreateSerializer,
    NetworkSessionSerializer,
    TableSessionCreateSerializer,
    TableSessionSerializer,
)


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

        if session.starred:
            return HttpResponseForbidden('Starred session state cannot be modified')

        serializer = SessionStatePatchSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data['state']

        session.state = data

        try:
            session.save()
        except ValueError as e:
            return HttpResponseBadRequest(str(e))

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

    def get_serializer_class(self):
        if self.action == 'create':
            return NetworkSessionCreateSerializer
        return NetworkSessionSerializer

    @swagger_auto_schema(
        request_body=NetworkSessionCreateSerializer, responses={201: NetworkSessionSerializer}
    )
    @require_workspace_permission(WorkspaceRoleChoice.WRITER)
    def create(self, request, parent_lookup_workspace__name: str):
        input_serializer = NetworkSessionCreateSerializer(data=request.data)
        input_serializer.is_valid(raise_exception=True)

        instance = NetworkSession.objects.create(**input_serializer.validated_data)

        output_serializer = NetworkSessionSerializer(instance)
        return Response(output_serializer.data, status=status.HTTP_201_CREATED)


class TableSessionViewSet(TableWorkspaceChildMixin, SessionViewSet):
    queryset = TableSession.objects.all().select_related('table__workspace')

    def get_serializer_class(self):
        if self.action == 'create':
            return TableSessionCreateSerializer
        return TableSessionSerializer

    @swagger_auto_schema(
        request_body=TableSessionCreateSerializer, responses={201: TableSessionSerializer}
    )
    @require_workspace_permission(WorkspaceRoleChoice.WRITER)
    def create(self, request, parent_lookup_workspace__name: str):
        input_serializer = TableSessionCreateSerializer(data=request.data)
        input_serializer.is_valid(raise_exception=True)

        instance = TableSession.objects.create(**input_serializer.validated_data)

        output_serializer = TableSessionSerializer(instance)
        return Response(output_serializer.data, status=status.HTTP_201_CREATED)
