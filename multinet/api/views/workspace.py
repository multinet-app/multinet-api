from typing import OrderedDict

from arango.cursor import Cursor
from arango.exceptions import AQLQueryExecuteError, ArangoServerError
from django.contrib.auth.models import User
from django.db.models import Q
from django.shortcuts import get_object_or_404
from django_filters import rest_framework as filters
from drf_yasg import openapi
from drf_yasg.utils import swagger_auto_schema
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from multinet.api.models import Workspace, WorkspaceRole, WorkspaceRoleChoice
from multinet.api.utils.arango import ArangoQuery
from multinet.api.views.serializers import (
    PermissionsCreateSerializer,
    PermissionsReturnSerializer,
    SingleUserWorkspacePermissionSerializer,
    WorkspaceCreateSerializer,
    WorkspaceRenameSerializer,
    WorkspaceSerializer,
)
from multinet.auth.decorators import require_workspace_ownership, require_workspace_permission

from .common import MultinetPagination


def build_user_list(validated_data: OrderedDict) -> list:
    """
    Build a list of user objects from an ordered dictionary of user data.

    Accepts an ordered dictionary containing a list of validated user data, e.g.
    as a result of validating a PermissionsSerializer with request data.
    Returns a list of user objects.
    """
    user_list = []
    for valid_user in validated_data:
        user_object = get_object_or_404(User, username=valid_user['username'])
        user_list.append(user_object)
    return user_list


class WorkspaceViewSet(ReadOnlyModelViewSet):
    queryset = Workspace.objects.select_related('owner').all()
    lookup_field = 'name'

    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = WorkspaceSerializer

    filter_backends = [filters.DjangoFilterBackend]
    filterset_fields = ['name']

    pagination_class = MultinetPagination

    # Categorize entire ViewSet
    swagger_tags = ['workspaces']

    def get_queryset(self):
        """
        Get the workspaces for a request.

        Filter the queryset on a per-request basis to include only public workspaces
        and those workspaces for which the request user has at least reader access.
        """
        readable_private_workspaces = Q(
            workspacerole__in=WorkspaceRole.objects.filter(user__id=self.request.user.id)
        )
        owned_workspaces = Q(owner__id=self.request.user.id)
        public_workspaces = Q(public=True)
        return self.queryset.filter(
            public_workspaces | readable_private_workspaces | owned_workspaces
        ).distinct()

    @swagger_auto_schema(
        request_body=WorkspaceCreateSerializer(),
        responses={200: WorkspaceSerializer()},
    )
    def create(self, request):
        serializer = WorkspaceCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        is_public = serializer.validated_data.get('public', False)

        workspace, created = Workspace.objects.get_or_create(
            name=serializer.validated_data['name'], public=is_public, owner=request.user
        )

        if created:
            workspace.save()
        return Response(WorkspaceSerializer(workspace).data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        request_body=WorkspaceRenameSerializer(),
        responses={200: WorkspaceSerializer()},
    )
    @require_workspace_permission(WorkspaceRoleChoice.MAINTAINER)
    def update(self, request, name):
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        serializer = WorkspaceRenameSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        workspace.name = serializer.validated_data['name']
        workspace.save()

        return Response(WorkspaceSerializer(workspace).data, status=status.HTTP_200_OK)

    @require_workspace_ownership
    def destroy(self, request, name):
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        workspace.delete()
        return Response(None, status=status.HTTP_204_NO_CONTENT)

    @swagger_auto_schema(responses={200: PermissionsReturnSerializer()})
    @action(detail=True, url_path='permissions')
    @require_workspace_permission(WorkspaceRoleChoice.MAINTAINER)
    def get_workspace_permissions(self, request, name: str):
        """
        Get workspace permission details for a workspace.

        Action to get all object permissions for a given workspace.
        Note that get_permissions is not allowed as a function name, since it
        is already in use by the framework.
        """
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        serializer = PermissionsReturnSerializer(workspace)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @swagger_auto_schema(responses={200: SingleUserWorkspacePermissionSerializer()})
    @action(detail=True, url_path='permissions/me')
    @require_workspace_permission(WorkspaceRoleChoice.READER)
    def get_current_user_workspace_permissions(self, request, name: str):
        """Get the workspace permission for the user of the request."""
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        user = request.user
        permission, permission_label = workspace.get_user_permission_tuple(user)
        data = {
            'username': user.username,
            'workspace': name,
            'permission': permission,
            'permission_label': permission_label,
        }

        serializer = SingleUserWorkspacePermissionSerializer(data=data)
        serializer.is_valid(raise_exception=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        request_body=PermissionsCreateSerializer(), responses={200: PermissionsReturnSerializer()}
    )
    @get_workspace_permissions.mapping.put
    @require_workspace_permission(WorkspaceRoleChoice.MAINTAINER)
    def put_workspace_permissions(self, request, name: str):
        """Update existing workspace permissions."""
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        serializer = PermissionsCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_data = serializer.validated_data

        # maintainers and owners can make changes to all of this data
        workspace.public = validated_data['public']
        workspace.save()

        new_readers = build_user_list(validated_data['readers'])
        new_writers = build_user_list(validated_data['writers'])
        new_maintainers = build_user_list(validated_data['maintainers'])
        workspace.set_user_permissions_bulk(
            readers=new_readers, writers=new_writers, maintainers=new_maintainers
        )

        if workspace.owner == request.user:
            new_owner_name = validated_data['owner']['username']
            new_owner = get_object_or_404(User, username=new_owner_name)
            workspace.set_owner(new_owner)

        return Response(PermissionsReturnSerializer(workspace).data, status=status.HTTP_200_OK)

    @swagger_auto_schema(manual_parameters=[openapi.Parameter('query', 'query', type='string')])
    @action(detail=True)
    @require_workspace_permission(WorkspaceRoleChoice.READER)
    def aql(self, request, name: str):
        """Execute AQL in a workspace."""
        query_str = request.query_params.get('query')
        if query_str is None:
            return Response('No query provided', status=status.HTTP_400_BAD_REQUEST)

        workspace: Workspace = get_object_or_404(Workspace, name=name)
        database = workspace.get_arango_db_readonly()
        query = ArangoQuery(database, query_str)

        try:
            cursor: Cursor = query.execute()
            return Response(
                cursor,
                status=status.HTTP_200_OK,
            )
        except AQLQueryExecuteError as err:
            # Invalid query, too much memory, invalid permissions
            return Response(
                err.error_message,
                status=status.HTTP_400_BAD_REQUEST,
            )
        except ArangoServerError as err:
            # Arango server errors unrelated to the client's query
            return Response(
                err.error_message,
                status=err.http_code,
            )
