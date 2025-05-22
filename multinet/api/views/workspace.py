from typing import OrderedDict

from arango.cursor import Cursor
from arango.exceptions import AQLQueryExecuteError, ArangoServerError
from django.contrib.auth.models import User
from django.db.models import Q
from django.shortcuts import get_object_or_404
from django_filters import rest_framework as filters
from drf_yasg.utils import swagger_auto_schema
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from multinet.api.auth.decorators import (
    require_workspace_ownership,
    require_workspace_permission,
)
from multinet.api.models import (
    Network,
    Table,
    TableTypeAnnotation,
    Workspace,
    WorkspaceRole,
    WorkspaceRoleChoice,
)
from multinet.api.utils.arango import ArangoQuery
from multinet.api.views.serializers import (
    AqlQuerySerializer,
    PermissionsCreateSerializer,
    PermissionsReturnSerializer,
    SingleUserWorkspacePermissionSerializer,
    WorkspaceCreateSerializer,
    WorkspaceRenameSerializer,
    WorkspaceSerializer,
)

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
        responses={200: WorkspaceSerializer()},
    )
    @action(detail=True, url_path='fork', methods=['POST'])
    def fork(self, request, name) -> Workspace:
        """
        Fork this workspace, creating a new workspace with the same tables and networks.

        Uses 'network' and 'table' query parameters to determine which tables and/or networks to copy.
        if the query parameter is undefined, no tables or networks will be copied.

        These should be comma-separated lists of table and network names.

        Example:
        /api/workspaces/miserables/fork/?tables=characters%2Crelationships&networks=miserables

        The new workspace will be private by default and the name will be:
        'Fork of {original workspace name}'
        """
        workspace: Workspace = get_object_or_404(Workspace, name=name)

        # get query params for tables and networks
        tables = request.query_params.get('tables', None)
        networks = request.query_params.get('networks', None)

        new_name = f'Fork of {workspace.name}'
        # if the new name is not unique, append a number to the end
        i = 1
        while Workspace.objects.filter(name=new_name).exists():
            new_name = f'Fork of {workspace.name} ({i})'
            i += 1

        new_workspace = Workspace.objects.create(name=new_name, owner=request.user, public=False)

        # Copy the tables and permissions from the original workspace
        # Parse the tables query parameter into a list, if provided
        table_names = [t.strip() for t in tables.split(',')] if tables else []

        for table in Table.objects.filter(workspace=workspace, name__in=table_names):
            if not table_names or table.name not in table_names:
                continue  # Skip tables not in the specified list
            new_table = table.copy(new_workspace)
            # Copy the type annotations
            for type_annotation in TableTypeAnnotation.objects.filter(table=new_table):
                TableTypeAnnotation.objects.create(
                    table=new_table, type=type_annotation.type, column=type_annotation.column
                )
            new_table.save()

        # Copy the networks and their permissions from the original workspace
        # Parse the networks query parameter into a list, if provided
        network_names = [n.strip() for n in networks.split(',')] if networks else []

        for network in Network.objects.filter(workspace=workspace, name__in=network_names):
            if not network_names or network.name not in network_names:
                continue  # Skip networks not in the specified list
            new_network = network.copy(new_workspace)
            new_network.save()

        new_workspace.save()

        return Response(WorkspaceSerializer(new_workspace).data, status=status.HTTP_200_OK)

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

    @swagger_auto_schema(request_body=AqlQuerySerializer())
    @action(detail=True, methods=['POST'])
    @require_workspace_permission(WorkspaceRoleChoice.READER)
    def aql(self, request, name: str):
        """Execute AQL in a workspace."""
        serializer = AqlQuerySerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        # Retrieve workspace and db
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        database = workspace.get_arango_db()

        # Form query
        query_str = serializer.validated_data['query']
        bind_vars = serializer.validated_data['bind_vars']
        query = ArangoQuery(database, query_str=query_str, bind_vars=bind_vars)

        try:
            cursor: Cursor = query.execute()
            return Response(
                cursor,
                status=status.HTTP_200_OK,
            )
        except AQLQueryExecuteError as err:
            # Invalid query, time/memory limit reached, or
            # attempt to run a mutating query as the readonly user
            return Response(
                err.error_message,
                status=status.HTTP_400_BAD_REQUEST,
            )
        except ArangoServerError as err:
            # Arango server errors unrelated to the client's query
            return Response(err.error_message, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=True, methods=['GET'])
    @require_workspace_permission(WorkspaceRoleChoice.READER)
    def network_build_requests(self, request, name: str):
        # Use the workspace parameter in your code
        workspace_obj: Workspace = Workspace.objects.get(name=name)

        # Needs root access since root is making the AQL query job
        db = workspace_obj.get_arango_db(readonly=False)
        jobs = db.async_jobs('pending')

        return Response(data=jobs, status=status.HTTP_200_OK)
