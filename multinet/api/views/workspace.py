from typing import OrderedDict, Union

from django.contrib.auth.models import User
from django.shortcuts import get_object_or_404
from django_filters import rest_framework as filters
from drf_yasg.utils import swagger_auto_schema
from guardian.shortcuts import get_objects_for_user
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet

from multinet.api.models import Workspace
from multinet.api.utils.workspace_permissions import WorkspacePermission
from multinet.api.views.serializers import (
    PermissionsReturnSerializer,
    PermissionsSerializer,
    WorkspaceCreateSerializer,
    WorkspaceSerializer,
)
from multinet.auth.decorators import require_workspace_permission

from .common import MultinetPagination


class WorkspaceViewSet(ReadOnlyModelViewSet):
    queryset = Workspace.objects.all()
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
        Override parent method.
        Filter the queryset on a per-request basis to include only public workspaces
        and those workspaces for which the request user has at least reader access.
        """
        public_workspaces = self.queryset.filter(public=True)
        private_workspaces = self.queryset.filter(public=False)
        reader_workspaces = get_objects_for_user(self.request.user,
                                                 WorkspacePermission.get_permission_codenames(),
                                                 private_workspaces,
                                                 any_perm=True,
                                                 accept_global_perms=False)
        all_readable_workspaces = public_workspaces | reader_workspaces
        return all_readable_workspaces

    @swagger_auto_schema(
        request_body=WorkspaceCreateSerializer(),
        responses={200: WorkspaceSerializer()},
    )
    def create(self, request):
        serializer = WorkspaceCreateSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        is_public = False
        if 'public' in serializer.validated_data:
            is_public = serializer.validated_data['public']

        workspace, created = Workspace.objects.get_or_create(
            name=serializer.validated_data['name'],
            public=is_public,
        )

        if created:
            workspace.save()

        workspace.set_user_permission(request.user, WorkspacePermission.owner)
        return Response(WorkspaceSerializer(workspace).data, status=status.HTTP_200_OK)

    @require_workspace_permission(WorkspacePermission.owner)
    def destroy(self, request, name):
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        workspace.delete()
        return Response(None, status=status.HTTP_204_NO_CONTENT)

    @swagger_auto_schema(
        responses={200: PermissionsReturnSerializer()}
    )
    @action(detail=True, url_path='permissions')
    @require_workspace_permission(WorkspacePermission.maintainer)
    def get_workspace_permissions(self, request, name: str):
        """
        Action to get all object permissions for a given workspace.
        Please note that get_permissions is not allowed as a function name, since it
        is already in use by the framework.
        """
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        serializer = PermissionsReturnSerializer(workspace)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def build_user_list(self, validated_data: OrderedDict) -> list:
        """
        Accepts an unordered dictionary containing a list of validated user data, e.g.
        as a result of validating a PermissionsSerializer with request data.
        Returns a list of user objects.
        """
        user_list = []
        for valid_user in validated_data:
            user_object = get_object_or_404(User, username=valid_user['username'])
            user_list.append(user_object)
        return user_list

    @swagger_auto_schema(
        request_body=PermissionsSerializer(),
        responses={200: PermissionsSerializer()}
    )
    @get_workspace_permissions.mapping.put
    @require_workspace_permission(WorkspacePermission.maintainer)
    def put_workspace_permissions(self, request, name: str):
        """
        Update existing workspace permissions

        PUT endpoint for object permissions on workspaces.
        """
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        serializer = PermissionsSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_data = serializer.validated_data

        # maintainers and owners can make changes to all of this data
        workspace.public = validated_data['public']
        workspace.save()

        new_readers = self.build_user_list(validated_data['readers'])
        workspace.set_readers(new_readers)

        new_writers = self.build_user_list(validated_data['writers'])
        workspace.set_writers(new_writers)

        new_maintainers = self.build_user_list(validated_data['maintainers'])
        workspace.set_maintainers(new_maintainers)

        # require ownership before editing owners list
        permission: Union[WorkspacePermission, None] = workspace.get_user_permission(request.user)
        if permission == WorkspacePermission.owner:
            new_owners = self.build_user_list(validated_data['owners'])
            workspace.set_owners(new_owners)

        return Response(PermissionsReturnSerializer(workspace).data, status=status.HTTP_200_OK)
