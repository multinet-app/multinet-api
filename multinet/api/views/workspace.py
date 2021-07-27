# from django.http import HttpResponseRedirect
from django.contrib.auth.models import User
from django.shortcuts import get_object_or_404
from django_filters import rest_framework as filters
from drf_yasg.utils import swagger_auto_schema
from guardian.shortcuts import assign_perm, get_users_with_perms, get_perms
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework_extensions.mixins import NestedViewSetMixin

from multinet.api.models import Workspace
from multinet.api.views.serializers import (
    WorkspaceCreateSerializer,
    WorkspaceSerializer,
    PermissionsSerializer,
    PermissionsReturnSerializer
)
from multinet.auth.decorators import require_permission
from multinet.api.utils.workspace_permissions import OWNER, READER

from .common import MultinetPagination


class WorkspaceViewSet(NestedViewSetMixin, ReadOnlyModelViewSet):
    queryset = Workspace.objects.all()
    lookup_field = 'name'

    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = WorkspaceSerializer

    filter_backends = [filters.DjangoFilterBackend]
    filterset_fields = ['name']

    pagination_class = MultinetPagination

    # Categorize entire ViewSet
    swagger_tags = ['workspaces']

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

        assign_perm(OWNER, request.user, workspace)
        return Response(WorkspaceSerializer(workspace).data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        responses={200: WorkspaceSerializer(many=True)}
    )
    def list(self, request):
        # filter for public or permissions. easy
        return super().list(request)

    @swagger_auto_schema(
        responses={200: WorkspaceSerializer()}
    )
    @require_permission(minimum_permission=READER)
    def retrieve(self, request, name):
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        return Response(WorkspaceSerializer(workspace).data, status=status.HTTP_200_OK)

    @require_permission(minimum_permission=OWNER)
    def destroy(self, request, name):
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        workspace.delete()
        return Response(None, status=status.HTTP_204_NO_CONTENT)

    @swagger_auto_schema(
        responses={200: PermissionsReturnSerializer(many=True)}
    )
    @action(detail=True, url_path='permissions')
    def get_workspace_permissions(self, request, name: str):
        """
        Action to get all object permissions for a given workspace.
        Please note that get_permissions is not allowed as a function name, since it
        is already in use by the framework.
        """
        workspace: Workspace = get_object_or_404(Workspace, name=name)

        users_with_perms = get_users_with_perms(workspace)
        permissions_list = [{'permissions': get_perms(user, workspace), 'username': user.username}
                            for user in users_with_perms]
        response_data = {
            "workspace" : workspace,
            "permissions" : permissions_list
        }
        return Response(PermissionsReturnSerializer(response_data).data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        request_body=PermissionsSerializer(many=True),
        responses={200: PermissionsReturnSerializer()}
    )
    @get_workspace_permissions.mapping.patch
    def patch_workspace_permissions(self, request, name: str):
        """
        Update existing workspace permissions

        PATCH endpoint for object permissions on workspaces.
        """
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        request_data = PermissionsSerializer(data=request.data, many=True)
        request_data.is_valid(raise_exception=True)
        validated_data = request_data.validated_data

        # unpack the request data and add the user object to each dictionary, checking for existence
        update_data = [dict(user_permissions) for user_permissions in validated_data]
        for user_permissions in update_data:
            user = get_object_or_404(User, username=user_permissions["username"])
            print(user)
            user_permissions["user"] = user

        workspace.update_user_permissions(update_data)

        return_data = {
            "permissions": update_data,
            "workspace": workspace
        }
        return Response(PermissionsReturnSerializer(return_data).data, status=status.HTTP_200_OK)
