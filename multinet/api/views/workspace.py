# from django.http import HttpResponseRedirect
from django.shortcuts import get_object_or_404
from django.contrib.auth.models import User
from django_filters import rest_framework as filters
from drf_yasg.utils import swagger_auto_schema
from guardian.shortcuts import assign_perm, get_objects_for_user
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework_extensions.mixins import NestedViewSetMixin

from typing import OrderedDict

from multinet.api.models import Workspace
from multinet.api.views.serializers import (
    WorkspaceCreateSerializer,
    WorkspaceSerializer,
    PermissionsSerializer,
    PermissionsReturnSerializer
)
from multinet.auth.decorators import require_permission
from multinet.api.utils.workspace_permissions import OWNER, READER, READER_LIST

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
        """
        Get all workspaces that the request user has permission to read.
        This includes public workspaces.
        Superusers have permission for every object.
        """
        public_workspaces = self.queryset.filter(public=True)
        private_workspaces = get_objects_for_user(request.user,
                                                  READER_LIST,
                                                  self.queryset.filter(public=False),
                                                  any_perm=True)
        all_readable_workspaces = public_workspaces | private_workspaces

        return Response(WorkspaceSerializer(all_readable_workspaces, many=True).data,
                        status=status.HTTP_200_OK)

    @swagger_auto_schema(
        responses={200: WorkspaceSerializer()}
    )
    @require_permission(minimum_permission=READER, allow_public=True)
    def retrieve(self, request, name):
        """
        Get a single workspace by name. Requesting user must have at least reader permission.
        """
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        return Response(WorkspaceSerializer(workspace).data, status=status.HTTP_200_OK)

    @require_permission(minimum_permission=OWNER)
    def destroy(self, request, name):
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        workspace.delete()
        return Response(None, status=status.HTTP_204_NO_CONTENT)

    @swagger_auto_schema(
        responses={200: PermissionsReturnSerializer()}
    )
    @action(detail=True, url_path='permissions')
    def get_workspace_permissions(self, request, name: str):
        """
        Action to get all object permissions for a given workspace.
        Please note that get_permissions is not allowed as a function name, since it
        is already in use by the framework.
        """
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        serializer = PermissionsReturnSerializer(workspace)
        print(serializer.data)
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
    def put_workspace_permissions(self, request, name: str):
        """
        Update existing workspace permissions

        PATCH endpoint for object permissions on workspaces.
        """
        workspace: Workspace = get_object_or_404(Workspace, name=name)
        serializer = PermissionsSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        validated_data = serializer.validated_data

        workspace.public = validated_data['public']

        new_owners = self.build_user_list(validated_data['owners'])
        workspace.set_owners(new_owners)

        new_maintainers = self.build_user_list(validated_data['maintainers'])
        workspace.set_maintainers(new_maintainers)

        new_writers = self.build_user_list(validated_data['writers'])
        workspace.set_writers(new_writers)

        new_readers = self.build_user_list(validated_data['readers'])
        workspace.set_readers(new_readers)

        return Response(PermissionsReturnSerializer(workspace).data, status=status.HTTP_200_OK)
