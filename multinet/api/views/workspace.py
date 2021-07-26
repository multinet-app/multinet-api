# from django.http import HttpResponseRedirect
from django.contrib.auth.models import User
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from django_filters import rest_framework as filters
from drf_yasg.utils import swagger_auto_schema
from guardian.decorators import permission_required_or_403
from guardian.shortcuts import assign_perm, remove_perm, get_users_with_perms, get_perms
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticatedOrReadOnly
from rest_framework.response import Response
from rest_framework.viewsets import ReadOnlyModelViewSet
from rest_framework_extensions.mixins import NestedViewSetMixin

from multinet.api.models import Workspace, workspace
from multinet.api.views.serializers import WorkspaceCreateSerializer, WorkspaceSerializer, PermissionsSerializer, PermissionsReturnSerializer

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

        workspace, created = Workspace.objects.get_or_create(
            name=serializer.validated_data['name'],
        )

        if created:
            workspace.save()

        assign_perm('owner', request.user, workspace)
        return Response(WorkspaceSerializer(workspace).data, status=status.HTTP_200_OK)

    @method_decorator(permission_required_or_403('owner', (Workspace, 'name', 'name')))
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
        Action to get all objet permissions for a given workspace.
        Please note that get_permissions is not allowed as a function name, since it 
        is already in use by the framework.
        """
        
        workspace: Workspace = get_object_or_404(Workspace, name=name)

        users_with_perms = get_users_with_perms(workspace)
        permissions_list = [{'permissions': get_perms(user, workspace), 'username': user.username} for user in users_with_perms]
        print(str(permissions_list)) # TEST
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

        # for user_permissions in validated_data:
        #     # get the user to update
        #     username = user_permissions["username"]
        #     user = get_object_or_404(User, username=username)
        #     permissions = user_permissions["permissions"]

        #     workspace.update_user_permissions(user, permissions)


        return_data = {
            "permissions": update_data,
            "workspace": workspace
        }
        
        return Response(PermissionsReturnSerializer(return_data).data, status=status.HTTP_200_OK)
