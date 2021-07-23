from django.contrib.auth.models import User
from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from drf_yasg.utils import swagger_auto_schema
from rest_framework import status
from rest_framework_extensions.mixins import NestedViewSetMixin
from rest_framework.viewsets import ViewSet
from rest_framework.permissions import IsAuthenticatedOrReadOnly, IsAuthenticated
from rest_framework.response import Response
from rest_framework.decorators import api_view, parser_classes, permission_classes, action
from rest_framework.parsers import JSONParser
from guardian.decorators import permission_required_or_403
from guardian.shortcuts import get_users_with_perms, get_perms, assign_perm, remove_perm

from multinet.api.models import Workspace
from multinet.api.utils.arango import ArangoQuery
from multinet.api.views.serializers import PermissionsSerializer, PermissionsReturnSerializer, WorkspaceSerializer

"""
Created: 7/22/2021
File for the ViewSet for getting permissions by workspace.
Currently permissions are not a model. Object-level permissions are handled by
django-guardian.
"""

class PermissionsViewSet(NestedViewSetMixin, ViewSet):
    
    """
    ViewSet for retrieving a list of permissions for a given workspace, and updating a user's permissions
    for that workspace. 
    """

    permission_classes = [IsAuthenticatedOrReadOnly]
    serializer_class = PermissionsReturnSerializer

    # categorize entire viewset
    swagger_tags = ['permissions']

    @swagger_auto_schema(
        responses={200: PermissionsReturnSerializer(many=True)},
    )
    # @api_view(['GET'])
    @parser_classes([JSONParser])
    def list(self, request, parent_lookup_workspace__name):
        workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        
        users_with_perms = get_users_with_perms(workspace)
        permissions_list = [{'permissions': get_perms(user, workspace), 'username': user.username} for user in users_with_perms]
        dict_return = {
            "workspace" : workspace, 
            "permissions" : permissions_list
        }
        return Response(PermissionsReturnSerializer(dict_return).data, status=status.HTTP_200_OK)

    @swagger_auto_schema(
        request_body=PermissionsSerializer(many=True),
        reponses={200: PermissionsReturnSerializer()}
    )
    @parser_classes([JSONParser])
    def create(self, request, parent_lookup_workspace__name):
        workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        serializer = PermissionsSerializer(
            data={
                **request.data,
            },
            many=True
        )
        serializer.is_valid(raise_exception=True)

        # TODO: make sure the user exists!
        query_set = User.objects.all().filter(username=serializer.validated_data['username'])
        user_list = list(query_set)
        if len(user_list) == 0:
            # uh oh
            return Response("Invalid username")

        update_user = user_list[0]
        update_permissions = serializer.validated_data['permissions']
        existing_permissions = get_perms(update_user, workspace)

        # add and remove permissions for the user as needed
        for permission in update_permissions:
            if permission not in existing_permissions:
                assign_perm(permission, update_user, workspace)

        for permission in existing_permissions:
            if permission not in update_permissions:
                remove_perm(permission, update_user, workspace)

        permissions = PermissionsSerializer({"username" : update_user.username, "permissions" : get_perms(update_user, workspace)})
        workspace_serializer = WorkspaceSerializer(workspace)

        return Response(PermissionsReturnSerializer({"permissions" : permissions, "workspace" : workspace_serializer}).data, status=status.HTTP_200_OK)

    @action(detail=False, methods=['put'], url_path='')
    def update_permissions(self, request, parent_lookup_workspace__name: str):
        pass

