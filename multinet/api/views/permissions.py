from django.shortcuts import get_object_or_404
from django.utils.decorators import method_decorator
from rest_framework_extensions.mixins import NestedViewSetMixin
from rest_framework.viewsets import ViewSet
from rest_framework.response import Response
from guardian.decorators import permission_required_or_403
from guardian.shortcuts import get_users_with_perms, get_perms

from multinet.api.models import Workspace
from multinet.api.utils.arango import ArangoQuery
from multinet.api.views.serializers import PermissionsSerializer, UserPermissionsSerializer

"""
Created: 7/22/2021
File for the ViewSet for getting permissions by workspace.
Currently permissions are not a model. Object-level permissions are handled by
django-guardian.
"""

class UserPermissionsViewSet(NestedViewSetMixin, ViewSet):
    
    def list(self, request, parent_lookup_workspace__name):
        workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        
        users_with_perms = get_users_with_perms(workspace)
        dict_results = [{'permissions': get_perms(user, workspace), 'username': user.username} for user in users_with_perms]

        response_serializer = UserPermissionsSerializer(dict_results, many=True)
        return Response(response_serializer.data)


class PermissionsViewSet(NestedViewSetMixin, ViewSet):
    serializer_class = PermissionsSerializer
    
    @method_decorator(
        permission_required_or_403('owner', (Workspace, 'name', 'parent_lookup_workspace__name'))
    )
    def list(self, request, parent_lookup_workspace__name):
        workspace = get_object_or_404(Workspace, name=parent_lookup_workspace__name)
        workspace_name = workspace.name
        return Response(workspace_name)

    def create(self, request):
        pass

    def retrieve(self, request, pk=None):
        pass

    def update(self, request, pk=None):
        pass

    def partial_update(self, request, pk=None):
        pass

    def destroy(self, request, pk=None):
        pass