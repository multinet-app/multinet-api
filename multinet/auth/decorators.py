from functools import wraps
from typing import Any

from django.http import HttpResponseForbidden
from django.http.response import HttpResponseNotFound
from django.shortcuts import get_object_or_404

from multinet.api.models import Workspace
from multinet.api.utils.workspace_permissions import WorkspacePermission


def require_permission(minimum_permission: WorkspacePermission, is_workspace, allow_public=False)\
        -> Any:

    """
    Decorate an API endpoint to check for object permissions.
    This decorator works for enpoints that take action on a single workspace (i.e.
    [GET, DELETE] /api/workspaces/{name}/). Returns Http403 if the request's user
    does not have appropriate permissions.
    """
    def require_permission_inner(func: Any) -> Any:

        @wraps(func)
        def wrapper(view_set: Any, request: Any, name="", **kwargs) -> Any:
            print(name)
            print(kwargs)

            workspace_name = ""
            return_func = None
            if(is_workspace):
                workspace_name = name
                return_func = func(view_set, request, workspace_name)
            elif 'parent_lookup_workspace__name' in kwargs:
                workspace_name = kwargs['parent_lookup_workspace__name']
                return_func = func(view_set, request, workspace_name, name)

            workspace: Workspace = get_object_or_404(Workspace, name=workspace_name)
            user = request.user
            user_perm = workspace.get_user_permission(user)

            if workspace.public and allow_public:
                return return_func
                # return func(view_set, request, workspace_name)

            if user_perm is None:
                if workspace.public:
                    return HttpResponseForbidden()
                return HttpResponseNotFound()

            if user_perm.value >= minimum_permission.value:
                return return_func
                # return func(view_set, request, workspace_name)
            return HttpResponseForbidden()

        return wrapper
    return require_permission_inner
