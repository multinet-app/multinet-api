from functools import wraps
from typing import Any

from django.http import HttpResponseForbidden
from django.http.response import HttpResponseNotFound
from django.shortcuts import get_object_or_404

from multinet.api.models import Workspace
from multinet.api.utils.workspace_permissions import WorkspacePermission


def _get_workspace_and_user(*args, **kwargs):
    """
    Helper function to retrieve the workspace and user from the arguments
    passed to an API endpoint function. Since different endpoints have different
    arguments, the permission-checking decorator must be able to handle a variety of scenarios.
    This function pulls out the workspace name passed on the keyword arguments passed in.
    """
    workspace_name = ""
    if 'parent_lookup_workspace__name' in kwargs:
        workspace_name = kwargs['parent_lookup_workspace__name']
    elif 'name' in kwargs:
        workspace_name = kwargs['name']

    workspace = get_object_or_404(Workspace, name=workspace_name)
    user = args[1].user

    return workspace, user


def require_workspace_permission(minimum_permission: WorkspacePermission, allow_public=False)\
        -> Any:
    """
    Decorate a Workspace API endpoint to check for object permissions.
    This decorator works for endpoints that take action on a single workspace, or on children
    (tables and networks) on a single workspace.
    Returns Http403 if the request's user does not have appropriate permissions,
    or Http404 if the request's user has no permissions and workspace is not public.
    """
    def require_permission_inner(func: Any) -> Any:

        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:

            workspace, user = _get_workspace_and_user(*args, **kwargs)
            user_perm = workspace.get_user_permission(user)

            if workspace.public and allow_public:
                return func(*args, **kwargs)

            if user_perm is None:
                if workspace.public:
                    return HttpResponseForbidden()
                return HttpResponseNotFound()

            if user_perm.value >= minimum_permission.value:
                return func(*args, **kwargs)
            return HttpResponseForbidden()

        return wrapper
    return require_permission_inner
