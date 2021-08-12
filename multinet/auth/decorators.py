from functools import wraps
from typing import Any

from django.http import HttpResponseForbidden
from django.http.response import HttpResponseNotFound
from django.shortcuts import get_object_or_404

from multinet.api.models import Workspace, WorkspaceRole, WorkspaceRoleChoice


def _get_workspace_and_user(*args, **kwargs):
    """
    Determine the workspace and user from ambiguous arguments.

    Helper function to get the workspace and user from the arguments
    passed to an API endpoint function. Since different endpoints have different
    arguments, the permission-checking decorator must be able to handle a variety of scenarios.
    This function pulls out the workspace name passed on the keyword arguments passed in.
    """
    workspace_name = ''
    if 'parent_lookup_workspace__name' in kwargs:
        workspace_name = kwargs['parent_lookup_workspace__name']
    elif 'name' in kwargs:
        workspace_name = kwargs['name']

    workspace = get_object_or_404(Workspace.objects.select_related('owner'), name=workspace_name)
    user = args[1].user

    return workspace, user


def require_workspace_permission(minimum_permission: WorkspaceRoleChoice) -> Any:
    """
    Check a request for proper workspace-level permissions.

    This decorator works for endpoints that take action on a single workspace, or on children
    (tables and networks) on a single workspace.
    Returns Http403 if the request's user does not have appropriate permissions,
    or Http404 if the request's user has no permissions and workspace is not public.
    """

    def require_permission_inner(func: Any) -> Any:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            workspace, user = _get_workspace_and_user(*args, **kwargs)
            user_permission: WorkspaceRole = workspace.get_user_permission(user)
            allow_public = workspace.public and minimum_permission == WorkspaceRoleChoice.READER
            has_minimum_permission = (
                user_permission is not None and user_permission.role >= minimum_permission
            )

            if allow_public or workspace.owner == user or has_minimum_permission:
                return func(*args, **kwargs)

            if workspace.public:
                return HttpResponseForbidden()

            # Private workspace
            if user_permission is not None:
                return HttpResponseForbidden()
            return HttpResponseNotFound()

        return wrapper

    return require_permission_inner


def require_workspace_ownership(func: Any) -> Any:
    """Check a request for workspace ownership."""

    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        workspace, user = _get_workspace_and_user(*args, **kwargs)

        if workspace.owner == user:
            return func(*args, **kwargs)

        user_permission: WorkspaceRole = workspace.get_user_permission(user)
        if user_permission is None:
            return HttpResponseNotFound()
        return HttpResponseForbidden()

    return wrapper
