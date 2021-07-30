from functools import wraps
from typing import Any

from django.http import HttpResponseForbidden
from django.http.response import HttpResponseNotFound
from django.shortcuts import get_object_or_404

from multinet.api.models import Workspace
from multinet.api.utils.workspace_permissions import WorkspacePermission


def require_workspace_permission(minimum_permission: WorkspacePermission, allow_public=False)\
        -> Any:
    """
    Decorate a Workspace API endpoint to check for object permissions.
    This decorator works for enpoints that take action on a single workspace (e.g.
    [DELETE] /api/workspaces/{name}/). Returns Http403 if the request's user
    does not have appropriate permissions, or Http404 if the request's user has no
    permissions and workspace is not public.
    """
    def require_permission_inner(func: Any) -> Any:

        @wraps(func)
        def wrapper(view_set: Any, request: Any, workspace_name) -> Any:

            workspace: Workspace = get_object_or_404(Workspace, name=workspace_name)
            user = request.user
            user_perm = workspace.get_user_permission(user)

            if workspace.public and allow_public:
                return func(view_set, request, workspace_name)

            if user_perm is None:
                if workspace.public:
                    return HttpResponseForbidden()
                return HttpResponseNotFound()

            if user_perm.value >= minimum_permission.value:
                return func(view_set, request, workspace_name)
            return HttpResponseForbidden()

        return wrapper
    return require_permission_inner


def require_parent_workspace_permission(minimum_permission: WorkspacePermission,
                                        allow_public=False) -> Any:

    def require_parent_workspace_permission_inner(func: Any) -> Any:
        @wraps(func)
        def wrapper(view_set: Any, request: Any, workspace_name: str, child_name: str) -> Any:
            workspace: Workspace = get_object_or_404(Workspace, name=workspace_name)
            user = request.user
            user_perm = workspace.get_user_permission(user)

            if workspace.public and allow_public:
                return func(view_set, request, workspace_name, child_name)

            if user_perm is None:
                if workspace.public:
                    return HttpResponseForbidden()
                return HttpResponseNotFound()

            if user_perm.value >= minimum_permission.value:
                return func(view_set, request, workspace_name, child_name)
            return HttpResponseForbidden()
        return wrapper
    return require_parent_workspace_permission_inner
