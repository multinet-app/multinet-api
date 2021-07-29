from functools import wraps
from typing import Any

from django.http import HttpResponseForbidden
from django.shortcuts import get_object_or_404

from multinet.api.models import Workspace
from multinet.api.utils.workspace_permissions import WorkspacePermission


def require_permission(minimum_permission: WorkspacePermission, allow_public=False) -> Any:
    """
    Decorate an API endpoint to check for object permissions.
    This decorator works for enpoints that take action on a single workspace (i.e.
    [GET, DELETE] /api/workspaces/{name}/). Returns Http403 if the request's user
    does not have appropriate permissions.
    """
    def require_permission_inner(func: Any) -> Any:

        @wraps(func)
        def wrapper(view_set: Any, request: Any, name: str) -> Any:
            workspace: Workspace = get_object_or_404(Workspace, name=name)
            if workspace.public and allow_public:
                return func(view_set, request, name)

            user = request.user
            user_perm = workspace.get_user_permission(user)

            if user_perm is None or minimum_permission.value not in user_perm.associated_perms:
                return HttpResponseForbidden()
            return func(view_set, request, name)

        return wrapper
    return require_permission_inner
