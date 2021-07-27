from django.http import HttpResponseForbidden
from django.shortcuts import get_object_or_404
from multinet.api.models import Workspace
from multinet.api.utils.workspace_permissions import (PERMISSION_RANK, highest_permission)
from guardian.shortcuts import get_user_perms
from typing import Any


def require_permission(minimum_permission: str) -> Any:
    """
    Decorate an API endpoint to check for object permissions.
    This decorator works for enpoints that take action on a single workspace (i.e.
    [GET, DELETE] /api/workspaces/{name}/). Returns Http403 if the request's user
    does not have appropriate permissions.
    """
    def require_permission_inner(func: Any) -> Any:
        def wrapper(view_set: Any, request: Any, name: str) -> Any:
            workspace: Workspace = get_object_or_404(Workspace, name=name)
            user = request.user
            user_perms = get_user_perms(user, workspace)

            print(PERMISSION_RANK[minimum_permission])
            print(highest_permission(user_perms))

            # minimum_permission should likely be validated
            if not highest_permission(user_perms) >= PERMISSION_RANK[minimum_permission]:
                return HttpResponseForbidden()

            return func(view_set, request, name)

        return wrapper
    return require_permission_inner
