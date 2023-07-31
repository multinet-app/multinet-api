from typing import Dict, List

from arango.cursor import Cursor
from django.http.response import Http404
from django.shortcuts import get_object_or_404
from rest_framework.pagination import LimitOffsetPagination
from rest_framework.request import Request
from rest_framework_extensions.mixins import NestedViewSetMixin

from multinet.api.models import Workspace, WorkspaceRole
from multinet.api.utils.arango import ArangoQuery


class MultinetPagination(LimitOffsetPagination):
    default_limit = 100


class ArangoPagination(LimitOffsetPagination):
    """Override the LimitOffsetPagination class to allow for use with arango cursors."""

    def _set_pre_query_params(self, request):
        self.limit = self.get_limit(request)
        if self.limit is None:
            return None

        self.offset = self.get_offset(request)
        self.request = request

    def _set_post_query_params(self):
        if self.count > self.limit and self.template is not None:
            self.display_page_controls = True

    def paginate_queryset(self, query: ArangoQuery, request: Request) -> List[Dict]:
        self._set_pre_query_params(request)

        paginated_query = query.paginate(self.limit, self.offset)
        cur: Cursor = paginated_query.execute(full_count=True)

        self.count = cur.statistics()['fullCount']
        self._set_post_query_params()
        return list(cur)


class WorkspaceChildMixin(NestedViewSetMixin):
    prefix = None

    @property
    def workspace_field(self):
        field = 'workspace__name'
        if self.prefix is not None:
            field = f'{self.prefix}__{field}'

        return field

    def get_parents_query_dict(self):
        parents_query_dict = super().get_parents_query_dict()

        # Replace the standard lookup field with one that (possibly) goes
        # through the session object's related network or table object.
        new_field = self.workspace_field
        if new_field not in parents_query_dict:
            old_field = 'workspace__name'
            parents_query_dict[new_field] = parents_query_dict.pop(old_field)

        return parents_query_dict

    def get_queryset(self):
        """
        Get the queryset for workspace child enpoints.

        Check that the requeting user has appropriate permissions for the associated workspace.
        """
        child_objects = super().get_queryset()

        # prevent warning for schema generation incompatibility
        if getattr(self, 'swagger_fake_view', False):
            return child_objects.none()

        parent_query_dict = self.get_parents_query_dict()
        workspace = get_object_or_404(
            Workspace.objects.select_related('owner'), name=parent_query_dict[self.workspace_field]
        )

        # No user or user permission required for public workspaces
        if workspace.public:
            return child_objects

        # Private workspace
        request_user = self.request.user
        if not request_user.is_authenticated:  # anonymous user
            raise Http404

        workspace_role = WorkspaceRole.objects.filter(
            workspace=workspace, user=request_user
        ).first()

        # If the user is at least a reader or the owner, grant access
        if workspace_role is not None or workspace.owner == request_user:
            return child_objects

        # Read access denied
        raise Http404


class NetworkWorkspaceChildMixin(WorkspaceChildMixin):
    prefix = 'network'


class TableWorkspaceChildMixin(WorkspaceChildMixin):
    prefix = 'table'
