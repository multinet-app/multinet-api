from typing import Dict, List

from arango.cursor import Cursor
from django.http.response import Http404
from django.shortcuts import get_object_or_404
from drf_yasg import openapi
from rest_framework.pagination import LimitOffsetPagination
from rest_framework.request import Request
from rest_framework_extensions.mixins import NestedViewSetMixin

from multinet.api.models import Workspace, WorkspaceRole
from multinet.api.utils.arango import ArangoQuery


class MultinetPagination(LimitOffsetPagination):
    default_limit = 100


ARRAY_OF_OBJECTS_SCHEMA = openapi.Schema(
    type=openapi.TYPE_ARRAY, items=openapi.Schema(type=openapi.TYPE_OBJECT)
)


LIMIT_OFFSET_QUERY_PARAMS = [
    openapi.Parameter('limit', 'query', type='integer'),
    openapi.Parameter('offset', 'query', type='integer'),
]

PAGINATED_RESULTS_SCHEMA = openapi.Schema(
    type=openapi.TYPE_OBJECT,
    required=['count', 'results'],
    properties={
        'count': openapi.Schema(type=openapi.TYPE_INTEGER),
        'next': openapi.Schema(type=openapi.TYPE_STRING, format='uri', x_nullable=True),
        'previous': openapi.Schema(type=openapi.TYPE_STRING, format='uri', x_nullable=True),
        'results': ARRAY_OF_OBJECTS_SCHEMA,
    },
)


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
        workspace = get_object_or_404(Workspace, name=parent_query_dict['workspace__name'])
        workspace_role = WorkspaceRole.objects.filter(
            workspace=workspace, user=self.request.user
        ).first()

        if workspace_role is not None or workspace.public:
            return child_objects
        raise Http404
