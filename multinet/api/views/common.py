from typing import Dict, List

from arango.cursor import Cursor
from drf_yasg import openapi
from rest_framework.pagination import LimitOffsetPagination, PageNumberPagination
from rest_framework.request import Request

from multinet.api.utils.arango import ArangoQuery


class MultinetPagination(PageNumberPagination):
    page_size = 25
    max_page_size = 100
    page_size_query_param = 'page_size'


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
        cur: Cursor = query.db.aql.execute(
            query=paginated_query.query_str, bind_vars=paginated_query.bind_vars, full_count=True
        )

        self.count = cur.statistics()['fullCount']
        self._set_post_query_params()
        return list(cur)
