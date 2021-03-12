from collections import OrderedDict
from typing import Dict, Iterable, List, Tuple

from arango.collection import StandardCollection
from arango.cursor import Cursor
from arango.database import StandardDatabase
from drf_yasg import openapi
from rest_framework.pagination import LimitOffsetPagination, PageNumberPagination
from rest_framework.response import Response
from rest_framework.utils.urls import remove_query_param, replace_query_param

from multinet.api.utils.arango import paginate_aql_query


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


class CustomPagination:
    def __init__(self, request, pagination_class) -> None:
        self.request = request
        self.pagination_class = pagination_class

        self.page, self.page_size = self._get_request_pagination_params()

    def _get_request_pagination_params(self) -> Tuple[int, int]:
        try:
            page = int(self.request.GET.get('page'))
        except (TypeError, ValueError):
            page = 1

        try:
            page_size = int(self.request.GET.get('page_size')) or 1
        except (TypeError, ValueError):
            page_size = self.pagination_class.page_size

        return (page, page_size)

    def create_paginated_response(self, results: Iterable, count: int) -> Response:
        url = self.request.build_absolute_uri()
        next_url = (
            replace_query_param(url, 'page', self.page + 1)
            if count > self.page * self.page_size
            else None
        )
        prev_url = None

        if self.page > 1:
            if self.page == 2:
                prev_url = remove_query_param(url, 'page')
            else:
                prev_url = replace_query_param(url, 'page', self.page - 1)

        return Response(
            OrderedDict(
                [
                    ('count', count),
                    ('next', next_url),
                    ('previous', prev_url),
                    ('results', results),
                ]
            )
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

    def paginate_queryset_from_collection(
        self, request, collection: StandardCollection
    ) -> List[Dict]:
        self._set_pre_query_params(request)
        cur: Cursor = collection.find({}, skip=self.offset, limit=self.limit)
        self.count = collection.count()

        self._set_post_query_params()
        return list(cur)

    def paginate_queryset(self, request, query: str, db: StandardDatabase) -> List[Dict]:
        self._set_pre_query_params(request)

        paginated_query_str = paginate_aql_query(query, self.limit, self.offset)
        cur: Cursor = db.aql.execute(paginated_query_str, full_count=True)
        self.count = cur.statistics()['fullCount']

        self._set_post_query_params()
        return list(cur)
