from collections import OrderedDict
from typing import Iterable, Tuple

from drf_yasg import openapi
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.utils.urls import remove_query_param, replace_query_param


class MultinetPagination(PageNumberPagination):
    page_size = 25
    max_page_size = 100
    page_size_query_param = 'page_size'


ARRAY_OF_OBJECTS_SCHEMA = openapi.Schema(
    type=openapi.TYPE_ARRAY, items=openapi.Schema(type=openapi.TYPE_OBJECT)
)

PAGINATION_QUERY_PARAMS = [
    openapi.Parameter('page', 'query', type='integer'),
    openapi.Parameter('page_size', 'query', type='integer'),
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
