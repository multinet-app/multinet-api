from drf_yasg import openapi
from rest_framework.pagination import PageNumberPagination


class MultinetPagination(PageNumberPagination):
    page_size = 25
    max_page_size = 100
    page_size_query_param = 'page_size'


OPENAPI_ROWS_SCHEMA = openapi.Schema(
    type=openapi.TYPE_ARRAY, items=openapi.Schema(type=openapi.TYPE_OBJECT)
)
