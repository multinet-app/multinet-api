from drf_yasg import openapi
from rest_framework.pagination import PageNumberPagination


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
