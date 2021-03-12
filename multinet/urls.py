from django.conf import settings
from django.contrib import admin
from django.urls import include, path
from drf_yasg import openapi
from drf_yasg.views import get_schema_view
from rest_framework import permissions
from rest_framework_extensions.routers import ExtendedSimpleRouter

from multinet.api.views import (
    NetworkViewSet,
    TableViewSet,
    WorkspaceViewSet,
    users_me_view,
    users_search_view,
)

router = ExtendedSimpleRouter()
workspaces_routes = router.register(r'workspaces', WorkspaceViewSet)
workspaces_routes.register(
    'tables',
    TableViewSet,
    basename='table',
    parents_query_lookups=[f'workspace__{WorkspaceViewSet.lookup_field}'],
)
workspaces_routes.register(
    'networks',
    NetworkViewSet,
    basename='network',
    parents_query_lookups=[f'workspace__{WorkspaceViewSet.lookup_field}'],
)

# OpenAPI generation
schema_view = get_schema_view(
    openapi.Info(title='multinet', default_version='v1', description=''),
    public=True,
    permission_classes=(permissions.AllowAny,),
)

urlpatterns = [
    path('accounts/', include('allauth.urls')),
    path('oauth/', include('oauth2_provider.urls', namespace='oauth2_provider')),
    path('admin/', admin.site.urls),
    path('api/s3-upload/', include('s3_file_field.urls')),
    path('api/', include(router.urls)),
    path('api/users/me', users_me_view),
    path('api/users/search', users_search_view),
    path('api/docs/redoc/', schema_view.with_ui('redoc'), name='docs-redoc'),
    path('swagger/', schema_view.with_ui('swagger'), name='docs-swagger'),
]

if settings.DEBUG:
    import debug_toolbar

    urlpatterns = [path('__debug__/', include(debug_toolbar.urls))] + urlpatterns
