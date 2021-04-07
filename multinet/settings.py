from __future__ import annotations

from pathlib import Path

from composed_configuration import (
    ComposedConfiguration,
    ConfigMixin,
    DevelopmentBaseConfiguration,
    HerokuProductionBaseConfiguration,
    ProductionBaseConfiguration,
    TestingBaseConfiguration,
)
from configurations import values


class MultinetMixin(ConfigMixin):
    WSGI_APPLICATION = 'multinet.wsgi.application'
    ROOT_URLCONF = 'multinet.urls'

    BASE_DIR = Path(__file__).resolve(strict=True).parent.parent

    @staticmethod
    def before_binding(configuration: ComposedConfiguration) -> None:
        configuration.INSTALLED_APPS = [
            'multinet.api.apps.ApiConfig',
        ] + configuration.INSTALLED_APPS

        configuration.INSTALLED_APPS += [
            's3_file_field',
            'guardian',
            'allauth.socialaccount.providers.google',
        ]

        configuration.AUTHENTICATION_BACKENDS += ['guardian.backends.ObjectPermissionBackend']

    MULTINET_ARANGO_URL = values.Value(environ_required=True)
    MULTINET_ARANGO_PASSWORD = values.Value(environ_required=True)
    SWAGGER_SETTINGS = {
        'DEFAULT_AUTO_SCHEMA_CLASS': 'multinet.api.utils.swagger.ImprovedAutoSchema'
    }


class DevelopmentConfiguration(MultinetMixin, DevelopmentBaseConfiguration):
    pass


class TestingConfiguration(MultinetMixin, TestingBaseConfiguration):
    pass


class ProductionConfiguration(MultinetMixin, ProductionBaseConfiguration):
    pass


class HerokuProductionConfiguration(MultinetMixin, HerokuProductionBaseConfiguration):
    pass
