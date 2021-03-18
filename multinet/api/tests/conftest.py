import pytest
from pytest_factoryboy import register
from rest_framework.test import APIClient

from multinet.api.utils.arango import arango_system_db

from .factories import NetworkFactory, TableFactory, UserFactory, WorkspaceFactory


@pytest.fixture
def api_client() -> APIClient:
    return APIClient()


@pytest.fixture
def authenticated_api_client(user) -> APIClient:
    client = APIClient()
    client.force_authenticate(user=user)
    return client


def pytest_configure():
    # Register which databases exist before the function is run.
    pytest.before_session_arango_databases = set(arango_system_db().databases())


def pytest_sessionfinish(session, exitstatus):
    # Remove any databases created since the session start. This is needed because pytest's
    # `pytest.mark.django_db` decorator doesn't run the model save/delete methods, meaning the sync
    # between arangodb and django doesn't happen.

    for db in arango_system_db().databases():
        if db not in pytest.before_session_arango_databases:
            arango_system_db().delete_database(db, ignore_missing=True)


register(UserFactory)
register(WorkspaceFactory)
register(NetworkFactory)
register(TableFactory)
