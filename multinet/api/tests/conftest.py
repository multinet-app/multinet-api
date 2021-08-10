import itertools

from django.contrib.auth.models import User
from faker import Faker
import pytest
from pytest_factoryboy import register
from rest_framework.test import APIClient
from s3_file_field.testing import S3FileFieldTestClient

from multinet.api.models import Network, Table, Workspace
from multinet.api.tests.utils import generate_arango_documents
from multinet.api.utils.arango import arango_system_db

from .factories import (
    NetworkFactory,
    PrivateWorkspaceFactory,
    PublicWorkspaceFactory,
    TableFactory,
    UploadFactory,
    UserFactory,
)


@pytest.fixture
def api_client() -> APIClient:
    return APIClient()


@pytest.fixture
def authenticated_api_client(user: User) -> APIClient:
    client = APIClient()
    client.force_authenticate(user=user)
    return client


@pytest.fixture()
def s3ff_client(authenticated_api_client):
    return S3FileFieldTestClient(
        authenticated_api_client,  # The test APIClient instance
        '/api/s3-upload',  # The (relative) path mounted in urlpatterns
    )


@pytest.fixture
def unowned_workspace(workspace: Workspace, user_factory: UserFactory) -> Workspace:
    # Create a private workspace NOT owned by the `user` fixture
    workspace.set_owner(user_factory())
    return workspace


@pytest.fixture
def public_workspace(workspace: Workspace, user_factory: UserFactory) -> Workspace:
    workspace.public = True
    workspace.owner = user_factory()  # don't use set_owner to prevent extra DB call
    workspace.save()
    return workspace


@pytest.fixture
def populated_node_table(workspace: Workspace) -> Table:
    table: Table = Table.objects.create(name=Faker().pystr(), edge=False, workspace=workspace)

    nodes = generate_arango_documents(5)
    table.put_rows(nodes)

    return table


def populated_table(workspace: Workspace, edge: bool) -> Table:
    if not edge:
        # create a node table
        table: Table = Table.objects.create(name=Faker().pystr(), edge=False, workspace=workspace)
        nodes = generate_arango_documents(5)
        table.put_rows(nodes)
        return table
    else:
        # create an edge table
        table: Table = Table.objects.create(name=Faker().pystr(), edge=True, workspace=workspace)
        node_table = populated_table(workspace, False)  # recursion
        nodes = list(node_table.get_rows())
        edges = [{'_from': a['_id'], '_to': b['_id']} for a, b in itertools.combinations(nodes, 2)]
        table.put_rows(edges)
        return table


def populated_network(workspace: Workspace) -> Network:
    populated_edge_table = populated_table(workspace, True)
    node_tables = list(populated_edge_table.find_referenced_node_tables().keys())
    network_name = Faker().pystr()
    return Network.create_with_edge_definition(
        name=network_name,
        workspace=workspace,
        edge_table=populated_edge_table.name,
        node_tables=node_tables,
    )


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
register(PrivateWorkspaceFactory)
register(PublicWorkspaceFactory, _name='public_workspace')
register(NetworkFactory)
register(TableFactory)
register(UploadFactory)
