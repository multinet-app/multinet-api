import itertools

from django.contrib.auth.models import User
from faker import Faker
from guardian.shortcuts import assign_perm
import pytest
from pytest_factoryboy import register
from rest_framework.test import APIClient

from multinet.api.models import Network, Table, Workspace
from multinet.api.tests.utils import generate_arango_documents
from multinet.api.utils.arango import arango_system_db

from .factories import NetworkFactory, TableFactory, UserFactory, WorkspaceFactory


@pytest.fixture
def api_client() -> APIClient:
    return APIClient()


@pytest.fixture
def authenticated_api_client(user: User) -> APIClient:
    client = APIClient()
    client.force_authenticate(user=user)
    return client


@pytest.fixture
def owned_workspace(user: User, workspace: Workspace) -> Workspace:
    """Return a workspace with the `user` fixture as an owner."""
    assign_perm('owner', user, workspace)

    return workspace


@pytest.fixture
def populated_node_table(owned_workspace: Workspace) -> Table:
    table: Table = Table.objects.create(name=Faker().pystr(), edge=False, workspace=owned_workspace)

    nodes = generate_arango_documents(5)
    table.put_rows(nodes)

    return table


@pytest.fixture
def populated_edge_table(owned_workspace: Workspace, populated_node_table: Table) -> Table:
    table: Table = Table.objects.create(name=Faker().pystr(), edge=True, workspace=owned_workspace)

    nodes = list(populated_node_table.get_rows())
    edges = [{'_from': a['_id'], '_to': b['_id']} for a, b in itertools.combinations(nodes, 2)]
    table.put_rows(edges)

    return table


@pytest.fixture
def populated_network(owned_workspace: Workspace, populated_edge_table: Table) -> Network:
    node_tables = list(populated_edge_table.find_referenced_node_tables().keys())
    network_name = Faker().pystr()

    # Create graph in arango before creating the Network object here
    owned_workspace.get_arango_db().create_graph(
        network_name,
        edge_definitions=[
            {
                'edge_collection': populated_edge_table.name,
                'from_vertex_collections': node_tables,
                'to_vertex_collections': node_tables,
            }
        ],
    )

    network: Network = Network.objects.create(name=network_name, workspace=owned_workspace)
    return network


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
