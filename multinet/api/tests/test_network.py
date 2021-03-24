from typing import List

from faker import Faker
import pytest
from rest_framework.test import APIClient

from multinet.api.models import Network, Table, Workspace
from multinet.api.tests.factories import NetworkFactory, WorkspaceFactory
from multinet.api.tests.utils import assert_limit_offset_results

from .fuzzy import INTEGER_ID_RE, TIMESTAMP_RE


@pytest.mark.django_db
def test_network_rest_list(
    network_factory: NetworkFactory, owned_workspace: Workspace, authenticated_api_client: APIClient
):
    fake = Faker()
    network_names: List[str] = [
        network_factory(name=fake.pystr(), workspace=owned_workspace).name for _ in range(3)
    ]

    r = authenticated_api_client.get(f'/api/workspaces/{owned_workspace.name}/networks/')
    r_json = r.json()

    # Test that we get the expected results from both django and arango
    arango_db = owned_workspace.get_arango_db()
    assert r_json['count'] == len(network_names)
    for network in r_json['results']:
        assert network['name'] in network_names
        assert arango_db.has_graph(network['name'])


@pytest.mark.django_db
def test_network_rest_create(
    owned_workspace: Workspace,
    populated_edge_table: Table,
    populated_node_table: Table,
    authenticated_api_client: APIClient,
):
    network_name = 'network'
    r = authenticated_api_client.post(
        f'/api/workspaces/{owned_workspace.name}/networks/',
        {'name': network_name, 'edge_table': populated_edge_table.name},
        format='json',
    )

    assert r.json() == {
        'name': network_name,
        'node_count': len(populated_node_table.get_rows()),
        'edge_count': len(populated_edge_table.get_rows()),
        'id': INTEGER_ID_RE,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
        'workspace': {
            'id': owned_workspace.pk,
            'name': owned_workspace.name,
            'created': TIMESTAMP_RE,
            'modified': TIMESTAMP_RE,
            'arango_db_name': owned_workspace.arango_db_name,
        },
    }

    # Django will raise an exception if this fails, implicitly validating that the object exists
    network: Network = Network.objects.get(name=network_name)

    # Assert that object was created in arango
    assert owned_workspace.get_arango_db().has_graph(network.name)


@pytest.mark.django_db
def test_network_rest_retrieve(populated_network: Network, authenticated_api_client: APIClient):
    workspace = populated_network.workspace

    assert authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/networks/{populated_network.name}/'
    ).data == {
        'id': populated_network.pk,
        'name': populated_network.name,
        'node_count': populated_network.node_count,
        'edge_count': populated_network.edge_count,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
        'workspace': {
            'id': workspace.pk,
            'name': workspace.name,
            'created': TIMESTAMP_RE,
            'modified': TIMESTAMP_RE,
            'arango_db_name': workspace.arango_db_name,
        },
    }


@pytest.mark.django_db
def test_network_rest_delete(populated_network: Network, authenticated_api_client: APIClient):
    workspace: Workspace = populated_network.workspace

    r = authenticated_api_client.delete(
        f'/api/workspaces/{workspace.name}/networks/{populated_network.name}/'
    )

    assert r.status_code == 204

    # Assert relevant objects are deleted
    assert Network.objects.filter(name=workspace.name).first() is None
    assert not workspace.get_arango_db().has_graph(populated_network.name)


@pytest.mark.django_db
def test_network_rest_delete_unauthorized(populated_network: Network, api_client: APIClient):
    workspace: Workspace = populated_network.workspace

    r = api_client.delete(f'/api/workspaces/{workspace.name}/networks/{populated_network.name}/')

    assert r.status_code == 401

    # Assert relevant objects are not deleted
    assert Network.objects.filter(name=populated_network.name).first() is not None
    assert workspace.get_arango_db().has_graph(populated_network.name)


@pytest.mark.django_db
def test_network_rest_delete_forbidden(
    workspace_factory: WorkspaceFactory,
    network_factory: NetworkFactory,
    authenticated_api_client: APIClient,
):

    # Create workspace this way, so the authenticated user isn't an owner
    workspace: Workspace = workspace_factory()
    network: Table = network_factory(workspace=workspace)
    r = authenticated_api_client.delete(
        f'/api/workspaces/{workspace.name}/networks/{network.name}/'
    )

    assert r.status_code == 403

    # Assert relevant objects are not deleted
    assert Network.objects.filter(name=network.name).first() is not None
    assert workspace.get_arango_db().has_graph(network.name)


@pytest.mark.django_db
def test_network_rest_retrieve_nodes(
    populated_network: Network, authenticated_api_client: APIClient
):
    workspace: Workspace = populated_network.workspace
    nodes = list(populated_network.nodes())

    assert_limit_offset_results(
        authenticated_api_client,
        f'/api/workspaces/{workspace.name}/networks/{populated_network.name}/nodes/',
        nodes,
    )


@pytest.mark.django_db
def test_network_rest_retrieve_edges(
    populated_network: Network, authenticated_api_client: APIClient
):
    workspace: Workspace = populated_network.workspace
    edges = list(populated_network.edges())

    assert_limit_offset_results(
        authenticated_api_client,
        f'/api/workspaces/{workspace.name}/networks/{populated_network.name}/edges/',
        edges,
    )
