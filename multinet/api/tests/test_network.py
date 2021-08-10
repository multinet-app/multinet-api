from typing import List

from django.contrib.auth.models import User
from faker import Faker
import pytest
from rest_framework.test import APIClient

from multinet.api.models import Network, Table, Workspace, WorkspaceRoleChoice
from multinet.api.tests.factories import (
    NetworkFactory,
    PrivateWorkspaceFactory,
    PublicWorkspaceFactory,
)
from multinet.api.tests.utils import assert_limit_offset_results

from .conftest import populated_network, populated_table
from .fuzzy import INTEGER_ID_RE, TIMESTAMP_RE
from .utils import ALL_ROLES, AT_LEAST_WRITER


@pytest.mark.django_db
@pytest.mark.parametrize('permission', ALL_ROLES)
def test_network_rest_list(
    network_factory: NetworkFactory,
    unowned_workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
):
    unowned_workspace.set_user_permission(user, permission)
    fake = Faker()
    network_names: List[str] = [
        network_factory(name=fake.pystr(), workspace=unowned_workspace).name for _ in range(3)
    ]

    r = authenticated_api_client.get(f'/api/workspaces/{unowned_workspace.name}/networks/')
    r_json = r.json()

    # Test that we get the expected results from both django and arango
    arango_db = unowned_workspace.get_arango_db()
    assert r_json['count'] == len(network_names)
    for network in r_json['results']:
        assert network['name'] in network_names
        assert arango_db.has_graph(network['name'])


@pytest.mark.django_db
def test_network_rest_list_public(
    network_factory: NetworkFactory,
    public_workspace_factory: PublicWorkspaceFactory,
    api_client: APIClient,
):
    """Test that an authenticated user can see networks on a public workspace."""
    fake = Faker()
    public_workspace: Workspace = public_workspace_factory()
    network_names: List[str] = [
        network_factory(name=fake.pystr(), workspace=public_workspace).name for _ in range(3)
    ]
    r = api_client.get(f'/api/workspaces/{public_workspace.name}/networks/')
    r_json = r.json()

    arango_db = public_workspace.get_arango_db()
    assert r_json['count'] == len(network_names)
    for network in r_json['results']:
        assert network['name'] in network_names
        assert arango_db.has_graph(network['name'])


@pytest.mark.django_db
def test_network_rest_list_private(
    network_factory: NetworkFactory,
    private_workspace_factory: PrivateWorkspaceFactory,
    authenticated_api_client: APIClient,
):
    """Test that an authenticated user can not see networks on a private workspace."""
    fake = Faker()
    private_workspace: Workspace = private_workspace_factory()
    for _ in range(3):
        network_factory(name=fake.pystr(), workspace=private_workspace)

    r = authenticated_api_client.get(f'/api/workspaces/{private_workspace.name}/networks/')
    assert r.status_code == 404


@pytest.mark.django_db
@pytest.mark.parametrize(
    'permission',
    AT_LEAST_WRITER,
)
def test_network_rest_create(
    unowned_workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
):
    unowned_workspace.set_user_permission(user, permission)

    edge_table = populated_table(unowned_workspace, True)
    node_table_name = list(edge_table.find_referenced_node_tables().keys())[0]
    node_table = Table.objects.get(name=node_table_name)
    network_name = 'network'

    r = authenticated_api_client.post(
        f'/api/workspaces/{unowned_workspace.name}/networks/',
        {'name': network_name, 'edge_table': edge_table.name},
        format='json',
    )

    assert r.json() == {
        'name': network_name,
        'node_count': len(node_table.get_rows()),
        'edge_count': len(edge_table.get_rows()),
        'id': INTEGER_ID_RE,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
        'workspace': {
            'id': unowned_workspace.pk,
            'name': unowned_workspace.name,
            'created': TIMESTAMP_RE,
            'modified': TIMESTAMP_RE,
            'arango_db_name': unowned_workspace.arango_db_name,
            'public': False,
        },
    }

    # Django will raise an exception if this fails, implicitly validating that the object exists
    network: Network = Network.objects.get(name=network_name)

    # Assert that object was created in arango
    assert unowned_workspace.get_arango_db().has_graph(network.name)


@pytest.mark.django_db
def test_network_rest_create_forbidden(
    unowned_workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
):
    unowned_workspace.set_user_permission(user, WorkspaceRoleChoice.READER)
    edge_table = populated_table(unowned_workspace, True)
    network_name = 'network'
    r = authenticated_api_client.post(
        f'/api/workspaces/{unowned_workspace.name}/networks/',
        {'name': network_name, 'edge_table': edge_table.name},
        format='json',
    )
    assert r.status_code == 403


@pytest.mark.django_db
def test_network_rest_create_no_access(
    unowned_workspace: Workspace,
    authenticated_api_client: APIClient,
):
    network_name = 'network'
    edge_table = populated_table(unowned_workspace, True)
    r = authenticated_api_client.post(
        f'/api/workspaces/{unowned_workspace.name}/networks/',
        {'name': network_name, 'edge_table': edge_table.name},
        format='json',
    )
    assert r.status_code == 404


@pytest.mark.django_db
@pytest.mark.parametrize('permission', ALL_ROLES)
def test_network_rest_retrieve(
    unowned_workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
):
    unowned_workspace.set_user_permission(user, permission)
    network = populated_network(unowned_workspace)

    assert authenticated_api_client.get(
        f'/api/workspaces/{unowned_workspace.name}/networks/{network.name}/'
    ).data == {
        'id': network.pk,
        'name': network.name,
        'node_count': network.node_count,
        'edge_count': network.edge_count,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
        'workspace': {
            'id': unowned_workspace.pk,
            'name': unowned_workspace.name,
            'created': TIMESTAMP_RE,
            'modified': TIMESTAMP_RE,
            'arango_db_name': unowned_workspace.arango_db_name,
            'public': False,
        },
    }


@pytest.mark.django_db
def test_network_rest_retrieve_public(public_workspace: Workspace, api_client: APIClient):
    network = populated_network(public_workspace)
    assert api_client.get(
        f'/api/workspaces/{public_workspace.name}/networks/{network.name}/'
    ).data == {
        'id': network.pk,
        'name': network.name,
        'node_count': network.node_count,
        'edge_count': network.edge_count,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
        'workspace': {
            'id': public_workspace.pk,
            'name': public_workspace.name,
            'created': TIMESTAMP_RE,
            'modified': TIMESTAMP_RE,
            'arango_db_name': public_workspace.arango_db_name,
            'public': True,
        },
    }


@pytest.mark.django_db
def test_network_rest_retrieve_no_access(
    unowned_workspace: Workspace, authenticated_api_client: APIClient
):
    network = populated_network(unowned_workspace)
    r = authenticated_api_client.get(
        f'/api/workspaces/{unowned_workspace.name}/networks/{network.name}/'
    )
    assert r.status_code == 404


@pytest.mark.django_db
@pytest.mark.parametrize(
    'permission',
    AT_LEAST_WRITER,
)
def test_network_rest_delete(
    unowned_workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
):
    """Tests deleting a network on a unowned_workspace for which the user is at least a writer."""
    unowned_workspace.set_user_permission(user, permission)
    network = populated_network(unowned_workspace)

    r = authenticated_api_client.delete(
        f'/api/workspaces/{unowned_workspace.name}/networks/{network.name}/'
    )

    assert r.status_code == 204

    # Assert relevant objects are deleted
    assert not Network.objects.filter(name=unowned_workspace.name).exists()
    assert not unowned_workspace.get_arango_db().has_graph(network.name)


@pytest.mark.django_db
def test_network_rest_delete_owned(
    workspace: Workspace,
    authenticated_api_client: APIClient,
):
    """Tests deleting a network on a unowned_workspace for which the user is at least a writer."""
    network = populated_network(workspace)

    r = authenticated_api_client.delete(
        f'/api/workspaces/{workspace.name}/networks/{network.name}/'
    )

    assert r.status_code == 204

    # Assert relevant objects are deleted
    assert not Network.objects.filter(name=workspace.name).exists()
    assert not workspace.get_arango_db().has_graph(network.name)


@pytest.mark.django_db
def test_network_rest_delete_unauthorized(workspace: Workspace, api_client: APIClient):
    """Tests deleting a network from a workspace with an unauthorized request."""
    network = populated_network(workspace)

    r = api_client.delete(f'/api/workspaces/{workspace.name}/networks/{network.name}/')

    assert r.status_code == 401

    # Assert relevant objects are not deleted
    assert Network.objects.filter(name=network.name).exists()
    assert workspace.get_arango_db().has_graph(network.name)


@pytest.mark.django_db
def test_network_rest_delete_forbidden(
    unowned_workspace: Workspace,
    user: User,
    network_factory: NetworkFactory,
    authenticated_api_client: APIClient,
):
    """Tests deleting a network on a workspace without sufficient permissions."""
    unowned_workspace.set_user_permission(user, WorkspaceRoleChoice.READER)
    network: Table = network_factory(workspace=unowned_workspace)
    r = authenticated_api_client.delete(
        f'/api/workspaces/{unowned_workspace.name}/networks/{network.name}/'
    )

    assert r.status_code == 403

    # Assert relevant objects are not deleted
    assert Network.objects.filter(name=network.name).exists()
    assert unowned_workspace.get_arango_db().has_graph(network.name)


@pytest.mark.django_db
def test_network_rest_delete_no_access(
    unowned_workspace: Workspace, authenticated_api_client: APIClient
):
    """Test deleting a network from a workspace for which the user has no access at all."""
    network = populated_network(unowned_workspace)
    r = authenticated_api_client.delete(
        f'/api/workspaces/{unowned_workspace.name}/networks/{network.name}/'
    )
    assert r.status_code == 404

    # Assert relevant objects are not deleted
    assert Network.objects.filter(name=network.name).exists()
    assert unowned_workspace.get_arango_db().has_graph(network.name)


@pytest.mark.django_db
@pytest.mark.parametrize('permission', ALL_ROLES)
def test_network_rest_retrieve_nodes(
    unowned_workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
):
    unowned_workspace.set_user_permission(user, permission)
    network = populated_network(unowned_workspace)
    nodes = list(network.nodes())

    assert_limit_offset_results(
        authenticated_api_client,
        f'/api/workspaces/{unowned_workspace.name}/networks/{network.name}/nodes/',
        nodes,
    )


@pytest.mark.django_db
def test_network_rest_retrieve_nodes_public(
    public_workspace: Workspace, authenticated_api_client: APIClient
):
    network = populated_network(public_workspace)
    nodes = list(network.nodes())

    assert_limit_offset_results(
        authenticated_api_client,
        f'/api/workspaces/{public_workspace.name}/networks/{network.name}/nodes/',
        nodes,
    )


@pytest.mark.django_db
def test_network_rest_retrieve_nodes_no_access(
    unowned_workspace: Workspace, authenticated_api_client: APIClient
):
    network = populated_network(unowned_workspace)
    r = authenticated_api_client.get(
        f'/api/workspaces/{unowned_workspace.name}/networks/{network.name}/nodes/',
        {'limit': 0, 'offset': 0},
    )
    assert r.status_code == 404


@pytest.mark.django_db
@pytest.mark.parametrize('permission', ALL_ROLES)
def test_network_rest_retrieve_edges(
    unowned_workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
):
    unowned_workspace.set_user_permission(user, permission)
    network = populated_network(unowned_workspace)
    edges = list(network.edges())

    assert_limit_offset_results(
        authenticated_api_client,
        f'/api/workspaces/{unowned_workspace.name}/networks/{network.name}/edges/',
        edges,
    )


@pytest.mark.django_db
def test_network_rest_retrieve_edges_public(
    public_workspace: Workspace, authenticated_api_client: APIClient
):
    network = populated_network(public_workspace)
    edges = list(network.edges())

    assert_limit_offset_results(
        authenticated_api_client,
        f'/api/workspaces/{public_workspace.name}/networks/{network.name}/edges/',
        edges,
    )


@pytest.mark.django_db
def test_network_rest_retrieve_edges_no_access(
    unowned_workspace: Workspace, authenticated_api_client: APIClient
):
    network = populated_network(unowned_workspace)
    r = authenticated_api_client.get(
        f'/api/workspaces/{unowned_workspace.name}/networks/{network.name}/edges/',
        {'limit': 0, 'offset': 0},
    )
    assert r.status_code == 404
