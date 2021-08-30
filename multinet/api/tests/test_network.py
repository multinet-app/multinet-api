from typing import List

from django.contrib.auth.models import User
from faker import Faker
import pytest
from rest_framework.test import APIClient

from multinet.api.models import Network, Table, Workspace, WorkspaceRoleChoice
from multinet.api.tests.factories import NetworkFactory, PublicWorkspaceFactory
from multinet.api.tests.utils import assert_limit_offset_results

from .conftest import populated_network, populated_table
from .fuzzy import INTEGER_ID_RE, TIMESTAMP_RE


@pytest.mark.django_db
@pytest.mark.parametrize(
    'permission,is_owner,status_code,success',
    [
        (None, False, 404, False),
        (WorkspaceRoleChoice.READER, False, 200, True),
        (WorkspaceRoleChoice.WRITER, False, 200, True),
        (WorkspaceRoleChoice.MAINTAINER, False, 200, True),
        (None, True, 200, True),
    ],
)
def test_network_rest_list(
    network_factory: NetworkFactory,
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)
    fake = Faker()
    network_names: List[str] = [
        network_factory(name=fake.pystr(), workspace=workspace).name for _ in range(3)
    ]

    r = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/networks/')
    assert r.status_code == status_code

    if success:
        r_json = r.json()

        # Test that we get the expected results from both django and arango
        arango_db = workspace.get_arango_db()
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
    """Test that an unauthenticated user can see networks on a public workspace."""
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
@pytest.mark.parametrize(
    'permission,is_owner,status_code,success',
    [
        (None, False, 404, False),
        (WorkspaceRoleChoice.READER, False, 403, False),
        (WorkspaceRoleChoice.WRITER, False, 200, True),
        (WorkspaceRoleChoice.MAINTAINER, False, 200, True),
        (None, True, 200, True),
    ],
)
def test_network_rest_create(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)

    edge_table = populated_table(workspace, True)
    node_table_name = list(edge_table.find_referenced_node_tables().keys())[0]
    node_table = Table.objects.get(name=node_table_name)
    network_name = 'network'

    r = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/networks/',
        {'name': network_name, 'edge_table': edge_table.name},
        format='json',
    )
    assert r.status_code == status_code

    if success:
        assert r.json() == {
            'name': network_name,
            'node_count': len(node_table.get_rows()),
            'edge_count': len(edge_table.get_rows()),
            'id': INTEGER_ID_RE,
            'created': TIMESTAMP_RE,
            'modified': TIMESTAMP_RE,
            'workspace': {
                'id': workspace.pk,
                'name': workspace.name,
                'created': TIMESTAMP_RE,
                'modified': TIMESTAMP_RE,
                'arango_db_name': workspace.arango_db_name,
                'public': False,
            },
        }

        # Django will raise an exception if this fails, implicitly validating that the object exists
        network: Network = Network.objects.get(name=network_name)
        # Assert that object was created in arango
        assert workspace.get_arango_db().has_graph(network.name)
    else:
        assert not Network.objects.filter(name=network_name).exists()
        assert not workspace.get_arango_db().has_graph(network_name)


@pytest.mark.django_db
@pytest.mark.parametrize(
    'permission,is_owner,status_code,success',
    [
        (None, False, 404, False),
        (WorkspaceRoleChoice.READER, False, 200, True),
        (WorkspaceRoleChoice.WRITER, False, 200, True),
        (WorkspaceRoleChoice.MAINTAINER, False, 200, True),
        (None, True, 200, True),
    ],
)
def test_network_rest_retrieve(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)
    network = populated_network(workspace)

    r = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/networks/{network.name}/')
    assert r.status_code == status_code

    if success:
        assert r.data == {
            'id': network.pk,
            'name': network.name,
            'node_count': network.node_count,
            'edge_count': network.edge_count,
            'created': TIMESTAMP_RE,
            'modified': TIMESTAMP_RE,
            'workspace': {
                'id': workspace.pk,
                'name': workspace.name,
                'created': TIMESTAMP_RE,
                'modified': TIMESTAMP_RE,
                'arango_db_name': workspace.arango_db_name,
                'public': False,
            },
        }
    else:
        assert r.data == {'detail': 'Not found.'}


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
@pytest.mark.parametrize(
    'permission,is_owner,status_code,success',
    [
        (None, False, 404, False),
        (WorkspaceRoleChoice.READER, False, 403, False),
        (WorkspaceRoleChoice.WRITER, False, 204, True),
        (WorkspaceRoleChoice.MAINTAINER, False, 204, True),
        (None, True, 204, True),
    ],
)
def test_network_rest_delete(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    """Tests deleting a network on a workspace for which the user is at least a writer."""
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)
    network = populated_network(workspace)
    r = authenticated_api_client.delete(
        f'/api/workspaces/{workspace.name}/networks/{network.name}/'
    )

    assert r.status_code == status_code

    if success:
        # Assert relevant objects are deleted
        assert not Network.objects.filter(name=network.name).exists()
        assert not workspace.get_arango_db().has_graph(network.name)
    else:
        # Assert objects are not deleted
        assert Network.objects.filter(name=network.name).exists()
        assert workspace.get_arango_db().has_graph(network.name)


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
@pytest.mark.parametrize(
    'permission,is_owner,success',
    [
        (None, False, False),
        (WorkspaceRoleChoice.READER, False, True),
        (WorkspaceRoleChoice.WRITER, False, True),
        (WorkspaceRoleChoice.MAINTAINER, False, True),
        (None, True, True),
    ],
)
def test_network_rest_retrieve_nodes(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)
    network = populated_network(workspace)
    nodes = list(network.nodes())

    if success:
        assert_limit_offset_results(
            authenticated_api_client,
            f'/api/workspaces/{workspace.name}/networks/{network.name}/nodes/',
            nodes,
        )
    else:
        r = authenticated_api_client.get(
            f'/api/workspaces/{workspace.name}/networks/{network.name}/nodes/',
            {'limit': 0, 'offset': 0},
        )
        assert r.status_code == 404


@pytest.mark.django_db
def test_network_rest_retrieve_nodes_public(public_workspace: Workspace, api_client: APIClient):
    network = populated_network(public_workspace)
    nodes = list(network.nodes())

    assert_limit_offset_results(
        api_client,
        f'/api/workspaces/{public_workspace.name}/networks/{network.name}/nodes/',
        nodes,
    )


@pytest.mark.django_db
@pytest.mark.parametrize(
    'permission,is_owner,success',
    [
        (None, False, False),
        (WorkspaceRoleChoice.READER, False, True),
        (WorkspaceRoleChoice.WRITER, False, True),
        (WorkspaceRoleChoice.MAINTAINER, False, True),
        (None, True, True),
    ],
)
def test_network_rest_retrieve_edges(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)
    network = populated_network(workspace)
    edges = list(network.edges())

    if success:
        assert_limit_offset_results(
            authenticated_api_client,
            f'/api/workspaces/{workspace.name}/networks/{network.name}/edges/',
            edges,
        )
    else:
        r = authenticated_api_client.get(
            f'/api/workspaces/{workspace.name}/networks/{network.name}/edges/',
            {'limit': 0, 'offset': 0},
        )
        assert r.status_code == 404


@pytest.mark.django_db
def test_network_rest_retrieve_edges_public(public_workspace: Workspace, api_client: APIClient):
    network = populated_network(public_workspace)
    edges = list(network.edges())

    assert_limit_offset_results(
        api_client,
        f'/api/workspaces/{public_workspace.name}/networks/{network.name}/edges/',
        edges,
    )


@pytest.mark.django_db
@pytest.mark.parametrize(
    'permission,is_owner,success',
    [
        (None, False, False),
        (WorkspaceRoleChoice.READER, False, True),
        (WorkspaceRoleChoice.WRITER, False, True),
        (WorkspaceRoleChoice.MAINTAINER, False, True),
        (None, True, True),
    ],
)
def test_network_rest_retrieve_tables_all(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)

    edge_table = populated_table(workspace, edge=True)
    network = populated_network(workspace, edge_table=edge_table)
    node_tables = list(edge_table.find_referenced_node_tables().keys())
    table_names = {*node_tables, edge_table.name}

    response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/networks/{network.name}/tables/'
    )

    if success:
        assert response.status_code == 200
        assert table_names == {table['name'] for table in response.data}
    else:
        assert response.status_code == 404


@pytest.mark.django_db
@pytest.mark.parametrize('type', ['node', 'edge'])
def test_network_rest_retrieve_tables_type(
    workspace: Workspace, user: User, authenticated_api_client: APIClient, type: str
):
    workspace.set_user_permission(user, WorkspaceRoleChoice.READER)

    edge_table = populated_table(workspace, edge=True)
    network = populated_network(workspace, edge_table=edge_table)
    node_tables = list(edge_table.find_referenced_node_tables().keys())

    response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/networks/{network.name}/tables/', data={'type': type}
    )
    assert response.status_code == 200

    if type == 'node':
        assert len(response.data) == len(node_tables)
        for table in response.data:
            assert not table['edge']
            assert table['name'] in node_tables
    else:  # type = 'edge'
        assert len(response.data) == 1  # test network created with one edge definition
        for table in response.data:
            assert table['edge']
            assert table['name'] == edge_table.name
