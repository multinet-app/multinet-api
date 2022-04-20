import pytest
from rest_framework.test import APIClient

from multinet.api.models.network import Network
from multinet.api.models.table import Table
from multinet.api.models.workspace import WorkspaceRoleChoice
from multinet.api.tasks.upload.csv import create_csv_network
from multinet.api.tests.fuzzy import dict_to_fuzzy_arango_doc
from multinet.api.views.serializers import CSVNetworkCreateSerializer


@pytest.fixture
def csv_network_def(workspace, table_factory):
    # Edge table isn't defined as an actual edge table,
    # as it just contains the data that will be used in a new one
    edge_table: Table = table_factory(workspace=workspace)
    table1: Table = table_factory(workspace=workspace)
    table2: Table = table_factory(workspace=workspace)
    table3: Table = table_factory(workspace=workspace)

    # Create small network
    table1.put_rows([{'id': 1, 'foo': 'bar'}])
    table2.put_rows([{'id': 2, 'bar': 'baz'}])
    edge_table.put_rows([{'a': 1, 'b': 2}])

    # Add extra data to join to table1
    table3.put_rows([{'other': 1, 'asd': 'asd', 'zxc': 'zxc'}])

    # Assert data is present
    assert edge_table.count() == 1
    assert table1.count() == 1
    assert table2.count() == 1
    assert table3.count() == 1

    # Create network definition
    network_name = 'test'
    serializer = CSVNetworkCreateSerializer(
        data={
            'name': network_name,
            'edge_table': {
                'name': edge_table.name,
                'source': {
                    'column': 'a',
                    'foreign_column': {
                        'table': table1.name,
                        'column': 'id',
                    },
                },
                'target': {
                    'column': 'b',
                    'foreign_column': {
                        'table': table2.name,
                        'column': 'id',
                    },
                },
            },
            'joins': {
                table3.name: {
                    'column': 'other',
                    'foreign_column': {
                        'table': table1.name,
                        'column': 'id',
                    },
                }
            },
        }
    )
    serializer.is_valid(raise_exception=True)

    return {
        'edge_table': edge_table,
        'tables': [table1, table2, table3],
        'name': network_name,
        'serializer': serializer,
    }


@pytest.mark.django_db
def test_create_csv_network(workspace, csv_network_def):
    network_name = csv_network_def['name']
    serializer = csv_network_def['serializer']
    table1, table2, table3 = csv_network_def['tables']

    create_csv_network(workspace, serializer)

    # Fetch stored rows
    table1_doc = table1.get_rows().next()
    table2_doc = table2.get_rows().next()
    table3_doc = table3.get_rows().next()

    # Assert node joining was performed correctly
    joined_table: Table = Table.objects.get(name=f'{table1}-joined-{table3}')
    joined_table_doc = joined_table.get_rows().next()
    assert joined_table_doc['other'] == table3_doc['other'] == table1_doc['id']
    assert joined_table_doc['asd'] == table3_doc['asd'] == 'asd'
    assert joined_table_doc['zxc'] == table3_doc['zxc'] == 'zxc'

    # Assert edge link was performed correctly
    edge_dict = {
        '_from': joined_table_doc['_id'],
        '_to': table2_doc['_id'],
        'a': 1,
        'b': 2,
    }
    new_edge_table_name = f'{network_name}_edges'
    new_edge_table: Table = Table.objects.get(workspace=workspace, name=new_edge_table_name)
    assert new_edge_table.get_rows().next() == dict_to_fuzzy_arango_doc(edge_dict)

    # Assert network created correctly
    node_tables = sorted([joined_table.name, table2.name])
    network: Network = Network.objects.get(workspace=workspace, name=network_name)
    assert network.edges().next() == dict_to_fuzzy_arango_doc(edge_dict)
    assert network.get_arango_graph().edge_definitions()[0] == {
        'edge_collection': new_edge_table_name,
        'from_vertex_collections': node_tables,
        'to_vertex_collections': node_tables,
    }


@pytest.mark.django_db
def test_rest_create_csv_network(
    workspace,
    user,
    csv_network_def,
    authenticated_api_client: APIClient,
):
    workspace.set_user_permission(user, WorkspaceRoleChoice.WRITER)

    network_name = csv_network_def['name']
    serializer = csv_network_def['serializer']
    r = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/networks/from_tables/',
        serializer.validated_data,
    )
    assert r.status_code == 200
    assert r.json()['name'] == network_name
    assert r.json()['edge_count'] == 1
    assert r.json()['node_count'] == 2

    # Assert network created
    Network.objects.get(workspace=workspace, name=network_name)


@pytest.mark.django_db
def test_rest_create_csv_network_already_exists(
    workspace,
    user,
    network_factory,
    csv_network_def,
    authenticated_api_client: APIClient,
):
    workspace.set_user_permission(user, WorkspaceRoleChoice.WRITER)

    network_factory(workspace=workspace, name=csv_network_def['name'])
    r = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/networks/from_tables/',
        csv_network_def['serializer'].validated_data,
    )
    assert r.status_code == 400
    assert r.json() == 'Network already exists'
