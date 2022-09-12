import pytest
from rest_framework.test import APIClient

from multinet.api.models.network import Network
from multinet.api.models.table import Table
from multinet.api.models.workspace import WorkspaceRoleChoice
from multinet.api.tasks.upload.csv import create_csv_network
from multinet.api.tests.fuzzy import dict_to_fuzzy_arango_doc
from multinet.api.views.serializers import CSVNetworkCreateSerializer


def new_table_name(network_name: str, table_name: str):
    return f'{network_name}--{table_name}'


@pytest.fixture
def csv_network_def(workspace, table_factory):
    # Edge table isn't defined as an actual edge table,
    # as it just contains the data that will be used in a new one
    edge_table: Table = table_factory(workspace=workspace)
    table1: Table = table_factory(workspace=workspace)
    table2: Table = table_factory(workspace=workspace)
    table3: Table = table_factory(workspace=workspace)

    # Create small network
    table1.put_rows([{'id': 1, 'foo': 'bar', 'test': 3}])
    table2.put_rows([{'id': 2, 'bar': 'baz'}])
    edge_table.put_rows([{'a': 1, 'b': 2, 'c': 3}])

    # Add extra data to join to table1 and edge_table
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
            'edge': {
                'table': {
                    'name': edge_table.name,
                    'excluded': ['c'],
                    'joined': {
                        'table': {
                            'name': table3.name,
                            'excluded': ['other'],
                        },
                        'link': {
                            'local': 'a',
                            'foreign': 'other',
                        },
                    },
                },
                'source': {
                    'local': 'a',
                    'foreign': 'id',
                },
                'target': {
                    'local': 'b',
                    'foreign': 'id',
                },
            },
            'source_table': {
                'name': table1.name,
                'excluded': ['test'],
                'joined': {
                    'table': {
                        'name': table3.name,
                        'excluded': [],
                    },
                    'link': {
                        'local': 'id',
                        'foreign': 'other',
                    },
                },
            },
            'target_table': {
                'name': table2.name,
                'excluded': [],
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
    edge_table = csv_network_def['edge_table']
    serializer = csv_network_def['serializer']
    table1, table2, table3 = csv_network_def['tables']

    create_csv_network(workspace, serializer)

    # Since new tables were created, need to replace current tables with those tables
    # (table3 isn't re-created)
    edge_table: Table = Table.objects.get(
        workspace=workspace, name=new_table_name(network_name, edge_table.name)
    )
    table1: Table = Table.objects.get(
        workspace=workspace, name=new_table_name(network_name, table1.name)
    )
    table2: Table = Table.objects.get(
        workspace=workspace, name=new_table_name(network_name, table2.name)
    )

    # Fetch stored rows
    table1_doc = table1.get_rows().next()
    table2_doc = table2.get_rows().next()
    table3_doc = table3.get_rows().next()

    # Assert node joining was performed correctly
    assert table1_doc['other'] == table3_doc['other']
    assert table1_doc['asd'] == table3_doc['asd'] == 'asd'
    assert table1_doc['zxc'] == table3_doc['zxc'] == 'zxc'

    # Assert column exclusion was performed correctly
    assert 'test' not in table1_doc

    # Assert edge linking, joining and exclusion was performed correctly
    edge_dict = {
        '_from': table1_doc['_id'],
        '_to': table2_doc['_id'],
        'a': 1,
        'b': 2,
        'asd': 'asd',
        'zxc': 'zxc',
    }
    assert edge_table.get_rows().next() == dict_to_fuzzy_arango_doc(edge_dict)

    # Assert network created correctly
    node_tables = sorted([table1.name, table2.name])
    network: Network = Network.objects.get(workspace=workspace, name=network_name)
    assert network.edges().next() == dict_to_fuzzy_arango_doc(edge_dict)
    assert network.get_arango_graph().edge_definitions()[0] == {
        'edge_collection': edge_table.name,
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


@pytest.mark.django_db
def test_create_csv_network_missing_joins(workspace, table_factory):
    edge_table: Table = table_factory(workspace=workspace)
    table1: Table = table_factory(workspace=workspace)
    table2: Table = table_factory(workspace=workspace)

    # Create small network
    table1.put_rows(
        [
            {'id': 1, 'foo': 'bar'},
            # This wont match to anything
            {'id': 8, 'foo': 'baz'},
        ]
    )
    table2.put_rows(
        [
            {'id': 2, 'bar': 'baz'},
            # This wont match to anything
            {'id': 40, 'bar': 'bat'},
        ]
    )
    edge_table.put_rows(
        [
            {'a': 1, 'b': 2, 'c': 3},
            # This wont match to anything
            {'a': 100, 'b': 101, 'c': 3},
        ]
    )

    serializer = CSVNetworkCreateSerializer(
        data={
            'name': 'test',
            'edge': {
                'table': {
                    'name': edge_table.name,
                    'excluded': ['c'],
                },
                'source': {
                    'local': 'a',
                    'foreign': 'id',
                },
                'target': {
                    'local': 'b',
                    'foreign': 'id',
                },
            },
            'source_table': {
                'name': table1.name,
                'excluded': ['test'],
            },
            'target_table': {
                'name': table2.name,
                'excluded': [],
            },
        }
    )
    serializer.is_valid(raise_exception=True)

    # Ensure no exceptions raised
    create_csv_network(workspace, serializer)
