import pytest

from multinet.api.models.network import Network
from multinet.api.models.table import Table
from multinet.api.tasks.upload.csv import create_csv_network
from multinet.api.tests.fuzzy import dict_to_fuzzy_arango_doc
from multinet.api.views.serializers import CSVNetworkCreateSerializer


@pytest.mark.django_db
def test_create_csv_network(workspace, table_factory):
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

    create_csv_network(workspace, serializer)

    # Fetch stored rows
    table1_doc = table1.get_rows().next()
    table2_doc = table2.get_rows().next()
    # table3_doc = table3.get_rows().next()

    # Assert edge link was performed correctly
    new_edge_table_name = f'{network_name}_edges'
    new_edge_table: Table = Table.objects.get(workspace=workspace, name=new_edge_table_name)
    assert new_edge_table.get_rows().next() == dict_to_fuzzy_arango_doc(
        {
            '_from': table1_doc['_id'],
            '_to': table2_doc['_id'],
            'a': 1,
            'b': 2,
        }
    )

    # Assert network created correctly
    network: Network = Network.objects.get(workspace=workspace, name=network_name)
    node_tables = sorted([table1.name, table2.name])
    assert network.get_arango_graph().edge_definitions()[0] == {
        'edge_collection': new_edge_table_name,
        'from_vertex_collections': node_tables,
        'to_vertex_collections': node_tables,
    }

    # TODO: Assert node joining was performed correctly
