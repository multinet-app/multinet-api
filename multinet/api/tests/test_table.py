from faker import Faker
import pytest
from rest_framework.test import APIClient

from multinet.api.models import Table, Workspace
from multinet.api.tests.factories import TableFactory

from .fuzzy import INTEGER_ID_RE, TIMESTAMP_RE, arango_doc_to_fuzzy_rev, dict_to_fuzzy_arango_doc
from .utils import assert_limit_offset_results, generate_arango_documents


@pytest.mark.django_db
@pytest.mark.parametrize('edge', [True, False])
def test_table_rest_create(
    owned_workspace: Workspace, authenticated_api_client: APIClient, edge: bool
):
    table_name = Faker().pystr()
    r = authenticated_api_client.post(
        f'/api/workspaces/{owned_workspace.name}/tables/',
        {'name': table_name, 'edge': edge},
        format='json',
    )
    r_json = r.json()

    assert r_json == {
        'name': table_name,
        'edge': edge,
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
    table: Table = Table.objects.get(name=table_name)

    # Assert that object was created in arango
    assert owned_workspace.get_arango_db().has_collection(table.name)


@pytest.mark.django_db
def test_table_rest_retrieve(owned_workspace: Workspace, authenticated_api_client: APIClient):
    assert authenticated_api_client.get(f'/api/workspaces/{owned_workspace.name}/').data == {
        'id': owned_workspace.pk,
        'name': owned_workspace.name,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
        'arango_db_name': owned_workspace.arango_db_name,
    }


@pytest.mark.django_db
def test_table_rest_delete(
    table_factory: TableFactory, owned_workspace: Workspace, authenticated_api_client: APIClient
):
    table: Table = table_factory(workspace=owned_workspace)

    r = authenticated_api_client.delete(
        f'/api/workspaces/{owned_workspace.name}/tables/{table.name}/'
    )

    assert r.status_code == 204

    # Assert relevant objects are deleted
    assert Table.objects.filter(name=owned_workspace.name).first() is None
    assert not owned_workspace.get_arango_db().has_collection(table.name)


@pytest.mark.django_db
def test_table_rest_retrieve_rows(
    populated_node_table: Table, owned_workspace: Workspace, authenticated_api_client: APIClient
):
    table_rows = list(populated_node_table.get_rows())
    assert_limit_offset_results(
        authenticated_api_client,
        f'/api/workspaces/{owned_workspace.name}/tables/{populated_node_table.name}/rows/',
        table_rows,
    )


@pytest.mark.django_db
def test_table_rest_insert_rows(
    table_factory: TableFactory, owned_workspace: Workspace, authenticated_api_client: APIClient
):
    table: Table = table_factory(workspace=owned_workspace)

    table_rows = generate_arango_documents(5)
    inserted_table_rows = list(map(dict_to_fuzzy_arango_doc, table_rows))

    # Test insert response
    r = authenticated_api_client.put(
        f'/api/workspaces/{owned_workspace.name}/tables/{table.name}/rows/',
        table_rows,
        format='json',
    )

    assert r.status_code == 200
    assert r.json() == {
        'inserted': inserted_table_rows,
        'errors': [],
    }

    # Test that rows are populated after
    r = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/tables/{table.name}/rows/',
    )

    assert r.status_code == 200
    assert r.json() == {
        'count': len(table_rows),
        'next': None,
        'previous': None,
        'results': inserted_table_rows,
    }


@pytest.mark.django_db
def test_table_rest_update_rows(
    populated_node_table: Table, owned_workspace: Workspace, authenticated_api_client: APIClient
):

    # Assert table contents beforehand
    original_table_rows = list(populated_node_table.get_rows())
    new_table_rows = [{**d, 'extra': 'field'} for d in original_table_rows]
    inserted_new_table_rows = list(map(dict_to_fuzzy_arango_doc, new_table_rows))

    # Assert row update succeeded
    r = authenticated_api_client.put(
        f'/api/workspaces/{owned_workspace.name}/tables/{populated_node_table.name}/rows/',
        new_table_rows,
        format='json',
    )

    assert r.status_code == 200
    assert r.json() == {
        'inserted': inserted_new_table_rows,
        'errors': [],
    }

    # Assert only existing rows were updated, and no new ones were added
    r = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/tables/{populated_node_table.name}/rows/',
    )

    r_json = r.json()
    assert r_json['count'] == len(original_table_rows)

    for i, row in enumerate(r_json['results']):
        assert row == inserted_new_table_rows[i]
        assert row['_id'] == original_table_rows[i]['_id']


@pytest.mark.django_db
def test_table_rest_upsert_rows(
    populated_node_table: Table, owned_workspace: Workspace, authenticated_api_client: APIClient
):
    # Create row lists
    original_table_rows = list(populated_node_table.get_rows())
    new_table_rows = generate_arango_documents(3)
    partially_updated_table_rows = [{**d, 'extra': 'field'} for d in original_table_rows[:2]]
    upsert_payload = [*partially_updated_table_rows, *new_table_rows]

    # Create fuzzy row lists
    fuzzy_new_table_rows = list(map(dict_to_fuzzy_arango_doc, new_table_rows))
    fuzzy_partially_updated_table_rows = list(
        map(arango_doc_to_fuzzy_rev, partially_updated_table_rows)
    )
    fuzzy_upsert_payload = [*fuzzy_partially_updated_table_rows, *fuzzy_new_table_rows]

    # Test combined row insert/update
    r = authenticated_api_client.put(
        f'/api/workspaces/{owned_workspace.name}/tables/{populated_node_table.name}/rows/',
        upsert_payload,
        format='json',
    )

    assert r.status_code == 200
    assert r.json() == {
        'inserted': fuzzy_upsert_payload,
        'errors': [],
    }

    # Test row population after
    r = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/tables/{populated_node_table.name}/rows/',
    )

    r_json = r.json()
    assert r_json['count'] == len(original_table_rows) + len(new_table_rows)
    for row in r_json['results']:
        assert (
            row in original_table_rows
            or row in fuzzy_partially_updated_table_rows
            or row in fuzzy_new_table_rows
        )


@pytest.mark.django_db
def test_table_rest_delete_rows(
    populated_node_table: Table, owned_workspace: Workspace, authenticated_api_client: APIClient
):

    table_rows = list(populated_node_table.get_rows())
    r = authenticated_api_client.delete(
        f'/api/workspaces/{owned_workspace.name}/tables/{populated_node_table.name}/rows/',
        table_rows,
        format='json',
    )

    assert r.status_code == 200
    assert r.json() == {
        'deleted': table_rows,
        'errors': [],
    }

    r = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/tables/{populated_node_table.name}/rows/'
    )

    assert r.status_code == 200
    assert r.json() == {
        'count': 0,
        'next': None,
        'previous': None,
        'results': [],
    }
