import json
from typing import Dict, List

from faker import Faker
import pytest
from rest_framework.test import APIClient

from multinet.api.models import Table, Workspace
from multinet.api.tests.factories import TableFactory, WorkspaceFactory

from .fuzzy import INTEGER_ID_RE, TIMESTAMP_RE, arango_doc_to_fuzzy_rev, dict_to_fuzzy_arango_doc
from .utils import assert_limit_offset_results, generate_arango_documents


@pytest.mark.django_db
def test_table_rest_list(
    table_factory: TableFactory, owned_workspace: Workspace, authenticated_api_client: APIClient
):
    fake = Faker()
    table_names: List[str] = [
        table_factory(name=fake.pystr(), workspace=owned_workspace).name for _ in range(3)
    ]

    r = authenticated_api_client.get(f'/api/workspaces/{owned_workspace.name}/tables/')
    r_json = r.json()

    # Test that we get the expected results from both django and arango
    arango_db = owned_workspace.get_arango_db()
    assert r_json['count'] == len(table_names)
    for table in r_json['results']:
        assert table['name'] in table_names
        assert arango_db.has_collection(table['name'])


@pytest.mark.django_db
def test_table_rest_list_public():
    """Test whether a user can see all tables on a public workspace."""
    assert 1 == 0


@pytest.mark.django_db
def test_table_rest_list_private():
    """Test to ensure the user cannot see tables on a private workspace."""
    assert 1 == 0


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
            'public': False,
        },
    }

    # Django will raise an exception if this fails, implicitly validating that the object exists
    table: Table = Table.objects.get(name=table_name)

    # Assert that object was created in arango
    assert owned_workspace.get_arango_db().has_collection(table.name)


@pytest.mark.django_db
def test_table_rest_create_forbidden():
    """
    Test that the user gets a 403 when trying to create a table on a workspace
    that they are not a writer on
    """
    assert 1 == 0


@pytest.mark.django_db
def test_table_rest_create_no_access():
    """
    Test that the user gets a 404 when trying to create a table on a workspace
    that they have no permission for
    """
    assert 1 == 0


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
def test_table_rest_retrieve_public():
    """Test that a user can see a specific table on a public workspace"""
    assert 1 == 0


@pytest.mark.django_db
def test_table_rest_retrieve_no_access():
    """Test that a user gets a 404 for trying to view a specific table on a workspace."""
    assert 1 == 0


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
    assert not Table.objects.filter(name=owned_workspace.name).exists()
    assert not owned_workspace.get_arango_db().has_collection(table.name)


@pytest.mark.django_db
def test_table_rest_delete_unauthorized(
    table_factory: TableFactory, owned_workspace: Workspace, api_client: APIClient
):
    table: Table = table_factory(workspace=owned_workspace)
    r = api_client.delete(f'/api/workspaces/{owned_workspace.name}/tables/{table.name}/')

    assert r.status_code == 401

    # Assert relevant objects are not deleted
    assert Table.objects.filter(name=table.name).exists()
    assert owned_workspace.get_arango_db().has_collection(table.name)


@pytest.mark.django_db
def test_table_rest_delete_forbidden(
    workspace_factory: WorkspaceFactory,
    table_factory: TableFactory,
    authenticated_api_client: APIClient,
):
    # Create workspace this way, so the authenticated user isn't an owner
    workspace: Workspace = workspace_factory()
    table: Table = table_factory(workspace=workspace)
    r = authenticated_api_client.delete(f'/api/workspaces/{workspace.name}/tables/{table.name}/')

    assert r.status_code == 403

    # Assert relevant objects are not deleted
    assert Table.objects.filter(name=table.name).exists()
    assert workspace.get_arango_db().has_collection(table.name)


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
def test_table_rest_retrieve_rows_filter_invalid(
    populated_node_table: Table, owned_workspace: Workspace, authenticated_api_client: APIClient
):
    """Test that the use of an invalid filter param returns the expected error."""
    r = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/tables/{populated_node_table.name}/rows/',
        {'filter': 'foobar'},  # Should be a JSON string, not 'foobar'
    )

    assert r.status_code == 400
    assert 'filter' in r.json()


@pytest.mark.django_db
def test_table_rest_retrieve_rows_filter_one(
    populated_node_table: Table, owned_workspace: Workspace, authenticated_api_client: APIClient
):
    table_rows: List[Dict] = list(populated_node_table.get_rows())
    filter_doc = dict(table_rows[0])
    filter_doc.pop('_key')
    filter_doc.pop('_id')
    filter_doc.pop('_rev')

    assert_limit_offset_results(
        authenticated_api_client,
        f'/api/workspaces/{owned_workspace.name}/tables/{populated_node_table.name}/rows/',
        result=[table_rows[0]],
        params={'filter': json.dumps(filter_doc)},
    )


@pytest.mark.django_db
def test_table_rest_retrieve_rows_filter_many(
    populated_node_table: Table, owned_workspace: Workspace, authenticated_api_client: APIClient
):
    # Create extra documents and insert common field
    docs = generate_arango_documents(5)
    for doc in docs:
        doc['extra_field'] = 'value'
    populated_node_table.put_rows(docs)
    docs = [doc for doc in populated_node_table.get_rows() if 'extra_field' in doc]

    filter_dict = {'extra_field': 'value'}
    assert_limit_offset_results(
        authenticated_api_client,
        f'/api/workspaces/{owned_workspace.name}/tables/{populated_node_table.name}/rows/',
        result=docs,
        params={'filter': json.dumps(filter_dict)},
    )


@pytest.mark.django_db
def test_table_rest_retrieve_rows_public():
    """User can see rows of a table on a public workspace"""
    assert 1 == 0


@pytest.mark.django_db
def test_table_rest_retrieve_rows_private():
    """User cannot see rows of a table on a private workspace without access"""
    assert 1 == 0


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
        'inserted': len(table_rows),
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

    # Assert all inserted, no errors
    assert r.status_code == 200
    assert r.json() == {
        'inserted': len(new_table_rows),
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

    # Create fuzzy payload for later assertions
    fuzzy_upsert_payload = [
        *map(dict_to_fuzzy_arango_doc, new_table_rows),
        *map(arango_doc_to_fuzzy_rev, partially_updated_table_rows),
    ]

    # Test combined row insert/update
    r = authenticated_api_client.put(
        f'/api/workspaces/{owned_workspace.name}/tables/{populated_node_table.name}/rows/',
        upsert_payload,
        format='json',
    )

    assert r.status_code == 200
    assert r.json() == {
        'inserted': len(upsert_payload),
        'errors': [],
    }

    # Test row population after
    r = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/tables/{populated_node_table.name}/rows/',
    )

    r_json = r.json()
    assert r_json['count'] == len(original_table_rows) + len(new_table_rows)
    assert all(
        row in original_table_rows or row in fuzzy_upsert_payload for row in r_json['results']
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
        'deleted': len(table_rows),
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


@pytest.mark.django_db
def test_table_rest_delete_rows_forbidden():
    """403 if a user tries to delete rows on a table they're not a writer for"""
    assert 1 == 0


@pytest.mark.django_db
def test_table_rest_delete_rows_no_acces():
    """404 if a user tries to delete rows on a table without any permission"""
    assert 1 == 0
