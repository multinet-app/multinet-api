from faker import Faker
import pytest
from rest_framework.test import APIClient

from multinet.api.models import Table, Workspace
from multinet.api.tests.factories import TableFactory

from .fuzzy import INTEGER_ID_RE, TIMESTAMP_RE, dict_to_fuzzy_arango_doc


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
    table_factory: TableFactory, owned_workspace: Workspace, authenticated_api_client: APIClient
):
    table: Table = table_factory(workspace=owned_workspace)

    table_rows = [{'foo': 'bar'}, {'foo2': 'bar2'}]
    inserted_table_rows = list(map(dict_to_fuzzy_arango_doc, table_rows))

    table.put_rows(table_rows)
    r = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/tables/{table.name}/rows/'
    )

    assert r.status_code == 200
    assert r.json() == {
        'count': 2,
        'next': None,
        'previous': None,
        'results': inserted_table_rows,
    }


@pytest.mark.django_db
def test_table_rest_upsert_rows(
    table_factory: TableFactory, owned_workspace: Workspace, authenticated_api_client: APIClient
):
    table: Table = table_factory(workspace=owned_workspace)

    # Test that rows are empty beforehand
    r = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/tables/{table.name}/rows/'
    )

    assert r.status_code == 200
    assert r.json() == {
        'count': 0,
        'next': None,
        'previous': None,
        'results': [],
    }

    table_rows = [{'foo': 'bar'}, {'foo2': 'bar2'}]
    inserted_table_rows = list(map(dict_to_fuzzy_arango_doc, table_rows))

    # Test that rows are populated after
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


@pytest.mark.django_db
def test_table_rest_delete_rows(
    table_factory: TableFactory, owned_workspace: Workspace, authenticated_api_client: APIClient
):
    table: Table = table_factory(workspace=owned_workspace)
    table_rows = [{'foo': 'bar'}, {'foo2': 'bar2'}]

    table.put_rows(table_rows)
    inserted_table_rows = list(table.get_rows())

    r = authenticated_api_client.delete(
        f'/api/workspaces/{owned_workspace.name}/tables/{table.name}/rows/',
        inserted_table_rows,
        format='json',
    )

    assert r.status_code == 200
    assert r.json() == {
        'deleted': inserted_table_rows,
        'errors': [],
    }

    r = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/tables/{table.name}/rows/'
    )

    assert r.status_code == 200
    assert r.json() == {
        'count': 0,
        'next': None,
        'previous': None,
        'results': [],
    }
