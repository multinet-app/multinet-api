from typing import List

from faker import Faker
import pytest
from rest_framework.test import APIClient
from django.contrib.auth.models import User

from multinet.api.models import Table, Workspace, WorkspaceRole
from multinet.api.tests.factories import TableFactory

from .conftest import populated_table
from .fuzzy import INTEGER_ID_RE, TIMESTAMP_RE, arango_doc_to_fuzzy_rev, dict_to_fuzzy_arango_doc
from .utils import assert_limit_offset_results, generate_arango_documents


@pytest.mark.django_db
def test_table_rest_list(
    table_factory: TableFactory,
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
):
    workspace.set_user_permission(user, WorkspaceRole.READER)
    fake = Faker()
    table_names: List[str] = [
        table_factory(name=fake.pystr(), workspace=workspace).name for _ in range(3)
    ]

    r = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/tables/')
    r_json = r.json()

    # Test that we get the expected results from both django and arango
    arango_db = workspace.get_arango_db()
    assert r_json['count'] == len(table_names)
    for table in r_json['results']:
        assert table['name'] in table_names
        assert arango_db.has_collection(table['name'])


@pytest.mark.django_db
def test_table_rest_list_public(
    table_factory: TableFactory, public_workspace: Workspace, authenticated_api_client: APIClient
):
    """Test whether a user can see all tables on a public workspace."""
    fake = Faker()
    table_names: List[str] = [
        table_factory(name=fake.pystr(), workspace=public_workspace).name for _ in range(3)
    ]
    r = authenticated_api_client.get(f'/api/workspaces/{public_workspace.name}/tables/')
    r_json = r.json()

    # Test that we get the expected results from both django and arango
    arango_db = public_workspace.get_arango_db()
    assert r_json['count'] == len(table_names)
    for table in r_json['results']:
        assert table['name'] in table_names
        assert arango_db.has_collection(table['name'])


@pytest.mark.django_db
def test_table_rest_list_private(
    table_factory: TableFactory, workspace: Workspace, authenticated_api_client: APIClient
):
    """Test that a user cannot see tables on private workspaces with no access."""
    for _ in range(3):
        table_factory(workspace=workspace)

    r = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/tables/')
    assert r.status_code == 404


@pytest.mark.django_db
@pytest.mark.parametrize('edge', [True, False])
def test_table_rest_create(
    workspace: Workspace, user: User, authenticated_api_client: APIClient, edge: bool
):
    workspace.set_user_permission(user, WorkspaceRole.WRITER)
    table_name = Faker().pystr()
    r = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/tables/',
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
            'id': workspace.pk,
            'name': workspace.name,
            'created': TIMESTAMP_RE,
            'modified': TIMESTAMP_RE,
            'arango_db_name': workspace.arango_db_name,
            'public': False,
        },
    }

    # Django will raise an exception if this fails, implicitly validating that the object exists
    table: Table = Table.objects.get(name=table_name)

    # Assert that object was created in arango
    assert workspace.get_arango_db().has_collection(table.name)


@pytest.mark.django_db
@pytest.mark.parametrize('edge', [True, False])
def test_table_rest_create_forbidden(
    workspace: Workspace, user: User, authenticated_api_client: APIClient, edge: bool
):
    """
    Test that the user gets a 403 when trying to create a table on a workspace
    that they are not a writer on
    """
    workspace.set_user_permission(user, WorkspaceRole.READER)
    table_name = Faker().pystr()
    r = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/tables/',
        {'name': table_name, 'edge': edge},
        format='json',
    )
    assert r.status_code == 403


@pytest.mark.django_db
@pytest.mark.parametrize('edge', [True, False])
def test_table_rest_create_no_access(
    workspace: Workspace, authenticated_api_client: APIClient, edge: bool
):
    """
    Test that the user gets a 404 when trying to create a table on a workspace
    that they have no permission for
    """
    table_name = Faker().pystr()
    r = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/tables/',
        {'name': table_name, 'edge': edge},
        format='json',
    )
    assert r.status_code == 404


@pytest.mark.django_db
def test_table_rest_retrieve(
    workspace: Workspace,
    table_factory: TableFactory,
    user: User,
    authenticated_api_client: APIClient,
):
    workspace.set_user_permission(user, WorkspaceRole.READER)
    table: Table = table_factory(workspace=workspace)

    assert authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/tables/{table.name}/'
    ).data == {
        'id': table.pk,
        'name': table.name,
        'edge': table.edge,
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


@pytest.mark.django_db
def test_table_rest_retrieve_public(
    public_workspace: Workspace, table_factory: TableFactory, authenticated_api_client: APIClient
):
    """Test that a user can see a specific table on a public workspace"""
    table = table_factory(workspace=public_workspace)

    assert authenticated_api_client.get(
        f'/api/workspaces/{public_workspace.name}/tables/{table.name}/'
    ).data == {
        'id': table.pk,
        'name': table.name,
        'edge': table.edge,
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
def test_table_rest_retrieve_no_access(
    workspace: Workspace, table_factory: TableFactory, authenticated_api_client: APIClient
):
    """Test that a user gets a 404 for trying to view a specific table on a workspace."""
    table = table_factory(workspace=workspace)
    r = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/tables/{table.name}/')
    assert r.status_code == 404


@pytest.mark.django_db
def test_table_rest_delete(
    table_factory: TableFactory,
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
):
    workspace.set_user_permission(user, WorkspaceRole.WRITER)
    table: Table = table_factory(workspace=workspace)

    r = authenticated_api_client.delete(f'/api/workspaces/{workspace.name}/tables/{table.name}/')

    assert r.status_code == 204

    # Assert relevant objects are deleted
    assert not Table.objects.filter(name=workspace.name).exists()
    assert not workspace.get_arango_db().has_collection(table.name)


@pytest.mark.django_db
def test_table_rest_delete_unauthorized(
    table_factory: TableFactory, workspace: Workspace, user: User, api_client: APIClient
):
    workspace.set_user_permission(user, WorkspaceRole.WRITER)
    table: Table = table_factory(workspace=workspace)
    r = api_client.delete(f'/api/workspaces/{workspace.name}/tables/{table.name}/')

    assert r.status_code == 401

    # Assert relevant objects are not deleted
    assert Table.objects.filter(name=table.name).exists()
    assert workspace.get_arango_db().has_collection(table.name)


@pytest.mark.django_db
def test_table_rest_delete_forbidden(
    workspace: Workspace,
    user: User,
    table_factory: TableFactory,
    authenticated_api_client: APIClient,
):
    # Create workspace this way, so the authenticated user doesn't have writer permission
    workspace.set_user_permission(user, WorkspaceRole.READER)
    table: Table = table_factory(workspace=workspace)
    r = authenticated_api_client.delete(f'/api/workspaces/{workspace.name}/tables/{table.name}/')

    assert r.status_code == 403

    # Assert relevant objects are not deleted
    assert Table.objects.filter(name=table.name).exists()
    assert workspace.get_arango_db().has_collection(table.name)


@pytest.mark.django_db
def test_table_rest_delete_no_access(
    workspace: Workspace,
    table_factory: TableFactory,
    authenticated_api_client,
):
    table: Table = table_factory(workspace=workspace)
    r = authenticated_api_client.delete(f'/api/workspaces/{workspace.name}/tables/{table.name}/')

    assert r.status_code == 404
    assert Table.objects.filter(name=table.name).exists()
    assert workspace.get_arango_db().has_collection(table.name)


@pytest.mark.django_db
def test_table_rest_retrieve_rows(
    workspace: Workspace, user: User, authenticated_api_client: APIClient
):
    workspace.set_user_permission(user, WorkspaceRole.READER)
    node_table = populated_table(workspace, False)
    table_rows = list(node_table.get_rows())
    assert_limit_offset_results(
        authenticated_api_client,
        f'/api/workspaces/{workspace.name}/tables/{node_table.name}/rows/',
        table_rows,
    )


@pytest.mark.django_db
def test_table_rest_retrieve_rows_public(
    public_workspace: Workspace, authenticated_api_client: APIClient
):
    """User can see rows of a table on a public workspace"""
    node_table = populated_table(public_workspace, False)
    table_rows = list(node_table.get_rows())

    assert_limit_offset_results(
        authenticated_api_client,
        f'/api/workspaces/{public_workspace.name}/tables/{node_table.name}/rows/',
        table_rows,
    )


@pytest.mark.django_db
def test_table_rest_retrieve_rows_private(
    workspace: Workspace, authenticated_api_client: APIClient
):
    """User cannot see rows of a table on a private workspace without access"""
    node_table = populated_table(workspace, False)
    r = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/tables/{node_table.name}/rows/',
        {'limit': 0, 'offset': 0},
    )

    assert r.status_code == 404


@pytest.mark.django_db
def test_table_rest_insert_rows(
    table_factory: TableFactory,
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
):
    workspace.set_user_permission(user, WorkspaceRole.WRITER)
    table: Table = table_factory(workspace=workspace)

    table_rows = generate_arango_documents(5)
    inserted_table_rows = list(map(dict_to_fuzzy_arango_doc, table_rows))

    # Test insert response
    r = authenticated_api_client.put(
        f'/api/workspaces/{workspace.name}/tables/{table.name}/rows/',
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
        f'/api/workspaces/{workspace.name}/tables/{table.name}/rows/',
    )

    assert r.status_code == 200
    assert r.json() == {
        'count': len(table_rows),
        'next': None,
        'previous': None,
        'results': inserted_table_rows,
    }


@pytest.mark.django_db
def test_table_rest_insert_rows_forbidden(
    table_factory: TableFactory,
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
):
    workspace.set_user_permission(user, WorkspaceRole.READER)
    table: Table = table_factory(workspace=workspace)
    table_rows = generate_arango_documents(5)

    r = authenticated_api_client.put(
        f'/api/workspaces/{workspace.name}/tables/{table.name}/rows/',
        table_rows,
        format='json',
    )

    assert r.status_code == 403


@pytest.mark.django_db
def test_table_rest_insert_rows_no_access(
    table_factory: TableFactory, workspace: Workspace, authenticated_api_client: APIClient
):
    table: Table = table_factory(workspace=workspace)
    table_rows = generate_arango_documents(5)

    r = authenticated_api_client.put(
        f'/api/workspaces/{workspace.name}/tables/{table.name}/rows/',
        table_rows,
        format='json',
    )

    assert r.status_code == 404


@pytest.mark.django_db
def test_table_rest_update_rows(
    populated_node_table: Table,
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
):
    workspace.set_user_permission(user, WorkspaceRole.WRITER)
    # Assert table contents beforehand
    original_table_rows = list(populated_node_table.get_rows())
    new_table_rows = [{**d, 'extra': 'field'} for d in original_table_rows]
    inserted_new_table_rows = list(map(dict_to_fuzzy_arango_doc, new_table_rows))

    # Assert row update succeeded
    r = authenticated_api_client.put(
        f'/api/workspaces/{workspace.name}/tables/{populated_node_table.name}/rows/',
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
        f'/api/workspaces/{workspace.name}/tables/{populated_node_table.name}/rows/',
    )

    r_json = r.json()
    assert r_json['count'] == len(original_table_rows)

    for i, row in enumerate(r_json['results']):
        assert row == inserted_new_table_rows[i]
        assert row['_id'] == original_table_rows[i]['_id']


@pytest.mark.django_db
def test_table_rest_update_rows_no_access(
    workspace: Workspace, authenticated_api_client: APIClient
):
    node_table = populated_table(workspace, False)
    original_table_rows = list(node_table.get_rows())
    new_table_rows = [{**d, 'extra': 'field'} for d in original_table_rows]

    r = authenticated_api_client.put(
        f'/api/workspaces/{workspace.name}/tables/{node_table.name}/rows/',
        new_table_rows,
        format='json',
    )
    assert r.status_code == 404


@pytest.mark.django_db
def test_table_rest_update_rows_forbidden(
    workspace: Workspace, user: User, authenticated_api_client: APIClient
):
    workspace.set_user_permission(user, WorkspaceRole.READER)
    node_table = populated_table(workspace, False)
    original_table_rows = list(node_table.get_rows())
    new_table_rows = [{**d, 'extra': 'field'} for d in original_table_rows]

    r = authenticated_api_client.put(
        f'/api/workspaces/{workspace.name}/tables/{node_table.name}/rows/',
        new_table_rows,
        format='json',
    )
    assert r.status_code == 403


@pytest.mark.django_db
def test_table_rest_upsert_rows(
    populated_node_table: Table,
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
):
    workspace.set_user_permission(user, WorkspaceRole.WRITER)
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
        f'/api/workspaces/{workspace.name}/tables/{populated_node_table.name}/rows/',
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
        f'/api/workspaces/{workspace.name}/tables/{populated_node_table.name}/rows/',
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
    populated_node_table: Table,
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
):
    workspace.set_user_permission(user, WorkspaceRole.WRITER)
    table_rows = list(populated_node_table.get_rows())
    r = authenticated_api_client.delete(
        f'/api/workspaces/{workspace.name}/tables/{populated_node_table.name}/rows/',
        table_rows,
        format='json',
    )

    assert r.status_code == 200
    assert r.json() == {
        'deleted': table_rows,
        'errors': [],
    }

    r = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/tables/{populated_node_table.name}/rows/'
    )

    assert r.status_code == 200
    assert r.json() == {
        'count': 0,
        'next': None,
        'previous': None,
        'results': [],
    }


@pytest.mark.django_db
def test_table_rest_delete_rows_forbidden(
    workspace: Workspace, user: User, authenticated_api_client: APIClient
):
    """403 if a user tries to delete rows on a table they're not a writer for"""
    workspace.set_user_permission(user, WorkspaceRole.READER)
    node_table = populated_table(workspace, False)
    table_rows = list(node_table.get_rows())
    r = authenticated_api_client.delete(
        f'/api/workspaces/{workspace.name}/tables/{node_table.name}/rows/',
        table_rows,
        format='json',
    )
    assert r.status_code == 403


@pytest.mark.django_db
def test_table_rest_delete_rows_no_access(
    workspace: Workspace, authenticated_api_client: APIClient
):
    """404 if a user tries to delete rows on a table without any permission"""
    node_table = populated_table(workspace, False)
    table_rows = list(node_table.get_rows())
    r = authenticated_api_client.delete(
        f'/api/workspaces/{workspace.name}/tables/{node_table.name}/rows/',
        table_rows,
        format='json',
    )
    assert r.status_code == 404
