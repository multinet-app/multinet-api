from typing import List

from faker import Faker
import pytest
from rest_framework.test import APIClient

from multinet.api.models import Workspace
from multinet.api.tests.factories import WorkspaceFactory
from multinet.api.utils.arango import arango_system_db

from .fuzzy import TIMESTAMP_RE


@pytest.mark.django_db
def test_workspace_arango_sync(workspace: Workspace):
    assert arango_system_db().has_database(workspace.arango_db_name)


@pytest.mark.django_db
def test_workspace_rest_list(
    workspace_factory: WorkspaceFactory,
    authenticated_api_client: APIClient,
):
    fake = Faker()
    workspace_names: List[str] = [workspace_factory(name=fake.pystr()).name for _ in range(3)]

    r = authenticated_api_client.get('/api/workspaces/')
    r_json = r.json()

    # Test that we get the expected results from both django and arango
    sysdb = arango_system_db()
    assert r_json['count'] == len(workspace_names)
    for workspace in r_json['results']:
        assert workspace['name'] in workspace_names
        assert sysdb.has_database(workspace['arango_db_name'])


@pytest.mark.django_db
def test_workspace_rest_create(authenticated_api_client: APIClient):
    fake = Faker()
    workspace_name = fake.pystr()

    r = authenticated_api_client.post('/api/workspaces/', {'name': workspace_name}, format='json')
    r_json = r.json()

    assert r_json['name'] == workspace_name
    assert arango_system_db().has_database(r_json['arango_db_name'])

    # Django will raise an exception if this fails
    Workspace.objects.get(name=workspace_name)


@pytest.mark.django_db
def test_workspace_rest_retrieve(workspace: Workspace, authenticated_api_client: APIClient):
    assert authenticated_api_client.get(f'/api/workspaces/{workspace.name}/').data == {
        'id': workspace.pk,
        'name': workspace.name,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
        'arango_db_name': workspace.arango_db_name,
    }


@pytest.mark.django_db
def test_workspace_rest_delete(owned_workspace: Workspace, authenticated_api_client: APIClient):
    r = authenticated_api_client.delete(f'/api/workspaces/{owned_workspace.name}/')

    assert r.status_code == 204

    # Assert relevant objects are deleted
    assert Workspace.objects.filter(name=owned_workspace.name).first() is None
    assert not arango_system_db().has_database(owned_workspace.arango_db_name)


@pytest.mark.django_db
def test_workspace_rest_delete_unauthorized(owned_workspace: Workspace, api_client: APIClient):
    r = api_client.delete(f'/api/workspaces/{owned_workspace.name}/')

    assert r.status_code == 401

    # Assert relevant objects are not deleted
    assert Workspace.objects.filter(name=owned_workspace.name).first() is not None
    assert arango_system_db().has_database(owned_workspace.arango_db_name)


@pytest.mark.django_db
def test_workspace_rest_delete_forbidden(
    workspace_factory: WorkspaceFactory, authenticated_api_client: APIClient
):
    # Create workspace this way, so the authenticated user isn't an owner
    workspace: Workspace = workspace_factory()
    r = authenticated_api_client.delete(f'/api/workspaces/{workspace.name}/')

    assert r.status_code == 403

    # Assert relevant objects are not deleted
    assert Workspace.objects.filter(name=workspace.name).first() is not None
    assert arango_system_db().has_database(workspace.arango_db_name)
