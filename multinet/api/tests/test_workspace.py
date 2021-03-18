from django.contrib.auth.models import User
from faker import Faker
from guardian.shortcuts import assign_perm
import pytest
from rest_framework.test import APIClient

from multinet.api.models import Workspace
from multinet.api.utils.arango import arango_system_db


@pytest.mark.django_db
def test_workspace_arango_sync(workspace: Workspace):
    assert arango_system_db().has_database(workspace.arango_db_name)


@pytest.mark.django_db
def test_workspace_rest_create(authenticated_api_client: APIClient):
    fake = Faker()
    workspace_name = fake.first_name()

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
        'created': workspace.created.isoformat().replace('+00:00', 'Z'),
        'modified': workspace.modified.isoformat().replace('+00:00', 'Z'),
        'arango_db_name': workspace.arango_db_name,
    }


@pytest.mark.django_db
def test_workspace_rest_delete(
    user: User, workspace: Workspace, authenticated_api_client: APIClient
):
    # Ensure owner perms
    assign_perm('owner', user, workspace)

    r = authenticated_api_client.delete(f'/api/workspaces/{workspace.name}/')

    assert r.status_code == 204

    # Assert relevant objects are deleted
    assert Workspace.objects.filter(name=workspace.name).first() is None
    assert not arango_system_db().has_database(workspace.arango_db_name)
