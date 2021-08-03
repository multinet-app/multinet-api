from typing import List

from faker import Faker
import pytest
from rest_framework.test import APIClient
from django.contrib.auth.models import User

from multinet.api.models import Workspace
from multinet.api.tests.factories import (
    PrivateWorkspaceFactory,
    PublicWorkspaceFactory,
)
from multinet.api.utils.arango import arango_system_db

from .fuzzy import TIMESTAMP_RE


@pytest.mark.django_db
def test_workspace_arango_sync(workspace: Workspace):
    assert arango_system_db().has_database(workspace.arango_db_name)


@pytest.mark.django_db
def test_workspace_rest_list(
    public_workspace_factory: PublicWorkspaceFactory,
    private_workspace_factory: PrivateWorkspaceFactory,
    user: User,
    authenticated_api_client: APIClient,
):
    """
    Test that a user can retrieve a list of workspaces that either are public or
    the user has permission to read.
    """
    fake = Faker()
    accessible_workspace_names: List[str] = [
        public_workspace_factory(name=fake.pystr()).name for _ in range(3)
    ]
    private_workspaces: List[Workspace] = [
        private_workspace_factory(name=fake.pystr()) for _ in range(3)
    ]

    private_workspaces[0].set_readers([user])
    private_workspaces[1].set_owner(user)
    accessible_workspace_names += [private_workspaces[0].name, private_workspaces[1].name]

    r = authenticated_api_client.get('/api/workspaces/')
    r_json = r.json()

    # Test that we get the expected results from both django and arango
    sysdb = arango_system_db()
    assert r_json['count'] == len(accessible_workspace_names)
    for workspace in r_json['results']:
        assert workspace['name'] in accessible_workspace_names
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
def test_workspace_rest_retrieve_owned(
    workspace: Workspace, user: User, authenticated_api_client: APIClient
):
    workspace.set_owner(user)
    assert authenticated_api_client.get(f'/api/workspaces/{workspace.name}/').data == {
        'id': workspace.pk,
        'name': workspace.name,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
        'arango_db_name': workspace.arango_db_name,
        'public': False,
    }


@pytest.mark.django_db
def test_workspace_rest_retrieve_public(
    public_workspace_factory: PublicWorkspaceFactory, authenticated_api_client: APIClient
):
    fake = Faker()
    public_workspace: Workspace = public_workspace_factory(name=fake.pystr())
    assert authenticated_api_client.get(f'/api/workspaces/{public_workspace.name}/').data == {
        'id': public_workspace.pk,
        'name': public_workspace.name,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
        'arango_db_name': public_workspace.arango_db_name,
        'public': True,
    }


@pytest.mark.django_db
def test_workspace_rest_retrieve_no_access(
    workspace: Workspace, authenticated_api_client: APIClient
):
    response = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/')

    # a user should not be able to know about a workspace they can't access
    assert response.status_code == 404


@pytest.mark.django_db
def test_workspace_rest_delete(
    workspace: Workspace, user: User, authenticated_api_client: APIClient
):
    workspace.set_owner(user)
    r = authenticated_api_client.delete(f'/api/workspaces/{workspace.name}/')

    assert r.status_code == 204

    # Assert relevant objects are deleted
    assert not Workspace.objects.filter(name=workspace.name).exists()
    assert not arango_system_db().has_database(workspace.arango_db_name)


@pytest.mark.django_db
def test_workspace_rest_delete_unauthorized(
    workspace: Workspace, user: User, api_client: APIClient
):
    workspace.set_owner(user)

    r = api_client.delete(f'/api/workspaces/{workspace.name}/')

    assert r.status_code == 401

    # Assert relevant objects are not deleted
    assert Workspace.objects.filter(name=workspace.name).exists()
    assert arango_system_db().has_database(workspace.arango_db_name)


@pytest.mark.django_db
def test_workspace_rest_delete_forbidden(
    workspace: Workspace, user: User, authenticated_api_client: APIClient
):
    workspace.set_readers([user])
    response = authenticated_api_client.delete(f'/api/workspaces/{workspace.name}/')
    assert response.status_code == 403

    # Assert relevant objects are not deleted
    assert Workspace.objects.filter(name=workspace.name).exists()
    assert arango_system_db().has_database(workspace.arango_db_name)


@pytest.mark.django_db
def test_workspace_rest_delete_no_access(
    private_workspace_factory: PrivateWorkspaceFactory, authenticated_api_client: APIClient
):
    # Create workspace this way, so the authenticated user has no access
    workspace: Workspace = private_workspace_factory()
    r = authenticated_api_client.delete(f'/api/workspaces/{workspace.name}/')

    # Without any access, don't reveal that this workspace exists at all
    assert r.status_code == 404

    # Assert relevant objects are not deleted
    assert Workspace.objects.filter(name=workspace.name).exists()
    assert arango_system_db().has_database(workspace.arango_db_name)
