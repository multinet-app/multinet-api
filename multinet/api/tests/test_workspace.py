from typing import Dict, List

from django.contrib.auth.models import User
from faker import Faker
import pytest
from rest_framework.test import APIClient

from multinet.api.models import Workspace, WorkspaceRoleChoice
from multinet.api.tests.factories import (
    PrivateWorkspaceFactory,
    PublicWorkspaceFactory,
    UserFactory,
)
from multinet.api.tests.utils import create_users_with_permissions
from multinet.api.utils.arango import arango_system_db

from .fuzzy import TIMESTAMP_RE, workspace_re


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
    """Test list endpoint for workspaces."""
    fake = Faker()
    accessible_workspace_names: List[str] = [
        public_workspace_factory(name=fake.pystr()).name for _ in range(3)
    ]
    private_workspaces: List[Workspace] = [
        private_workspace_factory(name=fake.pystr()) for _ in range(3)
    ]

    private_workspaces[0].set_user_permission(user, WorkspaceRoleChoice.READER)
    private_workspaces[1].set_user_permission(user, WorkspaceRoleChoice.READER)
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
def test_workspace_rest_list_no_duplicates(
    workspace: Workspace,
    user_factory: UserFactory,
    user: User,
    authenticated_api_client: APIClient,
):
    """Test that multiple roles on a workspace results in no duplicates."""
    # Set authenticated user as owner
    workspace.set_owner(user)

    # Give multiple users permissions on the workspace
    workspace.set_user_permissions_bulk(readers=[user_factory() for _ in range(5)])

    # Test that there's only one copy of this workspace returned
    r = authenticated_api_client.get('/api/workspaces/')
    assert r.json() == {
        'count': 1,
        'next': None,
        'previous': None,
        'results': [workspace_re(workspace)],
    }


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
    public_workspace_factory: PublicWorkspaceFactory, api_client: APIClient
):
    fake = Faker()
    public_workspace: Workspace = public_workspace_factory(name=fake.pystr())
    assert api_client.get(f'/api/workspaces/{public_workspace.name}/').data == {
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
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
):
    workspace.set_user_permission(user, WorkspaceRoleChoice.READER)
    response = authenticated_api_client.delete(f'/api/workspaces/{workspace.name}/')
    assert response.status_code == 403

    # Assert relevant objects are not deleted
    assert Workspace.objects.filter(name=workspace.name).exists()
    assert arango_system_db().has_database(workspace.arango_db_name)


@pytest.mark.django_db
def test_workspace_rest_delete_no_access(
    private_workspace_factory: PrivateWorkspaceFactory,
    authenticated_api_client: APIClient,
):
    # Create workspace this way, so the authenticated user has no access
    workspace: Workspace = private_workspace_factory()
    r = authenticated_api_client.delete(f'/api/workspaces/{workspace.name}/')

    # Without any access, don't reveal that this workspace exists at all
    assert r.status_code == 404

    # Assert relevant objects are not deleted
    assert Workspace.objects.filter(name=workspace.name).exists()
    assert arango_system_db().has_database(workspace.arango_db_name)


@pytest.mark.django_db
def test_workspace_rest_get_permissions(
    workspace: Workspace,
    user: User,
    user_factory: UserFactory,
    authenticated_api_client: APIClient,
):
    workspace.set_user_permission(user, WorkspaceRoleChoice.MAINTAINER)
    workspace.set_owner(user_factory())
    create_users_with_permissions(user_factory, workspace)
    maintainer_names = [maintainer.username for maintainer in workspace.maintainers]
    writer_names = [writer.username for writer in workspace.writers]
    reader_names = [reader.username for reader in workspace.readers]

    r = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/permissions/')
    r_json = r.json()

    assert r.status_code == 200
    assert r_json['public'] == workspace.public
    assert r_json['owner']['username'] == workspace.owner.username

    assert all(maintainer['username'] in maintainer_names for maintainer in r_json['maintainers'])
    assert all(writer['username'] in writer_names for writer in r_json['writers'])
    assert all(reader['username'] in reader_names for reader in r_json['readers'])


@pytest.mark.django_db
def test_workspace_rest_get_permissions_owner(
    workspace: Workspace,
    user: User,
    user_factory: UserFactory,
    authenticated_api_client: APIClient,
):
    workspace.set_owner(user)
    create_users_with_permissions(user_factory, workspace)
    maintainer_names = [maintainer.username for maintainer in workspace.maintainers]
    writer_names = [writer.username for writer in workspace.writers]
    reader_names = [reader.username for reader in workspace.readers]

    r = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/permissions/')
    r_json = r.json()

    assert r.status_code == 200
    assert r_json['public'] == workspace.public
    assert r_json['owner']['username'] == workspace.owner.username

    assert all(maintainer['username'] in maintainer_names for maintainer in r_json['maintainers'])
    assert all(writer['username'] in writer_names for writer in r_json['writers'])
    assert all(reader['username'] in reader_names for reader in r_json['readers'])


@pytest.mark.django_db
@pytest.mark.parametrize('permission', [WorkspaceRoleChoice.READER, WorkspaceRoleChoice.WRITER])
def test_workspace_rest_get_permissions_forbidden(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
):
    workspace.set_user_permission(user, permission)
    r = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/permissions/')
    assert r.status_code == 403


@pytest.mark.django_db
def test_workspace_rest_get_permissions_no_access(
    workspace: Workspace, authenticated_api_client: APIClient
):
    r = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/permissions/')
    assert r.status_code == 404


@pytest.mark.django_db
def test_workspace_rest_put_permissions_owner(
    workspace: Workspace,
    user: User,
    user_factory: UserFactory,
    authenticated_api_client: APIClient,
):
    workspace.set_owner(user)
    new_owner = user_factory()
    new_maintainers: List[Dict] = [{'username': user_factory().username} for _ in range(2)]
    new_writers: List[Dict] = [{'username': user_factory().username} for _ in range(2)]
    new_readers: List[Dict] = [{'username': user_factory().username} for _ in range(2)]
    request_data = {
        'public': True,
        'owner': {'username': new_owner.username},
        'maintainers': new_maintainers,
        'writers': new_writers,
        'readers': new_readers,
    }
    r = authenticated_api_client.put(
        f'/api/workspaces/{workspace.name}/permissions/', request_data, format='json'
    )
    workspace = Workspace.objects.get(id=workspace.pk)

    assert r.status_code == 200
    assert workspace.public == request_data['public']
    assert workspace.owner == new_owner

    readers_names = [reader['username'] for reader in new_readers]
    writers_names = [writer['username'] for writer in new_writers]
    maintainers_names = [maintainer['username'] for maintainer in new_maintainers]
    assert all([reader.username in readers_names for reader in workspace.readers])
    assert all([writer.username in writers_names for writer in workspace.writers])
    assert all([maintainer.username in maintainers_names for maintainer in workspace.maintainers])


@pytest.mark.django_db
def test_workspace_rest_put_permissions_maintainer(
    workspace: Workspace,
    user: User,
    user_factory: UserFactory,
    authenticated_api_client: APIClient,
):
    workspace.set_user_permission(user, WorkspaceRoleChoice.MAINTAINER)
    old_owner = workspace.owner
    new_owner = user_factory()
    new_maintainers: List[Dict] = [{'username': user_factory().username} for _ in range(2)]
    new_writers: List[Dict] = [{'username': user_factory().username} for _ in range(2)]
    new_readers: List[Dict] = [{'username': user_factory().username} for _ in range(2)]
    request_data = {
        'public': True,
        'owner': {'username': new_owner.username},
        'maintainers': new_maintainers,
        'writers': new_writers,
        'readers': new_readers,
    }
    r = authenticated_api_client.put(
        f'/api/workspaces/{workspace.name}/permissions/', request_data, format='json'
    )
    r_json = r.json()
    assert r.status_code == 200
    assert r_json['public'] == request_data['public']
    # maintainers cannot set the owner of a workspace
    assert workspace.owner == old_owner

    readers_names = [reader['username'] for reader in new_readers]
    writers_names = [writer['username'] for writer in new_writers]
    maintainers_names = [maintainer['username'] for maintainer in new_maintainers]
    assert all([reader.username in readers_names for reader in workspace.readers])
    assert all([writer.username in writers_names for writer in workspace.writers])
    assert all([maintainer.username in maintainers_names for maintainer in workspace.maintainers])


@pytest.mark.django_db
@pytest.mark.parametrize('permission', [WorkspaceRoleChoice.READER, WorkspaceRoleChoice.WRITER])
def test_workspace_rest_put_permissions_forbidden(
    workspace: Workspace,
    user: User,
    user_factory: UserFactory,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
):
    workspace.set_user_permission(user, permission)
    new_owner = user_factory()
    new_maintainers: List[Dict] = [{'username': user_factory().username} for _ in range(2)]
    public = not workspace.public
    request_data = {
        'public': public,
        'owner': {'username': new_owner.username},
        'maintainers': new_maintainers,
        'writers': [],
        'readers': [],
    }
    r = authenticated_api_client.put(
        f'/api/workspaces/{workspace.name}/permissions/', request_data, format='json'
    )

    assert r.status_code == 403
    assert workspace.owner != new_owner
    assert len(list(workspace.maintainers)) == 0


@pytest.mark.django_db
def test_workspace_rest_put_permissions_no_access(
    workspace: Workspace, user: User, authenticated_api_client: APIClient
):
    request_data = {
        'public': not workspace.public,
        'owner': {'username': user.username},
        'maintainers': [],
        'writers': [],
        'readers': [],
    }
    r = authenticated_api_client.put(
        f'/api/workspaces/{workspace.name}/permissions/', request_data, format='json'
    )
    workspace = Workspace.objects.get(id=workspace.pk)
    assert r.status_code == 404
    assert workspace.public != request_data['public']
    assert workspace.owner != user
