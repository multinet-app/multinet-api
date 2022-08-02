from typing import Dict, List

from arango.cursor import Cursor
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

from .conftest import populated_table
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
        private_workspace_factory(name=fake.pystr()) for _ in range(5)
    ]

    private_workspaces[0].set_user_permission(user, WorkspaceRoleChoice.READER)
    private_workspaces[1].set_user_permission(user, WorkspaceRoleChoice.WRITER)
    private_workspaces[2].set_user_permission(user, WorkspaceRoleChoice.MAINTAINER)
    private_workspaces[3].set_owner(user)
    accessible_workspace_names += [
        private_workspaces[0].name,
        private_workspaces[1].name,
        private_workspaces[2].name,
        private_workspaces[3].name,
    ]

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
@pytest.mark.parametrize(
    'permission,is_owner,status_code,success',
    [
        (None, False, 404, False),
        (WorkspaceRoleChoice.READER, False, 403, False),
        (WorkspaceRoleChoice.WRITER, False, 403, False),
        (WorkspaceRoleChoice.MAINTAINER, False, 200, True),
        (None, True, 200, True),
    ],
)
def test_workspace_rest_rename(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)

    old_name = workspace.name
    new_name = Faker().pystr()

    r = authenticated_api_client.put(
        f'/api/workspaces/{workspace.name}/',
        {
            'name': new_name,
        },
        format='json',
    )
    assert r.status_code == status_code

    # Retrieve workspace to ensure it's up to date
    workspace = Workspace.objects.get(id=workspace.pk)

    # Assert name is as expected
    expected_name = new_name if success else old_name
    assert workspace.name == expected_name


@pytest.mark.django_db
@pytest.mark.parametrize(
    'permission,is_owner,status_code,success',
    [
        (None, False, 404, False),
        (WorkspaceRoleChoice.READER, False, 200, True),
        (WorkspaceRoleChoice.WRITER, False, 200, True),
        (WorkspaceRoleChoice.MAINTAINER, False, 200, True),
        (None, True, 200, True),
    ],
)
def test_workspace_rest_retrieve(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)

    r = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/')

    assert r.status_code == status_code
    if success:
        assert r.data == {
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
@pytest.mark.parametrize(
    'permission,is_owner,status_code,success',
    [
        (None, False, 404, False),
        (WorkspaceRoleChoice.READER, False, 403, False),
        (WorkspaceRoleChoice.WRITER, False, 403, False),
        (WorkspaceRoleChoice.MAINTAINER, False, 403, False),
        (None, True, 204, True),
    ],
)
def test_workspace_rest_delete(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)
    r = authenticated_api_client.delete(f'/api/workspaces/{workspace.name}/')

    assert r.status_code == status_code

    if success:
        # Assert relevant objects are deleted
        assert not Workspace.objects.filter(name=workspace.name).exists()
        assert not arango_system_db().has_database(workspace.arango_db_name)
    else:
        # Assert objecsts are not deleted
        assert Workspace.objects.filter(name=workspace.name).exists()
        assert arango_system_db().has_database(workspace.arango_db_name)


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
@pytest.mark.parametrize(
    'permission,is_owner,status_code,success',
    [
        (None, False, 404, False),
        (WorkspaceRoleChoice.READER, False, 403, False),
        (WorkspaceRoleChoice.WRITER, False, 403, False),
        (WorkspaceRoleChoice.MAINTAINER, False, 200, True),
        (None, True, 200, True),
    ],
)
def test_workspace_rest_get_permissions(
    workspace: Workspace,
    user: User,
    user_factory: UserFactory,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)
        workspace.set_owner(user_factory())
    elif is_owner:
        workspace.set_owner(user)
    create_users_with_permissions(user_factory, workspace)
    maintainer_names = [maintainer.username for maintainer in workspace.maintainers]
    writer_names = [writer.username for writer in workspace.writers]
    reader_names = [reader.username for reader in workspace.readers]

    r = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/permissions/')
    assert r.status_code == status_code

    if success:
        r_json = r.json()
        assert r_json['public'] == workspace.public
        assert r_json['owner']['username'] == workspace.owner.username

        assert all(
            maintainer['username'] in maintainer_names for maintainer in r_json['maintainers']
        )
        assert all(writer['username'] in writer_names for writer in r_json['writers'])
        assert all(reader['username'] in reader_names for reader in r_json['readers'])


@pytest.mark.django_db
@pytest.mark.parametrize(
    'permission,is_owner,status_code,success',
    [
        (None, False, 404, False),
        (WorkspaceRoleChoice.READER, False, 403, False),
        (WorkspaceRoleChoice.WRITER, False, 403, False),
        (WorkspaceRoleChoice.MAINTAINER, False, 200, True),
        (None, True, 200, True),
    ],
)
def test_workspace_rest_put_permissions(
    workspace: Workspace,
    user: User,
    user_factory: UserFactory,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    old_owner = workspace.owner
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
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

    assert r.status_code == status_code

    if success:
        assert workspace.public == request_data['public']

        if is_owner:
            assert workspace.owner == new_owner
        else:
            assert workspace.owner == old_owner

        readers_names = [reader['username'] for reader in new_readers]
        writers_names = [writer['username'] for writer in new_writers]
        maintainers_names = [maintainer['username'] for maintainer in new_maintainers]
        assert all([reader.username in readers_names for reader in workspace.readers])
        assert all([writer.username in writers_names for writer in workspace.writers])
        assert all(
            [maintainer.username in maintainers_names for maintainer in workspace.maintainers]
        )
    else:
        assert workspace.public is False
        assert workspace.owner == old_owner


@pytest.mark.django_db
@pytest.mark.parametrize(
    'permission,is_owner,status_code,success',
    [
        (None, False, 404, False),
        (WorkspaceRoleChoice.READER, False, 200, True),
        (WorkspaceRoleChoice.WRITER, False, 200, True),
        (WorkspaceRoleChoice.MAINTAINER, False, 200, True),
        (None, True, 200, True),
    ],
)
def test_workspace_rest_get_user_permission(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)

    r = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/permissions/me/')
    assert r.status_code == status_code

    if success:
        permission, permission_label = workspace.get_user_permission_tuple(user)
        assert r.data == {
            'username': user.username,
            'workspace': workspace.name,
            'permission': permission,
            'permission_label': permission_label,
        }


@pytest.mark.django_db
def test_workspace_rest_get_user_permission_public(
    public_workspace_factory: PublicWorkspaceFactory, api_client: APIClient
):
    workspace = public_workspace_factory()
    r = api_client.get(f'/api/workspaces/{workspace.name}/permissions/me/')
    assert r.status_code == 200
    assert r.data == {
        'username': '',  # anonymous user
        'workspace': workspace.name,
        'permission': WorkspaceRoleChoice.READER.value,
        'permission_label': WorkspaceRoleChoice.READER.label,
    }


@pytest.mark.django_db
@pytest.mark.parametrize(
    'permission,is_owner,status_code,success',
    [
        (None, False, 404, False),
        (WorkspaceRoleChoice.READER, False, 200, True),
        (WorkspaceRoleChoice.WRITER, False, 200, True),
        (WorkspaceRoleChoice.MAINTAINER, False, 200, True),
        (None, True, 200, True),
    ],
)
def test_workspace_rest_aql(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, WorkspaceRoleChoice.READER)
    elif is_owner:
        workspace.set_owner(user)

    node_table = populated_table(workspace, False)
    nodes: Cursor = node_table.get_rows()
    nodes_list = list(nodes)

    # try and execute a valid non-mutating query on the data
    r = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/aql/',
        {
            'query': 'FOR doc IN @@TABLE RETURN doc',
            'bind_vars': {
                '@TABLE': node_table.name,
            },
        },
    )
    assert r.status_code == status_code

    if success:
        results = r.json()
        for node in nodes_list:
            assert node in results


@pytest.mark.django_db
def test_workspace_rest_aql_mutating_query(
    workspace: Workspace, user: User, authenticated_api_client: APIClient
):
    workspace.set_user_permission(user, WorkspaceRoleChoice.READER)
    fake = Faker()
    node_table = populated_table(workspace, False)

    # Mutating query
    r = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/aql/',
        data={
            'query': 'INSERT {name: @DOCNAME} INTO @@TABLE',
            'bind_vars': {
                '@TABLE': node_table.name,
                'DOCNAME': fake.pystr(),
            },
        },
    )
    assert r.status_code == 400
