from django.contrib.auth.models import User
import pytest
from rest_framework.response import Response
from rest_framework.test import APIClient

from multinet.api.models.tasks import Upload
from multinet.api.models.workspace import Workspace, WorkspaceRoleChoice
from multinet.api.tests.factories import UploadFactory
from multinet.api.tests.fuzzy import TIMESTAMP_RE, workspace_re


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
def test_upload_rest_retrieve(
    workspace: Workspace,
    user: User,
    upload_factory: UploadFactory,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    """Test retrieval of an upload on a private workspace."""
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)

    upload: Upload = upload_factory(workspace=workspace, user=user)
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/uploads/{upload.pk}/'
    )
    assert r.status_code == status_code

    if success:
        assert r.data == {
            'id': upload.pk,
            'blob': upload.blob,
            'user': user.username,
            'created': TIMESTAMP_RE,
            'modified': TIMESTAMP_RE,
            'error_messages': upload.error_messages,
            'data_type': upload.data_type,
            'status': upload.status,
            'workspace': workspace_re(workspace),
        }
    else:
        assert r.data == {'detail': 'Not found.'}


@pytest.mark.django_db
def test_upload_rest_retrieve_public(
    public_workspace: Workspace,
    user: User,
    upload_factory: UploadFactory,
    api_client: APIClient,
):
    """Test retrieval of an upload on a public workspace for an unauthorized user."""
    upload = upload_factory(workspace=public_workspace, user=user)
    r: Response = api_client.get(f'/api/workspaces/{public_workspace.name}/uploads/{upload.pk}/')
    assert r.status_code == 200
    assert r.data == {
        'id': upload.pk,
        'blob': upload.blob,
        'user': user.username,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
        'error_messages': upload.error_messages,
        'data_type': upload.data_type,
        'status': upload.status,
        'workspace': workspace_re(public_workspace),
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
def test_upload_rest_list(
    workspace: Workspace,
    user: User,
    upload_factory: UploadFactory,
    authenticated_api_client: APIClient,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    """Test listing all uploads on a workspace for which the user has permission."""
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)

    upload_ids = [upload_factory(workspace=workspace, user=user).pk for _ in range(3)]
    r: Response = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/uploads/')
    assert r.status_code == status_code

    if success:
        r_json = r.json()
        assert r_json['count'] == len(upload_ids)
        for upload in r_json['results']:
            assert upload['id'] in upload_ids
    else:
        assert r.data == {'detail': 'Not found.'}


@pytest.mark.django_db
def test_upload_rest_list_public(
    public_workspace: Workspace,
    user: User,
    upload_factory: UploadFactory,
    api_client: APIClient,
):
    """Test listing all uploads on a public workspace with an unauthorized user."""
    upload_ids = [upload_factory(workspace=public_workspace, user=user).pk for _ in range(3)]
    r: Response = api_client.get(f'/api/workspaces/{public_workspace.name}/uploads/')
    r_json = r.json()

    assert r_json['count'] == len(upload_ids)
    for upload in r_json['results']:
        assert upload['id'] in upload_ids
