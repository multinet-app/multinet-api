from django.contrib.auth.models import User
import pytest
from rest_framework.response import Response
from rest_framework.test import APIClient

from multinet.api.models.upload import Upload
from multinet.api.models.workspace import Workspace
from multinet.api.tests.factories import UploadFactory
from multinet.api.tests.fuzzy import TIMESTAMP_RE, workspace_re
from multinet.api.utils.workspace_permissions import WorkspacePermission


@pytest.mark.django_db
def test_upload_rest_retrieve(
    workspace: Workspace,
    user: User,
    upload_factory: UploadFactory,
    authenticated_api_client: APIClient,
):
    """Test retrieval of an upload on a workspace for a user with read access."""
    workspace.set_user_permission(user, WorkspacePermission.reader)
    upload: Upload = upload_factory(workspace=workspace, user=user)
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/uploads/{upload.pk}/'
    )
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
        'workspace': workspace_re(workspace),
    }


@pytest.mark.django_db
def test_upload_rest_retrieve_public(
    public_workspace: Workspace,
    user: User,
    upload_factory: UploadFactory,
    api_client: APIClient,
):
    """Test retrieval of an upload on a public workspace for which the user has no permission."""
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
def test_upload_rest_retrieve_private(
    workspace: Workspace,
    user: User,
    upload_factory: UploadFactory,
    authenticated_api_client: APIClient,
):
    """Test retrieval of an upload on a workspace for which the user has no permission."""
    upload = upload_factory(workspace=workspace, user=user)
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/uploads/{upload.pk}/'
    )
    assert r.status_code == 404


@pytest.mark.django_db
def test_upload_rest_list(
    workspace: Workspace,
    user: User,
    upload_factory: UploadFactory,
    authenticated_api_client: APIClient,
):
    """Test listing all uploads on a workspace for which the user has permission."""
    workspace.set_user_permission(user, WorkspacePermission.reader)
    upload_ids = [upload_factory(workspace=workspace, user=user).pk for _ in range(3)]
    r: Response = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/uploads/')
    r_json = r.json()

    assert r_json['count'] == len(upload_ids)
    for upload in r_json['results']:
        assert upload['id'] in upload_ids


@pytest.mark.django_db
def test_upload_rest_list_public(
    public_workspace: Workspace,
    user: User,
    upload_factory: UploadFactory,
    api_client: APIClient,
):
    """Test listing all uploads on a public workspace for which the user has no permission."""
    upload_ids = [upload_factory(workspace=public_workspace, user=user).pk for _ in range(3)]
    r: Response = api_client.get(f'/api/workspaces/{public_workspace.name}/uploads/')
    r_json = r.json()

    assert r_json['count'] == len(upload_ids)
    for upload in r_json['results']:
        assert upload['id'] in upload_ids


@pytest.mark.django_db
def test_upload_rest_list_private(
    workspace: Workspace,
    user: User,
    upload_factory: UploadFactory,
    authenticated_api_client: APIClient,
):
    """Test listing all uploads on a private workspace for which the user has no permission."""
    for _ in range(3):
        upload_factory(workspace=workspace, user=user)
    r: Response = authenticated_api_client.get(f'/api/workspaces/{workspace.name}/uploads/')

    assert r.status_code == 404
