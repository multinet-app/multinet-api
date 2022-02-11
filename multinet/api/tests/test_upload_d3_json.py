import json
import operator
import pathlib
from typing import Dict
import uuid

from django.contrib.auth.models import User
from django.core.files.uploadedfile import SimpleUploadedFile
import pytest
from rest_framework.response import Response
from rest_framework.test import APIClient

from multinet.api.models import (
    Network,
    Table,
    Upload,
    Workspace,
    WorkspaceRole,
    WorkspaceRoleChoice,
)
from multinet.api.tasks.upload.d3_json import d3_link_to_arango_doc, d3_node_to_arango_doc
from multinet.api.tests.fuzzy import (
    INTEGER_ID_RE,
    TIMESTAMP_RE,
    dict_to_fuzzy_arango_doc,
    s3_file_field_re,
    workspace_re,
)
from multinet.api.views.upload import InvalidFieldValueResponse

data_dir = pathlib.Path(__file__).parent / 'data'
miserables_json_file = data_dir / 'miserables.json'


def local_json_upload(path: pathlib.Path, workspace, user) -> Upload:
    with open(path, 'rb') as f:
        file = SimpleUploadedFile(name=path.name, content=f.read())

    return Upload.objects.create(
        workspace=workspace,
        user=user,
        blob=file,
        data_type=Upload.DataType.CSV,
    )


@pytest.fixture
def miserables_json_field_value(s3ff_field_value_factory, workspace, user) -> str:
    upload = local_json_upload(miserables_json_file, workspace, user)
    return s3ff_field_value_factory(upload.blob)


@pytest.fixture
def miserables_json(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    miserables_json_field_value,
) -> Dict:
    # Model creation request
    workspace.set_user_permission(user, WorkspaceRoleChoice.WRITER)
    network_name = f't{uuid.uuid4().hex}'
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/uploads/d3_json/',
        {
            'field_value': miserables_json_field_value,
            'network_name': network_name,
        },
        format='json',
    )
    WorkspaceRole.objects.filter(workspace=workspace, user=user).delete()
    return {
        'response': r,
        'network_name': network_name,
    }


@pytest.mark.django_db
def test_create_upload_model(workspace: Workspace, user: User, miserables_json):
    """Test just the response of the model creation, not the task itself."""
    r = miserables_json['response']

    assert r.status_code == 200
    assert r.json() == {
        'id': INTEGER_ID_RE,
        'workspace': workspace_re(workspace),
        'blob': s3_file_field_re(miserables_json_file.name),
        'user': user.username,
        'data_type': Upload.DataType.D3_JSON,
        'error_messages': None,
        'status': Upload.Status.PENDING,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
    }


@pytest.mark.django_db
def test_create_upload_model_duplicate_names(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    miserables_json_field_value,
):
    """Test that attempting to create a network with names that are already taken, fails."""
    workspace.set_user_permission(user, WorkspaceRoleChoice.WRITER)
    network_name = f't{uuid.uuid4().hex}'

    def assert_response():
        r: Response = authenticated_api_client.post(
            f'/api/workspaces/{workspace.name}/uploads/d3_json/',
            {
                'field_value': miserables_json_field_value,
                'network_name': network_name,
            },
            format='json',
        )

        assert r.status_code == 400
        assert 'network_name' in r.json()

    # Try with just node table
    node_table: Table = Table.objects.create(
        name=f'{network_name}_nodes', workspace=workspace, edge=False
    )
    assert_response()

    # Add edge table
    edge_table: Table = Table.objects.create(
        name=f'{network_name}_edges', workspace=workspace, edge=True
    )
    assert_response()

    # Add network
    Network.create_with_edge_definition(network_name, workspace, edge_table.name, [node_table.name])
    assert_response()


@pytest.mark.django_db
def test_create_upload_model_invalid_field_value(
    workspace: Workspace, user: User, authenticated_api_client: APIClient
):
    workspace.set_user_permission(user, WorkspaceRoleChoice.WRITER)
    network_name = f't{uuid.uuid4().hex}'
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/uploads/d3_json/',
        {
            'field_value': 'field_value',
            'network_name': network_name,
        },
        format='json',
    )

    assert r.status_code == 400
    assert r.json() == InvalidFieldValueResponse.json()


@pytest.mark.django_db
@pytest.mark.parametrize('permission,status_code', [(None, 404), (WorkspaceRoleChoice.READER, 403)])
def test_create_upload_model_invalid_permissions(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    miserables_json_field_value,
    permission: WorkspaceRoleChoice,
    status_code: int,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)

    network_name = f't{uuid.uuid4().hex}'
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/uploads/d3_json/',
        {
            'field_value': miserables_json_field_value,
            'network_name': network_name,
        },
        format='json',
    )
    assert r.status_code == status_code


@pytest.mark.django_db
def test_valid_d3_json_task_response(
    workspace: Workspace, user: User, authenticated_api_client: APIClient, miserables_json
):
    """Test just the response of the model creation, not the task itself."""
    # Get upload info
    workspace.set_user_permission(user, WorkspaceRoleChoice.WRITER)
    r = miserables_json['response']
    network_name = miserables_json['network_name']
    node_table_name = f'{network_name}_nodes'
    edge_table_name = f'{network_name}_edges'

    # Since we're running with celery_task_always_eager=True, this job is finished
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/uploads/{r.json()["id"]}/'
    )

    r_json = r.json()
    assert r.status_code == 200
    assert r_json['status'] == Upload.Status.FINISHED
    assert r_json['error_messages'] is None

    # Check that tables are created
    for table_name in (node_table_name, edge_table_name):
        r: Response = authenticated_api_client.get(
            f'/api/workspaces/{workspace.name}/tables/{table_name}/'
        )
        assert r.status_code == 200

    # Check that network was created
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/networks/{network_name}/'
    )
    assert r.status_code == 200

    # Get source data
    with open(miserables_json_file) as file_stream:
        loaded_miserables_json_file = json.load(file_stream)
        nodes = sorted(
            (d3_node_to_arango_doc(node) for node in loaded_miserables_json_file['nodes']),
            key=operator.itemgetter('_key'),
        )
        links = sorted(
            (
                d3_link_to_arango_doc(link, node_table_name)
                for link in loaded_miserables_json_file['links']
            ),
            key=operator.itemgetter('_from'),
        )

    # Check that nodes were ingested correctly
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/networks/{network_name}/nodes/'
    )

    r_json = r.json()
    assert r.status_code == 200
    assert r_json['count'] == len(nodes)

    results = sorted(r_json['results'], key=operator.itemgetter('_key'))
    for i, node in enumerate(nodes):
        assert results[i] == dict_to_fuzzy_arango_doc(node, exclude=['_key'])

    # Check that links were ingested correctly
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/networks/{network_name}/edges/'
    )

    r_json = r.json()
    assert r.status_code == 200
    assert r_json['count'] == len(links)

    results = sorted(r_json['results'], key=operator.itemgetter('_from'))
    for i, link in enumerate(links):
        assert results[i] == dict_to_fuzzy_arango_doc(link)
