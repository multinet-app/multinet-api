import io
import json
import operator
import pathlib
from typing import IO, Dict
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
miserables_key_from_to_json_file = data_dir / 'miserables-key-from-to.json'


def json_upload(obj: IO[bytes], name: str, workspace, user):
    return Upload.objects.create(
        workspace=workspace,
        user=user,
        blob=SimpleUploadedFile(name=name, content=obj.read()),
        data_type=Upload.DataType.CSV,
    )


def json_file_upload(path: pathlib.Path, workspace, user) -> Upload:
    with open(path, 'rb') as f:
        return json_upload(f, path.name, workspace, user)


@pytest.fixture
def miserables_json_field_value(s3ff_field_value_factory, workspace, user) -> str:
    upload = json_file_upload(miserables_json_file, workspace, user)
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
    node_table_name = f't{uuid.uuid4().hex}_nodes'
    edge_table_name = f't{uuid.uuid4().hex}_edges'
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/uploads/json_network/',
        {
            'field_value': miserables_json_field_value,
            'network_name': network_name,
            'node_table_name': node_table_name,
            'edge_table_name': edge_table_name,
            'node_columns': {'id': 'primary key', 'group': 'category'},
            'edge_columns': {
                'source': 'edge source',
                'target': 'edge target',
                'value': 'number',
            },
        },
        format='json',
    )
    WorkspaceRole.objects.filter(workspace=workspace, user=user).delete()
    return {
        'response': r,
        'network_name': network_name,
    }


@pytest.fixture
def miserables_json_key_from_to_field_value(s3ff_field_value_factory, workspace, user) -> str:
    upload = json_file_upload(miserables_key_from_to_json_file, workspace, user)
    return s3ff_field_value_factory(upload.blob)


@pytest.fixture
def miserables_json_key_from_to(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    miserables_json_key_from_to_field_value,
) -> Dict:
    # Model creation request
    workspace.set_user_permission(user, WorkspaceRoleChoice.WRITER)
    network_name = f't{uuid.uuid4().hex}'
    node_table_name = f't{uuid.uuid4().hex}_nodes'
    edge_table_name = f't{uuid.uuid4().hex}_edges'
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/uploads/json_network/',
        {
            'field_value': miserables_json_key_from_to_field_value,
            'network_name': network_name,
            'node_table_name': node_table_name,
            'edge_table_name': edge_table_name,
            'node_columns': {'_key': 'primary key', 'group': 'category'},
            'edge_columns': {
                '_from': 'edge source',
                '_to': 'edge target',
                'value': 'number',
            },
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
        'data_type': Upload.DataType.JSON_NETWORK,
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
            f'/api/workspaces/{workspace.name}/uploads/json_network/',
            {
                'field_value': miserables_json_field_value,
                'network_name': network_name,
                'node_table_name': f'{network_name}_nodes',
                'edge_table_name': f'{network_name}_edges',
                'node_columns': {'id': 'primary key', 'group': 'category'},
                'edge_columns': {
                    'source': 'edge source',
                    'target': 'edge target',
                    'value': 'number',
                },
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
        f'/api/workspaces/{workspace.name}/uploads/json_network/',
        {
            'field_value': 'field_value',
            'network_name': network_name,
            'node_table_name': f'{network_name}_nodes',
            'edge_table_name': f'{network_name}_edges',
            'node_columns': {'id': 'primary key', 'group': 'category'},
            'edge_columns': {
                'source': 'edge source',
                'target': 'edge target',
                'value': 'number',
            },
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
        f'/api/workspaces/{workspace.name}/uploads/json_network/',
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
        nodes = sorted(loaded_miserables_json_file['nodes'], key=operator.itemgetter('id'))
        links = sorted(loaded_miserables_json_file['links'], key=operator.itemgetter('source'))

    # Check that nodes were ingested correctly
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/networks/{network_name}/nodes/'
    )

    r_json = r.json()
    assert r.status_code == 200
    assert r_json['count'] == len(nodes)

    results = sorted(r_json['results'], key=operator.itemgetter('_key'))
    for i, node in enumerate(nodes):
        results[i]['group'] = int(results[i]['group'])
        node['_key'] = str(node['id'])
        del node['id']
        assert results[i] == dict_to_fuzzy_arango_doc(node)

    # Check that links were ingested correctly
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/networks/{network_name}/edges/'
    )

    r_json = r.json()
    assert r.status_code == 200
    assert r_json['count'] == len(links)

    results = sorted(r_json['results'], key=operator.itemgetter('_from'))
    for i, link in enumerate(links):
        results[i]['_from'] = results[i]['_from'].split('/')[1]
        results[i]['_to'] = results[i]['_to'].split('/')[1]
        link['_from'] = str(link['source'])
        link['_to'] = str(link['target'])
        del link['source'], link['target']
        assert results[i] == dict_to_fuzzy_arango_doc(link)


@pytest.mark.django_db
def test_valid_d3_json_task_response_key_from_to(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    miserables_json_key_from_to,
):
    """Test just the response of the model creation, not the task itself."""
    # Get upload info
    workspace.set_user_permission(user, WorkspaceRoleChoice.WRITER)
    r = miserables_json_key_from_to['response']
    network_name = miserables_json_key_from_to['network_name']
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
    with open(miserables_key_from_to_json_file) as file_stream:
        loaded_miserables_key_from_to_json_file = json.load(file_stream)
        nodes = sorted(
            loaded_miserables_key_from_to_json_file['nodes'], key=operator.itemgetter('_key')
        )
        edges = sorted(
            loaded_miserables_key_from_to_json_file['edges'], key=operator.itemgetter('_from')
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
        results[i]['group'] = int(results[i]['group'])
        assert results[i] == dict_to_fuzzy_arango_doc(node, exclude=['_key'])

    # Check that links were ingested correctly
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/networks/{network_name}/edges/'
    )

    r_json = r.json()
    assert r.status_code == 200
    assert r_json['count'] == len(edges)

    results = sorted(r_json['results'], key=operator.itemgetter('_from'))
    for i, link in enumerate(edges):
        results[i]['_from'] = results[i]['_from'].split('/')[1]
        results[i]['_to'] = results[i]['_to'].split('/')[1]
        assert results[i] == dict_to_fuzzy_arango_doc(link)


@pytest.mark.django_db
def test_d3_json_task_filter_missing(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    s3ff_field_value_factory,
):
    """Test that missing node.id or link.[source/target] fields are removed."""
    workspace.set_user_permission(user, WorkspaceRoleChoice.WRITER)

    # Read original file
    json_dict = json.load(open(miserables_json_file, 'r'))
    original_node_length = len(json_dict['nodes'])
    original_link_length = len(json_dict['links'])

    # Add empty entries
    json_dict['nodes'].extend([{} for _ in range(10)])
    new_node_length = len(json_dict['nodes'])
    assert new_node_length != original_node_length

    json_dict['links'].extend([{} for _ in range(15)])
    new_links_length = len(json_dict['links'])
    assert new_links_length != original_link_length

    # Upload new broken JSON
    file = io.BytesIO(json.dumps(json_dict).encode('utf-8'))
    upload = json_upload(file, 'miserables', workspace, user)
    field_value = s3ff_field_value_factory(upload.blob)

    network_name = f't{uuid.uuid4().hex}'
    node_table_name = f'{network_name}_nodes'
    edge_table_name = f'{network_name}_edges'
    upload_resp: Response = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/uploads/json_network/',
        {
            'field_value': field_value,
            'network_name': network_name,
            'node_table_name': node_table_name,
            'edge_table_name': edge_table_name,
            'node_columns': {'id': 'primary key', 'group': 'category'},
            'edge_columns': {
                'source': 'edge source',
                'target': 'edge target',
                'value': 'number',
            },
        },
        format='json',
    )

    # Assert upload succeeds
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/uploads/{upload_resp.json()["id"]}/'
    )
    assert r.status_code == 200
    assert r.json()['status'] == Upload.Status.FINISHED
    assert r.json()['error_messages'] is None

    # Assert node table doesn't contain empty rows
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/tables/{node_table_name}/rows/', {'limit': 1}
    )
    assert r.status_code == 200
    assert r.json()['count'] == original_node_length

    # Assert edge table doesn't contain empty rows
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/tables/{edge_table_name}/rows/', {'limit': 1}
    )
    assert r.status_code == 200
    assert r.json()['count'] == original_link_length
