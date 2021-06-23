import json
import operator
import pathlib
from typing import Dict
import uuid

from django.contrib.auth.models import User
import pytest
from rest_framework.response import Response

from multinet.api.models import Network, Table, Upload, Workspace
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


@pytest.fixture
def miserables_json_field_value(s3ff_client) -> str:
    with open(miserables_json_file) as file_stream:
        field_value = s3ff_client.upload_file(
            file_stream,
            miserables_json_file.name,
            'api.Upload.blob',
        )['field_value']

    return field_value


@pytest.fixture
def miserables_json(
    owned_workspace: Workspace, authenticated_api_client, miserables_json_field_value
) -> Dict:
    # Model creation request
    network_name = f't{uuid.uuid4().hex}'
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{owned_workspace.name}/uploads/d3_json/',
        {
            'field_value': miserables_json_field_value,
            'network_name': network_name,
        },
        format='json',
    )

    return {
        'response': r,
        'network_name': network_name,
    }


@pytest.mark.django_db
def test_create_upload_model(owned_workspace: Workspace, user: User, miserables_json):
    """Test just the response of the model creation, not the task itself."""
    r = miserables_json['response']

    assert r.status_code == 200
    assert r.json() == {
        'id': INTEGER_ID_RE,
        'workspace': workspace_re(owned_workspace),
        'blob': s3_file_field_re(miserables_json_file.name),
        'user': user.username,
        'data_type': Upload.DataType.D3_JSON,
        'error_messages': None,
        'status': Upload.UploadStatus.PENDING,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
    }


@pytest.mark.django_db
def test_create_upload_model_duplicate_names(
    owned_workspace: Workspace, user: User, authenticated_api_client, miserables_json_field_value
):
    """Test that attempting to create a network with names that are already taken, fails."""
    network_name = f't{uuid.uuid4().hex}'

    def assert_response():
        r: Response = authenticated_api_client.post(
            f'/api/workspaces/{owned_workspace.name}/uploads/d3_json/',
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
        name=f'{network_name}_nodes', workspace=owned_workspace, edge=False
    )
    assert_response()

    # Add edge table
    edge_table: Table = Table.objects.create(
        name=f'{network_name}_edges', workspace=owned_workspace, edge=True
    )
    assert_response()

    # Add network
    Network.create_with_edge_definition(
        network_name, owned_workspace, edge_table.name, [node_table.name]
    )
    assert_response()


@pytest.mark.django_db
def test_create_upload_model_invalid_field_value(
    owned_workspace: Workspace, authenticated_api_client
):
    network_name = f't{uuid.uuid4().hex}'
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{owned_workspace.name}/uploads/d3_json/',
        {
            'field_value': 'field_value',
            'network_name': network_name,
        },
        format='json',
    )

    assert r.status_code == 400
    assert r.json() == InvalidFieldValueResponse.json()


@pytest.mark.django_db
def test_valid_d3_json_task_response(
    owned_workspace: Workspace, authenticated_api_client, miserables_json
):
    """Test just the response of the model creation, not the task itself."""
    # Get upload info
    r = miserables_json['response']
    network_name = miserables_json['network_name']
    node_table_name = f'{network_name}_nodes'
    edge_table_name = f'{network_name}_edges'

    # Since we're running with celery_task_always_eager=True, this job is finished
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/uploads/{r.json()["id"]}/'
    )

    r_json = r.json()
    assert r.status_code == 200
    assert r_json['status'] == Upload.UploadStatus.FINISHED
    assert r_json['error_messages'] is None

    # Check that tables are created
    for table_name in (node_table_name, edge_table_name):
        r: Response = authenticated_api_client.get(
            f'/api/workspaces/{owned_workspace.name}/tables/{table_name}/'
        )
        assert r.status_code == 200

    # Check that network was created
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/networks/{network_name}/'
    )
    assert r.status_code == 200

    # Get source data
    with open(miserables_json_file) as file_stream:
        loaded_miserables_json_file = json.load(file_stream)
        nodes = sorted(loaded_miserables_json_file['nodes'], key=operator.itemgetter('id'))
        links = sorted(loaded_miserables_json_file['links'], key=operator.itemgetter('source'))

    # Check that nodes were ingested correctly
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/networks/{network_name}/nodes/'
    )

    r_json = r.json()
    assert r.status_code == 200
    assert r_json['count'] == len(nodes)

    results = sorted(r_json['results'], key=operator.itemgetter('_key'))
    for i, node in enumerate(nodes):
        result = results[i]

        # Fix key as is done in task
        node['_key'] = node.pop('id')

        # Assert documents match
        assert result == dict_to_fuzzy_arango_doc(node, exclude=['_key'])

    # Check that links were ingested correctly
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/networks/{network_name}/edges/'
    )

    r_json = r.json()
    assert r.status_code == 200
    assert r_json['count'] == len(links)

    results = sorted(r_json['results'], key=operator.itemgetter('_from'))
    for i, link in enumerate(links):
        result = results[i]

        # Fix key as is done in task
        link['_from'] = f'{node_table_name}/{link.pop("source")}'
        link['_to'] = f'{node_table_name}/{link.pop("target")}'

        # Assert documents match
        assert result == dict_to_fuzzy_arango_doc(link)
