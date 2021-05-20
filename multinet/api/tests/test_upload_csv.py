import csv
import pathlib
import time
from typing import Dict
import uuid

from django.contrib.auth.models import User
import pytest
from rest_framework.response import Response

from multinet.api.models.upload import Upload
from multinet.api.models.workspace import Workspace
from multinet.api.tasks.process.utils import str_to_number
from multinet.api.tests.fuzzy import (
    INTEGER_ID_RE,
    TIMESTAMP_RE,
    dict_to_fuzzy_arango_doc,
    s3_file_field_re,
    workspace_re,
)

data_dir = pathlib.Path(__file__).parent / 'data'


@pytest.fixture
def airports_csv(owned_workspace: Workspace, authenticated_api_client, s3ff_client) -> Dict:
    data_file = data_dir / 'airports.csv'
    with open(data_file) as file_stream:
        field_value = s3ff_client.upload_file(
            file_stream,  # This can be any file-like object
            data_file.name,
            'api.Upload.blob',  # The "<app>.<model>.<field>" to upload to
        )['field_value']

    # Model creation request
    table_name = f't{uuid.uuid4().hex}'
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{owned_workspace.name}/uploads/csv/',
        {
            'field_value': field_value,
            'edge': False,
            'table_name': table_name,
            'columns': [
                {
                    'key': 'latitude',
                    'type': 'number',
                },
                {
                    'key': 'longitude',
                    'type': 'number',
                },
                {
                    'key': 'altitude',
                    'type': 'number',
                },
                {
                    'key': 'timezone',
                    'type': 'number',
                },
                {
                    'key': 'year built',
                    'type': 'number',
                },
            ],
        },
        format='json',
    )

    return {
        'response': r,
        'table_name': table_name,
        'data_file': data_file,
    }


@pytest.mark.django_db
def test_create_upload_model_csv(owned_workspace: Workspace, user: User, airports_csv):
    """Test just the response of the model creation, not the task itself."""
    r = airports_csv['response']
    data_file = airports_csv['data_file']

    assert r.status_code == 200
    assert r.json() == {
        'id': INTEGER_ID_RE,
        'workspace': workspace_re(owned_workspace),
        'blob': s3_file_field_re(data_file.name),
        'user': user.username,
        'data_type': Upload.DataType.CSV,
        'error_messages': None,
        'status': Upload.UploadStatus.PENDING,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
    }


@pytest.mark.django_db
def test_create_upload_model_invalid_columns(owned_workspace: Workspace, authenticated_api_client):
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{owned_workspace.name}/uploads/csv/',
        {
            # Not an issue to specify invalid field_value, as that's checked after columns,
            # so the request will return before that is checked
            'field_value': 'field_value',
            'edge': False,
            'table_name': 'table',
            'columns': [{'key': 'foo', 'type': 'invalid'}],
        },
        format='json',
    )

    assert r.status_code == 400
    assert r.json() == {'columns': {'0': {'type': ['"invalid" is not a valid choice.']}}}


@pytest.mark.django_db
def test_create_upload_model_invalid_field_value(
    owned_workspace: Workspace, authenticated_api_client
):
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{owned_workspace.name}/uploads/csv/',
        {
            'field_value': 'field_value',
            'edge': False,
            'table_name': 'table',
            'columns': [],
        },
        format='json',
    )

    assert r.status_code == 400
    assert r.json() == {'field_value': ['field_value is not a valid signed string.']}


# @pytest.mark.usefixtures('celery_session_app')
# @pytest.mark.celery(task_always_eager=True)
@pytest.mark.django_db
def test_upload_valid_csv_task_response(
    owned_workspace: Workspace, authenticated_api_client, airports_csv
):
    """Test just the response of the model creation, not the task itself."""
    # Get upload info
    r = airports_csv['response']
    data_file = airports_csv['data_file']
    table_name = airports_csv['table_name']

    # Try fetching until job is finished
    finished = False
    r_json = r.json()
    upload_id = r_json['id']
    for _ in range(10):
        time.sleep(0.5)
        r: Response = authenticated_api_client.get(
            f'/api/workspaces/{owned_workspace.name}/uploads/{upload_id}/'
        )

        assert r.status_code == 200
        r_json = r.json()
        if r_json['status'] == Upload.UploadStatus.FINISHED:
            finished = True
            break

    assert finished
    assert r_json['error_messages'] is None

    # Check that table is created
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/tables/{table_name}/'
    )
    assert r.status_code == 200

    # Check that data was ingested correctly
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{owned_workspace.name}/tables/{table_name}/rows/'
    )

    assert r.status_code == 200
    r_json = r.json()
    results = r_json['results']

    # Get source data rows
    with open(data_file) as file_stream:
        rows = [row for row in csv.DictReader(file_stream)]

    # Check rows themselves
    assert r_json['count'] == len(rows)
    for i, row in enumerate(rows):
        result = results[i]

        # Convert these keys so we can compare documents
        for key in ['latitude', 'longitude', 'altitude', 'timezone', 'year built']:
            row[key] = str_to_number(row[key])

        # Assert documents match
        assert result == dict_to_fuzzy_arango_doc(row)
