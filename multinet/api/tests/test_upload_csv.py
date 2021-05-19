import pathlib

from django.contrib.auth.models import User
import pytest
from rest_framework.response import Response

from multinet.api.models.upload import Upload
from multinet.api.models.workspace import Workspace
from multinet.api.tests.fuzzy import INTEGER_ID_RE, TIMESTAMP_RE, s3_file_field_re, workspace_re

data_dir = pathlib.Path(__file__).parent / 'data'


@pytest.mark.django_db
def test_upload_valid_csv_dispatch(
    owned_workspace: Workspace, user: User, authenticated_api_client, s3ff_client
):
    """Test just the response of the model creation, not the task itself."""
    # Upload file using S3FF
    data_file = data_dir / 'airports.csv'
    with open(data_file) as file_stream:
        field_value = s3ff_client.upload_file(
            file_stream,  # This can be any file-like object
            data_file.name,
            'api.Upload.blob',  # The "<app>.<model>.<field>" to upload to
        )['field_value']

    # Model creation request
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{owned_workspace.name}/uploads/csv/',
        {
            'field_value': field_value,
            'edge': False,
            'table_name': 'test_table',
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
