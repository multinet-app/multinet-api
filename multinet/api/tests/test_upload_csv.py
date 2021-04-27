import pathlib

import pytest

data_dir = pathlib.Path(__file__).parent / 'data'


@pytest.mark.django_db
def test_upload_valid_csv(authenticated_api_client):
    # TODO: Implement once able
    pass
