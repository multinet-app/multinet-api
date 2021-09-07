from django.contrib.auth.models import User
import pytest
from rest_framework.response import Response
from rest_framework.test import APIClient

from multinet.api.models.tasks import AqlQuery
from multinet.api.models.workspace import Workspace, WorkspaceRole, WorkspaceRoleChoice
from multinet.api.tests.conftest import populated_table
from multinet.api.tests.fuzzy import INTEGER_ID_RE, TIMESTAMP_RE, workspace_re


@pytest.fixture
def simple_query(workspace: Workspace, user: User, authenticated_api_client: APIClient):
    """Create a fixture representing the response of a POST request for AQL queries."""
    workspace.set_user_permission(user, WorkspaceRoleChoice.READER)
    node_table = populated_table(workspace, False)
    query_str = f'FOR document IN {node_table.name} RETURN document'
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/queries/', {'query': query_str}, format='json'
    )
    WorkspaceRole.objects.filter(workspace=workspace, user=user).delete()
    return {'response': r, 'query': query_str, 'nodes': list(node_table.get_rows())}


@pytest.mark.django_db
def test_query_rest_create(workspace: Workspace, user: User, simple_query):
    r = simple_query['response']
    assert r.status_code == 200
    assert r.json() == {
        'id': INTEGER_ID_RE,
        'workspace': workspace_re(workspace),
        'query': simple_query['query'],
        'user': user.username,
        'error_messages': None,
        'status': AqlQuery.Status.PENDING,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
        'query_results': None,
    }


@pytest.mark.django_db
def test_query_rest_retrieve(
    workspace: Workspace, user: User, authenticated_api_client: APIClient, simple_query
):
    workspace.set_user_permission(user, WorkspaceRoleChoice.READER)
    query_info = simple_query['response'].json()
    query_id = query_info['id']
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/queries/{query_id}/'
    )
    assert r.status_code == 200
    r_json = r.json()
    results = r_json['query_results']
    expected_results = simple_query['nodes']
    assert len(results) == len(expected_results)
    for row in results:
        assert row in expected_results
