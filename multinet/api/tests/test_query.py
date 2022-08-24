from django.contrib.auth.models import User
from faker import Faker
import pytest
from rest_framework.response import Response
from rest_framework.test import APIClient

from multinet.api.models.tasks import AqlQuery
from multinet.api.models.workspace import Workspace, WorkspaceRole, WorkspaceRoleChoice
from multinet.api.tests.conftest import populated_table
from multinet.api.tests.fuzzy import INTEGER_ID_RE, TIMESTAMP_RE, workspace_re


@pytest.fixture
def valid_query(workspace: Workspace, user: User, authenticated_api_client: APIClient):
    """Create a fixture representing the response of a POST request for AQL queries."""
    workspace.set_user_permission(user, WorkspaceRoleChoice.READER)
    node_table = populated_table(workspace, False)

    query_str = 'FOR document IN @@TABLE RETURN document'
    bind_vars = {'@TABLE': node_table.name}
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/queries/',
        {'query': query_str, 'bind_vars': bind_vars},
        format='json',
    )
    WorkspaceRole.objects.filter(workspace=workspace, user=user).delete()

    return {
        'response': r,
        'query': query_str,
        'bind_vars': bind_vars,
        'nodes': list(node_table.get_rows()),
    }


@pytest.fixture
def mutating_query(workspace: Workspace, user: User, authenticated_api_client: APIClient):
    """Create a fixture for a mutating AQL query that will have an error message post processing."""
    workspace.set_user_permission(user, WorkspaceRoleChoice.READER)
    node_table = populated_table(workspace, False)

    query_str = 'INSERT {name: @DOCNAME} INTO @@TABLE'
    bind_vars = {'@TABLE': node_table.name, 'DOCNAME': Faker().pystr()}
    r: Response = authenticated_api_client.post(
        f'/api/workspaces/{workspace.name}/queries/',
        {'query': query_str, 'bind_vars': bind_vars},
        format='json',
    )
    WorkspaceRole.objects.filter(workspace=workspace, user=user).delete()

    return {
        'response': r,
        'query': query_str,
        'bind_vars': bind_vars,
        'nodes': list(node_table.get_rows()),
    }


@pytest.mark.django_db
def test_query_rest_create(workspace: Workspace, user: User, valid_query):
    r = valid_query['response']
    assert r.status_code == 200
    assert r.json() == {
        'id': INTEGER_ID_RE,
        'workspace': workspace_re(workspace),
        'query': valid_query['query'],
        'bind_vars': valid_query['bind_vars'],
        'user': user.username,
        'error_messages': None,
        'status': AqlQuery.Status.PENDING,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
    }


@pytest.mark.django_db
def test_query_rest_create_mutating(workspace: Workspace, user: User, mutating_query):
    r = mutating_query['response']

    # even though the query is not read-only, the task object should be created
    assert r.status_code == 200
    assert r.json() == {
        'id': INTEGER_ID_RE,
        'workspace': workspace_re(workspace),
        'query': mutating_query['query'],
        'bind_vars': mutating_query['bind_vars'],
        'user': user.username,
        'error_messages': None,
        'status': AqlQuery.Status.PENDING,
        'created': TIMESTAMP_RE,
        'modified': TIMESTAMP_RE,
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
def test_query_rest_retrieve(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    valid_query,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)

    query_info = valid_query['response'].json()
    query_id = query_info['id']
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/queries/{query_id}/'
    )
    assert r.status_code == status_code
    if success:
        r_json = r.json()
        assert r_json['status'] == AqlQuery.Status.FINISHED


@pytest.mark.django_db
def test_query_rest_retrieve_mutating(
    workspace: Workspace, user: User, authenticated_api_client: APIClient, mutating_query
):
    workspace.set_user_permission(user, WorkspaceRoleChoice.READER)

    query_info = mutating_query['response'].json()
    query_id = query_info['id']
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/queries/{query_id}/'
    )
    assert r.status_code == 200
    r_json = r.json()
    assert len(r_json['error_messages']) > 0
    assert r_json['status'] == AqlQuery.Status.FAILED


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
def test_query_rest_retrieve_results(
    workspace: Workspace,
    user: User,
    authenticated_api_client: APIClient,
    valid_query,
    permission: WorkspaceRoleChoice,
    is_owner: bool,
    status_code: int,
    success: bool,
):
    if permission is not None:
        workspace.set_user_permission(user, permission)
    elif is_owner:
        workspace.set_owner(user)

    query_info = valid_query['response'].json()
    query_id = query_info['id']
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/queries/{query_id}/results/'
    )
    assert r.status_code == status_code
    if success:
        r_json = r.json()
        assert r_json['id'] == query_id
        assert r_json['workspace'] == str(workspace)
        assert r_json['user'] == str(user)

        results = r_json['results']
        expected_results = valid_query['nodes']
        assert len(results) == len(expected_results)
        for row in results:
            assert row in expected_results


@pytest.mark.django_db
def test_query_rest_retrieve_results_mutating(
    workspace: Workspace, user: User, authenticated_api_client: APIClient, mutating_query
):
    workspace.set_user_permission(user, WorkspaceRoleChoice.READER)
    query_info = mutating_query['response'].json()
    query_id = query_info['id']
    r: Response = authenticated_api_client.get(
        f'/api/workspaces/{workspace.name}/queries/{query_id}/results/'
    )
    assert r.status_code == 400
    assert r.data == 'The given query could not be executed, and has no results'
