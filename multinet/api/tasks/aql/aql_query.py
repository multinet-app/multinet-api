from arango.cursor import Cursor
from arango.exceptions import AQLQueryExecuteError, ArangoServerError
from celery import shared_task

from multinet.api.models import AqlQuery, Workspace
from multinet.api.tasks import MultinetCeleryTask
from multinet.api.utils.arango import ArangoQuery


class ExecuteAqlQueryTask(MultinetCeleryTask):
    task_model = AqlQuery


@shared_task(base=ExecuteAqlQueryTask)
def execute_query(task_id: int) -> None:
    query_task: AqlQuery = AqlQuery.objects.select_related('workspace').get(id=task_id)
    workspace: Workspace = query_task.workspace
    query_str = query_task.query

    try:
        # Run the query on Arango DB
        database = workspace.get_arango_db()
        query = ArangoQuery(database, query_str, time_limit_secs=60)
        cursor: Cursor = query.execute()

        # Store the results on the task object
        query_task.results = list(cursor)
        query_task.save()
    except (AQLQueryExecuteError, ArangoServerError) as err:
        ExecuteAqlQueryTask.fail_task_with_message(query_task, err.error_message)
