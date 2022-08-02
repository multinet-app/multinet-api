from arango.cursor import Cursor

# from arango.exceptions import AQLQueryExecuteError, ArangoServerError
from celery import shared_task

from multinet.api.models import AqlQuery, Workspace
from multinet.api.tasks import MultinetCeleryTask
from multinet.api.utils.arango import ArangoQuery


class AqlQueryTask(MultinetCeleryTask):
    task_model = AqlQuery


@shared_task(base=AqlQueryTask)
def execute_query(task_id: int) -> None:
    query_task: AqlQuery = AqlQuery.objects.select_related('workspace').get(id=task_id)
    workspace: Workspace = query_task.workspace

    # Run the query on Arango DB
    database = workspace.get_arango_db()
    query = ArangoQuery(
        database,
        query_str=query_task.query,
        bind_vars=query_task.bind_vars,
        time_limit_secs=60,
    )
    cursor: Cursor = query.execute()

    # Store the results on the task object
    query_task.results = list(cursor)
    query_task.save()
