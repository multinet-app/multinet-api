import celery
from celery.utils.log import get_task_logger

from multinet.api.models import Task

logger = get_task_logger(__name__)


class MultinetCeleryTask(celery.Task):
    """
    A base class for multinet celery tasks.

    This class should not be instantiated directly. Instead, task classes should inherit from this
    class and override the `task_model` field with the desired model to associate to the tasks to.

    NOTE: This task assumes that all arguments are passed using kwargs.
    If an argument is passed positionally, this task will fail.
    """

    task_model = None

    @classmethod
    def start_task(cls, task_id: int):
        logger.info(f'Begin processing of {cls.task_model.__name__.lower()} {task_id}')
        task: Task = cls.task_model.objects.get(id=task_id)
        task.status = Task.Status.STARTED
        task.save()

    @staticmethod
    def fail_task_with_message(task: Task, message: str):
        task.status = Task.Status.FAILED
        if task.error_messages is None:
            task.error_messages = [message]
        else:
            task.error_messages.append(message)

        task.save()

    @staticmethod
    def complete_task(task: Task):
        task.status = Task.Status.FINISHED
        task.save()

    def __init__(self) -> None:
        if self.task_model is None:
            raise NotImplementedError('task_model cannot be None')

        super().__init__()

    def __call__(self, *args, **kwargs):
        """Wrap the inherited `__call__` method to set upload status."""
        self.start_task(kwargs['task_id'])
        return self.run(*args, **kwargs)

    def on_failure(self, exc, celery_task_id, args, kwargs, einfo):
        task: Task = self.task_model.objects.get(id=kwargs['task_id'])
        self.fail_task_with_message(task, exc)

    def on_success(self, retval, celery_task_id, args, kwargs):
        task: Task = self.task_model.objects.get(id=kwargs['task_id'])
        self.complete_task(task)
