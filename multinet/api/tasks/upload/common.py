from multinet.api.models import Upload
from multinet.api.tasks import MultinetCeleryTask


class ProcessUploadTask(MultinetCeleryTask):
    task_model = Upload
