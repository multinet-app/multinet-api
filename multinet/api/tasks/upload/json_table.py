import json
from typing import BinaryIO, Dict

from celery import shared_task

from multinet.api.models import TableTypeAnnotation, Upload

from .common import ProcessUploadTask
from .process_single_table import process_single_table


@shared_task(base=ProcessUploadTask)
def process_json_table(
    task_id: int,
    table_name: str,
    edge: bool,
    columns: Dict[str, TableTypeAnnotation.Type],
) -> None:
    upload: Upload = Upload.objects.get(id=task_id)

    # Download data from S3/MinIO and import it into ArangoDB
    with upload.blob as blob_file:
        blob_file: BinaryIO = blob_file
        imported_json = json.loads(blob_file.read().decode('utf-8'))

        process_single_table(
            imported_json,
            table_name,
            upload.workspace,
            edge,
            columns,
        )
