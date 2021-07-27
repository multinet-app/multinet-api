import csv
from io import StringIO
from typing import Any, BinaryIO, Dict

from celery import shared_task
from celery.utils.log import get_task_logger

from multinet.api.models import Table, Upload

from .utils import (
    ColumnTypeEnum,
    ProcessUploadTask,
    processor_dict,
)

logger = get_task_logger(__name__)


def process_row(row: Dict[str, Any], cols: Dict[str, ColumnTypeEnum]) -> Dict:
    """Process a CSV row."""
    new_row = dict(row)

    for col_key, col_type in cols.items():
        entry = row.get(col_key)

        # If null entry, skip
        if entry is None:
            continue

        process_func = processor_dict.get(col_type)
        if process_func is not None:
            try:
                new_row[col_key] = process_func(entry)
            except ValueError:
                # If error processing row, keep as string
                pass

    return new_row


@shared_task(base=ProcessUploadTask)
def process_csv(
    upload_id: int, table_name: str, edge: bool, columns: Dict[str, ColumnTypeEnum]
) -> None:
    logger.info(f'Begin processing of upload {upload_id}')
    upload: Upload = Upload.objects.get(id=upload_id)

    # Update status
    upload.status = Upload.UploadStatus.STARTED
    upload.save()

    # Download data from S3/MinIO
    with upload.blob as blob_file:
        blob_file: BinaryIO = blob_file
        try:
            csv_rows = list(csv.DictReader(StringIO(blob_file.read().decode('utf-8'))))
        except csv.Error:
            return ProcessUploadTask.fail_upload_with_message(upload, 'Failed to parse CSV.')

    # Cast entries in each row to appropriate type, if necessary
    for i, row in enumerate(csv_rows):
        csv_rows[i] = process_row(row, columns)

    # Create new table
    table: Table = Table.objects.create(
        name=table_name,
        edge=edge,
        workspace=upload.workspace,
    )

    # Insert rows
    table.put_rows(csv_rows)
