import csv
from io import StringIO
from typing import Any, BinaryIO, Dict, List

from celery import shared_task
from celery.utils.log import get_task_logger

from multinet.api.models import Table, Upload

from .utils import ColumnType, complete_upload, fail_upload_with_message, processor_dict

logger = get_task_logger(__name__)


def process_row(row: Dict[str, Any], cols: List[ColumnType]) -> Dict:
    """Process a CSV row."""
    new_row = dict(row)

    for col in cols:
        entry = row.get(col.key)

        # If null entry, skip
        if entry is None:
            continue

        process_func = processor_dict.get(col.type)
        if process_func is not None:
            try:
                new_row[col.key] = process_func(entry)
            except ValueError:
                # If error processing row, keep as string
                pass

    return new_row


@shared_task
def process_csv(upload_id: int, table_name: str, edge: bool, columns: List[Dict]) -> None:
    logger.info(f'Begin processing of upload {upload_id}')
    upload: Upload = Upload.objects.get(id=upload_id)
    columns: List[ColumnType] = [ColumnType(**entry) for entry in columns]

    # Update status
    upload.status = Upload.UploadStatus.STARTED
    upload.save()

    # Download data from S3/MinIO
    with upload.blob as blob_file:
        blob_file: BinaryIO = blob_file
        try:
            csv_rows = list(csv.DictReader(StringIO(blob_file.read().decode('utf-8'))))
        except csv.Error:
            return fail_upload_with_message(upload, 'Failed to parse CSV.')

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

    # Mark upload as finished, if it hasn't been marked as failed
    if upload.status == Upload.UploadStatus.STARTED:
        complete_upload(upload)
