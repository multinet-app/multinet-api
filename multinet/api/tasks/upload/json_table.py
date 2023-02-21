import json
from typing import Any, BinaryIO, Dict

from celery import shared_task

from multinet.api.models import Network, Table, TableTypeAnnotation, Upload

from .common import ProcessUploadTask
from .utils import processor_dict
from .exceptions import DataFormatException


def process_row(row: Dict[str, Any], cols: Dict[str, TableTypeAnnotation.Type]) -> Dict:
    new_row = dict(row)

    # Check for _key or id, if missing, skip row
    if not (new_row.get('_key') or new_row.get('id')):
        return None

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
def process_json_table(
    task_id: int,
    table_name: str,
    edge: bool,
    columns: Dict[str, TableTypeAnnotation.Type],
) -> None:
    upload: Upload = Upload.objects.get(id=task_id)

    # Create new table
    table: Table = Table.objects.create(
        name=table_name,
        edge=edge,
        workspace=upload.workspace,
    )

    # Create type annotations
    TableTypeAnnotation.objects.bulk_create(
        [
            TableTypeAnnotation(table=table, column=col_key, type=col_type)
            for col_key, col_type in columns.items()
        ]
    )

    # Download data from S3/MinIO
    with upload.blob as blob_file:
        blob_file: BinaryIO = blob_file
        imported_json = json.loads(blob_file.read().decode('utf-8'))

        processed_rows = [new_row for new_row in [process_row(row, columns) for row in imported_json] if new_row is not None]

        # Put rows in the table
        table.put_rows(processed_rows)
