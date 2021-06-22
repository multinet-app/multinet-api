import json
from typing import BinaryIO

from celery import shared_task
from celery.utils.log import get_task_logger

from multinet.api.models import Network, Table, Upload

from .utils import complete_upload, fail_upload_with_message

logger = get_task_logger(__name__)


@shared_task
def process_d3_json(
    upload_id: int,
    network_name: str,
    node_table_name: str,
    edge_table_name: str,
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
            d3_dict = json.loads(blob_file.read().decode('utf-8'))
        except json.JSONDecodeError:
            return fail_upload_with_message(upload, 'Failed to parse JSON.')

    # Change column names from the d3 format to the arango format
    for node in d3_dict['nodes']:
        node['_key'] = str(node.pop('id'))

    for link in d3_dict['links']:
        link['_from'] = f'{node_table_name}/{link.pop("source")}'
        link['_to'] = f'{node_table_name}/{link.pop("target")}'

    # Create ancillary tables
    node_table: Table = Table.objects.create(
        workspace=upload.workspace,
        name=node_table_name,
        edge=False,
    )
    edge_table: Table = Table.objects.create(
        workspace=upload.workspace,
        name=edge_table_name,
        edge=True,
    )

    # Insert rows
    node_table.put_rows(d3_dict['nodes'])
    edge_table.put_rows(d3_dict['links'])

    # Create network
    Network.create_with_edge_definition(
        name=network_name,
        workspace=upload.workspace,
        edge_table=edge_table_name,
        node_tables=[node_table_name],
    )

    # Mark upload as finished, if it hasn't been marked as failed
    if upload.status == Upload.UploadStatus.STARTED:
        complete_upload(upload)
