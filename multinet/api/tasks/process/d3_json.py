import json
from typing import BinaryIO, Dict

from celery import shared_task
from celery.utils.log import get_task_logger

from multinet.api.models import Network, Table, Upload

from .utils import complete_upload, fail_upload_with_message

logger = get_task_logger(__name__)


def d3_node_to_arango_doc(node: Dict) -> Dict:
    new_node = dict(node)
    new_node['_key'] = str(new_node.pop('id'))

    return new_node


def d3_link_to_arango_doc(link: Dict, node_table_name: str) -> Dict:
    new_link = dict(link)
    new_link['_from'] = f'{node_table_name}/{new_link.pop("source")}'
    new_link['_to'] = f'{node_table_name}/{new_link.pop("target")}'

    return new_link


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
        blob_file: BinaryIO
        try:
            d3_dict = json.loads(blob_file.read().decode('utf-8'))
        except json.JSONDecodeError:
            return fail_upload_with_message(upload, 'Failed to parse JSON.')

    # Change column names from the d3 format to the arango format
    d3_dict['nodes'] = [d3_node_to_arango_doc(node) for node in d3_dict['nodes']]
    d3_dict['links'] = [d3_link_to_arango_doc(link, node_table_name) for link in d3_dict['links']]

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
