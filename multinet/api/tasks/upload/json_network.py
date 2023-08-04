import json
from typing import BinaryIO, Dict

from celery import shared_task
from celery.utils.log import get_task_logger

from multinet.api.models import Network, Upload
from multinet.api.models.table import TableTypeAnnotation

from .common import ProcessUploadTask
from .exceptions import DataFormatError
from .process_single_table import process_single_table

logger = get_task_logger(__name__)


@shared_task(base=ProcessUploadTask)
def process_json_network(
    task_id: int,
    network_name: str,
    node_table_name: str,
    edge_table_name: str,
    node_column_types: Dict[str, TableTypeAnnotation.Type],
    edge_column_types: Dict[str, TableTypeAnnotation.Type],
) -> None:
    upload: Upload = Upload.objects.get(id=task_id)

    # Download data from S3/MinIO
    with upload.blob as blob_file:
        blob_file: BinaryIO
        d3_dict = json.loads(blob_file.read().decode('utf-8'))

    if 'links' in d3_dict:
        link_property_name = 'links'
    elif 'edges' in d3_dict:
        link_property_name = 'edges'
    else:
        raise DataFormatError("JSON network file missing 'links' or 'edges' property")

    nodes = d3_dict['nodes']
    edges = d3_dict[link_property_name]

    # Create node table
    process_single_table(
        nodes,
        node_table_name,
        upload.workspace,
        False,
        node_column_types,
    )

    # Create edge table
    process_single_table(
        edges,
        edge_table_name,
        upload.workspace,
        True,
        edge_column_types,
        node_table_name,
    )

    # Create network
    Network.create_with_edge_definition(
        name=network_name,
        workspace=upload.workspace,
        edge_table=edge_table_name,
        node_tables=[node_table_name],
    )
