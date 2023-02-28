import json
from typing import BinaryIO, Dict

from celery import shared_task
from celery.utils.log import get_task_logger

from multinet.api.models import Network, Table, Upload

from .common import ProcessUploadTask
from .exceptions import DataFormatError

logger = get_task_logger(__name__)


def d3_node_to_arango_doc(node: Dict) -> Dict:
    new_node = dict(node)

    # Check if we have a field we can use for key. _key is preferred, then id
    if '_key' in new_node.keys():
        node_id = new_node.get('_key', None)
    elif 'id' in new_node.keys():
        node_id = new_node.pop('id', None)
    else:
        node_id = None

    if node_id is None:
        return None

    new_node['_key'] = str(node_id)
    return new_node


def d3_link_to_arango_doc(link: Dict, node_table_name: str) -> Dict:
    new_link = dict(link)

    # Check if we have a field we can use for from and to. _from and _to are preferred
    # then source and target
    if '_to' in new_link.keys() and '_from' in new_link.keys():
        source = new_link.get('_to', None).split('/')[-1]
        target = new_link.get('_from', None).split('/')[-1]
    elif 'source' in new_link.keys() and 'target' in new_link.keys():
        source = new_link.pop('source', None)
        target = new_link.pop('target', None)
    else:
        source = None
        target = None

    if source is None or target is None:
        return None

    # Set values
    new_link['_from'] = f'{node_table_name}/{source}'
    new_link['_to'] = f'{node_table_name}/{target}'

    return new_link


@shared_task(base=ProcessUploadTask)
def process_d3_json(
    task_id: int,
    network_name: str,
    node_table_name: str,
    edge_table_name: str,
) -> None:
    upload: Upload = Upload.objects.get(id=task_id)

    # Download data from S3/MinIO
    with upload.blob as blob_file:
        blob_file: BinaryIO
        d3_dict = json.loads(blob_file.read().decode('utf-8'))

    # Change column names from the d3 format to the arango format
    d3_dict['nodes'] = [
        node
        for node in (d3_node_to_arango_doc(node) for node in d3_dict['nodes'])
        if node is not None
    ]

    if 'links' in d3_dict.keys():
        link_property_name = 'links'
    elif 'edges' in d3_dict.keys():
        link_property_name = 'edges'
    else:
        raise DataFormatError("JSON network file missing 'links' or 'edges' property")

    d3_dict[link_property_name] = [
        link
        for link in (
            d3_link_to_arango_doc(link, node_table_name) for link in d3_dict[link_property_name]
        )
        if link is not None
    ]

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
    edge_table.put_rows(d3_dict[link_property_name])

    # Create network
    Network.create_with_edge_definition(
        name=network_name,
        workspace=upload.workspace,
        edge_table=edge_table_name,
        node_tables=[node_table_name],
    )
