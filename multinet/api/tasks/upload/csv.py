import csv
from io import StringIO
from typing import Any, BinaryIO, Dict

from celery import shared_task
from celery.utils.log import get_task_logger
from django.shortcuts import get_object_or_404

from multinet.api.models import Table, TableTypeAnnotation, Upload
from multinet.api.models.workspace import Workspace
from multinet.api.utils.arango import ArangoQuery

from .common import ProcessUploadTask
from .utils import processor_dict

logger = get_task_logger(__name__)


def process_row(row: Dict[str, Any], cols: Dict[str, TableTypeAnnotation.Type]) -> Dict:
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
    task_id: int, table_name: str, edge: bool, columns: Dict[str, TableTypeAnnotation.Type]
) -> None:
    upload: Upload = Upload.objects.get(id=task_id)

    # Download data from S3/MinIO
    with upload.blob as blob_file:
        blob_file: BinaryIO = blob_file
        csv_rows = list(csv.DictReader(StringIO(blob_file.read().decode('utf-8'))))

    # Cast entries in each row to appropriate type, if necessary
    for i, row in enumerate(csv_rows):
        csv_rows[i] = process_row(row, columns)

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

    # Insert rows
    table.put_rows(csv_rows)


def create_csv_network(workspace: Workspace, serializer):
    """Create a network from a link of tables (in request thread)."""
    from multinet.api.views.serializers import CSVNetworkCreateSerializer

    serializer: CSVNetworkCreateSerializer
    serializer.is_valid(raise_exception=True)
    source_edge_table: Table = get_object_or_404(
        Table,
        workspace=workspace,
        name=serializer.validated_data['edge_table']['name'],
    )

    # Create new edge table
    new_edge_table: Table = Table.objects.create(
        name=f'{serializer.validated_data["name"]}_edges', workspace=workspace, edge=True
    )

    # Copy rows from original edge table to new edge table
    edge_table_data = serializer.validated_data['edge_table']
    bind_vars = {
        '@ORIGINAL': source_edge_table.get_arango_collection().name,
        '@NEW_COLL': new_edge_table.get_arango_collection().name,
        # Source
        'ORIGINAL_SOURCE_COLUMN': edge_table_data['source']['column'],
        '@FOREIGN_SOURCE_TABLE': edge_table_data['source']['foreign_column']['table'],
        'FOREIGN_SOURCE_COLUMN': edge_table_data['source']['foreign_column']['column'],
        # Target
        'ORIGINAL_TARGET_COLUMN': edge_table_data['target']['column'],
        '@FOREIGN_TARGET_TABLE': edge_table_data['target']['foreign_column']['table'],
        'FOREIGN_TARGET_COLUMN': edge_table_data['target']['foreign_column']['column'],
    }
    query_str = '''
        FOR edge_doc in @@ORIGINAL
            // Find matching source doc
            LET source_doc = FIRST(
                FOR dd in @@FOREIGN_SOURCE_TABLE
                    FILTER edge_doc.@ORIGINAL_SOURCE_COLUMN == dd.@FOREIGN_SOURCE_COLUMN
                    return dd
            )
            // Find matching target doc
            LET target_doc = FIRST(
                FOR dd in @@FOREIGN_TARGET_TABLE
                    FILTER edge_doc.@ORIGINAL_TARGET_COLUMN == dd.@FOREIGN_TARGET_COLUMN
                    return dd
            )

            // Add _from/_to to new doc, remove internal fields, insert into new coll
            LET new_doc = MERGE(edge_doc, {'_from': source_doc._id, '_to': target_doc._id})
            LET fixed = UNSET(new_doc, ['_id', '_key', 'rev'])
            INSERT fixed INTO @@NEW_COLL
    '''
    query = ArangoQuery(
        workspace.get_arango_db(readonly=False),
        query_str=query_str,
        bind_vars=bind_vars,
    )
    query.execute()

    # Create network
    # TODO
