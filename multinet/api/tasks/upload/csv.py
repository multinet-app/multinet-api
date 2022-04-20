import csv
from io import StringIO
from typing import Any, BinaryIO, Dict

from celery import shared_task
from celery.utils.log import get_task_logger
from django.shortcuts import get_object_or_404

from multinet.api.models import Network, Table, TableTypeAnnotation, Upload, Workspace
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

    # Perform joins before edge creation, since new tables are created
    mapped_tables = {}
    for table, mapping in serializer.validated_data.get('joins', {}).items():
        foreign_table = mapping['foreign_column']['table']
        foreign_column = mapping['foreign_column']['column']
        joined_table, created = Table.objects.get_or_create(
            name=f'{foreign_table}-joined-{table}', workspace=workspace
        )

        # Clear table rows if already exists
        joined_table: Table
        mapped_tables[foreign_table] = joined_table.name
        if created:
            joined_table.get_arango_collection(readonly=False).truncate()

        # Begin AQL query for joining
        bind_vars = {
            '@TABLE': table,
            'TABLE_COL': mapping['column'],
            '@FOREIGN_TABLE': foreign_table,
            'FOREIGN_TABLE_COL': foreign_column,
            '@JOINED_TABLE': joined_table.name,
        }
        query_str = """
            FOR foreign_doc in @@FOREIGN_TABLE
                // Find matching doc
                LET table_doc = FIRST(
                    FOR doc in @@TABLE
                        FILTER doc.@TABLE_COL == foreign_doc.@FOREIGN_TABLE_COL
                        return doc
                ) || {}

                LET new_doc = MERGE(
                    UNSET(foreign_doc, ['_id', '_key', 'rev']),
                    UNSET(table_doc, ['_id', '_key', 'rev'])
                )
                INSERT new_doc IN @@JOINED_TABLE
        """
        query = ArangoQuery(
            workspace.get_arango_db(readonly=False),
            query_str=query_str,
            bind_vars=bind_vars,
        )
        query.execute()

    source_edge_table: Table = get_object_or_404(
        Table,
        workspace=workspace,
        name=serializer.validated_data['edge_table']['name'],
    )

    # Create new edge table
    network_name = serializer.validated_data['name']
    new_edge_table: Table = Table.objects.create(
        name=f'{network_name}_edges', workspace=workspace, edge=True
    )

    # Use mapped source table if joined on, otherwise original
    edge_table_data = serializer.validated_data['edge_table']
    foreign_source_table = mapped_tables.get(
        edge_table_data['source']['foreign_column']['table'],
        edge_table_data['source']['foreign_column']['table'],
    )

    # Use mapped target table if joined on, otherwise original
    foreign_target_table = mapped_tables.get(
        edge_table_data['target']['foreign_column']['table'],
        edge_table_data['target']['foreign_column']['table'],
    )

    # Copy rows from original edge table to new edge table
    bind_vars = {
        '@ORIGINAL': source_edge_table.get_arango_collection().name,
        '@NEW_COLL': new_edge_table.get_arango_collection().name,
        # Source
        'ORIGINAL_SOURCE_COLUMN': edge_table_data['source']['column'],
        '@FOREIGN_SOURCE_TABLE': foreign_source_table,
        'FOREIGN_SOURCE_COLUMN': edge_table_data['source']['foreign_column']['column'],
        # Target
        'ORIGINAL_TARGET_COLUMN': edge_table_data['target']['column'],
        '@FOREIGN_TARGET_TABLE': foreign_target_table,
        'FOREIGN_TARGET_COLUMN': edge_table_data['target']['foreign_column']['column'],
    }
    query_str = """
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
    """
    query = ArangoQuery(
        workspace.get_arango_db(readonly=False),
        query_str=query_str,
        bind_vars=bind_vars,
    )
    query.execute()

    # Create network
    return Network.create_with_edge_definition(
        name=network_name,
        workspace=workspace,
        edge_table=new_edge_table.name,
        node_tables=[foreign_source_table, foreign_target_table],
    )
