import csv
from io import StringIO
from typing import Any, BinaryIO, Dict, Tuple

from celery import shared_task
from celery.utils.log import get_task_logger

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


def maybe_insert_join_statement(query: str, bind_vars: Dict, table_dict: Dict) -> Tuple[str, Dict]:
    """Return mutated query and bind_vars to account for joins."""
    join_table = None
    join_table_excluded = []
    join_col_local = None
    join_col_foreign = None
    if table_dict.get('joined') is not None:
        join_table = table_dict['joined']['table']['name']
        join_table_excluded = table_dict['joined']['table']['excluded']
        join_col_local = table_dict['joined']['link']['local']
        join_col_foreign = table_dict['joined']['link']['foreign']

    # Conditionally insert join vars and query text
    if join_table is None:
        query += """
            LET final_doc = new_doc
        """
    else:
        bind_vars.update(
            {
                '@JOINING_TABLE': join_table,
                'JOINING_TABLE_EXCLUDED': join_table_excluded,
                'JOIN_COL_LOCAL': join_col_local,
                'JOIN_COL_FOREIGN': join_col_foreign,
            }
        )
        query += """
            // Perform join
            LET foreign_doc = (
                FIRST(
                    FOR doc in @@JOINING_TABLE
                        FILTER new_doc.@JOIN_COL_LOCAL == doc.@JOIN_COL_FOREIGN
                        return doc
                ) || {}
            )
            LET foreign_excluded = APPEND(['_id', '_key', 'rev'], @JOINING_TABLE_EXCLUDED)
            LET new_foreign_doc = UNSET(foreign_doc, foreign_excluded)
            LET final_doc = MERGE(new_doc, new_foreign_doc)
        """

    return query, bind_vars


# CSV Network functions
def create_table(workspace: Workspace, network_name: str, table_dict: Dict) -> str:
    """Create table from definition, including joins."""
    # table_dict has the shape of the FullTable serializer
    original_table_name = table_dict['name']
    new_table_name = f'{network_name}--{table_dict["name"]}'
    excluded_columns = table_dict['excluded']

    # Create table, deleting any data if it already exists
    table, created = Table.objects.get_or_create(workspace=workspace, name=new_table_name)
    if not created:
        table.get_arango_collection(readonly=False).truncate()

    # AQL query for copying doc, excluding certain columns, and joining
    bind_vars = {
        '@ORIGINAL_TABLE': original_table_name,
        '@TABLE': new_table_name,
        'EXCLUDED_COLS': excluded_columns,
    }
    query_str = """
        FOR og_doc in @@ORIGINAL_TABLE
            // Copy doc, excluding specified columns
            LET excluded = APPEND(['_id', '_key', 'rev'], @EXCLUDED_COLS)
            LET new_doc = UNSET(og_doc, excluded)
    """

    # Add join statements if needed
    query_str, bind_vars = maybe_insert_join_statement(query_str, bind_vars, table_dict)

    # Add final insert
    query_str += """
            INSERT final_doc IN @@TABLE
    """

    # Execute query
    ArangoQuery(
        workspace.get_arango_db(readonly=False),
        query_str=query_str,
        bind_vars=bind_vars,
    ).execute()

    return new_table_name


def create_csv_network(workspace: Workspace, serializer):
    """Create a network from a link of tables (in request thread)."""
    from multinet.api.views.serializers import CSVNetworkCreateSerializer

    serializer: CSVNetworkCreateSerializer
    serializer.is_valid(raise_exception=True)

    # Create source/target tables
    data = serializer.validated_data
    network_name = data['name']
    source_table = create_table(workspace, network_name, data['source_table'])
    target_table = create_table(workspace, network_name, data['target_table'])

    # Create table, deleting any data if it already exists
    edge_table_name = data['edge']['table']['name']
    new_edge_table_name = f'{network_name}--{edge_table_name}'
    table, created = Table.objects.get_or_create(
        workspace=workspace, name=new_edge_table_name, edge=True
    )
    if not created:
        table.get_arango_collection(readonly=False).truncate()

    # Setup bind vars for query
    bind_vars = {
        '@ORIGINAL': edge_table_name,
        '@NEW_TABLE': new_edge_table_name,
        # Source
        '@SOURCE_TABLE': source_table,
        'SOURCE_LINK_LOCAL': data['edge']['source']['local'],
        'SOURCE_LINK_FOREIGN': data['edge']['source']['foreign'],
        # Target
        '@TARGET_TABLE': target_table,
        'TARGET_LINK_LOCAL': data['edge']['target']['local'],
        'TARGET_LINK_FOREIGN': data['edge']['target']['foreign'],
    }

    # Make query to copy edge table docs to new edge table, inserting from/to links
    query_str = """
        FOR edge_doc in @@ORIGINAL
            // Find matching source doc
            LET source_doc = FIRST(
                FOR dd in @@SOURCE_TABLE
                    FILTER edge_doc.@SOURCE_LINK_LOCAL == dd.@SOURCE_LINK_FOREIGN
                    return dd
            )
            // Find matching target doc
            LET target_doc = FIRST(
                FOR dd in @@TARGET_TABLE
                    FILTER edge_doc.@TARGET_LINK_LOCAL == dd.@TARGET_LINK_FOREIGN
                    return dd
            )
            // Add _from/_to to new doc, remove internal fields, insert into new coll
            LET new_edge_doc = MERGE(edge_doc, {'_from': source_doc._id, '_to': target_doc._id})
            LET new_doc = UNSET(new_edge_doc, ['_id', '_key', 'rev'])
    """

    # Add join statements if needed
    query_str, bind_vars = maybe_insert_join_statement(query_str, bind_vars, data['edge']['table'])
    query_str += """
            INSERT final_doc INTO @@NEW_TABLE
    """

    # Execute query
    ArangoQuery(
        workspace.get_arango_db(readonly=False),
        query_str=query_str,
        bind_vars=bind_vars,
    ).execute()

    # Create network
    return Network.create_with_edge_definition(
        name=network_name,
        workspace=workspace,
        edge_table=new_edge_table_name,
        node_tables=[source_table, target_table],
    )
