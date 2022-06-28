import csv
from io import StringIO
import json
import os
from pathlib import Path

from django.conf import settings
from django.contrib.auth.models import User
from django.core.management.base import BaseCommand

from multinet.api.models import Network, Table, TableTypeAnnotation, Workspace
from multinet.api.tasks.upload.csv import process_row


class Command(BaseCommand):
    help = 'Sets up a standard development environment with example workspaces and tables.'

    def create_workspace_if_not_exists(self, workspace_name):
        try:
            # Check if workspace exists, if so remove it
            if Workspace.objects.filter(name=workspace_name).exists():
                self.stdout.write(
                    self.style.SUCCESS(f'{workspace_name} already exists, removing to recreate')
                )
                Workspace.objects.filter(name=workspace_name).delete()

            # Create the workspace
            Workspace.objects.create(name=workspace_name, public=True, owner=self.user)
            self.stdout.write(self.style.SUCCESS(f'{workspace_name} created'))

        except Exception as e:
            self.stderr.write(self.style.ERROR(str(e)))

    def create_tables_for_workspace(self, workspace_name, edge_table_name):
        try:
            # Get the paths for all the data objects to upload
            data_dir_path = Path(settings.BASE_DIR) / 'data' / workspace_name
            csv_paths = list(data_dir_path.glob('*.csv'))
            workspace = Workspace.objects.get(name=workspace_name)

            for csv_path in csv_paths:
                filename = os.path.splitext(csv_path.name)[0]

                # Check if table with name exists, if so delete
                if Table.objects.filter(workspace=workspace, name=filename).exists():
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'{workspace_name}/{filename} already exists, removing to recreate'
                        )
                    )
                    Table.objects.filter(workspace=workspace, name=filename).delete()

                # Open the types file and read the types in
                types_path = data_dir_path / 'types' / f'{filename}.json'
                with types_path.open('rb') as f:
                    columns = json.load(f)

                # Open the csv and read in the rows
                with csv_path.open('rb') as f:
                    csv_rows = list(csv.DictReader(StringIO(f.read().decode('utf-8'))))

                # Process csv rows with the type annotations
                for i, row in enumerate(csv_rows):
                    csv_rows[i] = process_row(row, columns)

                # Create the table
                new_table = Table.objects.create(
                    workspace=workspace, name=filename, edge=filename == edge_table_name
                )

                # Put the rows into the table
                new_table.put_rows(csv_rows)

                # Create type annotations
                TableTypeAnnotation.objects.bulk_create(
                    [
                        TableTypeAnnotation(table=new_table, column=col_key, type=col_type)
                        for col_key, col_type in columns.items()
                    ]
                )

                self.stdout.write(self.style.SUCCESS(f'{workspace_name}/{filename} created'))

        except Exception as e:
            self.stderr.write(self.style.ERROR(str(e)))

    def create_network_in_workspace(self, workspace_name, edge_table_name, node_table_names):
        try:
            workspace = Workspace.objects.get(name=workspace_name)

            # Create the network
            Network.create_with_edge_definition(
                workspace_name, workspace, edge_table_name, node_table_names
            )
            self.stdout.write(self.style.SUCCESS(f'{workspace_name} network created'))

        except Exception as e:
            self.stderr.write(self.style.ERROR(str(e)))

    def add_arguments(self, parser):
        parser.add_argument('email', type=str)

    def handle(self, *args, **options):
        # Check that owner exists
        self.user = User.objects.filter(email=options['email']).first()
        if self.user is None:
            raise Exception('User did not exist')

        # Create the workspaces
        self.create_workspace_if_not_exists('boston')
        self.create_workspace_if_not_exists('eurovis-2019')
        self.create_workspace_if_not_exists('miserables')
        self.create_workspace_if_not_exists('movies')
        self.create_workspace_if_not_exists('openflights')

        # Create the tables
        self.create_tables_for_workspace('boston', 'membership')
        self.create_tables_for_workspace('eurovis-2019', 'connections')
        self.create_tables_for_workspace('miserables', 'relationships')
        self.create_tables_for_workspace('openflights', 'routes')

        # Create the networks from the tables
        self.create_network_in_workspace('boston', 'membership', ['clubs', 'members'])
        self.create_network_in_workspace('eurovis-2019', 'connections', ['people'])
        self.create_network_in_workspace('miserables', 'relationships', ['characters'])
        self.create_network_in_workspace('openflights', 'routes', ['airports'])

        # TODO: Include movies dataset here once new network creation API
        # is accessible internally from the API
