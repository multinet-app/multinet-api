from arango.exceptions import (
    ArangoClientError,
    ArangoServerError,
    PermissionUpdateError,
    UserCreateError,
)
from django.conf import settings
from django.core.management.base import BaseCommand
from django.contrib.auth.models import User

from multinet.api.utils.arango import arango_system_db
from multinet.api.models import Workspace, Network, Table
from multinet.api.tasks.upload.csv import process_row

from pathlib import Path
from io import StringIO
import csv



class Command(BaseCommand):
    help = "Sets up a standard development environment with example workspaces and tables."

    def create_workspace_if_not_exists(self, workspace_name):
        try:
            # Check if workspace exists, if so remove it
            if Workspace.objects.filter(name=workspace_name).exists():
                self.stdout.write(self.style.SUCCESS(
                    f"{workspace_name} already exists, removing to recreate"
                ))
                Workspace.objects.filter(name=workspace_name).delete()

            # Create the workspace
            Workspace.objects.create(name=workspace_name, public=True, owner=self.user)
            self.stdout.write(self.style.SUCCESS(
                f"{workspace_name} created"
            ))

        except Exception as e:
            self.stderr.write(self.style.ERROR(str(e)))

    def create_tables_for_workspace(self, workspace_name, edge_table_name):
        try:
            # Get the paths for all the data objects to upload
            data_dir_path = Path(settings.BASE_DIR) / "data" / workspace_name
            csv_paths = list(data_dir_path.glob("*.csv"))
            workspace = Workspace.objects.get(name=workspace_name)

            for csv_path in csv_paths:
                filename = str(csv_path).split("/")[-1].split(".")[0]
                
                # Check if table with name exists, if so delete
                if Table.objects.filter(workspace=workspace, name=filename).exists():
                    self.stdout.write(self.style.SUCCESS(
                        f"{workspace_name}/{filename} already exists, removing to recreate"
                    ))
                    Table.objects.filter(workspace=workspace, name=filename).delete()
                
                # Create the table
                new_table = Table.objects.create(workspace=workspace, name=filename, edge=filename==edge_table_name)

                # Open the file and read in the rows
                with csv_path.open('rb') as f:
                    csv_rows = list(csv.DictReader(StringIO(f.read().decode('utf-8'))))

                    # TODO: add back when table type annotations are added
                    # for i, row in enumerate(csv_rows):
                    #     csv_rows[i] = process_row(row, columns)

                    # Put the rows into the table
                    new_table.put_rows(csv_rows)

                # TODO: Create table type annotations

                self.stdout.write(self.style.SUCCESS(
                    f"{workspace_name}/{filename} created"
                ))

        except Exception as e:
            self.stderr.write(self.style.ERROR(str(e)))

    def create_network_in_workspace(self, workspace_name, edge_table_name, node_table_names):
        try:
            workspace = Workspace.objects.get(name=workspace_name)

            # # Get the table objects to create the network
            # edge_table = Table.objects.get(name=edge_table_name, workspace=workspace)

            # node_tables = []
            # for node_table_name in node_table_names:
            #     node_tables.append(Table.objects.get(name=node_table_name, workspace=workspace))

            # Create the network
            Network.create_with_edge_definition(workspace_name, workspace, edge_table_name, node_table_names)
            self.stdout.write(self.style.SUCCESS(
                f"{workspace_name} network created"
            ))

        except Exception as e:
            self.stderr.write(self.style.ERROR(str(e)))

    def add_arguments(self, parser):
        parser.add_argument('email', type=str)

    def handle(self, *args, **options):
        # Check that owner exists
        queried_users = User.objects.filter(email=options["email"])
        if not queried_users.exists():
            raise Exception('User did not exist')
        self.user = queried_users.first()

        # Create the workspaces
        self.create_workspace_if_not_exists("boston")
        self.create_workspace_if_not_exists("eurovis-2019")
        self.create_workspace_if_not_exists("miserables")
        self.create_workspace_if_not_exists("movies")
        self.create_workspace_if_not_exists("openflights")

        # Create the tables
        self.create_tables_for_workspace("boston", "membership")
        self.create_tables_for_workspace("eurovis-2019", "connections")
        self.create_tables_for_workspace("miserables", "relationships")
        self.create_tables_for_workspace("movies", "")
        self.create_tables_for_workspace("openflights", "routes")

        # Create the networks from the tables
        self.create_network_in_workspace("boston", "membership", ["clubs", "members"])
        self.create_network_in_workspace("eurovis-2019", "connections", ["people"])
        self.create_network_in_workspace("miserables", "relationships", ["characters"])
        # self.create_network_in_workspace("movies") Not possible with the data structure,
        # maybe we could use the new network creation API from the command line
        self.create_network_in_workspace("openflights", "routes", ["airports"])
