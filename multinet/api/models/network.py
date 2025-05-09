from __future__ import annotations

from typing import List, Type

from arango.cursor import Cursor
from arango.graph import Graph
from django.db import models
from django.db.models.signals import post_delete, pre_save
from django.dispatch.dispatcher import receiver
from django_extensions.db.models import TimeStampedModel

from multinet.api.utils.arango import ArangoQuery

from .workspace import Workspace


class Network(TimeStampedModel):
    name = models.CharField(max_length=300)
    workspace = models.ForeignKey(Workspace, related_name='networks', on_delete=models.CASCADE)

    class Meta:
        unique_together = ('workspace', 'name')

    @property
    def node_count(self):
        db = self.workspace.get_arango_db()
        return sum(
            db.collection(coll).count() for coll in self.get_arango_graph().vertex_collections()
        )

    def nodes(self, limit: int = 0, offset: int = 0) -> Cursor:
        return (
            ArangoQuery.from_collections(self.workspace.get_arango_db(), self.node_tables())
            .paginate(limit=limit, offset=offset)
            .execute()
        )

    @property
    def edge_count(self) -> int:
        db = self.workspace.get_arango_db()
        return sum(
            db.collection(edge_def['edge_collection']).count()
            for edge_def in self.get_arango_graph().edge_definitions()
        )

    def edges(self, limit: int = 0, offset: int = 0) -> Cursor:
        return (
            ArangoQuery.from_collections(self.workspace.get_arango_db(), self.edge_tables())
            .paginate(limit=limit, offset=offset)
            .execute()
        )

    def get_arango_graph(self) -> Graph:
        workspace: Workspace = self.workspace
        return workspace.get_arango_db().graph(self.name)

    def node_tables(self) -> List[str]:
        return self.get_arango_graph().vertex_collections()

    def edge_tables(self) -> List[str]:
        return [
            edge_def['edge_collection'] for edge_def in self.get_arango_graph().edge_definitions()
        ]

    @classmethod
    def create_with_edge_definition(
        cls, name: str, workspace: Workspace, edge_table: str, node_tables: List[str]
    ) -> Network:
        """Create a network with an edge definition, using the provided arguments."""
        # Create graph in arango before creating the Network object here
        workspace.get_arango_db(readonly=False).create_graph(
            name,
            edge_definitions=[
                {
                    'edge_collection': edge_table,
                    'from_vertex_collections': node_tables,
                    'to_vertex_collections': node_tables,
                }
            ],
        )

        return Network.objects.create(
            name=name,
            workspace=workspace,
        )

    def copy(self, new_workspace: Workspace) -> Network:
        """
        Copy the network to a new workspace.

        This function creates a new network in the provided workspace, with the same
        name and schema as this table. Permissions are wiped, the requester is the owner
        """
        # Create a new table in the new workspace
        new_network = Network.create_with_edge_definition(
            name=self.name,
            workspace=new_workspace,
            edge_table=self.edge_tables()[0],
            node_tables=self.node_tables(),
        )

        new_network.save()
        return new_network

    def __str__(self) -> str:
        return self.name


# Handle arango sync
@receiver(pre_save, sender=Network)
def arango_graph_save(sender: Type[Network], instance: Network, **kwargs):
    workspace: Workspace = instance.workspace

    db = workspace.get_arango_db(readonly=False)
    if not db.has_graph(instance.name):
        db.create_graph(instance.name)


@receiver(post_delete, sender=Network)
def arango_graph_delete(sender: Type[Network], instance: Network, **kwargs):
    workspace: Workspace = instance.workspace

    db = workspace.get_arango_db(readonly=False)
    if db.has_graph(instance.name):
        db.delete_graph(instance.name)
