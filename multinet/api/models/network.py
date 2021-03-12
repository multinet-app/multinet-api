from __future__ import annotations

from typing import List

from arango.graph import Graph
from django.db import models
from django_extensions.db.models import TimeStampedModel

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

    @property
    def edge_count(self) -> int:
        db = self.workspace.get_arango_db()
        return sum(
            db.collection(edge_def['edge_collection']).count()
            for edge_def in self.get_arango_graph().edge_definitions()
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

    def save(self, *args, **kwargs):
        workspace: Workspace = self.workspace

        db = workspace.get_arango_db()
        if not db.has_graph(self.name):
            db.create_graph(self.name)

        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        workspace: Workspace = self.workspace

        db = workspace.get_arango_db()
        if db.has_graph(self.name):
            db.delete_graph(self.name)

        super().delete(*args, **kwargs)

    def __str__(self) -> str:
        return self.name
