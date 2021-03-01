from __future__ import annotations

import itertools
from typing import List, Optional

from arango.collection import StandardCollection
from arango.cursor import Cursor
from arango.graph import Graph as ArangoGraph
from django.db import models
from django_extensions.db.models import TimeStampedModel

from .workspace import Workspace


class Graph(TimeStampedModel):
    name = models.CharField(max_length=300)
    workspace = models.ForeignKey(Workspace, related_name='graphs', on_delete=models.CASCADE)

    class Meta:
        unique_together = ('workspace', 'name')

    def get_arango_graph(self) -> ArangoGraph:
        workspace: Workspace = self.workspace
        return workspace.get_arango_db().graph(self.name)

    def node_count(self) -> int:
        db = self.workspace.get_arango_db()
        return sum(
            db.collection(coll).count() for coll in self.get_arango_graph().vertex_collections()
        )

    def nodes(self, page: Optional[int] = None, page_size: Optional[int] = None) -> Cursor:
        arango_graph = self.get_arango_graph()

        skip = 0
        if page and page_size:
            skip = (page - 1) * page_size

        cursors: List[Cursor] = []
        remaining = page_size

        for coll_name in arango_graph.vertex_collections():
            if remaining == 0:
                break

            coll: StandardCollection = self.workspace.get_arango_db().collection(coll_name)
            cursor: Cursor = coll.find({}, skip=skip, limit=remaining)
            cursors.append(cursor)

            skip -= min(skip, coll.count())
            remaining -= cursor.count()

        return itertools.chain(*cursors)

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
