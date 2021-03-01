from __future__ import annotations

import itertools
from typing import Iterable, List, Optional

from arango.collection import StandardCollection
from arango.cursor import Cursor
from arango.database import StandardDatabase
from arango.graph import Graph as ArangoGraph
from django.db import models
from django_extensions.db.models import TimeStampedModel

from .workspace import Workspace


class Graph(TimeStampedModel):
    name = models.CharField(max_length=300)
    workspace = models.ForeignKey(Workspace, related_name='graphs', on_delete=models.CASCADE)

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

    def get_arango_graph(self) -> ArangoGraph:
        workspace: Workspace = self.workspace
        return workspace.get_arango_db().graph(self.name)

    def _chained_collections_find(
        self, collections: List[str], page: Optional[int] = None, page_size: Optional[int] = None
    ) -> Iterable:
        """Chains document retreival across several collections, with pagination."""
        db: StandardDatabase = self.workspace.get_arango_db()

        skip = 0
        if page and page_size:
            skip = (page - 1) * page_size

        cursors: List[Cursor] = []
        remaining = page_size

        for coll_name in collections:
            if remaining == 0:
                break

            coll: StandardCollection = db.collection(coll_name)
            cursor: Cursor = coll.find({}, skip=skip, limit=remaining)
            cursors.append(cursor)

            skip -= min(skip, coll.count())
            remaining -= cursor.count()

        return itertools.chain(*cursors)

    def nodes(self, page: Optional[int] = None, page_size: Optional[int] = None) -> Iterable:
        return self._chained_collections_find(
            self.get_arango_graph().vertex_collections(), page, page_size
        )

    def edges(self, page: Optional[int] = None, page_size: Optional[int] = None) -> Cursor:
        edge_collections = [
            edge_def['edge_collection'] for edge_def in self.get_arango_graph().edge_definitions()
        ]
        return self._chained_collections_find(edge_collections, page, page_size)

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
