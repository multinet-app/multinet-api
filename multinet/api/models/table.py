from __future__ import annotations

from typing import Dict, List, Set, Optional, Tuple, Union
from arango.collection import StandardCollection
from arango.exceptions import DocumentInsertError

from arango.cursor import Cursor
from django.db import models
from django_extensions.db.models import TimeStampedModel

from .graph import Graph
from .workspace import Workspace


class Table(TimeStampedModel):
    name = models.CharField(max_length=300)
    edge = models.BooleanField(default=False)
    workspace = models.ForeignKey(Workspace, related_name='tables', on_delete=models.CASCADE)
    graphs = models.ManyToManyField(Graph)

    class Meta:
        unique_together = ('workspace', 'name')

    def save(self, *args, **kwargs):
        workspace: Workspace = self.workspace

        db = workspace.get_arango_db()
        if not db.has_collection(self.name):
            db.create_collection(self.name, edge=self.edge)

        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        workspace: Workspace = self.workspace

        db = workspace.get_arango_db()
        if db.has_collection(self.name):
            db.delete_collection(self.name)

        super().delete(*args, **kwargs)

    def count(self) -> int:
        return self.get_arango_collection().count()

    def get_arango_collection(self) -> StandardCollection:
        workspace: Workspace = self.workspace
        return workspace.get_arango_db().collection(self.name)

    def get_row(self, query: Optional[Dict] = None) -> Cursor:
        return self.get_arango_collection().find(query or {}, skip=None, limit=1)

    def get_rows(
        self, page: Optional[int] = None, page_size: Optional[int] = None
    ) -> Tuple[Cursor, int]:
        """Return a tuple containing the Cursor and the total doc count."""
        skip = None
        if page and page_size:
            skip = (page - 1) * page_size

        coll = self.get_arango_collection()
        return (coll.find({}, skip, page_size), coll.count())

    def put_rows(self, rows: List[Dict]) -> Tuple[List[Dict], List[Dict[str, Union[int, str]]]]:
        """Insert/update rows in the underlying arangodb collection."""
        res = self.get_arango_collection().insert_many(rows, overwrite=True, return_new=True)

        results = []
        errors: List[Dict[str, Union[int, str]]] = []

        for i, doc in enumerate(res):
            if isinstance(doc, DocumentInsertError):
                errors.append({'index': i, 'message': doc.error_message})
            else:
                results.append(doc['new'])

        return (results, errors)

    def delete_rows(self, rows: List[Dict]) -> Cursor:
        """Delete rows in the underlying arangodb collection."""
        self.get_arango_collection().delete_many(rows)
        return True

    def __str__(self) -> str:
        return self.name
