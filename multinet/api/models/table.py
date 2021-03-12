from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from arango.collection import StandardCollection
from arango.cursor import Cursor
from arango.exceptions import DocumentDeleteError, DocumentInsertError
from django.db import models
from django_extensions.db.models import TimeStampedModel

from .workspace import Workspace


@dataclass
class RowModifyError:
    index: int
    message: str


@dataclass
class RowInsertionResponse:
    inserted: List[Dict]
    errors: List[RowModifyError]


@dataclass
class RowDeletionResponse:
    deleted: List[Dict]
    errors: List[RowModifyError]


class Table(TimeStampedModel):
    name = models.CharField(max_length=300)
    edge = models.BooleanField(default=False)
    workspace = models.ForeignKey(Workspace, related_name='tables', on_delete=models.CASCADE)

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
        """Return a specific document."""
        return self.get_arango_collection().find(query or {}, skip=None, limit=1)

    def get_rows(self, page: Optional[int] = None, page_size: Optional[int] = None) -> Cursor:
        """Return all documents from the arango collection for this table.."""
        skip = None
        if page and page_size:
            skip = (page - 1) * page_size

        coll = self.get_arango_collection()
        return coll.find({}, skip, page_size)

    def put_rows(self, rows: List[Dict]) -> RowInsertionResponse:
        """Insert/update rows in the underlying arangodb collection."""
        res = self.get_arango_collection().insert_many(rows, overwrite=True, return_new=True)

        inserted = []
        errors = []

        for i, doc in enumerate(res):
            if isinstance(doc, DocumentInsertError):
                errors.append(RowModifyError(index=i, message=doc.error_message))
            else:
                inserted.append(doc['new'])

        return RowInsertionResponse(inserted=inserted, errors=errors)

    def delete_rows(self, rows: List[Dict]) -> RowDeletionResponse:
        """Delete rows in the underlying arangodb collection."""
        res = self.get_arango_collection().delete_many(rows, return_old=True)

        deleted = []
        errors = []

        for i, doc in enumerate(res):
            if isinstance(doc, DocumentDeleteError):
                errors.append(RowModifyError(index=i, message=doc.error_message))
            else:
                deleted.append(doc['old'])

        return RowDeletionResponse(deleted=deleted, errors=errors)

    def find_referenced_node_tables(self) -> Optional[Dict[str, Set[str]]]:
        """
        Return a dict which represents the node tables referenced by an edge table.

        Each key in this dict is the name of a node table referenced by this edge table.
        Each value is a set of the keys, within the respective node table, that are
        referenced by this edge table.

        If this function is called on a node table, it returns an empty dict.
        """
        if not self.edge:
            return None

        referenced: Dict[str] = {}
        rows = self.get_rows()
        for row in rows:
            _from = row.get('_from')
            _to = row.get('_to')

            for end in (_from, _to):
                if end is None:
                    # Ignore missing reference
                    continue

                end_split = end.split('/')
                if len(end_split) != 2:
                    # Ignore malformed reference
                    continue

                table, key = end_split
                if referenced.get(table) is None:
                    referenced[table] = set()

                if key:
                    referenced[table].add(key)

        return referenced

    def __str__(self) -> str:
        return self.name
