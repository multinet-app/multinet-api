from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Type

from arango.collection import StandardCollection
from arango.cursor import Cursor
from arango.exceptions import DocumentDeleteError, DocumentInsertError
from django.db import models
from django.db.models.signals import post_delete, pre_save
from django.dispatch.dispatcher import receiver
from django_extensions.db.models import TimeStampedModel
from more_itertools import chunked

from .workspace import Workspace

# The max number of documents that should be sent in bulk requests
DOCUMENT_CHUNK_SIZE = 5000


@dataclass
class RowModifyError:
    index: int
    message: str


@dataclass
class RowInsertionResponse:
    inserted: int
    errors: List[RowModifyError]


@dataclass
class RowDeletionResponse:
    deleted: int
    errors: List[RowModifyError]


class Table(TimeStampedModel):
    name = models.CharField(max_length=300)
    edge = models.BooleanField(default=False)
    workspace = models.ForeignKey(Workspace, related_name='tables', on_delete=models.CASCADE)

    class Meta:
        unique_together = ('workspace', 'name')

    def count(self) -> int:
        return self.get_arango_collection().count()

    def get_arango_collection(self, readonly=True) -> StandardCollection:
        workspace: Workspace = self.workspace
        return workspace.get_arango_db(readonly=readonly).collection(self.name)

    def get_row(self, query: Optional[Dict] = None) -> Cursor:
        """Return a specific document."""
        return self.get_arango_collection().find(query or {}, skip=None, limit=1)

    def get_rows(self, limit: Optional[int] = None, offset: Optional[int] = None) -> Cursor:
        """Return all documents from the arango collection for this table.."""
        coll = self.get_arango_collection()
        return coll.find({}, limit=limit, skip=offset)

    def put_rows(self, rows: List[Dict]) -> RowInsertionResponse:
        """Insert/update rows in the underlying arangodb collection."""
        errors = []

        # Limit the amount of rows inserted per request, to prevent timeouts
        for chunk in chunked(rows, DOCUMENT_CHUNK_SIZE):
            res = self.get_arango_collection(readonly=False).insert_many(chunk, overwrite=True)
            errors.extend(
                (
                    RowModifyError(index=i, message=doc.error_message)
                    for i, doc in enumerate(res)
                    if isinstance(doc, DocumentInsertError)
                )
            )

        inserted = len(rows) - len(errors)
        return RowInsertionResponse(inserted=inserted, errors=errors)

    def delete_rows(self, rows: List[Dict]) -> RowDeletionResponse:
        """Delete rows in the underlying arangodb collection."""
        errors = []

        # Limit the amount of rows deleted per request, to prevent timeouts
        for chunk in chunked(rows, DOCUMENT_CHUNK_SIZE):
            res = self.get_arango_collection(readonly=False).delete_many(chunk)
            errors.extend(
                (
                    RowModifyError(index=i, message=doc.error_message)
                    for i, doc in enumerate(res)
                    if isinstance(doc, DocumentDeleteError)
                )
            )

        deleted = len(rows) - len(errors)
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

    @property
    def type_annotations(self):
        return TableTypeAnnotation.objects.filter(table=self)


class TableTypeAnnotation(TimeStampedModel):
    class Type(models.TextChoices):
        PRIMARY = 'primary key'
        SOURCE = 'edge source'
        TARGET = 'edge target'
        LABEL = 'label'
        STRING = 'string'
        BOOLEAN = 'boolean'
        CATEGORY = 'category'
        NUMBER = 'number'
        DATE = 'date'
        IGNORED = 'ignored'

    table = models.ForeignKey(Table, related_name='type_annotations', on_delete=models.CASCADE)
    column = models.CharField(max_length=255)
    type = models.CharField(choices=Type.choices, max_length=16)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['table', 'column'], name='unique_column_type')
        ]


# Handle arango sync
@receiver(pre_save, sender=Table)
def arango_coll_save(sender: Type[Table], instance: Table, **kwargs):
    workspace: Workspace = instance.workspace

    db = workspace.get_arango_db(readonly=False)
    if not db.has_collection(instance.name):
        db.create_collection(instance.name, edge=instance.edge)


@receiver(post_delete, sender=Table)
def arango_coll_delete(sender: Type[Table], instance: Table, **kwargs):
    workspace: Workspace = instance.workspace

    db = workspace.get_arango_db(readonly=False)
    if db.has_collection(instance.name):
        db.delete_collection(instance.name)
