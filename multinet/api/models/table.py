from __future__ import annotations

from uuid import uuid4

from django.db import models
from django_extensions.db.models import TimeStampedModel

from multinet.api.utils.arango import get_or_create_db

from .workspace import Workspace


def create_default_arango_coll_name():
    # Arango db names must start with a letter
    return f'w-{uuid4().hex}'


class Table(TimeStampedModel):
    name = models.CharField(max_length=300)
    edge = models.BooleanField(default=False)
    workspace = models.ForeignKey(Workspace, related_name='tables', on_delete=models.CASCADE)

    # Max length of 34, since uuid hexes are 32, + 2 chars on the front
    arango_coll_name = models.CharField(
        max_length=34, unique=True, default=create_default_arango_coll_name
    )

    class Meta:
        unique_together = ('workspace', 'name')

    def save(self, *args, **kwargs):
        workspace: Workspace = self.workspace

        db = get_or_create_db(workspace.arango_db_name)
        if not db.has_collection(self.name):
            db.create_collection(self.name)

        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        workspace: Workspace = self.workspace

        db = get_or_create_db(workspace.arango_db_name)
        if db.has_collection(self.name):
            db.delete_collection(self.name)

        super().delete(*args, **kwargs)

    def __str__(self) -> str:
        return self.name
