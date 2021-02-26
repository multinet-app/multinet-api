from __future__ import annotations

from django.db import models
from django_extensions.db.models import TimeStampedModel

from .workspace import Workspace


class Graph(TimeStampedModel):
    name = models.CharField(max_length=300)
    workspace = models.ForeignKey(Workspace, related_name='graphs', on_delete=models.CASCADE)

    class Meta:
        unique_together = ('workspace', 'name')

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
