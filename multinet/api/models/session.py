from django.db import models
from django_extensions.db.models import TimeStampedModel

from .network import Network
from .table import Table


class Session(TimeStampedModel):
    name = models.CharField(max_length=300)

    visapp = models.CharField(max_length=64)
    state = models.JSONField()

    class Meta:
        abstract = True


class TableSession(Session):
    table = models.ForeignKey(Table, on_delete=models.CASCADE)


class NetworkSession(Session):
    network = models.ForeignKey(Network, on_delete=models.CASCADE)
