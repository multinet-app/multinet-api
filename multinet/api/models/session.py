from django.db import models
from django_extensions.db.models import TimeStampedModel

from .network import Network


class Session(TimeStampedModel):
    name = models.CharField(max_length=300)
    network = models.ForeignKey(Network, on_delete=models.CASCADE)
    state = models.JSONField()
