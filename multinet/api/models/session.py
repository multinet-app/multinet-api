from django.db import models
from django.db.models import CheckConstraint, Q
from django_extensions.db.models import TimeStampedModel

from .network import Network
from .table import Table


class Session(TimeStampedModel):
    name = models.CharField(max_length=300)

    # Exactly one of these will be non-null (see check constraint below).
    network = models.ForeignKey(Network, null=True, on_delete=models.CASCADE)
    table = models.ForeignKey(Table, null=True, on_delete=models.CASCADE)

    state = models.JSONField()

    class Meta:
        constraints = [
            CheckConstraint(
                name='network_xor_table',
                check=Q(network__isnull=True, table__isnull=False)
                | Q(network__isnull=False, table__isnull=True),
            )
        ]
