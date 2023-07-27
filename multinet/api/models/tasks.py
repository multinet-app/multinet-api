from __future__ import annotations

from django.contrib.auth.models import User
from django.contrib.postgres.fields import ArrayField
from django.db import models
from django_extensions.db.models import TimeStampedModel
from s3_file_field import S3FileField

from .workspace import Workspace


class Task(TimeStampedModel):
    """A generic task object."""

    class Meta:
        abstract = True

    class Status(models.TextChoices):
        PENDING = 'PENDING'
        STARTED = 'STARTED'
        FAILED = 'FAILED'
        FINISHED = 'FINISHED'

    workspace = models.ForeignKey(Workspace, related_name='%(class)ss', on_delete=models.CASCADE)
    user = models.ForeignKey(User, related_name='%(class)ss', null=True, on_delete=models.SET_NULL)
    error_messages = ArrayField(models.CharField(max_length=500), null=True, blank=True)
    status = models.CharField(max_length=10, choices=Status.choices, default=Status.PENDING)


class Upload(Task):
    """An object to track uploads."""

    class DataType(models.TextChoices):
        CSV = 'CSV'
        JSON_TABLE = 'JSON_TABLE'
        JSON_NETWORK = 'JSON_NETWORK'

    blob = S3FileField()
    data_type = models.CharField(max_length=20, choices=DataType.choices)


class AqlQuery(Task):
    """An object to track AQL queries."""

    query = models.TextField()
    bind_vars = models.JSONField(blank=True, default=dict)
    results = models.JSONField(blank=True, null=True)
