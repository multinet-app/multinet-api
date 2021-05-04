from __future__ import annotations

from django.contrib.auth.models import User
from django.contrib.postgres.fields import ArrayField
from django.db import models
from django_extensions.db.models import TimeStampedModel
from s3_file_field import S3FileField

from .workspace import Workspace


class Upload(TimeStampedModel):
    """A generic upload object."""

    class DataType(models.TextChoices):
        CSV = 'CSV'
        D3_JSON = 'D3_JSON'
        NESTED_JSON = 'NESTED_JSON'
        NEWICK = 'NEWICK'

    class UploadStatus(models.TextChoices):
        PENDING = 'PENDING'
        STARTED = 'STARTED'
        FAILED = 'FAILED'
        FINISHED = 'FINISHED'

    blob = S3FileField()
    workspace = models.ForeignKey(Workspace, related_name='uploads', on_delete=models.CASCADE)
    user = models.ForeignKey(User, related_name='uploads', null=True, on_delete=models.SET_NULL)
    data_type = models.CharField(max_length=20, choices=DataType.choices)
    error_messages = ArrayField(models.CharField(max_length=500), null=True)
    status = models.CharField(
        max_length=10, choices=UploadStatus.choices, default=UploadStatus.PENDING
    )
