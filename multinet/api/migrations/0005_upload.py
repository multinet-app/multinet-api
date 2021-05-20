# Generated by Django 3.1.6 on 2021-05-20 16:11

from django.conf import settings
import django.contrib.postgres.fields
from django.db import migrations, models
import django.db.models.deletion
import django_extensions.db.fields
import s3_file_field.fields


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('api', '0004_network'),
    ]

    operations = [
        migrations.CreateModel(
            name='Upload',
            fields=[
                (
                    'id',
                    models.AutoField(
                        auto_created=True, primary_key=True, serialize=False, verbose_name='ID'
                    ),
                ),
                (
                    'created',
                    django_extensions.db.fields.CreationDateTimeField(
                        auto_now_add=True, verbose_name='created'
                    ),
                ),
                (
                    'modified',
                    django_extensions.db.fields.ModificationDateTimeField(
                        auto_now=True, verbose_name='modified'
                    ),
                ),
                ('blob', s3_file_field.fields.S3FileField()),
                (
                    'data_type',
                    models.CharField(
                        choices=[
                            ('CSV', 'Csv'),
                            ('D3_JSON', 'D3 Json'),
                            ('NESTED_JSON', 'Nested Json'),
                            ('NEWICK', 'Newick'),
                        ],
                        max_length=20,
                    ),
                ),
                (
                    'error_messages',
                    django.contrib.postgres.fields.ArrayField(
                        base_field=models.CharField(max_length=500),
                        blank=True,
                        null=True,
                        size=None,
                    ),
                ),
                (
                    'status',
                    models.CharField(
                        choices=[
                            ('PENDING', 'Pending'),
                            ('STARTED', 'Started'),
                            ('FAILED', 'Failed'),
                            ('FINISHED', 'Finished'),
                        ],
                        default='PENDING',
                        max_length=10,
                    ),
                ),
                (
                    'user',
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.SET_NULL,
                        related_name='uploads',
                        to=settings.AUTH_USER_MODEL,
                    ),
                ),
                (
                    'workspace',
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        related_name='uploads',
                        to='api.workspace',
                    ),
                ),
            ],
            options={
                'get_latest_by': 'modified',
                'abstract': False,
            },
        ),
    ]
