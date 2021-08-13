# Generated by Django 3.2.5 on 2021-08-10 20:17

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django_extensions.db.fields


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('api', '0006_auto_20210805_1709'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='workspace',
            options={'ordering': ['id']},
        ),
        migrations.AddField(
            model_name='workspace',
            name='owner',
            field=models.ForeignKey(
                default=1, on_delete=django.db.models.deletion.PROTECT, to='auth.user'
            ),
            preserve_default=False,
        ),
        migrations.CreateModel(
            name='WorkspaceRole',
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
                (
                    'role',
                    models.PositiveSmallIntegerField(
                        choices=[(1, 'Reader'), (2, 'Writer'), (3, 'Maintainer')]
                    ),
                ),
                (
                    'user',
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL
                    ),
                ),
                (
                    'workspace',
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE, to='api.workspace'
                    ),
                ),
            ],
        ),
        migrations.AddConstraint(
            model_name='workspacerole',
            constraint=models.UniqueConstraint(
                fields=('workspace', 'user'), name='unique_workspace_permission'
            ),
        ),
    ]