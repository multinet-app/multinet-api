# Generated by Django 3.1.6 on 2021-02-17 18:55

from django.db import migrations, models
import django.db.models.deletion
import django_extensions.db.fields
import multinet.api.models.table
import multinet.api.models.workspace


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0002_workspace'),
    ]

    operations = [
        migrations.AlterField(
            model_name='workspace',
            name='arango_db_name',
            field=models.CharField(default=multinet.api.models.workspace.create_default_arango_db_name, max_length=34, unique=True),
        ),
        migrations.CreateModel(
            name='Table',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created', django_extensions.db.fields.CreationDateTimeField(auto_now_add=True, verbose_name='created')),
                ('modified', django_extensions.db.fields.ModificationDateTimeField(auto_now=True, verbose_name='modified')),
                ('name', models.CharField(max_length=300)),
                ('edge', models.BooleanField(default=False)),
                ('arango_coll_name', models.CharField(default=multinet.api.models.table.create_default_arango_coll_name, max_length=34, unique=True)),
                ('workspace', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='tables', to='api.workspace')),
            ],
            options={
                'unique_together': {('workspace', 'name')},
            },
        ),
    ]
