# Generated by Django 3.2.7 on 2021-09-27 18:56

from django.db import migrations, models
import django.db.models.deletion
import django_extensions.db.fields
import multinet.api.common_types


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0010_aqlquery'),
    ]

    operations = [
        migrations.CreateModel(
            name='TableTypeAnnotation',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('created', django_extensions.db.fields.CreationDateTimeField(auto_now_add=True, verbose_name='created')),
                ('modified', django_extensions.db.fields.ModificationDateTimeField(auto_now=True, verbose_name='modified')),
                ('column', models.CharField(max_length=255)),
                ('type', models.CharField(choices=[(multinet.api.common_types.ColumnTypeEnum['LABEL'], 'label'), (multinet.api.common_types.ColumnTypeEnum['BOOLEAN'], 'boolean'), (multinet.api.common_types.ColumnTypeEnum['CATEGORY'], 'category'), (multinet.api.common_types.ColumnTypeEnum['NUMBER'], 'number'), (multinet.api.common_types.ColumnTypeEnum['DATE'], 'date')], max_length=16)),
                ('table', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='type_annotations', to='api.table')),
            ],
        ),
        migrations.AddConstraint(
            model_name='tabletypeannotation',
            constraint=models.UniqueConstraint(fields=('table', 'column'), name='unique_column_type'),
        ),
    ]
