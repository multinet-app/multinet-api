# Generated by Django 3.2.6 on 2021-08-11 18:07

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0007_auto_20210810_2017'),
    ]

    operations = [
        migrations.AddField(
            model_name='table',
            name='metadata',
            field=models.JSONField(null=True),
        ),
    ]
