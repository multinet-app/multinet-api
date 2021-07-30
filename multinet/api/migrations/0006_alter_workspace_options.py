# Generated by Django 3.2.5 on 2021-07-23 15:43

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0005_upload'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='workspace',
            options={'ordering': ['id'], 'permissions': [('owner', 'Owns the workspace'), ('maintainer', 'May grant all roles but owner on the workspace'), ('writer', 'May write to and remove from the workspace'), ('reader', 'May read and perform non-mutating queries on the workspace'), ('read', 'Read from the workspace'), ('query', 'Access non-mutating AQL queries on the workspace'), ('write', 'Write new data and update existing data on the worksapce'), ('remove', 'Delete data on a workspace'), ('rename', 'Rename the workspace'), ('delete', 'Delete a workspace'), ('grant', 'Assign roles for a given workspace'), ('transfer', 'Transfer ownership of a given workspace')]},
        ),
    ]