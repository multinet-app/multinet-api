from __future__ import annotations

from uuid import uuid4
from arango.database import StandardDatabase

from django.db.models import CharField
from django_extensions.db.models import TimeStampedModel
from guardian.shortcuts import assign_perm, get_users_with_perms, remove_perm

from multinet.api.utils.arango import ensure_db_created, ensure_db_deleted, get_or_create_db


def create_default_arango_db_name():
    # Arango db names must start with a letter
    return f'w-{uuid4().hex}'


class Workspace(TimeStampedModel):
    name = CharField(max_length=300, unique=True)

    # Max length of 34, since uuid hexes are 32, + 2 chars on the front
    arango_db_name = CharField(max_length=34, unique=True, default=create_default_arango_db_name)

    class Meta:
        ordering = ['id']
        permissions = [('owner', 'Owns the workspace')]

    @property
    def owners(self):
        return get_users_with_perms(self, only_with_perms_in=['owner'])

    def set_owners(self, new_owners):
        old_owners = get_users_with_perms(self, only_with_perms_in=['owner'])

        removed_owners = []
        added_owners = []

        # Remove old owners
        for old_owner in old_owners:
            if old_owner not in new_owners:
                remove_perm('owner', old_owner, self)
                removed_owners.append(old_owner)

        # Add new owners
        for new_owner in new_owners:
            if new_owner not in old_owners:
                assign_perm('owner', new_owner, self)
                added_owners.append(new_owner)

        # Return the owners added/removed so they can be emailed
        return removed_owners, added_owners

    def add_owner(self, new_owner):
        old_owners = get_users_with_perms(self, only_with_perms_in=['owner'])
        if new_owner not in old_owners:
            assign_perm('owner', new_owner, self)

    def remove_owner(self, owner):
        owners = get_users_with_perms(self, only_with_perms_in=['owner'])
        if owner in owners:
            remove_perm('owner', owner, self)

    def save(self, *args, **kwargs):
        ensure_db_created(self.arango_db_name)
        super().save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        ensure_db_deleted(self.arango_db_name)
        super().delete(*args, **kwargs)

    def get_arango_db(self) -> StandardDatabase:
        return get_or_create_db(self.arango_db_name)

    def __str__(self) -> str:
        return self.name
