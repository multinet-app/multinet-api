from __future__ import annotations

from typing import Type
from uuid import uuid4

# from django.db.models.fields import BooleanField

from arango.database import StandardDatabase
from django.db.models import CharField, BooleanField
from django.db.models.signals import post_delete, pre_save
from django.dispatch import receiver
from django_extensions.db.models import TimeStampedModel
from guardian.shortcuts import assign_perm, get_users_with_perms, remove_perm, get_user_perms

from multinet.api.utils.arango import ensure_db_created, ensure_db_deleted, get_or_create_db
from multinet.api.utils.workspace_permissions import OWNER, MAINTAINER, WRITER, READER


def create_default_arango_db_name():
    # Arango db names must start with a letter
    return f'w-{uuid4().hex}'


class Workspace(TimeStampedModel):
    name = CharField(max_length=300, unique=True)
    public = BooleanField(default=False)

    # Max length of 34, since uuid hexes are 32, + 2 chars on the front
    arango_db_name = CharField(max_length=34, unique=True, default=create_default_arango_db_name)

    class Meta:
        ordering = ['id']
        permissions = [
            (OWNER, 'Owns the workspace'),
            (MAINTAINER, 'May grant all roles but owner on the workspace'),
            (WRITER, 'May write to and remove from the workspace'),
            (READER, 'May read and perform non-mutating queries on the workspace'),
        ]

    @property
    def owners(self):
        return get_users_with_perms(self, only_with_perms_in=[OWNER])

    @property
    def maintainers(self):
        return get_users_with_perms(self, only_with_perms_in=[MAINTAINER])

    @property
    def writers(self):
        return get_users_with_perms(self, only_with_perms_in=[WRITER])

    @property
    def readers(self):
        return get_users_with_perms(self, only_with_perms_in=[READER])

    def set_permissions(self, perm, new_users):
        old_users = get_users_with_perms(self, only_with_perms_in=[perm])

        removed_users = []
        added_users = []

        # remove old users
        for user in old_users:
            if user not in new_users:
                remove_perm(perm, user, self)
                removed_users.append(user)

        # add new users
        for user in new_users:
            if user not in old_users:
                assign_perm(perm, user, self)
                added_users.append(user)

        # return added/removed users so they can be emailed
        return removed_users, added_users

    def set_owners(self, new_owners):
        return self.set_permissions(OWNER, new_owners)

    def set_maintainers(self, new_maintainers):
        return self.set_permissions(MAINTAINER, new_maintainers)

    def set_writers(self, new_writers):
        return self.set_permissions(WRITER, new_writers)

    def set_readers(self, new_readers):
        return self.set_permissions(READER, new_readers)

    def add_user_permission(self, perm, user):
        current_users = get_user_perms(self, only_with_perms_in=[perm])
        if user not in current_users:
            assign_perm(perm, user, self)

    def remove_user_permission(self, perm, user):
        current_users = get_users_with_perms(self, only_with_perms_in=[perm])
        if user not in current_users:
            remove_perm(perm, user, self)

    def get_arango_db(self) -> StandardDatabase:
        return get_or_create_db(self.arango_db_name)

    def __str__(self) -> str:
        return self.name


# Handle arango sync
@receiver(pre_save, sender=Workspace)
def arango_db_save(sender: Type[Workspace], instance: Workspace, **kwargs):
    ensure_db_created(instance.arango_db_name)


@receiver(post_delete, sender=Workspace)
def arango_db_delete(sender: Type[Workspace], instance: Workspace, **kwargs):
    ensure_db_deleted(instance.arango_db_name)
