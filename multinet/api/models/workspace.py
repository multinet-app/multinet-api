from __future__ import annotations

from typing import Type
from uuid import uuid4

from arango.database import StandardDatabase
from django.contrib.auth.models import User
from django.db.models import BooleanField, CharField
from django.db.models.signals import post_delete, pre_save
from django.dispatch import receiver
from django_extensions.db.models import TimeStampedModel
from guardian.shortcuts import assign_perm, get_user_perms, get_users_with_perms, remove_perm

from multinet.api.utils.arango import ensure_db_created, ensure_db_deleted, get_or_create_db
from multinet.api.utils.workspace_permissions import WorkspacePermission


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
            (WorkspacePermission.owner.value, 'Owns the workspace'),
            (WorkspacePermission.maintainer.value, 'Grants roles except owner on the workspace'),
            (WorkspacePermission.reader.value, 'Creates and deletes data on the workspace'),
            (WorkspacePermission.writer.value, 'Views data on the workspace'),
        ]

    @property
    def owners(self):
        return get_users_with_perms(self, only_with_perms_in=[WorkspacePermission.owner.value])

    @property
    def maintainers(self):
        return get_users_with_perms(self, only_with_perms_in=[WorkspacePermission.maintainer.value])

    @property
    def writers(self):
        return get_users_with_perms(self, only_with_perms_in=[WorkspacePermission.writer.value])

    @property
    def readers(self):
        return get_users_with_perms(self, only_with_perms_in=[WorkspacePermission.reader.value])

    def get_user_permission(self, user: User) -> WorkspacePermission:
        """
        Get the object-level permission for a given user on this workspace.
        In the event that there are more than one (not ideal), return the highest
        ranking permission.
        Return None if the user has no permission for this workspace.
        """
        permission_keys = get_user_perms(user, self)

        valid_permission_keys = filter(lambda key: WorkspacePermission.get_rank_from_key(key) > 0,
                                       permission_keys)
        valid_permission_keys = list(valid_permission_keys)

        if len(valid_permission_keys) == 0:
            return None

        permission_key = max(valid_permission_keys, key=WorkspacePermission.get_rank_from_key)
        return WorkspacePermission(permission_key)

    def set_user_permission(self, user: User, permission: WorkspacePermission) -> bool:
        """
        Wrapper for django-guardian's assing_perm. Set a user permission for this workspace.
        This should be the only way object permissions are set. This ensures that a user only
        has one permission for the workspace.

        Returns True if the permission was added, False if there was no need to add the permission.
        """
        need_to_add = True  # assume we will add the permission
        current_permissions = get_user_perms(user, self)

        permission_level_codenames = WorkspacePermission.get_permission_codenames()
        for p in current_permissions:
            if p == permission.value:
                need_to_add = False  # no need to add, since the permission already exists
            elif p in permission_level_codenames:
                # for our defined permissions (owner, maintainer, writer, reader),
                # ensure the user only has one
                remove_perm(p, user, self)

        if need_to_add:
            assign_perm(permission.value, user, self)
        return need_to_add

    def set_permissions(self, perm: WorkspacePermission, new_users: list):
        old_users = get_users_with_perms(self, only_with_perms_in=[perm.value])

        removed_users = []
        added_users = []

        # remove old users
        for user in old_users:
            if user not in new_users:
                remove_perm(perm.value, user, self)
                removed_users.append(user)

        # add new users
        for user in new_users:
            was_added = self.set_user_permission(user, WorkspacePermission(perm))
            if was_added:
                added_users.append(user)

        # return added/removed users so they can be emailed
        return removed_users, added_users

    def set_owners(self, new_owners):
        return self.set_permissions(WorkspacePermission.owner, new_owners)

    def set_maintainers(self, new_maintainers):
        return self.set_permissions(WorkspacePermission.maintainer, new_maintainers)

    def set_writers(self, new_writers):
        return self.set_permissions(WorkspacePermission.writer, new_writers)

    def set_readers(self, new_readers):
        return self.set_permissions(WorkspacePermission.reader, new_readers)

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
