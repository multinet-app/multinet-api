from __future__ import annotations

from typing import Type
from uuid import uuid4

from arango.database import StandardDatabase
from django.contrib.auth.models import User
from django.db import models
from django.db.models.signals import post_delete, pre_save
from django.dispatch import receiver
from django_extensions.db.models import TimeStampedModel

from multinet.api.utils.arango import ensure_db_created, ensure_db_deleted, get_or_create_db


def create_default_arango_db_name():
    # Arango db names must start with a letter
    return f'w-{uuid4().hex}'


class WorkspaceRole(models.IntegerChoices):
    READER = 1
    WRITER = 2
    MAINTAINER = 3
    OWNER = 4


class WorkspacePermission(TimeStampedModel):
    # Use model name instead of directly, to avoid circular reference
    workspace = models.ForeignKey('api.Workspace', on_delete=models.CASCADE)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    role = models.PositiveSmallIntegerField(choices=WorkspaceRole.choices)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['workspace', 'user'], name='unique_workspace_permission'
            ),
            models.UniqueConstraint(
                fields=['workspace'],
                condition=models.Q(role=WorkspaceRole.OWNER),
                name='single_workspace_owner',
            ),
        ]


class Workspace(TimeStampedModel):
    name = models.CharField(max_length=300, unique=True)
    public = models.BooleanField(default=False)

    # Max length of 34, since uuid hexes are 32, + 2 chars on the front
    arango_db_name = models.CharField(
        max_length=34, unique=True, default=create_default_arango_db_name
    )

    class Meta:
        ordering = ['id']

    @property
    def owner(self):
        """Return the single workspace owner."""
        perm = WorkspacePermission.objects.get(workspace=self.id, role=WorkspaceRole.OWNER)

        if perm is not None:
            return perm.user

        return None

    @property
    def maintainers(self):
        return WorkspacePermission.objects.filter(role=WorkspaceRole.MAINTAINER)

    @property
    def writers(self):
        return WorkspacePermission.objects.filter(role=WorkspaceRole.WRITER)

    @property
    def readers(self):
        return WorkspacePermission.objects.filter(role=WorkspaceRole.READER)

    def get_user_permission(self, user: User) -> WorkspaceRole:
        """Get the permission for a given user on this workspace if they have one, or None."""
        perm = WorkspacePermission.objects.filter(user=user).first()
        return perm.role if perm is not None else None

    def set_user_permission(self, user: User, permission: WorkspaceRole):
        WorkspacePermission.objects.create(workspace=self.id, user=user, role=permission)

    def set_permissions(self, perm: WorkspacePermission, new_users: list):
        old_users = get_users_with_perms(self, only_with_perms_in=[perm.name])

        removed_users = []
        added_users = []

        # remove old users
        for user in old_users:
            if user not in new_users:
                remove_perm(perm.name, user, self)
                removed_users.append(user)

        # add new users
        for user in new_users:
            was_added = self.set_user_permission(user, perm)
            if was_added:
                added_users.append(user)

        # return added/removed users so they can be emailed
        return removed_users, added_users

    def set_owner(self, new_owner: User):
        """
        Set owner for this workspace, removing the current owner's permission.
        Note that this should be the only way ownership for a workspace is set.
        """
        current: WorkspacePermission = WorkspacePermission.objects.filter(workspace=self.id).first()
        if current is not None:
            current.delete()

        WorkspacePermission.objects.create(workspace=self, user=new_owner, role=WorkspaceRole.OWNER)

    def set_maintainers(self, new_maintainers):
        return self.set_permissions(WorkspaceRole.MAINTAINER, new_maintainers)

    def set_writers(self, new_writers):
        return self.set_permissions(WorkspaceRole.WRITER, new_writers)

    def set_readers(self, new_readers):
        return self.set_permissions(WorkspaceRole.READER, new_readers)

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
