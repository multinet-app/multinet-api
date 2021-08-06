from __future__ import annotations

from typing import Type, Union
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


class WorkspaceRoleChoice(models.IntegerChoices):
    READER = 1
    WRITER = 2
    MAINTAINER = 3
    OWNER = 4


class WorkspaceRole(TimeStampedModel):
    workspace = models.ForeignKey('api.Workspace', on_delete=models.CASCADE)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    role = models.PositiveSmallIntegerField(choices=WorkspaceRoleChoice.choices)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['workspace', 'user'], name='unique_workspace_permission'
            ),
            models.UniqueConstraint(
                fields=['workspace'],
                condition=models.Q(role=WorkspaceRoleChoice.OWNER),
                name='singe_workspace_owner',
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
        """Return the workspace owner."""
        owner_permission: WorkspaceRole = WorkspaceRole.objects.filter(
            workspace=self.pk, role=WorkspaceRoleChoice.OWNER
        ).first()
        if owner_permission is not None:
            return owner_permission.user
        return None

    @property
    def maintainers(self):
        return [
            role.user
            for role in WorkspaceRole.objects.filter(
                workspace=self.pk, role=WorkspaceRoleChoice.MAINTAINER
            )
        ]

    @property
    def writers(self):
        return [
            role.user
            for role in WorkspaceRole.objects.filter(
                workspace=self.pk, role=WorkspaceRoleChoice.WRITER
            )
        ]

    @property
    def readers(self):
        return [
            role.user
            for role in WorkspaceRole.objects.filter(
                workspace=self.pk, role=WorkspaceRoleChoice.READER
            )
        ]

    def get_user_permission(self, user: User) -> Union[WorkspaceRole | None]:
        """Get the WorkspaceRole for a given user on this workspace."""
        return WorkspaceRole.objects.filter(workspace=self.pk, user=user.pk).first()

    def set_user_permission(self, user: User, permission: WorkspaceRoleChoice) -> bool:
        """
        Set a user role for this workspace.

        This should be the only way object roles are set.
        """
        current_role = WorkspaceRole.objects.filter(workspace=self, user=user).first()
        if current_role is None:
            WorkspaceRole.objects.create(workspace=self, user=user, role=permission)
        else:
            current_role.role = permission
            current_role.save()

    def set_permission(self, permission: WorkspaceRoleChoice, new_users: list):
        current_roles = WorkspaceRole.objects.filter(workspace=self)

        for role in current_roles:
            user = role.user
            current_permission = role.role
            if user in new_users:
                if role.role != permission:
                    role.role = permission
                    role.save()
                new_users.remove(user)
            elif current_permission == permission:
                role.delete()

        for user in new_users:
            WorkspaceRole.objects.create(workspace=self, user=user, role=permission)

    def set_owner(self, new_owner):
        """
        Set owner for this workspace.

        Removes current owner's owner permission as a side effect. This should be the only
        way ownership for a workspace is set. Returns the tuple (old_owner, new_owner).
        """
        current_owner_permission: WorkspaceRole = WorkspaceRole.objects.filter(
            workspace=self.pk, role=WorkspaceRoleChoice.OWNER
        ).first()
        old_owner = None

        if current_owner_permission is not None:
            old_owner = current_owner_permission.user
            current_owner_permission.delete()
        WorkspaceRole.objects.create(workspace=self, user=new_owner, role=WorkspaceRoleChoice.OWNER)
        return old_owner, new_owner

    def set_maintainers(self, new_maintainers):
        return self.set_permission(WorkspaceRoleChoice.MAINTAINER, new_maintainers)

    def set_writers(self, new_writers):
        return self.set_permission(WorkspaceRoleChoice.WRITER, new_writers)

    def set_readers(self, new_readers):
        return self.set_permission(WorkspaceRoleChoice.READER, new_readers)

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
