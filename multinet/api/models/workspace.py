from __future__ import annotations

from typing import List, Optional, Type
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

    def get_client_name(self):
        if self.value == self.READER:
            return 'reader'
        elif self.value == self.WRITER:
            return 'writer'
        elif self.value == self.MAINTAINER:
            return 'maintainer'


class WorkspaceRole(TimeStampedModel):
    workspace = models.ForeignKey('api.Workspace', on_delete=models.CASCADE)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    role = models.PositiveSmallIntegerField(choices=WorkspaceRoleChoice.choices)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['workspace', 'user'], name='unique_workspace_permission'
            ),
        ]


class Workspace(TimeStampedModel):
    name = models.CharField(max_length=300, unique=True)
    public = models.BooleanField(default=False)
    owner = models.ForeignKey(User, on_delete=models.CASCADE)

    # Max length of 34, since uuid hexes are 32, + 2 chars on the front
    arango_db_name = models.CharField(
        max_length=34, unique=True, default=create_default_arango_db_name
    )

    class Meta:
        ordering = ['id']

    @property
    def maintainers(self):
        return [
            role.user
            for role in WorkspaceRole.objects.select_related('user').filter(
                workspace=self.pk, role=WorkspaceRoleChoice.MAINTAINER
            )
        ]

    @property
    def writers(self):
        return [
            role.user
            for role in WorkspaceRole.objects.select_related('user').filter(
                workspace=self.pk, role=WorkspaceRoleChoice.WRITER
            )
        ]

    @property
    def readers(self):
        return [
            role.user
            for role in WorkspaceRole.objects.select_related('user').filter(
                workspace=self.pk, role=WorkspaceRoleChoice.READER
            )
        ]

    def get_user_permission(self, user: User) -> Optional[WorkspaceRole]:
        """Get the WorkspaceRole for a given user on this workspace."""
        return WorkspaceRole.objects.filter(workspace=self.pk, user=user.pk).first()

    def get_user_permission_string(self, user: User) -> str:
        """Get a string representation of a user's workspace role."""
        if self.owner == user:
            return 'owner'

        workspace_role = self.get_user_permission(user)
        if workspace_role is None:
            if self.public:
                return 'reader'
            return ''
        else:
            return WorkspaceRoleChoice(workspace_role.role).get_client_name()

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

    def set_owner(self, new_owner):
        """
        Set owner for this workspace, replacing the current owner.

        If the new owner has some other permission for the workspace, e.g.
        writer, the corresponding WorkspaceRole object is deleted, as ownership
        encompasses all other roles.
        """
        # Delete existing WorkspaceRole for the new owner, if it exists
        WorkspaceRole.objects.filter(workspace=self.pk, user=new_owner).delete()
        self.owner = new_owner
        self.save()

    def set_user_permissions_bulk(
        self, readers: List[User], writers: List[User], maintainers: List[User]
    ):
        """Replace all existing permissions on this workspace."""
        WorkspaceRole.objects.filter(workspace=self).delete()

        new_reader_roles = [
            WorkspaceRole(workspace=self, user=user, role=WorkspaceRoleChoice.READER)
            for user in readers
        ]
        new_writer_roles = [
            WorkspaceRole(workspace=self, user=user, role=WorkspaceRoleChoice.WRITER)
            for user in writers
        ]
        new_maintainer_roles = [
            WorkspaceRole(workspace=self, user=user, role=WorkspaceRoleChoice.MAINTAINER)
            for user in maintainers
        ]

        # Create all new WorkspaceRole objects in one go
        WorkspaceRole.objects.bulk_create(
            [*new_reader_roles, *new_writer_roles, *new_maintainer_roles]
        )

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
