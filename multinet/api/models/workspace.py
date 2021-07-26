from __future__ import annotations

from typing import Type
from uuid import uuid4
from enum import Enum

from django.db.models.fields import BooleanField

from arango.database import StandardDatabase
from django.db.models import CharField
from django.db.models.signals import post_delete, pre_save
from django.dispatch import receiver
from django_extensions.db.models import TimeStampedModel
from guardian.shortcuts import assign_perm, get_users_with_perms, remove_perm, get_user_perms

from multinet.api.utils.arango import ensure_db_created, ensure_db_deleted, get_or_create_db


def create_default_arango_db_name():
    # Arango db names must start with a letter
    return f'w-{uuid4().hex}'

"""
Enum class of roles (see rfc-0005). Roles determine user permissions.

Evaluate w
"""
class PermissionLevel(Enum):
    READER = 1
    WRITER = 2
    MAINTAINER = 3
    OWNER = 4

class Workspace(TimeStampedModel):
    name = CharField(max_length=300, unique=True)
    # public = BooleanField(default=False)

    # Max length of 34, since uuid hexes are 32, + 2 chars on the front
    arango_db_name = CharField(max_length=34, unique=True, default=create_default_arango_db_name)

    class Meta:
        ordering = ['id']
        permissions = [
            # TODO: decide on a permissions model
            # roles determining permissions
            ('owner', 'Owns the workspace'),
            ('maintainer', 'May grant all roles but owner on the workspace'),
            ('writer', 'May write to and remove from the workspace'),
            ('reader', 'May read and perform non-mutating queries on the workspace'),
        ]

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
    
    def update_user_permissions(self, permissions: list[dict]):
        """
        Update workspace object permissions for this workspace.
        """

        owners = list(self.owners)

        for user_permissions in permissions:
            user = user_permissions["user"]
            new_permissions = user_permissions["permissions"]
            current_permissions = get_user_perms(user, self)

            if 'owner' in current_permissions and 'owner' not in new_permissions:
                # case for removing ownership
                owners.remove(user)

            if 'owner' in new_permissions and 'owner' not in current_permissions:
                # add user as a new owner
                owners.append(user)

            # remove current non-owner permissions for the user
            for perm in current_permissions:
                if perm != 'owner':
                    remove_perm(perm, user, self)

            # add new permissions for the user
            for perm in new_permissions:
                if perm != 'owner':
                    assign_perm(perm, user, self)
        
        self.set_owners(owners)

    def get_arango_db(self) -> StandardDatabase:
        return get_or_create_db(self.arango_db_name)

    def __str__(self) -> str:
        return self.name


class WorkspacePermissionsHelper():
    """
    Class to safely handle object permissions for workspaces. Ideally this class is the only way
    object permissions are granted to and removed from users. Unsafe adding or removing of permissions
    could result in a messy/illegal state for some users.
    """
    def make_owner(self, user, workspace):
        pass

    def make_maintainer(self, user, workspace):
        pass

    def make_writer(self, user, workspace):
        pass

    def make_reader(self, user, workspace):
        pass

    def remove_all(self, user, workspace):
        pass

# Handle arango sync
@receiver(pre_save, sender=Workspace)
def arango_db_save(sender: Type[Workspace], instance: Workspace, **kwargs):
    ensure_db_created(instance.arango_db_name)


@receiver(post_delete, sender=Workspace)
def arango_db_delete(sender: Type[Workspace], instance: Workspace, **kwargs):
    ensure_db_deleted(instance.arango_db_name)
