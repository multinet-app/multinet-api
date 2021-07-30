"""Module for storing constants and helper functions regarding workspace permissions"""
from enum import Enum


class WorkspacePermission(Enum):
    """
    Class that handles the hierarchy of permissions in multinet.
    There are 4 object-level permissions for workspaces. They are:

    1) Reader - lowest level
    2) Writer
    3) Maintainer
    4) Owner - highest level

    This enum class handles translating between django-guardian permission keys
    (which are strings) and each permission's inherent rank.
    """
    owner = 'owner'
    maintainer = 'maintainer'
    writer = 'writer'
    reader = 'reader'

    @classmethod
    def get_permission_codenames(cls):
        return [permission.value for permission in cls]

    @classmethod
    def get_rank_from_key(cls, permission_key: str):
        try:
            permission = WorkspacePermission(permission_key)
            return permission.rank
        except ValueError:
            return 0

    @property
    def rank(self):
        if self == WorkspacePermission.owner:
            return 4
        elif self == WorkspacePermission.maintainer:
            return 3
        elif self == WorkspacePermission.writer:
            return 2
        return 1

    @property
    def associated_perms(self):
        """
        For a WorkspacePermission, its associated_perms is a list of django-guardian permission
        code names that the given WorkspacePermission implies. For example, writers, maintainers,
        and owners are all implicity 'readers,' so this property will evaluate to
        ['owner', 'maintainer', 'writer', 'reader'] on WorkspacePermission.reader.
        to handle requests.
        """
        perms = [self.value]
        if self == WorkspacePermission.maintainer:
            perms += [WorkspacePermission.owner.value]
        elif self == WorkspacePermission.writer:
            perms += [
                WorkspacePermission.maintainer.value,
                WorkspacePermission.owner.value
            ]
        elif self == WorkspacePermission.reader:
            perms += [
                WorkspacePermission.writer.value,
                WorkspacePermission.maintainer.value,
                WorkspacePermission.owner.value
            ]
        return perms
