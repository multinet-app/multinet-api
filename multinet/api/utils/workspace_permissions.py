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

    owner = 4
    maintainer = 3
    writer = 2
    reader = 1

    @classmethod
    def get_permission_codenames(cls):
        """
        Returns all of the permission code names for the workspace as a list.
        This type of list is often useful with django-guardian shortcuts.
        """
        return [permission.name for permission in cls]
