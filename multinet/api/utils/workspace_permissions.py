"""Module for storing constants and helper functions regarding workspace permissions"""

OWNER = 'owner'
MAINTAINER = 'maintainer'
WRITER = 'writer'
READER = 'reader'

PERMISSION_RANK = {OWNER: 4, MAINTAINER: 3, WRITER: 2, READER: 1}


def valid_permission(permission: str):
    """Check a string to see if it is a valid permission."""
    return permission in [OWNER, MAINTAINER, WRITER, READER]


def get_rank(permission: str):
    """Get the rank associated with a permission string"""
    if not valid_permission:
        return 0
    return PERMISSION_RANK[permission]


def highest_permission(permissions: list):
    """
    Given a list of valid permissions, return the highest ranked permission.
    Owner is the highest ranking permission.
    """
    if len(permissions) == 0:
        return 0
    return max([get_rank(perm) for perm in permissions])
