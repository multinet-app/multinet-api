"""Module for storing constants and helper functions regarding workspace permissions"""

OWNER = 'owner'
MAINTAINER = 'maintainer'
WRITER = 'writer'
READER = 'reader'

PERMISSION_RANK = {
    OWNER: 4,
    MAINTAINER: 3,
    WRITER: 2,
    READER: 1
}

def valid_permission(permission: str):
    """Check a string to see if it is a valid permission."""
    return permission in [OWNER, MAINTAINER, WRITER, READER]

def valid_permission_list(permissions: list):
    """Check all permissions in a list for validity."""
    for permission in permissions:
        if not valid_permission(permission):
            return False
    return True

def highest_permission(permissions: list):
    """Given a list of valid permissions, return the highest ranked permission. Owner is the highest ranking permission."""
    if not valid_permission_list(permissions):
        return 0
    return max([PERMISSION_RANK[perm] for perm in permissions])