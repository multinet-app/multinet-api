from django.core.management import call_command

from multinet.api.management.commands.createarangoreadonlyuser import READONLY_USERNAME
from multinet.api.utils.arango import arango_system_db


def test_createarangoreadonlyuser():
    system_db = arango_system_db()

    if system_db.has_user(READONLY_USERNAME):
        system_db.delete_user(READONLY_USERNAME)
    assert not system_db.has_user(READONLY_USERNAME)

    call_command('createarangoreadonlyuser')

    assert system_db.has_user(READONLY_USERNAME)
    readonly_permissions = system_db.permission(READONLY_USERNAME, '*')
    assert readonly_permissions == 'ro'
