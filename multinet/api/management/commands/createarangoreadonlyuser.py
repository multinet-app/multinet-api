from arango.exceptions import (
    ArangoClientError,
    ArangoServerError,
    PermissionUpdateError,
    UserCreateError,
)
from django.conf import settings
from django.core.management.base import BaseCommand

from multinet.api.utils.arango import arango_system_db

READONLY_USERNAME = 'readonly'


class Command(BaseCommand):
    help = (
        'Ensure the existence of a user with universal read-only priveleges on the'
        'Arango DB server. Replaces the readonly user with a new one if it already exists.'
    )

    def handle(self, *args, **kwargs):
        try:
            password = settings.MULTINET_ARANGO_READONLY_PASSWORD
            system_db = arango_system_db()
            readonly_user_exists = system_db.has_user(READONLY_USERNAME)

            if not readonly_user_exists:
                system_db.create_user(READONLY_USERNAME, password, active=True)
                self.stdout.write(
                    self.style.SUCCESS(f'Successfully created user: {READONLY_USERNAME}')
                )
            else:
                system_db.replace_user(READONLY_USERNAME, password, active=True)
                self.stdout.write(
                    self.style.SUCCESS(f'Successfully replaced user: {READONLY_USERNAME}')
                )

            system_db.update_permission(username=READONLY_USERNAME, permission='ro', database='*')
            self.stdout.write(
                self.style.SUCCESS(
                    'Successfully set universal read-only permission for user: '
                    f'{READONLY_USERNAME}'
                )
            )
        except AttributeError:
            self.stderr.write(
                self.style.ERROR(
                    'Environment variable MULTINET_ARANGO_READONLY_PASSWORD is required.'
                )
            )
        except UserCreateError:
            self.stderr.write(self.style.ERROR(f'Failed to create user: {READONLY_USERNAME}'))
        except PermissionUpdateError:
            self.stderr.write(
                self.style.ERROR(f'Failed to set permissions for user: {READONLY_USERNAME}')
            )
        except (ArangoClientError, ArangoServerError) as error:
            self.stderr.write(self.style.ERROR(str(error)))
