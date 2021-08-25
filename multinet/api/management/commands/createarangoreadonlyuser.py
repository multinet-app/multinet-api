from arango import ArangoClient
from arango.database import StandardDatabase
from arango.exceptions import ArangoError
from arango.http import DefaultHTTPClient
from django.conf import settings
from django.core.management.base import BaseCommand

READONLY_USERNAME = 'readonly'


class Command(BaseCommand):
    help = (
        'Ensure the existence of a user with universal read-only priveleges on the Arango DB server'
    )

    def handle(self, *args, **kwargs):
        try:
            arango_client: ArangoClient = ArangoClient(
                hosts=settings.MULTINET_ARANGO_URL, http_client=DefaultHTTPClient()
            )
            system_db: StandardDatabase = arango_client.db(
                password=settings.MULTINET_ARANGO_PASSWORD
            )
            readonly_user_exists = system_db.has_user(READONLY_USERNAME)

            if not readonly_user_exists:
                system_db.create_user(READONLY_USERNAME, settings.MULTINET_ARANGO_READONLY_PASSWORD)
                self.stdout.write(
                    self.style.SUCCESS(f'Successfully created user: \'{READONLY_USERNAME}\'')
                )

            system_db.update_permission(username=READONLY_USERNAME, permission='ro', database='*')
            self.stdout.write(
                self.style.SUCCESS(
                    'Successfully set universal read-only permission for user: '
                    f'\'{READONLY_USERNAME}\''
                )
            )
        except ArangoError:
            self.stderr.write(self.style.ERROR('Failed to communicate with the Arango server'))
        except Exception:
            self.stderr.write(self.style.ERROR(f'Failed to create user: {READONLY_USERNAME}'))
