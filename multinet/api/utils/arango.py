from arango.database import StandardDatabase
from arango import ArangoClient
from django.conf import settings
from functools import lru_cache


@lru_cache()
def arango_client():
    return ArangoClient(hosts=settings.MULTINET_ARANGO_URL)


@lru_cache()
def arango_system_db():
    return arango_client().db(
        '_system', username='root', password=settings.MULTINET_ARANGO_PASSWORD
    )


def ensure_db_created(name: str) -> None:
    sysdb = arango_system_db()
    if not sysdb.has_database(name):
        sysdb.create_database(name)


def ensure_db_deleted(name: str) -> None:
    sysdb = arango_system_db()
    if sysdb.has_database(name):
        sysdb.delete_database(name)


def get_or_create_db(name: str) -> StandardDatabase:
    ensure_db_created(name)
    return arango_client.db(name)
