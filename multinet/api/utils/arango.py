from functools import lru_cache
from typing import List

from arango import ArangoClient
from arango.database import StandardDatabase
from django.conf import settings


@lru_cache()
def arango_client():
    return ArangoClient(hosts=settings.MULTINET_ARANGO_URL)


def db(name: str):
    return arango_client().db(name, username='root', password=settings.MULTINET_ARANGO_PASSWORD)


@lru_cache()
def arango_system_db():
    return db('_system')


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
    return db(name)


def paginate_aql_query(query: str, limit: int = 0, offset: int = 0) -> str:
    """Apply an offset and limit to an AQL query string."""
    if not limit and not offset:
        return query

    new_query = f'FOR doc IN ({query}) LIMIT {offset}, {limit} RETURN doc'
    return new_query


def get_aql_query_from_collections(collections: List[str]) -> str:
    """Generate an AQL query string from a list of collections."""
    collections_str = f'UNION({", ".join(collections)})' if len(collections) > 1 else collections[0]
    return f'FOR doc in {collections_str} RETURN doc'
