from __future__ import annotations

from functools import lru_cache
from typing import Dict, List, Optional

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


class ArangoQuery:
    """A class to represent an AQL query."""

    def __init__(
        self,
        db: StandardDatabase,
        query_str: Optional[str] = None,
        bind_vars: Optional[Dict[str, str]] = None,
    ) -> None:
        self.db = db
        self.query_str = query_str
        self.bind_vars = bind_vars

    @classmethod
    def from_collections(cls, db: StandardDatabase, collections: List[str]) -> ArangoQuery:
        """Generate an AQL query string from a list of collections."""
        coll_vars = []
        bind_vars = {}
        for i, coll in enumerate(collections):
            key = f'@coll{i}'
            coll_vars.append(f'@{key}')
            bind_vars[key] = coll

        collections_str = f'UNION({", ".join(coll_vars)})' if len(coll_vars) > 1 else coll_vars[0]
        query_str = f'FOR doc in {collections_str} RETURN doc'

        return ArangoQuery(db, query_str=query_str, bind_vars=bind_vars)

    def paginate(self, limit: int = 0, offset: int = 0) -> ArangoQuery:
        if not limit and not offset:
            return ArangoQuery(self.db, query_str=self.query_str, bind_vars=self.bind_vars)

        new_query_str = f'FOR doc IN ({self.query_str}) LIMIT {offset}, {limit} RETURN doc'
        return ArangoQuery(self.db, query_str=new_query_str, bind_vars=self.bind_vars)
