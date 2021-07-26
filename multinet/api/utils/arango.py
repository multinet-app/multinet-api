from __future__ import annotations

from functools import lru_cache
from typing import Dict, List, Optional
import uuid

from arango import ArangoClient
from arango.cursor import Cursor
from arango.database import StandardDatabase
from arango.http import DefaultHTTPClient
from django.conf import settings


class NoTimeoutHttpClient(DefaultHTTPClient):
    """Extend the default arango http client, to remove timeouts for bulk data."""

    REQUEST_TIMEOUT = None


@lru_cache()
def arango_client():
    return ArangoClient(hosts=settings.MULTINET_ARANGO_URL, http_client=NoTimeoutHttpClient())


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

    @staticmethod
    def from_collections(db: StandardDatabase, collections: List[str]) -> ArangoQuery:
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

    def filter(self, doc: Dict) -> ArangoQuery:
        """Filter an AQL query to match the provided document."""
        # If empty filter, do nothing
        if not doc:
            return self

        new_bind_vars = dict(self.bind_vars)
        filter_query_lines = []
        for k, v in doc.items():
            # Create unique bind var keys, so they don't
            # conflict with any previous or future keys
            key_key = str(uuid.uuid4())[:8]
            val_key = str(uuid.uuid4())[:8]

            new_bind_vars[key_key] = k
            new_bind_vars[val_key] = v
            filter_query_lines.append(f'doc[@{key_key}] == @{val_key}')

        filter_query_str = ' and '.join(filter_query_lines)
        new_query_str = f'FOR doc IN ({self.query_str}) FILTER ({filter_query_str}) RETURN doc'
        return ArangoQuery(self.db, query_str=new_query_str, bind_vars=new_bind_vars)

    def paginate(self, limit: int = 0, offset: int = 0) -> ArangoQuery:
        if not limit and not offset:
            return ArangoQuery(self.db, query_str=self.query_str, bind_vars=self.bind_vars)

        new_query_str = f'FOR doc IN ({self.query_str}) LIMIT {offset}, {limit} RETURN doc'
        return ArangoQuery(self.db, query_str=new_query_str, bind_vars=self.bind_vars)

    def execute(self, **kwargs) -> Cursor:
        """
        Execute an AQL query with the instantiated query_str and bind_vars.

        Accepts the same keyword arguments as `arango.database.StandardDatabase.aql.execute`.
        """
        return self.db.aql.execute(query=self.query_str, bind_vars=self.bind_vars, **kwargs)
