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

    # Set the request timeout to 1 hour (in seconds)
    REQUEST_TIMEOUT = 60 * 60


@lru_cache()
def arango_client():
    return ArangoClient(hosts=settings.MULTINET_ARANGO_URL, http_client=NoTimeoutHttpClient())


def db(name: str, readonly):
    username = 'readonly' if readonly else 'root'
    password = (
        settings.MULTINET_ARANGO_READONLY_PASSWORD
        if readonly
        else settings.MULTINET_ARANGO_PASSWORD
    )
    return arango_client().db(name, username=username, password=password)


@lru_cache()
def arango_system_db(readonly=True):
    return db('_system', readonly)


def ensure_db_created(name: str) -> None:
    sysdb = arango_system_db(readonly=False)
    if not sysdb.has_database(name):
        sysdb.create_database(name)


def ensure_db_deleted(name: str) -> None:
    sysdb = arango_system_db(readonly=False)
    if sysdb.has_database(name):
        sysdb.delete_database(name)


class ArangoQuery:
    """A class to represent an AQL query."""

    def __init__(
        self,
        db: StandardDatabase,
        query_str: Optional[str] = None,
        bind_vars: Optional[Dict[str, str]] = None,
        time_limit_secs: int = 30,
        memory_limit_bytes: int = 20000000,  # 20MB
    ) -> None:
        self.db = db
        self.query_str = query_str
        self.bind_vars = bind_vars
        self.time_limit_secs = time_limit_secs
        self.memory_limit_bytes = memory_limit_bytes

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
        """
        Filter an AQL query to match the provided document.

        This function transforms a dict into the syntax required for filtering an AQL query.
        For example, the dict `{"foo": "bar"}` would become `FILTER doc['foo'] == 'bar'`.
        This is done by creating bind variables for each key and value used, and then injecting
        those variables into the query (to prevent any unwanted behavior from injected variables).
        So instead of directly using the variables as shown in the above example query, the
        following would be done::

            bind_vars = {"f2e59b48": "foo", "a7f3755d": "bar"}
            query_str = "FILTER doc[@f2e59b48] == @a7f3755d"

        Since a dict almost always has multiple key/value pairs, this is done for every pair and
        joined together into the final query.
        """
        # If empty filter, do nothing
        if not doc:
            return self

        # Iterate through the dict, storing each key/value pair as bind vars,
        # and creating a query filter using them
        new_bind_vars = dict(self.bind_vars)
        filter_query_lines = []
        for k, v in doc.items():
            # Create unique bind var keys for dict key and value
            key_key = str(uuid.uuid4())[:8]
            val_key = str(uuid.uuid4())[:8]

            # Store the dict key and value using the generated keys
            new_bind_vars[key_key] = k
            new_bind_vars[val_key] = v

            # Reference the variable keys in the query
            filter_query_lines.append(f'doc[@{key_key}] == @{val_key}')

        # Join the filters and wrap query
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
        # Use time and memory limit of the query object unless different values
        # are explicitly passed.
        if 'max_runtime' not in kwargs:
            kwargs['max_runtime'] = self.time_limit_secs
        if 'memory_limit' not in kwargs:
            kwargs['memory_limit'] = self.memory_limit_bytes

        return self.db.aql.execute(query=self.query_str, bind_vars=self.bind_vars, **kwargs)
