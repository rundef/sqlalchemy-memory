from sqlalchemy.engine import URL, default
import types
import copy

from .connection import MemoryDBAPIConnection
from ..util import _raw_dbapi_connection
from ..events import register_events

register_events()

class MemoryDialect(default.DefaultDialect):
    name = "memory"
    driver = "memory"
    execution_ctx_cls = default.DefaultExecutionContext
    supports_native_boolean = True
    supports_statement_cache = False

    def initialize(self, connection):
        super().initialize(connection)

        # Turn off pool reset to preserve in-memory data
        connection.engine.pool._reset_on_return = None

    def create_connect_args(self, url: URL):
        return [], {}

    @classmethod
    def import_dbapi(cls, **kwargs):
        # Provide a dummy DBAPI module
        module = types.SimpleNamespace()
        module.paramstyle = "named"
        module.apilevel = "2.0"
        module.threadsafety = 1
        module.Error = Exception

        def connect(*args, **kw):
            return MemoryDBAPIConnection()

        module.connect = connect

        return module

    def do_begin(self, dbapi_conn):
        raw = _raw_dbapi_connection(dbapi_conn)
        raw._snapshot = copy.deepcopy(raw.data)

    def do_commit(self, dbapi_conn):
        raw = _raw_dbapi_connection(dbapi_conn)
        raw.session.commit()

    def do_rollback(self, dbapi_conn):
        raw = _raw_dbapi_connection(dbapi_conn)
        raw.data = raw._snapshot
        raw.session.rollback()
