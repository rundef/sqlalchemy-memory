from sqlalchemy.engine import URL, default
import types
import copy

from .connection import MemoryDBAPIConnection
from ..util import _raw_dbapi_connection

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
        module = types.SimpleNamespace(
            paramstyle="named",
            apilevel="2.0",
            threadsafety=1,
            Error=Exception
        )
        module.connect = lambda *a, **k: MemoryDBAPIConnection()
        return module

    def do_begin(self, dbapi_conn):
        connection = _raw_dbapi_connection(dbapi_conn)
        connection.store._snapshot = copy.deepcopy(connection.store.data)

    def do_commit(self, dbapi_conn):
        connection = _raw_dbapi_connection(dbapi_conn)
        connection.store.commit()

    def do_rollback(self, dbapi_conn):
        connection = _raw_dbapi_connection(dbapi_conn)
        connection.store.data = connection.store._snapshot
        connection.store.rollback()

    def has_table(self, *args, **kwargs):
        # Patch to make Base.metadata.create_all(engine) not throw any exception
        return True
