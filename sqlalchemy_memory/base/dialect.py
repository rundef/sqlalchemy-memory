from sqlalchemy.engine import URL, default
import types

from .connection import MemoryDBAPIConnection
from .store import InMemoryStore

class MemoryDialect(default.DefaultDialect):
    name = "memory"
    driver = "memory"
    execution_ctx_cls = default.DefaultExecutionContext
    supports_native_boolean = True
    supports_statement_cache = False

    _store = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._store = InMemoryStore()

    def initialize(self, connection):
        super().initialize(connection)

        # Turn off pool reset to preserve in-memory data
        connection.engine.pool._reset_on_return = None

    def create_connect_args(self, url: URL):
        db_name = url.database or "_default"
        return [db_name], {}

    @classmethod
    def import_dbapi(cls, **kwargs):
        # Provide a dummy DBAPI module
        module = types.SimpleNamespace(
            paramstyle="named",
            apilevel="2.0",
            threadsafety=1,
            Error=Exception,
            connect=lambda *a, **k: None
        )
        return module

    def connect(self, *args, **kwargs):
        connection = MemoryDBAPIConnection()
        connection.store = self._store
        return connection

    def do_commit(self, dbapi_conn):
        self._store.commit()

    def do_rollback(self, dbapi_conn):
        self._store.rollback()

    def has_table(self, *args, **kwargs):
        # Patch to make Base.metadata.create_all(engine) not throw any exception
        return True
