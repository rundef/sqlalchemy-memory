from sqlalchemy.engine import URL, default
from sqlalchemy import event
from sqlalchemy.orm import Mapper
import types
import contextvars
from .connection import MemoryDBAPIConnection
from .store import InMemoryStore
from ..logger import logger

_current_store = contextvars.ContextVar("current_store")

def set_current_store(store):
    _current_store.set(store)

def get_current_store():
    return _current_store.get()

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
        set_current_store(self._store)

        @event.listens_for(Mapper, "mapper_configured")
        def auto_attach_tracking(_, class_):
            logger.debug(f"Attaching tracking to class {class_}")

            for column in class_.__table__.columns:
                event.listen(
                    getattr(class_, column.name),
                    "set",
                    self._track_field_change_listener,
                    retval=False,
                )

    def _track_field_change_listener(self, *a, **kw):
        try:
            store = get_current_store()
        except LookupError:
            return

        store._track_field_change_listener(*a, **kw)

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
