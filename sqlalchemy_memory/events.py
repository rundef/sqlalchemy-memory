from sqlalchemy import event
from sqlalchemy.orm import Session as _Session

from sqlalchemy.orm.session import ORMExecuteState
from sqlalchemy.engine import IteratorResult
from sqlalchemy.engine.cursor import SimpleResultMetaData


def register_events():
    @event.listens_for(_Session, "before_commit")
    def _session_before_commit(session):
        # Disable expiry if this session uses our dialect
        if session.bind.dialect.name != "memory":
            return

        session.expire_on_commit = False

    @event.listens_for(_Session, "before_flush", propagate=True)
    def _session_before_flush(session, flush_context, instances):
        # Hook real ORM objects into MemorySession
        if session.bind.dialect.name != "memory":
            return

        conn = session.connection().connection.dbapi_connection

        for obj in list(session.new):
            conn.session.add(obj)

        for obj in list(session.deleted):
            conn.session.delete(obj)

        # prevent SQL emission
        session.new.clear()
        session.deleted.clear()

    @event.listens_for(_Session, "do_orm_execute")
    def _memstore_orm_execute(orm_state: ORMExecuteState):
        if orm_state.session.bind.dialect.name != "memory":
            return

        # 1) only intercept *top‑level* SELECTs, skip internal loads
        if not orm_state.is_select or orm_state.is_relationship_load or orm_state.is_column_load:
            return

        # 2) detect single‑entity selects: select(MyModel)
        cd = orm_state.statement.column_descriptions
        if len(cd) != 1 or cd[0]["entity"] is None:
            return

        model = cd[0]["entity"]
        table_name = model.__table__.name

        # 3) grab your MemorySession from the raw DBAPI connection
        raw_conn = orm_state.session.connection().connection.dbapi_connection
        objects = raw_conn.session.store.data.get(table_name, [])

        """
        # build a MemoryQuery over your objects
        mq = MemoryQuery(mem_sess, model, items, active_items=active)
    
        # apply all WHERE conditions
        for cond in stmt._where_criteria:
            mq = mq.filter(cond)
    
        # get the filtered list
        results = mq.all()
        """

        # 4) build cursor metadata from the table's columns
        cols = list(model.__table__.columns)
        metadata = SimpleResultMetaData([
            (col.name, None, None, None, None, None, None)
            for col in cols
        ])

        # 5) wrap each object in a single‑element tuple, so .scalars() yields it
        wrapped = ((obj,) for obj in objects)

        # 6) return an IteratorResult; SQLAlchemy will skip the normal SQL path
        return IteratorResult(metadata, wrapped)
