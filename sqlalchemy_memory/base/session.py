from sqlalchemy.orm import Session
from sqlalchemy.sql.selectable import Select
from sqlalchemy.sql.dml import Insert, Delete, Update
from sqlalchemy.engine import IteratorResult
from sqlalchemy.engine.cursor import SimpleResultMetaData

from .query import MemoryQuery

class MemorySession(Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._query_cls = MemoryQuery

    @property
    def raw_connection(self):
        return self.connection().connection.dbapi_connection

    @property
    def store(self):
        return self.raw_connection.store

    def add(self, instance, **kwargs):
        self.store.add(instance)

    def get(self, entity, id, **kwargs):
        """
        Return an instance based on the given primary key identifier, or ``None`` if not found.
        """
        instance = self.store.get_by_primary_key(entity, id)
        if instance:
            self.store.mark_as_fetched(instance)
        return instance

    def scalars(self, statement, **kwargs):
        return self.execute(statement, **kwargs).scalars()

    def scalar(self, statement, **kwargs):
        return self.execute(statement, **kwargs).scalar()

    def _handle_select(self, statement: Select, **kwargs):
        # Detect single‑entity selects: select(MyModel)
        cd = statement.column_descriptions
        if len(cd) != 1 or cd[0]["entity"] is None:
            raise Exception("Model not found")

        model = cd[0]["entity"]

        # Execute the query
        entities = statement._raw_columns
        q = MemoryQuery(entities, self)

        # Apply WHERE
        for cond in statement._where_criteria:
            q = q.filter(cond)

        # Apply ORDER BY
        for clause in statement._order_by_clauses:
            q = q.order_by(clause)

        # Apply LIMIT / OFFSET
        if statement._limit_clause is not None:
            q = q.limit(statement._limit_clause.value)
        if statement._offset_clause is not None:
            q = q.offset(statement._offset_clause.value)

        objs = q.all()

        for obj in objs:
            self.store.mark_as_fetched(obj)

        # Build minimal cursor metadata
        metadata = SimpleResultMetaData([
            (col.name, None, None, None, None, None, None)
            for col in list(model.__table__.columns)
        ])

        # Wrap each object in a single‑element tuple, so .scalars() yields it
        wrapped = ((obj,) for obj in objs)

        return IteratorResult(metadata, wrapped)

    def _handle_delete(self, statement: Delete, **kwargs):
        q = MemoryQuery([statement.table], self)

        for cond in statement._where_criteria:
            q = q.filter(cond)

        collection = q.all()

        for obj in collection:
            self.store.delete(obj)

        result = IteratorResult(SimpleResultMetaData([]), iter([]))
        result.rowcount = len(collection)
        return result

    def _handle_insert(self, statement: Insert, params=None, **kwargs):
        # Determine list of value-dicts to insert
        if params is None:
            vals_list = [
                {
                    col.name: (val.value if hasattr(val, "value") else val)
                    for col, val in statement._values.items()
                }
            ]
        elif isinstance(params, list):
            vals_list = params
        else:
            vals_list = [params]

        mapper = statement.table._annotations["parentmapper"]
        model = mapper.class_

        instances = []
        for vals in vals_list:
            obj = model(**vals)
            self.store.add(obj)
            instances.append(obj)

        rowcount = len(instances)

        # Handle RETURNING(...)
        if statement._returning:
            cols = list(statement._returning)
            metadata = SimpleResultMetaData([
                (col.name, None, None, None, None, None, None)
                for col in cols
            ])
            rows = [
                tuple(getattr(obj, col.name) for col in cols)
                for obj in instances
            ]
            return IteratorResult(metadata, iter(rows))

        result = IteratorResult(SimpleResultMetaData([]), iter([]))
        result.rowcount = rowcount
        return result

    def _handle_update(self, statement: Update, **kwargs):
        q = MemoryQuery([statement.table], self)

        for cond in statement._where_criteria:
            q = q.filter(cond)

        collection = q.all()

        data = {
            col.name: bindparam.value
            for col, bindparam in statement._values.items()
        }

        tablename = statement.table.name
        pk_col_name = None
        for obj in collection:
            if pk_col_name is None:
                pk_col_name = self.store._get_primary_key_name(obj)

            pk_value = getattr(obj, pk_col_name)
            self.store.update(tablename, pk_value, data)

        result = IteratorResult(SimpleResultMetaData([]), iter([]))
        result.rowcount = len(collection)
        return result

    def execute(self, statement, params=None, **kwargs):
        if isinstance(statement, Select):
            return self._handle_select(statement, **kwargs)

        elif isinstance(statement, Delete):
            return self._handle_delete(statement, **kwargs)

        elif isinstance(statement, Insert):
            return self._handle_insert(statement, params=params, **kwargs)

        elif isinstance(statement, Update):
            return self._handle_update(statement, **kwargs)

        raise Exception(f"Statement not handled: {statement} {type(statement)}")

    def merge(self, instance, **kwargs):
        """
        Merge a possibly detached instance into the current session
        """

        pk_name = self.store._get_primary_key_name(instance)
        id = getattr(instance, pk_name)
        existing = self.store.get_by_primary_key(instance, id)

        if existing:
            data = {
                col.name: getattr(instance, col.name)
                for col in instance.__table__.columns
                if col.name != pk_name
            }

            self.store.update(instance.__tablename__, id, data)
            return existing

        else:
            self.add(instance)
            return instance

    def delete(self, instance):
        self.store.delete(instance)

    def flush(self, objects=None):
        pass

    def rollback(self, **kwargs):
        self.store.rollback()

    def commit(self):
        self.store.commit()
