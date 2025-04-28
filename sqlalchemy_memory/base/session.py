from sqlalchemy.orm import Session
from sqlalchemy.sql.selectable import Select
from sqlalchemy.sql.dml import Insert, Delete, Update
from sqlalchemy.engine import IteratorResult
from sqlalchemy.engine.cursor import SimpleResultMetaData
from functools import lru_cache
from collections import defaultdict

from .query import MemoryQuery
from ..logger import logger


class MemorySession(Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._query_cls = MemoryQuery
        self._has_pending_merge = False
        self.store = self.get_bind().dialect._store

        # Non-committed inserts/deletes/updates
        self._to_add = defaultdict(list)
        self._to_delete = defaultdict(list)
        self._to_update = defaultdict(list)

        self._fetched = defaultdict(dict)

    def add(self, obj, **kwargs):
        tablename = obj.__tablename__
        if not any(id(x) == id(obj) for x in self._to_add[tablename]):
            self._to_add[tablename].append(obj)

    def delete(self, obj):
        tablename = obj.__tablename__
        self._to_delete[tablename].append(obj)

    def update(self, tablename, pk_value, data):
        self._to_update[tablename].append((pk_value, data))

    def _mark_as_fetched(self, instance):
        tablename = instance.__tablename__

        pk_name = self.store._get_primary_key_name(instance)
        pk_value = getattr(instance, pk_name)

        if pk_value in self._fetched[tablename]:
            # Don't mark as fetched again
            return

        original_values = {
            col.name: getattr(instance, col.name)
            for col in instance.__table__.columns
        }
        self._fetched[tablename][pk_value] = original_values

    def get(self, entity, id, **kwargs):
        """
        Return an instance based on the given primary key identifier, or ``None`` if not found.
        """
        instance = self.store.get_by_primary_key(entity, id)
        if instance:
            self._mark_as_fetched(instance)
        return instance

    def scalars(self, statement, **kwargs):
        return self.execute(statement, **kwargs).scalars()

    def scalar(self, statement, **kwargs):
        return self.execute(statement, **kwargs).scalar()

    @staticmethod
    @lru_cache(maxsize=256)
    def _get_metadata_for_annotated_table(annotated_table):
        """
        Build minimal cursor metadata
        """
        col_names = [col.name for col in annotated_table._columns]
        return SimpleResultMetaData([
            (col_name, None, None, None, None, None, None)
            for col_name in col_names
        ])


    def _handle_select(self, statement: Select, **kwargs):
        entities = statement._raw_columns
        if len(entities) != 1:
            raise Exception("Only single‑entity SELECTs are supported")

        # Execute the query
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
            self._mark_as_fetched(obj)

        # Wrap each object in a single‑element tuple, so .scalars() yields it
        wrapped = ((obj,) for obj in objs)

        metadata = MemorySession._get_metadata_for_annotated_table(entities[0])

        return IteratorResult(metadata, wrapped)


    def _handle_delete(self, statement: Delete, **kwargs):
        q = MemoryQuery([statement.table], self)

        for cond in statement._where_criteria:
            q = q.filter(cond)

        collection = q.all()

        for obj in collection:
            self.delete(obj)

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
            self.add(obj)
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
            self.update(tablename, pk_value, data)

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
        pk_value = getattr(instance, pk_name)
        existing = self.store.get_by_primary_key(instance, pk_value)

        if existing:
            self._mark_as_fetched(existing)
            self._has_pending_merge = True

            for column in instance.__table__.columns:
                field = column.name
                if field == pk_name:
                    continue
                value = getattr(instance, field)
                setattr(existing, field, value)

            return existing

        else:
            self.add(instance)
            return instance

    @property
    def dirty(self):
        return bool(self._to_add or self._to_delete or self._to_update) or self._has_pending_merge

    def _is_clean(self):
        return not self.dirty

    def flush(self, objects=None):
        if not self._transaction or not self._transaction._connections:
            self.connection()  # Ensure a real connection is created

        to_transfer = [
            "_to_add",
            "_to_update",
            "_to_delete",
            "_fetched",
        ]
        for key in to_transfer:
            item = getattr(self, key)
            if not item:
                continue
            setattr(self.store, key, item.copy())
            item.clear()

    def rollback(self, **kwargs):
        logger.debug("Rolling back ...")

        self.store._fetched = self._fetched
        self.store.rollback()

        self._has_pending_merge = False

        self._to_add.clear()
        self._to_delete.clear()
        self._to_update.clear()
        self._fetched.clear()


    def commit(self):
        if self.dirty:
            self.flush()

        if self.store.dirty or self._has_pending_merge:
            logger.debug("Committing ...")
            self.store.commit()
            self._has_pending_merge = False
