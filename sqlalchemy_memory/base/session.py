from sqlalchemy.orm import Session
from sqlalchemy.sql.selectable import Select, SelectLabelStyle
from sqlalchemy.sql.dml import Insert, Delete, Update
from sqlalchemy.engine import IteratorResult, ChunkedIteratorResult
from sqlalchemy.engine.cursor import SimpleResultMetaData
from sqlalchemy.sql.annotation import AnnotatedTable
from functools import partial

from unittest.mock import MagicMock

from .query import MemoryQuery
from .pending_changes import PendingChanges
from ..logger import logger
from ..helpers.utils import chunk_generator

class MemorySession(Session):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._has_pending_merge = False
        self.store = self.get_bind().dialect._store

        # Non-flushed changes
        self.pending_changes = PendingChanges()

    def add(self, obj, **kwargs):
        self.pending_changes.add(obj, **kwargs)

    def add_all(self, instances, **kwargs):
        for instance in instances:
            self.add(instance, **kwargs)

    def delete(self, obj):
        self.pending_changes.delete(obj)

    def update(self, tablename, pk_value, data):
        self.pending_changes.update(tablename, pk_value, data)

    def get(self, entity, id, **kwargs):
        """
        Return an instance based on the given primary key identifier, or ``None`` if not found.
        """
        return self.store.get_by_primary_key(entity, id)

    def scalars(self, statement, **kwargs):
        return self.execute(statement, **kwargs).scalars()

    def scalar(self, statement, **kwargs):
        return self.execute(statement, **kwargs).scalar()

    @staticmethod
    def _get_metadata_from_columns(columns):
        return SimpleResultMetaData([
            getattr(col, "name", str(col))
            for col in columns
        ])

    def _handle_select(self, statement: Select, **kwargs):
        # Execute the query
        q = MemoryQuery(statement, self)
        results = q.iter_items()

        metadata = self._get_metadata_from_columns(statement._raw_columns)

        if statement._label_style is SelectLabelStyle.LABEL_STYLE_LEGACY_ORM and all(
            isinstance(c, AnnotatedTable) for c in statement._raw_columns
        ):
            """
            Support for legacy session.query(...) style
            """
            it = IteratorResult(metadata, results)
            it._real_result = MagicMock(_source_supports_scalars=True)
            it._generate_rows = False
            return it

        it = ChunkedIteratorResult(metadata, partial(chunk_generator, results))

        return it


    def _handle_delete(self, statement: Delete, **kwargs):
        collection = MemoryQuery(statement, self).all()

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
            metadata = self._get_metadata_from_columns(cols)
            rows = [
                tuple(getattr(obj, col.name) for col in cols)
                for obj in instances
            ]
            return IteratorResult(metadata, iter(rows))

        result = IteratorResult(SimpleResultMetaData([]), iter([]))
        result.rowcount = rowcount
        return result

    def _handle_update(self, statement: Update, **kwargs):
        collection = MemoryQuery(statement, self).all()

        data = {
            col.name: bindparam.value
            for col, bindparam in statement._values.items()
        }

        tablename = statement.table.name
        pk_col_name = None
        for obj in collection:
            if pk_col_name is None:
                pk_col_name = self.store._get_primary_key_name(obj.__table__)

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

        pk_name = self.store._get_primary_key_name(instance.__table__)
        pk_value = getattr(instance, pk_name)
        existing = self.store.get_by_primary_key(instance, pk_value)

        if existing:
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
        return self.pending_changes.dirty or self._has_pending_merge

    def _is_clean(self):
        return not self.dirty

    def flush(self, objects=None):
        if not self._transaction or not self._transaction._connections:
            self.connection()  # Ensure a real connection is created

        self.pending_changes.flush_to(self.store.pending_changes)

    def rollback(self, **kwargs):
        logger.debug("Rolling back ...")

        self.store.rollback()

        self._has_pending_merge = False
        self.pending_changes.rollback()


    def commit(self):
        if self.dirty:
            self.flush()

        if self.store.dirty or self._has_pending_merge:
            logger.debug("Committing ...")
            self.store.commit()
            self._has_pending_merge = False
