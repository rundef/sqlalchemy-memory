from collections import defaultdict
from sqlalchemy import func
from sqlalchemy.sql.elements import TextClause
from datetime import datetime

from ..logger import logger

class InMemoryStore:
    def __init__(self):
        self._reset()

    def _reset(self):
        self.data = defaultdict(list)
        self.data_by_pk = defaultdict(dict)

        # Non-committed inserts/deletes/updates
        self._to_add = {}
        self._to_delete = {}
        self._to_update = {}

        self._fetched = {}

        # Auto increment counter per table
        self._pk_counter = defaultdict(int)

    @property
    def dirty(self):
        return bool(self._to_add or self._to_delete or self._to_update)

    def commit(self):
        # apply deletes
        for tablename, objs in self._to_delete.items():
            if not objs:
                continue

            data = self.data.get(tablename, [])
            pk_col_name = self._get_primary_key_name(objs[0])

            pk_values = set(getattr(obj, pk_col_name) for obj in objs)
            logger.debug(f"Deleting rows from table '{tablename}' with PK values={pk_values}")

            # Delete from table data
            self.data[tablename] = [
                row
                for row in data
                if getattr(row, pk_col_name) not in pk_values
            ]
            # Delete from PK lookup dict
            for pk_value in pk_values:
                del self.data_by_pk[tablename][pk_value]

        # apply adds
        for tablename, objs in self._to_add.items():
            if tablename not in self.data:
                self.data[tablename] = []

            for obj in objs:
                pk_value = self._assign_primary_key_if_needed(obj)
                if pk_value in self.data_by_pk[tablename].keys():
                    raise Exception(f"Cannot have duplicate PK value {pk_value} for table '{tablename}'")

                self._apply_column_defaults(obj)

                logger.debug(f"Adding {obj} to table '{tablename}'")

                self.data[tablename].append(obj)
                self.data_by_pk[tablename][pk_value] = obj

        # apply updates
        for tablename, updates in self._to_update.items():
            for pk_value, data in updates:
                if pk_value not in self.data_by_pk[tablename].keys():
                    raise Exception(f"Could not find item with PK value {pk_value} in table '{tablename}'")

                logger.debug(f"Updating table '{tablename}' where PK value={pk_value}: {data}")
                item = self.data_by_pk[tablename][pk_value]
                for k, v in data.items():
                    setattr(item, k, v)

        self._to_add.clear()
        self._to_delete.clear()
        self._to_update.clear()
        self._fetched.clear()

    def rollback(self):
        self._to_add.clear()
        self._to_delete.clear()
        self._to_update.clear()

        # Revert attributes changes
        for tablename, fetched_objs in self._fetched.items():
            for pk_value, original_values in fetched_objs.items():
                obj = self.data_by_pk[tablename].get(pk_value)

                for field, value in original_values.items():
                    setattr(obj, field, value)

        self._fetched.clear()

    def get_by_primary_key(self, entity, pk_value):
        tablename = entity.__tablename__
        if tablename not in self.data_by_pk:
            return None

        return self.data_by_pk[tablename].get(pk_value)

    def _get_primary_key_name(self, obj):
        """
        Return the PK column name
        """
        pk_cols = obj.__table__.primary_key.columns

        if len(pk_cols) != 1:
            raise NotImplementedError("Only single-column primary keys are supported.")

        col = list(pk_cols)[0]
        return col.name

    def _assign_primary_key_if_needed(self, obj):
        """
        Handle auto-increment primary keys.
        If user specifies an ID, use it and update the counter if necessary.
        If no ID is specified, assign the next available one.
        """
        pk_col_name = self._get_primary_key_name(obj)
        table = obj.__tablename__
        current_id = getattr(obj, pk_col_name, None)

        if current_id is None:
            # Auto-assign next ID
            self._pk_counter[table] += 1
            current_id = self._pk_counter[table]
            setattr(obj, pk_col_name, current_id)
        else:
            # Ensure auto-increment counter stays ahead
            self._pk_counter[table] = max(self._pk_counter[table], current_id)

        return current_id

    def _apply_column_defaults(self, obj):
        """
        Apply default and server_default values to an ORM object.
        """

        for column in obj.__table__.columns:
            attr_name = column.name
            current_value = getattr(obj, attr_name, None)

            if current_value is not None:
                continue

            elif column.default is not None:
                if callable(column.default.arg):
                    try:
                        value = column.default.arg()
                    except TypeError:
                        value = column.default.arg(ctx=None)
                else:
                    value = column.default.arg

                setattr(obj, attr_name, value)

            elif column.server_default is not None:
                if isinstance(column.server_default.arg, TextClause):
                    text_value = column.server_default.arg.text
                    setattr(obj, attr_name, text_value)

                elif isinstance(column.server_default.arg, func.now().__class__):
                    setattr(obj, attr_name, datetime.utcnow())

                else:
                    raise Exception(f"Unhandled server_default type: {type(column.server_default)}")
