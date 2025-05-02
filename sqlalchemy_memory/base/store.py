from collections import defaultdict
from sqlalchemy import func
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.orm.attributes import NEVER_SET, NO_VALUE, LoaderCallableStatus
from datetime import datetime

from ..logger import logger
from .pending_changes import PendingChanges
from .indexes import IndexManager

class InMemoryStore:
    def __init__(self):
        self._reset()

    def _reset(self):
        self.data = defaultdict(list)
        self.data_by_pk = defaultdict(dict)

        self.index_manager = IndexManager()

        # Non-committed changes
        self.pending_changes = PendingChanges()

        # Auto increment counter per table
        self._pk_counter = defaultdict(int)

        # Caches
        self.table_columns = {}
        self.table_pk_name = {}

    @property
    def dirty(self):
        return self.pending_changes.dirty

    def commit(self):
        self.update_modified_items_indexes()

        # apply deletes
        for tablename, objs in self.pending_changes._to_delete.items():
            if not objs:
                continue

            data = self.data.get(tablename, [])
            pk_col_name = self._get_primary_key_name(objs[0].__table__)

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

            # Update indexes
            for obj in objs:
                self.index_manager.on_delete(obj)

        # apply adds
        added = set()
        for tablename, objs in self.pending_changes._to_add.items():
            if tablename not in self.data:
                self.data[tablename] = []

            for obj in objs:
                if id(obj) in added:
                    continue
                added.add(id(obj))

                pk_value = self._assign_primary_key_if_needed(obj)
                if pk_value in self.data_by_pk[tablename].keys():
                    raise Exception(f"Cannot have duplicate PK value {pk_value} for table '{tablename}'")

                self._apply_column_defaults(obj)

                logger.debug(f"Adding {obj} to table '{tablename}'")

                self.data[tablename].append(obj)
                self.data_by_pk[tablename][pk_value] = obj
                self.index_manager.on_insert(obj)

        # apply updates
        for tablename, updates in self.pending_changes._to_update.items():
            for pk_value, data in updates:
                if pk_value not in self.data_by_pk[tablename].keys():
                    raise Exception(f"Could not find item with PK value {pk_value} in table '{tablename}'")

                logger.debug(f"Updating table '{tablename}' where PK value={pk_value}: {data}")
                item = self.data_by_pk[tablename][pk_value]

                values = {}
                for k, v in data.items():
                    values[k] = dict(old=getattr(item, k), new=v)
                    setattr(item, k, v)

                # Update indexes
                self.index_manager.on_update(item, values)

        self.pending_changes.clear()

    def rollback(self):
        # Revert attributes changes
        for updates in self.pending_changes._modifications.values():
            instance = updates.pop("__instance")
            for colname, (old_value, new_value) in updates.items():
                setattr(instance, colname, old_value)

        self.pending_changes.rollback()

    def get_by_primary_key(self, entity, pk_value):
        tablename = entity.__tablename__
        if tablename not in self.data_by_pk:
            return None

        return self.data_by_pk[tablename].get(pk_value)

    def _get_primary_key_name(self, table):
        """
        Return the PK column name
        """
        tablename = table.name
        if tablename not in self.table_pk_name:
            pk_cols = table.primary_key.columns

            if len(pk_cols) != 1:
                raise NotImplementedError("Only single-column primary keys are supported.")

            col = list(pk_cols)[0]
            self.table_pk_name[tablename] = col.name

        return self.table_pk_name[tablename]

    def _get_table_columns(self, table):
        """
        Returns the table columns
        """
        tablename = table.name
        if tablename not in self.table_columns:
            self.table_columns[tablename] = table.columns

        return self.table_columns[tablename]

    def _assign_primary_key_if_needed(self, obj):
        """
        Handle auto-increment primary keys.
        If user specifies an ID, use it and update the counter if necessary.
        If no ID is specified, assign the next available one.
        """
        pk_col_name = self._get_primary_key_name(obj.__table__)
        current_id = obj.__dict__.get(pk_col_name, None)
        tablename = obj.__tablename__

        if current_id is None:
            # Auto-assign next ID
            current_id = self._pk_counter[tablename] = self._pk_counter[tablename] + 1
            obj.__dict__[pk_col_name] = current_id

        else:
            # Ensure auto-increment counter stays ahead
            self._pk_counter[tablename] = max(self._pk_counter[tablename], current_id)

        return current_id

    def _apply_column_defaults(self, obj):
        """
        Apply default and server_default values to an ORM object.
        """

        for column in self._get_table_columns(obj.__table__):
            if column.default is None and column.server_default is None:
                continue

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

                obj.__dict__[attr_name] = value

            elif column.server_default is not None:
                if isinstance(column.server_default.arg, TextClause):
                    text_value = column.server_default.arg.text
                    obj.__dict__[attr_name] = text_value

                elif isinstance(column.server_default.arg, func.now().__class__):
                    obj.__dict__[attr_name] = datetime.utcnow()

                else:
                    raise Exception(f"Unhandled server_default type: {type(column.server_default)}")

    def query_index(self, collection, table_name, attr_name, op, value):
        result = self.index_manager.query(collection, table_name, attr_name, op, value)
        if result is not None:
            logger.debug(f"Reduced '{table_name}' dataset from {len(collection)} items to {len(result)} by using index on '{attr_name}")
        return result

    def count(self, tablename):
        return len(self.data[tablename])

    def update_modified_items_indexes(self):
        # update indexes of modified objects
        for updates in self.pending_changes._modifications.values():
            instance = updates.pop("__instance")

            values = {
                colname: dict(old=old_value, new=new_value)
                for colname, (old_value, new_value) in updates.items()
                if old_value != new_value
            }
            if not values:
                continue

            # Update indexes
            self.index_manager.on_update(instance, values)

    def _track_field_change_listener(self, target, value, oldvalue, initiator):
        if oldvalue in (NO_VALUE, NEVER_SET, LoaderCallableStatus.NO_VALUE):
            return
        if oldvalue == value:
            return

        self.pending_changes.mark_field_as_dirty(target, initiator.key, oldvalue, value)
