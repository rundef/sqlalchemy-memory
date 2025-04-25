from collections import defaultdict

class InMemoryStore:
    def __init__(self):
        self.data = defaultdict(list)
        self.data_by_pk = defaultdict(dict)

        self._snapshot = {}

        # Uncommitted inserts/deletes/updates
        self._to_add = defaultdict(list)
        self._to_delete = defaultdict(list)
        self._to_update = defaultdict(list)

        self._fetched = []

        # Auto increment counter per table
        self._pk_counter = defaultdict(int)

    def add(self, obj):
        tablename = obj.__tablename__
        self._to_add[tablename].append(obj)

    def delete(self, obj):
        tablename = obj.__tablename__
        self._to_delete[tablename].append(obj)

    def update(self, tablename, pk_value, data):
        self._to_update[tablename].append((pk_value, data))

    def mark_as_fetched(self, instance):
        original_values = {
            col.name: getattr(instance, col.name)
            for col in instance.__table__.columns
        }
        self._fetched.append((instance, original_values))

    def commit(self):
        # apply deletes
        for tablename, objs in self._to_delete.items():
            if not objs:
                continue

            data = self.data.get(tablename, [])
            pk_col_name = self._get_primary_key_name(objs[0])

            # Delete from table data
            self.data[tablename] = [
                row
                for row in data
                if not any(getattr(row, pk_col_name) == getattr(obj, pk_col_name) for obj in objs)
            ]
            # Delete from PK lookup dict
            for obj in objs:
                pk_value = getattr(obj, pk_col_name)
                del self.data_by_pk[tablename][pk_value]

        # apply adds
        for tablename, objs in self._to_add.items():
            data = self.data.setdefault(tablename, [])

            for obj in objs:
                pk_value = self._assign_primary_key_if_needed(obj)

                if pk_value in self.data_by_pk[tablename].keys():
                    raise Exception(f"Cannot have duplicate PK value {pk_value} for table {tablename}")

                data.append(obj)
                self.data_by_pk[tablename][pk_value] = obj

        # apply updates
        for tablename, updates in self._to_update.items():
            for pk_value, data in updates:
                if pk_value not in self.data_by_pk[tablename].keys():
                    raise Exception(f"Could not find item with PK value {pk_value} in table {tablename}")

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
        for obj, original_values in self._fetched:
            for field, value in original_values.items():
                setattr(obj, field, value)

        self._fetched.clear()

    def get_by_primary_key(self, entity, id):
        tablename = entity.__tablename__
        if tablename not in self.data_by_pk:
            return None

        return self.data_by_pk[tablename].get(id)

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
