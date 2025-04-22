from collections import defaultdict

class MemorySession:
    """
    Mimics SQLAlchemy Session but backs operations on an in-memory dict store.
    Buffers adds/deletes then applies or discards on commit/rollback.
    """

    def __init__(self, store):
        self.store = store
        self._to_add = defaultdict(list)
        self._to_delete = defaultdict(list)

    def add(self, obj):
        table = obj.__tablename__
        self._to_add[table].append(obj)

    def delete(self, obj):
        table = obj.__tablename__
        self._to_delete[table].append(obj)

    def commit(self):
        # apply deletes
        for table, objs in self._to_delete.items():
            data = self.store.data.get(table, [])
            self.store.data[table] = [row for row in data if not any(row.id == o.id for o in objs)]

        # apply adds
        for table, objs in self._to_add.items():
            data = self.store.data.setdefault(table, [])
            data.extend(objs)

        self._to_add.clear()
        self._to_delete.clear()

    def rollback(self):
        self._to_add.clear()
        self._to_delete.clear()
