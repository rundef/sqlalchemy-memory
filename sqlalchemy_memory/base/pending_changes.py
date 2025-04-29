from collections import defaultdict

class PendingChanges:
    def __init__(self):
        self._to_add = defaultdict(list)
        self._to_delete = defaultdict(list)
        self._to_update = defaultdict(list)
        self._fetched = defaultdict(dict)

    def clear(self):
        self.rollback()

    def rollback(self):
        self._to_add.clear()
        self._to_delete.clear()
        self._to_update.clear()
        self._fetched.clear()

    def add(self, obj, **kwargs):
        tablename = obj.__tablename__
        if not any(id(x) == id(obj) for x in self._to_add[tablename]):
            self._to_add[tablename].append(obj)

    def delete(self, obj):
        tablename = obj.__tablename__
        self._to_delete[tablename].append(obj)

    def update(self, tablename, pk_value, data):
        self._to_update[tablename].append((pk_value, data))

    def mark_as_fetched(self, obj, pk_value):
        tablename = obj.__tablename__

        if pk_value in self._fetched[tablename]:
            # Don't mark as fetched again
            return

        original_values = {
            col.name: getattr(obj, col.name)
            for col in obj.__table__.columns
        }
        self._fetched[tablename][pk_value] = original_values

    @property
    def dirty(self):
        return bool(self._to_add or self._to_delete or self._to_update)

    def flush_to(self, target):
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
            setattr(target, key, item.copy())
            item.clear()
