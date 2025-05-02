from collections import defaultdict

class PendingChanges:
    def __init__(self):
        self._to_add = defaultdict(list)
        self._to_delete = defaultdict(list)
        self._to_update = defaultdict(list)

        # Modifications done by the user, e.g.: instance.counter += 1
        self._modifications = defaultdict(dict)

    def clear(self):
        self.rollback()

    def rollback(self):
        self._to_add.clear()
        self._to_delete.clear()
        self._to_update.clear()
        self._modifications.clear()

    def add(self, obj, **kwargs):
        tablename = obj.__tablename__
        self._to_add[tablename].append(obj)

    def delete(self, obj):
        tablename = obj.__tablename__
        self._to_delete[tablename].append(obj)

    def update(self, tablename, pk_value, data):
        self._to_update[tablename].append((pk_value, data))

    @property
    def dirty(self):
        return bool(self._to_add or self._to_delete or self._to_update or self._modifications)

    def flush_to(self, target):
        to_transfer = [
            "_to_add",
            "_to_update",
            "_to_delete",
        ]
        for key in to_transfer:
            item = getattr(self, key)
            if not item:
                continue
            setattr(target, key, item.copy())
            item.clear()

    def mark_field_as_dirty(self, instance, colname, oldvalue, value):
        key = id(instance)
        if key not in self._modifications:
            self._modifications[key]["__instance"] = instance

        if colname in self._modifications[key]:
            self._modifications[key][colname][1] = value
        else:
            self._modifications[key][colname] = [oldvalue, value]
