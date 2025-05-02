from collections import OrderedDict

class OrderedSet:
    def __init__(self):
        self._data = OrderedDict()

    def add(self, item):
        self._data[item] = None

    def discard(self, item):
        self._data.pop(item, None)

    def __contains__(self, item):
        return item in self._data

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __bool__(self):
        return bool(self._data)

    def remove(self, item):
        del self._data[item]

    def clear(self):
        self._data.clear()
