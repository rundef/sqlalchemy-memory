from .store import InMemoryStore
from .cursor import MemoryCursor

class MemoryDBAPIConnection:
    def __init__(self):
        self.store = InMemoryStore()

    def cursor(self):
        return MemoryCursor(self)

    def commit(self):
        self.store.commit()

    def rollback(self):
        self.store.rollback()

    def close(self):
        pass
