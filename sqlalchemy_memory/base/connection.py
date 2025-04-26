from .store import InMemoryStore

class MemoryDBAPIConnection:
    def __init__(self):
        self.store = InMemoryStore()

    def commit(self):
        self.store.commit()

    def rollback(self):
        self.store.rollback()

    def close(self):
        pass
