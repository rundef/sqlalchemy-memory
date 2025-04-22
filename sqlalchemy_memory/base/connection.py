from .session import MemorySession
from .cursor import MemoryCursor

class MemoryDBAPIConnection:
    """
    Emulates a raw DBAPI connection for the in-memory store.
    Attaches a MemorySession for transactional operations.
    """
    def __init__(self):
        self.data = {}
        self._snapshot = {}
        self.session = MemorySession(self)

    def cursor(self):
        return MemoryCursor(self)

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def close(self):
        pass
