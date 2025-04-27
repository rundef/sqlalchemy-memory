class MemoryDBAPIConnection:
    store = None

    def commit(self):
        self.store.commit()

    def rollback(self):
        self.store.rollback()

    def close(self):
        pass

