class MemoryCursor:
    """
    A fake DBAPI cursor that provides description, rowcount, fetchall, and close.
    """
    def __init__(self, connection):
        self.connection = connection
        self._rows = []
        self.description = None
        self.rowcount = -1

    def execute(self, statement, parameters=None):
        print("EXECUTE", statement)
        assert False
        return self

    def fetchall(self):
        return self._rows

    def close(self):
        pass
