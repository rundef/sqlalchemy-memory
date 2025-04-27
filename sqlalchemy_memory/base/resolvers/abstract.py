class FunctionResolver:
    def __init__(self, clauses):
        self.clauses = clauses

    def accessor(self, item, attr_name):
        raise NotImplementedError
