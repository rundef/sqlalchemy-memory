from sqlalchemy.sql.elements import BinaryExpression, BindParameter, True_, False_, Null
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql import operators
from functools import cached_property
from operator import eq

class MemoryQuery:
    def __init__(self, session, model, items, active_items=None):
        self._session = session
        self._model = model
        self._items = items
        self._active_items = active_items
        self._filtered = None
        self._conditions = []

    @cached_property
    def tablename(self):
        return self._model.__tablename__

    def filter(self, condition):
        self._conditions.append(condition)
        return self

    def _extract_json_value(self, data_dict, path):
        """
        Traverse nested keys for a JSON path like 'ref.abc.xyz'
        """
        current = data_dict or {}
        for key in path.split('.'):
            if not isinstance(current, dict):
                return None
            current = current.get(key)
            if current is None:
                return None
        return current

    def _apply_condition(self, cond, collection):
        if not isinstance(cond, BinaryExpression):
            raise NotImplementedError(f"Unsupported condition type: {type(cond)}")

        # Extract the Python value it's being compared to
        rhs = cond.right
        if isinstance(rhs, BindParameter):
            value = rhs.value
        elif isinstance(rhs, True_):
            value = True
        elif isinstance(rhs, False_):
            value = False
        elif isinstance(rhs, Null):
            value = None
        else:
            raise NotImplementedError(f"Unsupported RHS: {type(rhs)}")

        # Handle JSON extraction: func.json_extract(column, path)
        if isinstance(cond.left, FunctionElement) and cond.left.name.lower() == 'json_extract':
            args = list(cond.left.clauses)
            column_expr, path_expr = args[0], args[1]
            attr_name = column_expr.name
            # Determine raw path string
            raw = path_expr.value if hasattr(path_expr, 'value') else str(path_expr).strip('"')

            # Strip leading '$.' or '$'
            if raw.startswith('$.'):
                raw_path = raw[2:]
            elif raw.startswith('$'):
                raw_path = raw[1:]
            else:
                raw_path = raw

            # Compare nested value
            op = cond.operator
            return [
                item for item in collection
                if op(
                    self._extract_json_value(getattr(item, attr_name), raw_path),
                    value
                )
            ]

        # Extract column name (LHS) and operator
        col = cond.left
        if not hasattr(col, "name"):
            raise NotImplementedError(f"Unsupported LHS: {col}")
        attr_name = col.name
        table_name = col.table.description
        if table_name != self.tablename:
            raise NotImplementedError(f"Unsupported condition on other table: {table_name} vs {self.tablename}")

        op = cond.operator

        # special‑case SQL "IS NULL" and "IS NOT NULL"
        if value is None:
            if op is operators.is_:
                op = lambda x, y: x is None
            elif op is operators.isnot:
                op = lambda x, y: x is not None

        return [
            item for item in collection
            if op(getattr(item, attr_name), value)
        ]

    def _execute_query(self):
        collection = self._items

        # 1) Performance shortcut: look for an `active == True` filter
        if self._active_items is not None:
            for cond in self._conditions:
                if (
                    isinstance(cond, BinaryExpression)
                    and cond.left.name == "active"
                    and cond.left.table.description == self.tablename
                    and cond.operator is eq
                    and (
                        (isinstance(cond.right, BindParameter) and cond.right.value is True)
                        or isinstance(cond.right, True_)
                    )
                ):

                    collection = self._active_items
                    self._conditions.remove(cond)
                    break

        # 2) Apply all remaining conditions
        for condition in self._conditions:
            collection = self._apply_condition(condition, collection)

        self._filtered = collection

    def first(self):
        if self._filtered is None:
            self._execute_query()

        return self._filtered[0] if self._filtered else None

    def all(self):
        if self._filtered is None:
            self._execute_query()

        return self._filtered

    def update(self, values: dict) -> int:
        matches = self.all()

        for obj in matches:
            for col, new_val in values.items():
                # derive the attribute name from the ColumnElement
                # SQLAlchemy InstrumentedAttribute has .key; ColumnElements have .name
                attr = getattr(col, "key", None) or getattr(col, "name", None)
                if not attr:
                    raise ValueError(f"Cannot derive attribute for {col!r}")
                setattr(obj, attr, new_val)

            # buffer the merge so commit() can move things like active->inactive
            self._session.merge(obj)

        return len(matches)