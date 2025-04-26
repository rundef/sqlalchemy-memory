from sqlalchemy.sql.elements import (
    UnaryExpression, BinaryExpression, BindParameter, ExpressionClauseList,
    True_, False_, Null
)
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql import operators
from sqlalchemy.sql.annotation import AnnotatedTable
from functools import cached_property
import fnmatch

from sqlalchemy.orm.query import Query

class MemoryQuery(Query):
    def __init__(self, entities, element):
        super().__init__(entities, element)
        assert len(entities) == 1, "Only single table queries are supported"

        self._model = entities[0]

        self._where_criteria = []
        self._order_by = []
        self._limit = None
        self._offset = None

    @cached_property
    def tablename(self):
        if isinstance(self._model, AnnotatedTable):
            return self._model.name
        return self._model.__tablename__

    def first(self):
        items = self._execute_query()
        return items[0] if items else None

    def all(self):
        items = self._execute_query()
        return items

    def filter(self, condition):
        self._where_criteria.append(condition)
        return self

    def limit(self, value):
        self._limit = value
        return self

    def offset(self, value):
        self._offset = value
        return self

    def order_by(self, clause):
        self._order_by.append(clause)
        return self

    def _extract_json_value(self, data_dict, path):
        # Traverse nested keys for a JSON path like 'ref.abc.xyz'
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
        elif isinstance(rhs, ExpressionClauseList):
            value = tuple(
                clause.value if isinstance(clause, BindParameter) else clause
                for clause in rhs.clauses
            )
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

        elif op is operators.like_op:
            fnmatch_pattern = value.replace('%', '*').replace('_', '?')
            op = lambda x, y: fnmatch.fnmatchcase(x or '', fnmatch_pattern)

        elif op is operators.not_like_op:
            fnmatch_pattern = value.replace('%', '*').replace('_', '?')
            op = lambda x, y: not fnmatch.fnmatchcase(x or '', fnmatch_pattern)

        elif op is operators.between_op:
            low, high = value
            op = lambda x, _: low <= x <= high

        elif op is operators.not_between_op:
            low, high = value
            op = lambda x, _: not (low <= x <= high)

        elif op is operators.in_op:
            op = lambda x, y: x in y

        elif op is operators.not_in_op:
            op = lambda x, y: x not in y

        return [
            item for item in collection
            if op(getattr(item, attr_name), value)
        ]

    def _execute_query(self):
        collection = self.session.store.data.get(self.tablename, [])
        if not collection:
            return collection

        # Apply conditions
        for condition in self._where_criteria:
            collection = self._apply_condition(condition, collection)

        # Apply order by
        for clause in reversed(self._order_by or []):
            reverse = False

            if isinstance(clause, UnaryExpression):
                if clause.modifier is operators.desc_op:
                    reverse = True
                elif clause.modifier is operators.asc_op:
                    reverse = False
                col = clause.element
            else:
                col = clause

            attr = col.name
            collection.sort(key=lambda x: getattr(x, attr), reverse=reverse)

        # Apply offset
        if self._offset is not None:
            collection = collection[self._offset:]

        # Apply limit
        if self._limit is not None:
            collection = collection[:self._limit]

        return collection
