from sqlalchemy.sql.elements import (
    UnaryExpression, BinaryExpression, BindParameter, ExpressionClauseList, BooleanClauseList,
    Grouping, True_, False_, Null
)
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql import operators
from sqlalchemy.sql.annotation import AnnotatedTable
from sqlalchemy.orm.query import Query
from functools import cached_property
import fnmatch

from ..logger import logger
from .resolvers import DateResolver, JsonExtractResolver

OPERATOR_ADAPTERS = {
    operators.is_: lambda value: lambda x, _: x is value,
    operators.isnot: lambda value: lambda x, _: x is not value,
    operators.like_op: lambda value: lambda x, _: fnmatch.fnmatchcase(x or '', value.replace('%', '*').replace('_', '?')),
    operators.not_like_op: lambda value: lambda x, _: not fnmatch.fnmatchcase(x or '', value.replace('%', '*').replace('_', '?')),
    operators.between_op: lambda bounds: lambda x, _: bounds[0] <= x <= bounds[1],
    operators.not_between_op: lambda bounds: lambda x, _: not (bounds[0] <= x <= bounds[1]),
    operators.in_op: lambda values: lambda x, _: x in values,
    operators.not_in_op: lambda values: lambda x, _: x not in values,
}

FUNCTION_RESOLVERS = {
    "date": DateResolver,
    "json_extract": JsonExtractResolver,
}

class MemoryQuery(Query):
    def __init__(self, entities, session):
        super().__init__(entities, session)
        self._model = entities[0]

        self._where_criteria = []
        self._order_by = []
        self._limit = None
        self._offset = None

    @property
    def store(self):
        return self.session.store

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

    def _apply_boolean_condition(self, cond: BooleanClauseList, collection):
        op = cond.operator  # and_ or or_

        # Recursively evaluate each sub-condition
        subresults = [
            set(self._apply_condition(subcond, collection))
            for subcond in cond.clauses
        ]

        if op is operators.and_:
            # Intersection: item must satisfy all sub-conditions
            result = set.intersection(*subresults)
        elif op is operators.or_:
            # Union: item can satisfy any sub-condition
            result = set.union(*subresults)
        else:
            raise NotImplementedError(f"Unsupported BooleanClauseList operator: {op}")

        return list(result)

    def _resolve_rhs(self, rhs):
        if isinstance(rhs, BindParameter):
            return rhs.value
        elif isinstance(rhs, True_):
            return True
        elif isinstance(rhs, False_):
            return False
        elif isinstance(rhs, Null):
            return None
        elif isinstance(rhs, ExpressionClauseList):
            return tuple(
                clause.value if isinstance(clause, BindParameter) else clause
                for clause in rhs.clauses
            )
        else:
            raise NotImplementedError(f"Unsupported RHS: {type(rhs)}")

    def _apply_binary_condition(self, cond: BinaryExpression, collection):
        # Extract the Python value it's being compared to
        value = self._resolve_rhs(cond.right)

        col = cond.left
        accessor = lambda obj, attr_name: getattr(obj, attr_name)

        if isinstance(cond.left, FunctionElement):
            fn_name = cond.left.name.lower()
            if fn_name not in FUNCTION_RESOLVERS:
                raise NotImplementedError(f"Unsupported LHS function: {fn_name}")

            clauses = list(cond.left.clauses)
            col = clauses[0]
            _class = FUNCTION_RESOLVERS[fn_name]
            _resolver = _class(clauses[1:])
            accessor = _resolver.accessor

        # Extract column name (LHS) and operator
        if not hasattr(col, "name"):
            raise NotImplementedError(f"Unsupported LHS: {col}")
        attr_name = col.name
        table_name = col.table.description
        if table_name != self.tablename:
            raise NotImplementedError(f"Unsupported condition on other table: {table_name} vs {self.tablename}")

        op = cond.operator

        # Use index if available
        index_result = self.store.query_index(collection, table_name, attr_name, op, value)
        if index_result is not None:
            return index_result

        if op in OPERATOR_ADAPTERS:
            op = OPERATOR_ADAPTERS[op](value)

        return [
            item for item in collection
            if op(accessor(item, attr_name), value)
        ]

    def _apply_condition(self, cond, collection):
        if isinstance(cond, Grouping):
            # Unwrap
            return self._apply_condition(cond.element, collection)

        if isinstance(cond, BinaryExpression):
            # Represent an expression that is ``LEFT <operator> RIGHT``
            return self._apply_binary_condition(cond, collection)

        if isinstance(cond, BooleanClauseList):
            # and_ / or_ expressions
            return self._apply_boolean_condition(cond, collection)

        raise NotImplementedError(f"Unsupported condition type: {type(cond)}")

    def _execute_query(self):
        collection = self.store.data.get(self.tablename, [])
        if not collection:
            logger.debug(f"Table '{self.tablename}' is empty")
            return collection

        # Apply conditions
        conditions = sorted(self._where_criteria, key=self._get_condition_selectivity)

        for condition in conditions:
            collection = self._apply_condition(condition, collection)

            if len(collection) == 0:
                # No need to go further
                return collection

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

            collection = sorted(collection, key=lambda x: getattr(x, col.name), reverse=reverse)

        # Apply offset
        if self._offset is not None:
            collection = collection[self._offset:]

        # Apply limit
        if self._limit is not None:
            collection = collection[:self._limit]

        return collection

    def _get_condition_selectivity(self, cond):
        total_count = self.store.count(self.tablename)

        if not isinstance(cond, BinaryExpression):
            return total_count

        col = cond.left
        if isinstance(col, FunctionElement):
            return total_count

        if not hasattr(col, "name"):
            return total_count

        value = self._resolve_rhs(cond.right)

        return self.store.index_manager.get_selectivity(
            tablename=self.tablename,
            colname=col.name,
            operator=cond.operator,
            value=value,
            total_count=total_count
        )
