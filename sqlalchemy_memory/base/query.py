from sqlalchemy.sql.elements import (
    UnaryExpression, BinaryExpression, BindParameter, ExpressionClauseList, BooleanClauseList,
    Grouping, True_, False_, Null,
    Label, Case,
)
from sqlalchemy.sql.functions import FunctionElement
from sqlalchemy.sql import operators
from sqlalchemy.sql.annotation import AnnotatedTable, AnnotatedColumn
from sqlalchemy.sql.schema import Table
from sqlalchemy.sql.functions import Function
from sqlalchemy.sql.selectable import Select, Join
from sqlalchemy.sql.dml import Delete, Update
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.decl_api import DeclarativeMeta
from functools import cached_property
from itertools import tee, islice
import fnmatch

from ..logger import logger
from ..helpers.utils import _dedup_chain
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
    def __init__(self, statement, session):
        self.session = session
        self._statement = statement

    @property
    def store(self):
        return self.session.store

    @property
    def table(self):
        if isinstance(self._statement, (Update, Delete)):
            return self._statement.table

        if len(self._statement._from_obj) == 1:
            return self._statement._from_obj[0]

        # Attempt to extract table from raw columns (quicker)
        table = self._extract_table_from_raw_columns()
        if table is not None:
            return table

        from_clauses = self._statement.get_final_froms()
        if len(from_clauses) != 1:
            raise Exception(f"Only select statement with a single FROM clause are supported")

        from_clause = from_clauses[0]

        if isinstance(from_clause, Table):
            return from_clause

        if isinstance(from_clause, Join):
            return from_clause.left

        raise Exception(f"Unhandled SELECT FROM clause type: {type(from_clause)}")

    @cached_property
    def tablename(self):
        return self.table.name

    @cached_property
    def is_select(self):
        return isinstance(self._statement, Select)

    @cached_property
    def _limit(self):
        if self.is_select and self._statement._limit_clause is not None:
            return self._statement._limit_clause.value

    @cached_property
    def _offset(self):
        if self.is_select and self._statement._offset_clause is not None:
            return self._statement._offset_clause.value

    @cached_property
    def _order_by(self):
        if self.is_select:
            return self._statement._order_by_clauses
        return []

    @cached_property
    def _where_criteria(self):
        return self._statement._where_criteria

    def iter_items(self):
        gen = self._execute_query()
        gen = self._project(gen)
        return gen

    def first(self):
        gen = self.iter_items()
        try:
            return next(gen)
        except StopIteration:
            return None

    def all(self):
        gen = self.iter_items()
        return list(gen)

    def filter(self, condition):
        self._statement._where_criteria.append(condition)
        return self

    def _apply_boolean_condition(self, cond: BooleanClauseList, stream):
        op = cond.operator  # and_ or or_

        if op is operators.and_:
            # Apply filters sequentially to the current stream
            for subcond in cond.clauses:
                stream = self._apply_condition(subcond, stream)
            return stream

        op = cond.operator

        if op is operators.and_:
            for subcond in cond.clauses:
                stream = self._apply_condition(subcond, stream)

            return stream

        elif op is operators.or_:
            # Materialize the stream once and tee for each OR branch

            streams = tee(stream, len(cond.clauses))
            substreams = [
                self._apply_condition(subcond, s)
                for subcond, s in zip(cond.clauses, streams)
            ]
            return _dedup_chain(*substreams)

        raise NotImplementedError(f"Unsupported BooleanClauseList op: {op}")

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

    def _apply_binary_condition(self, cond: BinaryExpression, stream, is_first=False):
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
        index_result = self.store.query_index(stream, table_name, attr_name, op, value, collection_is_full_table=is_first)
        if index_result is not None:
            return index_result

        if op in OPERATOR_ADAPTERS:
            op = OPERATOR_ADAPTERS[op](value)

        return (item for item in stream if op(accessor(item, attr_name), value))

    def _apply_condition(self, cond, stream, is_first=False):
        if isinstance(cond, Grouping):
            # Unwrap
            return self._apply_condition(cond.element, stream)

        if isinstance(cond, BinaryExpression):
            # Represent an expression that is ``LEFT <operator> RIGHT``
            return self._apply_binary_condition(cond, stream, is_first=is_first)

        if isinstance(cond, BooleanClauseList):
            # and_ / or_ expressions
            return self._apply_boolean_condition(cond, stream)

        raise NotImplementedError(f"Unsupported condition type: {type(cond)}")

    def _execute_query(self):
        stream = iter(self.store.data.get(self.tablename, []))
        if not stream:
            logger.debug(f"Table '{self.tablename}' is empty")
            return []

        # Apply conditions
        conditions = sorted(self._where_criteria, key=self._get_condition_selectivity)
        for idx, condition in enumerate(conditions):
            stream = self._apply_condition(condition, stream, is_first=(idx == 0))

        # Apply order by
        if self._order_by:
            stream = list(stream)
            for clause in reversed(self._order_by):
                col = clause.element if isinstance(clause, UnaryExpression) else clause
                reverse = isinstance(clause, UnaryExpression) and clause.modifier is operators.desc_op
                stream = sorted(stream, key=lambda x: getattr(x, col.name), reverse=reverse)

        # Offset / limit
        if self._limit or self._offset:
            start = self._offset or 0
            stop = start + self._limit if self._limit else None
            stream = islice(stream, start, stop)

        return stream

    def _get_condition_selectivity(self, cond):
        """
        Estimate the selectivity of a single WHERE condition.

        This method is used to rank or sort WHERE conditions by their estimated
        filtering power. A lower selectivity value indicates that the condition
        is expected to filter out more rows (i.e., fewer rows remain after applying it),
        making it more selective.
        """
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

    def _extract_table_from_column(self, c):
        if isinstance(c, AnnotatedTable):
            return c

        if isinstance(c, AnnotatedColumn):
            return c.table

        if isinstance(c, DeclarativeMeta):
            # Old session.query(...) api
            return c.__table__

        if isinstance(c, Label):
            return self._extract_table_from_column(c.element)

        if isinstance(c, Function):
            clause = next(iter(c.clauses))
            return self._extract_table_from_column(clause)

    def _extract_table_from_raw_columns(self):
        _tables = [
            self._extract_table_from_column(c)
            for c in self._statement._raw_columns
        ]

        if len(set(_tables)) == 1:
            return _tables[0]

        _tables = list(set(_tables))
        # Try to find a "root" table by checking if it has relationship to other tables
        for candidate in _tables:
            others = set(_tables) - {candidate}
            candidate_columns = candidate.columns if hasattr(candidate, "columns") else []

            foreign = [
                fk.column.table
                for col in candidate_columns
                for fk in col.foreign_keys
            ]
            if all(other in foreign for other in others):
                return candidate

    def _project(self, stream):
        """
        Apply SELECT column projection to the final collection.

        Supports raw columns, labels, and simple aggregates.
        """

        if not self.is_select:
            return stream

        cols = self._statement._raw_columns
        group_by = self._statement._group_by_clauses

        # Bypass projection if this is a simple SELECT [table]
        if not group_by and all(isinstance(c, (AnnotatedTable, DeclarativeMeta, Join)) for c in cols):
            return stream

        if group_by or self._contains_aggregation_function(cols):
            grouped = {}
            if group_by:
                for item in stream:
                    key = tuple(getattr(item, col.name) for col in group_by)
                    grouped.setdefault(key, []).append(item)
            else:
                grouped = {
                    "_all_": [item for item in stream]
                }

            result = []
            for key, group_items in grouped.items():
                row = []
                for col in cols:
                    value = self._evaluate_column(col, group_items)
                    row.append(value)
                result.append(tuple(row))
            return result

        else:
            return (
                tuple(self._evaluate_column(col, [item]) for col in cols)
                for item in stream
            )

    def _contains_aggregation_function(self, cols):
        for c in cols:
            if isinstance(c, Label):
                c = c.element

            if isinstance(c, FunctionElement):
                if c.name.lower() in ["count", "sum", "min", "max", "avg"]:
                    return True

        return False

    def _evaluate_column(self, col, items):
        """
        Evaluate a column or expression over one or many items.
        """

        if isinstance(col, AnnotatedTable):
            return items[0]

        if isinstance(col, Label):
            return self._evaluate_column(col.element, items)

        if isinstance(col, AnnotatedColumn):

            if self.tablename == col.table.name:
                # Column belongs to the primary ORM model
                return getattr(items[0], col.name)

            else:
                # Column belongs to a related model
                item = items[0]
                rel_name = col.table.name  # e.g., 'vendors'
                # Find matching attribute on the main object
                for attr_name in vars(item):
                    attr = getattr(item, attr_name, None)
                    if hasattr(attr, "__table__") and attr.__table__.name == rel_name:
                        return getattr(attr, col.name)

                raise ValueError(f"Could not find related model '{rel_name}' on '{type(item).__name__}'")

        if isinstance(col, FunctionElement):
            fn_name = col.name.lower()
            col_expr = next(iter(col.clauses))
            values = [getattr(item, col_expr.name) for item in items]

            if fn_name == "count":
                return len(values)
            elif fn_name == "sum":
                return sum(values)
            elif fn_name == "min":
                return min(values)
            elif fn_name == "max":
                return max(values)
            elif fn_name == "avg":
                return sum(values) / len(values) if values else None
            else:
                raise NotImplementedError(f"Function not supported: {fn_name}")

        if isinstance(col, Case):
            for condition_expr, result_expr in col.whens:
                condition_value = self._evaluate_expression(condition_expr, items)
                if condition_value:
                    return self._evaluate_expression(result_expr, items)

            # No condition matched; return else_
            return self._evaluate_expression(col.else_, items)

        raise NotImplementedError(f"Column type not handled: {type(col)}")

    def _evaluate_expression(self, expr, items):
        """
        Evaluate an expression (which might be a Grouping, BinaryExpression, BindParameter, etc.).
        """

        if isinstance(expr, BindParameter):
            return expr.value

        if isinstance(expr, Grouping):
            return self._evaluate_expression(expr.element, items)

        if isinstance(expr, BinaryExpression):
            left = self._evaluate_expression(expr.left, items)
            right = self._evaluate_expression(expr.right, items)
            op = expr.operator
            return op(left, right)

        if isinstance(expr, AnnotatedColumn):
            return self._evaluate_column(expr, items)

        raise NotImplementedError(f"Unsupported expression type: {type(expr)}")

