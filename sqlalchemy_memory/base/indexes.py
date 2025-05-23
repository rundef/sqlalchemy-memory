from collections import defaultdict
from sortedcontainers import SortedDict
from typing import Any, List, Generator
from itertools import chain
from sqlalchemy.sql import operators

from ..helpers.ordered_set import OrderedSet


class IndexManager:
    __slots__ = ('hash_index', 'range_index', 'table_indexes', 'columns_mapping', )

    def __init__(self):
        self.hash_index = HashIndex()
        self.range_index = RangeIndex()

        self.table_indexes = {}
        self.columns_mapping = {}

    
    def get_indexes(self, obj):
        """
        Retrieve index from object's table as dict: indexname => list of column name
        """
        tablename = obj.__tablename__

        if tablename not in self.table_indexes:
            self.table_indexes[tablename] = {}

            pk_col_name = obj.__table__.primary_key.columns[0].name

            for index in obj.__table__.indexes:
                if len(index.expressions) > 1:
                    # Ignoring compound indexes for now ...
                    continue

                if index.name == pk_col_name:
                    pk_col_name = None

                self.table_indexes[tablename][index.name] = [
                    col.name
                    for col in index.expressions
                ]

            if pk_col_name:
                self.table_indexes[tablename][pk_col_name] = [pk_col_name]

        return self.table_indexes[tablename]


    def _column_to_index(self, tablename, colname):
        """
        Get index name from tablename & column name
        """
        if tablename not in self.columns_mapping:
            self.columns_mapping[tablename] = {}

        if colname not in self.columns_mapping[tablename]:
            for indexname, indexcols in self.table_indexes.get(tablename, {}).items():
                if colname in indexcols:
                    self.columns_mapping[tablename][colname] = indexname
                    return indexname

            self.columns_mapping[tablename][colname] = None


        return self.columns_mapping[tablename][colname]

    
    def _get_index_key(self, obj, columns):
        if len(columns) == 1:
            return getattr(obj, columns[0])
        return tuple(getattr(obj, c) for c in columns)

    def on_insert(self, obj):
        tablename = obj.__tablename__
        indexes = self.get_indexes(obj)

        for indexname, columns in indexes.items():
            value = self._get_index_key(obj, columns)

            self.hash_index.add(tablename, indexname, value, obj)
            self.range_index.add(tablename, indexname, value, obj)
    
    def on_delete(self, obj):
        tablename = obj.__tablename__
        indexes = self.get_indexes(obj)

        for indexname, columns in indexes.items():
            value = self._get_index_key(obj, columns)

            self.hash_index.remove(tablename, indexname, value, obj)
            self.range_index.remove(tablename, indexname, value, obj)

    def on_update(self, obj, updates):
        tablename = obj.__tablename__
        indexes = self.get_indexes(obj)

        for indexname, columns in indexes.items():
            if columns[0] not in updates:
                continue

            old_value = updates[columns[0]]["old"]
            new_value = updates[columns[0]]["new"]

            self.hash_index.remove(tablename, indexname, old_value, obj)
            self.range_index.remove(tablename, indexname, old_value, obj)

            self.hash_index.add(tablename, indexname, new_value, obj)
            self.range_index.add(tablename, indexname, new_value, obj)

    def query(self, collection, tablename, colname, operator, value, collection_is_full_table=False):
        indexname = self._column_to_index(tablename, colname)
        if not indexname:
            return None

        if operator == operators.eq:
            result = self.hash_index.query(tablename, indexname, value)
            if collection_is_full_table:
                return result
            return (item for item in collection if item in result)

        elif operator == operators.ne:
            excluded = self.hash_index.query(tablename, indexname, value)
            return (item for item in collection if item not in excluded)

        elif operator == operators.in_op:
            result = chain.from_iterable(
                self.hash_index.query(tablename, indexname, v) for v in value
            )
            if collection_is_full_table:
                return result
            result = set(result)
            return (item for item in collection if item in result)

        elif operator == operators.notin_op:
            excluded = set(chain.from_iterable(
                self.hash_index.query(tablename, indexname, v) for v in value
            ))
            return (item for item in collection if item not in excluded)

        elif operator == operators.gt:
            result = self.range_index.query(tablename, indexname, gt=value)
            if collection_is_full_table:
                return result
            result = set(result)
            return (item for item in collection if item in result)

        elif operator == operators.ge:
            result = self.range_index.query(tablename, indexname, gte=value)
            if collection_is_full_table:
                return result
            result = set(result)
            return (item for item in collection if item in result)

        elif operator == operators.lt:
            result = self.range_index.query(tablename, indexname, lt=value)
            if collection_is_full_table:
                return result
            result = set(result)
            return (item for item in collection if item in result)

        elif operator == operators.le:
            result = self.range_index.query(tablename, indexname, lte=value)
            if collection_is_full_table:
                return result
            result = set(result)
            return (item for item in collection if item in result)

        elif operator == operators.between_op and isinstance(value, (tuple, list)) and len(value) == 2:
            result = self.range_index.query(tablename, indexname, gte=value[0], lte=value[1])
            if collection_is_full_table:
                return result
            result = set(result)
            return (item for item in collection if item in result)

        elif operator == operators.not_between_op and isinstance(value, (tuple, list)) and len(value) == 2:
            in_range = set(self.range_index.query(tablename, indexname, gte=value[0], lte=value[1]))
            return (item for item in collection if item not in in_range)

    
    def get_selectivity(self, tablename, colname, operator, value, total_count):
        """
        Estimate the selectivity of a single WHERE condition.

        This method is used to rank or sort WHERE conditions by their estimated
        filtering power. A lower selectivity value indicates that the condition
        is expected to filter out more rows (i.e., fewer rows remain after applying it),
        making it more selective.
        """

        indexname = self._column_to_index(tablename, colname)
        if not indexname:
            # Column isn't indexed
            return total_count

        if indexname in self.hash_index.index[tablename]:
            index = self.hash_index.index[tablename][indexname]
            num_keys = len(index)

            if operator == operators.eq:
                return len(index.get(value, []))

            elif operator == operators.ne:
                matched = len(index.get(value, []))
                return total_count - matched

            elif operator == operators.in_op:
                return sum(len(index.get(v, [])) for v in value)

            elif operator == operators.notin_op:
                matched = sum(len(index.get(v, [])) for v in value)
                return total_count - matched

            return total_count / num_keys

        return total_count

class HashIndex:
    """
    A hash-based index structure for fast exact-match lookups on table columns.

    Structure:
        index[tablename][indexname][value] = [obj1, obj2, ...]

    Maintains insertion order of objects.
    """

    __slots__ = ('index',)

    def __init__(self):
        self.index = defaultdict(lambda: defaultdict(lambda: defaultdict(OrderedSet)))


    def add(self, tablename: str, indexname: str, value: Any, obj: Any):
        self.index[tablename][indexname][value].add(obj)


    def remove(self, tablename: str, indexname: str, value: Any, obj: Any):
        s = self.index[tablename][indexname][value]
        s.discard(obj)
        if not s:
            del self.index[tablename][indexname][value]

    def query(self, tablename: str, indexname: str, value: Any) -> List[Any]:
        return self.index[tablename][indexname].get(value, [])


class RangeIndex:
    """
    A range-based index for fast lookups using comparison operators.

    Internally uses SortedDict to allow efficient bisecting and slicing.
    Structure:
        index[tablename][indexname] = SortedDict { value: [obj1, obj2, ...] }
    """

    __slots__ = ('index',)

    def __init__(self):
        self.index = defaultdict(lambda: defaultdict(SortedDict))

    def add(self, tablename: str, indexname: str, value: Any, obj: Any):
        index = self.index[tablename][indexname]
        if value in index:
            index[value].append(obj)
        else:
            index[value] = [obj]

    
    def remove(self, tablename: str, indexname: str, value: Any, obj: Any):
        col = self.index[tablename][indexname]
        if value in col:
            try:
                col[value].remove(obj)
                if not col[value]:
                    del col[value]
            except ValueError:
                pass

    def query(self, tablename: str, indexname: str, gt=None, gte=None, lt=None, lte=None) -> Generator:
        sd = self.index[tablename][indexname]

        # Define range bounds
        min_key = gte if gte is not None else gt
        max_key = lte if lte is not None else lt
        inclusive_min = gte is not None
        inclusive_max = lte is not None

        keys = sd.irange(
            minimum=min_key,
            maximum=max_key,
            inclusive=(inclusive_min, inclusive_max)
        )

        return chain.from_iterable(sd[key] for key in keys)
