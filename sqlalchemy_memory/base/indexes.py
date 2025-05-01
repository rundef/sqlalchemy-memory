from collections import defaultdict
from sortedcontainers import SortedDict
from typing import Any, List
from sqlalchemy.sql import operators


class IndexManager:
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

            for index in obj.__table__.indexes:
                if len(index.expressions) > 1:
                    # Ignoring compound indexes for now ...
                    continue

                self.table_indexes[tablename][index.name] = [
                    col.name
                    for col in index.expressions
                ]

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

    def query(self, collection, tablename, colname, operator, value):
        indexname = self._column_to_index(tablename, colname)
        if not indexname:
            return None

        # Use hash index for = / != / IN / NOT IN operators
        if operator == operators.eq:
            result = self.hash_index.query(tablename, indexname, value)
            return list(set(result) & set(collection))

        elif operator == operators.ne:
            # All values except the given one
            excluded = self.hash_index.query(tablename, indexname, value)
            return list(set(collection) - set(excluded))

        elif operator == operators.in_op:
            result = []
            for v in value:
                result.extend(self.hash_index.query(tablename, indexname, v))
            return list(set(result) & set(collection))

        elif operator == operators.notin_op:
            excluded = []
            for v in value:
                excluded.extend(self.hash_index.query(tablename, indexname, v))
            return list(set(collection) - set(excluded))

        # Use range index
        if operator == operators.gt:
            result = self.range_index.query(tablename, indexname, gt=value)
            return list(set(result) & set(collection))

        elif operator == operators.ge:
            result = self.range_index.query(tablename, indexname, gte=value)
            return list(set(result) & set(collection))

        elif operator == operators.lt:
            result = self.range_index.query(tablename, indexname, lt=value)
            return list(set(result) & set(collection))

        elif operator == operators.le:
            result = self.range_index.query(tablename, indexname, lte=value)
            return list(set(result) & set(collection))

        elif operator == operators.between_op and isinstance(value, (tuple, list)) and len(value) == 2:
            result = self.range_index.query(tablename, indexname, gte=value[0], lte=value[1])
            return list(set(result) & set(collection))

        elif operator == operators.not_between_op and isinstance(value, (tuple, list)) and len(value) == 2:
            in_range = self.range_index.query(tablename, indexname, gte=value[0], lte=value[1])
            return list(set(collection) - set(in_range))

    def get_selectivity(self, tablename, colname, operator, value, total_count):
        """
        Estimate selectivity: higher means worst filtering.
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

    def __init__(self):
        self.index = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    def add(self, tablename: str, indexname: str, value: Any, obj: Any):
        self.index[tablename][indexname][value].append(obj)

    def remove(self, tablename: str, indexname: str, value: Any, obj: Any):
        lst = self.index[tablename][indexname][value]
        try:
            lst.remove(obj)
            if not lst:
                del self.index[tablename][indexname][value]
        except ValueError:
            pass

    def query(self, tablename: str, indexname: str, value: Any) -> List[Any]:
        return self.index[tablename][indexname].get(value, [])


class RangeIndex:
    """
    A range-based index for fast lookups using comparison operators.

    Internally uses SortedDict to allow efficient bisecting and slicing.
    Structure:
        index[tablename][indexname] = SortedDict { value: [obj1, obj2, ...] }
    """

    def __init__(self):
        self.index = defaultdict(lambda: defaultdict(SortedDict))

    def add(self, tablename: str, indexname: str, value: Any, obj: Any):
        self.index[tablename][indexname].setdefault(value, []).append(obj)

    def remove(self, tablename: str, indexname: str, value: Any, obj: Any):
        col = self.index[tablename][indexname]
        if value in col:
            try:
                col[value].remove(obj)
                if not col[value]:
                    del col[value]
            except ValueError:
                pass

    def query(self, tablename: str, indexname: str, gt=None, gte=None, lt=None, lte=None) -> List[Any]:
        sd = self.index[tablename][indexname]

        # Define range bounds
        min_key = gte if gte is not None else gt
        max_key = lte if lte is not None else lt
        inclusive_min = gte is not None
        inclusive_max = lte is not None

        irange = sd.irange(
            minimum=min_key,
            maximum=max_key,
            inclusive=(inclusive_min, inclusive_max)
        )

        result = []
        for key in irange:
            result.extend(sd[key])

        return result
