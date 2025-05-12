Query
=====

Supported Operators
-------------------

- Comparison: `==`, `!=`, `<`, `>`, `<=`, `>=`, `between`, `not between`
- Membership: `in`, `not in`
- Identity: `is`, `is not`
- Logical: `and`, `or`
- String: `like`, `not like`

Supported Functions
-------------------

- `DATE(column)`
- `func.json_extract(col, '$.expr')`
- Aggregation functions: - Aggregations: `func.count()` / `func.sum()` / `func.min()` / `func.max()` / `func.avg()`

Indexes
-------

- Indexes are supported for single columns.

- Compound (multi-column) indexes are not supported.

**SELECT** queries are optimized using available indexes to speed up equality and range-based lookups.