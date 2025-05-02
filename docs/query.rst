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

Indexes
-------

- Indexes are supported for single columns.

- Compound (multi-column) indexes are not supported.

**SELECT** queries are optimized using available indexes to speed up equality and range-based lookups.