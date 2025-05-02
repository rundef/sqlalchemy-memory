Benchmark Comparison
====================

This benchmark compares `sqlalchemy-memory` to `in-memory SQLite` using 20,000 inserted items and a series of 500 queries, updates, and deletions.

As the results show, `sqlalchemy-memory` **excels in read-heavy workloads**, delivering significantly faster query performance. While SQLite performs slightly better on update and delete operations, the overall runtime of `sqlalchemy-memory` remains substantially lower, making it a strong choice for prototyping and simulation.

.. list-table::
   :header-rows: 1
   :widths: 25 25 25

   * - Operation
     - SQLite (in-memory)
     - sqlalchemy-memory
   * - Insert
     - 3.17 sec
     - 2.70 sec
   * - 500 Select Queries
     - 26.37 sec
     - 2.94 sec
   * - 500 Updates
     - 0.26 sec
     - 1.12 sec
   * - 500 Deletes
     - 0.09 sec
     - 0.90 sec
   * - **Total Runtime**
     - **29.89 sec**
     - **7.66 sec**
