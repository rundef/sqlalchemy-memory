Benchmark Comparison
====================

This benchmark compares `sqlalchemy-memory` to `in-memory SQLite` using 20,000 inserted items and a series of 500 queries, updates, and deletions.

As the results show, `sqlalchemy-memory` **excels in read-heavy workloads**, delivering significantly faster query performance. While SQLite performs slightly better on update and delete operations, the overall runtime of `sqlalchemy-memory` remains substantially lower, making it a strong choice for prototyping and simulation.

`Check the benchmark script on GitHub <https://github.com/rundef/sqlalchemy-memory/blob/main/benchmark.py>`_

.. list-table::
   :header-rows: 1
   :widths: 25 25 25

   * - Operation
     - SQLite (in-memory)
     - sqlalchemy-memory
   * - Insert
     - 3.30 sec
     - **3.10 sec**
   * - 500 Select Queries (all())
     - 30.07 sec
     - **4.14 sec**
   * - 500 Select Queries (limit(5))
     - **0.24** sec
     - 0.30 sec
   * - 500 Updates
     - 0.25 sec
     - **0.19** sec
   * - 500 Deletes
     - **0.09** sec
     - **0.09** sec
   * - *Total Runtime*
     - 33.95 sec
     - **7.81 sec**
