# sqlalchemy-memory

[![PyPI - Version](https://img.shields.io/pypi/v/sqlalchemy-memory)](https://pypi.org/project/sqlalchemy-memory/)
[![CI](https://github.com/rundef/sqlalchemy-memory/actions/workflows/ci.yml/badge.svg)](https://github.com/rundef/sqlalchemy-memory/actions/workflows/ci.yml)
[![Documentation](https://app.readthedocs.org/projects/sqlalchemy-memory/badge/?version=latest)](https://sqlalchemy-memory.readthedocs.io/en/latest/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/sqlalchemy-memory)](https://pypistats.org/packages/sqlalchemy-memory)


**In‑memory SQLAlchemy 2.0 dialect for blazing‑fast prototyping**

A pure‑Python SQLAlchemy 2.0 dialect that runs entirely in RAM.
It avoids typical database I/O and ORM overhead while maintaining full compatibility with the SQLAlchemy 2.0 Core and ORM APIs.
Ideal for rapid prototyping, backtesting engines, simulations.

## Why ?

This project was inspired by the idea of building a **fast, introspectable, no-dependency backend** for SQLAlchemy.

It is useful for:

- Prototyping new applications

- Educational purposes

- Testing ORM logic without spinning up a real database engine

Unlike traditional in-memory solutions like SQLite, `sqlalchemy-memory` fully avoids serialization, connection pooling, and driver overhead, leading to much faster in-memory performance while keeping the familiar SQLAlchemy API.

It is also perfect for **applications that need a lightweight, high-performance store** compatible with SQLAlchemy, such as backtesting engines, simulators, or other tools where you don't want to maintain a separate in-memory layer alongside your database models.

Data is kept purely in RAM and is **volatile**: it is **not persisted across application restarts** and is **cleared when the engine is disposed**.

## Features

- **SQLAlchemy 2.0 support**: ORM & Core expressions, sync & async modes
- **Zero I/O overhead**: pure in‑RAM storage (`dict`/`list` under the hood)
- **Commit/rollback support**
- **Index support**: indexes are recognized and used for faster lookups
- **Merge and `get()` support**: like real SQLAlchemy behavior

## Installation

```bash
pip install sqlalchemy-memory
```

## Documentation

[See the official documentation for usage examples](https://sqlalchemy-memory.readthedocs.io/en/latest/)


## Status

Currently supports basic functionality equivalent to:

- SQLite in-memory behavior for ORM + Core queries

- `declarative_base()` model support

Coming soon:

- `func.count()` / aggregations

- Joins and relationships (limited)

- Compound indexes

- Better expression support in `update(...).values()` (e.g., +=)

## Testing

Simply run `make tests`

## License

This project is licensed under the MIT License.
See [LICENSE](LICENSE) for details.
