# sqlalchemy-memory

[![PyPI - Version](https://img.shields.io/pypi/v/sqlalchemy-memory)](https://pypi.org/project/sqlalchemy-memory/)
[![CI](https://github.com/rundef/sqlalchemy-memory/actions/workflows/ci.yml/badge.svg)](https://github.com/rundef/sqlalchemy-memory/actions/workflows/ci.yml)
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
- **Merge and `get()` support**: like real SQLAlchemy behavior

## Installation

```bash
pip install sqlalchemy-memory
```

## Quickstart

```python
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column
from sqlalchemy_memory import MemorySession

engine = create_engine("memory://")
Session = sessionmaker(
    engine,
    class_=MemorySession,
    expire_on_commit=False,
)

Base = declarative_base()

class Item(Base):
    __tablename__ = "items"
    id:   Mapped[int]    = mapped_column(primary_key=True)
    name: Mapped[str]    = mapped_column()
    def __repr__(self):
        return f"Item(id={self.id} name={self.name})"

Base.metadata.create_all(engine)

# Use just like any other SQLAlchemy engine:
session = Session()

# Add & commit
item = Item(id=1, name="foo")
session.add(item)
session.commit()

# Query (no SQL under the hood: objects come straight back)
items = session.scalars(select(Item)).all()
print("Items", items)
assert items[0] is item
assert items[0].name == "foo"

# Delete & commit
session.delete(item)
session.commit()

# Confirm gone
assert session.scalars(select(Item)).all() == []
```

## Quickstart (async)

```python
import asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column
from sqlalchemy_memory import MemorySession, AsyncMemorySession

engine = create_async_engine("memory+asyncio://")
Session = sessionmaker(
    engine,
    class_=AsyncMemorySession,
    sync_session_class=MemorySession,
    expire_on_commit=False,
)

Base = declarative_base()

class Item(Base):
    __tablename__ = "items"
    id:   Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()

    def __repr__(self):
        return f"Item(id={self.id} name={self.name})"

Base.metadata.create_all(engine.sync_engine)

async def main():
    async with Session() as session:
        # Add & commit
        item = Item(id=1, name="foo")
        session.add(item)
        await session.commit()

        # Query (no SQL under the hood: objects come straight back)
        items = (await session.scalars(select(Item))).all()
        print("Items", items)
        assert items[0] is item
        assert items[0].name == "foo"

        # Delete & commit
        await session.delete(item)
        await session.commit()

        # Confirm gone
        assert (await session.scalars(select(Item))).all() == []

asyncio.run(main())
```

## Status

Currently supports basic functionality equivalent to:

- SQLite in-memory behavior for ORM + Core queries

- `declarative_base()` model support

Coming soon:

- `func.count()` / aggregations

- Joins and relationships (limited)

- Better expression support in `update(...).values()` (e.g., +=)

## Testing

Simply run `pytest`

## License

This project is licensed under the MIT License.
See [LICENSE](LICENSE) for details.
