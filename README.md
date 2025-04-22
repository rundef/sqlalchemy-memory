# sqlalchemy-memory

**In‑memory SQLAlchemy 2.0 dialect for blazing‑fast prototyping**  

A pure‑Python, zero‑configuration SQLAlchemy 2.0 dialect that lives entirely in RAM.
Ideal for rapid prototyping, backtesting, and demos-no external database required.

---

## Features

- **Zero setup**: just `create_engine("memory://")`
- **Full SQLAlchemy 2.0 support**: ORM & Core, sync & async modes   
- **High performance**: no de/serialization overhead
- **Pure‑Python store**: built on simple `dict`/`list` buffers under the hood  

---

## Installation

```bash
pip install sqlalchemy-memory
```

## Quickstart

```python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base, Mapped, mapped_column, select

engine  = create_engine("memory://", future=True)
Session = sessionmaker(engine, future=True)

Base = declarative_base()

class Item(Base):
    __tablename__ = "items"
    id:   Mapped[int]    = mapped_column(primary_key=True)
    name: Mapped[str]    = mapped_column()

Base.metadata.create_all(engine)

# Use just like any other SQLAlchemy engine:
session = Session()

# Add & commit
item = Item(id=1, name="foo")
session.add(item)
session.commit()

# Query (no SQL under the hood: objects come straight back)
items = session.scalars(select(Item)).all()
assert items[0] is item
assert items[0].name == "foo"

# Delete & commit
session.delete(item)
session.commit()

# Confirm gone
assert session.scalars(select(Item)).all() == []
```

## Testing

Simply run `pytest`

## License

This project is licensed under the MIT License.
See [LICENSE](LICENSE) for details.
