"""

## 1. Core Queries (SQLAlchemy 2.0 Core)

```python
from sqlalchemy import (
    create_engine,
    MetaData,
    Table, Column, Integer, String,
    select, insert, update, delete,
)
# 1) Setup
engine = create_engine("sqlite:///:memory:", future=True)
metadata = MetaData()

users = Table(
    "users", metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
    Column("age", Integer),
)

metadata.create_all(engine)

# 2) INSERT
with engine.begin() as conn:
    conn.execute(
        insert(users),
        [
            {"name": "alice", "age": 30},
            {"name": "bob",   "age": 25},
        ]
    )

# 3) SELECT
with engine.connect() as conn:
    stmt = select(users).where(users.c.age > 20)
    result = conn.execute(stmt)
    rows = result.all()  # list of Row objects
    for row in rows:
        print(row.id, row.name, row.age)

# 4) UPDATE
with engine.begin() as conn:
    stmt = (
        update(users)
        .where(users.c.name == "alice")
        .values(age=31)
        .returning(users.c.id, users.c.age)
    )
    updated = conn.execute(stmt).all()

# 5) DELETE
with engine.begin() as conn:
    stmt = delete(users).where(users.c.age < 28)
    result = conn.execute(stmt)
    print("deleted rows:", result.rowcount)









from sqlalchemy import select
from sqlalchemy.orm import (
    declarative_base, Mapped, mapped_column,
    sessionmaker,
)

Base = declarative_base()

class Item(Base):
    __tablename__ = "items"
    id:   Mapped[int]    = mapped_column(primary_key=True)
    x:    Mapped[int]
    y:    Mapped[int | None]

engine  = create_engine("sqlite:///:memory:", future=True)
Session = sessionmaker(engine, future=True)

Base.metadata.create_all(engine)
session = Session()

# add some sample data
session.add_all([
    Item(x=1, y=None),
    Item(x=1, y=10),
    Item(x=2, y=None),
])
session.commit()

# Filter: x == 1 AND y IS NULL
stmt = select(Item).where(
    Item.x == 1,
    Item.y.is_(None)
)
items = session.scalars(stmt).all()
# -> returns all Item instances matching those conditions



from sqlalchemy import func, select

# Core style:
stmt_core = select(
    func.max(users.c.age).label("max_age"),
    func.min(users.c.age).label("min_age"),
)
with engine.connect() as conn:
    max_age, min_age = conn.execute(stmt_core).one()

# ORM style:
stmt_orm = select(
    func.max(Item.x).label("max_x"),
    func.min(Item.y).label("min_y"),
)
max_x, min_y = session.execute(stmt_orm).one()




from sqlalchemy import func
stmt = select(Item).where(
    func.json_extract(Item.data, "$.key") == "value"
)



stmt = select(Item).order_by(Item.id).limit(10).offset(20)
"""