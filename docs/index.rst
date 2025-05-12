Welcome to sqlalchemy-memory's documentation!
=============================================

`sqlalchemy-memory` is a pure in-memory backend for SQLAlchemy 2.0 that supports both sync and async modes, with full compatibility for SQLAlchemy Core and ORM.

📦 GitHub: https://github.com/rundef/sqlalchemy-memory

Quickstart: sync example
------------------------

.. code-block:: python

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

Quickstart: async example
-------------------------

.. code-block:: python

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

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   insert
   query
   update
   delete
   commit_rollback
   benchmarks