from sqlalchemy import select
from sqlalchemy.orm import declarative_base, Mapped, mapped_column

Base = declarative_base()
class Item(Base):
    __tablename__ = "items"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()

    def __repr__(self):
        return f"Item(id={self.id} name={self.name})"

class TestBasic:
    def test_add_get_delete(self, SessionFactory):
        with SessionFactory() as session:
            item = Item(id=1, name="foo")
            session.add(item)

            # Assert nothing was added before the commit
            items = session.scalars(select(Item)).all()
            assert len(items) == 0

            session.commit()

            # Assert item was added
            items = session.scalars(select(Item)).all()
            assert len(items) == 1
            assert items[0].id == 1
            assert items[0].name == "foo"

            session.delete(item)

            # Assert nothing was deleted before commit
            items = session.scalars(select(Item)).all()
            assert len(items) == 1

            session.commit()

            # Assert item was deleted
            items = session.scalars(select(Item)).all()
            assert len(items) == 0

    def test_rollback(self, SessionFactory):
        with SessionFactory() as session:
            with session.begin():
                session.add(Item(id=1, name="foo"))
                session.rollback()

            with session.begin():
                session.add(Item(id=2, name="bar"))

            with session.begin():
                items = session.scalars(select(Item)).all()

                assert len(items) == 1
                assert items[0].id == 2
                assert items[0].name == "bar"


    async def test_async_add_get_delete(self, AsyncSessionFactory):
        async with AsyncSessionFactory() as session:
            item = Item(id=1, name="foo")
            session.add(item)

            # Assert nothing was added before the commit
            items = (await session.scalars(select(Item))).all()
            assert len(items) == 0

            await session.commit()

            # Assert item was added
            items = (await session.scalars(select(Item))).all()

            assert len(items) == 1
            assert items[0].id == 1
            assert items[0].name == "foo"

            await session.delete(item)

            # Assert nothing was deleted before commit
            items = (await session.scalars(select(Item))).all()
            assert len(items) == 1

            await session.commit()

            # Assert item was deleted
            items = (await session.scalars(select(Item))).all()
            assert len(items) == 0

    async def test_async_rollback(self, AsyncSessionFactory):
        async with AsyncSessionFactory() as session:

            async with session.begin():
                session.add(Item(id=1, name="foo"))
                await session.rollback()

            async with session.begin():
                session.add(Item(id=2, name="bar"))

            async with session.begin():
                items = (await session.scalars(select(Item))).all()

                assert len(items) == 1
                assert items[0].id == 2
                assert items[0].name == "bar"
