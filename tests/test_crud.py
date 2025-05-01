from sqlalchemy import select, insert, update, delete, desc

from models import Item


class TestCRUD:
    def test_insert(self, SessionFactory):
        with SessionFactory() as session:
            with session.begin():
                result = session.execute(insert(Item).values(name="foo"))
                assert result.rowcount == 1

                # Bulk insert
                result = session.execute(
                    insert(Item),
                    [dict(name="bar"), dict(name="fba")]
                )
                assert result.rowcount == 2

            with session.begin():
                items = session.scalars(select(Item)).all()

                assert len(items) == 3

                assert items[0].id == 1
                assert items[0].name == "foo"

                assert items[1].id == 2
                assert items[1].name == "bar"

                assert items[2].id == 3
                assert items[2].name == "fba"


    def test_update(self, SessionFactory):
        with SessionFactory() as session:
            with session.begin():
                session.add_all([
                    Item(id=1, name="foo"),
                    Item(id=2, name="bar"),
                    Item(id=3, name="three"),
                ])

        with SessionFactory() as session:
            with session.begin():
                item = session.get(Item, 2)
                item.name = "bar-modified"
                session.rollback()

            with session.begin():
                item = session.get(Item, 2)
                assert item.name == "bar"

                item.name = "bar-modified"
                session.commit()

                item = session.get(Item, 2)
                assert item.name == "bar-modified"

        with SessionFactory() as session:
            with session.begin():
                stmt = (
                    update(Item)
                    .where(Item.id != 2)
                    .values(name="hello")
                )
                result = session.execute(stmt)
                assert result.rowcount == 2

                result = session.execute(select(Item).order_by(Item.id))
                items = result.scalars().all()

                assert [item.name for item in items] == ["foo", "bar-modified", "three"]

                session.commit()

                result = session.execute(select(Item).order_by(Item.id))
                items = result.scalars().all()

                assert [item.name for item in items] == ["hello", "bar-modified", "hello"]

    def test_get(self, SessionFactory):
        with SessionFactory() as session:
            with session.begin():
                session.add_all([
                    Item(id=1, name="foo"),
                    Item(id=2, name="bar"),
                ])

            with session.begin():
                # Legacy style query: still supported
                items = session.query(Item).filter(Item.id == 2).all()
                assert len(items) == 1
                assert isinstance(items[0], Item)
                assert items[0].id == 2

                items = session.scalars(select(Item).filter(Item.id == 2)).all()
                assert len(items) == 1
                assert isinstance(items[0], Item)
                assert items[0].id == 2

                item = session.scalar(select(Item).filter(Item.id == 2))
                assert isinstance(item, Item)
                assert items[0].id == 2

                ret = session.execute(select(Item).filter(Item.id == 2)).one()
                item = ret[0]
                assert item is not None
                assert isinstance(item, Item)
                assert item.id == 2

                ret = session.get(Item, 1)
                assert ret is not None
                assert ret.id == 1

                ret = session.get(Item, 2)
                assert ret is not None
                assert ret.id == 2

                ret = session.get(Item, 3)
                assert ret is None

                # Test limit
                items = session.scalars(
                    select(Item).limit(1)
                ).all()
                assert len(items) == 1
                assert items[0].id == 1

                # Test offset
                items = session.scalars(
                    select(Item).limit(1).offset(1)
                ).all()
                assert len(items) == 1
                assert items[0].id == 2

                # Test order by
                items = session.scalars(
                    select(Item).order_by(desc(Item.id)).order_by(desc(Item.name))
                ).all()
                assert len(items) == 2
                assert items[0].id == 2
                assert items[1].id == 1

    def test_merge(self, SessionFactory):
        with SessionFactory() as session:
            with session.begin():
                session.add(Item(id=1, name="foo"))
                session.flush()
                session.commit()

        detached = Item(id=1, name="foo-modified")

        with SessionFactory() as session:
            with session.begin():
                session.merge(detached)

                item = session.get(Item, 1)
                assert item is not None
                assert item.name == "foo-modified"  # updated immediately

                session.rollback()

            with session.begin():
                item = session.get(Item, 1)
                assert item is not None
                assert item.name == "foo"  # rollback: restored to old value

                session.merge(detached)
                session.commit()

            with session.begin():
                item = session.get(Item, 1)
                assert item is not None
                assert item.name == "foo-modified"  # change now persisted

    def test_delete(self, SessionFactory):
        with SessionFactory() as session:
            with session.begin():
                session.add_all([
                    Item(id=1, name="foo"),
                    Item(id=2, name="bar"),
                    Item(id=3, name="three"),
                ])

        with SessionFactory() as session:
            with session.begin():
                result = session.execute(
                    delete(Item).where(Item.id < 3)
                )
                assert result.rowcount == 2

                items = session.scalars(select(Item)).all()
                assert len(items) == 3


        with SessionFactory() as session:
            items = session.scalars(select(Item)).all()
            assert len(items) == 1
            assert items[0].id == 3
