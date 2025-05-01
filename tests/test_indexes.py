import pytest
from unittest.mock import MagicMock
from sqlalchemy.sql import operators

from sqlalchemy_memory.base.indexes import HashIndex, RangeIndex, IndexManager

from models import ProductWithIndex

class TestIndexes:
    def test_hash_index(self):
        index = HashIndex()
        mock3 = MagicMock(id=3)

        index.add("table1", "activeIndex", True, MagicMock(id=1))
        index.add("table1", "activeIndex", False, MagicMock(id=2))
        index.add("table1", "activeIndex", False, mock3)
        index.add("table1", "activeIndex", True, MagicMock(id=4))

        index.add("table2", "activeIndex", True, MagicMock(id=100))

        results = index.query("table1", "activeIndex", True)
        assert {r.id for r in results} == {1, 4}

        results = index.query("table1", "activeIndex", False)
        assert {r.id for r in results} == {2, 3}

        index.remove("table1", "activeIndex", False, mock3)

        results = index.query("table1", "activeIndex", False)
        assert {r.id for r in results} == {2}

    def test_hash_compound_index(self):
        index = HashIndex()
        mock3 = MagicMock(id=3)

        index.add("table1", "active_category", (True, "A"), MagicMock(id=1))
        index.add("table1", "active_category", (True, "B"), MagicMock(id=2))
        index.add("table1", "active_category", (False, "A"), mock3)
        index.add("table1", "active_category", (False, "B"), MagicMock(id=4))

        results = index.query("table1", "active_category", (False, "A"))
        assert {r.id for r in results} == {3}

        index.remove("table1", "active_category", (False, "A"), mock3)

        results = index.query("table1", "active_category", (False, "A"))
        assert {r.id for r in results} == set()

    @pytest.mark.parametrize("query_kwargs,expected_ids", [
        ({"gt": 10}, {2, 3}),
        ({"gte": 20}, {2, 3}),
        ({"lt": 20}, {1}),
        ({"lte": 20}, {1, 3}),
        ({"gte": 15, "lte": 30}, {2, 3}),
        ({"gt": 5, "lt": 25}, {1, 3}),
        ({"gt": 10, "lte": 30}, {2, 3}),
        ({"gte": 10, "lt": 30}, {1, 3}),
        ({"gt": 30}, set()),
        ({"lt": 10}, set()),
        ({"lte": 10, "gt": 30}, set()),
    ])
    def test_range_index(self, query_kwargs, expected_ids):
        index = RangeIndex()

        objs = [
            MagicMock(id=1, price=10),
            MagicMock(id=2, price=30),
            MagicMock(id=3, price=20),
        ]

        for obj in objs:
            index.add("products", "price_index", obj.price, obj)

        results = index.query("products", "price_index", **query_kwargs)
        assert {r.id for r in results} == expected_ids

    @pytest.mark.parametrize("query_kwargs,expected_ids", [
        # All ES assets
        ({"gte": ("ES", -float("inf")), "lte": ("ES", float("inf"))}, {1, 2}),

        # ES assets with price > 10
        ({"gt": ("ES", 10), "lte": ("ES", float("inf"))}, {2}),

        # NQ assets with price <= 20
        ({"gte": ("NQ", -float("inf")), "lte": ("NQ", 20)}, {3}),

        # All between ("ES", 10) and ("NQ", 30)
        ({"gte": ("ES", 10), "lte": ("NQ", 30)}, {1, 2, 3}),

        # Nothing between ("ES", 40) and ("ES", 50)
        ({"gte": ("ES", 40), "lte": ("ES", 50)}, set()),

        # Full range
        ({"gte": ("ES", -float("inf")), "lte": ("NQ", float("inf"))}, {1, 2, 3, 4}),
    ])
    def test_compound_range_index(self, query_kwargs, expected_ids):
        index = RangeIndex()

        objs = [
            MagicMock(id=1, asset="ES", price=10),
            MagicMock(id=2, asset="ES", price=30),
            MagicMock(id=3, asset="NQ", price=20),
            MagicMock(id=4, asset="NQ", price=40),
        ]

        for obj in objs:
            key = (obj.asset, obj.price)
            index.add("products", "asset_price_index", key, obj)

        results = index.query("products", "asset_price_index", **query_kwargs)

        assert {r.id for r in results} == expected_ids

    @pytest.mark.parametrize("operator,value,expected", [
        (operators.eq, "A", 3),
        (operators.eq, "Z", 0),  # "Z" not present
        (operators.ne, "A", 7),
        (operators.ne, "Z", 10),  # "Z" not present
        (operators.in_op, ["A", "B"], 6),
        (operators.notin_op, ["A", "B"], 4),
        ("fallback", None, 10 / 3),  # 10/3
    ])
    def test_get_selectivity(self, operator, value, expected):
        tablename = "products"
        indexname = "ix_category"
        colname = "category"

        index_manager = IndexManager()
        index_manager.table_indexes = {
            tablename: {
                indexname: [colname]
            }
        }

        assert index_manager._column_to_index("nothing", "nothing") is None
        assert index_manager._column_to_index(tablename, "nothing") is None
        assert index_manager._column_to_index(tablename, colname) == indexname

        total_count = 10
        for category, count in zip(["A", "B", "C"], [3, 3, 4]):
            for _ in range(count):
                index_manager.hash_index.add(tablename, indexname, category, MagicMock())

        result = index_manager.get_selectivity(tablename, colname, operator, value, total_count)
        assert result == pytest.approx(expected)

    def test_synchronized_indexes(self, SessionFactory):
        tablename = ProductWithIndex.__tablename__

        with SessionFactory() as session:
            session.add_all([
                ProductWithIndex(
                    id=1,
                    active=True,
                    name="Hello",
                    category="A",
                    price=100,
                ),
                ProductWithIndex(
                    id=2,
                    active=True,
                    name="World",
                    category="B",
                    price=200,
                ),
            ])
            session.commit()

            store = session.store
            collection = store.data[tablename]

            assert len(store.query_index(collection, tablename, "active", operators.eq, True)) == 2
            assert len(store.query_index(collection, tablename, "active", operators.ne, True)) == 0
            assert len(store.query_index(collection, tablename, "active", operators.eq, False)) == 0
            assert len(store.query_index(collection, tablename, "active", operators.ne, False)) == 2

            assert len(store.query_index(collection, tablename, "category", operators.eq, "A")) == 1
            assert len(store.query_index(collection, tablename, "category", operators.eq, "B")) == 1
            assert len(store.query_index(collection, tablename, "category", operators.eq, "Z")) == 0

            # Assert nothing was changed on rollback
            item = session.get(ProductWithIndex, 2)
            item.category = "Z"
            session.rollback()
            assert len(store.query_index(collection, tablename, "category", operators.eq, "A")) == 1
            assert len(store.query_index(collection, tablename, "category", operators.eq, "B")) == 1
            assert len(store.query_index(collection, tablename, "category", operators.eq, "Z")) == 0

            # Assert index was synchronized after update
            item = session.get(ProductWithIndex, 2)
            item.category = "Z"
            session.commit()

            assert len(store.query_index(collection, tablename, "category", operators.eq, "A")) == 1
            assert len(store.query_index(collection, tablename, "category", operators.eq, "B")) == 0
            assert len(store.query_index(collection, tablename, "category", operators.eq, "Z")) == 1

            # Assert nothing was changed on rollback
            session.delete(collection[0])
            session.rollback()
            assert len(store.query_index(collection, tablename, "active", operators.eq, True)) == 2

            # Assert index was synchronized after deletion
            session.delete(collection[0])
            session.commit()
            assert len(store.query_index(collection, tablename, "active", operators.eq, True)) == 1
            assert len(store.query_index(collection, tablename, "active", operators.eq, False)) == 0
