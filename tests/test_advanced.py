from sqlalchemy import select, func, and_, or_, not_, case
from sqlalchemy.orm import joinedload, selectinload
from datetime import datetime, date
from sqlalchemy.sql.annotation import AnnotatedTable
import pytest

from models import Item, Product, ProductWithIndex, Vendor

class TestAdvanced:
    @pytest.mark.parametrize(
        "pattern, negate, expected_ids",
        [
            ("foo%", False, {1, 3}),  # starts with foo
            ("%foo", False, {1, 4}),  # ends with foo
            ("%foo%", False, {1, 3, 4}),  # contains foo
            ("foo", False, {1}),  # exactly foo
            ("%baz%", False, set()),  # no match
            ("%foo%", True, {2}),  # NOT LIKE contains foo
        ]
    )
    def test_like_patterns(self, SessionFactory, pattern, negate, expected_ids):
        with SessionFactory.begin() as session:
            session.add_all([
                Item(id=1, name="foo"),
                Item(id=2, name="bar"),
                Item(id=3, name="foobar"),
                Item(id=4, name="barfoo"),
            ])

        with SessionFactory() as session:
            if negate:
                stmt = select(Item).where(~Item.name.like(pattern))
            else:
                stmt = select(Item).where(Item.name.like(pattern))

            results = session.execute(stmt).scalars().all()
            assert {item.id for item in results} == expected_ids

    @pytest.mark.parametrize(
        "symbols, negate, expected_ids",
        [
            (["foo", "bar"], False, {1, 2}),  # IN list
            (["barfoo"], False, {4}),  # Single match
            (["baz"], False, set()),  # No match
            (["foo", "bar"], True, {3, 4}),  # NOT IN list
        ]
    )
    def test_in_filter(self, SessionFactory, symbols, negate, expected_ids):
        with SessionFactory() as session:
            with session.begin():
                session.add_all([
                    Item(id=1, name="foo"),
                    Item(id=2, name="bar"),
                    Item(id=3, name="foobar"),
                    Item(id=4, name="barfoo"),
                ])

        with SessionFactory() as session:
            if negate:
                stmt = select(Item).where(~Item.name.in_(symbols))
            else:
                stmt = select(Item).where(Item.name.in_(symbols))

            results = session.execute(stmt).scalars().all()
            assert {item.id for item in results} == expected_ids

    @pytest.mark.parametrize(
        "low, high, negate, expected_ids",
        [
            (1, 3, False, {1, 2, 3}),  # BETWEEN 1 and 3
            (2, 4, False, {2, 3, 4}),  # BETWEEN 2 and 4
            (5, 10, False, set()),  # No match
            (1, 3, True, {4}),  # NOT BETWEEN 1 and 3
        ]
    )
    def test_between_filter(self, SessionFactory, low, high, negate, expected_ids):
        with SessionFactory() as session:
            with session.begin():
                session.add_all([
                    Item(id=1, name="foo"),
                    Item(id=2, name="bar"),
                    Item(id=3, name="foobar"),
                    Item(id=4, name="barfoo"),
                ])

        with SessionFactory() as session:
            if negate:
                stmt = select(Item).where(~Item.id.between(low, high))
            else:
                stmt = select(Item).where(Item.id.between(low, high))

            results = session.execute(stmt).scalars().all()
            assert {item.id for item in results} == expected_ids

    @pytest.mark.parametrize(
        "pattern, value, expected_ids",
        [
            ("$.ref", 123, {1}),
            ("$.ref", 456, set()),
            ("$.subitem.prop", 456, set()),
            ("$.subitem.prop", "hello", {2}),
        ]
    )
    def test_json_extract_filter(self, SessionFactory, pattern, value, expected_ids):
        with SessionFactory() as session:
            with session.begin():
                session.add_all([
                    Product(id=1, name="foo", category="A", data=dict(ref=123)),
                    Product(id=2, name="bar", category="A", data=dict(
                        subitem=dict(prop="hello")
                    )),
                ])

        with SessionFactory() as session:
            stmt = select(Product).where(
                func.json_extract(Product.data, pattern) == value
            )

            results = session.execute(stmt).scalars().all()
            assert {item.id for item in results} == expected_ids

    def test_default_values(self, SessionFactory):
        dt = datetime(2025, 1, 1, 2, 3, 4)

        with SessionFactory() as session:
            session.add_all([
                Product(id=5, name="foo", category="A"),
                Product(name="bar", active=False, created_at=dt),
            ])
            session.commit()

            products = session.execute(select(Product)).scalars().all()

            assert products[0].active
            assert products[0].created_at is not None
            assert products[0].category == "A"

            assert products[1].id == 6
            assert not products[1].active
            assert products[1].created_at == dt
            assert products[1].category == "unknown"

    @pytest.mark.parametrize(
        "operator, value, expected_ids",
        [
            ("is", True, {1, 3}),
            ("is_not", True, {2}),
            ("is", False, {2}),
            ("is_not", False, {1, 3}),
        ]
    )
    def test_is_filter(self, SessionFactory, operator, value, expected_ids):
        with SessionFactory() as session:
            session.add_all([
                Product(id=1, name="foo", active=True),
                Product(id=2, name="bar", active=False),
                Product(id=3, name="foobar", active=True),
            ])
            session.commit()

            stmt = select(Product)
            if operator == "is":
                stmt = stmt.where(Product.active.is_(value))
            else:
                stmt = stmt.where(Product.active.is_not(value))

            results = session.execute(stmt).scalars().all()
            assert {item.id for item in results} == expected_ids

    @pytest.mark.parametrize(
        "operator, value, expected_ids",
        [
            ("==", date(2025, 1, 1), {1}),
            ("!=", date(2025, 1, 2), {1, 3, 4}),
            (">", date(2025, 1, 2), {3, 4}),
            (">", date(2025, 1, 10), set()),
        ]
    )
    def test_date_filter(self, SessionFactory, operator, value, expected_ids):
        with SessionFactory() as session:
            session.add_all([
                Product(id=1, name="foo", created_at=datetime(2025, 1, 1, 1, 1, 1)),
                Product(id=2, name="bar", created_at=datetime(2025, 1, 2, 2, 2, 2)),
                Product(id=3, name="foobar", created_at=datetime(2025, 1, 3, 3, 3, 3)),
                Product(id=4, name="barfoo", created_at=datetime(2025, 1, 4, 4, 4, 4)),
            ])
            session.commit()

            stmt = select(Product)
            if operator == "==":
                stmt = stmt.where(func.DATE(Product.created_at) == value)
            elif operator == "!=":
                stmt = stmt.where(func.DATE(Product.created_at) != value)
            elif operator == ">":
                stmt = stmt.where(func.DATE(Product.created_at) > value)

            results = session.execute(stmt).scalars().all()
            assert {item.id for item in results} == expected_ids

    @pytest.mark.parametrize("condition,expected_ids", [
        (
            (Product.id > 1) & ((Product.id < 4) | (Product.category == "A")),
            {2, 3, 5},
        ),
        (
            and_(
                Product.id > 1,
                or_(
                    Product.id < 4,
                    Product.category == "A"
                )
            ),
            {2, 3, 5},
        ),
        (
            not_(Product.category == "A"),
            {2, 3, 4}
        ),
        (
            or_(
                and_(
                    not_(Product.category == "A"), # 2,3,4
                    Product.id > 2 # 3,4
                ),
                and_(
                    Product.category == "A", # 1,5
                    not_(Product.id == 1)  # 2,3,4,5
                ),
            ),
            {3, 4, 5},
        ),
    ])
    def test_and_or_not(self, SessionFactory, condition, expected_ids):
        with SessionFactory() as session:
            session.add_all([
                Product(id=1, name="foo", category="A"),
                Product(id=2, name="bar", category="B"),
                Product(id=3, name="foobar", category="B"),
                Product(id=4, name="barfoo", category="B"),
                Product(id=5, name="boofar", category="A"),
            ])
            session.commit()

            stmt = (
                select(Product)
                .where(condition)
            )
            results = session.execute(stmt).scalars().all()
            assert {item.id for item in results} == expected_ids

    def test_session_inception(self, SessionFactory):
        with SessionFactory() as session1:
            session1.add(Item(id=1, name="foo"))
            session1.commit()

            with SessionFactory() as session2:
                results = session2.execute(select(Item)).scalars().all()
                assert len(results) == 1

    @pytest.mark.parametrize("query", [
        lambda: select(ProductWithIndex),
        lambda: select(ProductWithIndex.id, ProductWithIndex.name, ProductWithIndex.category),

        # Join shouldn't affect anything
        lambda: select(ProductWithIndex).options(joinedload(ProductWithIndex.vendor)),
        lambda: select(ProductWithIndex).options(selectinload(ProductWithIndex.vendor)),
        lambda: select(
            ProductWithIndex.id,
            ProductWithIndex.name,
            ProductWithIndex.category,
        ).join(ProductWithIndex.vendor)
    ])
    def test_select_subset_of_columns(self, SessionFactory, query):
        with SessionFactory() as session:
            vendor1 = Vendor(id=10, name="First vendor")
            vendor2 = Vendor(id=20, name="Second vendor")

            session.add_all([
                vendor1,
                vendor2,
            ])

            session.add_all([
                ProductWithIndex(id=1, name="foo", category="A", vendor_id=10, vendor=vendor1),
                ProductWithIndex(id=2, name="bar", category="B", vendor_id=10, vendor=vendor1),
                ProductWithIndex(id=3, name="foobar", category="B", vendor_id=20, vendor=vendor2),
            ])
            session.commit()

            if callable(query):
                query = query()

            results = session.execute(query)

            if isinstance(query._raw_columns[0], AnnotatedTable):
                results = results.scalars()

            # We get the objects straight back, no column selection
            assert {
                r.id: (r.name, r.category)
                for r in results
            } == {
                1: ("foo", "A"),
                2: ("bar", "B"),
                3: ("foobar", "B"),
            }

    @pytest.mark.parametrize("query, expected", [
        (
            # Labels
            select(
                ProductWithIndex.id.label("product_id"),
                ProductWithIndex.name.label("product_name"),
                Vendor.name.label("vendor_name"),
            ),
            [
                {"product_id": 1, "product_name": "foo", "vendor_name": "First vendor"},
                {"product_id": 2, "product_name": "bar", "vendor_name": "First vendor"},
                {"product_id": 3, "product_name": "foobar", "vendor_name": "Second vendor"},
            ]
        ),

        (
            # Simple case()
            select(
                ProductWithIndex.id,
                case(
                    (
                       ProductWithIndex.id >= 3, "High"
                    ),
                    (
                        ProductWithIndex.id < 2, "Low"
                    ),
                    else_="Medium"
                ).label("test"),
            ),
            [
                {"id": 1, "test": "Low"},
                {"id": 2, "test": "Medium"},
                {"id": 3, "test": "High"},
            ]
        ),
    ])
    def test_select_expressions(self, SessionFactory, query, expected):
        with SessionFactory() as session:
            vendor1 = Vendor(id=10, name="First vendor")
            vendor2 = Vendor(id=20, name="Second vendor")

            session.add_all([
                vendor1,
                vendor2,
            ])

            session.add_all([
                ProductWithIndex(id=1, name="foo", category="A", vendor_id=10, vendor=vendor1),
                ProductWithIndex(id=2, name="bar", category="B", vendor_id=10, vendor=vendor1),
                ProductWithIndex(id=3, name="foobar", category="B", vendor_id=20, vendor=vendor2),
            ])
            session.commit()

            results = session.execute(query)
            results = list(results)

            assert len(results) == len(expected)
            for idx, (result, expected_result) in enumerate(zip(results, expected)):
                for k, v in expected_result.items():
                    assert hasattr(result, k), f"Expected {k} to be in result, but keys are {result.__dict__.keys()}"
                    assert getattr(result, k) == v, f"Expected {k} to be == {v} for item #{idx}"
