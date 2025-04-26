from sqlalchemy import select, func
import pytest

from models import Item, Product

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
