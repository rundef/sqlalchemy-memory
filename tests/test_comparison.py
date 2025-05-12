import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload, joinedload
from collections.abc import Iterable

from models import ProductWithIndex, Vendor

def is_iterable(obj):
    return isinstance(obj, Iterable) and not isinstance(obj, (str, bytes))

class TestComparison:
    @pytest.mark.parametrize("query_lambda", [
        lambda s: s.execute(select(ProductWithIndex)),
        lambda s: s.execute(select(ProductWithIndex.id, ProductWithIndex.name)),
        lambda s: s.query(ProductWithIndex),
        lambda s: s.query(ProductWithIndex.id, ProductWithIndex.name),
        lambda s: s.execute(select(ProductWithIndex.id, ProductWithIndex.name)).scalars(),
        lambda s: s.execute(select(ProductWithIndex.id, ProductWithIndex.name)).scalar(),
        lambda s: s.execute(select(ProductWithIndex).options(selectinload(ProductWithIndex.vendor))),
        lambda s: s.execute(select(ProductWithIndex).options(joinedload(ProductWithIndex.vendor))),

        lambda s: s.execute(select(
            ProductWithIndex,
            Vendor,
        ).join(ProductWithIndex.vendor)),

        lambda s: s.execute(
            select(
                ProductWithIndex.id.label("product_id"),
                ProductWithIndex.name.label("product_name"),
                Vendor.name.label("vendor_name"),
            ).join(ProductWithIndex.vendor)
        ),

        lambda s: s.execute(select(ProductWithIndex).group_by(ProductWithIndex.category)),
        lambda s: s.execute(select(ProductWithIndex.id, ProductWithIndex.name).group_by(ProductWithIndex.category)),
    ])
    async def test_select_same_as_sqlite(self, SessionFactory, sqlite_SessionFactory, query_lambda):
        with sqlite_SessionFactory() as session:
            vendor1 = Vendor(id=10, name="First vendor")
            session.add_all([
                vendor1,
                ProductWithIndex(id=1, name="foo", category="A", vendor_id=10, vendor=vendor1),
                ProductWithIndex(id=2, name="foo", category="A", vendor_id=10, vendor=vendor1),
                ProductWithIndex(id=3, name="foo", category="B", vendor_id=10, vendor=vendor1),
            ])
            session.commit()

            result_sqlite = query_lambda(session)

            _original_type = type(result_sqlite)
            if is_iterable(result_sqlite):
                result_sqlite = list(result_sqlite)

        with SessionFactory() as session:
            vendor1 = Vendor(id=10, name="First vendor")
            session.add_all([
                vendor1,
                ProductWithIndex(id=1, name="foo", category="A", vendor_id=10, vendor=vendor1),
                ProductWithIndex(id=2, name="foo", category="A", vendor_id=10, vendor=vendor1),
                ProductWithIndex(id=3, name="foo", category="B", vendor_id=10, vendor=vendor1),
            ])
            session.commit()

            result = query_lambda(session)
            _type = type(result)

            if is_iterable(result):
                result = list(result)

        assert _type == _original_type

        if is_iterable(result):
            assert len(result) == len(result_sqlite)
            assert len(result) > 0

            for r1, r2 in zip(result, result_sqlite):
                assert type(r1) == type(r2)

        else:
            assert result == result_sqlite

