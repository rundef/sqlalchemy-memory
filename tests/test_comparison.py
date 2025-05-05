import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload, joinedload

from models import ProductWithIndex, Vendor

class TestComparison:
    @pytest.mark.parametrize("query_lambda", [
        lambda s: s.execute(select(ProductWithIndex)),
        lambda s: s.execute(select(ProductWithIndex.id, ProductWithIndex.name)),
        lambda s: s.query(ProductWithIndex),
        lambda s: s.query(ProductWithIndex.id, ProductWithIndex.name), # fails (OK.)
        lambda s: s.execute(select(ProductWithIndex.id)).scalars(), # fails (OK. projection not yet done)
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
    ])
    async def test_select_same_as_sqlite(self, SessionFactory, sqlite_SessionFactory, query_lambda):
        with sqlite_SessionFactory() as session:
            vendor1 = Vendor(id=10, name="First vendor")
            session.add_all([
                vendor1,
                ProductWithIndex(id=1, name="foo", category="A", vendor_id=10, vendor=vendor1),
            ])
            session.commit()

            result_sqlite = list(query_lambda(session))

        with SessionFactory() as session:
            vendor1 = Vendor(id=10, name="First vendor")
            session.add_all([
                vendor1,
                ProductWithIndex(id=1, name="foo", category="A", vendor_id=10, vendor=vendor1),
            ])
            session.commit()

            result = list(query_lambda(session))

        assert type(result) == type(result_sqlite)
        assert len(result) == len(result_sqlite)
        assert len(result) > 0

        for r1, r2 in zip(result, result_sqlite):
            assert type(r1) == type(r2)
