import pytest

from sqlalchemy import func, select, case

from models import ProductWithIndex, Vendor

class TestAggregation:
    @pytest.mark.parametrize("query_fn,expected", [
        (
            lambda: select(func.count(ProductWithIndex.price).label("count")),
            {"count": 3}
        ),
        (
            lambda: select(
                func.count(ProductWithIndex.id).label("count"),
                func.min(ProductWithIndex.id).label("minimum"),
                func.max(ProductWithIndex.id).label("maximum"),
                func.avg(ProductWithIndex.id).label("avg"),
                func.sum(ProductWithIndex.id),
            ),
            {
                "count": 3,
                "minimum": 1,
                "maximum": 3,
                "avg": 2,
                "sum": 6,
            }
        ),
    ])
    def test_select_aggr(self, SessionFactory, query_fn, expected):
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

            result = session.execute(query_fn()).mappings().one()

            assert result == expected

    def test_group_by(self, SessionFactory):
        with SessionFactory() as session:
            session.add_all([
                ProductWithIndex(id=1, name="foo", category="A", vendor_id=10),
                ProductWithIndex(id=2, name="bar", category="B", vendor_id=10),
                ProductWithIndex(id=3, name="foobar", category="B", vendor_id=20),
            ])
            session.commit()

            results = session.execute(select(ProductWithIndex).group_by(ProductWithIndex.vendor_id))
            results = results.scalars().all()

            assert len(results) == 2
            assert results[0].id == 1
            assert results[1].id == 3

            results = session.execute(
                select(
                    func.count(ProductWithIndex.id),
                    func.min(ProductWithIndex.id).label("minimum"),
                )
                .group_by(ProductWithIndex.vendor_id)
            )
            results = list(results)

            assert len(results) == 2

            assert results[0] == (2, 1)
            assert results[0].minimum == 1

            assert results[1] == (1, 3)
            assert results[1].minimum == 3
