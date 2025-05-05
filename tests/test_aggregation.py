import pytest

from sqlalchemy import func, select, case

from models import ProductWithIndex, Vendor

class TestAggregation:
    @pytest.mark.parametrize("query,expected", [
        (
            select(func.count(ProductWithIndex.price).label("count")),
            [{"count": 5}]
        ),
        (
            select(
                func.count(ProductWithIndex.id).label("count"),
                func.min(ProductWithIndex.id).label("minimum"),
                func.max(ProductWithIndex.id).label("maximum"),
                func.avg(ProductWithIndex.id).label("avg"),
                func.sum(ProductWithIndex.id),
            ),
            [{
                "count": 3,
                "minimum": 1,
                "maximum": 3,
                "avg": 2,
                "wtf": 6,
            }]
        ),
        (
            # More complex case()
            select(
                func.sum(
                    case([
                        (ProductWithIndex.category == 'A', ProductWithIndex.vendor_id - ProductWithIndex.id),
                        (ProductWithIndex.category == 'B', ProductWithIndex.id - ProductWithIndex.vendor_id)
                    ]) * ProductWithIndex.id
                ).label('final_value')
            ),
            [
                # ((10-1)*1) + ((2-10)*2) + ((3-20)*3)
                {"final_value": -58}
            ],
        )
    ])
    def test_select_aggr(self, SessionFactory, query, expected):
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

            result = session.execute(query).one()

            print(result)
            count_x, min_x, max_x, sum_x = result
            print(f"Count: {count_x}, Min: {min_x}, Max: {max_x}, Sum: {sum_x}")

    def test_group_by(self):
        return
        stmt = select(
            YourModel.category,
            func.count(YourModel.id),
            func.sum(YourModel.sales)
        ).group_by(YourModel.category)

    def test_having(self):
        return
        subq = (
            select(YourModel.category, func.sum(YourModel.sales).label("total_sales"))
            .group_by(YourModel.category)
        ).subquery()

        stmt = select(subq).where(subq.c.total_sales > 1000)