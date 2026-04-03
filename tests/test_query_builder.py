"""Tests for the vendored Cube query builder."""

from __future__ import annotations

import pytest

from iyree.cube._querybuilder import (
    And,
    Cube,
    DateRange,
    Dimension,
    Filter,
    FilterOperator,
    Measure,
    Or,
    Order,
    Query,
    Segment,
    TimeGranularity,
    TimeDimension,
)


class TestCubeFactory:
    def test_creates_measure(self):
        orders = Cube("orders")
        m = orders.measure("count")
        assert isinstance(m, Measure)
        assert str(m) == "orders.count"
        assert m.serialize() == "orders.count"

    def test_creates_dimension(self):
        orders = Cube("orders")
        d = orders.dimension("status")
        assert isinstance(d, Dimension)
        assert str(d) == "orders.status"

    def test_creates_segment(self):
        orders = Cube("orders")
        s = orders.segment("active")
        assert isinstance(s, Segment)
        assert str(s) == "orders.active"

    def test_equality(self):
        a = Cube("orders")
        b = Cube("orders")
        assert a == b
        assert a.measure("count") == b.measure("count")
        assert a.measure("count") != a.measure("total")


class TestDateRange:
    def test_absolute(self):
        dr = DateRange(start_date="2024-01-01", end_date="2024-01-31")
        assert dr.serialize() == ["2024-01-01", "2024-01-31"]

    def test_relative(self):
        dr = DateRange(relative="last 7 days")
        assert dr.serialize() == "last 7 days"

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            DateRange()


class TestTimeDimension:
    def test_serialize_with_date_range(self):
        orders = Cube("orders")
        td = TimeDimension(
            orders.dimension("created_at"),
            date_range=DateRange(relative="last 30 days"),
            granularity=TimeGranularity.day,
        )
        result = td.serialize()
        assert result["dimension"] == "orders.created_at"
        assert result["granularity"] == "day"
        assert result["dateRange"] == "last 30 days"

    def test_serialize_with_compare_date_range(self):
        orders = Cube("orders")
        td = TimeDimension(
            orders.dimension("created_at"),
            compare_date_range=[
                DateRange(relative="this week"),
                DateRange(relative="last week"),
            ],
            granularity=TimeGranularity.day,
        )
        result = td.serialize()
        assert "compareDateRange" in result
        assert len(result["compareDateRange"]) == 2

    def test_requires_date_range(self):
        orders = Cube("orders")
        with pytest.raises(ValueError):
            TimeDimension(orders.dimension("created_at"))


class TestFilter:
    def test_serialize(self):
        orders = Cube("orders")
        f = Filter(
            member=orders.dimension("status"),
            operator=FilterOperator.equals,
            values=["completed"],
        )
        result = f.serialize()
        assert result["member"] == "orders.status"
        assert result["operator"] == "equals"
        assert result["values"] == ["completed"]

    def test_no_values(self):
        orders = Cube("orders")
        f = Filter(
            member=orders.dimension("status"),
            operator=FilterOperator.is_set,
        )
        result = f.serialize()
        assert "values" not in result


class TestBooleanExpressions:
    def test_or(self):
        orders = Cube("orders")
        f1 = Filter(orders.dimension("status"), FilterOperator.equals, ["a"])
        f2 = Filter(orders.dimension("status"), FilterOperator.equals, ["b"])
        expr = Or(f1, f2)
        result = expr.serialize()
        assert "or" in result
        assert len(result["or"]) == 2

    def test_and(self):
        orders = Cube("orders")
        f1 = Filter(orders.dimension("status"), FilterOperator.equals, ["a"])
        f2 = Filter(orders.measure("count"), FilterOperator.gt, [10])
        expr = And(f1, f2)
        result = expr.serialize()
        assert "and" in result


class TestQuery:
    def test_minimal_serialize(self):
        orders = Cube("orders")
        q = Query(measures=[orders.measure("count")])
        result = q.serialize()
        assert result["measures"] == ["orders.count"]
        assert "limit" not in result
        assert "offset" not in result
        assert "timezone" not in result
        assert "ungrouped" not in result

    def test_full_serialize(self):
        orders = Cube("orders")
        q = Query(
            measures=[orders.measure("count")],
            dimensions=[orders.dimension("status")],
            time_dimensions=[
                TimeDimension(
                    orders.dimension("created_at"),
                    date_range=DateRange(relative="last 7 days"),
                    granularity=TimeGranularity.day,
                )
            ],
            filters=[
                Filter(orders.dimension("status"), FilterOperator.equals, ["completed"])
            ],
            segments=[orders.segment("active")],
            order=[(orders.measure("count"), Order.desc)],
            limit=100,
            offset=10,
        )
        result = q.serialize()
        assert len(result["measures"]) == 1
        assert len(result["dimensions"]) == 1
        assert len(result["timeDimensions"]) == 1
        assert len(result["filters"]) == 1
        assert len(result["segments"]) == 1
        assert result["order"] == [("orders.count", "desc")]
        assert result["limit"] == 100
        assert result["offset"] == 10
