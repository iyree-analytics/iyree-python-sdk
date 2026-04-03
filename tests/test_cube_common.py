"""Tests for cube/_common.py — pure unit tests."""

from __future__ import annotations

import time

import pytest

from iyree.cube._common import (
    build_load_params,
    compute_continue_wait_delay,
    is_continue_wait_response,
    is_token_expired,
    needs_multi_query,
    parse_jwt_expiry,
    parse_load_response,
    parse_token_response,
    serialize_query,
)
from iyree.cube._querybuilder import (
    Cube,
    DateRange,
    Query,
    TimeGranularity,
    TimeDimension,
)

from _helpers import make_jwt


class TestParseJwtExpiry:
    def test_valid_token(self):
        token = make_jwt(exp=1700000000)
        assert parse_jwt_expiry(token) == 1700000000.0

    def test_invalid_format(self):
        with pytest.raises(ValueError, match="expected 3"):
            parse_jwt_expiry("not.a.valid.jwt.token")


class TestIsTokenExpired:
    def test_not_expired(self):
        assert is_token_expired(time.time() + 120) is False

    def test_expired(self):
        assert is_token_expired(time.time() - 10) is True

    def test_within_buffer(self):
        assert is_token_expired(time.time() + 30, buffer_seconds=60) is True


class TestContinueWait:
    def test_is_continue_wait(self):
        assert is_continue_wait_response({"error": "Continue wait"}) is True

    def test_not_continue_wait(self):
        assert is_continue_wait_response({"data": []}) is False
        assert is_continue_wait_response({"error": "Some other error"}) is False

    def test_delay_sequence(self):
        assert compute_continue_wait_delay(0) == 0.5
        assert compute_continue_wait_delay(1) == 1.0
        assert compute_continue_wait_delay(2) == 2.0
        assert compute_continue_wait_delay(5) == 2.0


class TestSerializeQuery:
    def test_dict_passthrough(self):
        q = {"measures": ["orders.count"]}
        assert serialize_query(q) is q

    def test_query_object(self):
        orders = Cube("orders")
        q = Query(measures=[orders.measure("count")])
        result = serialize_query(q)
        assert result["measures"] == ["orders.count"]


class TestNeedsMultiQuery:
    def test_no_compare(self):
        orders = Cube("orders")
        q = Query(
            measures=[orders.measure("count")],
            time_dimensions=[
                TimeDimension(
                    orders.dimension("created_at"),
                    date_range=DateRange(relative="last 7 days"),
                )
            ],
        )
        assert needs_multi_query(q) is False

    def test_with_compare(self):
        orders = Cube("orders")
        q = Query(
            measures=[orders.measure("count")],
            time_dimensions=[
                TimeDimension(
                    orders.dimension("created_at"),
                    compare_date_range=[
                        DateRange(relative="this week"),
                        DateRange(relative="last week"),
                    ],
                )
            ],
        )
        assert needs_multi_query(q) is True

    def test_dict_returns_false(self):
        assert needs_multi_query({"measures": ["orders.count"]}) is False


class TestParseTokenResponse:
    def test_extracts_token(self):
        data = {"data": {"token": "eyJ..."}}
        assert parse_token_response(data) == "eyJ..."


class TestParseLoadResponse:
    def test_flat_response(self):
        data = {
            "data": [{"orders.count": 42}],
            "annotation": {"measures": {}},
            "query": {"measures": ["orders.count"]},
        }
        result = parse_load_response(data)
        assert result.data == [{"orders.count": 42}]

    def test_results_array_response(self):
        data = {
            "results": [{
                "data": [{"orders.count": 5}],
                "annotation": {},
                "query": {},
            }]
        }
        result = parse_load_response(data)
        assert result.data == [{"orders.count": 5}]
