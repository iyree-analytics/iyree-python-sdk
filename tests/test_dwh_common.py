"""Tests for dwh/_common.py — pure unit tests, no mocking required."""

from __future__ import annotations

import json

import pytest

from iyree._types import StreamLoadResult
from iyree.dwh._common import (
    build_raw_sql_request_body,
    build_sql_request_body,
    parse_ndjson_lines,
    parse_raw_sql_ndjson_lines,
    parse_stream_load_response,
    prepare_insert_data,
    prepare_stream_load_headers,
    validate_stream_load_status,
)
from iyree.exceptions import IyreeDuplicateLabelError, IyreeError, IyreeStreamLoadError


class TestBuildSqlRequestBody:
    def test_query_only(self):
        body = build_sql_request_body("SELECT 1")
        assert body == {"query": "SELECT 1"}

    def test_with_session_variables(self):
        body = build_sql_request_body("SELECT 1", {"parallelism": "4"})
        assert body == {"query": "SELECT 1", "sessionVariables": {"parallelism": "4"}}

    def test_empty_session_variables_omitted(self):
        body = build_sql_request_body("SELECT 1", {})
        assert "sessionVariables" not in body


class TestParseNdjsonLines:
    def test_full_response(self):
        lines = [
            '{"connectionId": 42}',
            '{"meta": [{"name": "id", "type": "INT"}, {"name": "name", "type": "VARCHAR"}]}',
            '{"data": [1, "Alice"]}',
            '{"data": [2, "Bob"]}',
            '{"statistics": {"scanRows": 2, "scanBytes": 100, "returnRows": 2}}',
        ]
        result = parse_ndjson_lines(lines)
        assert result.connection_id == 42
        assert len(result.columns) == 2
        assert result.columns[0].name == "id"
        assert result.columns[1].type == "VARCHAR"
        assert len(result.rows) == 2
        assert result.rows[0] == [1, "Alice"]
        assert result.statistics["returnRows"] == 2

    def test_empty_lines_skipped(self):
        lines = ["", '{"connectionId": 1}', "", '{"meta": []}', ""]
        result = parse_ndjson_lines(lines)
        assert result.connection_id == 1

    def test_failed_query_raises(self):
        lines = ['{"status": "FAILED", "msg": "Syntax error"}']
        with pytest.raises(IyreeError, match="Syntax error"):
            parse_ndjson_lines(lines)

    def test_to_dicts(self):
        lines = [
            '{"connectionId": 1}',
            '{"meta": [{"name": "a", "type": "INT"}, {"name": "b", "type": "VARCHAR"}]}',
            '{"data": [10, "x"]}',
            '{"statistics": {}}',
        ]
        result = parse_ndjson_lines(lines)
        dicts = result.to_dicts()
        assert dicts == [{"a": 10, "b": "x"}]


class TestPrepareStreamLoadHeaders:
    def test_csv_defaults(self):
        headers = prepare_stream_load_headers("my_table")
        assert headers["Expect"] == "100-continue"
        assert headers["format"] == "csv"
        assert headers["Content-Type"] == "text/csv"
        assert "column_separator" not in headers
        assert "label" not in headers

    def test_json_format(self):
        headers = prepare_stream_load_headers("t", format="json")
        assert headers["Content-Type"] == "application/json"
        assert headers["format"] == "json"

    def test_with_label_and_columns(self):
        headers = prepare_stream_load_headers(
            "t", label="load_001", columns=["a", "b"]
        )
        assert headers["label"] == "load_001"
        assert headers["columns"] == "a, b"

    def test_extra_headers(self):
        headers = prepare_stream_load_headers("t", max_filter_ratio="0.1")
        assert headers["max_filter_ratio"] == "0.1"


class TestPrepareInsertData:
    def test_bytes_passthrough(self):
        payload, fmt, cols, extra = prepare_insert_data(
            b"a,b\n1,2", "csv", None, ",", {}
        )
        assert payload == b"a,b\n1,2"
        assert fmt == "csv"

    def test_str_encoded(self):
        payload, _, _, _ = prepare_insert_data("hello", "csv", None, ",", {})
        assert payload == b"hello"

    def test_list_dict_converts_to_json(self):
        data = [{"a": 1}, {"a": 2}]
        payload, fmt, _, extra = prepare_insert_data(data, "csv", None, ",", {})
        assert fmt == "json"
        assert extra["strip_outer_array"] == "true"
        assert json.loads(payload) == data


class TestParseStreamLoadResponse:
    def test_success(self):
        data = {
            "TxnId": 123,
            "Label": "load_001",
            "Status": "Success",
            "Message": "OK",
            "NumberTotalRows": 100,
            "NumberLoadedRows": 100,
            "NumberFilteredRows": 0,
            "NumberUnselectedRows": 0,
            "LoadBytes": 5000,
            "LoadTimeMs": 150,
        }
        result = parse_stream_load_response(data)
        assert result.txn_id == 123
        assert result.status == "Success"
        assert result.number_loaded_rows == 100


class TestValidateStreamLoadStatus:
    def test_success_noop(self):
        result = StreamLoadResult(
            txn_id=1, label="l", status="Success", message="OK",
            number_total_rows=1, number_loaded_rows=1,
            number_filtered_rows=0, number_unselected_rows=0,
            load_bytes=10, load_time_ms=5,
        )
        validate_stream_load_status(result)

    def test_fail_raises(self):
        result = StreamLoadResult(
            txn_id=1, label="l", status="Fail", message="bad data",
            number_total_rows=1, number_loaded_rows=0,
            number_filtered_rows=0, number_unselected_rows=0,
            load_bytes=10, load_time_ms=5,
        )
        with pytest.raises(IyreeStreamLoadError, match="bad data"):
            validate_stream_load_status(result)

    def test_duplicate_label_raises(self):
        result = StreamLoadResult(
            txn_id=1, label="dup", status="Label Already Exists", message="",
            number_total_rows=0, number_loaded_rows=0,
            number_filtered_rows=0, number_unselected_rows=0,
            load_bytes=0, load_time_ms=0,
        )
        with pytest.raises(IyreeDuplicateLabelError):
            validate_stream_load_status(result)


# ---------------------------------------------------------------------------
# Raw SQL (streaming /rawSql endpoint)
# ---------------------------------------------------------------------------

class TestBuildRawSqlRequestBody:
    def test_builds_body(self):
        body = build_raw_sql_request_body("SELECT 1")
        assert body == {"query": "SELECT 1"}


class TestParseRawSqlNdjsonLines:
    def test_select_response(self):
        lines = [
            '{"meta":{"columns":["id","name","balance"]}}',
            '{"id":1,"name":"Alice","balance":99.5}',
            '{"id":2,"name":"Bob","balance":120.0}',
            '{"stats":{"row_count":2}}',
        ]
        result = parse_raw_sql_ndjson_lines(lines)
        assert result.columns == ["id", "name", "balance"]
        assert len(result.rows) == 2
        assert result.rows[0] == {"id": 1, "name": "Alice", "balance": 99.5}
        assert result.rows[1]["name"] == "Bob"
        assert result.row_count == 2
        assert result.affected_rows == 0

    def test_dml_response(self):
        lines = ['{"stats":{"affected_rows":42}}']
        result = parse_raw_sql_ndjson_lines(lines)
        assert result.columns == []
        assert result.rows == []
        assert result.affected_rows == 42
        assert result.row_count == 0

    def test_empty_select(self):
        lines = [
            '{"meta":{"columns":["id","name"]}}',
            '{"stats":{"row_count":0}}',
        ]
        result = parse_raw_sql_ndjson_lines(lines)
        assert result.columns == ["id", "name"]
        assert result.rows == []
        assert result.row_count == 0

    def test_empty_lines_skipped(self):
        lines = [
            '',
            '{"meta":{"columns":["x"]}}',
            '',
            '{"x":1}',
            '',
            '{"stats":{"row_count":1}}',
        ]
        result = parse_raw_sql_ndjson_lines(lines)
        assert len(result.rows) == 1

    def test_mid_stream_error_raises(self):
        lines = [
            '{"meta":{"columns":["id"]}}',
            '{"error":"query timeout exceeded"}',
        ]
        with pytest.raises(IyreeError, match="query timeout exceeded"):
            parse_raw_sql_ndjson_lines(lines)

    def test_to_dicts(self):
        lines = [
            '{"meta":{"columns":["a","b"]}}',
            '{"a":1,"b":"x"}',
            '{"stats":{"row_count":1}}',
        ]
        result = parse_raw_sql_ndjson_lines(lines)
        assert result.to_dicts() == [{"a": 1, "b": "x"}]
