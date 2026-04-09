"""Shared DWH helpers — zero httpx imports.

Handles request body building, NDJSON parsing, DataFrame serialisation, and
Stream Load header/response processing.
"""

from __future__ import annotations

import io
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from iyree._types import ColumnMeta, DwhQueryResult, DwhRawSqlResult, StreamLoadResult
from iyree.exceptions import IyreeDuplicateLabelError, IyreeError, IyreeStreamLoadError

logger = logging.getLogger("iyree")


# ---------------------------------------------------------------------------
# SQL query helpers
# ---------------------------------------------------------------------------

def build_sql_request_body(
    query: str,
    session_variables: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the JSON body for ``POST /api/v1/dwh/sql``."""
    body: Dict[str, Any] = {"query": query}
    if session_variables:
        body["sessionVariables"] = session_variables
    return body


def parse_ndjson_lines(lines: List[str]) -> DwhQueryResult:
    """Parse NDJSON lines into a :class:`DwhQueryResult`.

    Expected line types (in order):
    - ``{"connectionId": <int>}``
    - ``{"meta": [...]}``
    - zero or more ``{"data": [...]}``
    - ``{"statistics": {...}}``

    If the first line contains ``{"status": "FAILED", "msg": "..."}`` the query
    failed before producing data.

    Raises:
        IyreeError: If the query failed server-side.
    """
    connection_id = 0
    columns: List[ColumnMeta] = []
    rows: List[List[Any]] = []
    statistics: Dict[str, Any] = {}

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        obj = json.loads(line)

        if "status" in obj and obj["status"] == "FAILED":
            raise IyreeError(
                f"DWH query failed: {obj.get('msg', 'unknown error')}",
                status_code=None,
                response_body=obj,
            )

        if "connectionId" in obj:
            connection_id = obj["connectionId"]
        elif "meta" in obj:
            columns = [
                ColumnMeta(name=col["name"], type=col["type"])
                for col in obj["meta"]
            ]
        elif "data" in obj:
            rows.append(obj["data"])
        elif "statistics" in obj:
            statistics = obj["statistics"]

    return DwhQueryResult(
        columns=columns,
        rows=rows,
        statistics=statistics,
        connection_id=connection_id,
    )


# ---------------------------------------------------------------------------
# Raw SQL helpers (streaming /rawSql endpoint)
# ---------------------------------------------------------------------------

def build_raw_sql_request_body(query: str) -> Dict[str, Any]:
    """Build the JSON body for ``POST /api/v1/dwh/rawSql``."""
    return {"query": query}


def parse_raw_sql_ndjson_lines(lines: List[str]) -> DwhRawSqlResult:
    """Parse streaming NDJSON lines from the ``/rawSql`` endpoint.

    **SELECT / SHOW / DESCRIBE** response lines:
    - ``{"meta": {"columns": ["col1", "col2", ...]}}``
    - ``{"col1": value, "col2": value, ...}``  (one per row)
    - ``{"stats": {"row_count": N}}``

    **DML / DDL** response (single line):
    - ``{"stats": {"affected_rows": N}}``

    **Mid-stream error:**
    - ``{"error": "message"}``

    Raises:
        IyreeError: On a mid-stream error from StarRocks.
    """
    columns: List[str] = []
    rows: List[Dict[str, Any]] = []
    row_count = 0
    affected_rows = 0

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        obj = json.loads(line)

        if "error" in obj:
            raise IyreeError(
                f"DWH raw SQL error: {obj['error']}",
                status_code=None,
                response_body=obj,
            )

        if "meta" in obj:
            columns = obj["meta"].get("columns", [])
        elif "stats" in obj:
            stats = obj["stats"]
            row_count = stats.get("row_count", 0)
            affected_rows = stats.get("affected_rows", 0)
        else:
            rows.append(obj)

    _UINT64_MAX = 18446744073709551615
    if row_count == _UINT64_MAX:
        row_count = len(rows)

    return DwhRawSqlResult(
        columns=columns,
        rows=rows,
        row_count=row_count,
        affected_rows=affected_rows,
    )


# ---------------------------------------------------------------------------
# Stream Load helpers
# ---------------------------------------------------------------------------

def prepare_stream_load_headers(
    table: str,
    format: str = "csv",
    label: Optional[str] = None,
    columns: Optional[List[str]] = None,
    column_separator: str = ",",
    **extra_headers: str,
) -> Dict[str, str]:
    """Build the HTTP headers dict for a Stream Load request."""
    headers: Dict[str, str] = {
        "Expect": "100-continue",
        "format": format,
    }
    if column_separator != ",":
        headers["column_separator"] = column_separator

    if format == "csv":
        headers["Content-Type"] = "text/csv"
    else:
        headers["Content-Type"] = "application/json"

    if label is not None:
        headers["label"] = label
    if columns:
        headers["columns"] = ", ".join(columns)

    headers.update(extra_headers)
    return headers


def prepare_insert_data(
    data: Any,
    format: str,
    columns: Optional[List[str]],
    column_separator: str,
    extra_headers: Dict[str, str],
) -> Tuple[bytes, str, Optional[List[str]], Dict[str, str]]:
    """Normalise ``data`` into raw bytes and adjust format / columns / headers.

    Returns:
        ``(payload_bytes, format, columns, extra_headers)`` — possibly mutated
        copies of the input arguments.
    """
    if isinstance(data, bytes):
        return data, format, columns, extra_headers

    if isinstance(data, str):
        return data.encode("utf-8"), format, columns, extra_headers

    if isinstance(data, list):
        format = "json"
        extra_headers = {**extra_headers, "strip_outer_array": "true"}
        return json.dumps(data).encode("utf-8"), format, columns, extra_headers

    # Assume pandas DataFrame
    return _dataframe_to_payload(data, columns)


def _dataframe_to_payload(
    df: Any,
    columns: Optional[List[str]],
) -> Tuple[bytes, str, Optional[List[str]], Dict[str, str]]:
    """Convert a pandas DataFrame to CSV bytes for Stream Load."""
    try:
        import pandas as pd
    except ImportError:
        raise ImportError(
            "pandas is required to insert DataFrames. "
            "Install it with: pip install iyree[pandas]"
        ) from None

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Unsupported data type: {type(df).__name__}")

    csv_columns = columns if columns else list(df.columns)
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    return buf.getvalue().encode("utf-8"), "csv", csv_columns, {}


def parse_stream_load_response(data: Dict[str, Any]) -> StreamLoadResult:
    """Parse the JSON response from a Stream Load into :class:`StreamLoadResult`.

    Handles the ``{"data": {...}}`` envelope the gateway may wrap.
    """
    if "data" in data and isinstance(data["data"], dict) and "Status" in data["data"]:
        data = data["data"]
    return StreamLoadResult(
        txn_id=data.get("TxnId", 0),
        label=data.get("Label", ""),
        status=data.get("Status", ""),
        message=data.get("Message", ""),
        number_total_rows=data.get("NumberTotalRows", 0),
        number_loaded_rows=data.get("NumberLoadedRows", 0),
        number_filtered_rows=data.get("NumberFilteredRows", 0),
        number_unselected_rows=data.get("NumberUnselectedRows", 0),
        load_bytes=data.get("LoadBytes", 0),
        load_time_ms=data.get("LoadTimeMs", 0),
        error_url=data.get("ErrorURL"),
    )


def validate_stream_load_status(result: StreamLoadResult) -> None:
    """Raise if the Stream Load status indicates failure.

    Raises:
        IyreeDuplicateLabelError: When ``status`` is ``"Label Already Exists"``.
        IyreeStreamLoadError: When ``status`` is ``"Fail"``.
    """
    status_upper = result.status.upper() if result.status else ""
    if result.status == "Label Already Exists":
        raise IyreeDuplicateLabelError(
            f"Stream Load label already exists: {result.label}",
            status_code=200,
            response_body={"Label": result.label, "Status": result.status},
        )
    if status_upper in ("FAIL", "FAILED"):
        raise IyreeStreamLoadError(
            f"Stream Load failed: {result.message}",
            status_code=200,
            response_body={"Status": result.status, "Message": result.message},
        )
