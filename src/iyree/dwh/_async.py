"""Asynchronous DWH client."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from iyree._config import IyreeConfig
from iyree._http._async import AsyncHttpTransport
from iyree._types import DwhQueryResult, StreamLoadResult
from iyree.dwh._common import (
    build_sql_request_body,
    parse_ndjson_lines,
    parse_stream_load_response,
    prepare_insert_data,
    prepare_stream_load_headers,
    validate_stream_load_status,
)

logger = logging.getLogger("iyree")


class AsyncDwhClient:
    """Asynchronous client for the IYREE DWH (StarRocks) API.

    Args:
        http: Shared asynchronous HTTP transport.
        config: SDK configuration.
    """

    def __init__(self, http: AsyncHttpTransport, config: IyreeConfig) -> None:
        self._http = http
        self._config = config

    async def sql(
        self,
        query: str,
        *,
        session_variables: Optional[Dict[str, Any]] = None,
    ) -> DwhQueryResult:
        """Execute a SQL query against StarRocks.

        The response is streamed as NDJSON and parsed incrementally.

        Args:
            query: SQL query string.
            session_variables: Optional StarRocks session variables.

        Returns:
            Parsed query result with columns, rows, and statistics.

        Raises:
            IyreeError: On query failure or HTTP error.
        """
        body = build_sql_request_body(query, session_variables)
        url = f"{self._http._base_url}/api/v1/dwh/sql"
        headers = {**self._http._auth_headers, "Content-Type": "application/json"}

        async with self._http._client.stream(
            "POST", url, json=body, headers=headers,
        ) as response:
            if response.status_code >= 400:
                from iyree._http._common import map_status_to_exception, parse_response_body
                raw = await response.aread()
                raise map_status_to_exception(
                    response.status_code, parse_response_body(raw),
                )
            lines: List[str] = [
                line async for line in response.aiter_lines()
            ]

        return parse_ndjson_lines(lines)

    async def insert(
        self,
        table: str,
        data: Any,
        *,
        format: str = "csv",
        label: Optional[str] = None,
        columns: Optional[List[str]] = None,
        column_separator: str = ",",
        timeout: Optional[float] = None,
        **stream_load_headers: str,
    ) -> StreamLoadResult:
        """Insert data into a StarRocks table via Stream Load.

        Args:
            table: Target table name.
            data: Payload — ``str``, ``bytes``, ``pd.DataFrame``, or ``list[dict]``.
            format: Data format (``csv`` or ``json``).
            label: Idempotency label. When provided, the operation is safe to retry.
            columns: Column mapping for CSV data.
            column_separator: CSV column separator.
            timeout: Request timeout override (defaults to ``stream_load_timeout``).
            **stream_load_headers: Extra StarRocks Stream Load headers.

        Returns:
            :class:`StreamLoadResult` with load statistics.

        Raises:
            IyreeStreamLoadError: When Stream Load status is ``Fail``.
            IyreeDuplicateLabelError: When the label already exists.
        """
        payload, format, columns, stream_load_headers = prepare_insert_data(
            data, format, columns, column_separator, stream_load_headers,
        )
        headers = prepare_stream_load_headers(
            table,
            format=format,
            label=label,
            columns=columns,
            column_separator=column_separator,
            **stream_load_headers,
        )

        effective_timeout = timeout or self._config.stream_load_timeout

        response = await self._http.request(
            "PUT",
            f"/api/v1/dwh/{table}/_stream_load",
            content=payload,
            headers=headers,
            timeout=effective_timeout,
        )

        result = parse_stream_load_response(response.json())
        validate_stream_load_status(result)
        return result
