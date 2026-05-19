"""Synchronous Reports client."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional, Union

from iyree._config import IyreeConfig
from iyree._http._sync import HttpTransport
from iyree._types import Report, ReportPeriod
from iyree.reports._common import build_create_body, parse_report_response

logger = logging.getLogger("iyree")


class ReportsClient:
    """Synchronous client for the IYREE Reports API.

    Args:
        http: Shared synchronous HTTP transport.
        config: SDK configuration.
    """

    def __init__(self, http: HttpTransport, config: IyreeConfig) -> None:
        self._http = http
        self._config = config

    def create(
        self,
        *,
        main_period: Union[ReportPeriod, Dict[str, Any]],
        function_path: str,
        job_id: str,
        html_content: str,
        title: Optional[str] = None,
        description: Optional[str] = None,
        locations: Optional[List[int]] = None,
        compare_period: Optional[Union[ReportPeriod, Dict[str, Any]]] = None,
        summary: Optional[str] = None,
        name: Optional[str] = None,
        workspace_id: Optional[str] = None,
    ) -> Report:
        """Create a new report.

        ``datetime`` instances inside *main_period* / *compare_period* are
        serialized to ISO-8601 strings automatically.

        Args:
            main_period: Primary date range covered by the report.  Either a
                :class:`ReportPeriod` or a ``{"date_from": ..., "date_to": ...}``
                dict.
            function_path: Path identifying the source function.
            job_id: Identifier of the job that produced the report.
            html_content: Rendered HTML payload.
            title: Optional report title.
            description: Optional report description.
            locations: Optional location identifiers the report is scoped to.
            compare_period: Optional comparison date range (same shape as
                *main_period*).
            summary: Optional summary text.
            name: Optional report name.
            workspace_id: Optional workspace identifier.

        Returns:
            The created :class:`Report`.
        """
        body = build_create_body(
            main_period=main_period,
            function_path=function_path,
            job_id=job_id,
            html_content=html_content,
            title=title,
            description=description,
            locations=locations,
            compare_period=compare_period,
            summary=summary,
            name=name,
            workspace_id=workspace_id,
        )
        response = self._http.request("POST", "/api/v1/reports", json=body)
        return parse_report_response(response.json())
