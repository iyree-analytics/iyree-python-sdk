"""Shared Reports helpers — zero httpx imports.

Handles request body building and response parsing for both sync and async
Reports clients.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from iyree._types import (
    Report,
    ReportPeriod,
    ReportSourceFunction,
    ReportSourceJob,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_datetime(value: str) -> datetime:
    """Parse an ISO-8601 datetime string."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _maybe_parse_datetime(value: Any) -> Optional[datetime]:
    """Parse an optional ISO-8601 datetime string, returning ``None`` if missing."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    return _parse_datetime(value)


def _serialize_datetime(value: Any) -> Any:
    """Convert ``datetime`` instances to ISO-8601 strings, pass others through."""
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _serialize_period(
    period: Union[ReportPeriod, Dict[str, Any]],
) -> Dict[str, Any]:
    """Serialize a :class:`ReportPeriod` or plain dict to wire format."""
    if isinstance(period, ReportPeriod):
        return {
            "date_from": _serialize_datetime(period.date_from),
            "date_to": _serialize_datetime(period.date_to),
        }
    return {k: _serialize_datetime(v) for k, v in period.items()}


def _unwrap(data: Dict[str, Any]) -> Dict[str, Any]:
    """Unwrap the ``{"data": {...}}`` envelope if present."""
    if (
        "data" in data
        and isinstance(data["data"], dict)
        and "id" in data["data"]
    ):
        return data["data"]
    return data


# ---------------------------------------------------------------------------
# Request builders
# ---------------------------------------------------------------------------

def build_create_body(
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
) -> Dict[str, Any]:
    """Build the JSON body for ``POST /api/v1/reports/``.

    Required fields (``main_period``, ``function_path``, ``job_id``,
    ``html_content``) are always included.  Optional fields are dropped from
    the payload when ``None``.
    """
    body: Dict[str, Any] = {
        "main_period": _serialize_period(main_period),
        "function_path": function_path,
        "job_id": job_id,
        "html_content": html_content,
    }
    if title is not None:
        body["title"] = title
    if description is not None:
        body["description"] = description
    if locations is not None:
        body["locations"] = list(locations)
    if compare_period is not None:
        body["compare_period"] = _serialize_period(compare_period)
    if summary is not None:
        body["summary"] = summary
    if name is not None:
        body["name"] = name
    if workspace_id is not None:
        body["workspace_id"] = workspace_id
    return body


# ---------------------------------------------------------------------------
# Response parsers
# ---------------------------------------------------------------------------

def _parse_period(data: Optional[Dict[str, Any]]) -> Optional[ReportPeriod]:
    """Parse a ``{date_from, date_to}`` dict into :class:`ReportPeriod`."""
    if not data:
        return None
    date_from = _maybe_parse_datetime(data.get("date_from"))
    date_to = _maybe_parse_datetime(data.get("date_to"))
    if date_from is None or date_to is None:
        return None
    return ReportPeriod(date_from=date_from, date_to=date_to)


def _parse_source_function(
    data: Optional[Dict[str, Any]],
) -> Optional[ReportSourceFunction]:
    """Parse the ``source_function`` block of a report response."""
    if not data:
        return None
    return ReportSourceFunction(
        id=data["id"],
        name=data.get("name"),
        title=data.get("title"),
        description=data.get("description"),
        creation_type=data.get("creation_type"),
        function_type=data.get("function_type"),
        status=data.get("status"),
    )


def _parse_source_job(
    data: Optional[Dict[str, Any]],
) -> Optional[ReportSourceJob]:
    """Parse the ``source_job`` block of a report response."""
    if not data:
        return None
    return ReportSourceJob(
        id=data["id"],
        script_path=data.get("script_path"),
        job_kind=data.get("job_kind"),
        trigger_detail=data.get("trigger_detail"),
        status=data.get("status"),
        success=data.get("success"),
        created_at=_maybe_parse_datetime(data.get("created_at")),
        started_at=_maybe_parse_datetime(data.get("started_at")),
        duration_ms=data.get("duration_ms"),
        mem_peak_kb=data.get("mem_peak_kb"),
        is_flow_step=data.get("is_flow_step"),
        runnable_type=data.get("runnable_type"),
        runnable_id=data.get("runnable_id"),
    )


def parse_report_response(data: Dict[str, Any]) -> Report:
    """Parse a JSON response into a :class:`Report`."""
    payload = _unwrap(data)
    return Report(
        id=payload["id"],
        name=payload.get("name"),
        title=payload.get("title"),
        description=payload.get("description"),
        creation_type=payload.get("creation_type"),
        source_function=_parse_source_function(payload.get("source_function")),
        locations=list(payload.get("locations") or []),
        main_period=_parse_period(payload.get("main_period")),
        compare_period=_parse_period(payload.get("compare_period")),
        summary=payload.get("summary"),
        html_content=payload.get("html_content"),
        source_job=_parse_source_job(payload.get("source_job")),
    )
