"""Async Reports client integration tests with respx mocking."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import httpx
import pytest
import respx

from iyree._config import IyreeConfig
from iyree._http._async import AsyncHttpTransport
from iyree._types import ReportPeriod
from iyree.reports._async import AsyncReportsClient


@pytest.fixture
def transport(config: IyreeConfig) -> AsyncHttpTransport:
    return AsyncHttpTransport(config)


@pytest.fixture
def reports(transport: AsyncHttpTransport, config: IyreeConfig) -> AsyncReportsClient:
    return AsyncReportsClient(transport, config)


REPORT_RESPONSE = {
    "id": "rep_123",
    "name": "Weekly summary",
    "title": "Weekly summary",
    "description": "Lorem ipsum",
    "creation_type": "PREDEFINED",
    "source_function": {
        "id": "fn_1",
        "name": "weekly",
        "title": "Weekly",
        "description": "Weekly function",
        "creation_type": "PREDEFINED",
        "function_type": "basic",
        "status": "active",
    },
    "locations": [1, 2, 3],
    "main_period": {
        "date_from": "2026-05-12T00:00:00Z",
        "date_to": "2026-05-19T00:00:00Z",
    },
    "compare_period": {
        "date_from": "2026-05-05T00:00:00Z",
        "date_to": "2026-05-12T00:00:00Z",
    },
    "summary": "All good",
    "html_content": "<html></html>",
    "source_job": {
        "id": "job_1",
        "script_path": "/scripts/run.py",
        "job_kind": "unknown",
        "trigger_detail": {"type": "webapp"},
        "status": "running",
        "success": True,
        "created_at": "2026-05-19T17:19:31Z",
        "started_at": "2026-05-19T17:19:32Z",
        "duration_ms": 1234,
        "mem_peak_kb": 5678,
        "is_flow_step": False,
        "runnable_type": "function",
        "runnable_id": "fn_1",
    },
}


class TestAsyncReportsCreate:
    @respx.mock
    async def test_create_happy_path(
        self,
        reports: AsyncReportsClient,
        transport: AsyncHttpTransport,
        gateway_host: str,
    ):
        respx.post(f"{gateway_host}/api/v1/reports").mock(
            return_value=httpx.Response(201, json=REPORT_RESPONSE)
        )
        period = ReportPeriod(
            date_from=datetime(2026, 5, 12, tzinfo=timezone.utc),
            date_to=datetime(2026, 5, 19, tzinfo=timezone.utc),
        )
        report = await reports.create(
            main_period=period,
            function_path="/scripts/run.py",
            job_id="job_1",
            html_content="<html></html>",
        )
        assert report.id == "rep_123"
        assert report.creation_type == "PREDEFINED"
        assert report.locations == [1, 2, 3]
        assert report.source_function is not None
        assert report.source_function.id == "fn_1"
        assert report.main_period is not None
        assert report.main_period.date_from == datetime(2026, 5, 12, tzinfo=timezone.utc)
        assert report.source_job is not None
        assert report.source_job.success is True
        assert report.source_job.created_at == datetime(2026, 5, 19, 17, 19, 31, tzinfo=timezone.utc)
        await transport.close()

    @respx.mock
    async def test_create_serializes_datetimes(
        self,
        reports: AsyncReportsClient,
        transport: AsyncHttpTransport,
        gateway_host: str,
    ):
        route = respx.post(f"{gateway_host}/api/v1/reports").mock(
            return_value=httpx.Response(201, json=REPORT_RESPONSE)
        )
        period = ReportPeriod(
            date_from=datetime(2026, 5, 12, tzinfo=timezone.utc),
            date_to=datetime(2026, 5, 19, tzinfo=timezone.utc),
        )
        await reports.create(
            main_period=period,
            function_path="/scripts/run.py",
            job_id="job_1",
            html_content="<html></html>",
        )
        sent = json.loads(route.calls[0].request.content)
        assert sent["main_period"]["date_from"] == "2026-05-12T00:00:00+00:00"
        assert sent["main_period"]["date_to"] == "2026-05-19T00:00:00+00:00"
        await transport.close()

    @respx.mock
    async def test_create_omits_optional_fields(
        self,
        reports: AsyncReportsClient,
        transport: AsyncHttpTransport,
        gateway_host: str,
    ):
        route = respx.post(f"{gateway_host}/api/v1/reports").mock(
            return_value=httpx.Response(201, json=REPORT_RESPONSE)
        )
        await reports.create(
            main_period=ReportPeriod(
                date_from=datetime(2026, 5, 12, tzinfo=timezone.utc),
                date_to=datetime(2026, 5, 19, tzinfo=timezone.utc),
            ),
            function_path="/scripts/run.py",
            job_id="job_1",
            html_content="<html></html>",
        )
        sent = json.loads(route.calls[0].request.content)
        for missing in ("title", "description", "locations", "compare_period", "summary", "name", "workspace_id"):
            assert missing not in sent
        await transport.close()

    @respx.mock
    async def test_create_accepts_dict_period(
        self,
        reports: AsyncReportsClient,
        transport: AsyncHttpTransport,
        gateway_host: str,
    ):
        route = respx.post(f"{gateway_host}/api/v1/reports").mock(
            return_value=httpx.Response(201, json=REPORT_RESPONSE)
        )
        await reports.create(
            main_period={
                "date_from": datetime(2026, 5, 12, tzinfo=timezone.utc),
                "date_to": datetime(2026, 5, 19, tzinfo=timezone.utc),
            },
            function_path="/scripts/run.py",
            job_id="job_1",
            html_content="<html></html>",
        )
        sent = json.loads(route.calls[0].request.content)
        assert sent["main_period"]["date_from"] == "2026-05-12T00:00:00+00:00"
        assert sent["main_period"]["date_to"] == "2026-05-19T00:00:00+00:00"
        await transport.close()

    @respx.mock
    async def test_create_parses_envelope_response(
        self,
        reports: AsyncReportsClient,
        transport: AsyncHttpTransport,
        gateway_host: str,
    ):
        respx.post(f"{gateway_host}/api/v1/reports").mock(
            return_value=httpx.Response(201, json={"data": REPORT_RESPONSE})
        )
        report = await reports.create(
            main_period=ReportPeriod(
                date_from=datetime(2026, 5, 12, tzinfo=timezone.utc),
                date_to=datetime(2026, 5, 19, tzinfo=timezone.utc),
            ),
            function_path="/scripts/run.py",
            job_id="job_1",
            html_content="<html></html>",
        )
        assert report.id == "rep_123"
        assert report.source_function is not None
        assert report.source_function.id == "fn_1"
        await transport.close()

    @respx.mock
    async def test_create_handles_null_nested_fields(
        self,
        reports: AsyncReportsClient,
        transport: AsyncHttpTransport,
        gateway_host: str,
    ):
        minimal_response = {
            "id": "rep_minimal",
            "creation_type": "PREDEFINED",
            "main_period": {
                "date_from": "2026-05-12T00:00:00Z",
                "date_to": "2026-05-19T00:00:00Z",
            },
            "compare_period": None,
            "source_function": None,
            "source_job": None,
            "html_content": "<html></html>",
        }
        respx.post(f"{gateway_host}/api/v1/reports").mock(
            return_value=httpx.Response(201, json=minimal_response)
        )
        report = await reports.create(
            main_period=ReportPeriod(
                date_from=datetime(2026, 5, 12, tzinfo=timezone.utc),
                date_to=datetime(2026, 5, 19, tzinfo=timezone.utc),
            ),
            function_path="/scripts/run.py",
            job_id="job_1",
            html_content="<html></html>",
        )
        assert report.id == "rep_minimal"
        assert report.compare_period is None
        assert report.source_function is None
        assert report.source_job is None
        assert report.locations == []
        await transport.close()
