"""Sync DWH client integration tests with respx mocking."""

from __future__ import annotations

import json

import httpx
import pytest
import respx

from iyree._config import IyreeConfig
from iyree._http._sync import HttpTransport
from iyree.dwh._sync import DwhClient
from iyree.exceptions import IyreeStreamLoadError


@pytest.fixture
def transport(config: IyreeConfig) -> HttpTransport:
    t = HttpTransport(config)
    yield t
    t.close()


@pytest.fixture
def dwh(transport: HttpTransport, config: IyreeConfig) -> DwhClient:
    return DwhClient(transport, config)


class TestDwhSqlSync:
    @respx.mock
    def test_basic_query(self, dwh: DwhClient, gateway_host: str):
        ndjson = (
            '{"connectionId": 1}\n'
            '{"meta": [{"name": "n", "type": "INT"}]}\n'
            '{"data": [42]}\n'
            '{"statistics": {"scanRows": 1}}\n'
        )
        respx.post(f"{gateway_host}/api/v1/dwh/sql").mock(
            return_value=httpx.Response(
                200,
                content=ndjson.encode(),
                headers={"content-type": "application/x-ndjson"},
            )
        )
        result = dwh.sql("SELECT 42 AS n")
        assert len(result.rows) == 1
        assert result.rows[0] == [42]
        assert result.columns[0].name == "n"

    @respx.mock
    def test_empty_result(self, dwh: DwhClient, gateway_host: str):
        ndjson = (
            '{"connectionId": 1}\n'
            '{"meta": [{"name": "id", "type": "INT"}]}\n'
            '{"statistics": {"returnRows": 0}}\n'
        )
        respx.post(f"{gateway_host}/api/v1/dwh/sql").mock(
            return_value=httpx.Response(200, content=ndjson.encode())
        )
        result = dwh.sql("SELECT id FROM t WHERE 1=0")
        assert result.rows == []


class TestDwhInsertSync:
    @respx.mock
    def test_csv_insert(self, dwh: DwhClient, gateway_host: str):
        respx.put(f"{gateway_host}/api/v1/dwh/my_table/_stream_load").mock(
            return_value=httpx.Response(200, json={
                "TxnId": 1, "Label": "l", "Status": "Success", "Message": "OK",
                "NumberTotalRows": 2, "NumberLoadedRows": 2,
                "NumberFilteredRows": 0, "NumberUnselectedRows": 0,
                "LoadBytes": 20, "LoadTimeMs": 10,
            })
        )
        result = dwh.insert("my_table", "a,b\n1,2\n3,4")
        assert result.status == "Success"
        assert result.number_loaded_rows == 2

    @respx.mock
    def test_list_dict_insert(self, dwh: DwhClient, gateway_host: str):
        route = respx.put(f"{gateway_host}/api/v1/dwh/t/_stream_load").mock(
            return_value=httpx.Response(200, json={
                "TxnId": 1, "Label": "l", "Status": "Success", "Message": "OK",
                "NumberTotalRows": 1, "NumberLoadedRows": 1,
                "NumberFilteredRows": 0, "NumberUnselectedRows": 0,
                "LoadBytes": 10, "LoadTimeMs": 5,
            })
        )
        dwh.insert("t", [{"a": 1}])
        sent_headers = route.calls[0].request.headers
        assert sent_headers.get("format") == "json"

    @respx.mock
    def test_fail_status_raises(self, dwh: DwhClient, gateway_host: str):
        respx.put(f"{gateway_host}/api/v1/dwh/t/_stream_load").mock(
            return_value=httpx.Response(200, json={
                "TxnId": 1, "Label": "l", "Status": "Fail", "Message": "parse error",
                "NumberTotalRows": 1, "NumberLoadedRows": 0,
                "NumberFilteredRows": 0, "NumberUnselectedRows": 0,
                "LoadBytes": 10, "LoadTimeMs": 5,
            })
        )
        with pytest.raises(IyreeStreamLoadError, match="parse error"):
            dwh.insert("t", b"bad data")
