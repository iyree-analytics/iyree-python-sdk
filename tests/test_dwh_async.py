"""Async DWH client integration tests with respx mocking."""

from __future__ import annotations

import httpx
import pytest
import respx

from iyree._config import IyreeConfig
from iyree._http._async import AsyncHttpTransport
from iyree.dwh._async import AsyncDwhClient
from iyree.exceptions import IyreeStreamLoadError


@pytest.fixture
def transport(config: IyreeConfig) -> AsyncHttpTransport:
    return AsyncHttpTransport(config)


@pytest.fixture
def dwh(transport: AsyncHttpTransport, config: IyreeConfig) -> AsyncDwhClient:
    return AsyncDwhClient(transport, config)


class TestAsyncDwhSql:
    @respx.mock
    async def test_basic_query(self, dwh: AsyncDwhClient, transport: AsyncHttpTransport, gateway_host: str):
        ndjson = (
            '{"connectionId": 1}\n'
            '{"meta": [{"name": "n", "type": "INT"}]}\n'
            '{"data": [42]}\n'
            '{"statistics": {"scanRows": 1}}\n'
        )
        respx.post(f"{gateway_host}/api/v1/dwh/sql").mock(
            return_value=httpx.Response(200, content=ndjson.encode())
        )
        result = await dwh.sql("SELECT 42 AS n")
        assert result.rows == [[42]]
        await transport.close()


class TestAsyncDwhInsert:
    @respx.mock
    async def test_csv_insert(self, dwh: AsyncDwhClient, transport: AsyncHttpTransport, gateway_host: str):
        respx.put(f"{gateway_host}/api/v1/dwh/t/_stream_load").mock(
            return_value=httpx.Response(200, json={
                "TxnId": 1, "Label": "l", "Status": "Success", "Message": "OK",
                "NumberTotalRows": 2, "NumberLoadedRows": 2,
                "NumberFilteredRows": 0, "NumberUnselectedRows": 0,
                "LoadBytes": 20, "LoadTimeMs": 10,
            })
        )
        result = await dwh.insert("t", "1,2\n3,4")
        assert result.status == "Success"
        await transport.close()

    @respx.mock
    async def test_fail_status_raises(self, dwh: AsyncDwhClient, transport: AsyncHttpTransport, gateway_host: str):
        respx.put(f"{gateway_host}/api/v1/dwh/t/_stream_load").mock(
            return_value=httpx.Response(200, json={
                "TxnId": 1, "Label": "l", "Status": "Fail", "Message": "err",
                "NumberTotalRows": 1, "NumberLoadedRows": 0,
                "NumberFilteredRows": 0, "NumberUnselectedRows": 0,
                "LoadBytes": 10, "LoadTimeMs": 5,
            })
        )
        with pytest.raises(IyreeStreamLoadError):
            await dwh.insert("t", b"bad")
        await transport.close()
