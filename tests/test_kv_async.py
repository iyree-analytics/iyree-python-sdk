"""Async KV client integration tests with respx mocking."""

from __future__ import annotations

import httpx
import pytest
import respx

from iyree._config import IyreeConfig
from iyree._http._async import AsyncHttpTransport
from iyree.exceptions import IyreeNotFoundError
from iyree.kv._async import AsyncKvClient


@pytest.fixture
def transport(config: IyreeConfig) -> AsyncHttpTransport:
    return AsyncHttpTransport(config)


@pytest.fixture
def kv(transport: AsyncHttpTransport, config: IyreeConfig) -> AsyncKvClient:
    return AsyncKvClient(transport, config)


DOC_RESPONSE = {
    "key": "doc1",
    "data": {"name": "Alice"},
    "created_at": "2024-01-01T00:00:00Z",
    "updated_at": "2024-01-02T00:00:00Z",
    "expires_at": None,
}


class TestAsyncKvGet:
    @respx.mock
    async def test_get_document(self, kv: AsyncKvClient, transport: AsyncHttpTransport, gateway_host: str):
        respx.get(f"{gateway_host}/api/v1/store/users/documents/doc1").mock(
            return_value=httpx.Response(200, json=DOC_RESPONSE)
        )
        doc = await kv.get("users", "doc1")
        assert doc.key == "doc1"
        await transport.close()

    @respx.mock
    async def test_get_not_found(self, kv: AsyncKvClient, transport: AsyncHttpTransport, gateway_host: str):
        respx.get(f"{gateway_host}/api/v1/store/users/documents/nope").mock(
            return_value=httpx.Response(404, json={"detail": "not found"})
        )
        with pytest.raises(IyreeNotFoundError):
            await kv.get("users", "nope")
        await transport.close()


class TestAsyncKvExists:
    @respx.mock
    async def test_exists_true(self, kv: AsyncKvClient, transport: AsyncHttpTransport, gateway_host: str):
        respx.head(f"{gateway_host}/api/v1/store/users/documents/doc1").mock(
            return_value=httpx.Response(200)
        )
        assert await kv.exists("users", "doc1") is True
        await transport.close()

    @respx.mock
    async def test_exists_false(self, kv: AsyncKvClient, transport: AsyncHttpTransport, gateway_host: str):
        respx.head(f"{gateway_host}/api/v1/store/users/documents/nope").mock(
            return_value=httpx.Response(404, json={"detail": "not found"})
        )
        assert await kv.exists("users", "nope") is False
        await transport.close()
