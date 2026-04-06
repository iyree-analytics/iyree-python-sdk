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


class TestAsyncKvList:
    @respx.mock
    async def test_list_basic(self, kv: AsyncKvClient, transport: AsyncHttpTransport, gateway_host: str):
        respx.post(f"{gateway_host}/api/v1/store/users/documents:list").mock(
            return_value=httpx.Response(200, json={
                "data": {
                    "items": [DOC_RESPONSE],
                    "cursor": None,
                    "has_more": False,
                }
            })
        )
        result = await kv.list("users")
        assert len(result.items) == 1
        assert result.items[0].key == "doc1"
        await transport.close()

    @respx.mock
    async def test_list_iter(self, kv: AsyncKvClient, transport: AsyncHttpTransport, gateway_host: str):
        call_count = 0
        def side_effect(request):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(200, json={
                    "data": {
                        "items": [DOC_RESPONSE],
                        "cursor": "next",
                        "has_more": True,
                    }
                })
            return httpx.Response(200, json={
                "data": {
                    "items": [{**DOC_RESPONSE, "key": "doc2"}],
                    "cursor": None,
                    "has_more": False,
                }
            })

        respx.post(f"{gateway_host}/api/v1/store/users/documents:list").mock(
            side_effect=side_effect
        )
        keys = [doc.key async for doc in kv.list_iter("users")]
        assert keys == ["doc1", "doc2"]
        await transport.close()


class TestAsyncKvBulkGet:
    @respx.mock
    async def test_bulk_get(self, kv: AsyncKvClient, transport: AsyncHttpTransport, gateway_host: str):
        respx.post(f"{gateway_host}/api/v1/store/users/documents:bulkGet").mock(
            return_value=httpx.Response(200, json={
                "data": {
                    "doc1": DOC_RESPONSE,
                    "doc2": {**DOC_RESPONSE, "key": "doc2"},
                }
            })
        )
        result = await kv.bulk_get("users", ["doc1", "doc2"])
        assert len(result) == 2
        assert "doc1" in result
        await transport.close()


class TestAsyncKvBulkPut:
    @respx.mock
    async def test_bulk_put(self, kv: AsyncKvClient, transport: AsyncHttpTransport, gateway_host: str):
        respx.post(f"{gateway_host}/api/v1/store/users/documents:bulkPut").mock(
            return_value=httpx.Response(201, json={
                "data": {"keys": ["k1", "k2"]}
            })
        )
        keys = await kv.bulk_put("users", [
            {"data": {"name": "Alice"}},
            {"data": {"name": "Bob"}},
        ])
        assert keys == ["k1", "k2"]
        await transport.close()
