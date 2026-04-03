"""Sync KV client integration tests with respx mocking."""

from __future__ import annotations

import httpx
import pytest
import respx

from iyree._config import IyreeConfig
from iyree._http._sync import HttpTransport
from iyree.exceptions import IyreeNotFoundError
from iyree.kv._sync import KvClient


@pytest.fixture
def transport(config: IyreeConfig) -> HttpTransport:
    t = HttpTransport(config)
    yield t
    t.close()


@pytest.fixture
def kv(transport: HttpTransport, config: IyreeConfig) -> KvClient:
    return KvClient(transport, config)


DOC_RESPONSE = {
    "key": "doc1",
    "data": {"name": "Alice"},
    "created_at": "2024-01-01T00:00:00Z",
    "updated_at": "2024-01-02T00:00:00Z",
    "expires_at": None,
}


class TestKvGetSync:
    @respx.mock
    def test_get_document(self, kv: KvClient, gateway_host: str):
        respx.get(f"{gateway_host}/api/v1/store/users/documents/doc1").mock(
            return_value=httpx.Response(200, json=DOC_RESPONSE)
        )
        doc = kv.get("users", "doc1")
        assert doc.key == "doc1"
        assert doc.data == {"name": "Alice"}

    @respx.mock
    def test_get_not_found(self, kv: KvClient, gateway_host: str):
        respx.get(f"{gateway_host}/api/v1/store/users/documents/nope").mock(
            return_value=httpx.Response(404, json={"detail": "not found"})
        )
        with pytest.raises(IyreeNotFoundError):
            kv.get("users", "nope")


class TestKvPutSync:
    @respx.mock
    def test_put_returns_key(self, kv: KvClient, gateway_host: str):
        respx.post(f"{gateway_host}/api/v1/store/users/documents").mock(
            return_value=httpx.Response(200, json={"key": "new-key"})
        )
        key = kv.put("users", {"name": "Bob"})
        assert key == "new-key"


class TestKvDeleteSync:
    @respx.mock
    def test_delete(self, kv: KvClient, gateway_host: str):
        respx.delete(f"{gateway_host}/api/v1/store/users/documents/doc1").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        assert kv.delete("users", "doc1") is True


class TestKvExistsSync:
    @respx.mock
    def test_exists_true(self, kv: KvClient, gateway_host: str):
        respx.head(f"{gateway_host}/api/v1/store/users/documents/doc1").mock(
            return_value=httpx.Response(200)
        )
        assert kv.exists("users", "doc1") is True

    @respx.mock
    def test_exists_false(self, kv: KvClient, gateway_host: str):
        respx.head(f"{gateway_host}/api/v1/store/users/documents/nope").mock(
            return_value=httpx.Response(404, json={"detail": "not found"})
        )
        assert kv.exists("users", "nope") is False


class TestKvPatchSync:
    @respx.mock
    def test_patch(self, kv: KvClient, gateway_host: str):
        respx.patch(f"{gateway_host}/api/v1/store/users/documents/doc1").mock(
            return_value=httpx.Response(200, json={
                **DOC_RESPONSE,
                "data": {"name": "Alice", "age": 30},
            })
        )
        doc = kv.patch("users", "doc1", set={"age": 30})
        assert doc.data["age"] == 30
