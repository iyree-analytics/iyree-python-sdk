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


class TestKvListSync:
    @respx.mock
    def test_list_basic(self, kv: KvClient, gateway_host: str):
        respx.post(f"{gateway_host}/api/v1/store/users/documents:list").mock(
            return_value=httpx.Response(200, json={
                "data": {
                    "items": [DOC_RESPONSE],
                    "cursor": None,
                    "has_more": False,
                }
            })
        )
        result = kv.list("users")
        assert len(result.items) == 1
        assert result.items[0].key == "doc1"
        assert result.has_more is False

    @respx.mock
    def test_list_with_where(self, kv: KvClient, gateway_host: str):
        route = respx.post(f"{gateway_host}/api/v1/store/users/documents:list").mock(
            return_value=httpx.Response(200, json={
                "data": {"items": [], "cursor": None, "has_more": False}
            })
        )
        kv.list("users", where=[{"index_name": "status", "op": "eq", "value": "active"}])
        body = route.calls[0].request.content
        import json
        sent = json.loads(body)
        assert sent["where"][0]["index_name"] == "status"

    @respx.mock
    def test_list_datetime_serialized(self, kv: KvClient, gateway_host: str):
        from datetime import datetime, timezone
        route = respx.post(f"{gateway_host}/api/v1/store/users/documents:list").mock(
            return_value=httpx.Response(200, json={
                "data": {"items": [], "cursor": None, "has_more": False}
            })
        )
        dt = datetime(2025, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        kv.list("users", where=[{"index_name": "created", "op": "gte", "value": dt}])
        import json
        sent = json.loads(route.calls[0].request.content)
        assert sent["where"][0]["value"] == "2025-06-01T12:00:00+00:00"

    @respx.mock
    def test_list_iter_pagination(self, kv: KvClient, gateway_host: str):
        call_count = 0
        def side_effect(request):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(200, json={
                    "data": {
                        "items": [DOC_RESPONSE],
                        "cursor": "page2",
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
        keys = [doc.key for doc in kv.list_iter("users")]
        assert keys == ["doc1", "doc2"]


class TestKvBulkGetSync:
    @respx.mock
    def test_bulk_get(self, kv: KvClient, gateway_host: str):
        respx.post(f"{gateway_host}/api/v1/store/users/documents:bulkGet").mock(
            return_value=httpx.Response(200, json={
                "data": {
                    "doc1": DOC_RESPONSE,
                    "doc2": {**DOC_RESPONSE, "key": "doc2", "data": {"name": "Bob"}},
                }
            })
        )
        result = kv.bulk_get("users", ["doc1", "doc2"])
        assert len(result) == 2
        assert result["doc1"].data["name"] == "Alice"
        assert result["doc2"].data["name"] == "Bob"


class TestKvBulkPutSync:
    @respx.mock
    def test_bulk_put(self, kv: KvClient, gateway_host: str):
        respx.post(f"{gateway_host}/api/v1/store/users/documents:bulkPut").mock(
            return_value=httpx.Response(201, json={
                "data": {"keys": ["k1", "k2"]}
            })
        )
        keys = kv.bulk_put("users", [
            {"data": {"name": "Alice"}},
            {"data": {"name": "Bob"}, "key": "k2"},
        ])
        assert keys == ["k1", "k2"]

    @respx.mock
    def test_bulk_put_datetime_indexes(self, kv: KvClient, gateway_host: str):
        from datetime import datetime, timezone
        route = respx.post(f"{gateway_host}/api/v1/store/users/documents:bulkPut").mock(
            return_value=httpx.Response(201, json={
                "data": {"keys": ["k1"]}
            })
        )
        dt = datetime(2025, 1, 1, tzinfo=timezone.utc)
        kv.bulk_put("users", [
            {"data": {"x": 1}, "indexes": {"created": dt}},
        ])
        import json
        sent = json.loads(route.calls[0].request.content)
        assert sent["items"][0]["indexes"]["created"] == "2025-01-01T00:00:00+00:00"
