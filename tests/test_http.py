"""Tests for the shared HTTP layer."""

from __future__ import annotations

import httpx
import pytest
import respx

from iyree._config import IyreeConfig
from iyree._http._async import AsyncHttpTransport
from iyree._http._common import (
    build_auth_headers,
    get_retry_after,
    is_client_error,
    map_status_to_exception,
    should_retry,
)
from iyree._http._sync import HttpTransport
from iyree.exceptions import (
    IyreeAuthError,
    IyreeError,
    IyreeNotFoundError,
    IyreePermissionError,
    IyreeRateLimitError,
    IyreeServerError,
    IyreeTimeoutError,
    IyreeValidationError,
)


# ---------------------------------------------------------------------------
# _common.py unit tests
# ---------------------------------------------------------------------------


class TestShouldRetry:
    def test_retryable_codes(self):
        for code in (429, 502, 503, 504):
            assert should_retry(code) is True

    def test_non_retryable_codes(self):
        for code in (200, 201, 400, 401, 403, 404, 422, 500):
            assert should_retry(code) is False


class TestIsClientError:
    def test_client_error_codes(self):
        for code in (400, 401, 403, 404, 409, 422):
            assert is_client_error(code) is True

    def test_non_client_error_codes(self):
        for code in (200, 429, 500, 502):
            assert is_client_error(code) is False


class TestBuildAuthHeaders:
    def test_returns_api_key_header(self):
        result = build_auth_headers("my-key")
        assert result == {"x-api-key": "my-key"}


class TestGetRetryAfter:
    def test_present(self):
        assert get_retry_after({"Retry-After": "5"}) == 5.0

    def test_lowercase(self):
        assert get_retry_after({"retry-after": "2.5"}) == 2.5

    def test_absent(self):
        assert get_retry_after({}) is None

    def test_invalid(self):
        assert get_retry_after({"Retry-After": "not-a-number"}) is None


class TestMapStatusToException:
    def test_401(self):
        exc = map_status_to_exception(401, {"detail": "Unauthorized"})
        assert isinstance(exc, IyreeAuthError)
        assert exc.status_code == 401

    def test_403(self):
        exc = map_status_to_exception(403, None)
        assert isinstance(exc, IyreePermissionError)

    def test_404(self):
        exc = map_status_to_exception(404, "Not found")
        assert isinstance(exc, IyreeNotFoundError)

    def test_422(self):
        exc = map_status_to_exception(422, {"detail": "bad field"})
        assert isinstance(exc, IyreeValidationError)

    def test_429(self):
        exc = map_status_to_exception(429, None)
        assert isinstance(exc, IyreeRateLimitError)

    def test_500(self):
        exc = map_status_to_exception(500, None)
        assert isinstance(exc, IyreeServerError)

    def test_unknown_code(self):
        exc = map_status_to_exception(418, "I'm a teapot")
        assert isinstance(exc, IyreeError)
        assert exc.status_code == 418


# ---------------------------------------------------------------------------
# HttpTransport sync tests (with respx)
# ---------------------------------------------------------------------------


class TestHttpTransportSync:
    @pytest.fixture
    def transport(self, config: IyreeConfig) -> HttpTransport:
        t = HttpTransport(config)
        yield t
        t.close()

    @respx.mock
    def test_successful_request(self, transport: HttpTransport, gateway_host: str):
        respx.get(f"{gateway_host}/api/v1/test").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        resp = transport.request("GET", "/api/v1/test")
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}

    @respx.mock
    def test_auth_header_injected(self, transport: HttpTransport, gateway_host: str):
        route = respx.get(f"{gateway_host}/api/v1/test").mock(
            return_value=httpx.Response(200, json={})
        )
        transport.request("GET", "/api/v1/test")
        assert route.calls[0].request.headers["x-api-key"] == "test-api-key-12345"

    @respx.mock
    def test_client_error_raises(self, transport: HttpTransport, gateway_host: str):
        respx.get(f"{gateway_host}/api/v1/test").mock(
            return_value=httpx.Response(401, json={"detail": "bad key"})
        )
        with pytest.raises(IyreeAuthError):
            transport.request("GET", "/api/v1/test")

    @respx.mock
    def test_presigned_no_auth_header(self, transport: HttpTransport):
        url = "https://s3.example.com/presigned?sig=abc"
        respx.get(url).mock(return_value=httpx.Response(200, content=b"data"))
        resp = transport.request_presigned("GET", url)
        assert "x-api-key" not in respx.calls[0].request.headers
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# AsyncHttpTransport tests (with respx)
# ---------------------------------------------------------------------------


class TestAsyncHttpTransport:
    @pytest.fixture
    def transport(self, config: IyreeConfig) -> AsyncHttpTransport:
        t = AsyncHttpTransport(config)
        yield t

    @respx.mock
    async def test_successful_request(self, transport: AsyncHttpTransport, gateway_host: str):
        respx.get(f"{gateway_host}/api/v1/test").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        resp = await transport.request("GET", "/api/v1/test")
        assert resp.json() == {"ok": True}
        await transport.close()

    @respx.mock
    async def test_client_error_raises(self, transport: AsyncHttpTransport, gateway_host: str):
        respx.get(f"{gateway_host}/api/v1/test").mock(
            return_value=httpx.Response(404, json={"detail": "nope"})
        )
        with pytest.raises(IyreeNotFoundError):
            await transport.request("GET", "/api/v1/test")
        await transport.close()

    @respx.mock
    async def test_presigned_no_auth_header(self, transport: AsyncHttpTransport):
        url = "https://s3.example.com/presigned?sig=abc"
        respx.get(url).mock(return_value=httpx.Response(200, content=b"data"))
        await transport.request_presigned("GET", url)
        assert "x-api-key" not in respx.calls[0].request.headers
        await transport.close()
