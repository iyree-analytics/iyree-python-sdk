"""Synchronous HTTP transport wrapping ``httpx.Client``."""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import httpx
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential_jitter,
)

from iyree._config import IyreeConfig
from iyree._http._common import (
    build_auth_headers,
    get_retry_after,
    map_status_to_exception,
    parse_response_body,
    should_retry,
)
from iyree.exceptions import IyreeError, IyreeTimeoutError

logger = logging.getLogger("iyree")


class _RetryableResponse(Exception):
    """Thin wrapper so tenacity can intercept retryable HTTP status codes."""

    def __init__(self, response: httpx.Response) -> None:
        self.response = response


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, httpx.TimeoutException):
        return True
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, _RetryableResponse):
        return True
    return False


class HttpTransport:
    """Auth-injected, retry-aware synchronous HTTP transport.

    Args:
        config: SDK configuration.
    """

    def __init__(self, config: IyreeConfig) -> None:
        self._config = config
        self._base_url = config.gateway_host
        self._auth_headers = build_auth_headers(config.api_key)
        self._client = httpx.Client(timeout=config.timeout)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        content: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        stream: bool = False,
    ) -> httpx.Response:
        """Send a request to the gateway with auth headers and retries.

        Args:
            method: HTTP method.
            path: URL path appended to the gateway host.
            json: JSON-serialisable body.
            content: Raw bytes body.
            headers: Extra headers merged with auth headers.
            params: Query parameters.
            timeout: Per-request timeout override.
            stream: If ``True``, return a streaming response (caller must close).

        Returns:
            The :class:`httpx.Response`.

        Raises:
            IyreeError: On non-retryable HTTP errors or retries exhausted.
        """
        url = f"{self._base_url}{path}"
        merged = {**self._auth_headers, **(headers or {})}
        effective_timeout = timeout if timeout is not None else self._config.timeout

        return self._request_with_retry(
            method, url, json=json, content=content, headers=merged,
            params=params, timeout=effective_timeout, stream=stream,
        )

    def request_presigned(
        self,
        method: str,
        url: str,
        *,
        content: Optional[bytes] = None,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
        stream: bool = False,
    ) -> httpx.Response:
        """Send a request to a presigned S3 URL — no ``x-api-key`` injection.

        Args:
            method: HTTP method (``GET`` or ``PUT``).
            url: Full presigned URL.
            content: Raw bytes body (for ``PUT``).
            headers: Required headers from the presigned-URL response.
            timeout: Per-request timeout override.
            stream: If ``True``, return a streaming response.

        Returns:
            The :class:`httpx.Response`.
        """
        effective_timeout = timeout if timeout is not None else self._config.timeout
        return self._request_with_retry(
            method, url, content=content, headers=headers or {},
            timeout=effective_timeout, stream=stream,
        )

    def close(self) -> None:
        """Close the underlying ``httpx.Client``."""
        self._client.close()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        json: Optional[Dict[str, Any]] = None,
        content: Optional[bytes] = None,
        headers: Dict[str, str],
        params: Optional[Dict[str, str]] = None,
        timeout: float,
        stream: bool,
    ) -> httpx.Response:
        max_attempts = self._config.max_retries + 1

        @retry(
            retry=retry_if_exception(_is_retryable),
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential_jitter(initial=0.5, max=10, exp_base=2),
            reraise=True,
        )
        def _do() -> httpx.Response:
            try:
                if stream:
                    req = self._client.build_request(
                        method, url, json=json, content=content,
                        headers=headers, params=params,
                        timeout=timeout,
                    )
                    response = self._client.send(req, stream=True)
                else:
                    response = self._client.request(
                        method, url, json=json, content=content,
                        headers=headers, params=params, timeout=timeout,
                    )
            except httpx.TimeoutException as exc:
                logger.warning("Request timeout: %s %s", method, url)
                raise

            if should_retry(response.status_code):
                retry_after = get_retry_after(dict(response.headers))
                logger.warning(
                    "Retryable status %d for %s %s (Retry-After: %s)",
                    response.status_code, method, url, retry_after,
                )
                raise _RetryableResponse(response)

            if response.status_code >= 400:
                body = parse_response_body(response.text if not stream else response.read())
                raise map_status_to_exception(response.status_code, body)

            return response

        try:
            return _do()
        except _RetryableResponse as exc:
            body = parse_response_body(exc.response.text)
            raise map_status_to_exception(exc.response.status_code, body) from None
        except httpx.TimeoutException as exc:
            raise IyreeTimeoutError(
                f"Request timed out: {method} {url}",
                status_code=None,
                response_body=None,
            ) from exc
        except httpx.TransportError as exc:
            raise IyreeError(
                f"Transport error: {exc}",
                status_code=None,
                response_body=None,
            ) from exc
