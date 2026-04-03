"""Shared HTTP helpers — zero httpx imports.

Contains retry predicates, error-mapping logic, and header builders used by
both the sync and async transports.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Union

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

logger = logging.getLogger("iyree")

_RETRYABLE_STATUS_CODES = frozenset({429, 502, 503, 504})
_CLIENT_ERROR_CODES = frozenset({400, 401, 403, 404, 409, 422})

_STATUS_EXCEPTION_MAP: Dict[int, type] = {
    401: IyreeAuthError,
    403: IyreePermissionError,
    404: IyreeNotFoundError,
    422: IyreeValidationError,
    429: IyreeRateLimitError,
}


def should_retry(status_code: int) -> bool:
    """Return ``True`` if the HTTP status code is retryable."""
    return status_code in _RETRYABLE_STATUS_CODES


def is_client_error(status_code: int) -> bool:
    """Return ``True`` if the status code is a non-retryable client error."""
    return status_code in _CLIENT_ERROR_CODES


def build_auth_headers(api_key: str) -> Dict[str, str]:
    """Return the gateway authentication header dict."""
    return {"x-api-key": api_key}


def get_retry_after(headers: Dict[str, str]) -> Optional[float]:
    """Parse the ``Retry-After`` header value, if present.

    Returns:
        Seconds to wait, or ``None`` if the header is absent or unparseable.
    """
    value = headers.get("retry-after") or headers.get("Retry-After")
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_response_body(raw: Union[str, bytes]) -> Union[dict, str]:
    """Best-effort JSON parse of a response body."""
    import json

    text = raw if isinstance(raw, str) else raw.decode("utf-8", errors="replace")
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return text


def map_status_to_exception(
    status_code: int,
    response_body: Union[dict, str, None],
) -> IyreeError:
    """Map an HTTP status code to the appropriate SDK exception.

    Args:
        status_code: HTTP response status code.
        response_body: Parsed or raw response body.

    Returns:
        An :class:`IyreeError` subclass instance.
    """
    if isinstance(response_body, dict):
        detail = response_body.get("detail") or response_body.get("message") or ""
    else:
        detail = response_body or ""

    message = f"HTTP {status_code}: {detail}" if detail else f"HTTP {status_code}"

    exc_cls = _STATUS_EXCEPTION_MAP.get(status_code)
    if exc_cls is not None:
        return exc_cls(message, status_code=status_code, response_body=response_body)

    if 500 <= status_code < 600:
        return IyreeServerError(
            message, status_code=status_code, response_body=response_body
        )

    return IyreeError(message, status_code=status_code, response_body=response_body)
