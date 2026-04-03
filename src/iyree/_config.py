"""SDK configuration and environment variable resolution."""

from __future__ import annotations

import os
from dataclasses import dataclass

from iyree.exceptions import IyreeConfigError


@dataclass(frozen=True)
class IyreeConfig:
    """Immutable configuration for the IYREE SDK.

    Attributes:
        gateway_host: Base URL of the APISIX gateway (e.g. ``https://public-api.iyree.com``).
        api_key: API key sent in the ``x-api-key`` header.
        timeout: Default HTTP request timeout in seconds.
        stream_load_timeout: Timeout for DWH Stream Load operations.
        cube_continue_wait_timeout: Maximum time to poll Cube "Continue wait" responses.
        max_retries: Maximum retry attempts for retryable errors.
    """

    gateway_host: str
    api_key: str
    timeout: float = 30.0
    stream_load_timeout: float = 300.0
    cube_continue_wait_timeout: float = 120.0
    max_retries: int = 3


def build_config(
    api_key: str,
    gateway_host: str | None = None,
    timeout: float = 30.0,
    stream_load_timeout: float = 300.0,
    cube_continue_wait_timeout: float = 120.0,
    max_retries: int = 3,
) -> IyreeConfig:
    """Build and validate an :class:`IyreeConfig`.

    Args:
        api_key: API key for gateway authentication.
        gateway_host: Gateway base URL. Falls back to ``IYREE_GATEWAY_HOST`` env var.
        timeout: Default request timeout in seconds.
        stream_load_timeout: Timeout for Stream Load uploads.
        cube_continue_wait_timeout: Max Cube continue-wait polling duration.
        max_retries: Maximum retry attempts.

    Returns:
        Validated configuration instance.

    Raises:
        ValueError: If *api_key* is empty or ``None``.
        IyreeConfigError: If *gateway_host* cannot be determined.
    """
    if not api_key:
        raise ValueError("api_key is required and must not be empty")

    resolved_host = gateway_host or os.environ.get("IYREE_GATEWAY_HOST")
    if not resolved_host:
        raise IyreeConfigError(
            "gateway_host must be provided or set via the "
            "IYREE_GATEWAY_HOST environment variable"
        )

    resolved_host = resolved_host.rstrip("/")
    if not resolved_host.startswith(("http://", "https://")):
        resolved_host = f"https://{resolved_host}"

    return IyreeConfig(
        gateway_host=resolved_host,
        api_key=api_key,
        timeout=timeout,
        stream_load_timeout=stream_load_timeout,
        cube_continue_wait_timeout=cube_continue_wait_timeout,
        max_retries=max_retries,
    )
