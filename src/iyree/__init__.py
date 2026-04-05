"""IYREE Python SDK — convenient interface to the IYREE BI analytics platform.

Quick start (module-level)::

    import iyree

    iyree.init(api_key="my-key", gateway_host="https://api.iyree.io")
    result = iyree.dwh.sql("SELECT 1")

Explicit client::

    from iyree import IyreeClient

    with IyreeClient(api_key="my-key") as client:
        result = client.dwh.sql("SELECT 1")

Async::

    from iyree import AsyncIyreeClient

    async with AsyncIyreeClient(api_key="my-key") as client:
        result = await client.dwh.sql("SELECT 1")
"""

from __future__ import annotations

from typing import Optional

# Top-level clients
from iyree._async_client import AsyncIyreeClient
from iyree._client import IyreeClient

# Exceptions
from iyree.exceptions import (
    IyreeAuthError,
    IyreeConfigError,
    IyreeCubeTimeoutError,
    IyreeDuplicateLabelError,
    IyreeError,
    IyreeNotFoundError,
    IyreePermissionError,
    IyreeRateLimitError,
    IyreeS3Error,
    IyreeServerError,
    IyreeStreamLoadError,
    IyreeTimeoutError,
    IyreeValidationError,
)

# Result types
from iyree._types import (
    ColumnMeta,
    CubeQueryResult,
    DwhQueryResult,
    KvDocument,
    S3CopyResult,
    S3DeleteError,
    S3DeleteResult,
    S3ListResult,
    S3Object,
    StreamLoadResult,
)

# Cube query builder (convenience re-export)
from iyree.cube._querybuilder import (
    And,
    Cube,
    DateRange,
    Dimension,
    Filter,
    FilterOperator,
    Measure,
    Or,
    Order,
    Query,
    Segment,
    TimeGranularity,
    TimeDimension,
)

_default_client: Optional[IyreeClient] = None

_SUB_CLIENTS = frozenset({"dwh", "cube", "s3", "kv"})


def init(
    api_key: str,
    gateway_host: Optional[str] = None,
    timeout: float = 30.0,
    stream_load_timeout: float = 300.0,
    cube_continue_wait_timeout: float = 120.0,
    max_retries: int = 3,
) -> None:
    """Initialize the module-level default client.

    After calling this, sub-clients are accessible directly on the module::

        import iyree
        iyree.init(api_key="my-key")
        result = iyree.dwh.sql("SELECT 1")

    Calling ``init`` again replaces (and closes) the previous default client.

    Args:
        api_key: API key for gateway authentication.
        gateway_host: Gateway base URL.  Falls back to ``IYREE_GATEWAY_HOST``.
        timeout: Default request timeout in seconds.
        stream_load_timeout: Timeout for DWH Stream Load uploads.
        cube_continue_wait_timeout: Max Cube continue-wait polling duration.
        max_retries: Maximum retry attempts for retryable errors.
    """
    global _default_client
    if _default_client is not None:
        _default_client.close()
    _default_client = IyreeClient(
        api_key=api_key,
        gateway_host=gateway_host,
        timeout=timeout,
        stream_load_timeout=stream_load_timeout,
        cube_continue_wait_timeout=cube_continue_wait_timeout,
        max_retries=max_retries,
    )


def close() -> None:
    """Close the module-level default client, releasing resources."""
    global _default_client
    if _default_client is not None:
        _default_client.close()
        _default_client = None


def __getattr__(name: str):
    if name in _SUB_CLIENTS:
        if _default_client is not None:
            return getattr(_default_client, name)
        raise IyreeConfigError(
            "SDK not initialized. Call iyree.init(api_key=...) first."
        )
    raise AttributeError(f"module 'iyree' has no attribute {name!r}")


__all__ = [
    # Module-level API
    "init",
    "close",
    # Clients
    "IyreeClient",
    "AsyncIyreeClient",
    # Exceptions
    "IyreeError",
    "IyreeConfigError",
    "IyreeAuthError",
    "IyreePermissionError",
    "IyreeNotFoundError",
    "IyreeValidationError",
    "IyreeRateLimitError",
    "IyreeServerError",
    "IyreeTimeoutError",
    "IyreeStreamLoadError",
    "IyreeDuplicateLabelError",
    "IyreeCubeTimeoutError",
    "IyreeS3Error",
    # Result types
    "DwhQueryResult",
    "ColumnMeta",
    "StreamLoadResult",
    "CubeQueryResult",
    "S3Object",
    "S3ListResult",
    "S3CopyResult",
    "S3DeleteResult",
    "S3DeleteError",
    "KvDocument",
    # Query builder
    "Cube",
    "Measure",
    "Dimension",
    "Segment",
    "Query",
    "DateRange",
    "TimeDimension",
    "Filter",
    "Or",
    "And",
    "TimeGranularity",
    "Order",
    "FilterOperator",
]

# Remove subpackage references that Python auto-placed in this module's
# __dict__ during internal imports.  This allows __getattr__ to intercept
# iyree.dwh / iyree.cube / iyree.s3 / iyree.kv and delegate to the
# default client.  The subpackages remain in sys.modules, so explicit
# imports like ``from iyree.dwh import DwhClient`` still resolve correctly.
for _name in ("dwh", "cube", "s3", "kv"):
    globals().pop(_name, None)
del _name
