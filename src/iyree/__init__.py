"""IYREE Python SDK — convenient interface to the IYREE BI analytics platform.

Usage::

    from iyree import IyreeClient

    with IyreeClient(api_key="my-key") as client:
        result = client.dwh.sql("SELECT 1")

For async usage::

    from iyree import AsyncIyreeClient

    async with AsyncIyreeClient(api_key="my-key") as client:
        result = await client.dwh.sql("SELECT 1")
"""

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

__all__ = [
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
