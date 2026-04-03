"""Shared result dataclasses used across all sub-clients."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# DWH
# ---------------------------------------------------------------------------

@dataclass
class ColumnMeta:
    """Column metadata returned by the DWH SQL API."""

    name: str
    type: str


@dataclass
class DwhQueryResult:
    """Result of a DWH SQL query.

    Attributes:
        columns: Column metadata from the ``meta`` NDJSON line.
        rows: Data rows, each a list of values in column order.
        statistics: Query execution statistics from StarRocks.
        connection_id: Connection identifier from StarRocks.
    """

    columns: List[ColumnMeta]
    rows: List[List[Any]]
    statistics: Dict[str, Any]
    connection_id: int

    def to_dicts(self) -> List[Dict[str, Any]]:
        """Return rows as a list of ``{column_name: value}`` dicts."""
        col_names = [c.name for c in self.columns]
        return [dict(zip(col_names, row)) for row in self.rows]

    def to_dataframe(self) -> "pd.DataFrame":
        """Convert to a pandas ``DataFrame``.

        Raises:
            ImportError: If pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for to_dataframe(). "
                "Install it with: pip install iyree[pandas]"
            ) from None
        col_names = [c.name for c in self.columns]
        return pd.DataFrame(self.rows, columns=col_names)


@dataclass
class StreamLoadResult:
    """Result of a DWH Stream Load (insert) operation.

    Attributes:
        txn_id: StarRocks transaction ID.
        label: Label used for idempotency.
        status: Operation status (``Success``, ``Fail``, ``Publish Timeout``, etc.).
        message: Human-readable status message.
        number_total_rows: Total rows processed.
        number_loaded_rows: Rows successfully loaded.
        number_filtered_rows: Rows filtered out (e.g. by ``where`` clause).
        number_unselected_rows: Rows not selected.
        load_bytes: Bytes loaded.
        load_time_ms: Load duration in milliseconds.
        error_url: URL to fetch detailed error info when rows are filtered.
    """

    txn_id: int
    label: str
    status: str
    message: str
    number_total_rows: int
    number_loaded_rows: int
    number_filtered_rows: int
    number_unselected_rows: int
    load_bytes: int
    load_time_ms: int
    error_url: Optional[str] = None


# ---------------------------------------------------------------------------
# Cube
# ---------------------------------------------------------------------------

@dataclass
class CubeQueryResult:
    """Result of a Cube ``/load`` query.

    Attributes:
        data: Result rows as a list of dicts.
        annotation: Column annotation metadata from Cube.
        query: The original query echoed back by Cube.
        raw_response: The full JSON response for advanced use.
    """

    data: List[Dict[str, Any]]
    annotation: Dict[str, Any]
    query: Dict[str, Any]
    raw_response: Dict[str, Any]

    def to_dataframe(self) -> "pd.DataFrame":
        """Convert to a pandas ``DataFrame``.

        Raises:
            ImportError: If pandas is not installed.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for to_dataframe(). "
                "Install it with: pip install iyree[pandas]"
            ) from None
        return pd.DataFrame(self.data)


# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

@dataclass
class S3Object:
    """Metadata for a single object in S3."""

    key: str
    size: int
    last_modified: datetime
    etag: str


@dataclass
class S3ListResult:
    """Paginated result from listing S3 objects.

    Attributes:
        objects: Objects in this page.
        next_continuation_token: Token for the next page, or ``None`` if this is the last.
        is_truncated: Whether more pages are available.
        key_count: Number of keys returned in this page.
    """

    objects: List[S3Object]
    next_continuation_token: Optional[str]
    is_truncated: bool
    key_count: int


@dataclass
class S3CopyResult:
    """Result of an S3 copy operation."""

    source_key: str
    destination_key: str


@dataclass
class S3DeleteError:
    """Error detail for a single object deletion failure."""

    key: str
    code: str
    message: str


@dataclass
class S3DeleteResult:
    """Result of a batch S3 delete operation.

    If ``errors`` is non-empty the caller should inspect individual failures.
    """

    deleted: List[str]
    errors: List[S3DeleteError]


@dataclass
class PresignedUrlInfo:
    """Presigned URL details returned by the backend."""

    url: str
    expires_in: int
    method: str
    required_headers: Dict[str, str]


# ---------------------------------------------------------------------------
# KV
# ---------------------------------------------------------------------------

@dataclass
class KvDocument:
    """A document stored in the KV store.

    Attributes:
        key: Document key.
        data: Arbitrary document payload.
        created_at: Creation timestamp.
        updated_at: Last modification timestamp.
        expires_at: Expiry timestamp, or ``None`` if the document does not expire.
    """

    key: str
    data: Any
    created_at: datetime
    updated_at: datetime
    expires_at: Optional[datetime] = None
