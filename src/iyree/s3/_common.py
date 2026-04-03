"""Shared S3 helpers — zero httpx imports.

Handles request body building, response parsing, and URL construction for
both sync and async S3 clients.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from iyree._types import (
    PresignedUrlInfo,
    S3CopyResult,
    S3DeleteError,
    S3DeleteResult,
    S3ListResult,
    S3Object,
)


# ---------------------------------------------------------------------------
# Request builders
# ---------------------------------------------------------------------------

def build_presigned_url_body(
    key: str,
    method: str,
    content_type: Optional[str] = None,
) -> Dict[str, Any]:
    """Build the JSON body for generating a presigned URL."""
    body: Dict[str, Any] = {"key": key, "method": method}
    if content_type:
        body["contentType"] = content_type
    return body


def build_list_params(
    prefix: str = "",
    max_keys: int = 1000,
    continuation_token: Optional[str] = None,
) -> Dict[str, Any]:
    """Build query parameters for the list-objects endpoint."""
    params: Dict[str, Any] = {"prefix": prefix, "max_keys": str(max_keys)}
    if continuation_token:
        params["continuation_token"] = continuation_token
    return params


def build_copy_body(source_key: str, destination_key: str) -> Dict[str, str]:
    """Build the JSON body for a copy-object request."""
    return {"source_key": source_key, "destination_key": destination_key}


def build_delete_body(keys: List[str]) -> Dict[str, List[str]]:
    """Build the JSON body for a delete-objects request."""
    return {"keys": keys}


# ---------------------------------------------------------------------------
# Response parsers
# ---------------------------------------------------------------------------

def _parse_datetime(value: str) -> datetime:
    """Parse an ISO-8601 datetime string."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _unwrap(data: Dict[str, Any]) -> Dict[str, Any]:
    """Unwrap the ``{"data": {...}}`` envelope if present."""
    if "data" in data and isinstance(data["data"], (dict, list)):
        inner = data["data"]
        if isinstance(inner, dict):
            return inner
    return data


def parse_list_response(data: Dict[str, Any]) -> S3ListResult:
    """Parse a list-objects response."""
    data = _unwrap(data)
    objects = [
        S3Object(
            key=obj["key"],
            size=obj["size"],
            last_modified=_parse_datetime(obj["last_modified"]),
            etag=obj["etag"],
        )
        for obj in data.get("objects", [])
    ]
    return S3ListResult(
        objects=objects,
        next_continuation_token=data.get("next_continuation_token"),
        is_truncated=data.get("is_truncated", False),
        key_count=data.get("key_count", len(objects)),
    )


def parse_copy_response(
    data: Dict[str, Any],
    source_key: str,
    destination_key: str,
) -> S3CopyResult:
    """Parse a copy-object response."""
    data = _unwrap(data)
    return S3CopyResult(
        source_key=data.get("source_key", source_key),
        destination_key=data.get("destination_key", destination_key),
    )


def parse_delete_response(data: Dict[str, Any]) -> S3DeleteResult:
    """Parse a delete-objects response."""
    data = _unwrap(data)
    errors = [
        S3DeleteError(
            key=err["key"],
            code=err.get("code", ""),
            message=err.get("message", ""),
        )
        for err in data.get("errors", [])
    ]
    return S3DeleteResult(
        deleted=data.get("deleted", []),
        errors=errors,
    )


def parse_presigned_url_response(data: Dict[str, Any]) -> PresignedUrlInfo:
    """Parse a presigned-URL generation response.

    Handles the ``{"data": {...}}`` envelope the backend may return.
    """
    inner = data.get("data", data) if "data" in data and isinstance(data.get("data"), dict) else data
    return PresignedUrlInfo(
        url=inner["url"],
        expires_in=inner["expires_in"],
        method=inner["method"],
        required_headers=inner.get("required_headers", {}),
    )


def normalise_upload_data(data: Any) -> bytes:
    """Convert upload data to bytes.

    Accepts ``bytes``, ``str`` (encoded as UTF-8), or a file-like object
    (read to bytes).
    """
    if isinstance(data, bytes):
        return data
    if isinstance(data, str):
        return data.encode("utf-8")
    return data.read()
