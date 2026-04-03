"""Shared KV helpers — zero httpx imports.

Handles request body building and response parsing for both sync and async
KV clients.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from iyree._types import KvDocument


# ---------------------------------------------------------------------------
# Request builders
# ---------------------------------------------------------------------------

def build_put_body(
    data: Any,
    *,
    key: Optional[str] = None,
    indexes: Optional[Dict[str, Any]] = None,
    ttl: Optional[int] = None,
    upsert: bool = True,
) -> Dict[str, Any]:
    """Build the JSON body for ``POST /api/v1/store/{variable}/documents``."""
    body: Dict[str, Any] = {"data": data, "upsert": upsert}
    if key is not None:
        body["key"] = key
    if indexes is not None:
        body["indexes"] = indexes
    if ttl is not None:
        body["ttl"] = ttl
    return body


def build_patch_body(
    *,
    set_fields: Optional[Dict[str, Any]] = None,
    unset: Optional[List[str]] = None,
    inc: Optional[Dict[str, Any]] = None,
    indexes: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build the JSON body for ``PATCH /api/v1/store/{variable}/documents/{key}``."""
    body: Dict[str, Any] = {}
    if set_fields is not None:
        body["set"] = set_fields
    if unset is not None:
        body["unset"] = unset
    if inc is not None:
        body["inc"] = inc
    if indexes is not None:
        body["indexes"] = indexes
    return body


# ---------------------------------------------------------------------------
# Response parsers
# ---------------------------------------------------------------------------

def _parse_datetime(value: str) -> datetime:
    """Parse an ISO-8601 datetime string."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _unwrap(data: Dict[str, Any]) -> Dict[str, Any]:
    """Unwrap the ``{"data": {...}}`` envelope if present."""
    if "data" in data and isinstance(data["data"], dict) and "key" in data["data"]:
        return data["data"]
    return data


def parse_document_response(data: Dict[str, Any]) -> KvDocument:
    """Parse a JSON response into a :class:`KvDocument`."""
    doc = _unwrap(data)
    expires_at = None
    if doc.get("expires_at"):
        expires_at = _parse_datetime(doc["expires_at"])

    return KvDocument(
        key=doc["key"],
        data=doc["data"],
        created_at=_parse_datetime(doc["created_at"]),
        updated_at=_parse_datetime(doc["updated_at"]),
        expires_at=expires_at,
    )


def parse_put_response(data: Dict[str, Any]) -> str:
    """Extract the document key from a PUT response."""
    if "data" in data and isinstance(data["data"], dict):
        return data["data"].get("key", "")
    return data.get("key", "")
