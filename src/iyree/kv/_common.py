"""Shared KV helpers — zero httpx imports.

Handles request body building and response parsing for both sync and async
KV clients.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from iyree._types import KvDocument, KvListResult


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_datetime(value: str) -> datetime:
    """Parse an ISO-8601 datetime string."""
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _serialize_value(value: Any) -> Any:
    """Convert ``datetime`` instances to ISO-8601 strings, pass others through."""
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _serialize_indexes(indexes: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Serialize index values, converting datetimes to ISO-8601 strings."""
    if indexes is None:
        return None
    return {k: _serialize_value(v) for k, v in indexes.items()}


def _unwrap(data: Dict[str, Any]) -> Dict[str, Any]:
    """Unwrap the ``{"data": {...}}`` envelope if present."""
    if "data" in data and isinstance(data["data"], dict) and "key" in data["data"]:
        return data["data"]
    return data


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
        body["indexes"] = _serialize_indexes(indexes)
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
        body["indexes"] = _serialize_indexes(indexes)
    return body


def build_list_body(
    *,
    where: Optional[List[Dict[str, Any]]] = None,
    order_by: Optional[Dict[str, str]] = None,
    limit: int = 100,
    cursor: Optional[str] = None,
    select: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Build the JSON body for ``POST /api/v1/store/{variable}/documents:list``."""
    body: Dict[str, Any] = {"limit": limit}
    if where is not None:
        serialized = []
        for clause in where:
            c = dict(clause)
            if "value" in c:
                c["value"] = _serialize_value(c["value"])
            if "value_to" in c and c["value_to"] is not None:
                c["value_to"] = _serialize_value(c["value_to"])
            serialized.append(c)
        body["where"] = serialized
    if order_by is not None:
        body["order_by"] = order_by
    if cursor is not None:
        body["cursor"] = cursor
    if select is not None:
        body["select"] = select
    return body


def build_bulk_get_body(keys: List[str]) -> Dict[str, Any]:
    """Build the JSON body for ``POST /api/v1/store/{variable}/documents:bulkGet``."""
    return {"keys": keys}


def build_bulk_put_body(
    items: List[Dict[str, Any]],
    *,
    upsert: bool = True,
) -> Dict[str, Any]:
    """Build the JSON body for ``POST /api/v1/store/{variable}/documents:bulkPut``.

    Each item in *items* should have at minimum a ``data`` key. Optional keys
    are ``key``, ``indexes``, and ``ttl``.
    """
    serialized_items = []
    for item in items:
        out: Dict[str, Any] = {"data": item["data"]}
        if "key" in item and item["key"] is not None:
            out["key"] = item["key"]
        if "indexes" in item and item["indexes"] is not None:
            out["indexes"] = _serialize_indexes(item["indexes"])
        if "ttl" in item and item["ttl"] is not None:
            out["ttl"] = item["ttl"]
        serialized_items.append(out)
    return {"items": serialized_items, "upsert": upsert}


# ---------------------------------------------------------------------------
# Response parsers
# ---------------------------------------------------------------------------

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


def parse_list_response(data: Dict[str, Any]) -> KvListResult:
    """Parse a list-documents response into a :class:`KvListResult`."""
    inner = data.get("data", data) if "data" in data and isinstance(data.get("data"), dict) else data
    items = [_parse_doc(doc) for doc in inner.get("items", [])]
    return KvListResult(
        items=items,
        cursor=inner.get("cursor"),
        has_more=inner.get("has_more", False),
    )


def parse_bulk_get_response(data: Dict[str, Any]) -> Dict[str, KvDocument]:
    """Parse a bulk-get response into a ``{key: KvDocument}`` dict."""
    inner = data.get("data", data)
    if not isinstance(inner, dict):
        return {}
    return {k: _parse_doc(v) for k, v in inner.items()}


def parse_bulk_put_response(data: Dict[str, Any]) -> List[str]:
    """Extract the list of keys from a bulk-put response."""
    inner = data.get("data", data) if "data" in data and isinstance(data.get("data"), dict) else data
    return inner.get("keys", [])


def _parse_doc(doc: Dict[str, Any]) -> KvDocument:
    """Parse a single KVDoc dict into a :class:`KvDocument`."""
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
