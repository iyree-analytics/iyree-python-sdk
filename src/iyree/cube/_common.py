"""Shared Cube helpers — zero httpx imports.

Handles JWT parsing, token-expiry checks, continue-wait logic, and query
serialisation for both sync and async Cube clients.
"""

from __future__ import annotations

import base64
import json
import logging
import urllib.parse
from typing import Any, Dict, Optional, Union

from iyree._types import CubeQueryResult
from iyree.cube._querybuilder.query import Query

logger = logging.getLogger("iyree")


# ---------------------------------------------------------------------------
# JWT helpers (no PyJWT dependency)
# ---------------------------------------------------------------------------

def parse_jwt_expiry(token: str) -> float:
    """Extract the ``exp`` claim from a JWT without verification.

    The token is split on ``"."``, the middle (payload) segment is
    base64url-decoded and JSON-parsed.  Only the ``exp`` field is read.

    Returns:
        The expiry timestamp as a UNIX epoch float.
    """
    parts = token.split(".")
    if len(parts) != 3:
        raise ValueError("Invalid JWT: expected 3 dot-separated segments")

    payload_b64 = parts[1]
    # Pad to a multiple of 4 for base64 decoding
    padded = payload_b64 + "=" * (-len(payload_b64) % 4)
    payload_bytes = base64.urlsafe_b64decode(padded)
    payload = json.loads(payload_bytes)
    return float(payload["exp"])


def is_token_expired(token_expiry: float, buffer_seconds: int = 60) -> bool:
    """Return ``True`` if the token will expire within *buffer_seconds*."""
    import time

    return time.time() >= (token_expiry - buffer_seconds)


# ---------------------------------------------------------------------------
# Continue-wait helpers
# ---------------------------------------------------------------------------

def is_continue_wait_response(data: Dict[str, Any]) -> bool:
    """Check whether a Cube.js response is a "Continue wait" polling signal."""
    return data.get("error") == "Continue wait"


def compute_continue_wait_delay(attempt: int) -> float:
    """Return the polling delay for the given zero-based attempt index.

    Sequence: 0.5 → 1.0 → 2.0 → 2.0 → ...
    """
    if attempt <= 0:
        return 0.5
    if attempt == 1:
        return 1.0
    return 2.0


# ---------------------------------------------------------------------------
# Query serialisation
# ---------------------------------------------------------------------------

def serialize_query(query: Union[Dict[str, Any], Query]) -> Dict[str, Any]:
    """Normalize a query to a plain dict."""
    if isinstance(query, Query):
        return query.serialize()
    return query


def build_load_params(query: Union[Dict[str, Any], Query]) -> str:
    """Serialize a query to a JSON string for use as a query parameter.

    The caller (httpx) handles URL-encoding when placed in ``params``.
    """
    query_dict = serialize_query(query)
    return json.dumps(query_dict, separators=(",", ":"))


def needs_multi_query(query: Union[Dict[str, Any], Query]) -> bool:
    """Return ``True`` if the query should use ``queryType: "multi"``.

    This is the case when any time dimension has a ``compare_date_range``.
    """
    if not isinstance(query, Query):
        return False
    if not query.time_dimensions:
        return False
    return any(td.compare_date_range for td in query.time_dimensions)


def build_load_query_body(query: Union[Dict[str, Any], Query]) -> Dict[str, Any]:
    """Build the full query body including ``queryType`` if needed."""
    query_dict = serialize_query(query)
    body: Dict[str, Any] = {"query": json.dumps(query_dict, separators=(",", ":"))}
    if needs_multi_query(query):
        body["queryType"] = "multi"
    return body


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def parse_token_response(data: Dict[str, Any]) -> str:
    """Extract the JWT string from a token endpoint response."""
    return data["data"]["token"]


def parse_load_response(data: Dict[str, Any]) -> CubeQueryResult:
    """Parse a successful Cube ``/load`` response into :class:`CubeQueryResult`."""
    results = data.get("results", [{}])
    first = results[0] if results else {}

    return CubeQueryResult(
        data=first.get("data", data.get("data", [])),
        annotation=first.get("annotation", data.get("annotation", {})),
        query=first.get("query", data.get("query", {})),
        raw_response=data,
    )
