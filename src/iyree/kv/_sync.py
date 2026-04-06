"""Synchronous KV client."""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterator, List, Optional

from iyree._config import IyreeConfig
from iyree._http._sync import HttpTransport
from iyree._types import KvDocument, KvListResult
from iyree.exceptions import IyreeNotFoundError
from iyree.kv._common import (
    build_bulk_get_body,
    build_bulk_put_body,
    build_list_body,
    build_patch_body,
    build_put_body,
    parse_bulk_get_response,
    parse_bulk_put_response,
    parse_document_response,
    parse_list_response,
    parse_put_response,
)

logger = logging.getLogger("iyree")


class KvClient:
    """Synchronous client for the IYREE Key-Value store.

    Args:
        http: Shared synchronous HTTP transport.
        config: SDK configuration.
    """

    def __init__(self, http: HttpTransport, config: IyreeConfig) -> None:
        self._http = http
        self._config = config

    def get(self, variable: str, key: str) -> KvDocument:
        """Retrieve a document by key.

        Args:
            variable: KV store variable (namespace).
            key: Document key.

        Returns:
            The stored document.

        Raises:
            IyreeNotFoundError: If the document does not exist.
        """
        response = self._http.request(
            "GET", f"/api/v1/store/{variable}/documents/{key}",
        )
        return parse_document_response(response.json())

    def put(
        self,
        variable: str,
        data: Any,
        *,
        key: Optional[str] = None,
        indexes: Optional[Dict[str, Any]] = None,
        ttl: Optional[int] = None,
        upsert: bool = True,
    ) -> str:
        """Create or update a document.

        ``datetime`` objects in *indexes* are automatically converted to
        ISO-8601 strings.

        Args:
            variable: KV store variable (namespace).
            data: Document payload.
            key: Optional document key. Auto-generated if ``None``.
            indexes: Optional index fields.
            ttl: Time-to-live in seconds.
            upsert: If ``True`` (default), overwrite an existing document.

        Returns:
            The document key.
        """
        body = build_put_body(data, key=key, indexes=indexes, ttl=ttl, upsert=upsert)
        response = self._http.request(
            "POST", f"/api/v1/store/{variable}/documents", json=body,
        )
        return parse_put_response(response.json())

    def delete(self, variable: str, key: str) -> bool:
        """Delete a document.

        Args:
            variable: KV store variable (namespace).
            key: Document key.

        Returns:
            ``True`` if the document was deleted.
        """
        self._http.request(
            "DELETE", f"/api/v1/store/{variable}/documents/{key}",
        )
        return True

    def exists(self, variable: str, key: str) -> bool:
        """Check whether a document exists.

        Does **not** raise on 404 — returns ``False`` instead.

        Args:
            variable: KV store variable (namespace).
            key: Document key.

        Returns:
            ``True`` if the document exists, ``False`` otherwise.
        """
        try:
            self._http.request(
                "HEAD", f"/api/v1/store/{variable}/documents/{key}",
            )
            return True
        except IyreeNotFoundError:
            return False

    def patch(
        self,
        variable: str,
        key: str,
        *,
        set: Optional[Dict[str, Any]] = None,
        unset: Optional[List[str]] = None,
        inc: Optional[Dict[str, Any]] = None,
        indexes: Optional[Dict[str, Any]] = None,
    ) -> KvDocument:
        """Partially update a document.

        Args:
            variable: KV store variable (namespace).
            key: Document key.
            set: Fields to set or overwrite.
            unset: Field names to remove.
            inc: Fields to increment.
            indexes: Index fields to update.

        Returns:
            The updated document.
        """
        body = build_patch_body(
            set_fields=set, unset=unset, inc=inc, indexes=indexes,
        )
        response = self._http.request(
            "PATCH",
            f"/api/v1/store/{variable}/documents/{key}",
            json=body,
        )
        return parse_document_response(response.json())

    # ------------------------------------------------------------------
    # List
    # ------------------------------------------------------------------

    def list(
        self,
        variable: str,
        *,
        where: Optional[List[Dict[str, Any]]] = None,
        order_by: Optional[Dict[str, str]] = None,
        limit: int = 100,
        cursor: Optional[str] = None,
        select: Optional[List[str]] = None,
    ) -> KvListResult:
        """List documents with optional filtering, ordering, and cursor pagination.

        ``datetime`` objects in *where* clause ``value`` / ``value_to`` fields
        are automatically converted to ISO-8601 strings.

        Args:
            variable: KV store variable (namespace).
            where: Filter conditions on secondary indexes. Each dict should
                contain ``index_name``, ``value``, and optionally ``op``
                (``eq``, ``gt``, ``gte``, ``lt``, ``lte``, ``between``)
                and ``value_to`` (for ``between``).
            order_by: Sort directive with ``field`` (``updated_at`` or
                ``created_at``) and ``direction`` (``asc`` or ``desc``).
            limit: Maximum documents per page (1–1000, default 100).
            cursor: Opaque cursor from a previous page for pagination.
            select: Subset of top-level data keys to return.

        Returns:
            A page of documents with a cursor for the next page.
        """
        body = build_list_body(
            where=where, order_by=order_by, limit=limit,
            cursor=cursor, select=select,
        )
        response = self._http.request(
            "POST", f"/api/v1/store/{variable}/documents:list", json=body,
        )
        return parse_list_response(response.json())

    def list_iter(
        self,
        variable: str,
        *,
        where: Optional[List[Dict[str, Any]]] = None,
        order_by: Optional[Dict[str, str]] = None,
        limit: int = 100,
        select: Optional[List[str]] = None,
    ) -> Iterator[KvDocument]:
        """Auto-paginating iterator over documents matching the query.

        Yields:
            :class:`KvDocument` instances across all pages.
        """
        cursor: Optional[str] = None
        while True:
            page = self.list(
                variable, where=where, order_by=order_by,
                limit=limit, cursor=cursor, select=select,
            )
            yield from page.items
            if not page.has_more or page.cursor is None:
                break
            cursor = page.cursor

    # ------------------------------------------------------------------
    # Bulk operations
    # ------------------------------------------------------------------

    def bulk_get(
        self,
        variable: str,
        keys: List[str],
    ) -> Dict[str, KvDocument]:
        """Fetch multiple documents by keys in a single request.

        Args:
            variable: KV store variable (namespace).
            keys: Document keys to fetch.

        Returns:
            A dict mapping each found key to its :class:`KvDocument`.
            Keys that do not exist are omitted from the result.
        """
        body = build_bulk_get_body(keys)
        response = self._http.request(
            "POST", f"/api/v1/store/{variable}/documents:bulkGet", json=body,
        )
        return parse_bulk_get_response(response.json())

    def bulk_put(
        self,
        variable: str,
        items: List[Dict[str, Any]],
        *,
        upsert: bool = True,
    ) -> List[str]:
        """Create or upsert multiple documents in a single request.

        ``datetime`` objects in item ``indexes`` are automatically converted
        to ISO-8601 strings.

        Args:
            variable: KV store variable (namespace).
            items: List of dicts, each with at minimum a ``data`` key.
                Optional keys: ``key``, ``indexes``, ``ttl``.
            upsert: If ``True`` (default), overwrite existing documents.

        Returns:
            List of document keys (in the same order as *items*).
        """
        body = build_bulk_put_body(items, upsert=upsert)
        response = self._http.request(
            "POST", f"/api/v1/store/{variable}/documents:bulkPut", json=body,
        )
        return parse_bulk_put_response(response.json())
