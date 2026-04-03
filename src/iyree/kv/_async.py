"""Asynchronous KV client."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from iyree._config import IyreeConfig
from iyree._http._async import AsyncHttpTransport
from iyree._types import KvDocument
from iyree.exceptions import IyreeNotFoundError
from iyree.kv._common import (
    build_patch_body,
    build_put_body,
    parse_document_response,
    parse_put_response,
)

logger = logging.getLogger("iyree")


class AsyncKvClient:
    """Asynchronous client for the IYREE Key-Value store.

    Args:
        http: Shared asynchronous HTTP transport.
        config: SDK configuration.
    """

    def __init__(self, http: AsyncHttpTransport, config: IyreeConfig) -> None:
        self._http = http
        self._config = config

    async def get(self, variable: str, key: str) -> KvDocument:
        """Retrieve a document by key.

        Args:
            variable: KV store variable (namespace).
            key: Document key.

        Returns:
            The stored document.

        Raises:
            IyreeNotFoundError: If the document does not exist.
        """
        response = await self._http.request(
            "GET", f"/api/v1/store/{variable}/documents/{key}",
        )
        return parse_document_response(response.json())

    async def put(
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
        response = await self._http.request(
            "POST", f"/api/v1/store/{variable}/documents", json=body,
        )
        return parse_put_response(response.json())

    async def delete(self, variable: str, key: str) -> bool:
        """Delete a document.

        Args:
            variable: KV store variable (namespace).
            key: Document key.

        Returns:
            ``True`` if the document was deleted.
        """
        await self._http.request(
            "DELETE", f"/api/v1/store/{variable}/documents/{key}",
        )
        return True

    async def exists(self, variable: str, key: str) -> bool:
        """Check whether a document exists.

        Does **not** raise on 404 — returns ``False`` instead.

        Args:
            variable: KV store variable (namespace).
            key: Document key.

        Returns:
            ``True`` if the document exists, ``False`` otherwise.
        """
        try:
            await self._http.request(
                "HEAD", f"/api/v1/store/{variable}/documents/{key}",
            )
            return True
        except IyreeNotFoundError:
            return False

    async def patch(
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
        response = await self._http.request(
            "PATCH",
            f"/api/v1/store/{variable}/documents/{key}",
            json=body,
        )
        return parse_document_response(response.json())
