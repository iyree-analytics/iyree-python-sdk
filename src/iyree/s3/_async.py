"""Asynchronous S3 client."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, AsyncIterator, BinaryIO, Dict, List, Optional, Union

from iyree._config import IyreeConfig
from iyree._http._async import AsyncHttpTransport
from iyree._types import (
    PresignedUrlInfo,
    S3CopyResult,
    S3DeleteResult,
    S3ListResult,
    S3Object,
)
from iyree.exceptions import IyreeS3Error
from iyree.s3._common import (
    build_copy_body,
    build_delete_body,
    build_list_params,
    build_presigned_url_body,
    normalise_upload_data,
    parse_copy_response,
    parse_delete_response,
    parse_list_response,
    parse_presigned_url_response,
)

logger = logging.getLogger("iyree")


class AsyncS3Client:
    """Asynchronous client for the IYREE S3 API.

    Management operations go through the gateway; data operations use presigned
    URLs to interact with S3 directly.

    Args:
        http: Shared asynchronous HTTP transport.
        config: SDK configuration.
    """

    def __init__(self, http: AsyncHttpTransport, config: IyreeConfig) -> None:
        self._http = http
        self._config = config

    # ------------------------------------------------------------------
    # Management operations
    # ------------------------------------------------------------------

    async def list_objects(
        self,
        prefix: str = "",
        max_keys: int = 1000,
        continuation_token: Optional[str] = None,
    ) -> S3ListResult:
        """List objects under *prefix* (single page).

        Args:
            prefix: Key prefix filter.
            max_keys: Maximum number of keys per page.
            continuation_token: Token from a previous page for pagination.

        Returns:
            A single page of results. Use :meth:`list_objects_iter` for
            auto-pagination.
        """
        params = build_list_params(prefix, max_keys, continuation_token)
        response = await self._http.request("GET", "/api/v1/s3/objects", params=params)
        return parse_list_response(response.json())

    async def list_objects_iter(
        self,
        prefix: str = "",
        max_keys: int = 1000,
    ) -> AsyncIterator[S3Object]:
        """Auto-paginating async iterator over S3 objects matching *prefix*.

        Yields:
            :class:`S3Object` instances across all pages.
        """
        token: Optional[str] = None
        while True:
            page = await self.list_objects(prefix, max_keys, token)
            for obj in page.objects:
                yield obj
            if not page.is_truncated or page.next_continuation_token is None:
                break
            token = page.next_continuation_token

    async def copy_object(
        self,
        source_key: str,
        destination_key: str,
    ) -> S3CopyResult:
        """Copy an object within the bucket.

        Args:
            source_key: Key of the source object.
            destination_key: Key for the destination copy.
        """
        body = build_copy_body(source_key, destination_key)
        response = await self._http.request("POST", "/api/v1/s3/objects:copy", json=body)
        return parse_copy_response(response.json(), source_key, destination_key)

    async def delete_objects(self, keys: List[str]) -> S3DeleteResult:
        """Delete one or more objects.

        If some deletions fail, the errors are returned in
        :attr:`S3DeleteResult.errors` — no exception is raised.

        Args:
            keys: Object keys to delete.
        """
        body = build_delete_body(keys)
        response = await self._http.request("POST", "/api/v1/s3/objects:delete", json=body)
        return parse_delete_response(response.json())

    # ------------------------------------------------------------------
    # Data operations (via presigned URLs)
    # ------------------------------------------------------------------

    async def upload_object(
        self,
        key: str,
        data: Union[bytes, str, BinaryIO],
        *,
        content_type: str = "application/octet-stream",
    ) -> None:
        """Upload an object using a presigned PUT URL.

        Args:
            key: Destination object key.
            data: Upload payload (bytes, str, or file-like).
            content_type: MIME type of the object.
        """
        info = await self._generate_presigned_url(key, "PUT", content_type)
        payload = normalise_upload_data(data)
        headers = {**info.required_headers, "Content-Type": content_type}
        response = await self._http.request_presigned(
            "PUT", info.url, content=payload, headers=headers,
        )
        if response.status_code >= 400:
            raise IyreeS3Error(
                f"S3 upload failed: HTTP {response.status_code}",
                status_code=response.status_code,
                response_body=response.text,
            )

    async def download_object(self, key: str) -> bytes:
        """Download an object and return its bytes.

        Args:
            key: Object key to download.
        """
        info = await self._generate_presigned_url(key, "GET")
        response = await self._http.request_presigned(
            "GET", info.url, headers=info.required_headers,
        )
        if response.status_code >= 400:
            raise IyreeS3Error(
                f"S3 download failed: HTTP {response.status_code}",
                status_code=response.status_code,
                response_body=response.text,
            )
        return response.content

    async def download_object_to_file(
        self,
        key: str,
        path: Union[str, Path],
    ) -> None:
        """Download an object and stream it to a local file.

        Args:
            key: Object key to download.
            path: Local file path to write to.
        """
        info = await self._generate_presigned_url(key, "GET")
        response = await self._http.request_presigned(
            "GET", info.url, headers=info.required_headers, stream=True,
        )
        if response.status_code >= 400:
            await response.aread()
            await response.aclose()
            raise IyreeS3Error(
                f"S3 download failed: HTTP {response.status_code}",
                status_code=response.status_code,
                response_body=response.text,
            )

        dest = Path(path)
        with open(dest, "wb") as f:
            async for chunk in response.aiter_bytes(chunk_size=65_536):
                f.write(chunk)
        await response.aclose()

    async def upload_dataframe(
        self,
        key: str,
        df: Any,
        *,
        format: str = "csv",
    ) -> None:
        """Upload a pandas DataFrame as CSV or Parquet.

        Args:
            key: Destination object key.
            df: A ``pd.DataFrame``.
            format: ``"csv"`` or ``"parquet"``.

        Raises:
            ImportError: If pandas is not installed.
            ValueError: If *format* is unsupported.
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError(
                "pandas is required for upload_dataframe(). "
                "Install it with: pip install iyree[pandas]"
            ) from None

        if format == "csv":
            payload = df.to_csv(index=False).encode("utf-8")
            content_type = "text/csv"
        elif format == "parquet":
            import io

            buf = io.BytesIO()
            df.to_parquet(buf, index=False)
            payload = buf.getvalue()
            content_type = "application/octet-stream"
        else:
            raise ValueError(f"Unsupported format: {format!r}. Use 'csv' or 'parquet'.")

        await self.upload_object(key, payload, content_type=content_type)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _generate_presigned_url(
        self,
        key: str,
        method: str,
        content_type: Optional[str] = None,
    ) -> PresignedUrlInfo:
        body = build_presigned_url_body(key, method, content_type)
        response = await self._http.request(
            "POST", "/api/v1/s3/objects:generatePresignedUrl", json=body,
        )
        return parse_presigned_url_response(response.json())
