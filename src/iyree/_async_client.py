"""Asynchronous top-level IYREE client."""

from __future__ import annotations

from typing import Optional

from iyree._config import IyreeConfig, build_config
from iyree._http._async import AsyncHttpTransport
from iyree.cube._async import AsyncCubeClient
from iyree.dwh._async import AsyncDwhClient
from iyree.kv._async import AsyncKvClient
from iyree.s3._async import AsyncS3Client


class AsyncIyreeClient:
    """Asynchronous entry point for the IYREE SDK.

    Sub-clients (:attr:`dwh`, :attr:`cube`, :attr:`s3`, :attr:`kv`) are lazily
    instantiated on first access and share the underlying HTTP session.

    The constructor is **not** async — it stores configuration and eagerly
    creates the ``httpx.AsyncClient``.  Call :meth:`close` or use
    ``async with`` to release resources.

    Args:
        api_key: API key for gateway authentication.
        gateway_host: Gateway base URL. Falls back to ``IYREE_GATEWAY_HOST`` env var.
        timeout: Default request timeout in seconds.
        stream_load_timeout: Timeout for DWH Stream Load uploads.
        cube_continue_wait_timeout: Max Cube continue-wait polling duration.
        max_retries: Maximum retry attempts for retryable errors.

    Raises:
        ValueError: If *api_key* is empty or ``None``.
        IyreeConfigError: If *gateway_host* cannot be determined.

    Example::

        async with AsyncIyreeClient(api_key="my-key") as client:
            result = await client.dwh.sql("SELECT 1")
    """

    def __init__(
        self,
        api_key: str,
        gateway_host: Optional[str] = None,
        timeout: float = 30.0,
        stream_load_timeout: float = 300.0,
        cube_continue_wait_timeout: float = 120.0,
        max_retries: int = 3,
    ) -> None:
        self._config = build_config(
            api_key=api_key,
            gateway_host=gateway_host,
            timeout=timeout,
            stream_load_timeout=stream_load_timeout,
            cube_continue_wait_timeout=cube_continue_wait_timeout,
            max_retries=max_retries,
        )
        self._http = AsyncHttpTransport(self._config)

        self._dwh: Optional[AsyncDwhClient] = None
        self._cube: Optional[AsyncCubeClient] = None
        self._s3: Optional[AsyncS3Client] = None
        self._kv: Optional[AsyncKvClient] = None

    # ------------------------------------------------------------------
    # Lazy sub-client properties
    # ------------------------------------------------------------------

    @property
    def dwh(self) -> AsyncDwhClient:
        """DWH (StarRocks) sub-client."""
        if self._dwh is None:
            self._dwh = AsyncDwhClient(self._http, self._config)
        return self._dwh

    @property
    def cube(self) -> AsyncCubeClient:
        """Cube.js sub-client."""
        if self._cube is None:
            self._cube = AsyncCubeClient(self._http, self._config)
        return self._cube

    @property
    def s3(self) -> AsyncS3Client:
        """S3 sub-client."""
        if self._s3 is None:
            self._s3 = AsyncS3Client(self._http, self._config)
        return self._s3

    @property
    def kv(self) -> AsyncKvClient:
        """Key-Value store sub-client."""
        if self._kv is None:
            self._kv = AsyncKvClient(self._http, self._config)
        return self._kv

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def close(self) -> None:
        """Close the underlying HTTP session."""
        await self._http.close()

    async def __aenter__(self) -> AsyncIyreeClient:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()
