"""Synchronous top-level IYREE client."""

from __future__ import annotations

from typing import Optional

from iyree._config import IyreeConfig, build_config
from iyree._http._sync import HttpTransport
from iyree.cube._sync import CubeClient
from iyree.dwh._sync import DwhClient
from iyree.kv._sync import KvClient
from iyree.s3._sync import S3Client


class IyreeClient:
    """Synchronous entry point for the IYREE SDK.

    Sub-clients (:attr:`dwh`, :attr:`cube`, :attr:`s3`, :attr:`kv`) are lazily
    instantiated on first access and share the underlying HTTP session.

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

        with IyreeClient(api_key="my-key") as client:
            result = client.dwh.sql("SELECT 1")
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
        self._http = HttpTransport(self._config)

        self._dwh: Optional[DwhClient] = None
        self._cube: Optional[CubeClient] = None
        self._s3: Optional[S3Client] = None
        self._kv: Optional[KvClient] = None

    # ------------------------------------------------------------------
    # Lazy sub-client properties
    # ------------------------------------------------------------------

    @property
    def dwh(self) -> DwhClient:
        """DWH (StarRocks) sub-client."""
        if self._dwh is None:
            self._dwh = DwhClient(self._http, self._config)
        return self._dwh

    @property
    def cube(self) -> CubeClient:
        """Cube.js sub-client."""
        if self._cube is None:
            self._cube = CubeClient(self._http, self._config)
        return self._cube

    @property
    def s3(self) -> S3Client:
        """S3 sub-client."""
        if self._s3 is None:
            self._s3 = S3Client(self._http, self._config)
        return self._s3

    @property
    def kv(self) -> KvClient:
        """Key-Value store sub-client."""
        if self._kv is None:
            self._kv = KvClient(self._http, self._config)
        return self._kv

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Close the underlying HTTP session."""
        self._http.close()

    def __enter__(self) -> IyreeClient:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()
