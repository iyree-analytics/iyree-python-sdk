"""Synchronous Cube client with JWT management and continue-wait polling."""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, Optional, Union

from iyree._config import IyreeConfig
from iyree._http._sync import HttpTransport
from iyree._types import CubeQueryResult
from iyree.cube._common import (
    build_load_params,
    compute_continue_wait_delay,
    is_continue_wait_response,
    is_token_expired,
    needs_multi_query,
    parse_jwt_expiry,
    parse_load_response,
    parse_token_response,
    serialize_query,
)
from iyree.cube._querybuilder.query import Query
from iyree.exceptions import IyreeAuthError, IyreeCubeTimeoutError, IyreePermissionError

logger = logging.getLogger("iyree")


class CubeClient:
    """Synchronous client for the IYREE Cube.js API.

    Args:
        http: Shared synchronous HTTP transport.
        config: SDK configuration.
    """

    def __init__(self, http: HttpTransport, config: IyreeConfig) -> None:
        self._http = http
        self._config = config
        self._token: Optional[str] = None
        self._token_expiry: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load(
        self,
        query: Union[Dict[str, Any], Query],
        *,
        timeout: Optional[float] = None,
    ) -> CubeQueryResult:
        """Execute a Cube.js query.

        Handles token refresh and the Cube "Continue wait" polling loop.

        Args:
            query: A :class:`Query` object or a raw query dict.
            timeout: Override for the continue-wait polling timeout.

        Returns:
            :class:`CubeQueryResult` with data, annotations, and the original query.

        Raises:
            IyreeCubeTimeoutError: If polling exceeds the configured timeout.
        """
        self._ensure_token()
        effective_timeout = timeout or self._config.cube_continue_wait_timeout

        query_str = build_load_params(query)
        params: Dict[str, str] = {"query": query_str}
        if needs_multi_query(query):
            params["queryType"] = "multi"

        start = time.monotonic()
        attempt = 0

        while True:
            elapsed = time.monotonic() - start
            if elapsed >= effective_timeout:
                raise IyreeCubeTimeoutError(
                    f"Cube continue-wait polling exceeded {effective_timeout}s",
                    status_code=None,
                    response_body=None,
                )

            try:
                response = self._http.request(
                    "GET",
                    "/api/v1/cube/load",
                    params=params,
                    headers={"Authorization": f"Bearer {self._token}"},
                )
            except (IyreeAuthError, IyreePermissionError):
                if attempt == 0:
                    self._invalidate_token()
                    self._ensure_token()
                    attempt += 1
                    continue
                raise

            data = response.json()

            if is_continue_wait_response(data):
                delay = compute_continue_wait_delay(attempt)
                logger.debug(
                    "Cube continue-wait (attempt %d), sleeping %.1fs", attempt, delay,
                )
                time.sleep(delay)
                attempt += 1
                continue

            return parse_load_response(data)

    def meta(self, *, timeout: Optional[float] = None) -> Dict[str, Any]:
        """Fetch Cube.js metadata (cubes, measures, dimensions).

        Args:
            timeout: Per-request timeout override.

        Returns:
            The raw metadata dict from Cube.js.
        """
        self._ensure_token()
        response = self._http.request(
            "GET",
            "/api/v1/cube/meta",
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=timeout,
        )
        return response.json()

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def _ensure_token(self) -> None:
        if self._token is None or is_token_expired(self._token_expiry):
            self._refresh_token()

    def _refresh_token(self) -> None:
        logger.debug("Fetching new Cube JWT token")
        response = self._http.request("GET", "/api/v1/cube/token")
        data = response.json()
        self._token = parse_token_response(data)
        self._token_expiry = parse_jwt_expiry(self._token)

    def _invalidate_token(self) -> None:
        self._token = None
        self._token_expiry = 0.0
