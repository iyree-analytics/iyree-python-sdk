"""SDK exception hierarchy for the IYREE Python SDK."""

from __future__ import annotations

from typing import Any, Optional, Union


class IyreeError(Exception):
    """Base exception for all IYREE SDK errors."""

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        response_body: Union[dict, str, None] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.response_body = response_body


class IyreeConfigError(IyreeError):
    """Raised when SDK configuration is invalid or incomplete."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class IyreeAuthError(IyreeError):
    """Raised on HTTP 401 — invalid or missing API key."""


class IyreePermissionError(IyreeError):
    """Raised on HTTP 403 — insufficient permissions."""


class IyreeNotFoundError(IyreeError):
    """Raised on HTTP 404 — requested resource not found."""


class IyreeValidationError(IyreeError):
    """Raised on HTTP 422 — request payload failed server-side validation."""


class IyreeRateLimitError(IyreeError):
    """Raised on HTTP 429 after retries exhausted."""


class IyreeServerError(IyreeError):
    """Raised on HTTP 5xx after retries exhausted."""


class IyreeTimeoutError(IyreeError):
    """Raised when a request times out."""


class IyreeStreamLoadError(IyreeError):
    """Raised when a DWH Stream Load reports status != 'Success'."""


class IyreeDuplicateLabelError(IyreeStreamLoadError):
    """Raised when Stream Load responds with 'Label Already Exists'."""


class IyreeCubeTimeoutError(IyreeError):
    """Raised when Cube 'Continue wait' polling exceeds the configured timeout."""


class IyreeS3Error(IyreeError):
    """Raised when an S3 presigned-URL operation fails."""
