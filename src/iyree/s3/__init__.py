"""IYREE S3 sub-client."""

from iyree._types import (
    S3CopyResult,
    S3DeleteError,
    S3DeleteResult,
    S3ListResult,
    S3Object,
)
from iyree.s3._async import AsyncS3Client
from iyree.s3._sync import S3Client

__all__ = [
    "S3Client",
    "AsyncS3Client",
    "S3Object",
    "S3ListResult",
    "S3CopyResult",
    "S3DeleteResult",
    "S3DeleteError",
]
