"""Sync S3 client integration tests with respx mocking."""

from __future__ import annotations

import httpx
import pytest
import respx

from iyree._config import IyreeConfig
from iyree._http._sync import HttpTransport
from iyree.exceptions import IyreeS3Error
from iyree.s3._sync import S3Client


@pytest.fixture
def transport(config: IyreeConfig) -> HttpTransport:
    t = HttpTransport(config)
    yield t
    t.close()


@pytest.fixture
def s3(transport: HttpTransport, config: IyreeConfig) -> S3Client:
    return S3Client(transport, config)


PRESIGNED_PUT_RESPONSE = {
    "url": "https://s3.example.com/bucket/key?sig=abc",
    "expires_in": 3600,
    "method": "PUT",
    "required_headers": {"x-amz-acl": "private"},
}

PRESIGNED_GET_RESPONSE = {
    "url": "https://s3.example.com/bucket/key?sig=def",
    "expires_in": 3600,
    "method": "GET",
    "required_headers": {},
}


class TestS3ListSync:
    @respx.mock
    def test_list_objects(self, s3: S3Client, gateway_host: str):
        respx.get(f"{gateway_host}/api/v1/s3/objects").mock(
            return_value=httpx.Response(200, json={
                "objects": [
                    {"key": "a.txt", "size": 100,
                     "last_modified": "2024-01-01T00:00:00Z", "etag": "abc"},
                ],
                "is_truncated": False,
                "key_count": 1,
            })
        )
        result = s3.list_objects(prefix="")
        assert len(result.objects) == 1
        assert result.objects[0].key == "a.txt"
        assert result.is_truncated is False

    @respx.mock
    def test_list_objects_iter_pagination(self, s3: S3Client, gateway_host: str):
        call_count = 0
        def side_effect(request):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(200, json={
                    "objects": [
                        {"key": "a.txt", "size": 10,
                         "last_modified": "2024-01-01T00:00:00Z", "etag": "e1"},
                    ],
                    "is_truncated": True,
                    "next_continuation_token": "tok",
                    "key_count": 1,
                })
            return httpx.Response(200, json={
                "objects": [
                    {"key": "b.txt", "size": 20,
                     "last_modified": "2024-01-02T00:00:00Z", "etag": "e2"},
                ],
                "is_truncated": False,
                "key_count": 1,
            })

        respx.get(f"{gateway_host}/api/v1/s3/objects").mock(side_effect=side_effect)
        keys = [obj.key for obj in s3.list_objects_iter()]
        assert keys == ["a.txt", "b.txt"]


class TestS3CopyDeleteSync:
    @respx.mock
    def test_copy_object(self, s3: S3Client, gateway_host: str):
        respx.post(f"{gateway_host}/api/v1/s3/objects:copy").mock(
            return_value=httpx.Response(200, json={
                "source_key": "src.txt", "destination_key": "dst.txt",
            })
        )
        result = s3.copy_object("src.txt", "dst.txt")
        assert result.destination_key == "dst.txt"

    @respx.mock
    def test_delete_objects(self, s3: S3Client, gateway_host: str):
        respx.post(f"{gateway_host}/api/v1/s3/objects:delete").mock(
            return_value=httpx.Response(200, json={
                "deleted": ["a.txt"],
                "errors": [{"key": "b.txt", "code": "NoSuchKey", "message": "not found"}],
            })
        )
        result = s3.delete_objects(["a.txt", "b.txt"])
        assert "a.txt" in result.deleted
        assert len(result.errors) == 1
        assert result.errors[0].key == "b.txt"


class TestS3UploadDownloadSync:
    @respx.mock
    def test_upload_object(self, s3: S3Client, gateway_host: str):
        respx.post(f"{gateway_host}/api/v1/s3/objects:generatePresignedUrl").mock(
            return_value=httpx.Response(200, json=PRESIGNED_PUT_RESPONSE)
        )
        put_route = respx.put(PRESIGNED_PUT_RESPONSE["url"]).mock(
            return_value=httpx.Response(200)
        )
        s3.upload_object("test.txt", b"hello")
        assert "x-api-key" not in put_route.calls[0].request.headers
        assert put_route.calls[0].request.headers.get("x-amz-acl") == "private"

    @respx.mock
    def test_download_object(self, s3: S3Client, gateway_host: str):
        respx.post(f"{gateway_host}/api/v1/s3/objects:generatePresignedUrl").mock(
            return_value=httpx.Response(200, json=PRESIGNED_GET_RESPONSE)
        )
        respx.get(PRESIGNED_GET_RESPONSE["url"]).mock(
            return_value=httpx.Response(200, content=b"file contents")
        )
        data = s3.download_object("key")
        assert data == b"file contents"
