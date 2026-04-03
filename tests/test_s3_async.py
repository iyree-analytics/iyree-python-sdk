"""Async S3 client integration tests with respx mocking."""

from __future__ import annotations

import httpx
import pytest
import respx

from iyree._config import IyreeConfig
from iyree._http._async import AsyncHttpTransport
from iyree.s3._async import AsyncS3Client


@pytest.fixture
def transport(config: IyreeConfig) -> AsyncHttpTransport:
    return AsyncHttpTransport(config)


@pytest.fixture
def s3(transport: AsyncHttpTransport, config: IyreeConfig) -> AsyncS3Client:
    return AsyncS3Client(transport, config)


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


class TestAsyncS3List:
    @respx.mock
    async def test_list_objects(self, s3: AsyncS3Client, transport: AsyncHttpTransport, gateway_host: str):
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
        result = await s3.list_objects()
        assert len(result.objects) == 1
        await transport.close()

    @respx.mock
    async def test_list_objects_iter(self, s3: AsyncS3Client, transport: AsyncHttpTransport, gateway_host: str):
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
        keys = [obj.key async for obj in s3.list_objects_iter()]
        assert keys == ["a.txt", "b.txt"]
        await transport.close()


class TestAsyncS3UploadDownload:
    @respx.mock
    async def test_upload_object(self, s3: AsyncS3Client, transport: AsyncHttpTransport, gateway_host: str):
        respx.post(f"{gateway_host}/api/v1/s3/objects:generatePresignedUrl").mock(
            return_value=httpx.Response(200, json=PRESIGNED_PUT_RESPONSE)
        )
        put_route = respx.put(PRESIGNED_PUT_RESPONSE["url"]).mock(
            return_value=httpx.Response(200)
        )
        await s3.upload_object("test.txt", b"hello")
        assert "x-api-key" not in put_route.calls[0].request.headers
        await transport.close()

    @respx.mock
    async def test_download_object(self, s3: AsyncS3Client, transport: AsyncHttpTransport, gateway_host: str):
        respx.post(f"{gateway_host}/api/v1/s3/objects:generatePresignedUrl").mock(
            return_value=httpx.Response(200, json=PRESIGNED_GET_RESPONSE)
        )
        respx.get(PRESIGNED_GET_RESPONSE["url"]).mock(
            return_value=httpx.Response(200, content=b"data")
        )
        data = await s3.download_object("key")
        assert data == b"data"
        await transport.close()
