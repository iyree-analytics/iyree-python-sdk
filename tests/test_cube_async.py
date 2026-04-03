"""Async Cube client integration tests with respx mocking."""

from __future__ import annotations

import httpx
import pytest
import respx

from iyree._config import IyreeConfig
from iyree._http._async import AsyncHttpTransport
from iyree.cube._async import AsyncCubeClient

from _helpers import make_jwt


@pytest.fixture
def transport(config: IyreeConfig) -> AsyncHttpTransport:
    return AsyncHttpTransport(config)


@pytest.fixture
def cube(transport: AsyncHttpTransport, config: IyreeConfig) -> AsyncCubeClient:
    return AsyncCubeClient(transport, config)


class TestAsyncCubeLoad:
    @respx.mock
    async def test_basic_load(self, cube: AsyncCubeClient, transport: AsyncHttpTransport, gateway_host: str):
        token = make_jwt()
        respx.get(f"{gateway_host}/api/v1/cube/token").mock(
            return_value=httpx.Response(200, json={"data": {"token": token}})
        )
        respx.get(f"{gateway_host}/api/v1/cube/load").mock(
            return_value=httpx.Response(200, json={
                "data": [{"orders.count": 42}],
                "annotation": {},
                "query": {},
            })
        )
        result = await cube.load({"measures": ["orders.count"]})
        assert result.data == [{"orders.count": 42}]
        await transport.close()

    @respx.mock
    async def test_continue_wait_polling(self, cube: AsyncCubeClient, transport: AsyncHttpTransport, gateway_host: str):
        token = make_jwt()
        respx.get(f"{gateway_host}/api/v1/cube/token").mock(
            return_value=httpx.Response(200, json={"data": {"token": token}})
        )

        call_count = 0
        def side_effect(request):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return httpx.Response(200, json={"error": "Continue wait"})
            return httpx.Response(200, json={
                "data": [{"orders.count": 10}],
                "annotation": {},
                "query": {},
            })

        respx.get(f"{gateway_host}/api/v1/cube/load").mock(side_effect=side_effect)
        result = await cube.load({"measures": ["orders.count"]})
        assert result.data == [{"orders.count": 10}]
        await transport.close()

    @respx.mock
    async def test_token_cached(self, cube: AsyncCubeClient, transport: AsyncHttpTransport, gateway_host: str):
        token = make_jwt()
        token_route = respx.get(f"{gateway_host}/api/v1/cube/token").mock(
            return_value=httpx.Response(200, json={"data": {"token": token}})
        )
        respx.get(f"{gateway_host}/api/v1/cube/load").mock(
            return_value=httpx.Response(200, json={
                "data": [], "annotation": {}, "query": {},
            })
        )
        await cube.load({"measures": ["orders.count"]})
        await cube.load({"measures": ["orders.count"]})
        assert len(token_route.calls) == 1
        await transport.close()
