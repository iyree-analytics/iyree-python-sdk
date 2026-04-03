"""Sync Cube client integration tests with respx mocking."""

from __future__ import annotations

import time

import httpx
import pytest
import respx

from iyree._config import IyreeConfig
from iyree._http._sync import HttpTransport
from iyree.cube._sync import CubeClient
from iyree.exceptions import IyreeCubeTimeoutError

from _helpers import make_jwt


@pytest.fixture
def transport(config: IyreeConfig) -> HttpTransport:
    t = HttpTransport(config)
    yield t
    t.close()


@pytest.fixture
def cube(transport: HttpTransport, config: IyreeConfig) -> CubeClient:
    return CubeClient(transport, config)


class TestCubeLoadSync:
    @respx.mock
    def test_basic_load(self, cube: CubeClient, gateway_host: str):
        token = make_jwt()
        respx.get(f"{gateway_host}/api/v1/cube/token").mock(
            return_value=httpx.Response(200, json={"data": {"token": token}})
        )
        respx.get(f"{gateway_host}/api/v1/cube/load").mock(
            return_value=httpx.Response(200, json={
                "data": [{"orders.count": 42}],
                "annotation": {},
                "query": {"measures": ["orders.count"]},
            })
        )
        result = cube.load({"measures": ["orders.count"]})
        assert result.data == [{"orders.count": 42}]

    @respx.mock
    def test_continue_wait_polling(self, cube: CubeClient, gateway_host: str):
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
        result = cube.load({"measures": ["orders.count"]})
        assert result.data == [{"orders.count": 10}]
        assert call_count == 3

    @respx.mock
    def test_token_cached(self, cube: CubeClient, gateway_host: str):
        token = make_jwt()
        token_route = respx.get(f"{gateway_host}/api/v1/cube/token").mock(
            return_value=httpx.Response(200, json={"data": {"token": token}})
        )
        respx.get(f"{gateway_host}/api/v1/cube/load").mock(
            return_value=httpx.Response(200, json={
                "data": [], "annotation": {}, "query": {},
            })
        )
        cube.load({"measures": ["orders.count"]})
        cube.load({"measures": ["orders.count"]})
        assert len(token_route.calls) == 1


class TestCubeMetaSync:
    @respx.mock
    def test_meta(self, cube: CubeClient, gateway_host: str):
        token = make_jwt()
        respx.get(f"{gateway_host}/api/v1/cube/token").mock(
            return_value=httpx.Response(200, json={"data": {"token": token}})
        )
        respx.get(f"{gateway_host}/api/v1/cube/meta").mock(
            return_value=httpx.Response(200, json={"cubes": [{"name": "orders"}]})
        )
        result = cube.meta()
        assert result["cubes"][0]["name"] == "orders"
