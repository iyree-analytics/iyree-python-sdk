"""Tests for the module-level iyree.init() / iyree.dwh / iyree.cube API."""

from __future__ import annotations

import httpx
import pytest
import respx

import iyree
from iyree.exceptions import IyreeConfigError


@pytest.fixture(autouse=True)
def _reset_default_client():
    """Ensure each test starts and ends with no default client."""
    iyree._default_client = None
    for name in ("dwh", "cube", "s3", "kv"):
        vars(iyree).pop(name, None)
    yield
    if iyree._default_client is not None:
        iyree._default_client.close()
        iyree._default_client = None
    for name in ("dwh", "cube", "s3", "kv"):
        vars(iyree).pop(name, None)


class TestInit:
    def test_init_creates_default_client(self):
        iyree.init(api_key="test-key", gateway_host="https://gw.example.com")
        assert iyree._default_client is not None

    def test_init_replaces_previous_client(self):
        iyree.init(api_key="key1", gateway_host="https://gw.example.com")
        first = iyree._default_client
        iyree.init(api_key="key2", gateway_host="https://gw.example.com")
        assert iyree._default_client is not first

    def test_init_missing_api_key_raises(self):
        with pytest.raises(ValueError, match="api_key"):
            iyree.init(api_key="", gateway_host="https://gw.example.com")

    def test_init_missing_host_raises(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("IYREE_GATEWAY_HOST", raising=False)
        with pytest.raises(IyreeConfigError, match="gateway_host"):
            iyree.init(api_key="key")


class TestClose:
    def test_close_clears_client(self):
        iyree.init(api_key="key", gateway_host="https://gw.example.com")
        assert iyree._default_client is not None
        iyree.close()
        assert iyree._default_client is None

    def test_close_when_not_initialized(self):
        iyree.close()


class TestModuleLevelAccess:
    def test_dwh_before_init_raises(self):
        with pytest.raises(IyreeConfigError, match="not initialized"):
            _ = iyree.dwh

    def test_cube_before_init_raises(self):
        with pytest.raises(IyreeConfigError, match="not initialized"):
            _ = iyree.cube

    def test_s3_before_init_raises(self):
        with pytest.raises(IyreeConfigError, match="not initialized"):
            _ = iyree.s3

    def test_kv_before_init_raises(self):
        with pytest.raises(IyreeConfigError, match="not initialized"):
            _ = iyree.kv

    def test_unknown_attr_raises_attribute_error(self):
        with pytest.raises(AttributeError, match="no attribute 'foobar'"):
            _ = iyree.foobar  # type: ignore[attr-defined]

    def test_dwh_after_init(self):
        iyree.init(api_key="key", gateway_host="https://gw.example.com")
        assert iyree.dwh is not None

    def test_cube_after_init(self):
        iyree.init(api_key="key", gateway_host="https://gw.example.com")
        assert iyree.cube is not None

    def test_s3_after_init(self):
        iyree.init(api_key="key", gateway_host="https://gw.example.com")
        assert iyree.s3 is not None

    def test_kv_after_init(self):
        iyree.init(api_key="key", gateway_host="https://gw.example.com")
        assert iyree.kv is not None

    @respx.mock
    def test_dwh_sql_via_module(self):
        iyree.init(api_key="test-key", gateway_host="https://gw.example.com")
        ndjson = (
            '{"connectionId": 1}\n'
            '{"meta": [{"name": "n", "type": "INT"}]}\n'
            '{"data": [42]}\n'
            '{"statistics": {"scanRows": 1}}\n'
        )
        respx.post("https://gw.example.com/api/v1/dwh/sql").mock(
            return_value=httpx.Response(200, content=ndjson.encode())
        )
        result = iyree.dwh.sql("SELECT 42 AS n")
        assert result.rows == [[42]]

    def test_existing_imports_still_work(self):
        from iyree import IyreeClient, AsyncIyreeClient, IyreeError, Cube
        assert IyreeClient is not None
        assert AsyncIyreeClient is not None
        assert IyreeError is not None
        assert Cube is not None
