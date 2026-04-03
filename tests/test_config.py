"""Tests for _config.py."""

from __future__ import annotations

import os

import pytest

from iyree._config import IyreeConfig, build_config
from iyree.exceptions import IyreeConfigError


class TestBuildConfig:
    def test_basic_config(self):
        cfg = build_config(api_key="key", gateway_host="https://gw.example.com")
        assert cfg.api_key == "key"
        assert cfg.gateway_host == "https://gw.example.com"

    def test_trailing_slash_stripped(self):
        cfg = build_config(api_key="key", gateway_host="https://gw.example.com/")
        assert cfg.gateway_host == "https://gw.example.com"

    def test_scheme_auto_added(self):
        cfg = build_config(api_key="key", gateway_host="gw.example.com")
        assert cfg.gateway_host == "https://gw.example.com"

    def test_empty_api_key_raises(self):
        with pytest.raises(ValueError, match="api_key is required"):
            build_config(api_key="", gateway_host="https://gw.example.com")

    def test_none_api_key_raises(self):
        with pytest.raises(ValueError, match="api_key is required"):
            build_config(api_key=None, gateway_host="https://gw.example.com")  # type: ignore[arg-type]

    def test_no_host_raises(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.delenv("IYREE_GATEWAY_HOST", raising=False)
        with pytest.raises(IyreeConfigError, match="gateway_host"):
            build_config(api_key="key")

    def test_host_from_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("IYREE_GATEWAY_HOST", "https://env-gw.example.com")
        cfg = build_config(api_key="key")
        assert cfg.gateway_host == "https://env-gw.example.com"

    def test_explicit_host_overrides_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("IYREE_GATEWAY_HOST", "https://env.example.com")
        cfg = build_config(api_key="key", gateway_host="https://explicit.example.com")
        assert cfg.gateway_host == "https://explicit.example.com"

    def test_frozen_config(self):
        cfg = build_config(api_key="key", gateway_host="https://gw.example.com")
        with pytest.raises(AttributeError):
            cfg.api_key = "new"  # type: ignore[misc]
