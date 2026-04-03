"""Shared test fixtures."""

from __future__ import annotations

import pytest

from iyree._config import IyreeConfig


@pytest.fixture
def config() -> IyreeConfig:
    return IyreeConfig(
        gateway_host="https://api.test.iyree.io",
        api_key="test-api-key-12345",
        timeout=5.0,
        stream_load_timeout=30.0,
        cube_continue_wait_timeout=10.0,
        max_retries=2,
    )


@pytest.fixture
def gateway_host(config: IyreeConfig) -> str:
    return config.gateway_host


@pytest.fixture
def api_key(config: IyreeConfig) -> str:
    return config.api_key
