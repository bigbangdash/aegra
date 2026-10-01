"""Health and readiness probe the per-tenant checkpoint backend through its provider."""

from collections.abc import Iterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from aegra_api.core import health as health_module
from aegra_api.core.tenancy.checkpointer import TenantRoutingCheckpointer
from tests.fixtures.clients import create_test_app, make_client


class FakeProvider:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.health_calls = 0

    async def for_tenant(self, tenant_id: str) -> Any:
        raise AssertionError("health must not build tenant savers")

    def for_tenant_sync(self, tenant_id: str) -> Any:
        raise AssertionError("health must not build tenant savers")

    async def health(self) -> None:
        self.health_calls += 1
        if self.error is not None:
            raise self.error


def _db_manager(checkpointer: Any) -> MagicMock:
    engine = MagicMock()

    @asynccontextmanager
    async def begin() -> Any:
        yield AsyncMock()

    engine.begin = begin
    db = MagicMock()
    db.engine = engine
    db.get_checkpointer.return_value = checkpointer
    db.get_store.return_value = AsyncMock()
    return db


@pytest.fixture
def provider(monkeypatch: pytest.MonkeyPatch) -> Iterator[FakeProvider]:
    fake = FakeProvider()
    monkeypatch.setattr(health_module, "db_manager", _db_manager(TenantRoutingCheckpointer(fake)))
    yield fake


def test_health_reports_connected_when_the_provider_is_healthy(provider: FakeProvider) -> None:
    client = make_client(create_test_app(include_runs=False, include_threads=False))

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["langgraph_checkpointer"] == "connected"
    assert provider.health_calls == 1


def test_health_is_503_when_the_provider_fails(provider: FakeProvider) -> None:
    provider.error = RuntimeError("sts unreachable")
    client = make_client(create_test_app(include_runs=False, include_threads=False))

    response = client.get("/health")

    assert response.status_code == 503
    assert provider.health_calls == 1


def test_ready_is_503_when_the_provider_fails(provider: FakeProvider) -> None:
    provider.error = RuntimeError("sts unreachable")
    client = make_client(create_test_app(include_runs=False, include_threads=False))

    response = client.get("/ready")

    assert response.status_code == 503
    assert "sts unreachable" in response.json()["detail"]


def test_ready_passes_when_the_provider_is_healthy(provider: FakeProvider) -> None:
    client = make_client(create_test_app(include_runs=False, include_threads=False))

    response = client.get("/ready")

    assert response.status_code == 200
    assert provider.health_calls == 1


def test_postgres_checkpointer_is_probed_with_a_tuple_read(monkeypatch: pytest.MonkeyPatch) -> None:
    checkpointer = AsyncMock()
    checkpointer.aget_tuple.side_effect = RuntimeError("no such thread")  # suppressed, as before
    monkeypatch.setattr(health_module, "db_manager", _db_manager(checkpointer))
    client = make_client(create_test_app(include_runs=False, include_threads=False))

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["langgraph_checkpointer"] == "connected"
    checkpointer.aget_tuple.assert_awaited_once_with({"configurable": {"thread_id": "health-check"}})
