"""A tenant without checkpoint storage gets a defined 403, not a 500 (proposal §4.1)."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aegra_api.api import threads as threads_module
from aegra_api.core.orm import get_session as core_get_session
from aegra_api.core.tenancy.checkpointer import TenantCheckpointerError, TenantCheckpointTableMissingError
from aegra_api.main import exception_handlers, tenant_checkpointer_exception_handler
from tests.fixtures.clients import create_test_app, make_client
from tests.fixtures.database import DummySessionBase, override_get_session_dep
from tests.fixtures.test_helpers import DummyThread


class _Result:
    def all(self) -> list[object]:
        return []


class Session(DummySessionBase):
    async def scalar(self, _stmt: object) -> object:
        return DummyThread("thread-1", "idle", None, "test-user")

    async def scalars(self, _stmt: object) -> _Result:
        return _Result()

    async def delete(self, obj: object) -> None:
        raise AssertionError("the thread row must survive when its checkpoints cannot be deleted")

    async def commit(self) -> None:
        raise AssertionError("nothing to commit")


def test_handler_is_registered_for_tenant_checkpointer_errors() -> None:
    assert exception_handlers[TenantCheckpointerError] is tenant_checkpointer_exception_handler


def test_delete_thread_returns_403_when_the_tenant_table_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    checkpointer = AsyncMock()
    checkpointer.adelete_thread.side_effect = TenantCheckpointTableMissingError("org-1", "aegra-ckpt-org-1")
    db = MagicMock()
    db.get_checkpointer.return_value = checkpointer
    monkeypatch.setattr(threads_module, "db_manager", db)

    app = create_test_app(include_runs=False, include_threads=True)
    app.add_exception_handler(TenantCheckpointerError, tenant_checkpointer_exception_handler)
    app.dependency_overrides[core_get_session] = override_get_session_dep(Session)
    client = make_client(app)

    response = client.delete("/threads/thread-1")

    assert response.status_code == 403
    body = response.json()
    assert body["error"] == "forbidden"
    assert body["message"] == "Tenant checkpoint storage is not provisioned"
    assert "aegra-ckpt-org-1" not in response.text


def _get_graph_raising(exc: Exception) -> MagicMock:
    mock = MagicMock()

    @asynccontextmanager
    async def raising(*_args: object, **_kwargs: object) -> AsyncIterator[object]:
        raise exc
        yield  # pragma: no cover - makes this an async generator

    mock.side_effect = lambda *args, **kwargs: raising(*args, **kwargs)
    return mock


@pytest.mark.parametrize("path", ["/threads/thread-1/state", "/threads/thread-1/history"])
def test_state_routes_pass_the_error_through_instead_of_a_500(path: str) -> None:
    thread = DummyThread("thread-1", "idle", {"graph_id": "stress_test"}, "test-user")
    thread.metadata_json = {"graph_id": "stress_test"}

    class ThreadSession(DummySessionBase):
        async def scalar(self, _stmt: object) -> object:
            return thread

    app = create_test_app(include_runs=False, include_threads=True)
    app.add_exception_handler(TenantCheckpointerError, tenant_checkpointer_exception_handler)
    app.dependency_overrides[core_get_session] = override_get_session_dep(ThreadSession)
    client = make_client(app)

    with patch("aegra_api.services.langgraph_service.get_langgraph_service") as get_service:
        get_service.return_value.get_graph = _get_graph_raising(
            TenantCheckpointTableMissingError("org-1", "aegra-ckpt-org-1")
        )
        response = client.get(path)

    assert response.status_code == 403
    assert response.json()["error"] == "forbidden"
