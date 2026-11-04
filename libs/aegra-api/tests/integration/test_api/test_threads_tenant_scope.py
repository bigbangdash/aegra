"""Threads API wiring for tenant RLS: request scope, 403 without org_id, tenant_id on insert."""

from typing import Any

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Insert
from sqlalchemy.dialects import postgresql

from aegra_api.core.auth_deps import get_current_user, require_auth
from aegra_api.core.tenancy.scope import DbScope, DbScopeMissingError, current_db_scope
from aegra_api.models.auth import User
from aegra_api.settings import settings
from tests.fixtures.clients import create_test_app, make_client
from tests.fixtures.database import DummyScalarResult, DummySessionBase
from tests.fixtures.session_fixtures import override_session_dependency


def _insert_params(stmt: Insert) -> dict[str, Any]:
    compiled = stmt.compile(dialect=postgresql.dialect())
    params: dict[str, Any] = dict(compiled.params)
    # Python-side column defaults (tenant_id) are resolved at execution time, not compile time.
    for column in compiled.prefetch:
        params[column.key] = column.default.arg(None)
    return params


def _scope_or_none() -> DbScope | None:
    try:
        return current_db_scope()
    except DbScopeMissingError:
        return None


class RecordingSession(DummySessionBase):
    """Records the DB scope active at each query and the params of each INSERT."""

    scopes: list[DbScope | None] = []
    inserts: list[dict[str, Any]] = []

    async def scalar(self, _stmt: Any) -> None:
        RecordingSession.scopes.append(_scope_or_none())
        return None

    async def scalars(self, stmt: Any = None) -> DummyScalarResult:
        RecordingSession.scopes.append(_scope_or_none())
        if isinstance(stmt, Insert):
            RecordingSession.inserts.append(_insert_params(stmt))
        return await super().scalars(stmt)


@pytest.fixture(autouse=True)
def _reset_recording() -> None:
    RecordingSession.scopes = []
    RecordingSession.inserts = []


def _client(user: User | None = None) -> TestClient:
    app = create_test_app(include_runs=False, include_threads=True)
    if user is not None:
        app.dependency_overrides[require_auth] = lambda: user
        app.dependency_overrides[get_current_user] = lambda: user
    override_session_dependency(app, RecordingSession)
    return make_client(app)


def _enable_rls(monkeypatch: pytest.MonkeyPatch, enabled: bool) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", enabled)


def test_create_thread_tags_row_with_org_tenant_when_rls_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_rls(monkeypatch, True)

    resp = _client().post("/threads", json={"metadata": {}})

    assert resp.status_code == 200
    assert [params["tenant_id"] for params in RecordingSession.inserts] == ["org-1"]


def test_thread_queries_run_inside_tenant_scope_when_rls_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_rls(monkeypatch, True)

    resp = _client().get("/threads/missing-thread")

    assert resp.status_code == 404
    assert RecordingSession.scopes
    assert all(scope is not None and scope.tenant_id == "org-1" for scope in RecordingSession.scopes)


@pytest.mark.parametrize("org_id", [None, ""])
def test_user_without_org_id_is_forbidden_when_rls_enabled(monkeypatch: pytest.MonkeyPatch, org_id: str | None) -> None:
    _enable_rls(monkeypatch, True)
    client = _client(User(identity="no-org-user", org_id=org_id))

    create = client.post("/threads", json={"metadata": {}})
    read = client.get("/threads/some-thread")

    assert create.status_code == 403
    assert read.status_code == 403
    assert RecordingSession.scopes == []


def test_rls_disabled_keeps_current_behavior(monkeypatch: pytest.MonkeyPatch) -> None:
    _enable_rls(monkeypatch, False)
    client = _client(User(identity="no-org-user"))

    resp = client.post("/threads", json={"metadata": {}})

    assert resp.status_code == 200
    assert [params["tenant_id"] for params in RecordingSession.inserts] == [None]
    assert all(scope is None for scope in RecordingSession.scopes)
