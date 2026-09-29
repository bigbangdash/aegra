import pytest
from fastapi import HTTPException

from aegra_api.core.db_scope import DbScopeMissingError, current_db_scope
from aegra_api.core.tenant import resolve_tenant_id, tenant_db_scope, tenant_id_for
from aegra_api.models.auth import User
from aegra_api.settings import settings


def test_resolve_tenant_id_uses_org_id() -> None:
    assert resolve_tenant_id(User(identity="u1", org_id="org-a")) == "org-a"


@pytest.mark.parametrize("org_id", [None, ""])
def test_resolve_tenant_id_rejects_user_without_org(org_id: str | None) -> None:
    with pytest.raises(HTTPException) as exc_info:
        resolve_tenant_id(User(identity="u1", org_id=org_id))

    assert exc_info.value.status_code == 403


def test_resolve_tenant_id_ignores_client_supplied_extra_fields() -> None:
    user = User(identity="u1", org_id="org-a", tenant_id="org-evil")

    assert resolve_tenant_id(user) == "org-a"


def test_tenant_id_for_returns_none_when_rls_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", False)

    assert tenant_id_for(User(identity="u1")) is None


def test_tenant_id_for_requires_org_when_rls_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)

    with pytest.raises(HTTPException):
        tenant_id_for(User(identity="u1"))


async def test_tenant_db_scope_sets_and_clears_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)
    dependency = tenant_db_scope(User(identity="u1", org_id="org-a"))

    await anext(dependency)
    inside = current_db_scope()
    with pytest.raises(StopAsyncIteration):
        await anext(dependency)

    assert inside.tenant_id == "org-a"
    with pytest.raises(DbScopeMissingError):
        current_db_scope()


async def test_tenant_db_scope_sets_nothing_when_rls_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", False)
    dependency = tenant_db_scope(User(identity="u1"))

    await anext(dependency)

    with pytest.raises(DbScopeMissingError):
        current_db_scope()
    await dependency.aclose()
