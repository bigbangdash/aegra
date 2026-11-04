from collections.abc import Iterator

import pytest
from fastapi import HTTPException

from aegra_api.core.tenancy.resolver import (
    TenantRejectedError,
    configure_tenant_resolver,
    resolve_tenant_id,
    tenant_db_scope,
)
from aegra_api.core.tenancy.scope import (
    DbScopeMissingError,
    current_db_scope,
    scoped_tenant_id,
    system_scope,
    tenant_scope,
)
from aegra_api.models.auth import User
from aegra_api.settings import settings


@pytest.fixture(autouse=True)
def _default_resolver() -> Iterator[None]:
    yield
    configure_tenant_resolver(None)


async def test_resolve_tenant_id_uses_org_id_by_default() -> None:
    assert await resolve_tenant_id(User(identity="u1", org_id="org-a")) == "org-a"


@pytest.mark.parametrize("org_id", [None, ""])
async def test_resolve_tenant_id_rejects_user_without_org(org_id: str | None) -> None:
    with pytest.raises(TenantRejectedError, match="requires an org_id"):
        await resolve_tenant_id(User(identity="u1", org_id=org_id))


@pytest.mark.parametrize("org_id", ["org.a", "org a", "x" * 65])
async def test_resolve_tenant_id_rejects_malformed_org(org_id: str) -> None:
    with pytest.raises(TenantRejectedError, match="tenant id must match"):
        await resolve_tenant_id(User(identity="u1", org_id=org_id))


async def test_resolve_tenant_id_ignores_client_supplied_extra_fields() -> None:
    user = User(identity="u1", org_id="org-a", tenant_id="org-evil")

    assert await resolve_tenant_id(user) == "org-a"


async def test_configured_resolver_replaces_the_org_id_default() -> None:
    async def by_service_claim(user: User) -> str:
        return f"svc-{user.identity}"

    configure_tenant_resolver(by_service_claim)

    assert await resolve_tenant_id(User(identity="bff", org_id="ignored")) == "svc-bff"


async def test_configured_resolver_can_reject_an_inactive_tenant() -> None:
    async def registry(user: User) -> str:
        raise TenantRejectedError(f"tenant {user.org_id} is inactive")

    configure_tenant_resolver(registry)

    with pytest.raises(TenantRejectedError, match="inactive"):
        await resolve_tenant_id(User(identity="u1", org_id="org-a"))


@pytest.mark.parametrize("bad", ["org.a", "", 42])
async def test_resolver_output_is_validated_even_for_a_custom_resolver(bad: object) -> None:
    async def sloppy(_user: User) -> object:
        return bad

    configure_tenant_resolver(sloppy)  # type: ignore[arg-type]  # the point is a resolver breaking its contract

    with pytest.raises(TenantRejectedError):
        await resolve_tenant_id(User(identity="u1", org_id="org-a"))


async def test_configure_none_restores_the_default() -> None:
    async def other(_user: User) -> str:
        return "other"

    configure_tenant_resolver(other)
    configure_tenant_resolver(None)

    assert await resolve_tenant_id(User(identity="u1", org_id="org-a")) == "org-a"


def test_scoped_tenant_id_is_none_when_rls_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", False)

    assert scoped_tenant_id() is None


def test_scoped_tenant_id_reads_the_current_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)

    with tenant_scope("org-a"):
        in_tenant = scoped_tenant_id()
    with system_scope("unit test"):
        in_system = scoped_tenant_id()

    assert in_tenant == "org-a"
    assert in_system is None


def test_scoped_tenant_id_fails_closed_without_a_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)

    with pytest.raises(DbScopeMissingError):
        scoped_tenant_id()


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


async def test_tenant_db_scope_uses_the_configured_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)

    async def mapped(_user: User) -> str:
        return "org-mapped"

    configure_tenant_resolver(mapped)
    dependency = tenant_db_scope(User(identity="u1", org_id="org-a"))

    await anext(dependency)
    inside = current_db_scope()
    await dependency.aclose()

    assert inside.tenant_id == "org-mapped"


async def test_tenant_db_scope_turns_a_rejection_into_403(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)

    async def registry(_user: User) -> str:
        raise TenantRejectedError("tenant org-a is inactive")

    configure_tenant_resolver(registry)
    dependency = tenant_db_scope(User(identity="u1", org_id="org-a"))

    with pytest.raises(HTTPException) as exc_info:
        await anext(dependency)

    assert exc_info.value.status_code == 403
    assert "inactive" in exc_info.value.detail
    with pytest.raises(DbScopeMissingError):
        current_db_scope()


async def test_tenant_db_scope_sets_nothing_when_rls_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", False)

    async def never(_user: User) -> str:
        raise AssertionError("resolver must not run with RLS off")

    configure_tenant_resolver(never)
    dependency = tenant_db_scope(User(identity="u1"))

    await anext(dependency)

    with pytest.raises(DbScopeMissingError):
        current_db_scope()
    await dependency.aclose()
