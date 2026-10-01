"""Resolve the authenticated user's tenant and scope requests to it.

A tenant is resolved only at the three edges: the HTTP dependency, execute_run
and each cron fire. Code below an edge reads it from the DB scope (the ORM
tenant_id default), so one request or run never resolves twice.
"""

from collections.abc import AsyncIterator, Awaitable, Callable

from fastapi import Depends, HTTPException

from aegra_api.core.auth_deps import get_current_user
from aegra_api.core.tenancy.scope import TENANT_ID_PATTERN, is_valid_tenant_id, tenant_scope
from aegra_api.models.auth import User
from aegra_api.settings import settings

TenantResolver = Callable[[User], Awaitable[str]]


class TenantRejectedError(Exception):
    """A resolver refused the user's tenant (missing, malformed, unknown or inactive)."""


async def org_id_tenant_resolver(user: User) -> str:
    # The tenant comes only from the authenticated identity, never from a
    # client-supplied header, so a caller cannot pick someone else's tenant.
    if not user.org_id:
        raise TenantRejectedError("Tenant-scoped access requires an org_id on the authenticated user")
    return user.org_id


_resolver: TenantResolver = org_id_tenant_resolver


def configure_tenant_resolver(resolver: TenantResolver | None) -> None:
    # Hook for a tenant registry (known/active tenants) or a BFF mapping; None restores
    # the org_id default. Call it at import time, before the first request.
    global _resolver
    _resolver = resolver or org_id_tenant_resolver


async def resolve_tenant_id(user: User) -> str:
    tenant_id = await _resolver(user)
    # Checked here, not in each resolver: the id reaches store namespaces, AAD and logs.
    if not isinstance(tenant_id, str) or not is_valid_tenant_id(tenant_id):
        raise TenantRejectedError(f"tenant id must match {TENANT_ID_PATTERN.pattern}")
    return tenant_id


async def tenant_db_scope(user: User = Depends(get_current_user)) -> AsyncIterator[None]:
    # Must stay async: a sync generator dependency runs in a worker thread,
    # so the contextvar it sets would never reach the endpoint.
    if not settings.tenant.AEGRA_TENANT_RLS_ENABLED:
        yield
        return
    try:
        tenant_id = await resolve_tenant_id(user)
    except TenantRejectedError as e:
        raise HTTPException(403, str(e)) from e
    with tenant_scope(tenant_id):
        yield


tenant_scope_dependency = [Depends(tenant_db_scope)]
