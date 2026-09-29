"""Resolve the authenticated user's tenant and scope requests to it."""

from collections.abc import AsyncIterator

from fastapi import Depends, HTTPException

from aegra_api.core.auth_deps import get_current_user
from aegra_api.core.db_scope import tenant_scope
from aegra_api.models.auth import User
from aegra_api.settings import settings


def resolve_tenant_id(user: User) -> str:
    # The tenant comes only from the authenticated identity, never from a
    # client-supplied header, so a caller cannot pick someone else's tenant.
    tenant_id = user.org_id
    if not tenant_id:
        raise HTTPException(403, "Tenant-scoped access requires an org_id on the authenticated user")
    return tenant_id


def tenant_id_for(user: User) -> str | None:
    if not settings.tenant.AEGRA_TENANT_RLS_ENABLED:
        return None
    return resolve_tenant_id(user)


async def tenant_db_scope(user: User = Depends(get_current_user)) -> AsyncIterator[None]:
    # Must stay async: a sync generator dependency runs in a worker thread,
    # so the contextvar it sets would never reach the endpoint.
    tenant_id = tenant_id_for(user)
    if tenant_id is None:
        yield
        return
    with tenant_scope(tenant_id):
        yield


tenant_scope_dependency = [Depends(tenant_db_scope)]
