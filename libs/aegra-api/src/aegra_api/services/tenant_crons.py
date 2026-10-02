"""Fire crons as their own tenant.

The scheduler claims due crons as the system; each fire is re-scoped here
to the tenant stored on the cron, and skipped when the resolver no longer
serves that tenant. With tenant RLS off every cron fires as before.
"""

import contextlib
from contextlib import AbstractContextManager

import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from aegra_api.core.orm import Cron as CronORM
from aegra_api.core.tenancy.resolver import TenantRejectedError, resolve_tenant_id
from aegra_api.core.tenancy.scope import DbScope, is_valid_tenant_id, tenant_scope
from aegra_api.models.auth import User
from aegra_api.services.cron_service import CronService
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)


def cron_fire_scope(cron: CronORM) -> AbstractContextManager[DbScope | None] | None:
    """Scope to fire *cron* in; None when its stored tenant_id is malformed and the fire must be skipped."""
    # Rows saved before tenant ids were validated would make tenant_scope raise
    # and abort the whole batch; skip just that cron.
    if cron.tenant_id and not is_valid_tenant_id(cron.tenant_id):
        logger.error("Skipping cron with a malformed tenant_id", cron_id=cron.cron_id)
        return None
    # A cron without a tenant stays in the system scope, where _prepare_run refuses to create a run under RLS.
    return tenant_scope(cron.tenant_id) if cron.tenant_id else contextlib.nullcontext()


async def skip_if_tenant_rejected(session: AsyncSession, cron: CronORM, user: User) -> bool:
    """Under RLS, skip this occurrence when the resolver rejects or remaps the cron's tenant."""
    if not settings.tenant.AEGRA_TENANT_RLS_ENABLED or not cron.tenant_id:
        return False
    reason = await _tenant_rejection(user, cron.tenant_id)
    if reason is None:
        return False
    # Skip this occurrence but keep the cron enabled, so a reactivated tenant resumes.
    logger.warning("Skipping cron fire: tenant rejected", cron_id=cron.cron_id, reason=reason)
    await CronService(session).advance_next_run(cron)
    return True


async def _tenant_rejection(user: User, stored_tenant_id: str) -> str | None:
    # The stored tenant scopes the fire; the resolver only confirms it is still served, and as the same tenant.
    try:
        resolved = await resolve_tenant_id(user)
    except TenantRejectedError as e:
        return str(e)
    if resolved != stored_tenant_id:
        return f"resolver now maps this cron's owner to tenant {resolved!r}"
    return None
