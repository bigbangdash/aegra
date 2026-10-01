"""LangGraph connection pool that applies tenant RLS context per checkout.

The LangGraph pool runs with autocommit, so there is no transaction to hang
SET LOCAL on. Tenant checkouts therefore run inside one explicit transaction
for their whole lifetime; commit/rollback clears the role and tenant setting
before the connection returns to the pool.

System checkouts raise SYSTEM_SETTING for the session instead (setup() runs
CREATE INDEX CONCURRENTLY, which cannot sit in a transaction) and clear it on
return. Under FORCE ROW LEVEL SECURITY the login role sees rows only through the
system policy keyed on that setting, which is scoped to the login role: a value
left behind never widens a tenant checkout, which runs as the tenant role.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import psycopg
import structlog
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from aegra_api.core.tenancy.scope import current_db_scope

logger = structlog.get_logger(__name__)

TENANT_SETTING = "aegra.tenant_id"
SYSTEM_SETTING = "aegra.system"
# Role, tenant and a cleared system flag in one round trip; set_config('role', ...) is SET LOCAL ROLE.
# The role is a value, not SQL, so it needs no identifier quoting.
_APPLY_TENANT = "SELECT set_config('role', %s, true), set_config(%s, %s, true), set_config(%s, '', true)"


class TenantScopedConnectionPool(AsyncConnectionPool[AsyncConnection[Any]]):
    def __init__(self, *args: Any, tenant_role: str, **kwargs: Any) -> None:
        if not tenant_role:
            raise ValueError("tenant_role must be a non-empty role name")
        self._tenant_role = tenant_role
        super().__init__(*args, **kwargs)

    @asynccontextmanager
    async def connection(self, timeout: float | None = None) -> AsyncIterator[AsyncConnection[Any]]:
        # Resolve before checkout so a missing scope never touches the pool.
        scope = current_db_scope()
        async with super().connection(timeout) as conn:
            if scope.is_system:
                await conn.execute("SELECT set_config(%s, 'on', false)", (SYSTEM_SETTING,))
                try:
                    yield conn
                finally:
                    try:
                        await conn.execute("SELECT set_config(%s, '', false)", (SYSTEM_SETTING,))
                    except psycopg.Error as e:
                        # A leftover flag is harmless (see module doc); a broken connection is discarded by the pool.
                        logger.warning("Could not clear the system flag on a pooled connection", error=str(e))
                return
            async with conn.transaction():
                await conn.execute(_APPLY_TENANT, (self._tenant_role, TENANT_SETTING, scope.tenant_id, SYSTEM_SETTING))
                yield conn
