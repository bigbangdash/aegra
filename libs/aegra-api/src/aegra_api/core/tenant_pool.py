"""LangGraph connection pool that applies tenant RLS context per checkout.

The LangGraph pool runs with autocommit, so there is no transaction to hang
SET LOCAL on. Tenant checkouts therefore run inside one explicit transaction
for their whole lifetime; commit/rollback clears the role and tenant setting
before the connection returns to the pool.
"""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from psycopg import AsyncConnection, sql
from psycopg_pool import AsyncConnectionPool

from aegra_api.core.db_scope import current_db_scope

TENANT_SETTING = "aegra.tenant_id"


class TenantScopedConnectionPool(AsyncConnectionPool[AsyncConnection[Any]]):
    def __init__(self, *args: Any, tenant_role: str, **kwargs: Any) -> None:
        if not tenant_role:
            raise ValueError("tenant_role must be a non-empty role name")
        self._set_tenant_role = sql.SQL("SET LOCAL ROLE {}").format(sql.Identifier(tenant_role))
        super().__init__(*args, **kwargs)

    @asynccontextmanager
    async def connection(self, timeout: float | None = None) -> AsyncIterator[AsyncConnection[Any]]:
        # Resolve before checkout so a missing scope never touches the pool.
        scope = current_db_scope()
        async with super().connection(timeout) as conn:
            if scope.is_system:
                yield conn
                return
            async with conn.transaction():
                await conn.execute(self._set_tenant_role)
                await conn.execute("SELECT set_config(%s, %s, true)", (TENANT_SETTING, scope.tenant_id))
                yield conn
