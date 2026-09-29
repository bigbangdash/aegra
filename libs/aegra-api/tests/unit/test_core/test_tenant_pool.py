from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from psycopg_pool import AsyncConnectionPool

from aegra_api.core.db_scope import DbScopeMissingError, system_scope, tenant_scope
from aegra_api.core.tenant_pool import TENANT_SETTING, TenantScopedConnectionPool


def _make_conn() -> MagicMock:
    conn = MagicMock()
    conn.execute = AsyncMock()
    conn.transaction = MagicMock()
    conn.transaction.return_value.__aenter__ = AsyncMock()
    conn.transaction.return_value.__aexit__ = AsyncMock(return_value=False)
    return conn


@pytest.fixture
def pooled_conn(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    conn = _make_conn()
    checkouts: list[int] = []

    @asynccontextmanager
    async def fake_connection(self: Any, timeout: float | None = None) -> AsyncIterator[MagicMock]:
        checkouts.append(1)
        yield conn

    monkeypatch.setattr(AsyncConnectionPool, "connection", fake_connection)
    conn.checkouts = checkouts
    return conn


def _make_pool() -> TenantScopedConnectionPool:
    return TenantScopedConnectionPool(conninfo="", open=False, tenant_role="aegra_tenant")


async def test_tenant_checkout_sets_role_and_tenant_inside_transaction(pooled_conn: MagicMock) -> None:
    pool = _make_pool()

    with tenant_scope("tenant-a"):
        async with pool.connection() as conn:
            assert conn is pooled_conn

    pooled_conn.transaction.assert_called_once()
    role_stmt, tenant_call = pooled_conn.execute.await_args_list
    assert "aegra_tenant" in role_stmt.args[0].as_string(None)
    assert tenant_call.args[1] == (TENANT_SETTING, "tenant-a")


async def test_system_checkout_leaves_connection_untouched(pooled_conn: MagicMock) -> None:
    pool = _make_pool()

    with system_scope("unit test"):
        async with pool.connection() as conn:
            assert conn is pooled_conn

    pooled_conn.transaction.assert_not_called()
    pooled_conn.execute.assert_not_awaited()


async def test_missing_scope_raises_before_checkout(pooled_conn: MagicMock) -> None:
    pool = _make_pool()

    with pytest.raises(DbScopeMissingError):
        async with pool.connection():
            pass

    assert pooled_conn.checkouts == []


def test_rejects_empty_tenant_role() -> None:
    with pytest.raises(ValueError):
        TenantScopedConnectionPool(conninfo="", open=False, tenant_role="")


def test_role_name_is_quoted_as_identifier() -> None:
    pool = TenantScopedConnectionPool(conninfo="", open=False, tenant_role='evil"; DROP TABLE x; --')

    rendered = pool._set_tenant_role.as_string(None)

    assert rendered == 'SET LOCAL ROLE "evil""; DROP TABLE x; --"'
