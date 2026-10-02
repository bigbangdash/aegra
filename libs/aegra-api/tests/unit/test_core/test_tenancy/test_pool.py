from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import psycopg
import pytest
from psycopg_pool import AsyncConnectionPool

from aegra_api.core.tenancy.pool import SYSTEM_SETTING, TENANT_SETTING, TenantScopedConnectionPool
from aegra_api.core.tenancy.scope import DbScopeMissingError, system_scope, tenant_scope


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


async def test_tenant_checkout_sets_role_and_tenant_in_one_statement_inside_transaction(
    pooled_conn: MagicMock,
) -> None:
    pool = _make_pool()

    with tenant_scope("tenant-a"):
        async with pool.connection() as conn:
            assert conn is pooled_conn

    pooled_conn.transaction.assert_called_once()
    (apply_call,) = pooled_conn.execute.await_args_list
    statement, params = apply_call.args
    assert statement.startswith("SELECT set_config('role', %s, true)")
    # Transaction-local clear of the system flag, in case the session still carries one.
    assert params == ("aegra_tenant", TENANT_SETTING, "tenant-a", SYSTEM_SETTING)


async def test_system_checkout_raises_the_system_flag_for_the_session_and_clears_it(pooled_conn: MagicMock) -> None:
    pool = _make_pool()

    with system_scope("unit test"):
        async with pool.connection() as conn:
            assert conn is pooled_conn
            assert pooled_conn.execute.await_count == 1

    # No transaction: setup() runs CREATE INDEX CONCURRENTLY through system checkouts.
    pooled_conn.transaction.assert_not_called()
    raise_call, clear_call = pooled_conn.execute.await_args_list
    assert raise_call.args == ("SELECT set_config(%s, 'on', false)", (SYSTEM_SETTING,))
    assert clear_call.args == ("SELECT set_config(%s, '', false)", (SYSTEM_SETTING,))


async def test_system_flag_is_cleared_when_the_checkout_body_fails(pooled_conn: MagicMock) -> None:
    pool = _make_pool()

    with system_scope("unit test"), pytest.raises(RuntimeError):
        async with pool.connection():
            raise RuntimeError("boom")

    assert pooled_conn.execute.await_args_list[-1].args == ("SELECT set_config(%s, '', false)", (SYSTEM_SETTING,))


async def test_missing_scope_raises_before_checkout(pooled_conn: MagicMock) -> None:
    pool = _make_pool()

    with pytest.raises(DbScopeMissingError):
        async with pool.connection():
            pass

    assert pooled_conn.checkouts == []


def test_rejects_empty_tenant_role() -> None:
    with pytest.raises(ValueError):
        TenantScopedConnectionPool(conninfo="", open=False, tenant_role="")


async def test_role_name_is_sent_as_a_bound_value_not_sql(pooled_conn: MagicMock) -> None:
    hostile = 'evil"; DROP TABLE x; --'
    pool = TenantScopedConnectionPool(conninfo="", open=False, tenant_role=hostile)

    with tenant_scope("tenant-a"):
        async with pool.connection():
            pass

    statement, params = pooled_conn.execute.await_args.args
    assert hostile not in statement
    assert params[0] == hostile


async def test_failing_to_clear_the_system_flag_does_not_fail_the_checkout(pooled_conn: MagicMock) -> None:
    pool = _make_pool()
    pooled_conn.execute.side_effect = [None, psycopg.OperationalError("connection lost")]

    with system_scope("unit test"):
        async with pool.connection() as conn:
            assert conn is pooled_conn

    assert pooled_conn.execute.await_count == 2
