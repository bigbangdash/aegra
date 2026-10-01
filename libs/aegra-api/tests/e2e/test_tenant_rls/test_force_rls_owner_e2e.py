"""FORCE ROW LEVEL SECURITY with a non-superuser table owner as the login role.

The other tenant-RLS suites log in as the superuser, which bypasses RLS even
under FORCE, so they cannot show that the owner is bound. Here the server-shaped
login role (NOSUPERUSER, NOBYPASSRLS) owns every table, as in production.
No Aegra server; a throwaway database and role per test; skipped when
Postgres is unreachable.
"""

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import psycopg
import pytest
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg import conninfo, sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from sqlalchemy import func, select, update
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from aegra_api.core.orm import Base, build_session_maker
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.tenancy.pool import TenantScopedConnectionPool
from aegra_api.core.tenancy.rls import enable_tenant_rls
from aegra_api.core.tenancy.scope import system_scope, tenant_scope
from aegra_api.core.tenancy.store import TenantScopedPostgresStore
from aegra_api.settings import settings

pytestmark = pytest.mark.e2e

TENANT_ROLE = "aegra_tenant"
OWNER_PASSWORD = "owner-e2e-only"


@dataclass
class OwnerEnv:
    owner_dsn: str
    maker: async_sessionmaker[AsyncSession]
    pool: TenantScopedConnectionPool
    saver: AsyncPostgresSaver


def _config(thread_id: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}


@pytest.fixture
async def owner_env(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[OwnerEnv]:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_DB_ROLE", TENANT_ROLE)
    admin_dsn = settings.db.database_url_sync
    suffix = uuid.uuid4().hex[:12]
    db_name, owner = f"aegra_rls_owner_{suffix}", f"aegra_owner_{suffix}"
    try:
        admin = await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")

    await admin.execute(
        sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD {}").format(
            sql.Identifier(owner), sql.Literal(OWNER_PASSWORD)
        )
    )
    # Database owner => owns schema public (pg_database_owner), so it creates every table itself.
    await admin.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(db_name), sql.Identifier(owner)))
    owner_dsn = conninfo.make_conninfo(admin_dsn, dbname=db_name, user=owner, password=OWNER_PASSWORD)
    url = make_url(settings.db.database_url).set(database=db_name, username=owner, password=OWNER_PASSWORD)
    engine = create_async_engine(url, pool_size=1, max_overflow=0, connect_args={"prepared_statement_cache_size": 0})
    pool = TenantScopedConnectionPool(
        conninfo=owner_dsn,
        min_size=2,
        max_size=2,
        open=False,
        kwargs={"autocommit": True, "prepare_threshold": None, "row_factory": dict_row},
        tenant_role=TENANT_ROLE,
    )
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        await pool.open()
        saver = AsyncPostgresSaver(conn=pool)
        with system_scope("test schema setup"):
            await saver.setup()
            await TenantScopedPostgresStore(conn=pool).setup()
        # Operator step, as a superuser, naming the owner as the server login.
        ddl_dsn = conninfo.make_conninfo(admin_dsn, dbname=db_name)
        async with await psycopg.AsyncConnection.connect(ddl_dsn, autocommit=True) as ddl_conn:
            await enable_tenant_rls(ddl_conn, TENANT_ROLE, app_login_role=owner)
        yield OwnerEnv(owner_dsn=owner_dsn, maker=build_session_maker(engine), pool=pool, saver=saver)
    finally:
        await pool.close()
        await engine.dispose()
        await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(db_name)))
        await admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(owner)))
        await admin.close()


async def _seed(env: OwnerEnv) -> None:
    for tenant_id in ("tenant-a", "tenant-b"):
        thread_id = f"thread-{tenant_id}"
        with tenant_scope(tenant_id):
            async with env.maker() as session:
                session.add(ThreadORM(thread_id=thread_id, status="idle", metadata_json={}, user_id="u"))
                await session.commit()
            checkpoint = {"v": 1, "id": str(uuid.uuid4()), "ts": "2026-09-30T00:00:00+00:00", "channel_values": {}}
            await env.saver.aput(_config(thread_id), checkpoint, {}, {})


async def _count_as_owner_without_scope(env: OwnerEnv, table: str, *, setup: str | None = None) -> int:
    # A raw connection: no pool, no session hook, exactly what a forgotten code path would open.
    async with await psycopg.AsyncConnection.connect(env.owner_dsn, autocommit=True) as conn:
        if setup:
            await conn.execute(setup)
        cur = await conn.execute(sql.SQL("SELECT count(*) FROM {}").format(sql.Identifier(table)))
        row = await cur.fetchone()
        return int(row[0]) if row else -1


async def test_owner_without_a_scope_sees_no_rows(owner_env: OwnerEnv) -> None:
    await _seed(owner_env)

    for table in ("thread", "checkpoints", "assistant"):
        assert await _count_as_owner_without_scope(owner_env, table) == 0, table


async def test_system_scope_sees_every_tenant_through_both_paths(owner_env: OwnerEnv) -> None:
    await _seed(owner_env)

    with system_scope("test: cross-tenant read"):
        async with owner_env.maker() as session:
            threads = sorted((await session.scalars(select(ThreadORM.thread_id))).all())
        checkpoints = [c async for c in owner_env.saver.alist(None)]

    assert threads == ["thread-tenant-a", "thread-tenant-b"]
    assert len(checkpoints) == 2


async def test_system_scope_can_write_across_tenants(owner_env: OwnerEnv) -> None:
    await _seed(owner_env)

    # The shape of the lease reaper and TTL sweeper: one statement over every tenant.
    with system_scope("test: cross-tenant write"):
        async with owner_env.maker() as session:
            result = await session.execute(update(ThreadORM).values(status="busy"))
            await session.commit()

    assert result.rowcount == 2


async def test_tenant_scope_still_sees_only_its_own_rows(owner_env: OwnerEnv) -> None:
    await _seed(owner_env)

    with tenant_scope("tenant-a"):
        async with owner_env.maker() as session:
            count = await session.scalar(select(func.count()).select_from(ThreadORM))
        checkpoints = [c async for c in owner_env.saver.alist(None)]

    assert count == 1
    assert [c.config["configurable"]["thread_id"] for c in checkpoints] == ["thread-tenant-a"]


async def test_a_leaked_system_flag_does_not_widen_a_tenant(owner_env: OwnerEnv) -> None:
    await _seed(owner_env)

    # The system policy admits only the login role; the tenant role never matches it.
    setup = (
        "SELECT set_config('aegra.system', 'on', false); "
        f"SET ROLE {TENANT_ROLE}; SELECT set_config('aegra.tenant_id', 'tenant-a', false)"
    )
    assert await _count_as_owner_without_scope(owner_env, "thread", setup=setup) == 1


async def test_system_flag_is_cleared_when_a_pooled_system_checkout_returns(owner_env: OwnerEnv) -> None:
    await _seed(owner_env)

    with system_scope("test: raise then return"):
        async with owner_env.pool.connection() as conn:
            await conn.execute("SELECT 1")
    # Pool of two: hold both through the base class (no scope hooks) and check neither kept the flag.
    async with (
        AsyncConnectionPool.connection(owner_env.pool) as first,
        AsyncConnectionPool.connection(owner_env.pool) as second,
    ):
        for conn in (first, second):
            cur = await conn.execute("SELECT current_setting('aegra.system', true) AS flag")
            row = await cur.fetchone()
            assert row is not None
            assert row["flag"] != "on"


async def test_langgraph_setup_still_runs_after_force(owner_env: OwnerEnv) -> None:
    with system_scope("test: rerun setup"):
        await owner_env.saver.setup()
        await TenantScopedPostgresStore(conn=owner_env.pool).setup()
