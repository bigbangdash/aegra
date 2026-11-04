"""Upgrading a single-tenant install: enable_tenant_rls(assign_existing_to=...) on real data.

The data is written the way a flag-off server writes it (no tenant_id, store
namespaces without the hidden tenant head, semantic-search vectors), then the
enable step either refuses or hands everything to one tenant. No Aegra server;
a throwaway database per test; skipped when Postgres is unreachable.
"""

import uuid
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Any

import psycopg
import pytest
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.store.postgres.aio import AsyncPostgresStore
from psycopg import conninfo, sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from aegra_api.core.orm import Assistant as AssistantORM
from aegra_api.core.orm import Base, build_session_maker
from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.tenancy.pool import TenantScopedConnectionPool
from aegra_api.core.tenancy.rls import UntaggedRowsError, enable_tenant_rls
from aegra_api.core.tenancy.scope import system_scope, tenant_scope
from aegra_api.core.tenancy.store import TenantScopedPostgresStore
from aegra_api.settings import settings

pytestmark = pytest.mark.e2e

TENANT_ROLE = "aegra_tenant"
LEGACY = "legacy"
THREAD = "thread-legacy"
POOL_ARGS: dict[str, Any] = {
    "min_size": 2,
    "max_size": 2,
    "open": False,
    "kwargs": {"autocommit": True, "prepare_threshold": None, "row_factory": dict_row},
}


def _embed(texts: Sequence[str]) -> list[list[float]]:
    return [[1.0, float(len(text))] for text in texts]


INDEX: dict[str, Any] = {"dims": 2, "embed": _embed, "fields": ["text"]}


def _config(thread_id: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}


@dataclass
class LegacyEnv:
    dsn: str
    engine: AsyncEngine


@pytest.fixture
async def legacy_env(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[LegacyEnv]:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_DB_ROLE", TENANT_ROLE)
    admin_dsn = settings.db.database_url_sync
    db_name = f"aegra_rls_legacy_{uuid.uuid4().hex[:12]}"
    try:
        admin = await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")

    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    dsn = conninfo.make_conninfo(admin_dsn, dbname=db_name)
    url = make_url(settings.db.database_url).set(database=db_name)
    engine = create_async_engine(url, pool_size=1, max_overflow=0, connect_args={"prepared_statement_cache_size": 0})
    try:
        await _write_like_a_flag_off_server(dsn, engine)
        yield LegacyEnv(dsn=dsn, engine=engine)
    finally:
        await engine.dispose()
        await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(db_name)))
        await admin.close()


async def _write_like_a_flag_off_server(dsn: str, engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.execute(
            AssistantORM.__table__.insert(),
            [
                {"assistant_id": "shared", "name": "s", "graph_id": "g", "user_id": "system", "config": {}},
                {"assistant_id": "mine", "name": "m", "graph_id": "g", "user_id": "u1", "config": {"x": 1}},
            ],
        )
        await conn.execute(ThreadORM.__table__.insert(), [{"thread_id": THREAD, "status": "idle", "user_id": "u1"}])
        await conn.execute(RunORM.__table__.insert(), [{"run_id": "run-1", "thread_id": THREAD, "user_id": "u1"}])
    pool = AsyncConnectionPool(conninfo=dsn, **POOL_ARGS)
    await pool.open()
    try:
        saver = AsyncPostgresSaver(conn=pool)
        store = AsyncPostgresStore(conn=pool, index=INDEX)
        await saver.setup()
        await store.setup()
        checkpoint = {"v": 1, "id": str(uuid.uuid4()), "ts": "2026-09-30T00:00:00+00:00", "channel_values": {}}
        await saver.aput(_config(THREAD), checkpoint, {}, {})
        await store.aput(("notes",), "k1", {"text": "hello"})
    finally:
        await pool.close()


async def _enable(dsn: str, **kwargs: Any) -> None:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await enable_tenant_rls(conn, TENANT_ROLE, **kwargs)


async def _scalar(dsn: str, query: str) -> Any:
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        row = await (await conn.execute(query)).fetchone()
        return row[0] if row else None


async def test_enable_without_assignment_refuses_and_changes_nothing(legacy_env: LegacyEnv) -> None:
    with pytest.raises(UntaggedRowsError) as exc_info:
        await _enable(legacy_env.dsn)

    counts = exc_info.value.counts
    assert {"thread", "runs", "checkpoints", "store", "store_vectors"} <= set(counts)
    assert counts["assistant"] == 1  # the shared system assistant is not counted
    assert await _scalar(legacy_env.dsn, "SELECT relrowsecurity FROM pg_class WHERE relname = 'thread'") is False
    assert await _scalar(legacy_env.dsn, "SELECT count(*) FROM pg_policies") == 0


async def test_assigned_tenant_sees_all_legacy_data_and_others_see_none(
    legacy_env: LegacyEnv, monkeypatch: pytest.MonkeyPatch
) -> None:
    await _enable(legacy_env.dsn, assign_existing_to=LEGACY)
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)
    maker = build_session_maker(legacy_env.engine)
    pool = TenantScopedConnectionPool(conninfo=legacy_env.dsn, **POOL_ARGS, tenant_role=TENANT_ROLE)
    await pool.open()
    try:
        saver = AsyncPostgresSaver(conn=pool)
        store = TenantScopedPostgresStore(conn=pool, index=INDEX)
        seen: dict[str, dict[str, Any]] = {}
        for tenant in (LEGACY, "someone-else"):
            with tenant_scope(tenant):
                async with maker() as session:
                    threads = (await session.scalars(select(ThreadORM.thread_id))).all()
                    runs = (await session.scalars(select(RunORM.run_id))).all()
                    assistants = sorted((await session.scalars(select(AssistantORM.assistant_id))).all())
                item = await store.aget(("notes",), "k1")
                hits = await store.asearch(("notes",), query="hello")
                seen[tenant] = {
                    "threads": threads,
                    "runs": runs,
                    "assistants": assistants,
                    "checkpoint": await saver.aget_tuple(_config(THREAD)) is not None,
                    "item": item.value if item else None,
                    "search": [h.key for h in hits],
                }
    finally:
        await pool.close()

    assert seen[LEGACY] == {
        "threads": [THREAD],
        "runs": ["run-1"],
        "assistants": ["mine", "shared"],
        "checkpoint": True,
        "item": {"text": "hello"},
        "search": ["k1"],
    }
    assert seen["someone-else"] == {
        "threads": [],
        "runs": [],
        "assistants": ["shared"],
        "checkpoint": False,
        "item": None,
        "search": [],
    }


async def test_assignment_moves_store_rows_under_the_tenant_head_and_keeps_shared_assistants(
    legacy_env: LegacyEnv,
) -> None:
    await _enable(legacy_env.dsn, assign_existing_to=LEGACY)

    with system_scope("test: inspect physical layout"):
        prefixes = await _scalar(legacy_env.dsn, "SELECT string_agg(DISTINCT prefix, ',') FROM store")
        vector_prefixes = await _scalar(legacy_env.dsn, "SELECT string_agg(DISTINCT prefix, ',') FROM store_vectors")
        shared = await _scalar(legacy_env.dsn, "SELECT tenant_id IS NULL FROM assistant WHERE assistant_id = 'shared'")
        untagged = await _scalar(legacy_env.dsn, "SELECT count(*) FROM thread WHERE tenant_id IS NULL")

    assert prefixes == "aegra_tenant.legacy.notes"
    assert vector_prefixes == "aegra_tenant.legacy.notes"
    assert shared is True
    assert untagged == 0


async def test_rerunning_after_assignment_is_a_no_op(legacy_env: LegacyEnv) -> None:
    await _enable(legacy_env.dsn, assign_existing_to=LEGACY)

    await _enable(legacy_env.dsn)

    assert await _scalar(legacy_env.dsn, "SELECT count(*) FROM store") == 1
