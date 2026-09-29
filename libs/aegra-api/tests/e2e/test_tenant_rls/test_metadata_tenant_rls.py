"""Tenant RLS on Aegra's own thread/runs tables through the real session factory.

Like test_langgraph_tenant_rls: no Aegra server, a throwaway database per test,
skipped when Postgres is unreachable.
"""

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass

import psycopg
import pytest
from psycopg import sql
from sqlalchemy import delete, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from aegra_api.core.db_scope import DbScopeMissingError, system_scope, tenant_scope
from aegra_api.core.orm import Assistant as AssistantORM
from aegra_api.core.orm import AssistantVersion as AssistantVersionORM
from aegra_api.core.orm import Base, build_session_maker
from aegra_api.core.orm import Run as RunORM
from aegra_api.core.orm import Thread as ThreadORM
from aegra_api.core.tenant_rls import GRANT_ONLY_TABLES, METADATA_TENANT_TABLES, enable_tenant_rls
from aegra_api.settings import settings

pytestmark = pytest.mark.e2e

TENANT_ROLE = "aegra_tenant"


@dataclass
class MetaEnv:
    engine: AsyncEngine
    maker: async_sessionmaker[AsyncSession]


def _thread(thread_id: str, tenant_id: str | None, user_id: str = "user-1") -> ThreadORM:
    return ThreadORM(thread_id=thread_id, status="idle", metadata_json={}, user_id=user_id, tenant_id=tenant_id)


@pytest.fixture
async def meta_env(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[MetaEnv]:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_DB_ROLE", TENANT_ROLE)
    admin_dsn = settings.db.database_url_sync
    db_name = f"aegra_rls_meta_{uuid.uuid4().hex[:12]}"
    try:
        admin = await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")

    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    url = make_url(settings.db.database_url).set(database=db_name)
    # One connection: every session reuses it, so leaked tenant state would show up.
    engine = create_async_engine(url, pool_size=1, max_overflow=0, connect_args={"prepared_statement_cache_size": 0})
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        ddl_dsn = psycopg.conninfo.make_conninfo(admin_dsn, dbname=db_name)
        async with await psycopg.AsyncConnection.connect(ddl_dsn, autocommit=True) as ddl_conn:
            await enable_tenant_rls(
                ddl_conn, TENANT_ROLE, tables=METADATA_TENANT_TABLES, grant_only_tables=GRANT_ONLY_TABLES
            )
        yield MetaEnv(engine=engine, maker=build_session_maker(engine))
    finally:
        await engine.dispose()
        await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(db_name)))
        await admin.close()


async def _seed(env: MetaEnv) -> None:
    for tenant_id, thread_id in (("tenant-a", "thread-a"), ("tenant-b", "thread-b")):
        with tenant_scope(tenant_id):
            async with env.maker() as session:
                session.add(_thread(thread_id, tenant_id))
                await session.commit()
                session.add(
                    RunORM(run_id=f"run-{thread_id}", thread_id=thread_id, user_id="user-1", tenant_id=tenant_id)
                )
                await session.commit()


async def test_tenant_sees_only_own_threads_and_runs(meta_env: MetaEnv) -> None:
    await _seed(meta_env)

    with tenant_scope("tenant-a"):
        async with meta_env.maker() as session:
            threads = (await session.scalars(select(ThreadORM.thread_id))).all()
            runs = (await session.scalars(select(RunORM.run_id))).all()

    assert threads == ["thread-a"]
    assert runs == ["run-thread-a"]


async def test_query_without_tenant_filter_cannot_reach_other_tenant(meta_env: MetaEnv) -> None:
    await _seed(meta_env)

    with tenant_scope("tenant-a"):
        async with meta_env.maker() as session:
            other = await session.scalar(select(ThreadORM).where(ThreadORM.thread_id == "thread-b"))
            updated = await session.execute(
                update(ThreadORM).where(ThreadORM.thread_id == "thread-b").values(status="busy")
            )
            deleted = await session.execute(delete(RunORM).where(RunORM.run_id == "run-thread-b"))
            await session.commit()

    assert other is None
    assert updated.rowcount == 0
    assert deleted.rowcount == 0
    with system_scope("test: verify untouched"):
        async with meta_env.maker() as session:
            status = await session.scalar(select(ThreadORM.status).where(ThreadORM.thread_id == "thread-b"))
    assert status == "idle"


async def test_scope_survives_commit_within_one_session(meta_env: MetaEnv) -> None:
    await _seed(meta_env)

    with tenant_scope("tenant-a"):
        async with meta_env.maker() as session:
            first = (await session.scalars(select(ThreadORM.thread_id))).all()
            await session.commit()
            second = (await session.scalars(select(ThreadORM.thread_id))).all()
            role = await session.scalar(text("SELECT current_user"))

    assert first == second == ["thread-a"]
    assert role == TENANT_ROLE


async def test_insert_for_another_tenant_is_rejected(meta_env: MetaEnv) -> None:
    with pytest.raises(DBAPIError, match="row-level security"), tenant_scope("tenant-a"):
        async with meta_env.maker() as session:
            session.add(_thread("thread-x", "tenant-b"))
            await session.commit()


async def test_insert_without_tenant_id_takes_scope_tenant(meta_env: MetaEnv) -> None:
    # Leave the attribute unset: an explicit None is sent as NULL and bypasses the default.
    with tenant_scope("tenant-a"):
        async with meta_env.maker() as session:
            session.add(ThreadORM(thread_id="thread-default", status="idle", metadata_json={}, user_id="user-1"))
            await session.commit()

    with system_scope("test: read back"):
        async with meta_env.maker() as session:
            tenant_id = await session.scalar(select(ThreadORM.tenant_id).where(ThreadORM.thread_id == "thread-default"))
    assert tenant_id == "tenant-a"


async def test_create_on_other_tenants_thread_id_inserts_nothing(meta_env: MetaEnv) -> None:
    # create_thread's ON CONFLICT DO NOTHING path must fall through to its 409,
    # not adopt or reveal the other tenant's row.
    await _seed(meta_env)

    with tenant_scope("tenant-a"):
        async with meta_env.maker() as session:
            stmt = (
                pg_insert(ThreadORM)
                .values(thread_id="thread-b", status="idle", metadata_json={}, user_id="user-1", tenant_id="tenant-a")
                .on_conflict_do_nothing(index_elements=["thread_id"])
                .returning(ThreadORM.thread_id)
            )
            created = (await session.scalars(stmt)).first()
            await session.commit()

    assert created is None


async def test_query_without_scope_fails_closed(meta_env: MetaEnv) -> None:
    with pytest.raises(DbScopeMissingError):
        async with meta_env.maker() as session:
            await session.scalar(select(ThreadORM))


async def test_system_scope_sees_all_but_cannot_write_untagged(meta_env: MetaEnv) -> None:
    await _seed(meta_env)

    with system_scope("test: cross-tenant read"):
        async with meta_env.maker() as session:
            all_threads = set((await session.scalars(select(ThreadORM.thread_id))).all())

    assert all_threads == {"thread-a", "thread-b"}
    with pytest.raises(DBAPIError, match="null value"), system_scope("test: untagged write"):
        async with meta_env.maker() as session:
            session.add(_thread("thread-orphan", None))
            await session.commit()


async def test_tenant_state_does_not_leak_to_next_session(meta_env: MetaEnv) -> None:
    await _seed(meta_env)
    with tenant_scope("tenant-a"):
        async with meta_env.maker() as session:
            await session.scalar(select(ThreadORM))

    with system_scope("test: leak probe"):
        async with meta_env.maker() as session:
            row = (
                await session.execute(
                    text("SELECT current_user AS u, NULLIF(current_setting('aegra.tenant_id', true), '') AS tenant")
                )
            ).one()

    assert row.u != TENANT_ROLE
    assert row.tenant is None


async def test_explicit_null_tenant_falls_back_to_scope_tenant(meta_env: MetaEnv) -> None:
    # A path that passes tenant_id=None must never write an orphan row; the
    # DB default fills in the scope's tenant instead.
    with tenant_scope("tenant-a"):
        async with meta_env.maker() as session:
            session.add(_thread("thread-null", None))
            await session.commit()

    with system_scope("test: read back"):
        async with meta_env.maker() as session:
            tenant_id = await session.scalar(select(ThreadORM.tenant_id).where(ThreadORM.thread_id == "thread-null"))
    assert tenant_id == "tenant-a"


def _assistant(
    assistant_id: str, tenant_id: str | None, *, user_id: str = "user-1", config: dict | None = None
) -> AssistantORM:
    return AssistantORM(
        assistant_id=assistant_id,
        name=assistant_id,
        graph_id="stress_test",
        config=config or {},
        context={},
        user_id=user_id,
        tenant_id=tenant_id,
        metadata_dict={},
    )


async def _seed_assistants(env: MetaEnv) -> None:
    with system_scope("test: seed shared assistant"):
        async with env.maker() as session:
            session.add(_assistant("shared", None, user_id="system"))
            await session.flush()
            session.add(AssistantVersionORM(assistant_id="shared", version=1, graph_id="stress_test"))
            await session.commit()
    for tenant_id in ("tenant-a", "tenant-b"):
        with tenant_scope(tenant_id):
            async with env.maker() as session:
                session.add(_assistant(f"asst-{tenant_id}", tenant_id))
                await session.flush()
                session.add(AssistantVersionORM(assistant_id=f"asst-{tenant_id}", version=1, graph_id="stress_test"))
                await session.commit()


async def test_tenant_sees_own_and_shared_assistants_only(meta_env: MetaEnv) -> None:
    await _seed_assistants(meta_env)

    with tenant_scope("tenant-a"):
        async with meta_env.maker() as session:
            assistants = set((await session.scalars(select(AssistantORM.assistant_id))).all())
            versions = set((await session.scalars(select(AssistantVersionORM.assistant_id))).all())

    assert assistants == {"shared", "asst-tenant-a"}
    assert versions == {"shared", "asst-tenant-a"}


async def test_tenant_cannot_modify_shared_assistant(meta_env: MetaEnv) -> None:
    await _seed_assistants(meta_env)

    with tenant_scope("tenant-a"):
        async with meta_env.maker() as session:
            updated = await session.execute(
                update(AssistantORM).where(AssistantORM.assistant_id == "shared").values(name="hijacked")
            )
            deleted = await session.execute(
                delete(AssistantVersionORM).where(AssistantVersionORM.assistant_id == "shared")
            )
            await session.commit()

    assert updated.rowcount == 0
    assert deleted.rowcount == 0


async def test_tenant_cannot_add_version_to_shared_or_foreign_assistant(meta_env: MetaEnv) -> None:
    await _seed_assistants(meta_env)

    for target in ("shared", "asst-tenant-b"):
        with pytest.raises(DBAPIError, match="row-level security"), tenant_scope("tenant-a"):
            async with meta_env.maker() as session:
                session.add(AssistantVersionORM(assistant_id=target, version=2, graph_id="stress_test"))
                await session.commit()


async def test_same_user_in_two_tenants_can_hold_identical_assistants(meta_env: MetaEnv) -> None:
    for tenant_id in ("tenant-a", "tenant-b"):
        with tenant_scope(tenant_id):
            async with meta_env.maker() as session:
                session.add(_assistant(f"dup-{tenant_id}", tenant_id, user_id="shared-user", config={"k": "v"}))
                await session.commit()

    with system_scope("test: read back"):
        async with meta_env.maker() as session:
            owners = (
                await session.scalars(select(AssistantORM.tenant_id).where(AssistantORM.user_id == "shared-user"))
            ).all()
    assert sorted(owners) == ["tenant-a", "tenant-b"]


async def test_untagged_assistant_must_be_system_owned(meta_env: MetaEnv) -> None:
    with pytest.raises(DBAPIError, match="aegra_tenant_shared_rows_are_system"), system_scope("test: untagged write"):
        async with meta_env.maker() as session:
            session.add(_assistant("orphan", None, user_id="someone"))
            await session.commit()
