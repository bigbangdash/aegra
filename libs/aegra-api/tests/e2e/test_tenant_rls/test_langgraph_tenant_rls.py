"""Tenant RLS on LangGraph checkpoint/store tables against a real PostgreSQL.

Needs no running Aegra server: each test builds a throwaway database from
settings.db.database_url_sync and drops it afterwards. Skips when Postgres
is unreachable.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, TypedDict

import psycopg
import pytest
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.store.postgres.aio import AsyncPostgresStore
from psycopg import conninfo, sql
from psycopg.rows import dict_row

from aegra_api.core.db_scope import DbScopeMissingError, system_scope, tenant_scope
from aegra_api.core.tenant_pool import TenantScopedConnectionPool
from aegra_api.core.tenant_rls import LANGGRAPH_TENANT_TABLES, enable_tenant_rls
from aegra_api.core.tenant_store import TENANT_NAMESPACE_ROOT, TenantScopedPostgresStore
from aegra_api.settings import settings

pytestmark = pytest.mark.e2e

TENANT_ROLE = "aegra_tenant"
POOL_SIZE = 2


class CounterState(TypedDict):
    count: int


def _increment(state: CounterState) -> CounterState:
    return {"count": state["count"] + 1}


def _build_graph(checkpointer: AsyncPostgresSaver) -> Any:
    builder = StateGraph(CounterState)
    builder.add_node("increment", _increment)
    builder.add_edge(START, "increment")
    builder.add_edge("increment", END)
    return builder.compile(checkpointer=checkpointer)


def _config(thread_id: str) -> dict[str, Any]:
    return {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}


@dataclass
class RlsEnv:
    pool: TenantScopedConnectionPool
    saver: AsyncPostgresSaver
    store: TenantScopedPostgresStore


# "aegra_custom" covers installs outside `public` (a search_path in the DSN, as #327 would set):
# the tenant role then needs USAGE on the schema, which PUBLIC only has on `public` by default.
@pytest.fixture(params=["public", "aegra_custom"])
async def rls_env(request: pytest.FixtureRequest) -> AsyncIterator[RlsEnv]:
    schema: str = request.param
    admin_dsn = settings.db.database_url_sync
    db_name = f"aegra_rls_{uuid.uuid4().hex[:12]}"
    try:
        admin = await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")

    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    test_dsn = conninfo.make_conninfo(admin_dsn, dbname=db_name)
    if schema != "public":
        async with await psycopg.AsyncConnection.connect(test_dsn, autocommit=True) as schema_conn:
            await schema_conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        test_dsn = conninfo.make_conninfo(test_dsn, options=f"-c search_path={schema}")
    # Store ops hold two checkouts at once (abatch + _cursor), so two is the floor.
    pool = TenantScopedConnectionPool(
        conninfo=test_dsn,
        min_size=POOL_SIZE,
        max_size=POOL_SIZE,
        open=False,
        kwargs={"autocommit": True, "prepare_threshold": None, "row_factory": dict_row},
        tenant_role=TENANT_ROLE,
    )
    try:
        await pool.open()
        saver = AsyncPostgresSaver(conn=pool)
        store = TenantScopedPostgresStore(conn=pool)
        with system_scope("test schema setup"):
            await saver.setup()
            await store.setup()
        async with await psycopg.AsyncConnection.connect(test_dsn, autocommit=True) as ddl_conn:
            covered = await enable_tenant_rls(
                ddl_conn,
                TENANT_ROLE,
                tables=LANGGRAPH_TENANT_TABLES,
                shared_tables=(),
                child_tables={},
                grant_only_tables=(),
            )
            assert covered == schema
            cur = await ddl_conn.execute(
                "SELECT count(*) FROM pg_tables WHERE schemaname = %s AND tablename = 'checkpoints'", (schema,)
            )
            assert (await cur.fetchone())[0] == 1, "LangGraph tables must live in the schema under test"
        yield RlsEnv(pool=pool, saver=saver, store=store)
    finally:
        await pool.close()
        await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(db_name)))
        await admin.close()


async def test_tenant_reads_own_graph_state(rls_env: RlsEnv) -> None:
    graph = _build_graph(rls_env.saver)

    with tenant_scope("tenant-a"):
        await graph.ainvoke({"count": 0}, _config("thread-a"))
        state = await graph.aget_state(_config("thread-a"))

    assert state.values == {"count": 1}


async def test_other_tenant_cannot_read_checkpoint(rls_env: RlsEnv) -> None:
    graph = _build_graph(rls_env.saver)
    with tenant_scope("tenant-a"):
        await graph.ainvoke({"count": 0}, _config("thread-a"))

    with tenant_scope("tenant-b"):
        checkpoint = await rls_env.saver.aget_tuple(_config("thread-a"))
        history = [c async for c in rls_env.saver.alist(_config("thread-a"))]
        listed_all = [c async for c in rls_env.saver.alist(None)]

    assert checkpoint is None
    assert history == []
    assert listed_all == []


async def test_other_tenant_cannot_delete_checkpoint(rls_env: RlsEnv) -> None:
    graph = _build_graph(rls_env.saver)
    with tenant_scope("tenant-a"):
        await graph.ainvoke({"count": 0}, _config("thread-a"))

    with tenant_scope("tenant-b"):
        await rls_env.saver.adelete_thread("thread-a")
    with tenant_scope("tenant-a"):
        state = await graph.aget_state(_config("thread-a"))

    assert state.values == {"count": 1}


async def test_system_scope_sees_every_tenant(rls_env: RlsEnv) -> None:
    graph = _build_graph(rls_env.saver)
    with tenant_scope("tenant-a"):
        await graph.ainvoke({"count": 0}, _config("thread-a"))
    with tenant_scope("tenant-b"):
        await graph.ainvoke({"count": 10}, _config("thread-b"))

    with system_scope("test: cross-tenant maintenance"):
        thread_ids = {c.config["configurable"]["thread_id"] async for c in rls_env.saver.alist(None)}

    assert thread_ids == {"thread-a", "thread-b"}


async def test_checkout_without_scope_fails_closed(rls_env: RlsEnv) -> None:
    with pytest.raises(DbScopeMissingError):
        await rls_env.saver.aget_tuple(_config("thread-a"))


async def _probe_every_pooled_connection(pool: TenantScopedConnectionPool) -> list[dict[str, Any]]:
    # Hold every pooled connection at once so each one gets inspected.
    rows: list[dict[str, Any]] = []
    with system_scope("test: leak probe"):
        async with pool.connection() as first, pool.connection() as second:
            for conn in (first, second):
                cur = await conn.execute(
                    "SELECT current_user AS u, NULLIF(current_setting('aegra.tenant_id', true), '') AS t"
                )
                row = await cur.fetchone()
                assert row is not None
                rows.append(row)
    return rows


async def test_tenant_context_does_not_leak_to_next_checkout(rls_env: RlsEnv) -> None:
    with tenant_scope("tenant-a"):
        async with rls_env.pool.connection() as a1, rls_env.pool.connection() as a2:
            inside = [await (await c.execute("SELECT current_user AS u")).fetchone() for c in (a1, a2)]

    after = await _probe_every_pooled_connection(rls_env.pool)

    assert [row["u"] for row in inside if row] == [TENANT_ROLE, TENANT_ROLE]
    assert all(row["u"] != TENANT_ROLE and row["t"] is None for row in after)


async def test_failed_tenant_checkout_rolls_back_and_resets(rls_env: RlsEnv) -> None:
    with pytest.raises(psycopg.errors.DivisionByZero), tenant_scope("tenant-a"):
        async with rls_env.pool.connection() as conn:
            await conn.execute("SELECT 1 / 0")

    after = await _probe_every_pooled_connection(rls_env.pool)

    assert all(row["u"] != TENANT_ROLE and row["t"] is None for row in after)


async def test_store_items_are_isolated_per_tenant(rls_env: RlsEnv) -> None:
    with tenant_scope("tenant-a"):
        await rls_env.store.aput(("memories", "tenant-a"), "pref", {"lang": "ja"})

    with tenant_scope("tenant-b"):
        item = await rls_env.store.aget(("memories", "tenant-a"), "pref")
        found = await rls_env.store.asearch(("memories",))
        namespaces = await rls_env.store.alist_namespaces()

    assert item is None
    assert found == []
    assert namespaces == []


async def test_same_namespace_and_key_are_independent_per_tenant(rls_env: RlsEnv) -> None:
    # Without the tenant namespace prefix these collide on the global (prefix, key)
    # primary key and RLS rejects tenant B's write, leaking that the key exists.
    with tenant_scope("tenant-a"):
        await rls_env.store.aput(("memories",), "pref", {"owner": "a"})
    with tenant_scope("tenant-b"):
        await rls_env.store.aput(("memories",), "pref", {"owner": "b"})

    with tenant_scope("tenant-a"):
        item_a = await rls_env.store.aget(("memories",), "pref")
    with tenant_scope("tenant-b"):
        item_b = await rls_env.store.aget(("memories",), "pref")

    assert item_a is not None and item_a.value == {"owner": "a"} and item_a.namespace == ("memories",)
    assert item_b is not None and item_b.value == {"owner": "b"} and item_b.namespace == ("memories",)


async def test_store_rows_are_physically_prefixed_and_tagged_with_tenant(rls_env: RlsEnv) -> None:
    with tenant_scope("tenant-a"):
        await rls_env.store.aput(("memories",), "pref", {"v": 1})

    with system_scope("test: inspect physical rows"):
        async with rls_env.pool.connection() as conn:
            cur = await conn.execute("SELECT prefix, tenant_id FROM store")
            rows = await cur.fetchall()

    assert rows == [{"prefix": f"{TENANT_NAMESPACE_ROOT}.tenant-a.memories", "tenant_id": "tenant-a"}]


async def test_list_namespaces_hides_the_tenant_prefix_and_honors_max_depth(rls_env: RlsEnv) -> None:
    with tenant_scope("tenant-a"):
        await rls_env.store.aput(("notes", "x", "deep"), "k", {"v": 1})
        await rls_env.store.aput(("memories",), "k", {"v": 1})
    with tenant_scope("tenant-b"):
        await rls_env.store.aput(("other",), "k", {"v": 1})

    with tenant_scope("tenant-a"):
        top = await rls_env.store.alist_namespaces(max_depth=1)
        under_notes = await rls_env.store.alist_namespaces(prefix=("notes",))

    assert sorted(top) == [("memories",), ("notes",)]
    assert under_notes == [("notes", "x", "deep")]


async def test_sync_store_call_from_graph_node_keeps_tenant(rls_env: RlsEnv) -> None:
    store = rls_env.store

    def sync_node() -> dict[str, Any]:
        store.put(("memories",), "sync", {"from": "thread"})
        item = store.get(("memories",), "sync")
        return item.value if item else {}

    with tenant_scope("tenant-a"):
        value = await asyncio.to_thread(sync_node)
    with tenant_scope("tenant-b"):
        leaked = await store.aget(("memories",), "sync")

    assert value == {"from": "thread"}
    assert leaked is None


async def test_system_scope_cannot_write_untagged_rows(rls_env: RlsEnv) -> None:
    with pytest.raises(psycopg.errors.NotNullViolation), system_scope("test: untagged write"):
        await rls_env.store.aput(("memories",), "orphan", {"v": 1})


async def test_concurrent_tenants_store_ops_do_not_mix(rls_env: RlsEnv) -> None:
    async def put_and_read(tenant_id: str) -> list[str]:
        with tenant_scope(tenant_id):
            await rls_env.store.aput(("notes", tenant_id), "k", {"owner": tenant_id})
            await asyncio.sleep(0)
            return [item.value["owner"] for item in await rls_env.store.asearch(("notes",))]

    seen_a, seen_b = await asyncio.gather(put_and_read("tenant-a"), put_and_read("tenant-b"))

    assert seen_a == ["tenant-a"]
    assert seen_b == ["tenant-b"]


async def test_stock_batched_store_fails_closed_under_rls(rls_env: RlsEnv) -> None:
    # The stock store runs ops in a background task that never sees the caller's
    # scope; it must error out rather than run unscoped or as another tenant.
    stock = AsyncPostgresStore(conn=rls_env.pool)

    with pytest.raises(DbScopeMissingError), tenant_scope("tenant-a"):
        await stock.aput(("memories",), "k", {"v": 1})
