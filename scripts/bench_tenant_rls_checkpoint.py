"""Benchmark LangGraph checkpoint/store latency with tenant RLS off vs on (FORCE).

Builds two throwaway databases on the PostgreSQL named by POSTGRES_* (the login
must be a superuser: it creates roles and databases). Each is owned by a
non-superuser role, like a real server login, and filled with the same
background data: TENANTS x PER_TENANT checkpoints spread over threads. The "on"
database gets `enable_tenant_rls` and is read through TenantScopedConnectionPool
and TenantScopedPostgresStore, as the server does.

    POSTGRES_HOST=localhost POSTGRES_PORT=5434 POSTGRES_USER=user POSTGRES_PASSWORD=password \\
      POSTGRES_DB=aegra uv run --package aegra-api python scripts/bench_tenant_rls_checkpoint.py

Prints p50/p95 per operation and EXPLAIN plans for the tenant-scoped queries.
--variants also measures candidate fixes on the "on" side, without changing the server:
  composite-index  an index on checkpoints (tenant_id, thread_id, checkpoint_ns, checkpoint_id)
"""

import argparse
import asyncio
import statistics
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import psycopg
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.store.postgres.aio import AsyncPostgresStore
from psycopg import conninfo, sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from aegra_api.core.db_scope import system_scope, tenant_scope
from aegra_api.core.tenant_pool import TenantScopedConnectionPool
from aegra_api.core.tenant_rls import enable_tenant_rls
from aegra_api.core.tenant_store import TenantScopedPostgresStore
from aegra_api.settings import settings

TENANT_ROLE = "aegra_tenant"
OWNER_PASSWORD = "bench-only"
BENCH_TENANT = "tenant-1"
BENCH_THREAD = "bench-thread"
THREADS_PER_TENANT = 20
POOL_SIZE = 4
VARIANTS = ("baseline", "composite-index")


@dataclass
class BenchDb:
    name: str
    owner: str
    rls: bool
    pool: AsyncConnectionPool
    saver: AsyncPostgresSaver
    store: AsyncPostgresStore


def _config(thread_id: str, checkpoint_id: str | None = None) -> dict[str, Any]:
    configurable: dict[str, Any] = {"thread_id": thread_id, "checkpoint_ns": ""}
    if checkpoint_id:
        configurable["checkpoint_id"] = checkpoint_id
    return {"configurable": configurable}


def _checkpoint() -> dict[str, Any]:
    return {"v": 1, "id": str(uuid.uuid4()), "ts": "2026-09-30T00:00:00+00:00", "channel_values": {"count": 1}}


@asynccontextmanager
async def _bench_db(
    admin: psycopg.AsyncConnection,
    *,
    rls: bool,
    variant: str,
    tenants: int,
    per_tenant: int,
    thread_checkpoints: int,
) -> AsyncIterator[BenchDb]:
    suffix = uuid.uuid4().hex[:10]
    name, owner = f"aegra_bench_{'on' if rls else 'off'}_{suffix}", f"aegra_bench_owner_{suffix}"
    await admin.execute(
        sql.SQL("CREATE ROLE {} LOGIN NOSUPERUSER NOBYPASSRLS PASSWORD {}").format(
            sql.Identifier(owner), sql.Literal(OWNER_PASSWORD)
        )
    )
    await admin.execute(sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(name), sql.Identifier(owner)))
    owner_dsn = conninfo.make_conninfo(settings.db.database_url_sync, dbname=name, user=owner, password=OWNER_PASSWORD)
    pool_args: dict[str, Any] = {
        "conninfo": owner_dsn,
        "min_size": POOL_SIZE,
        "max_size": POOL_SIZE,
        "open": False,
        "kwargs": {"autocommit": True, "prepare_threshold": None, "row_factory": dict_row},
    }
    pool = TenantScopedConnectionPool(**pool_args, tenant_role=TENANT_ROLE) if rls else AsyncConnectionPool(**pool_args)
    try:
        await pool.open()
        saver = AsyncPostgresSaver(conn=pool)
        store = TenantScopedPostgresStore(conn=pool) if rls else AsyncPostgresStore(conn=pool)
        with system_scope("bench: schema setup"):
            await saver.setup()
            await store.setup()
        superuser_dsn = conninfo.make_conninfo(settings.db.database_url_sync, dbname=name)
        async with await psycopg.AsyncConnection.connect(superuser_dsn, autocommit=True) as ddl:
            if rls:
                await enable_tenant_rls(
                    ddl,
                    TENANT_ROLE,
                    app_login_role=owner,
                    shared_tables=(),
                    child_tables={},
                    grant_only_tables=(),
                    tables=("checkpoints", "checkpoint_blobs", "checkpoint_writes", "store"),
                )
                if variant == "composite-index":
                    await ddl.execute(
                        "CREATE INDEX idx_checkpoints_tenant_thread "
                        "ON checkpoints (tenant_id, thread_id, checkpoint_ns, checkpoint_id)"
                    )
        db = BenchDb(name=name, owner=owner, rls=rls, pool=pool, saver=saver, store=store)
        await _seed(db, superuser_dsn, tenants=tenants, per_tenant=per_tenant, thread_checkpoints=thread_checkpoints)
        yield db
    finally:
        await pool.close()
        await admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(owner)))


def _scope(db: BenchDb) -> Any:
    return tenant_scope(BENCH_TENANT) if db.rls else system_scope("bench: rls off")


async def _seed(db: BenchDb, superuser_dsn: str, *, tenants: int, per_tenant: int, thread_checkpoints: int) -> None:
    # The bench thread gets real checkpoints through the saver; the background rows are a copy of one.
    with _scope(db):
        config = _config(BENCH_THREAD)
        for _ in range(thread_checkpoints):
            config = await db.saver.aput(config, _checkpoint(), {"source": "bench"}, {})
    tenant_col = sql.SQL(", tenant_id") if db.rls else sql.SQL("")
    tenant_val = sql.SQL(", 'tenant-' || t") if db.rls else sql.SQL("")
    fill = sql.SQL(
        "INSERT INTO checkpoints (thread_id, checkpoint_ns, checkpoint_id, type, checkpoint, metadata{col}) "
        "SELECT 't' || t || '-th' || (g % {threads}), '', lpad(g::text, 12, '0'), s.type, s.checkpoint, s.metadata{val} "
        "FROM generate_series(1, {tenants}) t, generate_series(1, {per_tenant}) g, "
        "(SELECT type, checkpoint, metadata FROM checkpoints LIMIT 1) s"
    ).format(
        col=tenant_col,
        val=tenant_val,
        threads=sql.Literal(THREADS_PER_TENANT),
        tenants=sql.Literal(tenants),
        per_tenant=sql.Literal(per_tenant),
    )
    async with await psycopg.AsyncConnection.connect(superuser_dsn, autocommit=True) as conn:
        await conn.execute(fill)
        await conn.execute("ANALYZE")


async def _time(op: Callable[[int], Awaitable[Any]], iterations: int, warmup: int = 20) -> list[float]:
    for i in range(warmup):
        await op(-1 - i)
    samples: list[float] = []
    for i in range(iterations):
        start = time.perf_counter()
        await op(i)
        samples.append((time.perf_counter() - start) * 1000)
    return samples


async def _measure(db: BenchDb, iterations: int) -> dict[str, list[float]]:
    results: dict[str, list[float]] = {}
    with _scope(db):
        latest = await db.saver.aget_tuple(_config(BENCH_THREAD))
        assert latest is not None, "bench thread has no checkpoint"
        parent = latest.config

        async def put(_i: int) -> None:
            await db.saver.aput(parent, _checkpoint(), {"source": "bench"}, {})

        async def get_latest(_i: int) -> None:
            assert await db.saver.aget_tuple(_config(BENCH_THREAD)) is not None

        async def list_thread(_i: int) -> None:
            assert len([c async for c in db.saver.alist(_config(BENCH_THREAD), limit=10)]) == 10

        async def list_all(_i: int) -> None:
            # No thread filter: only the tenant boundary narrows this one.
            assert len([c async for c in db.saver.alist(None, limit=10)]) == 10

        async def store_put(i: int) -> None:
            await db.store.aput(("bench",), f"key-{i}", {"i": i})

        async def store_get(i: int) -> None:
            await db.store.aget(("bench",), f"key-{max(i, 0) % 50}")

        results["checkpoint aput"] = await _time(put, iterations)
        results["checkpoint aget_tuple (latest)"] = await _time(get_latest, iterations)
        results["checkpoint alist (thread, limit 10)"] = await _time(list_thread, iterations)
        results["checkpoint alist (no thread, limit 10)"] = await _time(list_all, max(iterations // 5, 20))
        results["store aput"] = await _time(store_put, iterations)
        results["store aget"] = await _time(store_get, iterations)
    return results


async def _explain(db: BenchDb) -> dict[str, str]:
    queries = {
        "latest checkpoint of a thread": (
            "SELECT checkpoint FROM checkpoints WHERE thread_id = %s AND checkpoint_ns = '' "
            "ORDER BY checkpoint_id DESC LIMIT 1",
            (BENCH_THREAD,),
        ),
        "tenant-wide listing": ("SELECT checkpoint_id FROM checkpoints ORDER BY checkpoint_id DESC LIMIT 10", ()),
    }
    plans: dict[str, str] = {}
    with _scope(db):
        async with db.pool.connection() as conn:
            for label, (query, params) in queries.items():
                cur = await conn.execute(f"EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF) {query}", params)
                plans[label] = "\n".join(row["QUERY PLAN"] for row in await cur.fetchall())
    return plans


def _pct(samples: list[float], q: float) -> float:
    return statistics.quantiles(samples, n=100)[int(q) - 1]


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--tenants", type=int, default=100)
    parser.add_argument("--per-tenant", type=int, default=2000)
    parser.add_argument("--thread-checkpoints", type=int, default=100, help="length of the measured thread")
    parser.add_argument("--iterations", type=int, default=300)
    parser.add_argument("--variants", default="baseline", help=f"comma-separated subset of {','.join(VARIANTS)}")
    args = parser.parse_args()
    variants = [v for v in args.variants.split(",") if v]
    unknown = set(variants) - set(VARIANTS)
    if unknown:
        parser.error(f"unknown variants: {sorted(unknown)}")

    sizes = {"tenants": args.tenants, "per_tenant": args.per_tenant, "thread_checkpoints": args.thread_checkpoints}
    runs: dict[str, dict[str, list[float]]] = {}
    plans: dict[str, dict[str, str]] = {}
    async with await psycopg.AsyncConnection.connect(settings.db.database_url_sync, autocommit=True) as admin:
        async with _bench_db(admin, rls=False, variant="baseline", **sizes) as db:
            runs["off"] = await _measure(db, args.iterations)
        for variant in variants:
            async with _bench_db(admin, rls=True, variant=variant, **sizes) as db:
                runs[f"on:{variant}"] = await _measure(db, args.iterations)
                plans[variant] = await _explain(db)

    print(
        f"\n{args.tenants} tenants x {args.per_tenant} checkpoints = {args.tenants * args.per_tenant} background "
        f"rows, measured thread {args.thread_checkpoints}+ checkpoints, {args.iterations} iterations, "
        f"pool {POOL_SIZE}, non-superuser owner login (ms, p50 / p95)\n"
    )
    labels = list(runs)
    print("| operation | " + " | ".join(labels) + " |")
    print("|---|" + "---|" * len(labels))
    for op in runs["off"]:
        off_p95 = _pct(runs["off"][op], 95)
        cells = []
        for label in labels:
            samples = runs[label][op]
            ratio = "" if label == "off" else f" ({_pct(samples, 95) / off_p95:.2f}x)"
            cells.append(f"{_pct(samples, 50):.2f} / {_pct(samples, 95):.2f}{ratio}")
        print(f"| {op} | " + " | ".join(cells) + " |")
    for variant, variant_plans in plans.items():
        for label, plan in variant_plans.items():
            print(f"\nEXPLAIN on:{variant} (tenant scope) — {label}:\n{plan}")


if __name__ == "__main__":
    asyncio.run(main())
