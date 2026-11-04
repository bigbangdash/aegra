from collections.abc import Callable
from unittest.mock import AsyncMock, MagicMock

import pytest
from psycopg import sql

from aegra_api.core.tenancy.rls import (
    CHECKPOINT_TABLES,
    CHILD_TENANT_TABLES,
    GRANT_ONLY_TABLES,
    SHARED_TENANT_TABLES,
    TENANT_TABLES,
    UntaggedRowsError,
    build_child_table_statements,
    build_shared_table_statements,
    build_table_statements,
    enable_tenant_rls,
    isolated_tables_for_settings,
)
from aegra_api.settings import settings


def _render(table: str, role: str = "aegra_tenant") -> list[str]:
    return [stmt.as_string(None) for stmt in build_table_statements(table, role, login_role="aegra_app")]


def test_policy_treats_empty_tenant_setting_as_no_tenant() -> None:
    policy = next(s for s in _render("checkpoints") if s.startswith("CREATE POLICY"))

    assert policy.count("NULLIF(current_setting('aegra.tenant_id', true), '')") == 2


def test_column_default_uses_nullif_tenant_setting() -> None:
    default = next(s for s in _render("store") if "SET DEFAULT" in s)

    assert "NULLIF(current_setting('aegra.tenant_id', true), '')" in default


def test_rls_is_enabled_after_policy_is_created() -> None:
    statements = _render("checkpoints")
    policy_idx = next(i for i, s in enumerate(statements) if s.startswith("CREATE POLICY"))
    enable_idx = next(i for i, s in enumerate(statements) if "ENABLE ROW LEVEL SECURITY" in s)

    assert policy_idx < enable_idx


def test_identifiers_are_quoted() -> None:
    statements = _render("store", role='r"; DROP ROLE x; --')

    grant = next(s for s in statements if s.startswith("GRANT"))
    assert '"r""; DROP ROLE x; --"' in grant


def test_covers_metadata_and_langgraph_tables() -> None:
    assert set(TENANT_TABLES) == {
        "thread",
        "runs",
        "crons",
        "checkpoints",
        "checkpoint_blobs",
        "checkpoint_writes",
        "store",
    }


def test_each_table_gets_exactly_one_treatment() -> None:
    groups = [set(TENANT_TABLES), set(SHARED_TENANT_TABLES), set(CHILD_TENANT_TABLES), set(GRANT_ONLY_TABLES)]

    assert sum(len(g) for g in groups) == len(set().union(*groups))
    assert {"assistant", "assistant_versions"} <= set().union(*groups[1:3])


def _render_shared() -> list[str]:
    return [
        stmt.as_string(None)
        for stmt in build_shared_table_statements("assistant", "aegra_tenant", login_role="aegra_app")
    ]


def test_shared_rows_are_readable_but_not_writable() -> None:
    statements = _render_shared()
    read = next(s for s in statements if "FOR SELECT" in s)
    writes = [s for s in statements if any(f"FOR {op}" in s for op in ("INSERT", "UPDATE", "DELETE"))]

    assert "tenant_id IS NULL AND user_id = 'system'" in read
    assert len(writes) == 3
    assert all("tenant_id IS NULL" not in s for s in writes)


def test_shared_table_keeps_nullable_tenant_but_restricts_null_to_system() -> None:
    statements = _render_shared()

    assert not any("SET NOT NULL" in s for s in statements)
    assert any("CHECK (tenant_id IS NOT NULL OR user_id = 'system')" in s for s in statements)


def test_child_rows_follow_parent_visibility_and_ownership() -> None:
    statements = [
        stmt.as_string(None)
        for stmt in build_child_table_statements(
            "assistant_versions", "assistant", "assistant_id", "r", login_role="app"
        )
    ]
    read = next(s for s in statements if "FOR SELECT" in s)
    insert = next(s for s in statements if "FOR INSERT" in s)

    assert '"assistant"."assistant_id" = "assistant_versions"."assistant_id"' in read
    assert "tenant_id" not in read
    assert '"assistant".tenant_id = NULLIF' in insert


async def test_enable_rejects_non_autocommit_connection() -> None:
    conn = MagicMock(autocommit=False)

    with pytest.raises(ValueError):
        await enable_tenant_rls(conn, "aegra_tenant")


async def test_enable_creates_role_only_when_missing_and_grants_app_login() -> None:
    conn = _conn_where_exists(set())

    await enable_tenant_rls(
        conn,
        "aegra_tenant",
        app_login_role="aegra_app",
        tables=("store",),
        shared_tables=(),
        child_tables={},
        grant_only_tables=("thread_ttl",),
    )

    rendered = [
        c.args[0] if isinstance(c.args[0], str) else c.args[0].as_string(None) for c in conn.execute.await_args_list
    ]
    assert not any(s.startswith("CREATE ROLE") for s in rendered)
    assert 'GRANT "aegra_tenant" TO "aegra_app"' in rendered
    assert 'GRANT SELECT, INSERT, UPDATE, DELETE ON "public"."thread_ttl" TO "aegra_tenant"' in rendered
    assert not any("thread_ttl" in s and "ROW LEVEL SECURITY" in s for s in rendered)


def _rendered(conn: MagicMock) -> list[str]:
    return [
        c.args[0] if isinstance(c.args[0], str) else c.args[0].as_string(None) for c in conn.execute.await_args_list
    ]


def _conn_where_exists(
    existing: set[str], *, current_schema: str = "public", untagged: dict[str, int] | None = None
) -> MagicMock:
    untagged = untagged or {}

    async def execute(statement: str | sql.Composable, params: tuple[str, ...] | None = None) -> MagicMock:
        cursor = MagicMock()
        rendered = statement if isinstance(statement, str) else statement.as_string(None)
        if rendered.startswith("SELECT count(*) FROM"):
            table = rendered.split("FROM ")[1].split(" WHERE")[0].split(".")[-1].strip('"')
            cursor.fetchone = AsyncMock(return_value=(untagged.get(table, 0),))
        elif isinstance(statement, str) and "pg_tables" in statement and params:
            cursor.fetchone = AsyncMock(return_value=(params[1] in existing,))
        elif statement == "SELECT current_schema()":
            cursor.fetchone = AsyncMock(return_value=(current_schema,))
        elif isinstance(statement, str) and "table_name = 'store'" in statement:
            cursor.fetchall = AsyncMock(return_value=[("key",), ("value",)])
        elif statement == "SELECT current_user":
            cursor.fetchone = AsyncMock(return_value=("aegra_owner",))
        else:
            cursor.fetchone = AsyncMock(return_value=(1,))
        return cursor

    conn = MagicMock(autocommit=True)
    conn.execute = AsyncMock(side_effect=execute)
    conn.transaction.return_value.__aenter__ = AsyncMock()
    conn.transaction.return_value.__aexit__ = AsyncMock(return_value=False)
    return conn


async def test_enable_isolates_store_vectors_when_semantic_search_created_it() -> None:
    conn = _conn_where_exists({"store_vectors"})

    await enable_tenant_rls(
        conn, "aegra_tenant", tables=("store",), shared_tables=(), child_tables={}, grant_only_tables=()
    )

    assert 'ALTER TABLE "public"."store_vectors" ENABLE ROW LEVEL SECURITY' in _rendered(conn)


async def test_enable_skips_store_vectors_when_table_is_absent() -> None:
    conn = _conn_where_exists(set())

    await enable_tenant_rls(
        conn, "aegra_tenant", tables=("store",), shared_tables=(), child_tables={}, grant_only_tables=()
    )

    rendered = _rendered(conn)
    assert 'ALTER TABLE "public"."store" ENABLE ROW LEVEL SECURITY' in rendered
    assert not any("store_vectors" in s and not s.startswith("SELECT") for s in rendered)


async def test_enable_covers_the_connection_schema_and_grants_usage() -> None:
    conn = _conn_where_exists({"store_vectors"}, current_schema="aegra")

    schema = await enable_tenant_rls(
        conn, "aegra_tenant", tables=("store",), shared_tables=("assistant",), child_tables={}, grant_only_tables=()
    )

    rendered = _rendered(conn)
    assert schema == "aegra"
    assert 'GRANT USAGE ON SCHEMA "aegra" TO "aegra_tenant"' in rendered
    assert 'ALTER TABLE "aegra"."store" ENABLE ROW LEVEL SECURITY' in rendered
    assert 'ALTER TABLE "aegra"."store_vectors" ENABLE ROW LEVEL SECURITY' in rendered
    assert 'ALTER TABLE "aegra"."assistant" ENABLE ROW LEVEL SECURITY' in rendered
    assert not any('"public"' in s for s in rendered)


async def test_enable_uses_an_explicit_schema_over_the_connection_default() -> None:
    conn = _conn_where_exists(set(), current_schema="public")

    schema = await enable_tenant_rls(
        conn, "aegra_tenant", tables=("store",), shared_tables=(), child_tables={}, grant_only_tables=(), schema="other"
    )

    rendered = _rendered(conn)
    assert schema == "other"
    assert "SELECT current_schema()" not in rendered
    assert 'ALTER TABLE "other"."store" ENABLE ROW LEVEL SECURITY' in rendered


def test_child_policy_qualifies_the_parent_table() -> None:
    read, *_ = (
        stmt.as_string(None)
        for stmt in build_child_table_statements(
            "assistant_versions", "assistant", "assistant_id", "r", login_role="app", schema="s"
        )
        if "CREATE POLICY" in stmt.as_string(None)
    )
    assert 'FROM "s"."assistant" WHERE "s"."assistant"."assistant_id" = "s"."assistant_versions"."assistant_id"' in read


@pytest.mark.parametrize(
    "statements",
    [
        pytest.param(lambda: _render("checkpoints"), id="tenant"),
        pytest.param(_render_shared, id="shared"),
        pytest.param(
            lambda: [
                stmt.as_string(None)
                for stmt in build_child_table_statements(
                    "assistant_versions", "assistant", "assistant_id", "r", login_role="aegra_app"
                )
            ],
            id="child",
        ),
    ],
)
def test_every_isolated_table_binds_its_owner_and_admits_only_the_login_role_as_system(
    statements: Callable[[], list[str]],
) -> None:
    rendered = statements()
    enable_idx = next(i for i, s in enumerate(rendered) if "ENABLE ROW LEVEL SECURITY" in s)
    force_idx = next(i for i, s in enumerate(rendered) if "FORCE ROW LEVEL SECURITY" in s)
    system = [s for s in rendered if s.startswith('CREATE POLICY "aegra_system_access"')]

    assert enable_idx < force_idx
    assert len(system) == 1
    # Scoped to the login role, never PUBLIC: the tenant role must not match it.
    assert ' TO "aegra_app" ' in system[0]
    assert system[0].count("current_setting('aegra.system', true) = 'on'") == 2
    assert not any("aegra.system" in s for s in rendered if s.startswith("CREATE POLICY") and s not in system)


async def test_enable_admits_the_connecting_role_as_system_by_default() -> None:
    conn = _conn_where_exists(set())

    await enable_tenant_rls(
        conn, "aegra_tenant", tables=("store",), shared_tables=(), child_tables={}, grant_only_tables=()
    )

    system = next(s for s in _rendered(conn) if s.startswith('CREATE POLICY "aegra_system_access"'))
    assert ' TO "aegra_owner" ' in system


async def test_enable_refuses_untagged_rows_before_changing_anything() -> None:
    conn = _conn_where_exists(set(), untagged={"thread": 3, "assistant": 1})

    with pytest.raises(UntaggedRowsError) as exc_info:
        await enable_tenant_rls(conn, "aegra_tenant", tables=("thread",), child_tables={}, grant_only_tables=())

    assert exc_info.value.counts == {"thread": 3, "assistant": 1}
    changed = [s for s in _rendered(conn) if s.split(" ", 1)[0] in ("ALTER", "CREATE", "GRANT", "UPDATE", "INSERT")]
    assert changed == []


async def test_untagged_count_leaves_shared_system_assistants_out() -> None:
    conn = _conn_where_exists(set())

    await enable_tenant_rls(conn, "aegra_tenant", tables=(), child_tables={}, grant_only_tables=())

    count = next(s for s in _rendered(conn) if s.startswith('SELECT count(*) FROM "public"."assistant"'))
    assert "user_id <> 'system'" in count


async def test_assign_existing_tags_rows_and_moves_store_under_the_tenant_head_before_policies() -> None:
    conn = _conn_where_exists({"store_vectors"}, untagged={"thread": 2, "store": 1})

    await enable_tenant_rls(
        conn,
        "aegra_tenant",
        tables=("thread", "store"),
        child_tables={},
        grant_only_tables=(),
        assign_existing_to="legacy",
    )

    rendered = _rendered(conn)
    assert """UPDATE "public"."thread" SET tenant_id = 'legacy' WHERE tenant_id IS NULL""" in rendered
    assert any(s.startswith('UPDATE "public"."assistant"') and s.endswith("AND user_id <> 'system'") for s in rendered)
    insert = next(s for s in rendered if s.startswith('INSERT INTO "public"."store"'))
    assert "'aegra_tenant.legacy.' || prefix" in insert
    vectors = next(s for s in rendered if s.startswith('UPDATE "public"."store_vectors"'))
    assert "'aegra_tenant.legacy.' || prefix" in vectors
    delete_idx = rendered.index('DELETE FROM "public"."store" WHERE tenant_id IS NULL')
    # Vectors are repointed before the old store rows (and their cascade) go away.
    assert rendered.index(vectors) < delete_idx
    assert delete_idx < next(i for i, s in enumerate(rendered) if s.startswith("CREATE POLICY"))
    assert not any(s.startswith('UPDATE "public"."store" ') for s in rendered)
    conn.transaction.assert_called_once()


async def test_assign_existing_is_a_no_op_when_every_row_is_tagged() -> None:
    conn = _conn_where_exists(set())

    await enable_tenant_rls(
        conn, "aegra_tenant", tables=("thread",), child_tables={}, grant_only_tables=(), assign_existing_to="legacy"
    )

    assert not any(s.startswith("UPDATE") for s in _rendered(conn))


async def test_assign_existing_rejects_a_malformed_tenant_before_touching_the_database() -> None:
    conn = _conn_where_exists(set())

    with pytest.raises(ValueError):
        await enable_tenant_rls(conn, "aegra_tenant", assign_existing_to="bad.tenant")

    conn.execute.assert_not_awaited()


async def test_enable_raises_the_system_flag_so_an_owner_rerun_sees_rows() -> None:
    conn = _conn_where_exists(set())

    await enable_tenant_rls(conn, "aegra_tenant", tables=(), shared_tables=(), child_tables={}, grant_only_tables=())

    assert conn.execute.await_args_list[2].args == ("SELECT set_config(%s, 'on', false)", ("aegra.system",))


def test_isolated_tables_default_to_every_tenant_table(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.checkpoint, "AEGRA_CHECKPOINT_BACKEND", "postgres")

    assert isolated_tables_for_settings() == TENANT_TABLES


def test_isolated_tables_leave_out_checkpoint_tables_on_dynamodb(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.checkpoint, "AEGRA_CHECKPOINT_BACKEND", "dynamodb")

    tables = isolated_tables_for_settings()

    assert set(tables) == set(TENANT_TABLES) - set(CHECKPOINT_TABLES)
    assert "store" in tables
    assert set(CHECKPOINT_TABLES) == {"checkpoints", "checkpoint_blobs", "checkpoint_writes"}


async def test_enable_skips_checkpoint_tables_on_dynamodb(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.checkpoint, "AEGRA_CHECKPOINT_BACKEND", "dynamodb")
    conn = _conn_where_exists(set())

    await enable_tenant_rls(conn, "aegra_tenant", shared_tables=(), child_tables={}, grant_only_tables=())

    rendered = _rendered(conn)
    assert not any("checkpoint" in s for s in rendered)
    assert any(s == 'ALTER TABLE "public"."store" ENABLE ROW LEVEL SECURITY' for s in rendered)
    assert any(s == 'ALTER TABLE "public"."thread" ENABLE ROW LEVEL SECURITY' for s in rendered)


async def test_enable_covers_checkpoint_tables_on_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.checkpoint, "AEGRA_CHECKPOINT_BACKEND", "postgres")
    conn = _conn_where_exists(set())

    await enable_tenant_rls(conn, "aegra_tenant", shared_tables=(), child_tables={}, grant_only_tables=())

    rendered = _rendered(conn)
    for table in CHECKPOINT_TABLES:
        assert any(f'ALTER TABLE "public"."{table}" ENABLE ROW LEVEL SECURITY' == s for s in rendered)
