from unittest.mock import AsyncMock, MagicMock

import pytest

from aegra_api.core.tenant_rls import (
    CHILD_TENANT_TABLES,
    GRANT_ONLY_TABLES,
    SHARED_TENANT_TABLES,
    TENANT_TABLES,
    build_child_table_statements,
    build_shared_table_statements,
    build_table_statements,
    enable_tenant_rls,
)


def _render(table: str, role: str = "aegra_tenant") -> list[str]:
    return [stmt.as_string(None) for stmt in build_table_statements(table, role)]


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
    return [stmt.as_string(None) for stmt in build_shared_table_statements("assistant", "aegra_tenant")]


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
        for stmt in build_child_table_statements("assistant_versions", "assistant", "assistant_id", "r")
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
    cursor = MagicMock()
    cursor.fetchone = AsyncMock(return_value=(1,))
    conn = MagicMock(autocommit=True)
    conn.execute = AsyncMock(return_value=cursor)

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
    assert 'GRANT SELECT, INSERT, UPDATE, DELETE ON "thread_ttl" TO "aegra_tenant"' in rendered
    assert not any("thread_ttl" in s and "ROW LEVEL SECURITY" in s for s in rendered)


def _rendered(conn: MagicMock) -> list[str]:
    return [
        c.args[0] if isinstance(c.args[0], str) else c.args[0].as_string(None) for c in conn.execute.await_args_list
    ]


def _conn_where_exists(existing: set[str]) -> MagicMock:
    async def execute(statement: object, params: tuple[str, ...] | None = None) -> MagicMock:
        cursor = MagicMock()
        if statement == "SELECT to_regclass(%s) IS NOT NULL" and params:
            cursor.fetchone = AsyncMock(return_value=(params[0] in existing,))
        else:
            cursor.fetchone = AsyncMock(return_value=(1,))
        return cursor

    conn = MagicMock(autocommit=True)
    conn.execute = AsyncMock(side_effect=execute)
    return conn


async def test_enable_isolates_store_vectors_when_semantic_search_created_it() -> None:
    conn = _conn_where_exists({"store_vectors"})

    await enable_tenant_rls(
        conn, "aegra_tenant", tables=("store",), shared_tables=(), child_tables={}, grant_only_tables=()
    )

    assert 'ALTER TABLE "store_vectors" ENABLE ROW LEVEL SECURITY' in _rendered(conn)


async def test_enable_skips_store_vectors_when_table_is_absent() -> None:
    conn = _conn_where_exists(set())

    await enable_tenant_rls(
        conn, "aegra_tenant", tables=("store",), shared_tables=(), child_tables={}, grant_only_tables=()
    )

    rendered = _rendered(conn)
    assert 'ALTER TABLE "store" ENABLE ROW LEVEL SECURITY' in rendered
    assert not any("store_vectors" in s and not s.startswith("SELECT") for s in rendered)
