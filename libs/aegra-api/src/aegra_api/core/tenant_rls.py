"""Operator-run DDL that turns on tenant RLS.

LangGraph creates its tables in setup(), after alembic has run, so they
cannot be migrated by alembic; roles and RLS are kept out of migrations too.
Run this once after the first startup, as a role allowed to CREATE ROLE and
ALTER the tables (never from app startup).
"""

from collections.abc import Mapping, Sequence

from psycopg import AsyncConnection, sql

from aegra_api.core.tenant_pool import TENANT_SETTING

METADATA_TENANT_TABLES: tuple[str, ...] = ("thread", "runs", "crons")
LANGGRAPH_TENANT_TABLES: tuple[str, ...] = (
    "checkpoints",
    "checkpoint_blobs",
    "checkpoint_writes",
    "store",
)
TENANT_TABLES: tuple[str, ...] = METADATA_TENANT_TABLES + LANGGRAPH_TENANT_TABLES
# Created by store.setup() only when semantic search (an index) is configured.
OPTIONAL_TENANT_TABLES: tuple[str, ...] = ("store_vectors",)
# Tenant rows plus read-only shared rows owned by user_id 'system' (tenant_id NULL).
SHARED_TENANT_TABLES: tuple[str, ...] = ("assistant",)
# Child table -> (parent table, key column): rows follow the parent's visibility.
CHILD_TENANT_TABLES: dict[str, tuple[str, str]] = {"assistant_versions": ("assistant", "assistant_id")}
# Touched inside tenant-scoped transactions but holding no tenant content.
GRANT_ONLY_TABLES: tuple[str, ...] = ("thread_ttl",)
POLICY_NAME = "aegra_tenant_isolation"
SHARED_ROWS_CHECK = "aegra_tenant_shared_rows_are_system"
_SPLIT_POLICIES = ("aegra_tenant_read", "aegra_tenant_insert", "aegra_tenant_update", "aegra_tenant_delete")

# NULLIF matters: a pooled connection that ever set the GUC reports '' (not
# NULL) afterwards, and '' = '' would silently match untagged rows.
_CURRENT_TENANT = sql.SQL("NULLIF(current_setting({}, true), '')").format(sql.Literal(TENANT_SETTING))


def build_table_statements(table: str, tenant_role: str) -> list[sql.Composed]:
    t = sql.Identifier(table)
    role = sql.Identifier(tenant_role)
    return [
        sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS tenant_id text").format(t),
        sql.SQL("ALTER TABLE {} ALTER COLUMN tenant_id SET DEFAULT {}").format(t, _CURRENT_TENANT),
        sql.SQL("ALTER TABLE {} ALTER COLUMN tenant_id SET NOT NULL").format(t),
        sql.SQL("CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {} (tenant_id)").format(
            sql.Identifier(f"idx_{table}_tenant_id"), t
        ),
        sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON {} TO {}").format(t, role),
        sql.SQL("DROP POLICY IF EXISTS {} ON {}").format(sql.Identifier(POLICY_NAME), t),
        sql.SQL("CREATE POLICY {} ON {} USING (tenant_id = {}) WITH CHECK (tenant_id = {})").format(
            sql.Identifier(POLICY_NAME), t, _CURRENT_TENANT, _CURRENT_TENANT
        ),
        sql.SQL("ALTER TABLE {} ENABLE ROW LEVEL SECURITY").format(t),
    ]


def _split_policy_statements(t: sql.Identifier, *, read: sql.Composable, write: sql.Composable) -> list[sql.Composed]:
    read_name, insert_name, update_name, delete_name = (sql.Identifier(name) for name in _SPLIT_POLICIES)
    return [
        *(sql.SQL("DROP POLICY IF EXISTS {} ON {}").format(sql.Identifier(name), t) for name in _SPLIT_POLICIES),
        sql.SQL("CREATE POLICY {} ON {} FOR SELECT USING ({})").format(read_name, t, read),
        sql.SQL("CREATE POLICY {} ON {} FOR INSERT WITH CHECK ({})").format(insert_name, t, write),
        sql.SQL("CREATE POLICY {} ON {} FOR UPDATE USING ({}) WITH CHECK ({})").format(update_name, t, write, write),
        sql.SQL("CREATE POLICY {} ON {} FOR DELETE USING ({})").format(delete_name, t, write),
        sql.SQL("ALTER TABLE {} ENABLE ROW LEVEL SECURITY").format(t),
    ]


def build_shared_table_statements(table: str, tenant_role: str) -> list[sql.Composed]:
    t = sql.Identifier(table)
    own = sql.SQL("tenant_id = {}").format(_CURRENT_TENANT)
    # Shared rows are readable by every tenant but writable by none; the CHECK
    # stops a tenant row that lost its tenant_id from turning into a shared one.
    shared = sql.SQL("{} OR (tenant_id IS NULL AND user_id = 'system')").format(own)
    return [
        sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS tenant_id text").format(t),
        sql.SQL("ALTER TABLE {} ALTER COLUMN tenant_id SET DEFAULT {}").format(t, _CURRENT_TENANT),
        sql.SQL("ALTER TABLE {} DROP CONSTRAINT IF EXISTS {}").format(t, sql.Identifier(SHARED_ROWS_CHECK)),
        sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} CHECK (tenant_id IS NOT NULL OR user_id = 'system')").format(
            t, sql.Identifier(SHARED_ROWS_CHECK)
        ),
        sql.SQL("CREATE INDEX CONCURRENTLY IF NOT EXISTS {} ON {} (tenant_id)").format(
            sql.Identifier(f"idx_{table}_tenant_id"), t
        ),
        sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON {} TO {}").format(t, sql.Identifier(tenant_role)),
        *_split_policy_statements(t, read=shared, write=own),
    ]


def build_child_table_statements(table: str, parent: str, key: str, tenant_role: str) -> list[sql.Composed]:
    t = sql.Identifier(table)
    # The parent's own RLS applies inside the subquery, so "visible parent"
    # already means own tenant or shared.
    parent_visible = sql.SQL("EXISTS (SELECT 1 FROM {p} WHERE {p}.{k} = {t}.{k})").format(
        p=sql.Identifier(parent), k=sql.Identifier(key), t=t
    )
    parent_owned = sql.SQL("EXISTS (SELECT 1 FROM {p} WHERE {p}.{k} = {t}.{k} AND {p}.tenant_id = {cur})").format(
        p=sql.Identifier(parent), k=sql.Identifier(key), t=t, cur=_CURRENT_TENANT
    )
    return [
        sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON {} TO {}").format(t, sql.Identifier(tenant_role)),
        *_split_policy_statements(t, read=parent_visible, write=parent_owned),
    ]


async def _table_exists(conn: AsyncConnection, table: str) -> bool:
    cur = await conn.execute("SELECT to_regclass(%s) IS NOT NULL", (table,))
    row = await cur.fetchone()
    return bool(row and row[0])


async def ensure_tenant_role(conn: AsyncConnection, tenant_role: str, *, app_login_role: str | None = None) -> None:
    cur = await conn.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (tenant_role,))
    if await cur.fetchone() is None:
        await conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(tenant_role)))
    # A non-superuser app login can only SET ROLE into roles it is a member of.
    if app_login_role:
        await conn.execute(
            sql.SQL("GRANT {} TO {}").format(sql.Identifier(tenant_role), sql.Identifier(app_login_role))
        )


async def enable_tenant_rls(
    conn: AsyncConnection,
    tenant_role: str,
    *,
    app_login_role: str | None = None,
    tables: Sequence[str] = TENANT_TABLES,
    shared_tables: Sequence[str] = SHARED_TENANT_TABLES,
    child_tables: Mapping[str, tuple[str, str]] = CHILD_TENANT_TABLES,
    grant_only_tables: Sequence[str] = GRANT_ONLY_TABLES,
    optional_tables: Sequence[str] = OPTIONAL_TENANT_TABLES,
) -> None:
    if not conn.autocommit:
        raise ValueError("enable_tenant_rls needs an autocommit connection (CREATE INDEX CONCURRENTLY)")
    await ensure_tenant_role(conn, tenant_role, app_login_role=app_login_role)
    present_optional = [table for table in optional_tables if await _table_exists(conn, table)]
    for table in (*tables, *present_optional):
        for statement in build_table_statements(table, tenant_role):
            await conn.execute(statement)
    for table in shared_tables:
        for statement in build_shared_table_statements(table, tenant_role):
            await conn.execute(statement)
    for table, (parent, key) in child_tables.items():
        for statement in build_child_table_statements(table, parent, key, tenant_role):
            await conn.execute(statement)
    for table in grant_only_tables:
        await conn.execute(
            sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON {} TO {}").format(
                sql.Identifier(table), sql.Identifier(tenant_role)
            )
        )
