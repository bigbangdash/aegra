"""Operator-run DDL that turns on tenant RLS.

LangGraph creates its tables in setup(), after alembic has run, so they
cannot be migrated by alembic; roles and RLS are kept out of migrations too.
Run this once after the first startup, as a role allowed to CREATE ROLE and
ALTER the tables (never from app startup).

Every isolated table uses FORCE ROW LEVEL SECURITY, so the table owner (the
server's login role) is bound by the policies too: a connection that declared
no scope sees no rows, whichever code path opened it. The system scope gets
rows through one extra policy, scoped to the login role and keyed on the
aegra.system setting that only system_scope raises (core.tenant_pool). No
BYPASSRLS role is needed, and the login role keeps owning the tables, so
LangGraph setup() and alembic still run their DDL.

Tables are addressed schema-qualified: the schema defaults to the DDL
connection's current_schema(), so an install outside `public` (a search_path
in the DSN) is covered, and the tenant role gets USAGE on it.
"""

from collections.abc import Mapping, Sequence

from psycopg import AsyncConnection, sql

from aegra_api.core.db_scope import is_valid_tenant_id
from aegra_api.core.tenant_pool import SYSTEM_SETTING, TENANT_SETTING
from aegra_api.core.tenant_store import TENANT_NAMESPACE_ROOT

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
SYSTEM_POLICY_NAME = "aegra_system_access"
SHARED_ROWS_CHECK = "aegra_tenant_shared_rows_are_system"
_SPLIT_POLICIES = ("aegra_tenant_read", "aegra_tenant_insert", "aegra_tenant_update", "aegra_tenant_delete")

# NULLIF matters: a pooled connection that ever set the GUC reports '' (not
# NULL) afterwards, and '' = '' would silently match untagged rows.
_CURRENT_TENANT = sql.SQL("NULLIF(current_setting({}, true), '')").format(sql.Literal(TENANT_SETTING))
_SYSTEM_ON = sql.SQL("current_setting({}, true) = 'on'").format(sql.Literal(SYSTEM_SETTING))


def _table_ident(table: str, schema: str | None) -> sql.Identifier:
    return sql.Identifier(schema, table) if schema else sql.Identifier(table)


def _force_statements(t: sql.Identifier, login_role: str) -> list[sql.Composed]:
    # Scoped TO the login role: the tenant role never matches it, even if the setting leaked.
    return [
        sql.SQL("DROP POLICY IF EXISTS {} ON {}").format(sql.Identifier(SYSTEM_POLICY_NAME), t),
        sql.SQL("CREATE POLICY {} ON {} TO {} USING ({}) WITH CHECK ({})").format(
            sql.Identifier(SYSTEM_POLICY_NAME), t, sql.Identifier(login_role), _SYSTEM_ON, _SYSTEM_ON
        ),
        sql.SQL("ALTER TABLE {} FORCE ROW LEVEL SECURITY").format(t),
    ]


def build_table_statements(
    table: str, tenant_role: str, *, login_role: str, schema: str | None = None
) -> list[sql.Composed]:
    t = _table_ident(table, schema)
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
        *_force_statements(t, login_role),
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


def build_shared_table_statements(
    table: str, tenant_role: str, *, login_role: str, schema: str | None = None
) -> list[sql.Composed]:
    t = _table_ident(table, schema)
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
        *_force_statements(t, login_role),
    ]


def build_child_table_statements(
    table: str, parent: str, key: str, tenant_role: str, *, login_role: str, schema: str | None = None
) -> list[sql.Composed]:
    t = _table_ident(table, schema)
    # The parent's own RLS applies inside the subquery, so "visible parent"
    # already means own tenant or shared.
    parent_visible = sql.SQL("EXISTS (SELECT 1 FROM {p} WHERE {p}.{k} = {t}.{k})").format(
        p=_table_ident(parent, schema), k=sql.Identifier(key), t=t
    )
    parent_owned = sql.SQL("EXISTS (SELECT 1 FROM {p} WHERE {p}.{k} = {t}.{k} AND {p}.tenant_id = {cur})").format(
        p=_table_ident(parent, schema), k=sql.Identifier(key), t=t, cur=_CURRENT_TENANT
    )
    return [
        sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON {} TO {}").format(t, sql.Identifier(tenant_role)),
        *_split_policy_statements(t, read=parent_visible, write=parent_owned),
        *_force_statements(t, login_role),
    ]


async def _table_exists(conn: AsyncConnection, schema: str, table: str) -> bool:
    cur = await conn.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = %s AND tablename = %s)",
        (schema, table),
    )
    row = await cur.fetchone()
    return bool(row and row[0])


class UntaggedRowsError(Exception):
    """Rows without a tenant exist; enabling would fail on NOT NULL half-way, so nothing was changed."""

    def __init__(self, counts: Mapping[str, int]) -> None:
        self.counts = dict(counts)
        detail = ", ".join(f"{table}: {n}" for table, n in self.counts.items())
        super().__init__(f"rows without a tenant_id ({detail})")


async def _column_exists(conn: AsyncConnection, schema: str, table: str, column: str) -> bool:
    cur = await conn.execute(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s)",
        (schema, table, column),
    )
    row = await cur.fetchone()
    return bool(row and row[0])


async def count_untagged_rows(
    conn: AsyncConnection, schema: str, *, tables: Sequence[str], shared_tables: Sequence[str]
) -> dict[str, int]:
    """Rows the enable step would reject: no tenant, and not a shared system assistant."""
    counts: dict[str, int] = {}
    for table, shared in [*((t, False) for t in tables), *((t, True) for t in shared_tables)]:
        # Before the first enable the column does not exist yet: every row is untagged.
        untagged = (
            sql.SQL("tenant_id IS NULL") if await _column_exists(conn, schema, table, "tenant_id") else sql.SQL("true")
        )
        where = sql.SQL("{} AND user_id <> 'system'").format(untagged) if shared else untagged
        cur = await conn.execute(sql.SQL("SELECT count(*) FROM {} WHERE {}").format(_table_ident(table, schema), where))
        row = await cur.fetchone()
        if row and row[0]:
            counts[table] = int(row[0])
    return counts


async def _assign_existing_rows(
    conn: AsyncConnection, schema: str, tenant_id: str, *, tables: Sequence[str], shared_tables: Sequence[str]
) -> None:
    add_column = sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS tenant_id text")
    assign = sql.SQL("UPDATE {} SET tenant_id = {} WHERE tenant_id IS NULL")
    tenant = sql.Literal(tenant_id)
    async with conn.transaction():
        for table in (*tables, *shared_tables):
            await conn.execute(add_column.format(_table_ident(table, schema)))
        for table in tables:
            if table not in ("store", "store_vectors"):
                await conn.execute(assign.format(_table_ident(table, schema), tenant))
        for table in shared_tables:
            await conn.execute(
                sql.SQL("{} AND user_id <> 'system'").format(assign.format(_table_ident(table, schema), tenant))
            )
        if "store" in tables:
            await _move_store_rows_under_tenant(conn, schema, tenant_id, with_vectors="store_vectors" in tables)


async def _move_store_rows_under_tenant(
    conn: AsyncConnection, schema: str, tenant_id: str, *, with_vectors: bool
) -> None:
    # Tenant store rows live under the hidden namespace head (core.tenant_store). The key is
    # (prefix, key) and store_vectors references it without ON UPDATE, so copy, repoint, delete.
    head = sql.Literal(f"{TENANT_NAMESPACE_ROOT}.{tenant_id}.")
    store = _table_ident("store", schema)
    cur = await conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = 'store' "
        "AND column_name NOT IN ('prefix', 'tenant_id') ORDER BY ordinal_position",
        (schema,),
    )
    rest = [sql.Identifier(row[0]) for row in await cur.fetchall()]
    await conn.execute(
        sql.SQL(
            "INSERT INTO {s} (prefix, tenant_id, {cols}) SELECT {h} || prefix, {t}, {cols} FROM {s} "
            "WHERE tenant_id IS NULL"
        ).format(s=store, cols=sql.SQL(", ").join(rest), h=head, t=sql.Literal(tenant_id))
    )
    if with_vectors:
        await conn.execute(
            sql.SQL("UPDATE {} SET prefix = {} || prefix, tenant_id = {} WHERE tenant_id IS NULL").format(
                _table_ident("store_vectors", schema), head, sql.Literal(tenant_id)
            )
        )
    await conn.execute(sql.SQL("DELETE FROM {} WHERE tenant_id IS NULL").format(store))


async def _current_user(conn: AsyncConnection) -> str:
    cur = await conn.execute("SELECT current_user")
    row = await cur.fetchone()
    if not row or not row[0]:
        raise ValueError("could not determine the connecting role; pass app_login_role")
    return str(row[0])


async def _current_schema(conn: AsyncConnection) -> str:
    cur = await conn.execute("SELECT current_schema()")
    row = await cur.fetchone()
    if not row or not row[0]:
        raise ValueError("search_path names no existing schema; pass schema explicitly")
    return str(row[0])


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
    schema: str | None = None,
    assign_existing_to: str | None = None,
) -> str:
    """Apply roles, grants and policies; returns the schema that was covered.

    app_login_role is the role the server logs in as (default: the connecting
    role). It is granted the tenant role and is the only role the system policy admits.
    Rows without a tenant raise UntaggedRowsError before anything changes, unless
    assign_existing_to names the tenant that takes them all (a single-tenant install).
    """
    if not conn.autocommit:
        raise ValueError("enable_tenant_rls needs an autocommit connection (CREATE INDEX CONCURRENTLY)")
    if assign_existing_to is not None and not is_valid_tenant_id(assign_existing_to):
        raise ValueError(f"assign_existing_to is not a valid tenant id: {assign_existing_to!r}")
    schema = schema or await _current_schema(conn)
    login_role = app_login_role or await _current_user(conn)
    # Re-runs as the owner under FORCE would otherwise count and move nothing.
    await conn.execute("SELECT set_config(%s, 'on', false)", (SYSTEM_SETTING,))
    present_optional = [table for table in optional_tables if await _table_exists(conn, schema, table)]
    isolated = (*tables, *present_optional)
    untagged = await count_untagged_rows(conn, schema, tables=isolated, shared_tables=shared_tables)
    if untagged and assign_existing_to is None:
        raise UntaggedRowsError(untagged)
    if untagged and assign_existing_to is not None:
        await _assign_existing_rows(conn, schema, assign_existing_to, tables=isolated, shared_tables=shared_tables)
    await ensure_tenant_role(conn, tenant_role, app_login_role=app_login_role)
    # Without USAGE every tenant-scoped query fails outside `public` (PUBLIC only has it there by default).
    await conn.execute(
        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(sql.Identifier(schema), sql.Identifier(tenant_role))
    )
    for table in isolated:
        for statement in build_table_statements(table, tenant_role, login_role=login_role, schema=schema):
            await conn.execute(statement)
    for table in shared_tables:
        for statement in build_shared_table_statements(table, tenant_role, login_role=login_role, schema=schema):
            await conn.execute(statement)
    for table, (parent, key) in child_tables.items():
        for statement in build_child_table_statements(
            table, parent, key, tenant_role, login_role=login_role, schema=schema
        ):
            await conn.execute(statement)
    for table in grant_only_tables:
        await conn.execute(
            sql.SQL("GRANT SELECT, INSERT, UPDATE, DELETE ON {} TO {}").format(
                _table_ident(table, schema), sql.Identifier(tenant_role)
            )
        )
    return schema
