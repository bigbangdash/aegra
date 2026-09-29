"""Add nullable tenant_id to thread, runs and crons

Groundwork for AEGRA_TENANT_RLS_ENABLED. The column stays nullable so existing
installs keep working with the flag off; NOT NULL, the RLS policies and the
tenant role are applied by the separate operator-run enable step
(aegra_api.core.tenant_rls), never by migrations.

Indexes are built CONCURRENTLY for the same reason as d9e0f1a23456: a
transactional build holds a SHARE lock that stalls writes on large tables.
If a concurrent build is interrupted, drop the INVALID index and re-run.

Revision ID: e4b7c2a9f1d3
Revises: a3f7c1d9e2b4
Create Date: 2026-09-27 00:00:00.000000

"""

import sqlalchemy as sa

from alembic import op

revision = "e4b7c2a9f1d3"
down_revision = "a3f7c1d9e2b4"
branch_labels = None
depends_on = None

TABLE_INDEXES = (
    ("thread", "idx_thread_tenant_id"),
    ("runs", "idx_runs_tenant_id"),
    ("crons", "idx_crons_tenant_id"),
)


def upgrade() -> None:
    for table, _ in TABLE_INDEXES:
        # Adding a nullable column without a default is a catalog-only change.
        op.add_column(table, sa.Column("tenant_id", sa.Text(), nullable=True), if_not_exists=True)
    with op.get_context().autocommit_block():
        for table, index in TABLE_INDEXES:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index}")
            op.execute(f"CREATE INDEX CONCURRENTLY {index} ON {table} (tenant_id)")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for _, index in TABLE_INDEXES:
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index}")
    for table, _ in TABLE_INDEXES:
        op.drop_column(table, "tenant_id", if_exists=True)
