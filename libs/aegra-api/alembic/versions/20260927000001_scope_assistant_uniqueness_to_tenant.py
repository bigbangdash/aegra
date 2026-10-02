"""Add assistant.tenant_id and scope assistant uniqueness to the tenant

With AEGRA_TENANT_RLS_ENABLED the same user id can exist in two tenants. The
old unique index on (user_id, graph_id, md5(config)) spans tenants, so the
second tenant's create collided with a row RLS hides from it. COALESCE keeps
uniqueness identical for installs that never set tenant_id.

Indexes are built CONCURRENTLY (see d9e0f1a23456). If a build is interrupted,
drop the INVALID index and re-run.

Downgrade restores the cross-tenant index and fails if two tenants already
hold the same (user_id, graph_id, config).

Revision ID: f5c8d3b0a2e6
Revises: e4b7c2a9f1d3
Create Date: 2026-09-27 00:00:01.000000

"""

import sqlalchemy as sa

from alembic import op

revision = "f5c8d3b0a2e6"
down_revision = "e4b7c2a9f1d3"
branch_labels = None
depends_on = None

OLD_UNIQUE = "idx_assistant_user_graph_config"
NEW_UNIQUE = "idx_assistant_tenant_user_graph_config"
TENANT_INDEX = "idx_assistant_tenant_id"


def upgrade() -> None:
    op.add_column("assistant", sa.Column("tenant_id", sa.Text(), nullable=True), if_not_exists=True)
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {NEW_UNIQUE}")
        op.execute(
            f"CREATE UNIQUE INDEX CONCURRENTLY {NEW_UNIQUE} "
            "ON assistant (COALESCE(tenant_id, ''), user_id, graph_id, md5(config::text))"
        )
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {OLD_UNIQUE}")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {TENANT_INDEX}")
        op.execute(f"CREATE INDEX CONCURRENTLY {TENANT_INDEX} ON assistant (tenant_id)")


def downgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {OLD_UNIQUE}")
        op.execute(f"CREATE UNIQUE INDEX CONCURRENTLY {OLD_UNIQUE} ON assistant (user_id, graph_id, md5(config::text))")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {NEW_UNIQUE}")
        op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {TENANT_INDEX}")
    op.drop_column("assistant", "tenant_id", if_exists=True)
