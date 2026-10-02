"""SQLAlchemy session class that applies the current DB scope per transaction.

SET LOCAL dies with each COMMIT, and AsyncSession autobegins a new
transaction after every commit, so the scope is re-applied on each
after_begin rather than once per request.

System transactions raise SYSTEM_SETTING, the only way the login role sees rows
once the tables use FORCE ROW LEVEL SECURITY (see core.tenancy.pool).
"""

from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session, SessionTransaction

from aegra_api.core.tenancy.pool import SYSTEM_SETTING, TENANT_SETTING
from aegra_api.core.tenancy.scope import current_db_scope
from aegra_api.settings import settings

# One round trip, as in core.tenancy.pool: set_config('role', ...) is SET LOCAL ROLE.
_APPLY_TENANT = text(
    "SELECT set_config('role', :role, true), set_config(:tenant_name, :tenant, true), "
    "set_config(:system_name, '', true)"
)


class TenantScopedSession(Session):
    pass


@event.listens_for(TenantScopedSession, "after_begin")
def _apply_db_scope(session: Session, transaction: SessionTransaction, connection: Connection) -> None:
    scope = current_db_scope()
    set_local = text("SELECT set_config(:name, :value, true)")
    if scope.is_system:
        connection.execute(set_local, {"name": SYSTEM_SETTING, "value": "on"})
        return
    connection.execute(
        _APPLY_TENANT,
        {
            "role": settings.tenant.AEGRA_TENANT_DB_ROLE,
            "tenant_name": TENANT_SETTING,
            "tenant": scope.tenant_id,
            "system_name": SYSTEM_SETTING,
        },
    )


def session_class_for_settings() -> type[Session]:
    return TenantScopedSession if settings.tenant.AEGRA_TENANT_RLS_ENABLED else Session
