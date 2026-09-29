"""SQLAlchemy session class that applies the current DB scope per transaction.

SET LOCAL dies with each COMMIT, and AsyncSession autobegins a new
transaction after every commit, so the scope is re-applied on each
after_begin rather than once per request.
"""

from sqlalchemy import event, text
from sqlalchemy.engine import Connection
from sqlalchemy.orm import Session, SessionTransaction

from aegra_api.core.db_scope import current_db_scope
from aegra_api.core.tenant_pool import TENANT_SETTING
from aegra_api.settings import settings


class TenantScopedSession(Session):
    pass


@event.listens_for(TenantScopedSession, "after_begin")
def _apply_db_scope(session: Session, transaction: SessionTransaction, connection: Connection) -> None:
    scope = current_db_scope()
    if scope.is_system:
        return
    role = connection.dialect.identifier_preparer.quote(settings.tenant.AEGRA_TENANT_DB_ROLE)
    connection.exec_driver_sql(f"SET LOCAL ROLE {role}")
    connection.execute(
        text("SELECT set_config(:name, :value, true)"), {"name": TENANT_SETTING, "value": scope.tenant_id}
    )


def session_class_for_settings() -> type[Session]:
    return TenantScopedSession if settings.tenant.AEGRA_TENANT_RLS_ENABLED else Session
