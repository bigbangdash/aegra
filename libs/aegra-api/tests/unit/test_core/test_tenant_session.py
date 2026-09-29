from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

from aegra_api.core.db_scope import DbScopeMissingError, system_scope, tenant_scope
from aegra_api.core.tenant_pool import TENANT_SETTING
from aegra_api.core.tenant_session import TenantScopedSession, _apply_db_scope, session_class_for_settings
from aegra_api.settings import settings


def _connection() -> MagicMock:
    conn = MagicMock()
    conn.dialect = postgresql.dialect()
    return conn


def test_session_class_follows_rls_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)
    enabled = session_class_for_settings()
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", False)
    disabled = session_class_for_settings()

    assert enabled is TenantScopedSession
    assert disabled is Session


def test_tenant_scope_switches_role_and_sets_tenant(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_DB_ROLE", "aegra_tenant")
    conn = _connection()

    with tenant_scope("org-a"):
        _apply_db_scope(MagicMock(), MagicMock(), conn)

    conn.exec_driver_sql.assert_called_once_with("SET LOCAL ROLE aegra_tenant")
    _, params = conn.execute.call_args.args
    assert params == {"name": TENANT_SETTING, "value": "org-a"}


def test_role_name_is_quoted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_DB_ROLE", 'bad"; DROP TABLE thread; --')
    conn = _connection()

    with tenant_scope("org-a"):
        _apply_db_scope(MagicMock(), MagicMock(), conn)

    conn.exec_driver_sql.assert_called_once_with('SET LOCAL ROLE "bad""; DROP TABLE thread; --"')


def test_system_scope_leaves_transaction_untouched() -> None:
    conn = _connection()

    with system_scope("unit test"):
        _apply_db_scope(MagicMock(), MagicMock(), conn)

    conn.exec_driver_sql.assert_not_called()
    conn.execute.assert_not_called()


def test_missing_scope_fails_closed() -> None:
    with pytest.raises(DbScopeMissingError):
        _apply_db_scope(MagicMock(), MagicMock(), _connection())
