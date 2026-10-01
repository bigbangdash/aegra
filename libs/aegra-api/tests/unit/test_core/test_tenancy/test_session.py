from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Session

from aegra_api.core.tenancy.pool import SYSTEM_SETTING, TENANT_SETTING
from aegra_api.core.tenancy.scope import DbScopeMissingError, system_scope, tenant_scope
from aegra_api.core.tenancy.session import TenantScopedSession, _apply_db_scope, session_class_for_settings
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

    conn.exec_driver_sql.assert_not_called()
    (apply_call,) = conn.execute.call_args_list
    statement, params = apply_call.args
    assert str(statement).startswith("SELECT set_config('role', :role, true)")
    assert params == {
        "role": "aegra_tenant",
        "tenant_name": TENANT_SETTING,
        "tenant": "org-a",
        "system_name": SYSTEM_SETTING,
    }


def test_role_name_is_sent_as_a_bound_value_not_sql(monkeypatch: pytest.MonkeyPatch) -> None:
    hostile = 'bad"; DROP TABLE thread; --'
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_DB_ROLE", hostile)
    conn = _connection()

    with tenant_scope("org-a"):
        _apply_db_scope(MagicMock(), MagicMock(), conn)

    statement, params = conn.execute.call_args.args
    assert hostile not in str(statement)
    assert params["role"] == hostile


def test_system_scope_keeps_the_login_role_and_raises_the_system_flag() -> None:
    conn = _connection()

    with system_scope("unit test"):
        _apply_db_scope(MagicMock(), MagicMock(), conn)

    conn.exec_driver_sql.assert_not_called()
    statement, params = conn.execute.call_args.args
    assert "set_config(:name, :value, true)" in str(statement)
    assert params == {"name": SYSTEM_SETTING, "value": "on"}


def test_missing_scope_fails_closed() -> None:
    with pytest.raises(DbScopeMissingError):
        _apply_db_scope(MagicMock(), MagicMock(), _connection())
