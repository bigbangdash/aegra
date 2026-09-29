"""Tests for `aegra db enable-tenant-rls`."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import psycopg
import pytest
from aegra_api.settings import settings
from click.testing import CliRunner

from aegra_cli.cli import cli


def _connection(current_user: str = "app_user") -> MagicMock:
    cursor = MagicMock()
    cursor.fetchone = AsyncMock(return_value=(current_user,))
    conn = MagicMock()
    conn.execute = AsyncMock(return_value=cursor)
    conn.__aenter__ = AsyncMock(return_value=conn)
    conn.__aexit__ = AsyncMock(return_value=False)
    return conn


@pytest.fixture
def rls_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)


def test_refuses_when_flag_is_off(cli_runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", False)

    with patch("psycopg.AsyncConnection.connect", new_callable=AsyncMock) as connect:
        result = cli_runner.invoke(cli, ["db", "enable-tenant-rls"])

    assert result.exit_code == 1
    assert "AEGRA_TENANT_RLS_ENABLED is not true" in result.output
    connect.assert_not_awaited()


@pytest.mark.usefixtures("rls_flag")
def test_grants_tenant_role_to_connecting_role_by_default(cli_runner: CliRunner) -> None:
    conn = _connection("app_user")
    applied: list[dict[str, Any]] = []

    async def fake_apply(_conn: Any, tenant_role: str, *, app_login_role: str | None) -> None:
        applied.append({"tenant_role": tenant_role, "app_login_role": app_login_role})

    with (
        patch(
            "psycopg.AsyncConnection.connect", new_callable=AsyncMock, return_value=conn
        ) as connect,
        patch("aegra_api.core.tenant_rls.enable_tenant_rls", side_effect=fake_apply),
    ):
        result = cli_runner.invoke(cli, ["db", "enable-tenant-rls"])

    assert result.exit_code == 0, result.output
    assert connect.await_args.kwargs == {"autocommit": True}
    assert applied == [
        {"tenant_role": settings.tenant.AEGRA_TENANT_DB_ROLE, "app_login_role": "app_user"}
    ]
    assert "Tenant RLS enabled" in result.output


@pytest.mark.usefixtures("rls_flag")
def test_app_role_option_overrides_connecting_role(cli_runner: CliRunner) -> None:
    conn = _connection("admin")
    apply = AsyncMock()

    with (
        patch("psycopg.AsyncConnection.connect", new_callable=AsyncMock, return_value=conn),
        patch("aegra_api.core.tenant_rls.enable_tenant_rls", apply),
    ):
        result = cli_runner.invoke(cli, ["db", "enable-tenant-rls", "--app-role", "aegra_app"])

    assert result.exit_code == 0, result.output
    assert apply.await_args.kwargs == {"app_login_role": "aegra_app"}
    conn.execute.assert_not_awaited()


@pytest.mark.usefixtures("rls_flag")
def test_missing_tables_explain_to_start_the_server_first(cli_runner: CliRunner) -> None:
    error = psycopg.errors.UndefinedTable()

    with (
        patch(
            "psycopg.AsyncConnection.connect", new_callable=AsyncMock, return_value=_connection()
        ),
        patch("aegra_api.core.tenant_rls.enable_tenant_rls", AsyncMock(side_effect=error)),
    ):
        result = cli_runner.invoke(cli, ["db", "enable-tenant-rls"])

    assert result.exit_code == 1
    assert "Start the server once" in result.output
