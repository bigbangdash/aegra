"""execute_run_as_tenant: the run-side edge where a tenant is chosen (see core.tenancy.resolver)."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aegra_api.core.active_runs import active_run_tenants, active_runs
from aegra_api.core.tenancy.resolver import configure_tenant_resolver
from aegra_api.core.tenancy.scope import DbScope, DbScopeMissingError, current_db_scope, system_scope
from aegra_api.models.auth import User
from aegra_api.models.run_job import RunExecution, RunIdentity, RunJob
from aegra_api.services import tenant_runs as tenant_runs_module
from aegra_api.services.tenant_runs import execute_run_as_tenant
from aegra_api.settings import settings


def _make_job(run_id: str = "run-1") -> RunJob:
    return RunJob(
        identity=RunIdentity(run_id=run_id, thread_id="thread-1", graph_id="graph-1"),
        user=User(identity="user-1"),
        execution=RunExecution(input_data={"msg": "hello"}),
    )


class TestExecuteRunAsTenant:
    """execute_run_as_tenant is the single place a run picks its tenant."""

    @staticmethod
    def _scoped_job(org_id: str | None) -> RunJob:
        return _make_job().model_copy(update={"user": User(identity="user-1", org_id=org_id)})

    @staticmethod
    def _scope_or_none() -> DbScope | None:
        try:
            return current_db_scope()
        except DbScopeMissingError:
            return None

    @pytest.mark.asyncio
    async def test_runs_in_job_tenant_scope_when_rls_enabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)
        seen: list[DbScope | None] = []

        async def record(_job: RunJob) -> None:
            seen.append(self._scope_or_none())

        with patch.object(tenant_runs_module, "execute_run", side_effect=record):
            await execute_run_as_tenant(self._scoped_job("tenant-a"))

        assert len(seen) == 1 and seen[0] is not None and seen[0].tenant_id == "tenant-a"
        assert self._scope_or_none() is None

    @pytest.mark.asyncio
    async def test_tenant_scope_overrides_inherited_system_scope(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Worker loops run in a system scope; each job must drop to its tenant.
        monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)
        seen: list[DbScope | None] = []

        async def record(_job: RunJob) -> None:
            seen.append(self._scope_or_none())

        with patch.object(tenant_runs_module, "execute_run", side_effect=record), system_scope("test: worker loop"):
            await execute_run_as_tenant(self._scoped_job("tenant-b"))

        assert seen[0] is not None and seen[0].tenant_id == "tenant-b"

    @pytest.mark.asyncio
    async def test_run_tenant_is_registered_only_while_running(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # The cancel listener reads this to write the end event under the run's own tenant.
        monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)
        job = self._scoped_job("tenant-a")
        seen: list[str | None] = []

        async def record(_job: RunJob) -> None:
            seen.append(active_run_tenants.get(job.identity.run_id))
            raise RuntimeError("boom")

        with patch.object(tenant_runs_module, "execute_run", side_effect=record), pytest.raises(RuntimeError):
            await execute_run_as_tenant(job)

        assert seen == ["tenant-a"]
        assert job.identity.run_id not in active_run_tenants

    @pytest.mark.asyncio
    async def test_job_whose_tenant_is_rejected_fails_the_run_without_executing(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)
        job = self._scoped_job(None)
        active_runs[job.identity.run_id] = MagicMock()
        inner = AsyncMock()
        finalize_scopes: list[DbScope | None] = []

        async def record_finalize(*_args: Any, **kwargs: Any) -> bool:
            finalize_scopes.append(self._scope_or_none())
            assert kwargs["status"] == "error"
            assert "Tenant rejected" in kwargs["error"]
            return True

        with (
            patch.object(tenant_runs_module, "execute_run", inner),
            patch.object(tenant_runs_module, "finalize_run", side_effect=record_finalize) as finalize,
            patch.object(tenant_runs_module, "_signal_run_done", AsyncMock()) as done,
            patch.object(tenant_runs_module.streaming_service, "cleanup_run", AsyncMock()),
        ):
            await execute_run_as_tenant(job)

        inner.assert_not_awaited()
        finalize.assert_awaited_once()
        assert finalize_scopes[0] is not None and finalize_scopes[0].is_system
        done.assert_awaited_once_with(job.identity.run_id)
        assert job.identity.run_id not in active_runs

    @pytest.mark.asyncio
    async def test_configured_resolver_picks_the_run_tenant(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)
        seen: list[DbScope | None] = []

        async def mapped(_user: User) -> str:
            return "tenant-mapped"

        async def record(_job: RunJob) -> None:
            seen.append(self._scope_or_none())

        configure_tenant_resolver(mapped)
        try:
            with patch.object(tenant_runs_module, "execute_run", side_effect=record):
                await execute_run_as_tenant(self._scoped_job("tenant-a"))
        finally:
            configure_tenant_resolver(None)

        assert seen[0] is not None and seen[0].tenant_id == "tenant-mapped"

    @pytest.mark.asyncio
    async def test_no_scope_is_set_when_rls_disabled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", False)
        seen: list[DbScope | None] = []

        async def record(_job: RunJob) -> None:
            seen.append(self._scope_or_none())

        with patch.object(tenant_runs_module, "execute_run", side_effect=record):
            await execute_run_as_tenant(self._scoped_job(None))

        assert seen == [None]
