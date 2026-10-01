"""Ephemeral-thread cleanup inherits the caller's tenant scope (proposal §4.3)."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

from aegra_api.core.tenancy.scope import DbScope, DbScopeMissingError, current_db_scope, tenant_scope
from aegra_api.services import run_cleanup


def _scope_or_none() -> DbScope | None:
    try:
        return current_db_scope()
    except DbScopeMissingError:
        return None


async def test_background_cleanup_task_runs_in_the_scheduling_tenant_scope() -> None:
    seen: list[DbScope | None] = []

    async def record(_run_id: str, _thread_id: str, _user_id: str) -> None:
        seen.append(_scope_or_none())

    with patch.object(run_cleanup, "cleanup_after_background_run", record), tenant_scope("tenant-a"):
        task = run_cleanup.schedule_background_cleanup("run-1", "thread-1", "user-1")
    await task

    assert seen == [DbScope(tenant_id="tenant-a")]
    assert _scope_or_none() is None


async def test_background_cleanup_without_a_scope_stays_unscoped() -> None:
    seen: list[DbScope | None] = []

    async def record(_run_id: str, _thread_id: str, _user_id: str) -> None:
        seen.append(_scope_or_none())

    with patch.object(run_cleanup, "cleanup_after_background_run", record):
        await run_cleanup.schedule_background_cleanup("run-1", "thread-1", "user-1")

    assert seen == [None]


async def test_delete_thread_by_id_deletes_checkpoints_in_the_caller_scope() -> None:
    seen: list[DbScope | None] = []

    async def adelete_thread(_thread_id: str) -> None:
        seen.append(_scope_or_none())

    checkpointer = MagicMock()
    checkpointer.adelete_thread = AsyncMock(side_effect=adelete_thread)
    db = MagicMock()
    db.get_checkpointer.return_value = checkpointer

    session = AsyncMock()
    scalars: Any = MagicMock()
    scalars.all.return_value = []
    session.scalars.return_value = scalars
    session.scalar.return_value = MagicMock(thread_id="thread-1")
    maker = MagicMock()
    maker.return_value.__aenter__ = AsyncMock(return_value=session)
    maker.return_value.__aexit__ = AsyncMock(return_value=False)

    with (
        patch.object(run_cleanup, "_get_session_maker", return_value=maker),
        patch.object(run_cleanup, "db_manager", db),
        tenant_scope("tenant-b"),
    ):
        await asyncio.wait_for(run_cleanup.delete_thread_by_id("thread-1", "user-1"), timeout=5)

    assert seen == [DbScope(tenant_id="tenant-b")]
    session.delete.assert_awaited_once()
