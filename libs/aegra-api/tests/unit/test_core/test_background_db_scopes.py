"""Background loops declare a system DB scope; nothing inherits one by accident."""

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any
from unittest.mock import patch

import pytest

from aegra_api.core.db_scope import DbScope, DbScopeMissingError, current_db_scope
from aegra_api.core.health import _probe_db_scope
from aegra_api.services.lease_reaper import LeaseReaper
from aegra_api.services.thread_ttl import ThreadTTLSweeper
from aegra_api.services.worker_executor import WorkerExecutor
from aegra_api.settings import settings


def _scope_or_none() -> DbScope | None:
    try:
        return current_db_scope()
    except DbScopeMissingError:
        return None


def _recorder(seen: list[DbScope | None]) -> Callable[..., Coroutine[Any, Any, None]]:
    async def record(*_args: Any) -> None:
        seen.append(_scope_or_none())

    return record


def _assert_single_system_scope(seen: list[DbScope | None]) -> None:
    assert len(seen) == 1
    assert seen[0] is not None and seen[0].is_system
    assert seen[0].system_reason
    assert _scope_or_none() is None


async def test_lease_reaper_loop_runs_in_system_scope() -> None:
    reaper = LeaseReaper()
    seen: list[DbScope | None] = []

    with patch.object(reaper, "_loop", side_effect=_recorder(seen)):
        await reaper.start()
        await asyncio.sleep(0)
        await reaper.stop()

    _assert_single_system_scope(seen)


async def test_thread_ttl_sweeper_loop_runs_in_system_scope() -> None:
    sweeper = ThreadTTLSweeper()
    seen: list[DbScope | None] = []

    with patch.object(sweeper, "_loop", side_effect=_recorder(seen)):
        await sweeper.start()
        await asyncio.sleep(0)
        await sweeper.stop()

    _assert_single_system_scope(seen)


async def test_worker_loops_run_in_system_scope(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.worker, "WORKER_COUNT", 1)
    worker = WorkerExecutor()
    seen: list[DbScope | None] = []

    with patch.object(worker, "_worker_loop", side_effect=_recorder(seen)):
        await worker.start()
        await asyncio.sleep(0)
        await worker.stop()

    _assert_single_system_scope(seen)


async def test_health_probes_run_in_system_scope() -> None:
    dependency = _probe_db_scope()

    await anext(dependency)
    inside = _scope_or_none()
    await dependency.aclose()

    assert inside is not None and inside.is_system
    assert _scope_or_none() is None
