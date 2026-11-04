import asyncio
from collections.abc import Iterable
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from langgraph.store.base import GetOp, Item, ListNamespacesOp, MatchCondition, Op, PutOp, Result, SearchOp
from langgraph.store.postgres.aio import AsyncPostgresStore

from aegra_api.core.tenancy.scope import DbScopeMissingError, system_scope, tenant_scope
from aegra_api.core.tenancy.store import TENANT_NAMESPACE_ROOT, TenantScopedPostgresStore, tenant_namespace_prefix

HEAD = (TENANT_NAMESPACE_ROOT, "tenant-a")


def _item(namespace: tuple[str, ...], key: str = "k") -> Item:
    now = datetime.now(UTC)
    return Item(value={"v": 1}, key=key, namespace=namespace, created_at=now, updated_at=now)


class RecordingBackend:
    def __init__(self, results: list[Result] | None = None) -> None:
        self.ops: list[Op] = []
        self.results = results

    async def abatch(self, ops: Iterable[Op]) -> list[Result]:
        ops = list(ops)
        self.ops.extend(ops)
        return self.results if self.results is not None else [None] * len(ops)


@pytest.fixture
def backend(monkeypatch: pytest.MonkeyPatch) -> RecordingBackend:
    recorder = RecordingBackend()

    async def fake_abatch(self: AsyncPostgresStore, ops: Iterable[Op]) -> list[Result]:
        return await recorder.abatch(ops)

    monkeypatch.setattr(AsyncPostgresStore, "abatch", fake_abatch)
    return recorder


@pytest.fixture
async def store() -> TenantScopedPostgresStore:
    # Ops never reach the connection; the backend fixture replaces abatch.
    return TenantScopedPostgresStore(conn=MagicMock())


async def test_put_and_get_are_stored_under_the_tenant_prefix(
    store: TenantScopedPostgresStore, backend: RecordingBackend
) -> None:
    with tenant_scope("tenant-a"):
        await store.aput(("memories",), "pref", {"lang": "ja"})
        await store.aget(("memories",), "pref")

    put, get = backend.ops
    assert isinstance(put, PutOp) and put.namespace == (*HEAD, "memories")
    assert isinstance(get, GetOp) and get.namespace == (*HEAD, "memories")


async def test_results_are_returned_without_the_tenant_prefix(
    store: TenantScopedPostgresStore, backend: RecordingBackend
) -> None:
    backend.results = [_item((*HEAD, "memories"))]

    with tenant_scope("tenant-a"):
        item = await store.aget(("memories",), "k")

    assert item is not None and item.namespace == ("memories",)


async def test_search_prefixes_namespace_and_strips_every_hit(
    store: TenantScopedPostgresStore, backend: RecordingBackend
) -> None:
    backend.results = [[_item((*HEAD, "notes", "x"), "a"), _item((*HEAD, "notes", "y"), "b")]]

    with tenant_scope("tenant-a"):
        hits = await store.asearch(("notes",))

    (op,) = backend.ops
    assert isinstance(op, SearchOp) and op.namespace_prefix == (*HEAD, "notes")
    assert [hit.namespace for hit in hits] == [("notes", "x"), ("notes", "y")]


async def test_empty_search_prefix_is_confined_to_the_tenant(
    store: TenantScopedPostgresStore, backend: RecordingBackend
) -> None:
    backend.results = [[]]

    with tenant_scope("tenant-a"):
        await store.asearch(())

    (op,) = backend.ops
    assert isinstance(op, SearchOp) and op.namespace_prefix == HEAD


async def test_list_namespaces_adds_tenant_prefix_and_shifts_max_depth(
    store: TenantScopedPostgresStore, backend: RecordingBackend
) -> None:
    backend.results = [[(*HEAD, "notes"), (*HEAD, "memories")]]

    with tenant_scope("tenant-a"):
        namespaces = await store.alist_namespaces(suffix=("v1",), max_depth=1)

    (op,) = backend.ops
    assert isinstance(op, ListNamespacesOp)
    assert op.match_conditions == (
        MatchCondition(match_type="prefix", path=HEAD),
        MatchCondition(match_type="suffix", path=("v1",)),
    )
    assert op.max_depth == 1 + len(HEAD)
    assert namespaces == [("notes",), ("memories",)]


async def test_list_namespaces_prefix_condition_is_nested_under_tenant(
    store: TenantScopedPostgresStore, backend: RecordingBackend
) -> None:
    backend.results = [[]]

    with tenant_scope("tenant-a"):
        await store.alist_namespaces(prefix=("users", "*"))

    (op,) = backend.ops
    assert isinstance(op, ListNamespacesOp)
    assert op.match_conditions == (MatchCondition(match_type="prefix", path=(*HEAD, "users", "*")),)
    assert op.max_depth is None


async def test_result_outside_the_tenant_prefix_raises(
    store: TenantScopedPostgresStore, backend: RecordingBackend
) -> None:
    backend.results = [_item((TENANT_NAMESPACE_ROOT, "tenant-b", "memories"))]

    with pytest.raises(RuntimeError), tenant_scope("tenant-a"):
        await store.aget(("memories",), "k")


async def test_system_scope_passes_namespaces_through_unchanged(
    store: TenantScopedPostgresStore, backend: RecordingBackend
) -> None:
    with system_scope("test: health probe"):
        await store.aget(("health",), "check")

    (op,) = backend.ops
    assert isinstance(op, GetOp) and op.namespace == ("health",)


async def test_missing_scope_fails_before_any_op_runs(
    store: TenantScopedPostgresStore, backend: RecordingBackend
) -> None:
    with pytest.raises(DbScopeMissingError):
        await store.aget(("memories",), "k")

    assert backend.ops == []


async def test_sync_call_from_worker_thread_keeps_caller_tenant(
    store: TenantScopedPostgresStore, backend: RecordingBackend
) -> None:
    def sync_node() -> None:
        with tenant_scope("tenant-a"):
            store.put(("memories",), "pref", {"lang": "ja"})

    await asyncio.to_thread(sync_node)

    (op,) = backend.ops
    assert isinstance(op, PutOp) and op.namespace == (*HEAD, "memories")


def test_tenant_id_with_dot_is_rejected() -> None:
    with pytest.raises(ValueError):
        tenant_namespace_prefix("org.a")


def test_tenant_prefix_is_root_then_tenant_id() -> None:
    assert tenant_namespace_prefix("org_a") == (TENANT_NAMESPACE_ROOT, "org_a")
