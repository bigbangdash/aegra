"""TenantRoutingCheckpointer: every method goes to the scoped tenant's saver, nothing else."""

import asyncio
from collections.abc import AsyncIterator, Callable, Iterator, Mapping, Sequence
from typing import Any

import pytest
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    DeltaChannelHistory,
    empty_checkpoint,
)
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from aegra_api.core.tenancy.checkpointer import (
    SystemScopeCheckpointerError,
    TenantRoutingCheckpointer,
    scoped_checkpoint_tenant,
)
from aegra_api.core.tenancy.scope import DbScopeMissingError, system_scope, tenant_scope

CONFIG: RunnableConfig = {"configurable": {"thread_id": "thread-1", "checkpoint_ns": ""}}
CHECKPOINT: Checkpoint = empty_checkpoint()
METADATA: CheckpointMetadata = {"source": "input", "step": -1, "parents": {}}
VERSIONS: ChannelVersions = {"ch": 1}
TUPLE = CheckpointTuple(config=CONFIG, checkpoint=CHECKPOINT, metadata=METADATA, parent_config=None, pending_writes=[])


class RecordingSaver(BaseCheckpointSaver[int]):
    """Records (method, args, kwargs) and returns canned values."""

    def __init__(self, tenant_id: str) -> None:
        super().__init__()
        self.tenant_id = tenant_id
        self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

    def _record(self, name: str, *args: Any, **kwargs: Any) -> None:
        self.calls.append((name, args, kwargs))

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        self._record("aget_tuple", config)
        return TUPLE

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        self._record("alist", config, filter=filter, before=before, limit=limit)
        yield TUPLE

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        self._record("aput", config, checkpoint, metadata, new_versions)
        return config

    async def aput_writes(
        self, config: RunnableConfig, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = ""
    ) -> None:
        self._record("aput_writes", config, writes, task_id, task_path)

    async def adelete_thread(self, thread_id: str) -> None:
        self._record("adelete_thread", thread_id)

    async def adelete_for_runs(self, run_ids: Sequence[str]) -> None:
        self._record("adelete_for_runs", run_ids)

    async def acopy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        self._record("acopy_thread", source_thread_id, target_thread_id)

    async def aprune(self, thread_ids: Sequence[str], *, strategy: str = "keep_latest") -> None:
        self._record("aprune", thread_ids, strategy=strategy)

    async def aget_delta_channel_history(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, DeltaChannelHistory]:
        self._record("aget_delta_channel_history", config=config, channels=channels)
        return {ch: {"writes": []} for ch in channels}

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        self._record("get_tuple", config)
        return TUPLE

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        self._record("list", config, filter=filter, before=before, limit=limit)
        yield TUPLE

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        self._record("put", config, checkpoint, metadata, new_versions)
        return config

    def put_writes(
        self, config: RunnableConfig, writes: Sequence[tuple[str, Any]], task_id: str, task_path: str = ""
    ) -> None:
        self._record("put_writes", config, writes, task_id, task_path)

    def delete_thread(self, thread_id: str) -> None:
        self._record("delete_thread", thread_id)

    def delete_for_runs(self, run_ids: Sequence[str]) -> None:
        self._record("delete_for_runs", run_ids)

    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        self._record("copy_thread", source_thread_id, target_thread_id)

    def prune(self, thread_ids: Sequence[str], *, strategy: str = "keep_latest") -> None:
        self._record("prune", thread_ids, strategy=strategy)

    def get_delta_channel_history(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, DeltaChannelHistory]:
        self._record("get_delta_channel_history", config=config, channels=channels)
        return {ch: {"writes": []} for ch in channels}


class RecordingProvider:
    def __init__(self) -> None:
        self.savers: dict[str, RecordingSaver] = {}
        self.requested: list[str] = []
        self.health_calls = 0

    def _get(self, tenant_id: str) -> RecordingSaver:
        self.requested.append(tenant_id)
        return self.savers.setdefault(tenant_id, RecordingSaver(tenant_id))

    async def for_tenant(self, tenant_id: str) -> BaseCheckpointSaver:
        return self._get(tenant_id)

    def for_tenant_sync(self, tenant_id: str) -> BaseCheckpointSaver:
        return self._get(tenant_id)

    async def health(self) -> None:
        self.health_calls += 1


@pytest.fixture
def provider() -> RecordingProvider:
    return RecordingProvider()


@pytest.fixture
def router(provider: RecordingProvider) -> TenantRoutingCheckpointer:
    return TenantRoutingCheckpointer(provider)


AsyncCall = Callable[[TenantRoutingCheckpointer], Any]

WRITES = [("ch", 1)]


async def _collect_alist(router: TenantRoutingCheckpointer) -> list[CheckpointTuple]:
    return [item async for item in router.alist(CONFIG, filter={"source": "input"}, before=None, limit=3)]


ASYNC_CALLS: dict[str, tuple[AsyncCall, tuple[Any, ...], dict[str, Any]]] = {
    "aget_tuple": (lambda r: r.aget_tuple(CONFIG), (CONFIG,), {}),
    "alist": (_collect_alist, (CONFIG,), {"filter": {"source": "input"}, "before": None, "limit": 3}),
    "aput": (lambda r: r.aput(CONFIG, CHECKPOINT, METADATA, VERSIONS), (CONFIG, CHECKPOINT, METADATA, VERSIONS), {}),
    "aput_writes": (lambda r: r.aput_writes(CONFIG, WRITES, "task-1", "~p"), (CONFIG, WRITES, "task-1", "~p"), {}),
    "adelete_thread": (lambda r: r.adelete_thread("thread-1"), ("thread-1",), {}),
    "adelete_for_runs": (lambda r: r.adelete_for_runs(["run-1"]), (["run-1"],), {}),
    "acopy_thread": (lambda r: r.acopy_thread("thread-1", "thread-2"), ("thread-1", "thread-2"), {}),
    "aprune": (lambda r: r.aprune(["thread-1"], strategy="delete"), (["thread-1"],), {"strategy": "delete"}),
    "aget_delta_channel_history": (
        lambda r: r.aget_delta_channel_history(config=CONFIG, channels=["ch"]),
        (),
        {"config": CONFIG, "channels": ["ch"]},
    ),
}

SYNC_CALLS: dict[str, tuple[Callable[[TenantRoutingCheckpointer], Any], tuple[Any, ...], dict[str, Any]]] = {
    "get_tuple": (lambda r: r.get_tuple(CONFIG), (CONFIG,), {}),
    "list": (
        lambda r: list(r.list(CONFIG, filter=None, before=CONFIG, limit=None)),
        (CONFIG,),
        {"filter": None, "before": CONFIG, "limit": None},
    ),
    "put": (lambda r: r.put(CONFIG, CHECKPOINT, METADATA, VERSIONS), (CONFIG, CHECKPOINT, METADATA, VERSIONS), {}),
    "put_writes": (lambda r: r.put_writes(CONFIG, WRITES, "task-1"), (CONFIG, WRITES, "task-1", ""), {}),
    "delete_thread": (lambda r: r.delete_thread("thread-1"), ("thread-1",), {}),
    "delete_for_runs": (lambda r: r.delete_for_runs(["run-1"]), (["run-1"],), {}),
    "copy_thread": (lambda r: r.copy_thread("thread-1", "thread-2"), ("thread-1", "thread-2"), {}),
    "prune": (lambda r: r.prune(["thread-1"]), (["thread-1"],), {"strategy": "keep_latest"}),
    "get_delta_channel_history": (
        lambda r: r.get_delta_channel_history(config=CONFIG, channels=["ch"]),
        (),
        {"config": CONFIG, "channels": ["ch"]},
    ),
}


def test_every_base_method_is_covered_by_the_tests() -> None:
    public = {
        n for n in dir(BaseCheckpointSaver) if not n.startswith("_") and callable(getattr(BaseCheckpointSaver, n, None))
    }
    # get/aget wrap get_tuple/aget_tuple in the base class; the rest are not data paths.
    untouched = {"get", "aget", "get_next_version", "with_allowlist"}
    assert public - untouched == set(ASYNC_CALLS) | set(SYNC_CALLS)


@pytest.mark.parametrize("name", sorted(ASYNC_CALLS))
async def test_async_methods_delegate_to_the_scoped_tenant(
    router: TenantRoutingCheckpointer, provider: RecordingProvider, name: str
) -> None:
    call, args, kwargs = ASYNC_CALLS[name]

    with tenant_scope("tenant-a"):
        await call(router)

    assert provider.requested == ["tenant-a"]
    assert provider.savers["tenant-a"].calls == [(name, args, kwargs)]


@pytest.mark.parametrize("name", sorted(SYNC_CALLS))
def test_sync_methods_delegate_to_the_scoped_tenant(
    router: TenantRoutingCheckpointer, provider: RecordingProvider, name: str
) -> None:
    call, args, kwargs = SYNC_CALLS[name]

    with tenant_scope("tenant-a"):
        call(router)

    assert provider.requested == ["tenant-a"]
    assert provider.savers["tenant-a"].calls == [(name, args, kwargs)]


@pytest.mark.parametrize("name", sorted(ASYNC_CALLS))
async def test_async_methods_fail_closed_without_a_scope(
    router: TenantRoutingCheckpointer, provider: RecordingProvider, name: str
) -> None:
    call, _, _ = ASYNC_CALLS[name]

    with pytest.raises(DbScopeMissingError):
        await call(router)

    assert provider.requested == []


@pytest.mark.parametrize("name", sorted(SYNC_CALLS))
def test_sync_methods_fail_closed_without_a_scope(
    router: TenantRoutingCheckpointer, provider: RecordingProvider, name: str
) -> None:
    call, _, _ = SYNC_CALLS[name]

    with pytest.raises(DbScopeMissingError):
        call(router)

    assert provider.requested == []


@pytest.mark.parametrize("name", sorted(ASYNC_CALLS))
async def test_async_methods_refuse_the_system_scope(
    router: TenantRoutingCheckpointer, provider: RecordingProvider, name: str
) -> None:
    call, _, _ = ASYNC_CALLS[name]

    with system_scope("ttl sweep"), pytest.raises(SystemScopeCheckpointerError, match="ttl sweep"):
        await call(router)

    assert provider.requested == []


@pytest.mark.parametrize("name", sorted(SYNC_CALLS))
def test_sync_methods_refuse_the_system_scope(
    router: TenantRoutingCheckpointer, provider: RecordingProvider, name: str
) -> None:
    call, _, _ = SYNC_CALLS[name]

    with system_scope("ttl sweep"), pytest.raises(SystemScopeCheckpointerError):
        call(router)

    assert provider.requested == []


async def test_each_tenant_reaches_its_own_saver(
    router: TenantRoutingCheckpointer, provider: RecordingProvider
) -> None:
    with tenant_scope("tenant-a"):
        await router.adelete_thread("thread-1")
    with tenant_scope("tenant-b"):
        await router.adelete_thread("thread-1")

    assert [c[0] for c in provider.savers["tenant-a"].calls] == ["adelete_thread"]
    assert [c[0] for c in provider.savers["tenant-b"].calls] == ["adelete_thread"]


async def test_tenant_is_fixed_before_delegation_even_if_the_saver_hops_threads(
    router: TenantRoutingCheckpointer, provider: RecordingProvider
) -> None:
    seen_in_thread: list[str | None] = []

    class ThreadHoppingSaver(RecordingSaver):
        async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
            # Raw run_in_executor: no context copy, so the scope is gone inside the thread.
            def probe() -> str | None:
                try:
                    return scoped_checkpoint_tenant()
                except DbScopeMissingError:
                    return None

            seen_in_thread.append(await asyncio.get_running_loop().run_in_executor(None, probe))
            return await super().aget_tuple(config)

    provider.savers["tenant-a"] = ThreadHoppingSaver("tenant-a")

    with tenant_scope("tenant-a"):
        await router.aget_tuple(CONFIG)

    assert provider.requested == ["tenant-a"]
    assert seen_in_thread == [None]
    assert provider.savers["tenant-a"].calls == [("aget_tuple", (CONFIG,), {})]


async def test_scope_entered_inside_a_worker_thread_is_used(
    router: TenantRoutingCheckpointer, provider: RecordingProvider
) -> None:
    def sync_node() -> None:
        with tenant_scope("tenant-c"):
            router.put_writes(CONFIG, WRITES, "task-1")

    await asyncio.to_thread(sync_node)

    assert provider.requested == ["tenant-c"]


async def test_alist_resolves_the_tenant_before_the_first_item(
    router: TenantRoutingCheckpointer, provider: RecordingProvider
) -> None:
    with tenant_scope("tenant-a"):
        iterator = router.alist(CONFIG)
        first = await anext(iterator)

    # Leaving the scope after the first item must not re-resolve on the next one.
    assert first is TUPLE
    assert [item async for item in iterator] == []
    assert provider.requested == ["tenant-a"]


async def test_setup_is_a_no_op(router: TenantRoutingCheckpointer, provider: RecordingProvider) -> None:
    await router.setup()

    assert provider.requested == []
    assert provider.savers == {}


async def test_health_is_reachable_through_the_provider(
    router: TenantRoutingCheckpointer, provider: RecordingProvider
) -> None:
    await router.provider.health()

    assert provider.health_calls == 1


def test_versions_and_serializer_match_the_delegates(router: TenantRoutingCheckpointer) -> None:
    delegate = RecordingSaver("tenant-a")

    assert router.get_next_version(None, None) == delegate.get_next_version(None, None) == 1
    assert router.get_next_version(4, None) == delegate.get_next_version(4, None) == 5
    assert isinstance(router.serde, JsonPlusSerializer)
    assert type(router.serde) is type(delegate.serde)


def test_versions_match_the_dynamodb_saver() -> None:
    pytest.importorskip("langgraph_checkpoint_aws")
    from langgraph_checkpoint_aws import DynamoDBSaver

    from aegra_api.core.tenancy.dynamodb import PrunableDynamoDBSaver

    # Both leave get_next_version to the base class; a bump in either must be noticed here.
    assert PrunableDynamoDBSaver.get_next_version is BaseCheckpointSaver.get_next_version
    assert DynamoDBSaver.get_next_version is BaseCheckpointSaver.get_next_version
    assert TenantRoutingCheckpointer.get_next_version is BaseCheckpointSaver.get_next_version
