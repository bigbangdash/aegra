"""Per-tenant checkpoint backend (dev-docs/tenant-dynamodb-checkpoints-proposal.md).

With AEGRA_CHECKPOINT_BACKEND=dynamodb each tenant's LangGraph checkpoints live
in their own DynamoDB table; threads, runs and the store stay in Postgres.
TenantRoutingCheckpointer reads the DB scope and hands every call to that
tenant's saver; it never picks a tenant itself and has no system-wide view.
"""

from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from importlib.util import find_spec
from typing import Any, Protocol

import structlog
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    DeltaChannelHistory,
)

from aegra_api.core.tenancy.scope import current_db_scope
from aegra_api.settings import settings

logger = structlog.get_logger(__name__)

# The `dynamodb` extra (langgraph-checkpoint-aws) is optional; nothing here imports it eagerly.
DYNAMODB_EXTRA_AVAILABLE: bool = find_spec("langgraph_checkpoint_aws") is not None


class CheckpointBackendError(RuntimeError):
    """The configured checkpoint backend cannot run in this process."""


class TenantCheckpointerError(RuntimeError):
    """A tenant has no usable checkpoint saver (not provisioned or credentials refused)."""

    def __init__(self, tenant_id: str, detail: str) -> None:
        self.tenant_id = tenant_id
        super().__init__(f"tenant {tenant_id!r}: {detail}")


class TenantCheckpointTableMissingError(TenantCheckpointerError):
    """The tenant's table does not exist; the server never creates it (proposal §4.1)."""

    def __init__(self, tenant_id: str, table_name: str) -> None:
        self.table_name = table_name
        super().__init__(tenant_id, f"checkpoint table {table_name!r} does not exist")


class TenantCheckpointCredentialsError(TenantCheckpointerError):
    """Credentials for the tenant could not be obtained (STS refused the role)."""


class SystemScopeCheckpointerError(RuntimeError):
    """Checkpoints have no cross-tenant view: system-scoped code must enter tenant_scope() first."""


class TenantCheckpointerProvider(Protocol):
    """Internal seam between the routing checkpointer and a backend; not a public hook."""

    async def for_tenant(self, tenant_id: str) -> BaseCheckpointSaver: ...

    def for_tenant_sync(self, tenant_id: str) -> BaseCheckpointSaver: ...

    async def health(self) -> None: ...


def scoped_checkpoint_tenant() -> str:
    """Tenant of the current DB scope; fails closed without a scope and refuses the system scope."""
    scope = current_db_scope()
    if scope.tenant_id is None:
        raise SystemScopeCheckpointerError(
            f"checkpoints cannot be accessed in system scope ({scope.system_reason!r}); "
            "enter tenant_scope() for one tenant at a time"
        )
    return scope.tenant_id


class TenantRoutingCheckpointer(BaseCheckpointSaver[int]):
    """Delegates every BaseCheckpointSaver method to the current tenant's saver (proposal §4.2).

    The scope is read before delegating, so a saver that hops threads cannot
    change which tenant was chosen. Versions and the serializer are the base
    class defaults, the same ones the delegates use.
    """

    def __init__(self, provider: TenantCheckpointerProvider) -> None:
        super().__init__()
        self._provider = provider

    @property
    def provider(self) -> TenantCheckpointerProvider:
        return self._provider

    async def setup(self) -> None:
        # Tables are provisioned per tenant outside the server (proposal §6.2).
        return None

    async def _saver(self) -> BaseCheckpointSaver:
        return await self._provider.for_tenant(scoped_checkpoint_tenant())

    def _saver_sync(self) -> BaseCheckpointSaver:
        return self._provider.for_tenant_sync(scoped_checkpoint_tenant())

    # -- async ------------------------------------------------------------------

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        saver = await self._saver()
        return await saver.aget_tuple(config)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        saver = await self._saver()
        async for item in saver.alist(config, filter=filter, before=before, limit=limit):
            yield item

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        saver = await self._saver()
        return await saver.aput(config, checkpoint, metadata, new_versions)

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        saver = await self._saver()
        await saver.aput_writes(config, writes, task_id, task_path)

    async def adelete_thread(self, thread_id: str) -> None:
        saver = await self._saver()
        await saver.adelete_thread(thread_id)

    async def adelete_for_runs(self, run_ids: Sequence[str]) -> None:
        saver = await self._saver()
        await saver.adelete_for_runs(run_ids)

    async def acopy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        saver = await self._saver()
        await saver.acopy_thread(source_thread_id, target_thread_id)

    async def aprune(self, thread_ids: Sequence[str], *, strategy: str = "keep_latest") -> None:
        saver = await self._saver()
        await saver.aprune(thread_ids, strategy=strategy)

    async def aget_delta_channel_history(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, DeltaChannelHistory]:
        saver = await self._saver()
        return await saver.aget_delta_channel_history(config=config, channels=channels)

    # -- sync -------------------------------------------------------------------

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        return self._saver_sync().get_tuple(config)

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        saver = self._saver_sync()
        yield from saver.list(config, filter=filter, before=before, limit=limit)

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        return self._saver_sync().put(config, checkpoint, metadata, new_versions)

    def put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        self._saver_sync().put_writes(config, writes, task_id, task_path)

    def delete_thread(self, thread_id: str) -> None:
        self._saver_sync().delete_thread(thread_id)

    def delete_for_runs(self, run_ids: Sequence[str]) -> None:
        self._saver_sync().delete_for_runs(run_ids)

    def copy_thread(self, source_thread_id: str, target_thread_id: str) -> None:
        self._saver_sync().copy_thread(source_thread_id, target_thread_id)

    def prune(self, thread_ids: Sequence[str], *, strategy: str = "keep_latest") -> None:
        self._saver_sync().prune(thread_ids, strategy=strategy)

    def get_delta_channel_history(
        self, *, config: RunnableConfig, channels: Sequence[str]
    ) -> Mapping[str, DeltaChannelHistory]:
        return self._saver_sync().get_delta_channel_history(config=config, channels=channels)


def ensure_checkpoint_backend_available() -> None:
    """Startup guard: refuse to boot with a checkpoint backend that cannot route."""
    if not settings.checkpoint.dynamodb_enabled:
        return
    if not settings.tenant.AEGRA_TENANT_RLS_ENABLED:
        raise CheckpointBackendError(
            "AEGRA_CHECKPOINT_BACKEND=dynamodb needs AEGRA_TENANT_RLS_ENABLED=true: "
            "checkpoints are routed by the request's tenant"
        )
    if not DYNAMODB_EXTRA_AVAILABLE:
        raise CheckpointBackendError(
            "AEGRA_CHECKPOINT_BACKEND=dynamodb needs the optional dependency: pip install 'aegra-api[dynamodb]'"
        )
    logger.info(
        "Checkpoint backend: per-tenant DynamoDB tables",
        table_prefix=settings.checkpoint.AEGRA_DYNAMODB_TABLE_PREFIX,
        region=settings.checkpoint.AEGRA_DYNAMODB_REGION,
        endpoint_url=settings.checkpoint.AEGRA_DYNAMODB_ENDPOINT_URL,
    )
