"""Per-tenant checkpoint backend (dev-docs/tenant-dynamodb-checkpoints-proposal.md).

With AEGRA_CHECKPOINT_BACKEND=dynamodb each tenant's LangGraph checkpoints live
in their own DynamoDB table; threads, runs and the store stay in Postgres.
"""

from importlib.util import find_spec
from typing import Protocol

import structlog
from langgraph.checkpoint.base import BaseCheckpointSaver

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


class TenantCheckpointerProvider(Protocol):
    """Internal seam between the routing checkpointer and a backend; not a public hook."""

    async def for_tenant(self, tenant_id: str) -> BaseCheckpointSaver: ...

    async def health(self) -> None: ...


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
