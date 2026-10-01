"""Per-tenant checkpoint backend (dev-docs/tenant-dynamodb-checkpoints-proposal.md).

With AEGRA_CHECKPOINT_BACKEND=dynamodb each tenant's LangGraph checkpoints live
in their own DynamoDB table; threads, runs and the store stay in Postgres.
"""

from importlib.util import find_spec

import structlog

from aegra_api.settings import settings

logger = structlog.get_logger(__name__)

# The `dynamodb` extra (langgraph-checkpoint-aws) is optional; nothing here imports it eagerly.
DYNAMODB_EXTRA_AVAILABLE: bool = find_spec("langgraph_checkpoint_aws") is not None


class CheckpointBackendError(RuntimeError):
    """The configured checkpoint backend cannot run in this process."""


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
