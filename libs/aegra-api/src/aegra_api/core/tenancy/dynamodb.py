"""Per-tenant DynamoDB checkpoint savers (proposal §4.1 and §6.1).

Imports the optional `dynamodb` extra at the top, so this module is imported
only after core.tenancy.checkpointer confirmed the extra is installed.

Every tenant gets one DynamoDBSaver on its own table and its own credentials:
the configured role is assumed with a `tenant_id` session tag, so IAM can pin
the session to `{prefix}${aws:PrincipalTag/tenant_id}`. With a Local endpoint
the default credentials are used and nothing expires.
"""

import asyncio
import threading
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
import structlog
from botocore.exceptions import BotoCoreError, ClientError
from langchain_core.runnables import run_in_executor
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph_checkpoint_aws import DynamoDBSaver

from aegra_api.core.tenancy.checkpointer import (
    TenantCheckpointCredentialsError,
    TenantCheckpointerError,
    TenantCheckpointTableMissingError,
)
from aegra_api.core.tenancy.scope import TENANT_ID_PATTERN, is_valid_tenant_id
from aegra_api.settings import CheckpointSettings

logger = structlog.get_logger(__name__)

# Rebuild a tenant's saver this long before its STS credentials expire.
CREDENTIAL_REFRESH_MARGIN = timedelta(minutes=5)
STS_SESSION_DURATION_SECONDS = 3600
TENANT_SESSION_TAG = "tenant_id"
PRUNE_STRATEGIES: tuple[str, ...] = ("keep_latest", "delete")


def _utcnow() -> datetime:
    return datetime.now(UTC)


class PrunableDynamoDBSaver(DynamoDBSaver):
    """DynamoDBSaver plus prune(): the Aegra TTL sweeper needs keep_latest (proposal §4.2)."""

    def prune(self, thread_ids: Sequence[str], *, strategy: str = "keep_latest") -> None:
        if strategy not in PRUNE_STRATEGIES:
            raise ValueError(f"unknown prune strategy {strategy!r}; expected one of {PRUNE_STRATEGIES}")
        for thread_id in thread_ids:
            if strategy == "delete":
                self.delete_thread(thread_id)
                continue
            self._keep_latest(thread_id)

    async def aprune(self, thread_ids: Sequence[str], *, strategy: str = "keep_latest") -> None:
        await run_in_executor(None, self.prune, thread_ids, strategy=strategy)

    def _keep_latest(self, thread_id: str) -> None:
        # Same rule as the saver's own "latest" lookup: the greatest checkpoint_id per namespace.
        checkpoints = self.repo.get_thread_checkpoint_info(thread_id)
        latest: dict[str, str] = {}
        for checkpoint_ns, checkpoint_id in checkpoints:
            if checkpoint_id > latest.get(checkpoint_ns, ""):
                latest[checkpoint_ns] = checkpoint_id
        stale = [(ns, cid) for ns, cid in checkpoints if cid != latest[ns]]
        if not stale:
            return
        self.repo.delete_thread_writes(thread_id, stale)
        self.repo.delete_thread_checkpoints(thread_id, stale)


@dataclass
class _TenantSaver:
    saver: PrunableDynamoDBSaver
    # None: default credentials (Local endpoint), nothing to refresh.
    expires_at: datetime | None


def _session_name(tenant_id: str) -> str:
    # RoleSessionName allows [\w+=,.@-]{2,64}; tenant ids are a subset of that alphabet.
    return f"aegra-{tenant_id}"[:64]


class DynamoDBCheckpointerProvider:
    """Builds and caches one saver per tenant; never creates tables."""

    def __init__(
        self,
        config: CheckpointSettings,
        *,
        session_factory: Callable[..., boto3.Session] = boto3.Session,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        if not config.dynamodb_enabled:
            raise ValueError("DynamoDBCheckpointerProvider needs AEGRA_CHECKPOINT_BACKEND=dynamodb")
        self._config = config
        self._session_factory = session_factory
        self._clock = clock
        self._savers: dict[str, _TenantSaver] = {}
        # A thread lock: the sync path serves Pregel's sync methods from worker threads.
        self._lock = threading.Lock()
        self._last_tenant_id: str | None = None

    def table_name(self, tenant_id: str) -> str:
        return f"{self._config.AEGRA_DYNAMODB_TABLE_PREFIX}{tenant_id}"

    @property
    def cached_tenants(self) -> frozenset[str]:
        return frozenset(self._savers)

    async def for_tenant(self, tenant_id: str) -> BaseCheckpointSaver:
        cached = self._cached(tenant_id)
        if cached is not None:
            return cached
        return await asyncio.to_thread(self.for_tenant_sync, tenant_id)

    def for_tenant_sync(self, tenant_id: str) -> BaseCheckpointSaver:
        cached = self._cached(tenant_id)
        if cached is not None:
            return cached
        with self._lock:
            entry = self._savers.get(tenant_id)
            if entry is None or self._needs_refresh(entry):
                entry = self._build(tenant_id)
                self._savers[tenant_id] = entry
        self._last_tenant_id = tenant_id
        return entry.saver

    def _cached(self, tenant_id: str) -> BaseCheckpointSaver | None:
        if not is_valid_tenant_id(tenant_id):
            raise ValueError(f"tenant_id must match {TENANT_ID_PATTERN.pattern}: {tenant_id!r}")
        entry = self._savers.get(tenant_id)
        if entry is None or self._needs_refresh(entry):
            return None
        self._last_tenant_id = tenant_id
        return entry.saver

    async def health(self) -> None:
        """One call with real credentials; never walks every tenant's table (proposal §6.1)."""
        entry = self._savers.get(self._last_tenant_id) if self._last_tenant_id else None
        if entry is not None:
            await asyncio.to_thread(entry.saver.client.describe_table, TableName=entry.saver.table_name)
            return
        session = self._session_factory(region_name=self._config.AEGRA_DYNAMODB_REGION)
        endpoint_url = self._config.AEGRA_DYNAMODB_ENDPOINT_URL
        if endpoint_url:
            await asyncio.to_thread(session.client("dynamodb", endpoint_url=endpoint_url).list_tables, Limit=1)
            return
        await asyncio.to_thread(session.client("sts").get_caller_identity)

    def _needs_refresh(self, entry: _TenantSaver) -> bool:
        if entry.expires_at is None:
            return False
        return self._clock() >= entry.expires_at - CREDENTIAL_REFRESH_MARGIN

    def _build(self, tenant_id: str) -> _TenantSaver:
        # Blocking boto3 calls (STS, DescribeTable); the async path runs it in a thread.
        session, expires_at = self._session_for(tenant_id)
        saver = PrunableDynamoDBSaver(
            table_name=self.table_name(tenant_id),
            session=session,
            region_name=self._config.AEGRA_DYNAMODB_REGION,
            endpoint_url=self._config.AEGRA_DYNAMODB_ENDPOINT_URL,
            ttl_seconds=self._config.AEGRA_DYNAMODB_TTL_SECONDS,
            s3_offload_config=self._s3_offload_config(tenant_id),
        )
        self._require_table(saver, tenant_id)
        logger.info("Tenant checkpoint saver ready", tenant_id=tenant_id, table=saver.table_name)
        return _TenantSaver(saver=saver, expires_at=expires_at)

    def _session_for(self, tenant_id: str) -> tuple[boto3.Session, datetime | None]:
        region = self._config.AEGRA_DYNAMODB_REGION
        if self._config.AEGRA_DYNAMODB_ENDPOINT_URL:
            return self._session_factory(region_name=region), None
        sts = self._session_factory(region_name=region).client("sts")
        try:
            response = sts.assume_role(
                RoleArn=self._config.AEGRA_DYNAMODB_TENANT_ROLE_ARN,
                RoleSessionName=_session_name(tenant_id),
                DurationSeconds=STS_SESSION_DURATION_SECONDS,
                Tags=[{"Key": TENANT_SESSION_TAG, "Value": tenant_id}],
            )
        except (ClientError, BotoCoreError) as e:
            raise TenantCheckpointCredentialsError(tenant_id, f"could not assume the tenant role: {e}") from e
        credentials = response["Credentials"]
        session = self._session_factory(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=region,
        )
        return session, credentials["Expiration"]

    def _s3_offload_config(self, tenant_id: str) -> Any:
        bucket = self._config.AEGRA_DYNAMODB_S3_BUCKET
        if not bucket:
            return None
        # Keys start with the tenant id so the role policy can be limited to that prefix.
        return {"bucket_name": bucket, "key_prefix": tenant_id}

    @staticmethod
    def _require_table(saver: PrunableDynamoDBSaver, tenant_id: str) -> None:
        try:
            saver.client.describe_table(TableName=saver.table_name)
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") == "ResourceNotFoundException":
                raise TenantCheckpointTableMissingError(tenant_id, saver.table_name) from e
            raise TenantCheckpointerError(tenant_id, f"cannot access table {saver.table_name!r}: {e}") from e
