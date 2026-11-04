"""DynamoDBCheckpointerProvider: one saver per tenant, STS per tenant, no table creation."""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from botocore.exceptions import ClientError

from aegra_api.core.tenancy.checkpointer import (
    TenantCheckpointCredentialsError,
    TenantCheckpointerError,
    TenantCheckpointTableMissingError,
)
from aegra_api.core.tenancy.dynamodb import (
    CREDENTIAL_REFRESH_MARGIN,
    DynamoDBCheckpointerProvider,
    PrunableDynamoDBSaver,
)
from aegra_api.settings import CheckpointSettings

ROLE_ARN = "arn:aws:iam::123456789012:role/aegra-tenant"
REGION = "ap-northeast-1"
T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)


def _client_error(code: str, operation: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, operation)


class FakeClient:
    def __init__(self, aws: "FakeAws", service: str, **kwargs: Any) -> None:
        self.aws = aws
        self.service = service
        self.kwargs = kwargs
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def describe_table(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("describe_table", kwargs))
        table = kwargs["TableName"]
        if table in self.aws.missing_tables:
            raise _client_error("ResourceNotFoundException", "DescribeTable")
        if table in self.aws.denied_tables:
            raise _client_error("AccessDeniedException", "DescribeTable")
        return {"Table": {"TableName": table}}

    def list_tables(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("list_tables", kwargs))
        return {"TableNames": []}

    def assume_role(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("assume_role", kwargs))
        self.aws.assume_role_calls.append(kwargs)
        if self.aws.deny_assume_role:
            raise _client_error("AccessDenied", "AssumeRole")
        n = len(self.aws.assume_role_calls)
        return {
            "Credentials": {
                "AccessKeyId": f"AKIA{n}",
                "SecretAccessKey": f"secret{n}",
                "SessionToken": f"token{n}",
                "Expiration": self.aws.expiration,
            }
        }

    def get_caller_identity(self) -> dict[str, Any]:
        self.calls.append(("get_caller_identity", {}))
        return {"Arn": "arn:aws:sts::123456789012:assumed-role/server/x"}

    def get_bucket_lifecycle_configuration(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("get_bucket_lifecycle_configuration", kwargs))
        return {"Rules": []}

    def put_bucket_lifecycle_configuration(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(("put_bucket_lifecycle_configuration", kwargs))
        return {}


class FakeSession:
    def __init__(self, aws: "FakeAws", **kwargs: Any) -> None:
        self.aws = aws
        self.kwargs = kwargs
        self.clients: list[FakeClient] = []

    def client(self, service: str, **kwargs: Any) -> FakeClient:
        client = FakeClient(self.aws, service, **kwargs)
        self.clients.append(client)
        self.aws.clients.append(client)
        return client


class FakeAws:
    def __init__(self) -> None:
        self.sessions: list[FakeSession] = []
        self.clients: list[FakeClient] = []
        self.assume_role_calls: list[dict[str, Any]] = []
        self.missing_tables: set[str] = set()
        self.denied_tables: set[str] = set()
        self.deny_assume_role = False
        self.expiration = T0 + timedelta(hours=1)

    def session(self, **kwargs: Any) -> FakeSession:
        session = FakeSession(self, **kwargs)
        self.sessions.append(session)
        return session

    def clients_for(self, service: str) -> list[FakeClient]:
        return [c for c in self.clients if c.service == service]


def _aws_settings(**overrides: Any) -> CheckpointSettings:
    values: dict[str, Any] = {
        "AEGRA_CHECKPOINT_BACKEND": "dynamodb",
        "AEGRA_DYNAMODB_REGION": REGION,
        "AEGRA_DYNAMODB_TENANT_ROLE_ARN": ROLE_ARN,
        "AEGRA_DYNAMODB_S3_BUCKET": "aegra-ckpt-offload",
    }
    values.update(overrides)
    return CheckpointSettings(**values)


def _local_settings(**overrides: Any) -> CheckpointSettings:
    values: dict[str, Any] = {
        "AEGRA_CHECKPOINT_BACKEND": "dynamodb",
        "AEGRA_DYNAMODB_REGION": "us-east-1",
        "AEGRA_DYNAMODB_ENDPOINT_URL": "http://localhost:8100",
    }
    values.update(overrides)
    return CheckpointSettings(**values)


class Clock:
    def __init__(self, now: datetime = T0) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


@pytest.fixture
def aws() -> FakeAws:
    return FakeAws()


@pytest.fixture
def clock() -> Clock:
    return Clock()


def _provider(config: CheckpointSettings, aws: FakeAws, clock: Clock) -> DynamoDBCheckpointerProvider:
    return DynamoDBCheckpointerProvider(config, session_factory=aws.session, clock=clock)


def test_requires_the_dynamodb_backend(aws: FakeAws, clock: Clock) -> None:
    with pytest.raises(ValueError, match="AEGRA_CHECKPOINT_BACKEND=dynamodb"):
        _provider(CheckpointSettings(AEGRA_CHECKPOINT_BACKEND="postgres"), aws, clock)


async def test_saver_targets_the_tenant_table_and_is_cached(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_aws_settings(AEGRA_DYNAMODB_TABLE_PREFIX="ckpt-"), aws, clock)

    saver_a = await provider.for_tenant("tenant-a")
    saver_b = await provider.for_tenant("tenant-b")

    assert isinstance(saver_a, PrunableDynamoDBSaver)
    assert saver_a.table_name == "ckpt-tenant-a"
    assert saver_b.table_name == "ckpt-tenant-b"
    assert saver_a is not saver_b
    assert await provider.for_tenant("tenant-a") is saver_a
    assert len(aws.assume_role_calls) == 2
    assert provider.cached_tenants == {"tenant-a", "tenant-b"}


@pytest.mark.parametrize("tenant_id", ["", "a.b", "x" * 65, "tenant a"])
async def test_rejects_invalid_tenant_ids_before_any_aws_call(aws: FakeAws, clock: Clock, tenant_id: str) -> None:
    provider = _provider(_aws_settings(), aws, clock)

    with pytest.raises(ValueError, match="tenant_id must match"):
        await provider.for_tenant(tenant_id)

    assert aws.sessions == []


async def test_assumes_the_tenant_role_with_a_tenant_id_session_tag(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_aws_settings(), aws, clock)

    saver = await provider.for_tenant("tenant-a")

    [call] = aws.assume_role_calls
    assert call["RoleArn"] == ROLE_ARN
    assert call["RoleSessionName"] == "aegra-tenant-a"
    assert call["Tags"] == [{"Key": "tenant_id", "Value": "tenant-a"}]
    tenant_session = aws.sessions[-1]
    assert tenant_session.kwargs == {
        "aws_access_key_id": "AKIA1",
        "aws_secret_access_key": "secret1",
        "aws_session_token": "token1",
        "region_name": REGION,
    }
    assert saver.client in tenant_session.clients
    assert "endpoint_url" not in saver.client.kwargs


async def test_session_name_is_capped_at_64_characters(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_aws_settings(), aws, clock)

    await provider.for_tenant("t" * 64)

    assert len(aws.assume_role_calls[0]["RoleSessionName"]) == 64


async def test_rebuilds_the_saver_shortly_before_the_credentials_expire(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_aws_settings(), aws, clock)
    first = await provider.for_tenant("tenant-a")

    clock.now = aws.expiration - CREDENTIAL_REFRESH_MARGIN - timedelta(seconds=1)
    assert await provider.for_tenant("tenant-a") is first
    assert len(aws.assume_role_calls) == 1

    clock.now = aws.expiration - CREDENTIAL_REFRESH_MARGIN
    aws.expiration = clock.now + timedelta(hours=1)
    second = await provider.for_tenant("tenant-a")

    assert second is not first
    assert len(aws.assume_role_calls) == 2
    assert aws.sessions[-1].kwargs["aws_access_key_id"] == "AKIA2"


async def test_local_endpoint_uses_default_credentials_and_never_refreshes(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_local_settings(), aws, clock)

    saver = await provider.for_tenant("tenant-a")
    clock.now = T0 + timedelta(days=30)

    assert await provider.for_tenant("tenant-a") is saver
    assert aws.clients_for("sts") == []
    assert aws.sessions[0].kwargs == {"region_name": "us-east-1"}
    assert saver.client.kwargs["endpoint_url"] == "http://localhost:8100"
    assert saver.storage.s3_enabled is False


async def test_missing_table_raises_and_is_not_created_or_cached(aws: FakeAws, clock: Clock) -> None:
    aws.missing_tables.add("aegra-ckpt-tenant-a")
    provider = _provider(_aws_settings(), aws, clock)

    with pytest.raises(TenantCheckpointTableMissingError) as exc_info:
        await provider.for_tenant("tenant-a")

    assert exc_info.value.tenant_id == "tenant-a"
    assert exc_info.value.table_name == "aegra-ckpt-tenant-a"
    assert provider.cached_tenants == frozenset()
    operations = {name for client in aws.clients for name, _ in client.calls}
    assert operations == {"assume_role", "describe_table"}


async def test_denied_table_access_raises_a_tenant_checkpointer_error(aws: FakeAws, clock: Clock) -> None:
    aws.denied_tables.add("aegra-ckpt-tenant-a")
    provider = _provider(_aws_settings(), aws, clock)

    with pytest.raises(TenantCheckpointerError, match="cannot access table"):
        await provider.for_tenant("tenant-a")

    assert provider.cached_tenants == frozenset()


async def test_refused_role_raises_a_credentials_error(aws: FakeAws, clock: Clock) -> None:
    aws.deny_assume_role = True
    provider = _provider(_aws_settings(), aws, clock)

    with pytest.raises(TenantCheckpointCredentialsError, match="could not assume the tenant role"):
        await provider.for_tenant("tenant-a")

    assert aws.clients_for("dynamodb") == []
    assert provider.cached_tenants == frozenset()


async def test_concurrent_requests_for_one_tenant_build_a_single_saver(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_aws_settings(), aws, clock)

    savers = await asyncio.gather(*(provider.for_tenant("tenant-a") for _ in range(5)))

    assert len({id(s) for s in savers}) == 1
    assert len(aws.assume_role_calls) == 1


async def test_s3_offload_keys_are_prefixed_with_the_tenant_id(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_aws_settings(AEGRA_DYNAMODB_TTL_SECONDS=3600), aws, clock)

    saver = await provider.for_tenant("tenant-a")

    assert saver.storage.s3_enabled is True
    assert saver.storage.s3_bucket == "aegra-ckpt-offload"
    assert saver.storage.s3_key_prefix == "tenant-a"
    assert saver.storage.ttl_seconds == 3600
    [s3_client] = aws.clients_for("s3")
    assert s3_client in aws.sessions[-1].clients


async def test_version_format_and_serializer_match_across_tenants(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_aws_settings(), aws, clock)

    saver_a = await provider.for_tenant("tenant-a")
    saver_b = await provider.for_tenant("tenant-b")

    assert saver_a.get_next_version(None, None) == saver_b.get_next_version(None, None) == 1
    assert saver_a.get_next_version(7, None) == 8
    assert type(saver_a.serde) is type(saver_b.serde)


async def test_health_probes_the_last_used_tenant_table(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_aws_settings(), aws, clock)
    await provider.for_tenant("tenant-a")
    saver_b = await provider.for_tenant("tenant-b")
    saver_b.client.calls.clear()

    await provider.health()

    assert saver_b.client.calls == [("describe_table", {"TableName": "aegra-ckpt-tenant-b"})]
    assert len(aws.assume_role_calls) == 2


async def test_health_without_tenants_checks_sts_on_aws(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_aws_settings(), aws, clock)

    await provider.health()

    [sts] = aws.clients_for("sts")
    assert sts.calls == [("get_caller_identity", {})]
    assert aws.assume_role_calls == []


async def test_health_without_tenants_lists_tables_on_a_local_endpoint(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_local_settings(), aws, clock)

    await provider.health()

    [dynamodb] = aws.clients_for("dynamodb")
    assert dynamodb.kwargs["endpoint_url"] == "http://localhost:8100"
    assert dynamodb.calls == [("list_tables", {"Limit": 1})]


async def test_health_propagates_backend_errors(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_aws_settings(), aws, clock)
    await provider.for_tenant("tenant-a")
    aws.denied_tables.add("aegra-ckpt-tenant-a")

    with pytest.raises(ClientError):
        await provider.health()


async def test_health_stays_green_when_the_last_tenant_was_decommissioned(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_aws_settings(), aws, clock)
    await provider.for_tenant("tenant-a")
    aws.missing_tables.add("aegra-ckpt-tenant-a")

    await provider.health()

    # The backend answered, so the service is healthy; the stale saver is dropped and re-checked next time.
    assert provider.cached_tenants == frozenset()
    with pytest.raises(TenantCheckpointTableMissingError):
        await provider.for_tenant("tenant-a")
    await provider.health()  # falls back to the backend probe
    assert aws.clients_for("sts")[-1].calls[-1][0] == "get_caller_identity"


class RecordingRepo:
    def __init__(self, checkpoints: list[tuple[str, str]]) -> None:
        self.checkpoints = checkpoints
        self.calls: list[tuple[str, str, list[tuple[str, str]]]] = []

    def get_thread_checkpoint_info(self, thread_id: str) -> list[tuple[str, str]]:
        return list(self.checkpoints)

    def delete_thread_writes(self, thread_id: str, info: list[tuple[str, str]]) -> None:
        self.calls.append(("writes", thread_id, info))

    def delete_thread_checkpoints(self, thread_id: str, info: list[tuple[str, str]]) -> None:
        self.calls.append(("checkpoints", thread_id, info))


async def _saver_with_repo(aws: FakeAws, clock: Clock, checkpoints: list[tuple[str, str]]) -> tuple[Any, RecordingRepo]:
    provider = _provider(_local_settings(), aws, clock)
    saver = await provider.for_tenant("tenant-a")
    repo = RecordingRepo(checkpoints)
    saver.repo = repo
    return saver, repo


class TestPrunableDynamoDBSaver:
    async def test_keep_latest_keeps_the_greatest_checkpoint_per_namespace(self, aws: FakeAws, clock: Clock) -> None:
        saver, repo = await _saver_with_repo(
            aws,
            clock,
            [
                ("", "01-old"),
                ("", "02-mid"),
                ("", "03-new"),
                ("child", "01-only"),
                ("other", "01-a"),
                ("other", "02-b"),
            ],
        )

        await saver.aprune(["thread-1"])

        stale = [("", "01-old"), ("", "02-mid"), ("other", "01-a")]
        assert repo.calls == [("writes", "thread-1", stale), ("checkpoints", "thread-1", stale)]

    async def test_keep_latest_deletes_nothing_when_history_is_compact(self, aws: FakeAws, clock: Clock) -> None:
        saver, repo = await _saver_with_repo(aws, clock, [("", "01-only"), ("child", "01-only")])

        await saver.aprune(["thread-1"], strategy="keep_latest")

        assert repo.calls == []

    async def test_keep_latest_handles_threads_without_checkpoints(self, aws: FakeAws, clock: Clock) -> None:
        saver, repo = await _saver_with_repo(aws, clock, [])

        saver.prune(["thread-1", "thread-2"])

        assert repo.calls == []

    async def test_delete_strategy_removes_every_thread(
        self, aws: FakeAws, clock: Clock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        saver, repo = await _saver_with_repo(aws, clock, [("", "01")])
        deleted: list[str] = []
        monkeypatch.setattr(saver, "delete_thread", deleted.append)

        await saver.aprune(["thread-1", "thread-2"], strategy="delete")

        assert deleted == ["thread-1", "thread-2"]
        assert repo.calls == []

    async def test_unknown_strategy_is_rejected_before_touching_the_table(self, aws: FakeAws, clock: Clock) -> None:
        saver, repo = await _saver_with_repo(aws, clock, [("", "01"), ("", "02")])

        with pytest.raises(ValueError, match="unknown prune strategy"):
            await saver.aprune(["thread-1"], strategy="purge")

        assert repo.calls == []


def test_sync_lookup_builds_and_shares_the_cache_with_the_async_path(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_aws_settings(), aws, clock)

    saver = provider.for_tenant_sync("tenant-a")

    assert saver.table_name == "aegra-ckpt-tenant-a"
    assert provider.for_tenant_sync("tenant-a") is saver
    assert asyncio.run(provider.for_tenant("tenant-a")) is saver
    assert len(aws.assume_role_calls) == 1


def test_sync_lookup_rejects_invalid_tenant_ids(aws: FakeAws, clock: Clock) -> None:
    provider = _provider(_aws_settings(), aws, clock)

    with pytest.raises(ValueError, match="tenant_id must match"):
        provider.for_tenant_sync("bad id")

    assert aws.sessions == []
