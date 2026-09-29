"""RedisRunBroker with tenant RLS on: payloads are sealed per tenant and opened only under that tenant."""

import json
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from aegra_api.core import tenant_crypto
from aegra_api.core.db_scope import DbScopeMissingError, system_scope, tenant_scope
from aegra_api.core.tenant_crypto import StaticKeyProvider, TenantPayloadError
from aegra_api.services.redis_broker import RedisBrokerManager, RedisRunBroker
from aegra_api.settings import settings

RUN_ID = "run-123"


class FakePipeline:
    def __init__(self, store: dict[str, list[str]]) -> None:
        self._store = store
        self._ops: list[tuple[str, str]] = []

    def rpush(self, key: str, value: str) -> None:
        self._ops.append((key, value))

    def ltrim(self, key: str, start: int, end: int) -> None:
        return None

    def expire(self, key: str, seconds: int) -> None:
        return None

    def incr(self, key: str) -> None:
        return None

    async def execute(self) -> None:
        for key, value in self._ops:
            self._store.setdefault(key, []).append(value)


class FakeRedis:
    def __init__(self) -> None:
        self.lists: dict[str, list[str]] = {}
        self.published: list[tuple[str, str]] = []

    def pipeline(self) -> FakePipeline:
        return FakePipeline(self.lists)

    async def publish(self, channel: str, message: str) -> None:
        self.published.append((channel, message))

    async def lrange(self, key: str, start: int, end: int) -> list[str]:
        items = self.lists.get(key, [])
        if start == -1 and end == -1:
            return items[-1:]
        return items[start : end + 1]


@pytest.fixture(autouse=True)
def rls_on(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)
    tenant_crypto.configure_key_provider(StaticKeyProvider(bytes(range(32))))
    yield
    tenant_crypto.configure_key_provider(None)


@pytest.fixture
def redis() -> Iterator[FakeRedis]:
    fake = FakeRedis()
    with patch("aegra_api.services.redis_broker.redis_manager") as manager:
        manager.get_client.return_value = fake
        yield fake


def _broker() -> RedisRunBroker:
    return RedisRunBroker(RUN_ID, f"aegra:run:{RUN_ID}", f"aegra:run:cache:{RUN_ID}", f"aegra:run:counter:{RUN_ID}")


def _cached(redis: FakeRedis) -> list[dict[str, Any]]:
    return [json.loads(raw) for raw in redis.lists[f"aegra:run:cache:{RUN_ID}"]]


@pytest.mark.asyncio
async def test_put_stores_and_publishes_only_sealed_payloads(redis: FakeRedis) -> None:
    with tenant_scope("tenant-a"):
        await _broker().put("evt-1", ("values", {"note": "customer secret"}))

    for raw in [*redis.lists[f"aegra:run:cache:{RUN_ID}"], *(m for _, m in redis.published)]:
        assert "customer secret" not in raw
        assert "values" not in raw
        message = json.loads(raw)
        assert set(message) == {"event_id", "end", "sealed"}
        assert message["end"] is False


@pytest.mark.asyncio
async def test_replay_under_the_writing_tenant_returns_payloads(redis: FakeRedis) -> None:
    broker = _broker()
    with tenant_scope("tenant-a"):
        await broker.put("evt-1", ("values", {"n": 1}))
        await broker.put("evt-2", ("end", {"status": "success"}))

        events = await broker.replay(None)

    assert events == [("evt-1", ("values", {"n": 1})), ("evt-2", ("end", {"status": "success"}))]
    assert _cached(redis)[-1]["end"] is True


@pytest.mark.asyncio
async def test_replay_under_another_tenant_fails_to_decrypt(redis: FakeRedis) -> None:
    broker = _broker()
    with tenant_scope("tenant-a"):
        await broker.put("evt-1", ("values", {"n": 1}))

    with tenant_scope("tenant-b"), pytest.raises(TenantPayloadError, match="does not belong to tenant tenant-b"):
        await broker.replay(None)


@pytest.mark.asyncio
async def test_replay_refuses_system_scope(redis: FakeRedis) -> None:
    broker = _broker()
    with tenant_scope("tenant-a"):
        await broker.put("evt-1", ("values", {"n": 1}))

    with system_scope("test"), pytest.raises(TenantPayloadError, match="system scope"):
        await broker.replay(None)


@pytest.mark.asyncio
async def test_put_without_a_scope_fails_closed(redis: FakeRedis) -> None:
    with pytest.raises(DbScopeMissingError):
        await _broker().put("evt-1", ("values", {"n": 1}))

    assert redis.lists == {}
    assert redis.published == []


@pytest.mark.asyncio
async def test_unsealed_message_is_refused_while_rls_is_on(redis: FakeRedis) -> None:
    redis.lists[f"aegra:run:cache:{RUN_ID}"] = [json.dumps({"event_id": "evt-1", "payload": ["values", {}]})]

    with tenant_scope("tenant-a"), pytest.raises(TenantPayloadError, match="unsealed"):
        await _broker().replay(None)


@pytest.mark.asyncio
async def test_end_in_buffer_is_detected_without_a_key(redis: FakeRedis) -> None:
    with tenant_scope("tenant-a"):
        await _broker().put("evt-1", ("end", {"status": "success"}))
    tenant_crypto.configure_key_provider(MagicMock(get_key=AsyncMock(side_effect=AssertionError("no key needed"))))

    reader = _broker()
    assert await reader._check_end_in_buffer() is True
    assert reader.is_finished()


@pytest.mark.asyncio
async def test_cancel_listener_writes_end_event_under_the_runs_tenant(redis: FakeRedis) -> None:
    manager = RedisBrokerManager()
    task = MagicMock()
    task.done.return_value = False
    broker = _broker()

    with (
        patch.dict("aegra_api.core.active_runs.active_runs", {RUN_ID: task}, clear=True),
        patch.dict("aegra_api.services.redis_broker.active_run_tenants", {RUN_ID: "tenant-a"}, clear=True),
        patch("aegra_api.services.redis_broker.explicit_run_cancellations", set()),
        patch.object(manager, "get_or_create_broker", return_value=broker),
        patch.object(manager, "allocate_event_id", new_callable=AsyncMock, return_value="evt-9"),
    ):
        await manager._execute_cancel(RUN_ID)

    task.cancel.assert_called_once()
    with tenant_scope("tenant-a"):
        assert await _broker().replay(None) == [("evt-9", ("end", {"status": "interrupted"}))]


@pytest.mark.asyncio
async def test_cancel_listener_skips_end_event_when_run_tenant_is_unknown(redis: FakeRedis) -> None:
    manager = RedisBrokerManager()
    task = MagicMock()
    task.done.return_value = False

    with (
        patch.dict("aegra_api.core.active_runs.active_runs", {RUN_ID: task}, clear=True),
        patch.dict("aegra_api.services.redis_broker.active_run_tenants", {}, clear=True),
        patch("aegra_api.services.redis_broker.explicit_run_cancellations", set()),
        patch.object(manager, "get_or_create_broker", return_value=_broker()),
        patch.object(manager, "allocate_event_id", new_callable=AsyncMock, return_value="evt-9"),
    ):
        await manager._execute_cancel(RUN_ID)

    task.cancel.assert_called_once()
    assert redis.lists == {}
