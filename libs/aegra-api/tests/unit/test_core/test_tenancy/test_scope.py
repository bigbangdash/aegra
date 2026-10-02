import asyncio

import pytest

from aegra_api.core.tenancy.scope import (
    DbScope,
    DbScopeMissingError,
    bind_scope,
    current_db_scope,
    system_scope,
    tenant_scope,
)


def test_current_db_scope_raises_when_no_scope_declared() -> None:
    with pytest.raises(DbScopeMissingError):
        current_db_scope()


def test_tenant_scope_exposes_tenant_and_is_not_system() -> None:
    with tenant_scope("tenant-a"):
        scope = current_db_scope()

    assert scope.tenant_id == "tenant-a"
    assert scope.is_system is False


def test_system_scope_records_reason_and_is_system() -> None:
    with system_scope("ttl sweeper"):
        scope = current_db_scope()

    assert scope.is_system is True
    assert scope.system_reason == "ttl sweeper"


def test_scope_is_cleared_after_block_exits() -> None:
    with tenant_scope("tenant-a"):
        pass

    with pytest.raises(DbScopeMissingError):
        current_db_scope()


def test_nested_scope_restores_outer_scope() -> None:
    with tenant_scope("tenant-a"):
        with system_scope("nested maintenance"):
            assert current_db_scope().is_system is True
        outer = current_db_scope()

    assert outer.tenant_id == "tenant-a"


def test_scope_is_cleared_when_block_raises() -> None:
    with pytest.raises(RuntimeError), tenant_scope("tenant-a"):
        raise RuntimeError("boom")

    with pytest.raises(DbScopeMissingError):
        current_db_scope()


@pytest.mark.parametrize("tenant_id", ["", None])
def test_tenant_scope_rejects_empty_tenant(tenant_id: str | None) -> None:
    with pytest.raises(ValueError), tenant_scope(tenant_id):  # type: ignore[arg-type]  # invalid input on purpose
        pass


def test_system_scope_rejects_empty_reason() -> None:
    with pytest.raises(ValueError), system_scope(""):
        pass


async def test_concurrent_tasks_do_not_share_scope() -> None:
    seen: dict[str, str | None] = {}

    async def worker(tenant_id: str) -> None:
        with tenant_scope(tenant_id):
            await asyncio.sleep(0)
            seen[tenant_id] = current_db_scope().tenant_id

    await asyncio.gather(worker("tenant-a"), worker("tenant-b"))

    assert seen == {"tenant-a": "tenant-a", "tenant-b": "tenant-b"}


def test_bind_scope_reenters_a_captured_scope_and_restores_previous() -> None:
    captured = DbScope(tenant_id="tenant-a")

    with system_scope("test: outer"), bind_scope(captured):
        inner = current_db_scope()

    assert inner is captured
    with pytest.raises(DbScopeMissingError):
        current_db_scope()


@pytest.mark.parametrize("tenant_id", ["tenant.a", "a/b", "a b", "a\x1fb", "ｔｅｎａｎｔ", "x" * 65])
def test_tenant_scope_rejects_ids_outside_the_allowed_format(tenant_id: str) -> None:
    with pytest.raises(ValueError, match="must match"), tenant_scope(tenant_id):
        pass


@pytest.mark.parametrize("tenant_id", ["org_2abcXYZ", "tenant-1", "A", "x" * 64])
def test_tenant_scope_accepts_plain_ids(tenant_id: str) -> None:
    with tenant_scope(tenant_id) as scope:
        assert scope.tenant_id == tenant_id
