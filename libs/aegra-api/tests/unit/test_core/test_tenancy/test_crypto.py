import base64
from collections.abc import Iterator

import pytest
from pydantic import SecretStr

from aegra_api.core.tenancy import crypto
from aegra_api.core.tenancy.crypto import (
    StaticKeyProvider,
    TenantPayloadError,
    encryption_tenant,
    get_key_provider,
    open_sealed,
    seal,
)
from aegra_api.core.tenancy.scope import DbScopeMissingError, system_scope, tenant_scope
from aegra_api.settings import settings

MASTER = bytes(range(32))


@pytest.fixture
def provider() -> StaticKeyProvider:
    return StaticKeyProvider(MASTER)


@pytest.fixture
def rls_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)


@pytest.fixture(autouse=True)
def reset_provider() -> Iterator[None]:
    crypto.configure_key_provider(None)
    yield
    crypto.configure_key_provider(None)


@pytest.mark.asyncio
async def test_round_trip_under_same_tenant(provider: StaticKeyProvider) -> None:
    token = await seal(provider, "tenant-a", "run-1", "evt-1", b'["values", {"x": 1}]')

    assert await open_sealed(provider, "tenant-a", "run-1", "evt-1", token) == b'["values", {"x": 1}]'


@pytest.mark.asyncio
async def test_ciphertext_does_not_contain_plaintext(provider: StaticKeyProvider) -> None:
    token = await seal(provider, "tenant-a", "run-1", "evt-1", b"secret-customer-text")

    assert "secret-customer-text" not in token
    assert b"secret-customer-text" not in base64.urlsafe_b64decode(token.split(".", 2)[2])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tenant_id", "run_id", "event_id"),
    [("tenant-b", "run-1", "evt-1"), ("tenant-a", "run-2", "evt-1"), ("tenant-a", "run-1", "evt-2")],
)
async def test_open_fails_when_any_bound_id_differs(
    provider: StaticKeyProvider, tenant_id: str, run_id: str, event_id: str
) -> None:
    token = await seal(provider, "tenant-a", "run-1", "evt-1", b"payload")

    with pytest.raises(TenantPayloadError):
        await open_sealed(provider, tenant_id, run_id, event_id, token)


@pytest.mark.asyncio
async def test_tampered_ciphertext_is_rejected(provider: StaticKeyProvider) -> None:
    token = await seal(provider, "tenant-a", "run-1", "evt-1", b"payload")
    prefix, version, body = token.split(".", 2)
    raw = bytearray(base64.urlsafe_b64decode(body))
    raw[-1] ^= 0x01
    tampered = f"{prefix}.{version}.{base64.urlsafe_b64encode(bytes(raw)).decode()}"

    with pytest.raises(TenantPayloadError):
        await open_sealed(provider, "tenant-a", "run-1", "evt-1", tampered)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "token", ["", "v1", "v1.s1", "v2.s1.AAAA", "v1.s1.!!!not-base64!!!", "v1.s1.YWJj", "v1.s1.AAAAAAAAAAAAAAAA"]
)
async def test_malformed_tokens_are_rejected(provider: StaticKeyProvider, token: str) -> None:
    with pytest.raises(TenantPayloadError):
        await open_sealed(provider, "tenant-a", "run-1", "evt-1", token)


@pytest.mark.asyncio
async def test_unknown_key_version_is_rejected(provider: StaticKeyProvider) -> None:
    token = await seal(provider, "tenant-a", "run-1", "evt-1", b"payload")

    with pytest.raises(TenantPayloadError, match="unknown key version"):
        await open_sealed(provider, "tenant-a", "run-1", "evt-1", token.replace(".s1.", ".s9.", 1))


@pytest.mark.asyncio
async def test_tenants_get_distinct_keys(provider: StaticKeyProvider) -> None:
    _, key_a = await provider.get_key("tenant-a")
    _, key_b = await provider.get_key("tenant-b")

    assert key_a != key_b


def test_master_key_must_be_32_bytes() -> None:
    with pytest.raises(ValueError, match="32 bytes"):
        StaticKeyProvider(b"short")


def test_missing_master_key_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_REDIS_MASTER_KEY", None)

    with pytest.raises(TenantPayloadError, match="AEGRA_TENANT_REDIS_MASTER_KEY"):
        get_key_provider()


def test_master_key_must_be_base64(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_REDIS_MASTER_KEY", SecretStr("not base64 !"))

    with pytest.raises(TenantPayloadError, match="base64"):
        get_key_provider()


def test_master_key_from_settings_builds_static_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_REDIS_MASTER_KEY", SecretStr(base64.b64encode(MASTER).decode()))

    assert isinstance(get_key_provider(), StaticKeyProvider)


def test_configured_provider_takes_precedence(provider: StaticKeyProvider, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_REDIS_MASTER_KEY", None)
    crypto.configure_key_provider(provider)

    assert get_key_provider() is provider


def test_encryption_tenant_is_none_when_rls_is_off() -> None:
    assert encryption_tenant() is None


@pytest.mark.usefixtures("rls_on")
def test_encryption_tenant_follows_tenant_scope() -> None:
    with tenant_scope("tenant-a"):
        assert encryption_tenant() == "tenant-a"


@pytest.mark.usefixtures("rls_on")
def test_encryption_tenant_refuses_system_scope() -> None:
    with system_scope("test"), pytest.raises(TenantPayloadError, match="system scope"):
        encryption_tenant()


@pytest.mark.usefixtures("rls_on")
def test_encryption_tenant_requires_a_scope() -> None:
    with pytest.raises(DbScopeMissingError):
        encryption_tenant()
