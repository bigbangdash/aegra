"""Per-tenant envelope encryption for event payloads stored in Redis.

Each tenant has its own data key from a TenantKeyProvider. Ciphertexts bind the
tenant, run and event ids as AES-GCM associated data, so a payload read under
the wrong tenant (or moved to another run or event) fails to decrypt.
"""

import base64
import binascii
import os
from typing import Protocol

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.hkdf import HKDF

from aegra_api.core.tenancy.scope import DbScope, current_db_scope
from aegra_api.settings import settings

SEALED_PREFIX = "v1"
_NONCE_BYTES = 12
_KEY_BYTES = 32


class TenantPayloadError(RuntimeError):
    """Raised when a payload cannot be sealed or opened for the current tenant."""


class TenantKeyProvider(Protocol):
    # version=None asks for the current key; a stored version must stay readable
    # until every ciphertext under it has expired.
    async def get_key(self, tenant_id: str, version: str | None = None) -> tuple[str, bytes]: ...


class StaticKeyProvider:
    """Derives each tenant's key from one master key. For dev, tests and single-key deployments."""

    VERSION = "s1"

    def __init__(self, master_key: bytes) -> None:
        if len(master_key) != _KEY_BYTES:
            raise ValueError(f"master key must be {_KEY_BYTES} bytes, got {len(master_key)}")
        self._master_key = master_key

    async def get_key(self, tenant_id: str, version: str | None = None) -> tuple[str, bytes]:
        if version not in (None, self.VERSION):
            raise TenantPayloadError(f"unknown key version {version!r}")
        hkdf = HKDF(
            algorithm=hashes.SHA256(), length=_KEY_BYTES, salt=None, info=b"aegra-redis-events:" + tenant_id.encode()
        )
        return self.VERSION, hkdf.derive(self._master_key)


def _aad(tenant_id: str, run_id: str, event_id: str, version: str) -> bytes:
    # Unit separator cannot appear in ids we generate, so fields cannot be shifted across boundaries.
    return "\x1f".join((tenant_id, run_id, event_id, version)).encode()


async def seal(provider: TenantKeyProvider, tenant_id: str, run_id: str, event_id: str, plaintext: bytes) -> str:
    version, key = await provider.get_key(tenant_id)
    nonce = os.urandom(_NONCE_BYTES)
    ciphertext = AESGCM(key).encrypt(nonce, plaintext, _aad(tenant_id, run_id, event_id, version))
    body = base64.urlsafe_b64encode(nonce + ciphertext).decode()
    return f"{SEALED_PREFIX}.{version}.{body}"


async def open_sealed(provider: TenantKeyProvider, tenant_id: str, run_id: str, event_id: str, token: str) -> bytes:
    prefix, _, rest = token.partition(".")
    version, _, body = rest.partition(".")
    if prefix != SEALED_PREFIX or not version or not body:
        raise TenantPayloadError("malformed sealed payload")
    try:
        raw = base64.urlsafe_b64decode(body)
    except (binascii.Error, ValueError) as exc:
        raise TenantPayloadError("malformed sealed payload") from exc
    if len(raw) <= _NONCE_BYTES:
        raise TenantPayloadError("malformed sealed payload")
    _, key = await provider.get_key(tenant_id, version)
    try:
        return AESGCM(key).decrypt(raw[:_NONCE_BYTES], raw[_NONCE_BYTES:], _aad(tenant_id, run_id, event_id, version))
    except InvalidTag as exc:
        raise TenantPayloadError(f"payload for run {run_id} does not belong to tenant {tenant_id}") from exc


_provider: TenantKeyProvider | None = None


def configure_key_provider(provider: TenantKeyProvider | None) -> None:
    # Hook for deployments that keep tenant keys elsewhere (e.g. KMS-wrapped keys). Call it at
    # import time; the startup check in main.py runs before any user lifespan.
    global _provider
    _provider = provider


def get_key_provider() -> TenantKeyProvider:
    global _provider
    if _provider is not None:
        return _provider
    secret = settings.tenant.AEGRA_TENANT_REDIS_MASTER_KEY
    if secret is None:
        raise TenantPayloadError(
            "AEGRA_TENANT_REDIS_MASTER_KEY is required when tenant RLS and the Redis broker are both enabled"
        )
    try:
        master_key = base64.b64decode(secret.get_secret_value(), validate=True)
    except (binascii.Error, ValueError) as exc:
        raise TenantPayloadError("AEGRA_TENANT_REDIS_MASTER_KEY must be base64") from exc
    _provider = StaticKeyProvider(master_key)
    return _provider


def encryption_tenant() -> str | None:
    """The tenant whose key protects Redis payloads in this context; None when RLS is off."""
    if not settings.tenant.AEGRA_TENANT_RLS_ENABLED:
        return None
    scope: DbScope = current_db_scope()
    if scope.is_system:
        # No system path reads or writes run events; refusing keeps tenant data out of system scope.
        raise TenantPayloadError(f"run events cannot be accessed from system scope ({scope.system_reason})")
    return scope.tenant_id
