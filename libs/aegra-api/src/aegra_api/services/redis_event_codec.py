"""Envelope codec for run events in Redis: plaintext, or sealed per tenant.

Plain messages are ``{"event_id", "payload"}``; sealed ones are
``{"event_id", "end", "sealed"}`` with the end marker in clear so a reader
can stop without a key. Which codec applies follows the DB scope: with
tenant RLS on, events are sealed with the scope's tenant key.
"""

import contextlib
import json
from collections.abc import Iterator
from typing import Any, Protocol

import structlog

from aegra_api.core.active_runs import active_run_tenants
from aegra_api.core.tenancy.crypto import (
    TenantKeyProvider,
    TenantPayloadError,
    encryption_tenant,
    get_key_provider,
    open_sealed,
    seal,
)
from aegra_api.core.tenancy.scope import DbScopeMissingError, tenant_scope

logger = structlog.getLogger(__name__)


class EventCodec(Protocol):
    # body is the JSON-serialized payload; decode returns the JSON-decoded payload.
    async def encode(self, run_id: str, event_id: str, body: str, *, is_end: bool) -> str: ...

    async def decode(self, run_id: str, data: dict[str, Any]) -> Any: ...


class PlainEventCodec:
    async def encode(self, run_id: str, event_id: str, body: str, *, is_end: bool) -> str:
        return json.dumps({"event_id": event_id, "payload": json.loads(body)})

    async def decode(self, run_id: str, data: dict[str, Any]) -> Any:
        if data.get("sealed") is not None:
            raise TenantPayloadError(f"sealed event for run {run_id} but tenant RLS is off")
        return data["payload"]


class SealedEventCodec:
    def __init__(self, tenant_id: str, provider: TenantKeyProvider) -> None:
        self._tenant_id = tenant_id
        self._provider = provider

    async def encode(self, run_id: str, event_id: str, body: str, *, is_end: bool) -> str:
        sealed = await seal(self._provider, self._tenant_id, run_id, event_id, body.encode())
        return json.dumps({"event_id": event_id, "end": is_end, "sealed": sealed})

    async def decode(self, run_id: str, data: dict[str, Any]) -> Any:
        sealed = data.get("sealed")
        if sealed is None:
            raise TenantPayloadError(f"unsealed event for run {run_id} while tenant RLS is on")
        plaintext = await open_sealed(self._provider, self._tenant_id, run_id, data["event_id"], sealed)
        return json.loads(plaintext)


def event_codec() -> EventCodec:
    # The caller's own scope picks the key, so a run read under the wrong tenant fails to open.
    tenant_id = encryption_tenant()
    if tenant_id is None:
        return PlainEventCodec()
    return SealedEventCodec(tenant_id, get_key_provider())


def message_is_end(data: dict[str, Any]) -> bool:
    # Sealed messages carry the end marker in clear so the broker can stop without a key.
    if "sealed" in data:
        return data.get("end") is True
    payload = data["payload"]
    return isinstance(payload, list) and len(payload) >= 1 and payload[0] == "end"


@contextlib.contextmanager
def active_run_scope(run_id: str) -> Iterator[None]:
    """Write as the tenant of a run executing on this instance; the cancel listener has no scope of its own."""
    tenant_id = active_run_tenants.get(run_id)
    try:
        with tenant_scope(tenant_id) if tenant_id else contextlib.nullcontext():
            yield
    except (TenantPayloadError, DbScopeMissingError) as e:
        # The cancelled run's own finally path still emits its end event under its tenant.
        logger.warning(f"Cancel listener could not write the end event for run {run_id}: {e}")
