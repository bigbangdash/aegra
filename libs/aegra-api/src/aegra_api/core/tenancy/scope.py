"""Request/job-scoped database scope for tenant row-level security.

With AEGRA_TENANT_RLS_ENABLED every pooled checkout must declare whether it
acts for one tenant or as the system. A missing scope is an error, never an
implicit bypass: forgetting to declare must fail closed.
"""

import contextvars
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass

from aegra_api.settings import settings

# Tenant ids end up in store namespaces (dot-joined), AES-GCM associated data and
# log lines, so only a plain token is accepted. Tighten once the IdP's format is known.
TENANT_ID_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,64}")


def is_valid_tenant_id(tenant_id: str) -> bool:
    return TENANT_ID_PATTERN.fullmatch(tenant_id) is not None


class DbScopeMissingError(RuntimeError):
    """Raised when a tenant-RLS checkout runs without a declared DB scope."""


@dataclass(frozen=True)
class DbScope:
    tenant_id: str | None
    system_reason: str | None = None

    @property
    def is_system(self) -> bool:
        return self.tenant_id is None


_db_scope: contextvars.ContextVar[DbScope | None] = contextvars.ContextVar("AegraDbScope", default=None)


def current_db_scope() -> DbScope:
    scope = _db_scope.get()
    if scope is None:
        raise DbScopeMissingError(
            "No DB scope declared for this operation; wrap it in tenant_scope() or system_scope()"
        )
    return scope


def scoped_tenant_id() -> str | None:
    """Tenant of the current DB scope for the tenant_id column; None with RLS off or in system scope."""
    if not settings.tenant.AEGRA_TENANT_RLS_ENABLED:
        return None
    return current_db_scope().tenant_id


@contextmanager
def tenant_scope(tenant_id: str) -> Iterator[DbScope]:
    if not tenant_id or not is_valid_tenant_id(tenant_id):
        raise ValueError(f"tenant_id must match {TENANT_ID_PATTERN.pattern}: {tenant_id!r}")
    scope = DbScope(tenant_id=tenant_id)
    token = _db_scope.set(scope)
    try:
        yield scope
    finally:
        _db_scope.reset(token)


@contextmanager
def system_scope(reason: str) -> Iterator[DbScope]:
    # The reason is mandatory so every RLS bypass is greppable and auditable.
    if not reason:
        raise ValueError("system_scope requires a reason")
    scope = DbScope(tenant_id=None, system_reason=reason)
    token = _db_scope.set(scope)
    try:
        yield scope
    finally:
        _db_scope.reset(token)


@contextmanager
def bind_scope(scope: DbScope) -> Iterator[DbScope]:
    # Re-enters a scope captured elsewhere, e.g. across a thread -> event loop hop.
    token = _db_scope.set(scope)
    try:
        yield scope
    finally:
        _db_scope.reset(token)
