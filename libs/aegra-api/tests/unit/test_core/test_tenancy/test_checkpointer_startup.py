"""Startup guard for AEGRA_CHECKPOINT_BACKEND=dynamodb."""

import pytest

from aegra_api.core.tenancy import checkpointer
from aegra_api.core.tenancy.checkpointer import CheckpointBackendError, ensure_checkpoint_backend_available
from aegra_api.settings import settings


@pytest.fixture
def dynamodb_backend(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.checkpoint, "AEGRA_CHECKPOINT_BACKEND", "dynamodb")
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", True)


def test_postgres_backend_passes_without_rls_or_extra(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.checkpoint, "AEGRA_CHECKPOINT_BACKEND", "postgres")
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", False)
    monkeypatch.setattr(checkpointer, "DYNAMODB_EXTRA_AVAILABLE", False)

    ensure_checkpoint_backend_available()


def test_dynamodb_backend_refused_when_rls_is_off(dynamodb_backend: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings.tenant, "AEGRA_TENANT_RLS_ENABLED", False)

    with pytest.raises(CheckpointBackendError, match="AEGRA_TENANT_RLS_ENABLED"):
        ensure_checkpoint_backend_available()


def test_dynamodb_backend_refused_when_extra_is_missing(
    dynamodb_backend: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(checkpointer, "DYNAMODB_EXTRA_AVAILABLE", False)

    with pytest.raises(CheckpointBackendError, match=r"aegra-api\[dynamodb\]"):
        ensure_checkpoint_backend_available()


def test_dynamodb_backend_passes_with_rls_and_extra(dynamodb_backend: None, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(checkpointer, "DYNAMODB_EXTRA_AVAILABLE", True)

    ensure_checkpoint_backend_available()
