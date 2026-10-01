"""Run a queued job as its tenant.

The one place a run picks its tenant: both executors call
`execute_run_as_tenant`, which resolves the job's tenant and runs
`run_executor.execute_run` inside that `tenant_scope`. With tenant RLS
off it is a plain passthrough.
"""

import structlog

from aegra_api.core.active_runs import active_run_tenants, active_runs
from aegra_api.core.tenancy.resolver import TenantRejectedError, resolve_tenant_id
from aegra_api.core.tenancy.scope import system_scope, tenant_scope
from aegra_api.models.run_job import RunJob
from aegra_api.services.run_executor import _signal_run_done, execute_run
from aegra_api.services.run_status import finalize_run
from aegra_api.services.streaming_service import streaming_service
from aegra_api.settings import settings

logger = structlog.getLogger(__name__)


async def execute_run_as_tenant(job: RunJob) -> None:
    if not settings.tenant.AEGRA_TENANT_RLS_ENABLED:
        await execute_run(job)
        return
    # Every DB access in the run, including tasks the graph spawns, inherits this scope.
    try:
        tenant_id = await resolve_tenant_id(job.user)
    except TenantRejectedError as e:
        await _fail_rejected_run(job, str(e))
        return
    run_id = job.identity.run_id
    with tenant_scope(tenant_id):
        active_run_tenants[run_id] = tenant_id
        try:
            await execute_run(job)
        finally:
            active_run_tenants.pop(run_id, None)


async def _fail_rejected_run(job: RunJob, reason: str) -> None:
    # A queued run whose tenant was deactivated since. No tenant to act as, so the system closes it;
    # stream signals are skipped because they need the tenant's key and its readers get 403 anyway.
    run_id = job.identity.run_id
    logger.warning("Run tenant rejected, failing the run", run_id=run_id, reason=reason)
    try:
        with system_scope("execute_run: fail a run whose tenant was rejected"):
            await finalize_run(
                run_id,
                job.identity.thread_id,
                user_id=job.user.identity,
                status="error",
                thread_status="error",
                output={},
                error=f"Tenant rejected: {reason}",
            )
    finally:
        active_runs.pop(run_id, None)
        await streaming_service.cleanup_run(run_id)
        await _signal_run_done(run_id)
